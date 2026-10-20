/*
 * Copyright 2026 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * external_catalog_cache.c
 *
 * A table pg_lake does not own keeps its current metadata location outside
 * Postgres, so a reference to such a table has to ask the catalog where the
 * table is right now. Asking twice within one read exposes the reader to
 * whatever the catalog did in between: a query that references the same
 * table twice can read two versions of it, and two tables of one catalog can
 * be read as of two different points in time. Neither is a state those
 * tables ever had together. That is the problem this module exists for.
 * Fewer requests to the catalog are a side effect and not a reason to hold
 * anything.
 *
 * Tables pg_lake owns never get here: their files are listed in our own
 * catalogs, which the transaction snapshot already holds still.
 *
 * What a reader asked is held for exactly as long as Postgres would hold a
 * heap table still for it, which is as long as the snapshot it reads with:
 *
 * - REPEATABLE READ and SERIALIZABLE read one snapshot for the whole
 *   transaction, so all of its queries share one read.
 * - READ COMMITTED takes a new snapshot for every statement, for every
 *   statement of a multi-statement message, and for every query a volatile
 *   function, DO block, procedure or trigger runs, while the queries of a
 *   STABLE or IMMUTABLE function run under their caller's snapshot. So a
 *   read belongs to the snapshot its query runs under, and a query under
 *   another snapshot asks again.
 *
 * Postgres gives a snapshot no identity but its address, and it reuses the
 * memory of a snapshot it frees for the next one. So a read holds its
 * snapshot, which keeps that address from being reused while the read
 * exists, and goes as soon as nothing but the read holds the snapshot.
 *
 * What a read holds follows how the catalog publishes:
 *
 * - a REST catalog answers for one table per request, so its answer is held
 *   under the table it was asked about rather than under the relation that
 *   asked. A relation that ALTER FOREIGN TABLE points at another table then
 *   asks about that table, and two relations naming one table read one
 *   version of it. Two tables of such a catalog are two requests and never
 *   were a single state to begin with, so there is nothing to hold them
 *   together with.
 * - an object store catalog publishes all of its tables as one document at
 *   a fixed path, so that document is held instead. Holding it is what
 *   makes two tables a read resolves from one such catalog come from the
 *   same version of it. Holding a location per table cannot do that,
 *   because each table would still read the document once.
 *
 * Two limits are deliberate, and the tests lock them:
 *
 * - a REPEATABLE READ or SERIALIZABLE transaction is held to the catalog
 *   version of its first read of an external table, not to the version that
 *   stood when it took its snapshot. A transaction that takes its snapshot,
 *   lets the catalog move, and only then reads the table sees the newer
 *   version, where a heap table would not. Resolving every external table at
 *   BEGIN is the wrong price for closing that gap.
 * - attaching an object_store table reads its catalog as it stands, the way
 *   the listing function does. A table published since a REPEATABLE READ or
 *   SERIALIZABLE transaction first read that catalog can therefore be
 *   attached in that transaction, but reading it waits for the next one,
 *   because the transaction's read holds the document as it was, which does
 *   not list the table yet.
 */

#include "postgres.h"

#include "access/xact.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/snapmgr.h"

/* foreach_ptr() on the versions of Postgres that do not have it yet */
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/iceberg/external_catalog_cache.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/s3_reader_utils.h"


/* what an external catalog answered, under the question it answered */
typedef struct CachedAnswer
{
	char	   *question;
	char	   *answer;
}			CachedAnswer;

/*
 * What external catalogs answered the queries that run under one snapshot,
 * or under the one snapshot of a transaction that uses a transaction
 * snapshot.
 */
typedef struct ExternalCatalogRead
{
	/*
	 * The snapshot the read belongs to, held while the read exists, or
	 * InvalidSnapshot for the read of a whole transaction.
	 */
	Snapshot	snapshot;

	/* the read and its answers live here */
	MemoryContext context;

	/* metadata locations, under the REST catalog table each was asked about */
	List	   *restCatalogLocations;

	/* catalog documents, under the path each was read from */
	List	   *catalogDocuments;
}			ExternalCatalogRead;


/*
 * The reads of the current transaction live in a context of their own under
 * TopTransactionContext, so nothing they hold outlives the transaction.
 */
static MemoryContext ExternalCatalogCacheContext = NULL;

/* list of ExternalCatalogRead, in ExternalCatalogCacheContext */
static List *ExternalCatalogReads = NIL;

#if PG_VERSION_NUM >= 170000
/*
 * Since Postgres 17 a resource release callback may not release a resource
 * of an owner that is itself being released, which the transaction's owner is
 * at commit, so the snapshots the reads hold are registered on an owner of
 * our own.
 */
static ResourceOwner ReadSnapshotsOwner = NULL;
#endif

static char *ResolveRestCatalogTable(Oid relationId);
static char *ResolveObjectStoreCatalogTable(Oid relationId);
static char *RestCatalogRequestKey(Oid relationId);
static char *GetCachedAnswer(List *answers, const char *question);
static List *CacheAnswer(ExternalCatalogRead * read, List *answers,
						 const char *question, const char *answer);
static ExternalCatalogRead * GetCurrentExternalCatalogRead(void);
static ExternalCatalogRead * FindOrStartExternalCatalogRead(Snapshot snapshot);
static void CreateExternalCatalogCache(void);
static void ReleaseExternalCatalogReads(bool transactionEnding);
static void ReleaseUnusedExternalCatalogReads(ResourceReleasePhase phase,
											  bool isCommit, bool isTopLevel,
											  void *arg);
static ResourceOwner GetReadSnapshotsOwner(void);
static void ForgetExternalCatalogCache(void *arg);


/*
 * ResolveIcebergExternalMetadataLocation returns the metadata location of a
 * table pg_lake does not own, as the current read is entitled to see it.
 */
char *
ResolveIcebergExternalMetadataLocation(Oid relationId)
{
	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	if (icebergCatalogType == REST_CATALOG_READ_ONLY)
		return ResolveRestCatalogTable(relationId);

	if (icebergCatalogType == OBJECT_STORE_READ_ONLY)
		return ResolveObjectStoreCatalogTable(relationId);

	PgLakeTableProperties tableProperties = GetPgLakeTableProperties(relationId);

	if (icebergCatalogType == NONE_CATALOG &&
		tableProperties.tableType == PG_LAKE_TABLE_TYPE &&
		tableProperties.format == DATA_FORMAT_ICEBERG)
	{
		/*
		 * The location is a table option, and the lock a reader holds on the
		 * relation already keeps that still.
		 */
		return GetForeignTablePath(relationId);
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported iceberg external table type for relation %s",
					get_rel_name(relationId))));
}


/*
 * ResolveRestCatalogTable returns where the REST catalog of a read-only
 * table says that table is, asking the catalog only if the current read has
 * not asked it about that table yet.
 */
static char *
ResolveRestCatalogTable(Oid relationId)
{
	char	   *requestKey = RestCatalogRequestKey(relationId);
	ExternalCatalogRead *read = GetCurrentExternalCatalogRead();

	if (read != NULL)
	{
		char	   *cachedLocation =
			GetCachedAnswer(read->restCatalogLocations, requestKey);

		/* the read goes when its snapshot does, which a caller may outlive */
		if (cachedLocation != NULL)
			return pstrdup(cachedLocation);
	}

	char	   *metadataLocation =
		GetMetadataLocationForRestCatalogForIcebergTable(relationId);

	if (read != NULL)
		read->restCatalogLocations =
			CacheAnswer(read, read->restCatalogLocations, requestKey,
						metadataLocation);

	return metadataLocation;
}


/*
 * ResolveObjectStoreCatalogTable returns where the catalog document of a
 * read-only object_store table says that table is, reading the document only
 * if the current read has not read it yet.
 */
static char *
ResolveObjectStoreCatalogTable(Oid relationId)
{
	char	   *documentPath =
		GetExternalObjectStoreCatalogFilePathForTable(relationId);
	ExternalCatalogRead *read = GetCurrentExternalCatalogRead();
	char	   *document = NULL;

	if (read != NULL)
		document = GetCachedAnswer(read->catalogDocuments, documentPath);

	if (document == NULL)
	{
		/* catalog.json is a fixed path that may change out of band */
		document = GetJsonFromURINoCache(documentPath);

		/* which version of the catalog a reader got is otherwise unanswerable */
		ereport(DEBUG1,
				(errmsg("read external catalog document \"%s\"", documentPath)));

		if (read != NULL)
			read->catalogDocuments =
				CacheAnswer(read, read->catalogDocuments, documentPath, document);
	}

	return GetMetadataLocationFromExternalObjectStoreCatalogForTable(relationId,
																	 document);
}


/*
 * RestCatalogRequestKey returns a key for the table a REST catalog request
 * asks about, rather than for the relation asking, which ALTER FOREIGN TABLE
 * can point at another table. The server is the one the catalog option
 * names, because every Iceberg table has the same foreign server, and the
 * names are length prefixed because an identifier may contain the separator.
 */
static char *
RestCatalogRequestKey(Oid relationId)
{
	Oid			catalogServerId = GetRestCatalogServerIdForRelation(relationId);
	char	   *catalogName = GetRestCatalogName(relationId);
	char	   *namespaceName = GetRestCatalogNamespace(relationId);
	char	   *tableName = GetRestCatalogTableName(relationId);

	return psprintf("%u %zu:%s %zu:%s %zu:%s",
					catalogServerId,
					strlen(catalogName), catalogName,
					strlen(namespaceName), namespaceName,
					strlen(tableName), tableName);
}


/*
 * GetCachedAnswer returns what an external catalog answered to the given
 * question, or NULL if the read holds no answer to it.
 */
static char *
GetCachedAnswer(List *answers, const char *question)
{
	foreach_ptr(CachedAnswer, cachedAnswer, answers)
	{
		if (strcmp(cachedAnswer->question, question) == 0)
			return cachedAnswer->answer;
	}

	return NULL;
}


/*
 * CacheAnswer adds what an external catalog answered to a question the
 * read held no answer to, and returns the list it was added to.
 */
static List *
CacheAnswer(ExternalCatalogRead * read, List *answers, const char *question,
			const char *answer)
{
	MemoryContext previousContext = MemoryContextSwitchTo(read->context);

	CachedAnswer *cachedAnswer = palloc0(sizeof(CachedAnswer));

	cachedAnswer->question = pstrdup(question);
	cachedAnswer->answer = pstrdup(answer);

	answers = lappend(answers, cachedAnswer);

	MemoryContextSwitchTo(previousContext);

	return answers;
}


/*
 * GetCurrentExternalCatalogRead returns the read the answers of the current
 * query belong to, or NULL when the query runs under no snapshot and asks the
 * catalog every time.
 */
static ExternalCatalogRead *
GetCurrentExternalCatalogRead(void)
{
	Assert(IsTransactionState());

	if (ExternalCatalogCacheContext == NULL)
		CreateExternalCatalogCache();

	if (IsolationUsesXactSnapshot())
		return FindOrStartExternalCatalogRead(InvalidSnapshot);

	if (!ActiveSnapshotSet())
		return NULL;

	/* the reads of earlier queries whose snapshots nothing else holds */
	ReleaseExternalCatalogReads(false);

	return FindOrStartExternalCatalogRead(GetActiveSnapshot());
}


/*
 * FindOrStartExternalCatalogRead returns the read that belongs to the given
 * snapshot, starting one if there is none yet.
 */
static ExternalCatalogRead *
FindOrStartExternalCatalogRead(Snapshot snapshot)
{
	foreach_ptr(ExternalCatalogRead, read, ExternalCatalogReads)
	{
		if (read->snapshot == snapshot)
			return read;
	}

	MemoryContext readContext =
		AllocSetContextCreate(ExternalCatalogCacheContext,
							  "pg_lake external catalog read",
							  ALLOCSET_SMALL_SIZES);
	ExternalCatalogRead *read =
		MemoryContextAllocZero(readContext, sizeof(ExternalCatalogRead));

	read->context = readContext;

	MemoryContext previousContext =
		MemoryContextSwitchTo(ExternalCatalogCacheContext);

	ExternalCatalogReads = lappend(ExternalCatalogReads, read);

	MemoryContextSwitchTo(previousContext);

	/*
	 * Held only once the read is listed, so that every snapshot held is one
	 * ReleaseExternalCatalogReads can find. An active snapshot is a copy
	 * already, so registering it returns the snapshot itself.
	 */
	if (snapshot != InvalidSnapshot)
		read->snapshot = RegisterSnapshotOnOwner(snapshot, GetReadSnapshotsOwner());

	Assert(read->snapshot == snapshot);

	return read;
}


/*
 * CreateExternalCatalogCache sets up the cache for a transaction that is
 * about to read its first external catalog.
 */
static void
CreateExternalCatalogCache(void)
{
	static bool releaseCallbackRegistered = false;

	if (!releaseCallbackRegistered)
	{
		RegisterResourceReleaseCallback(ReleaseUnusedExternalCatalogReads, NULL);
		releaseCallbackRegistered = true;
	}

	/*
	 * The cache belongs to the transaction rather than to the current
	 * subtransaction, because a location a subtransaction resolved is still
	 * the version of the table the transaction is reading after that
	 * subtransaction rolls back.
	 */
	MemoryContext cacheContext =
		AllocSetContextCreate(TopTransactionContext,
							  "pg_lake external catalog cache",
							  ALLOCSET_SMALL_SIZES);

	MemoryContextCallback *forgetCallback =
		MemoryContextAlloc(cacheContext, sizeof(MemoryContextCallback));

	forgetCallback->func = ForgetExternalCatalogCache;
	forgetCallback->arg = NULL;

	MemoryContextRegisterResetCallback(cacheContext, forgetCallback);

	/* published only once the callback that forgets it is in place */
	ExternalCatalogCacheContext = cacheContext;
}


/*
 * ReleaseExternalCatalogReads drops the reads whose snapshot nothing but the
 * read holds any more, or every read that holds a snapshot when the
 * transaction is ending, and lets go of their snapshots.
 */
static void
ReleaseExternalCatalogReads(bool transactionEnding)
{
	ListCell   *readCell = NULL;

	foreach(readCell, ExternalCatalogReads)
	{
		ExternalCatalogRead *read = lfirst(readCell);
		Snapshot	snapshot = read->snapshot;

		/* the read of a whole transaction goes with the transaction */
		if (snapshot == InvalidSnapshot)
			continue;

		/* a query still runs under it, or something else still holds it */
		if (!transactionEnding &&
			(snapshot->active_count > 0 || snapshot->regd_count > 1))
			continue;

		ExternalCatalogReads =
			foreach_delete_current(ExternalCatalogReads, readCell);

		MemoryContextDelete(read->context);
		UnregisterSnapshotFromOwner(snapshot, GetReadSnapshotsOwner());
	}
}


/*
 * ReleaseUnusedExternalCatalogReads runs whenever Postgres releases a
 * resource owner, which it does at the end of every statement, subtransaction
 * and transaction, including at abort and at PREPARE TRANSACTION.
 */
static void
ReleaseUnusedExternalCatalogReads(ResourceReleasePhase phase, bool isCommit,
								  bool isTopLevel, void *arg)
{
	/*
	 * Snapshot references are released in a later phase, so at commit the
	 * reads let go of theirs before Postgres reports any still held as
	 * leaked.
	 */
	if (phase != RESOURCE_RELEASE_BEFORE_LOCKS)
		return;

	if (ExternalCatalogCacheContext == NULL)
		return;

	ReleaseExternalCatalogReads(isTopLevel);
}


/*
 * GetReadSnapshotsOwner returns the resource owner the snapshots of the reads
 * are registered on.
 */
static ResourceOwner
GetReadSnapshotsOwner(void)
{
#if PG_VERSION_NUM >= 170000
	if (ReadSnapshotsOwner == NULL)
		ReadSnapshotsOwner = ResourceOwnerCreate(NULL,
												 "pg_lake external catalog reads");

	return ReadSnapshotsOwner;
#else
	return TopTransactionResourceOwner;
#endif
}


/*
 * ForgetExternalCatalogCache forgets what pointed into the cache when the
 * transaction's memory goes, by which time the release callback has let go
 * of every snapshot a read held.
 */
static void
ForgetExternalCatalogCache(void *arg)
{
	ExternalCatalogCacheContext = NULL;
	ExternalCatalogReads = NIL;
}
