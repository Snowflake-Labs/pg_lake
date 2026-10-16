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
 * What a reader asked is held for as long as that reader is entitled to the
 * same catalog state. Three rules decide that, and
 * CurrentCommandMayReadNewerState() is the one place that applies them:
 *
 * 1. within one command, every isolation level reuses what the command
 *    already asked, so one command reads one version of a table,
 * 2. a later command of a REPEATABLE READ or SERIALIZABLE transaction
 *    reuses it too, because those levels read one state of the database
 *    for the whole transaction,
 * 3. a later command of a READ COMMITTED transaction asks again, because it
 *    is entitled to the latest committed state.
 *
 * What is held follows how the catalog publishes:
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
 *   makes two tables a command resolves from one such catalog come from the
 *   same version of it. Holding a location per table cannot do that,
 *   because each table would still read the document once.
 *
 * Three limits are deliberate, and the tests lock them:
 *
 * - the state is pinned when a table is first resolved, not when the
 *   transaction took its snapshot. A REPEATABLE READ transaction that
 *   takes its snapshot, lets the catalog move, and only then reads the
 *   table sees the newer version, where a heap table would not. Resolving
 *   every external table at BEGIN is the wrong price for closing that
 *   gap.
 * - commands are told apart by the statement start timestamp, which does
 *   not advance for the statements inside a function body, nor between
 *   the statements of one multi-statement message. READ COMMITTED
 *   therefore holds one version of a table across those, which is longer
 *   than the isolation level requires. Holding longer cannot tear a read,
 *   and that is the direction to err in.
 * - attaching an object_store table reads its catalog as it stands, the
 *   way the listing function does. A table published since a transaction
 *   or function body first read that catalog can therefore be attached
 *   there, but reading it waits for a command entitled to newer state,
 *   because the reads answer from the document already held, which does
 *   not list the table yet.
 */

#include "postgres.h"

#include "access/xact.h"
#include "nodes/pg_list.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

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
 * Everything the cache holds lives in a context of its own under
 * TopTransactionContext, so dropping the cache is one delete away and nothing
 * it holds outlives the transaction.
 */
static MemoryContext ExternalCatalogCacheContext = NULL;

/* metadata locations, under the REST catalog table each was asked about */
static List *RestCatalogLocations = NIL;

/* catalog documents, under the path each was read from */
static List *CatalogDocuments = NIL;

/*
 * The command whose answers the cache holds, as its statement start
 * timestamp. Under an isolation level that uses a transaction snapshot,
 * this is the command that asked first and later commands of the
 * transaction keep reading its answers.
 */
static TimestampTz CacheFilledInCommand = 0;

static char *ResolveRestCatalogTable(Oid relationId);
static char *ResolveObjectStoreCatalogTable(Oid relationId);
static char *RestCatalogRequestKey(Oid relationId);
static char *GetCachedAnswer(List *answers, const char *question);
static List *CacheAnswer(List *answers, const char *question,
						 const char *answer);
static void PrepareExternalCatalogCache(void);
static bool CurrentCommandMayReadNewerState(void);
static void CreateExternalCatalogCache(void);
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

	PrepareExternalCatalogCache();

	char	   *cachedLocation = GetCachedAnswer(RestCatalogLocations, requestKey);

	/* a later READ COMMITTED command frees what this points into */
	if (cachedLocation != NULL)
		return pstrdup(cachedLocation);

	char	   *metadataLocation =
		GetMetadataLocationForRestCatalogForIcebergTable(relationId);

	RestCatalogLocations =
		CacheAnswer(RestCatalogLocations, requestKey, metadataLocation);

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

	PrepareExternalCatalogCache();

	char	   *document = GetCachedAnswer(CatalogDocuments, documentPath);

	if (document == NULL)
	{
		/* catalog.json is a fixed path that may change out of band */
		document = GetJsonFromURINoCache(documentPath);

		/* which version of the catalog a reader got is otherwise unanswerable */
		ereport(DEBUG1,
				(errmsg("read external catalog document \"%s\"", documentPath)));

		CatalogDocuments = CacheAnswer(CatalogDocuments, documentPath, document);
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
 * question, or NULL if the cache holds no answer to it.
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
 * cache held no answer to, and returns the list it was added to.
 */
static List *
CacheAnswer(List *answers, const char *question, const char *answer)
{
	MemoryContext previousContext =
		MemoryContextSwitchTo(ExternalCatalogCacheContext);

	CachedAnswer *cachedAnswer = palloc0(sizeof(CachedAnswer));

	cachedAnswer->question = pstrdup(question);
	cachedAnswer->answer = pstrdup(answer);

	answers = lappend(answers, cachedAnswer);

	MemoryContextSwitchTo(previousContext);

	return answers;
}


/*
 * PrepareExternalCatalogCache makes sure the cache exists and holds only
 * answers the current command is entitled to.
 */
static void
PrepareExternalCatalogCache(void)
{
	Assert(IsTransactionState());

	/* deleting the context runs ForgetExternalCatalogCache */
	if (ExternalCatalogCacheContext != NULL && CurrentCommandMayReadNewerState())
		MemoryContextDelete(ExternalCatalogCacheContext);

	if (ExternalCatalogCacheContext == NULL)
		CreateExternalCatalogCache();
}


/*
 * CurrentCommandMayReadNewerState returns whether the current command is
 * entitled to newer catalog state than the answers the cache holds.
 */
static bool
CurrentCommandMayReadNewerState(void)
{
	/*
	 * REPEATABLE READ and SERIALIZABLE read one state of the database for the
	 * whole transaction, so all of its commands read what the cache holds.
	 */
	if (IsolationUsesXactSnapshot())
		return false;

	/*
	 * READ COMMITTED reads the latest committed state as of the start of each
	 * command, so the command that filled the cache keeps reading what it
	 * holds, and a later command may read newer state.
	 */
	return CacheFilledInCommand != GetCurrentStatementStartTimestamp();
}


/*
 * CreateExternalCatalogCache starts an empty cache for the current command.
 */
static void
CreateExternalCatalogCache(void)
{
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

	/*
	 * The cache only ever goes by this context being deleted, either for a
	 * later READ COMMITTED command or along with TopTransactionContext, which
	 * commit, abort and PREPARE TRANSACTION all reset or delete. So a reset
	 * callback on the context is the one place that forgets the cache.
	 */
	MemoryContextCallback *forgetCallback =
		MemoryContextAlloc(cacheContext, sizeof(MemoryContextCallback));

	forgetCallback->func = ForgetExternalCatalogCache;
	forgetCallback->arg = NULL;

	MemoryContextRegisterResetCallback(cacheContext, forgetCallback);

	/* published only once the callback that forgets it is in place */
	ExternalCatalogCacheContext = cacheContext;
	CacheFilledInCommand = GetCurrentStatementStartTimestamp();
}


/*
 * ForgetExternalCatalogCache drops what pointed into the cache, as its
 * context is deleted.
 */
static void
ForgetExternalCatalogCache(void *arg)
{
	ExternalCatalogCacheContext = NULL;
	RestCatalogLocations = NIL;
	CatalogDocuments = NIL;
	CacheFilledInCommand = 0;
}
