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
 * A table in an external catalog keeps its current metadata location
 * outside Postgres, so a reference to such a table has to ask the catalog
 * where the table is right now. Asking twice within one read exposes the
 * reader to whatever the catalog did in between: a query that references
 * the same table twice can read two versions of it, and two tables of one
 * catalog can be read as of two different points in time. Neither is a
 * state those tables ever had together. That is the problem this module
 * exists for. Fewer requests to the catalog are a side effect and not a
 * reason to hold anything.
 *
 * What a reader resolved is held for as long as that reader is entitled to
 * the same catalog state. Three rules decide that, and
 * DecideExternalCatalogCacheUse() is the one place that applies them:
 *
 * 1. within one command, every isolation level reuses what the command
 *    already resolved, so one command reads one version of a table,
 * 2. a later command of a REPEATABLE READ or SERIALIZABLE transaction
 *    reuses it too, because those levels read one state of the database
 *    for the whole transaction,
 * 3. a later command of a READ COMMITTED transaction resolves again,
 *    because it is entitled to the latest committed state.
 *
 * Two kinds of state are held, one per catalog shape, because what has to
 * stay still differs with how the catalog publishes:
 *
 * - a REST catalog answers for one table per request, so the location it
 *   answered with is held per relation that asked. Two tables of such a
 *   catalog are two requests and never were a single state to begin with,
 *   so there is nothing to hold them together with. ALTER FOREIGN TABLE
 *   can point a read-only table at another table of the same catalog,
 *   which is the one command that changes the answer for a relation while
 *   the cache holds it, and it drops what the relation resolved.
 * - an object store catalog publishes all of its tables as one document
 *   at a fixed path, so that document is held instead. Holding it is what
 *   makes two tables a command resolves from one such catalog come from
 *   the same version of it. A per relation cache cannot do that, because
 *   each relation is resolved exactly once per command and the two
 *   resolutions would still read the document twice.
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
 * - attaching a table resolves it through the held document too, so
 *   CREATE TABLE ... read_only for a table published since that document
 *   was read fails with no table found, inside a transaction or function
 *   body that already read the catalog. It is the same version the reads
 *   of that transaction come from. The listing function asks the catalog
 *   every time and does show the table.
 */

#include "postgres.h"

#include "access/xact.h"
#include "nodes/pg_list.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"

/* foreach_ptr() on the versions of Postgres that do not have it yet */
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/iceberg/external_catalog_cache.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/s3_reader_utils.h"


/* a metadata location resolved from an external catalog */
typedef struct CachedMetadataLocation
{
	Oid			relationId;
	char	   *metadataLocation;
}			CachedMetadataLocation;

/* the contents of a document that lists the tables of a catalog */
typedef struct CachedCatalogDocument
{
	char	   *documentPath;
	char	   *documentContent;
}			CachedCatalogDocument;

/* what the current caller may do with the state the cache holds */
typedef enum ExternalCatalogCacheUse
{
	/* the caller is entitled to the state the cache holds */
	EXTERNAL_CATALOG_CACHE_REUSE,

	/* the caller is entitled to newer state, so the cache starts over */
	EXTERNAL_CATALOG_CACHE_REFRESH,
}			ExternalCatalogCacheUse;


/*
 * Everything the cache holds lives in a context of its own, so starting
 * over is one reset away and nothing outlives the transaction.
 */
static MemoryContext ExternalCatalogCacheContext = NULL;

/* list of CachedMetadataLocation, for catalogs answering per table */
static List *MetadataLocationCache = NIL;

/* list of CachedCatalogDocument, for catalogs publishing one document */
static List *CatalogDocumentCache = NIL;

/*
 * The command whose resolutions the cache holds, as its statement start
 * timestamp. Under an isolation level that uses a transaction snapshot,
 * this is the command that resolved them first and later commands of the
 * transaction keep reading them.
 */
static TimestampTz CacheFilledInCommand = 0;

static char *GetCachedExternalMetadataLocation(Oid relationId);
static void CacheExternalMetadataLocation(Oid relationId,
										  const char *metadataLocation);
static char *GetCachedExternalCatalogDocument(const char *documentPath);
static void CacheExternalCatalogDocument(const char *documentPath,
										 const char *documentContent);
static void PrepareExternalCatalogCache(void);
static ExternalCatalogCacheUse DecideExternalCatalogCacheUse(void);
static void CreateExternalCatalogCache(void);
static void DiscardExternalCatalogCache(void);
static void ForgetExternalCatalogCache(void *arg);


/*
 * GetRestCatalogMetadataLocationOncePerRead returns where the given
 * relation's table is in its REST catalog, asking the catalog on the first
 * read that needs it and answering the rest of that read with what it
 * answered then.
 *
 * GetMetadataLocationForRestCatalogForIcebergTable is the resolver that
 * asks every time, which is what a caller acting on the table as the
 * catalog has it now wants.
 */
char *
GetRestCatalogMetadataLocationOncePerRead(Oid relationId)
{
	char	   *cachedLocation = GetCachedExternalMetadataLocation(relationId);

	if (cachedLocation != NULL)
		return cachedLocation;

	char	   *metadataLocation =
		GetMetadataLocationForRestCatalogForIcebergTable(relationId);

	CacheExternalMetadataLocation(relationId, metadataLocation);

	return metadataLocation;
}


/*
 * GetObjectStoreCatalogDocumentOncePerRead returns the document that lists
 * the tables of the catalog at the given path, reading it on the first
 * read that needs it and answering the rest of that read with the version
 * it read then.
 *
 * The document sits at a fixed path and whoever owns the catalog replaces
 * it in place, so it is read with the file cache out of the way.
 */
char *
GetObjectStoreCatalogDocumentOncePerRead(const char *documentPath)
{
	char	   *cachedDocument = GetCachedExternalCatalogDocument(documentPath);

	if (cachedDocument != NULL)
		return cachedDocument;

	char	   *documentContent = GetJsonFromURINoCache(documentPath);

	/* which version of the catalog a reader got is otherwise unanswerable */
	ereport(DEBUG1,
			(errmsg("read external catalog document \"%s\"", documentPath)));

	CacheExternalCatalogDocument(documentPath, documentContent);

	return documentContent;
}


/*
 * GetCachedExternalMetadataLocation returns the metadata location this
 * transaction resolved for the given relation and is still entitled to, or
 * NULL if the caller has to ask the catalog.
 */
static char *
GetCachedExternalMetadataLocation(Oid relationId)
{
	PrepareExternalCatalogCache();

	foreach_ptr(CachedMetadataLocation, cachedLocation, MetadataLocationCache)
	{
		if (cachedLocation->relationId == relationId)
			return cachedLocation->metadataLocation;
	}

	return NULL;
}


/*
 * CacheExternalMetadataLocation records a metadata location resolved from
 * an external catalog.
 */
static void
CacheExternalMetadataLocation(Oid relationId, const char *metadataLocation)
{
	PrepareExternalCatalogCache();

	/*
	 * Only reached after the lookup above found nothing for this relation, so
	 * there is nothing to replace here.
	 */
	MemoryContext previousContext =
		MemoryContextSwitchTo(ExternalCatalogCacheContext);

	CachedMetadataLocation *cachedLocation = palloc0(sizeof(CachedMetadataLocation));

	cachedLocation->relationId = relationId;
	cachedLocation->metadataLocation = pstrdup(metadataLocation);

	MetadataLocationCache = lappend(MetadataLocationCache, cachedLocation);

	MemoryContextSwitchTo(previousContext);
}


/*
 * ForgetRestCatalogMetadataLocation drops what the given relation
 * resolved, because what it resolves has changed under the cache rather
 * than the reader becoming entitled to newer catalog state.
 */
void
ForgetRestCatalogMetadataLocation(Oid relationId)
{
	ListCell   *locationCell = NULL;

	/* nothing was resolved in this transaction yet */
	if (ExternalCatalogCacheContext == NULL)
		return;

	foreach(locationCell, MetadataLocationCache)
	{
		CachedMetadataLocation *cachedLocation = lfirst(locationCell);

		if (cachedLocation->relationId == relationId)
		{
			MetadataLocationCache =
				foreach_delete_current(MetadataLocationCache, locationCell);
			return;
		}
	}
}


/*
 * GetCachedExternalCatalogDocument returns the contents of the catalog
 * document at the given path if this transaction already read a version of
 * it that it is still entitled to, and NULL otherwise.
 */
static char *
GetCachedExternalCatalogDocument(const char *documentPath)
{
	PrepareExternalCatalogCache();

	foreach_ptr(CachedCatalogDocument, cachedDocument, CatalogDocumentCache)
	{
		if (strcmp(cachedDocument->documentPath, documentPath) == 0)
			return cachedDocument->documentContent;
	}

	return NULL;
}


/*
 * CacheExternalCatalogDocument records the contents of a catalog document.
 */
static void
CacheExternalCatalogDocument(const char *documentPath, const char *documentContent)
{
	PrepareExternalCatalogCache();

	/*
	 * Only reached after the lookup above found no version of this document,
	 * so there is nothing to replace here.
	 */
	MemoryContext previousContext =
		MemoryContextSwitchTo(ExternalCatalogCacheContext);

	CachedCatalogDocument *cachedDocument = palloc0(sizeof(CachedCatalogDocument));

	cachedDocument->documentPath = pstrdup(documentPath);
	cachedDocument->documentContent = pstrdup(documentContent);

	CatalogDocumentCache = lappend(CatalogDocumentCache, cachedDocument);

	MemoryContextSwitchTo(previousContext);
}


/*
 * PrepareExternalCatalogCache makes sure the cache exists and holds only
 * state the current caller is entitled to read.
 */
static void
PrepareExternalCatalogCache(void)
{
	Assert(IsTransactionState());

	if (ExternalCatalogCacheContext == NULL)
	{
		CreateExternalCatalogCache();
		return;
	}

	if (DecideExternalCatalogCacheUse() == EXTERNAL_CATALOG_CACHE_REFRESH)
		DiscardExternalCatalogCache();
}


/*
 * DecideExternalCatalogCacheUse decides whether the current caller reads
 * the catalog state the cache holds or newer state than that.
 */
static ExternalCatalogCacheUse
DecideExternalCatalogCacheUse(void)
{
	bool		sameCommand =
		CacheFilledInCommand == GetCurrentStatementStartTimestamp();

	if (sameCommand)
	{
		/*
		 * The command that filled the cache is the command asking now. One
		 * command reads one version of a table, at every isolation level.
		 */
		return EXTERNAL_CATALOG_CACHE_REUSE;
	}

	if (IsolationUsesXactSnapshot())
	{
		/*
		 * A later command of a REPEATABLE READ or SERIALIZABLE transaction.
		 * Those levels read one state of the database for the whole
		 * transaction, so it reads what the earlier command resolved.
		 */
		return EXTERNAL_CATALOG_CACHE_REUSE;
	}

	/*
	 * A later command of a READ COMMITTED transaction, which is entitled to
	 * the latest committed state rather than to the state an earlier command
	 * of the same transaction read.
	 */
	return EXTERNAL_CATALOG_CACHE_REFRESH;
}


/*
 * CreateExternalCatalogCache sets up the cache for the transaction that is
 * about to resolve its first external catalog reference.
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
	ExternalCatalogCacheContext =
		AllocSetContextCreate(TopTransactionContext,
							  "pg_lake external catalog cache",
							  ALLOCSET_SMALL_SIZES);

	/*
	 * Every way out of a transaction resets or deletes TopTransactionContext,
	 * so one reset callback covers commit, abort and PREPARE TRANSACTION
	 * alike. An end-of-transaction callback would have to enumerate those
	 * events and would miss the ones it did not list.
	 */
	MemoryContextCallback *forgetCallback =
		MemoryContextAlloc(TopTransactionContext,
						   sizeof(MemoryContextCallback));

	forgetCallback->func = ForgetExternalCatalogCache;
	forgetCallback->arg = NULL;

	MemoryContextRegisterResetCallback(TopTransactionContext, forgetCallback);

	MetadataLocationCache = NIL;
	CatalogDocumentCache = NIL;
	CacheFilledInCommand = GetCurrentStatementStartTimestamp();
}


/*
 * DiscardExternalCatalogCache drops what earlier commands of this
 * transaction resolved and hands the cache to the current command.
 */
static void
DiscardExternalCatalogCache(void)
{
	MemoryContextReset(ExternalCatalogCacheContext);

	MetadataLocationCache = NIL;
	CatalogDocumentCache = NIL;
	CacheFilledInCommand = GetCurrentStatementStartTimestamp();
}


/*
 * ForgetExternalCatalogCache drops the cache along with the transaction
 * that filled it.
 */
static void
ForgetExternalCatalogCache(void *arg)
{
	/*
	 * TopTransactionContext deletes its children before it runs its reset
	 * callbacks, so the cache context is already gone by now.
	 */
	ExternalCatalogCacheContext = NULL;
	MetadataLocationCache = NIL;
	CatalogDocumentCache = NIL;
	CacheFilledInCommand = 0;
}
