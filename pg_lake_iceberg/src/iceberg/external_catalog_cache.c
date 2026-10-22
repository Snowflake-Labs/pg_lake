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
 * A table pg_lake does not own keeps its current metadata location in an
 * external catalog, so reading the table starts with asking that catalog
 * where the table is. Asking more than once while one statement runs exposes
 * the statement to whatever the catalog did in between: two references to
 * one table can read two versions of it, and two tables of one object store
 * catalog can be read as of two different points in time. Neither is a state
 * those tables ever had together.
 *
 * So the answers are held in a read, and each question is asked once per
 * read:
 *
 * - under REPEATABLE READ and SERIALIZABLE, a read lasts for the transaction,
 *   because those levels read one state of the database for all of it;
 * - under READ COMMITTED, a read lasts for the portal that runs the
 *   statement, and the next statement asks again. A portal runs every query
 *   of its statement, including the queries of the functions, DO blocks,
 *   procedures and triggers the statement calls, and with the extended
 *   protocol it spans the Bind and Execute messages that start and run it.
 *   A cursor is a portal of its own and reads for as long as it is open.
 *
 * A read ends when Postgres releases the resource owner of its portal or of
 * its transaction: when a statement is done, when a cursor closes, when the
 * subtransaction that opened a cursor aborts, and when the transaction ends
 * by commit, abort or PREPARE TRANSACTION. Subtransactions that abort inside
 * a statement leave its read in place, because what the statement read is
 * still what it is reading. Reads live under TopTransactionContext, so
 * nothing they hold outlives the transaction either way.
 *
 * What a read holds follows how the catalog publishes:
 *
 * - a REST catalog answers for one table per request, so a read holds the
 *   metadata location of each relation it asked about;
 * - an object store catalog publishes all of its tables as one document at a
 *   fixed path, so a read holds the document, which is what makes two tables
 *   of one catalog come from the same version of it.
 *
 * The limits, which the tests lock:
 *
 * - a REPEATABLE READ or SERIALIZABLE transaction is held to the version of
 *   its first read of an external table, not to the version that stood when
 *   it took its snapshot;
 * - under READ COMMITTED a statement holds its version longer than Postgres
 *   holds a heap table still, which every query of a volatile function, DO
 *   block, procedure or trigger reads under a new snapshot. A loop that polls
 *   an external table inside one statement therefore keeps reading the
 *   version the statement started with, until a procedure commits;
 * - a cursor, including the one a PL/pgSQL FOR loop over a query opens, reads
 *   the catalog as it stands when it opens, which can be newer than what the
 *   rest of its statement reads;
 * - a read holds a REST answer under the relation that asked, so a relation
 *   that ALTER FOREIGN TABLE points at another table keeps reading the old
 *   one until the read ends;
 * - attaching an object_store table reads its catalog as it stands, the way
 *   the listing function does, so a table published since a read started can
 *   be attached, but that read cannot see it.
 */

#include "postgres.h"

#include "access/xact.h"
#include "nodes/pg_list.h"
#include "tcop/pquery.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/resowner.h"

/* foreach_ptr() on the versions of Postgres that do not have it yet */
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/iceberg/external_catalog_cache.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/s3_reader_utils.h"


/* where the REST catalog of a read-only table said that table is */
typedef struct RestCatalogTableLocation
{
	Oid			relationId;
	char	   *metadataLocation;
}			RestCatalogTableLocation;

/* an object store catalog document, as it was read from its path */
typedef struct ObjectStoreCatalogDocument
{
	char	   *path;
	char	   *content;
}			ObjectStoreCatalogDocument;

/* what external catalogs answered one portal or one transaction */
typedef struct ExternalCatalogRead
{
	/* releasing this resource owner ends the read */
	ResourceOwner owner;

	/* the read and everything it holds live here */
	MemoryContext context;

	/* list of RestCatalogTableLocation */
	List	   *restCatalogTableLocations;

	/* list of ObjectStoreCatalogDocument */
	List	   *objectStoreCatalogDocuments;
}			ExternalCatalogRead;


/* the reads of the current transaction, in ExternalCatalogReadsContext */
static List *ExternalCatalogReads = NIL;
static MemoryContext ExternalCatalogReadsContext = NULL;

static char *ResolveRestCatalogTable(Oid relationId);
static char *ResolveObjectStoreCatalogTable(Oid relationId);
static char *FindRestCatalogTableLocation(ExternalCatalogRead * read,
										  Oid relationId);
static void RememberRestCatalogTableLocation(ExternalCatalogRead * read,
											 Oid relationId,
											 const char *metadataLocation);
static char *FindObjectStoreCatalogDocument(ExternalCatalogRead * read,
											const char *path);
static void RememberObjectStoreCatalogDocument(ExternalCatalogRead * read,
											   const char *path,
											   const char *content);
static ExternalCatalogRead * GetCurrentExternalCatalogRead(void);
static ResourceOwner GetCurrentReadOwner(void);
static ExternalCatalogRead * StartExternalCatalogRead(ResourceOwner owner);
static void EndExternalCatalogReads(ResourceReleasePhase phase, bool isCommit,
									bool isTopLevel, void *arg);
static void ForgetExternalCatalogReads(void *arg);


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
 * not asked it about that relation yet.
 */
static char *
ResolveRestCatalogTable(Oid relationId)
{
	ExternalCatalogRead *read = GetCurrentExternalCatalogRead();
	char	   *metadataLocation = FindRestCatalogTableLocation(read, relationId);

	if (metadataLocation == NULL)
	{
		metadataLocation =
			GetMetadataLocationForRestCatalogForIcebergTable(relationId);

		RememberRestCatalogTableLocation(read, relationId, metadataLocation);
	}

	/* the read can end before whatever the caller keeps this in */
	return pstrdup(metadataLocation);
}


/*
 * ResolveObjectStoreCatalogTable returns where the catalog document of a
 * read-only object_store table says that table is, reading the document only
 * if the current read has not read it yet.
 */
static char *
ResolveObjectStoreCatalogTable(Oid relationId)
{
	char	   *path = GetExternalObjectStoreCatalogFilePathForTable(relationId);
	ExternalCatalogRead *read = GetCurrentExternalCatalogRead();
	char	   *content = FindObjectStoreCatalogDocument(read, path);

	if (content == NULL)
	{
		/* catalog.json is a fixed path that may change out of band */
		content = GetJsonFromURINoCache(path);

		/* which version of the catalog a reader got is otherwise unanswerable */
		ereport(DEBUG1,
				(errmsg("read external catalog document \"%s\"", path)));

		RememberObjectStoreCatalogDocument(read, path, content);
	}

	return GetMetadataLocationFromExternalObjectStoreCatalogForTable(relationId,
																	 content);
}


/*
 * FindRestCatalogTableLocation returns the metadata location the given read
 * got for a relation, or NULL if it has not asked about that relation.
 */
static char *
FindRestCatalogTableLocation(ExternalCatalogRead * read, Oid relationId)
{
	foreach_ptr(RestCatalogTableLocation, location,
				read->restCatalogTableLocations)
	{
		if (location->relationId == relationId)
			return location->metadataLocation;
	}

	return NULL;
}


/*
 * RememberRestCatalogTableLocation adds the metadata location the REST
 * catalog gave for a relation to the given read.
 */
static void
RememberRestCatalogTableLocation(ExternalCatalogRead * read, Oid relationId,
								 const char *metadataLocation)
{
	MemoryContext previousContext = MemoryContextSwitchTo(read->context);

	RestCatalogTableLocation *location = palloc(sizeof(RestCatalogTableLocation));

	location->relationId = relationId;
	location->metadataLocation = pstrdup(metadataLocation);

	read->restCatalogTableLocations =
		lappend(read->restCatalogTableLocations, location);

	MemoryContextSwitchTo(previousContext);
}


/*
 * FindObjectStoreCatalogDocument returns the content of the catalog document
 * the given read got from a path, or NULL if it has not read that path.
 */
static char *
FindObjectStoreCatalogDocument(ExternalCatalogRead * read, const char *path)
{
	foreach_ptr(ObjectStoreCatalogDocument, document,
				read->objectStoreCatalogDocuments)
	{
		if (strcmp(document->path, path) == 0)
			return document->content;
	}

	return NULL;
}


/*
 * RememberObjectStoreCatalogDocument adds a catalog document read from a path
 * to the given read.
 */
static void
RememberObjectStoreCatalogDocument(ExternalCatalogRead * read, const char *path,
								   const char *content)
{
	MemoryContext previousContext = MemoryContextSwitchTo(read->context);

	ObjectStoreCatalogDocument *document =
		palloc(sizeof(ObjectStoreCatalogDocument));

	document->path = pstrdup(path);
	document->content = pstrdup(content);

	read->objectStoreCatalogDocuments =
		lappend(read->objectStoreCatalogDocuments, document);

	MemoryContextSwitchTo(previousContext);
}


/*
 * GetCurrentExternalCatalogRead returns the read the current query belongs
 * to, starting it when the query is the first of its read to ask an external
 * catalog anything.
 */
static ExternalCatalogRead *
GetCurrentExternalCatalogRead(void)
{
	Assert(IsTransactionState());

	ResourceOwner owner = GetCurrentReadOwner();

	foreach_ptr(ExternalCatalogRead, read, ExternalCatalogReads)
	{
		if (read->owner == owner)
			return read;
	}

	return StartExternalCatalogRead(owner);
}


/*
 * GetCurrentReadOwner returns the resource owner whose release ends the read
 * the current query belongs to: the active portal's under READ COMMITTED,
 * and the transaction's under REPEATABLE READ and SERIALIZABLE. A portal
 * that has no resource owner of its own, which is what a procedure's portal
 * is left with once the procedure commits, and a query that runs outside any
 * portal, read for the rest of the transaction.
 */
static ResourceOwner
GetCurrentReadOwner(void)
{
	if (!IsolationUsesXactSnapshot() &&
		ActivePortal != NULL && ActivePortal->resowner != NULL)
		return ActivePortal->resowner;

	return TopTransactionResourceOwner;
}


/*
 * StartExternalCatalogRead starts an empty read that ends when the given
 * resource owner is released.
 */
static ExternalCatalogRead *
StartExternalCatalogRead(ResourceOwner owner)
{
	static bool releaseCallbackRegistered = false;

	if (!releaseCallbackRegistered)
	{
		RegisterResourceReleaseCallback(EndExternalCatalogReads, NULL);
		releaseCallbackRegistered = true;
	}

	if (ExternalCatalogReadsContext == NULL)
	{
		MemoryContext readsContext =
			AllocSetContextCreate(TopTransactionContext,
								  "pg_lake external catalog reads",
								  ALLOCSET_SMALL_SIZES);
		MemoryContextCallback *forgetCallback =
			MemoryContextAlloc(readsContext, sizeof(MemoryContextCallback));

		forgetCallback->func = ForgetExternalCatalogReads;
		forgetCallback->arg = NULL;

		MemoryContextRegisterResetCallback(readsContext, forgetCallback);

		ExternalCatalogReadsContext = readsContext;
	}

	MemoryContext readContext =
		AllocSetContextCreate(ExternalCatalogReadsContext,
							  "pg_lake external catalog read",
							  ALLOCSET_SMALL_SIZES);
	ExternalCatalogRead *read =
		MemoryContextAllocZero(readContext, sizeof(ExternalCatalogRead));

	read->owner = owner;
	read->context = readContext;

	MemoryContext previousContext =
		MemoryContextSwitchTo(ExternalCatalogReadsContext);

	ExternalCatalogReads = lappend(ExternalCatalogReads, read);

	MemoryContextSwitchTo(previousContext);

	return read;
}


/*
 * EndExternalCatalogReads ends the reads of a resource owner as Postgres
 * releases that owner. resowner.c makes the owner it releases the current one
 * while it calls back, and calls back for each phase of the release, so the
 * first phase ends the reads and the others find nothing left to end.
 */
static void
EndExternalCatalogReads(ResourceReleasePhase phase, bool isCommit,
						bool isTopLevel, void *arg)
{
	ListCell   *readCell = NULL;

	foreach(readCell, ExternalCatalogReads)
	{
		ExternalCatalogRead *read = lfirst(readCell);

		if (read->owner != CurrentResourceOwner)
			continue;

		ExternalCatalogReads =
			foreach_delete_current(ExternalCatalogReads, readCell);

		MemoryContextDelete(read->context);
	}
}


/*
 * ForgetExternalCatalogReads forgets the reads of a transaction as its memory
 * goes, by which time releasing its resource owners has ended every read.
 */
static void
ForgetExternalCatalogReads(void *arg)
{
	ExternalCatalogReads = NIL;
	ExternalCatalogReadsContext = NULL;
}
