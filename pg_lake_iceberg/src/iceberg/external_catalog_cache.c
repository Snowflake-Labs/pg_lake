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
 * So the first answer is held, and every later question in the same version
 * scope gets the same answer. The version scope is how long readers see one
 * version of each external catalog, and it follows the isolation level the
 * way the snapshot of a statement does:
 *
 * - REPEATABLE READ and SERIALIZABLE read the whole transaction from one
 *   snapshot, so the version scope is the transaction;
 * - READ COMMITTED reads each statement from a new snapshot, so the version
 *   scope is the statement, and the next statement asks again.
 *
 * GetCatalogVersionScope() applies that rule. Each version scope ends when
 * Postgres releases a resource owner, which GetCatalogVersionScopeOwner()
 * picks: TopTransactionResourceOwner for a transaction, and the resource
 * owner of the portal that runs a statement for a statement. A resource
 * release callback drops the versions held for a scope as its owner goes, so
 * they go however the scope ends: a statement finishing, a cursor closing,
 * the subtransaction that opened a cursor aborting, or the transaction ending
 * by commit, abort or PREPARE TRANSACTION. A subtransaction that aborts inside
 * a statement leaves the statement's versions in place, since they are still
 * what the statement reads. They are all allocated under
 * TopTransactionContext, so nothing outlives the transaction either way.
 *
 * What a scope holds follows how the catalog publishes:
 *
 * - a REST catalog answers for one table per request, so the metadata
 *   location of each relation asked about is held;
 * - an object store catalog publishes all of its tables as one document at a
 *   fixed path, so the document is held, which is what makes two tables of
 *   one catalog come from the same version of it.
 *
 * The limits, which the tests lock:
 *
 * - a REPEATABLE READ or SERIALIZABLE transaction sees the versions that
 *   stood when it first asked, not the ones that stood when it took its
 *   snapshot;
 * - under READ COMMITTED a statement holds its versions longer than Postgres
 *   holds a heap table still, which every query of a volatile function, DO
 *   block, procedure or trigger reads under a new snapshot. A loop that polls
 *   an external table inside one statement therefore keeps seeing the version
 *   the statement started with, until a procedure commits;
 * - a cursor, including the one a PL/pgSQL FOR loop over a query opens, is a
 *   version scope of its own, so it sees the catalog as it stands when it
 *   opens, which can be newer than what the rest of its statement sees;
 * - a REST answer is held under the relation that asked, so a relation that
 *   ALTER FOREIGN TABLE points at another table keeps seeing the old one until
 *   its version scope ends;
 * - attaching an object_store table reads its catalog as it stands, the way
 *   the listing function does, so a table published since a version scope
 *   started can be attached, but that scope cannot see it.
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


/* how long readers see one version of each external catalog */
typedef enum CatalogVersionScope
{
	VERSION_SCOPE_TRANSACTION,
	VERSION_SCOPE_STATEMENT
}			CatalogVersionScope;

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

/* the versions of external catalogs held for one version scope */
typedef struct CatalogVersions
{
	/* releasing this resource owner ends the version scope */
	ResourceOwner scopeOwner;

	/* the versions and everything they hold live here */
	MemoryContext context;

	/* list of RestCatalogTableLocation */
	List	   *restCatalogTableLocations;

	/* list of ObjectStoreCatalogDocument */
	List	   *objectStoreCatalogDocuments;
}			CatalogVersions;


/*
 * The versions held for the version scopes of the current transaction, one
 * CatalogVersions per scope, in HeldCatalogVersionsContext.
 */
static List *HeldCatalogVersions = NIL;
static MemoryContext HeldCatalogVersionsContext = NULL;

static char *ResolveRestCatalogTable(Oid relationId);
static char *ResolveObjectStoreCatalogTable(Oid relationId);
static char *FindRestCatalogTableLocation(CatalogVersions * versions,
										  Oid relationId);
static void RememberRestCatalogTableLocation(CatalogVersions * versions,
											 Oid relationId,
											 const char *metadataLocation);
static char *FindObjectStoreCatalogDocument(CatalogVersions * versions,
											const char *path);
static void RememberObjectStoreCatalogDocument(CatalogVersions * versions,
											   const char *path,
											   const char *content);
static CatalogVersions * GetCurrentCatalogVersions(void);
static CatalogVersionScope GetCatalogVersionScope(void);
static ResourceOwner GetCatalogVersionScopeOwner(CatalogVersionScope scope);
static CatalogVersions * CreateCatalogVersions(ResourceOwner scopeOwner);
static void ReleaseCatalogVersions(ResourceReleasePhase phase, bool isCommit,
								   bool isTopLevel, void *arg);
static void ForgetHeldCatalogVersions(void *arg);


/*
 * ResolveIcebergExternalMetadataLocation returns the metadata location of a
 * table pg_lake does not own, as the current version scope sees it.
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
 * table says that table is, asking the catalog only if nothing in the
 * current version scope has asked about that relation yet.
 */
static char *
ResolveRestCatalogTable(Oid relationId)
{
	CatalogVersions *versions = GetCurrentCatalogVersions();
	char	   *metadataLocation =
		FindRestCatalogTableLocation(versions, relationId);

	if (metadataLocation == NULL)
	{
		metadataLocation =
			GetMetadataLocationForRestCatalogForIcebergTable(relationId);

		RememberRestCatalogTableLocation(versions, relationId,
										 metadataLocation);
	}

	/* the scope can end before whatever the caller keeps this in */
	return pstrdup(metadataLocation);
}


/*
 * ResolveObjectStoreCatalogTable returns where the catalog document of a
 * read-only object_store table says that table is, reading the document only
 * if nothing in the current version scope has read it yet.
 */
static char *
ResolveObjectStoreCatalogTable(Oid relationId)
{
	char	   *path = GetExternalObjectStoreCatalogFilePathForTable(relationId);
	CatalogVersions *versions = GetCurrentCatalogVersions();
	char	   *content = FindObjectStoreCatalogDocument(versions, path);

	if (content == NULL)
	{
		/* catalog.json is a fixed path that may change out of band */
		content = GetJsonFromURINoCache(path);

		/* which version of the catalog a reader got is otherwise unanswerable */
		ereport(DEBUG1,
				(errmsg("read external catalog document \"%s\"", path)));

		RememberObjectStoreCatalogDocument(versions, path, content);
	}

	return GetMetadataLocationFromExternalObjectStoreCatalogForTable(relationId,
																	 content);
}


/*
 * FindRestCatalogTableLocation returns the metadata location held for a
 * relation, or NULL if the scope has not asked about that relation yet.
 */
static char *
FindRestCatalogTableLocation(CatalogVersions * versions, Oid relationId)
{
	foreach_ptr(RestCatalogTableLocation, location,
				versions->restCatalogTableLocations)
	{
		if (location->relationId == relationId)
			return location->metadataLocation;
	}

	return NULL;
}


/*
 * RememberRestCatalogTableLocation holds the metadata location the REST
 * catalog gave for a relation.
 */
static void
RememberRestCatalogTableLocation(CatalogVersions * versions, Oid relationId,
								 const char *metadataLocation)
{
	MemoryContext previousContext = MemoryContextSwitchTo(versions->context);

	RestCatalogTableLocation *location = palloc(sizeof(RestCatalogTableLocation));

	location->relationId = relationId;
	location->metadataLocation = pstrdup(metadataLocation);

	versions->restCatalogTableLocations =
		lappend(versions->restCatalogTableLocations, location);

	MemoryContextSwitchTo(previousContext);
}


/*
 * FindObjectStoreCatalogDocument returns the content of the catalog document
 * held for a path, or NULL if the scope has not read that path yet.
 */
static char *
FindObjectStoreCatalogDocument(CatalogVersions * versions, const char *path)
{
	foreach_ptr(ObjectStoreCatalogDocument, document,
				versions->objectStoreCatalogDocuments)
	{
		if (strcmp(document->path, path) == 0)
			return document->content;
	}

	return NULL;
}


/*
 * RememberObjectStoreCatalogDocument holds a catalog document read from a
 * path.
 */
static void
RememberObjectStoreCatalogDocument(CatalogVersions * versions, const char *path,
								   const char *content)
{
	MemoryContext previousContext = MemoryContextSwitchTo(versions->context);

	ObjectStoreCatalogDocument *document =
		palloc(sizeof(ObjectStoreCatalogDocument));

	document->path = pstrdup(path);
	document->content = pstrdup(content);

	versions->objectStoreCatalogDocuments =
		lappend(versions->objectStoreCatalogDocuments, document);

	MemoryContextSwitchTo(previousContext);
}


/*
 * GetCurrentCatalogVersions returns the versions held for the version scope
 * of the current query, starting to hold them when the query is the first of
 * its scope to ask an external catalog anything.
 */
static CatalogVersions *
GetCurrentCatalogVersions(void)
{
	Assert(IsTransactionState());

	CatalogVersionScope scope = GetCatalogVersionScope();
	ResourceOwner scopeOwner = GetCatalogVersionScopeOwner(scope);

	foreach_ptr(CatalogVersions, versions, HeldCatalogVersions)
	{
		if (versions->scopeOwner == scopeOwner)
			return versions;
	}

	return CreateCatalogVersions(scopeOwner);
}


/*
 * GetCatalogVersionScope returns how long the current transaction sees one
 * version of each external catalog, which is how long it reads the rest of
 * the database from one snapshot.
 */
static CatalogVersionScope
GetCatalogVersionScope(void)
{
	/* one snapshot serves every statement of the transaction */
	if (IsolationUsesXactSnapshot())
		return VERSION_SCOPE_TRANSACTION;

	/* every statement takes a snapshot of its own */
	return VERSION_SCOPE_STATEMENT;
}


/*
 * GetCatalogVersionScopeOwner returns the resource owner whose release ends
 * the given version scope.
 *
 * A transaction ends with TopTransactionResourceOwner.
 *
 * A statement runs in a portal, and Postgres releases the portal's resource
 * owner once the statement is done. The portal is also exactly the extent of
 * the statement: the queries of the functions, DO blocks, procedures, triggers
 * and rule actions the statement runs execute inside its portal, and with the
 * extended protocol the portal spans both the Bind message that starts the
 * statement's scans and the Execute message that runs its functions, which
 * Postgres gives two different statement start times. A cursor is a portal of
 * its own, so it is a statement of its own and keeps its versions for as long
 * as it is open.
 *
 * A statement can also run where no portal has a resource owner for it: in a
 * procedure after the procedure commits, at which point Postgres has released
 * the portal's owner with the transaction, and outside any portal. Such a
 * statement keeps its versions for the rest of its transaction.
 */
static ResourceOwner
GetCatalogVersionScopeOwner(CatalogVersionScope scope)
{
	if (scope == VERSION_SCOPE_STATEMENT &&
		ActivePortal != NULL && ActivePortal->resowner != NULL)
		return ActivePortal->resowner;

	return TopTransactionResourceOwner;
}


/*
 * CreateCatalogVersions starts holding versions for the version scope that
 * releasing the given resource owner ends.
 */
static CatalogVersions *
CreateCatalogVersions(ResourceOwner scopeOwner)
{
	static bool releaseCallbackRegistered = false;

	if (!releaseCallbackRegistered)
	{
		RegisterResourceReleaseCallback(ReleaseCatalogVersions, NULL);
		releaseCallbackRegistered = true;
	}

	if (HeldCatalogVersionsContext == NULL)
	{
		MemoryContext heldContext =
			AllocSetContextCreate(TopTransactionContext,
								  "pg_lake held catalog versions",
								  ALLOCSET_SMALL_SIZES);
		MemoryContextCallback *forgetCallback =
			MemoryContextAlloc(heldContext, sizeof(MemoryContextCallback));

		forgetCallback->func = ForgetHeldCatalogVersions;
		forgetCallback->arg = NULL;

		MemoryContextRegisterResetCallback(heldContext, forgetCallback);

		HeldCatalogVersionsContext = heldContext;
	}

	MemoryContext versionsContext =
		AllocSetContextCreate(HeldCatalogVersionsContext,
							  "pg_lake catalog versions",
							  ALLOCSET_SMALL_SIZES);
	CatalogVersions *versions =
		MemoryContextAllocZero(versionsContext, sizeof(CatalogVersions));

	versions->scopeOwner = scopeOwner;
	versions->context = versionsContext;

	MemoryContext previousContext =
		MemoryContextSwitchTo(HeldCatalogVersionsContext);

	HeldCatalogVersions = lappend(HeldCatalogVersions, versions);

	MemoryContextSwitchTo(previousContext);

	return versions;
}


/*
 * ReleaseCatalogVersions drops the versions of a version scope as Postgres
 * releases the resource owner that ends it. resowner.c makes the owner it
 * releases the current one while it calls back, and calls back for each phase
 * of the release, so the first phase drops the versions and the others find
 * nothing left to drop.
 */
static void
ReleaseCatalogVersions(ResourceReleasePhase phase, bool isCommit,
					   bool isTopLevel, void *arg)
{
	ListCell   *versionsCell = NULL;

	foreach(versionsCell, HeldCatalogVersions)
	{
		CatalogVersions *versions = lfirst(versionsCell);

		if (versions->scopeOwner != CurrentResourceOwner)
			continue;

		HeldCatalogVersions =
			foreach_delete_current(HeldCatalogVersions, versionsCell);

		MemoryContextDelete(versions->context);
	}
}


/*
 * ForgetHeldCatalogVersions forgets the versions of a transaction as its
 * memory goes, by which time releasing its resource owners has dropped them
 * all.
 */
static void
ForgetHeldCatalogVersions(void *arg)
{
	HeldCatalogVersions = NIL;
	HeldCatalogVersionsContext = NULL;
}
