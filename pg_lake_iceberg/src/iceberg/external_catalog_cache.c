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
 * owner of the portal that runs a statement for a statement. A cursor the
 * statement opens reads the statement's version too, and keeps a copy under
 * its own owner, so it goes on reading that version after the statement
 * ends, the way a cursor keeps the snapshot it opened with. Every held
 * answer carries the owner of its scope, and a resource release callback
 * drops the answers of an owner as Postgres releases it, so they go however
 * the scope ends: a statement finishing, a cursor closing, the subtransaction
 * that opened a cursor aborting, or the transaction ending by commit, abort
 * or PREPARE TRANSACTION. A subtransaction that aborts inside a statement
 * leaves the statement's answers in place, since they are still what the
 * statement reads. They are all allocated under TopTransactionContext, so
 * nothing outlives the transaction either way.
 *
 * What is held follows how the catalog publishes:
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
 * - a cursor that a statement opens, such as the one a PL/pgSQL FOR loop over
 *   a query opens, sees its statement's version only when the client runs
 *   that statement in the unnamed portal, which the simple protocol always
 *   does. Under a named portal of the extended protocol, and outside any
 *   portal, it sees the catalog as it stands when it opens;
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

/*
 * The answers external catalogs gave in the current transaction, each held
 * for the version scope it was asked in and found again by that scope and the
 * question: the relation for a REST catalog, the document path for an object
 * store catalog. A scope is named by the resource owner whose release ends
 * it.
 *
 * Under REPEATABLE READ and SERIALIZABLE every answer belongs to the one
 * scope of the transaction. Under READ COMMITTED a cursor is a scope of its
 * own and can outlive the statement that opened it, so answers of several
 * scopes can be held at once, and for one catalog they can be two versions of
 * it:
 *
 *   BEGIN;
 *   DECLARE c CURSOR FOR SELECT * FROM ext;    -- held for the scope of c
 *   SELECT * FROM ext;                         -- held for this statement
 *   FETCH c;                                   -- still the version of c
 *
 * A cursor a running statement opens, such as a PL/pgSQL FOR loop's, holds
 * copies of the answers of that statement, so it reads the same version.
 */
typedef struct HeldRestTableLocation
{
	ResourceOwner scopeOwner;
	Oid			relationId;
	char	   *metadataLocation;
}			HeldRestTableLocation;

typedef struct HeldObjectStoreDocument
{
	ResourceOwner scopeOwner;
	char	   *path;
	char	   *content;
}			HeldObjectStoreDocument;

/* list of HeldRestTableLocation, in HeldAnswersContext */
static List *HeldRestTableLocations = NIL;

/* list of HeldObjectStoreDocument, in HeldAnswersContext */
static List *HeldObjectStoreDocuments = NIL;

static MemoryContext HeldAnswersContext = NULL;

static char *ResolveRestCatalogTable(Oid relationId);
static char *ResolveObjectStoreCatalogTable(Oid relationId);
static char *FindHeldRestTableLocation(ResourceOwner scopeOwner,
									   Oid relationId);
static void HoldRestTableLocation(ResourceOwner scopeOwner, Oid relationId,
								  const char *metadataLocation);
static char *FindHeldObjectStoreDocument(ResourceOwner scopeOwner,
										 const char *path);
static void HoldObjectStoreDocument(ResourceOwner scopeOwner, const char *path,
									const char *content);
static ResourceOwner GetCurrentVersionScopeOwner(ResourceOwner *openingCursorOwner);
static CatalogVersionScope GetCatalogVersionScope(void);
static ResourceOwner GetCatalogVersionScopeOwner(CatalogVersionScope scope,
												 ResourceOwner *openingCursorOwner);
static Portal GetPortalOpeningActiveCursor(void);
static MemoryContext GetHeldAnswersContext(void);
static void ReleaseHeldAnswers(ResourceReleasePhase phase, bool isCommit,
							   bool isTopLevel, void *arg);
static void ForgetHeldAnswers(void *arg);


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
	ResourceOwner openingCursorOwner = NULL;
	ResourceOwner scopeOwner = GetCurrentVersionScopeOwner(&openingCursorOwner);
	char	   *metadataLocation =
		FindHeldRestTableLocation(scopeOwner, relationId);

	if (metadataLocation == NULL)
	{
		metadataLocation =
			GetMetadataLocationForRestCatalogForIcebergTable(relationId);

		HoldRestTableLocation(scopeOwner, relationId, metadataLocation);
	}

	if (openingCursorOwner != NULL &&
		FindHeldRestTableLocation(openingCursorOwner, relationId) == NULL)
		HoldRestTableLocation(openingCursorOwner, relationId, metadataLocation);

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
	ResourceOwner openingCursorOwner = NULL;
	ResourceOwner scopeOwner = GetCurrentVersionScopeOwner(&openingCursorOwner);
	char	   *content = FindHeldObjectStoreDocument(scopeOwner, path);

	if (content == NULL)
	{
		/* catalog.json is a fixed path that may change out of band */
		content = GetJsonFromURINoCache(path);

		/* which version of the catalog a reader got is otherwise unanswerable */
		ereport(DEBUG1,
				(errmsg("read external catalog document \"%s\"", path)));

		HoldObjectStoreDocument(scopeOwner, path, content);
	}

	if (openingCursorOwner != NULL &&
		FindHeldObjectStoreDocument(openingCursorOwner, path) == NULL)
		HoldObjectStoreDocument(openingCursorOwner, path, content);

	return GetMetadataLocationFromExternalObjectStoreCatalogForTable(relationId,
																	 content);
}


/*
 * FindHeldRestTableLocation returns the metadata location held for a
 * relation in the given scope, or NULL if the scope has not asked about it.
 */
static char *
FindHeldRestTableLocation(ResourceOwner scopeOwner, Oid relationId)
{
	foreach_ptr(HeldRestTableLocation, location, HeldRestTableLocations)
	{
		if (location->scopeOwner == scopeOwner &&
			location->relationId == relationId)
			return location->metadataLocation;
	}

	return NULL;
}


/*
 * HoldRestTableLocation holds the metadata location the REST catalog gave
 * for a relation, for the rest of the given scope.
 */
static void
HoldRestTableLocation(ResourceOwner scopeOwner, Oid relationId,
					  const char *metadataLocation)
{
	MemoryContext previousContext =
		MemoryContextSwitchTo(GetHeldAnswersContext());

	HeldRestTableLocation *location = palloc(sizeof(HeldRestTableLocation));

	location->scopeOwner = scopeOwner;
	location->relationId = relationId;
	location->metadataLocation = pstrdup(metadataLocation);

	HeldRestTableLocations = lappend(HeldRestTableLocations, location);

	MemoryContextSwitchTo(previousContext);
}


/*
 * FindHeldObjectStoreDocument returns the content of the catalog document
 * held for a path in the given scope, or NULL if the scope has not read it.
 */
static char *
FindHeldObjectStoreDocument(ResourceOwner scopeOwner, const char *path)
{
	foreach_ptr(HeldObjectStoreDocument, document, HeldObjectStoreDocuments)
	{
		if (document->scopeOwner == scopeOwner &&
			strcmp(document->path, path) == 0)
			return document->content;
	}

	return NULL;
}


/*
 * HoldObjectStoreDocument holds a catalog document read from a path, for the
 * rest of the given scope.
 */
static void
HoldObjectStoreDocument(ResourceOwner scopeOwner, const char *path,
						const char *content)
{
	MemoryContext previousContext =
		MemoryContextSwitchTo(GetHeldAnswersContext());

	HeldObjectStoreDocument *document = palloc(sizeof(HeldObjectStoreDocument));

	document->scopeOwner = scopeOwner;
	document->path = pstrdup(path);
	document->content = pstrdup(content);

	HeldObjectStoreDocuments = lappend(HeldObjectStoreDocuments, document);

	MemoryContextSwitchTo(previousContext);
}


/*
 * GetCurrentVersionScopeOwner returns the resource owner that names the
 * version scope of the current query, and in *openingCursorOwner the owner
 * of a cursor the query is opening inside that scope, or NULL.
 */
static ResourceOwner
GetCurrentVersionScopeOwner(ResourceOwner *openingCursorOwner)
{
	Assert(IsTransactionState());

	return GetCatalogVersionScopeOwner(GetCatalogVersionScope(),
									   openingCursorOwner);
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
 * Postgres gives two different statement start times.
 *
 * A cursor is a portal of its own, which keeps its versions for as long as it
 * is open. A cursor that the client's statement opens reads the statement's
 * versions as it starts its scans, and keeps a copy of each under its own
 * owner, returned in *openingCursorOwner, for what it reads after the
 * statement ends.
 *
 * A statement can also run where no portal has a resource owner for it: in a
 * procedure after the procedure commits, at which point Postgres has released
 * the portal's owner with the transaction, and outside any portal. Such a
 * statement keeps its versions for the rest of its transaction, so a
 * background worker that polls an external table outside a portal has to
 * commit between polls to see the catalog move, the way a procedure does.
 */
static ResourceOwner
GetCatalogVersionScopeOwner(CatalogVersionScope scope,
							ResourceOwner *openingCursorOwner)
{
	*openingCursorOwner = NULL;

	if (scope == VERSION_SCOPE_TRANSACTION)
		return TopTransactionResourceOwner;

	Portal		statementPortal = ActivePortal;
	Portal		openingStatementPortal = GetPortalOpeningActiveCursor();

	if (openingStatementPortal != NULL)
	{
		*openingCursorOwner = ActivePortal->resowner;
		statementPortal = openingStatementPortal;
	}

	if (statementPortal != NULL && statementPortal->resowner != NULL)
		return statementPortal->resowner;

	return TopTransactionResourceOwner;
}


/*
 * GetPortalOpeningActiveCursor returns the portal of the client's statement
 * when the active portal is a cursor that statement is opening, such as the
 * one a PL/pgSQL FOR loop over a query opens, or NULL otherwise.
 *
 * Postgres does not link a portal to the portal that opened it, but a client
 * runs its statement in the unnamed portal, as the simple protocol always
 * does, and so does the extended protocol for a portal the client leaves
 * unnamed.
 */
static Portal
GetPortalOpeningActiveCursor(void)
{
	/* PortalStart starts the scans of a cursor before marking it ready */
	if (ActivePortal == NULL || ActivePortal->status != PORTAL_DEFINED)
		return NULL;

	Portal		clientPortal = GetPortalByName("");

	if (clientPortal == NULL || clientPortal == ActivePortal ||
		clientPortal->status != PORTAL_ACTIVE)
		return NULL;

	return clientPortal;
}


/*
 * GetHeldAnswersContext returns the context the held answers of the current
 * transaction live in, creating it for the first answer of the transaction.
 */
static MemoryContext
GetHeldAnswersContext(void)
{
	static bool releaseCallbackRegistered = false;

	if (!releaseCallbackRegistered)
	{
		RegisterResourceReleaseCallback(ReleaseHeldAnswers, NULL);
		releaseCallbackRegistered = true;
	}

	if (HeldAnswersContext == NULL)
	{
		MemoryContext answersContext =
			AllocSetContextCreate(TopTransactionContext,
								  "pg_lake held external catalog answers",
								  ALLOCSET_SMALL_SIZES);
		MemoryContextCallback *forgetCallback =
			MemoryContextAlloc(answersContext, sizeof(MemoryContextCallback));

		forgetCallback->func = ForgetHeldAnswers;
		forgetCallback->arg = NULL;

		MemoryContextRegisterResetCallback(answersContext, forgetCallback);

		HeldAnswersContext = answersContext;
	}

	return HeldAnswersContext;
}


/*
 * ReleaseHeldAnswers drops the answers held for a version scope as Postgres
 * releases the resource owner that ends it. resowner.c makes the owner it
 * releases the current one while it calls back, and calls back for each phase
 * of the release, so the first phase drops the answers and the others find
 * nothing left to drop.
 */
static void
ReleaseHeldAnswers(ResourceReleasePhase phase, bool isCommit,
				   bool isTopLevel, void *arg)
{
	ListCell   *cell = NULL;

	foreach(cell, HeldRestTableLocations)
	{
		HeldRestTableLocation *location = lfirst(cell);

		if (location->scopeOwner != CurrentResourceOwner)
			continue;

		HeldRestTableLocations =
			foreach_delete_current(HeldRestTableLocations, cell);

		pfree(location->metadataLocation);
		pfree(location);
	}

	foreach(cell, HeldObjectStoreDocuments)
	{
		HeldObjectStoreDocument *document = lfirst(cell);

		if (document->scopeOwner != CurrentResourceOwner)
			continue;

		HeldObjectStoreDocuments =
			foreach_delete_current(HeldObjectStoreDocuments, cell);

		pfree(document->path);
		pfree(document->content);
		pfree(document);
	}
}


/*
 * ForgetHeldAnswers forgets the held answers of a transaction as its memory
 * goes, by which time releasing its resource owners has dropped them all.
 */
static void
ForgetHeldAnswers(void *arg)
{
	HeldRestTableLocations = NIL;
	HeldObjectStoreDocuments = NIL;
	HeldAnswersContext = NULL;
}
