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
 * external catalog. Asking that catalog more than once in one read lets the
 * read mix versions: two references to one table, or two tables of one
 * object store catalog, can come back from different points in time.
 *
 * So the first answer is held for the rest of the portal that asked, or for
 * the rest of the transaction under REPEATABLE READ and SERIALIZABLE, the
 * same span over which Postgres reuses a snapshot for the top-level query.
 * A portal covers a statement across Bind and Execute, and everything that
 * runs inside it. A cursor, including the one a PL/pgSQL FOR loop opens, is
 * a portal of its own and asks again when it opens, like a READ COMMITTED
 * query in a volatile function takes a new snapshot.
 *
 * A REST catalog answers per table, so we hold the metadata location of each
 * relation asked about. An object store catalog publishes all of its tables
 * as one document, so we hold the document, which keeps its tables
 * consistent with each other.
 *
 * Held answers live in TopTransactionContext and are dropped when Postgres
 * releases the resource owner they were held for.
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


/*
 * An answer an external catalog gave, held until scopeOwner is released.
 * The key is "rest:<relation oid>" for a REST table location, or the path of
 * an object store catalog document.
 */
typedef struct HeldAnswer
{
	ResourceOwner scopeOwner;
	char	   *key;
	char	   *value;
}			HeldAnswer;

/* list of HeldAnswer, in HeldAnswersContext */
static List *HeldAnswers = NIL;

static MemoryContext HeldAnswersContext = NULL;

static char *ResolveRestCatalogTable(Oid relationId);
static char *ResolveObjectStoreCatalogTable(Oid relationId);
static char *FindHeldAnswer(ResourceOwner scopeOwner, const char *key);
static void HoldAnswer(ResourceOwner scopeOwner, const char *key,
					   const char *value);
static ResourceOwner GetVersionScopeOwner(void);
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
		/* the location is a table option, which the relation lock keeps still */
		return GetForeignTablePath(relationId);
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unsupported iceberg external table type for relation %s",
					get_rel_name(relationId))));
}


/*
 * ResolveRestCatalogTable returns the metadata location the REST catalog
 * gives for a read-only table, asking at most once per version scope.
 */
static char *
ResolveRestCatalogTable(Oid relationId)
{
	ResourceOwner scopeOwner = GetVersionScopeOwner();
	char	   *key = psprintf("rest:%u", relationId);
	char	   *metadataLocation = FindHeldAnswer(scopeOwner, key);

	if (metadataLocation == NULL)
	{
		metadataLocation =
			GetMetadataLocationForRestCatalogForIcebergTable(relationId);

		HoldAnswer(scopeOwner, key, metadataLocation);
	}

	/* the scope can end before whatever the caller keeps this in */
	return pstrdup(metadataLocation);
}


/*
 * ResolveObjectStoreCatalogTable returns the metadata location the catalog
 * document of a read-only object_store table gives for it, reading the
 * document at most once per version scope.
 */
static char *
ResolveObjectStoreCatalogTable(Oid relationId)
{
	char	   *path = GetExternalObjectStoreCatalogFilePathForTable(relationId);
	ResourceOwner scopeOwner = GetVersionScopeOwner();
	char	   *content = FindHeldAnswer(scopeOwner, path);

	if (content == NULL)
	{
		/* catalog.json is a fixed path that may change out of band */
		content = GetJsonFromURINoCache(path);

		/* lets tests count how many versions a read could have seen */
		ereport(DEBUG1,
				(errmsg("read external catalog document \"%s\"", path)));

		HoldAnswer(scopeOwner, path, content);
	}

	return GetMetadataLocationFromExternalObjectStoreCatalogForTable(relationId,
																	 content);
}


/*
 * FindHeldAnswer returns the answer held for a key in the given scope, or
 * NULL if there is none.
 */
static char *
FindHeldAnswer(ResourceOwner scopeOwner, const char *key)
{
	foreach_ptr(HeldAnswer, answer, HeldAnswers)
	{
		if (answer->scopeOwner == scopeOwner && strcmp(answer->key, key) == 0)
			return answer->value;
	}

	return NULL;
}


/*
 * HoldAnswer holds an answer for a key until scopeOwner is released.
 */
static void
HoldAnswer(ResourceOwner scopeOwner, const char *key, const char *value)
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

	MemoryContext previousContext = MemoryContextSwitchTo(HeldAnswersContext);

	HeldAnswer *answer = palloc(sizeof(HeldAnswer));

	answer->scopeOwner = scopeOwner;
	answer->key = pstrdup(key);
	answer->value = pstrdup(value);

	HeldAnswers = lappend(HeldAnswers, answer);

	MemoryContextSwitchTo(previousContext);
}


/*
 * GetVersionScopeOwner returns the resource owner whose release ends the
 * current version scope: the transaction's when it reads from one snapshot,
 * otherwise the active portal's.
 *
 * With no active portal owner, as after a procedure commits or in a
 * background worker, answers are held for the rest of the transaction, so a
 * caller that polls has to commit between polls.
 */
static ResourceOwner
GetVersionScopeOwner(void)
{
	Assert(IsTransactionState());

	if (!IsolationUsesXactSnapshot() &&
		ActivePortal != NULL && ActivePortal->resowner != NULL)
		return ActivePortal->resowner;

	return TopTransactionResourceOwner;
}


/*
 * ReleaseHeldAnswers drops the answers held for the resource owner being
 * released, which resowner.c makes CurrentResourceOwner while it calls back.
 * It is called once per release phase; only the first finds anything.
 */
static void
ReleaseHeldAnswers(ResourceReleasePhase phase, bool isCommit,
				   bool isTopLevel, void *arg)
{
	ListCell   *cell = NULL;

	foreach(cell, HeldAnswers)
	{
		HeldAnswer *answer = lfirst(cell);

		if (answer->scopeOwner != CurrentResourceOwner)
			continue;

		HeldAnswers = foreach_delete_current(HeldAnswers, cell);

		pfree(answer->key);
		pfree(answer->value);
		pfree(answer);
	}
}


/*
 * ForgetHeldAnswers resets our pointers when TopTransactionContext goes, by
 * which point releasing the resource owners has dropped every answer.
 */
static void
ForgetHeldAnswers(void *arg)
{
	HeldAnswers = NIL;
	HeldAnswersContext = NULL;
}
