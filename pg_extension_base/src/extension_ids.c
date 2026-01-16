/*
 * Copyright 2025 Snowflake Inc.
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
 * Utility functions for caching extension OIDs.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "pg_extension_base/extension_ids.h"
#include "parser/parse_func.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"


/*
 * ExtensionStatus reflects whether an extension is created or dropped.
 */
typedef enum ExtensionStatus
{
	EXTENSION_STATUS_UNKNOWN,
	EXTENSION_STATUS_EXISTS,
	EXTENSION_STATUS_NOT_EXISTS
}			ExtensionStatus;


/*
 * CachedExtensionIdsData tracks top-level metadata about an extension.
 */
struct CachedExtensionIdsData
{
	/* whether the extension currently exists */
	ExtensionStatus existsStatus;

	/* pre-defined name of the extension */
	char		extensionName[NAMEDATALEN];

	/* OID of the extension */
	Oid			extensionId;

	/* OID of the extension schema */
	Oid			schemaId;

	/* OID of the extension owner */
	Oid			ownerId;

	/* function to call when clearing the extension ID cache */
	ClearFunction clearFn;
	void	   *clearContext;

};


/* list of all registered extension caches, allocated in CacheMemoryContext */
static List *registeredExtensionIds = NIL;

static void InitializeExtensionIdCache(void);
static void ExtensionIdAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
								  int subId, void *arg);
static void InvalidateExtensionIdCache(Datum argument, Oid relationId);

static object_access_hook_type PreviousObjectAccessHook = NULL;


/*
 * InitializeExtensionIdCache sets up the object access hook for invalidating
 * the extension ID cache.
 */
static void
InitializeExtensionIdCache(void)
{
	static bool IsInitialized = false;

	if (IsInitialized)
		return;

	PreviousObjectAccessHook = object_access_hook;
	object_access_hook = ExtensionIdAccessHook;

	IsInitialized = true;
}


/*
 * InitializeExtensionIdsCache initialized an extension ID cache.
 */
CachedExtensionIds *
CreateExtensionIdsCache(char *extensionName, ClearFunction clearFn,
						void *clearContext)
{
	if (strlen(extensionName) >= NAMEDATALEN)
		elog(ERROR, "extension name %s is too long", extensionName);

	InitializeExtensionIdCache();
	CreateCacheMemoryContext();

	CachedExtensionIds *extension =
		MemoryContextAllocZero(CacheMemoryContext, sizeof(struct CachedExtensionIdsData));

	/* copy in the extension name */
	strlcpy(extension->extensionName, extensionName, NAMEDATALEN);

	/* remember how to clear additional IDs */
	extension->clearFn = clearFn;
	extension->clearContext = clearContext;

	/*
	 * We currently do not distinguish between extensions IDs, so registering
	 * the cache invalidation can be done only once.
	 *
	 * Instead of Postgres keeping track of the parameters to registered
	 * objects, we do it ourselves. We keep a list of all registered extension
	 * caches and invalidate them all when we receive a pg_extension
	 * invalidation message.
	 *
	 * We do this because Postgres limits the number of relcache invalidation
	 * callbacks to only MAX_RELCACHE_CALLBACKS(10).
	 */
	bool		firstExtension = (registeredExtensionIds == NIL);

	if (firstExtension)
	{
		/* set up invalidation handler to clear cache */
		CacheRegisterRelcacheCallback(InvalidateExtensionIdCache, (Datum) 0);
	}

	MemoryContext oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

	registeredExtensionIds = lappend(registeredExtensionIds, extension);

	MemoryContextSwitchTo(oldcontext);

	return extension;
}


/*
 * ExtensionIdAccessHook checks for CREATE/DROP extension events to
 * invalidate the extension OID cache.
 */
static void
ExtensionIdAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
					  int subId, void *arg)
{
	if (PreviousObjectAccessHook)
		PreviousObjectAccessHook(access, classId, objectId, subId, arg);

	if (classId == ExtensionRelationId)
		CacheInvalidateRelcacheByRelid(ExtensionRelationId);
}



/*
 * InvalidateExtensionIdCache invalidates the extension cache in the current backend
 * in response to a pg_extension invalidation message.
 *
 * We currently do not distinguish between extensions IDs. Presumably these events are
 * rare enough to not care a whole lot about the cost of rebuilding.
 */
static void
InvalidateExtensionIdCache(Datum argument, Oid relationId)
{
	if (relationId == ExtensionRelationId || relationId == 0)
	{
		ListCell   *lc;

		foreach(lc, registeredExtensionIds)
		{
			CachedExtensionIds *extension = (CachedExtensionIds *) lfirst(lc);

			extension->existsStatus = EXTENSION_STATUS_UNKNOWN;
			extension->extensionId = InvalidOid;
			extension->schemaId = InvalidOid;

			if (extension->clearFn != NULL)
				extension->clearFn(extension->clearContext);
		}
	}
}


/*
 * IsExtensionCreated returns whether exists.
 */
bool
IsExtensionCreated(CachedExtensionIds * extension)
{
	if (extension->existsStatus == EXTENSION_STATUS_EXISTS)
		return true;
	if (extension->existsStatus == EXTENSION_STATUS_NOT_EXISTS)
		return false;

	bool		missingOk = true;
	Oid			extensionId = get_extension_oid(extension->extensionName, missingOk);

	if (extensionId != InvalidOid)
	{
		extension->existsStatus = EXTENSION_STATUS_EXISTS;
		return true;
	}
	else
	{
		extension->existsStatus = EXTENSION_STATUS_NOT_EXISTS;
		return false;
	}
}


/*
 * ExtensionId returns the OID of the extension.
 */
Oid
ExtensionId(CachedExtensionIds * extension)
{
	if (extension->extensionId == InvalidOid)
	{
		extension->extensionId = get_extension_oid(extension->extensionName, false);
	}

	return extension->extensionId;
}


/*
 * ExtensionSchemaId returns the OID of the extension schema.
 */
Oid
ExtensionSchemaId(CachedExtensionIds * extension)
{
	if (extension->schemaId == InvalidOid)
		extension->schemaId = get_extension_schema(ExtensionId(extension));

	return extension->schemaId;
}


/*
 * ExtensionOwnerId returns the OID of the extension owner.
 */
Oid
ExtensionOwnerId(CachedExtensionIds * extension)
{
	if (extension->ownerId == InvalidOid)
	{
		Oid			extensionId = ExtensionId(extension);
		Relation	pgExtension = table_open(ExtensionRelationId, AccessShareLock);

		ScanKeyData key[1];

		ScanKeyInit(&key[0], Anum_pg_extension_oid, BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(extensionId));

		SysScanDesc extensionScan = systable_beginscan(pgExtension, ExtensionOidIndexId, true,
													   NULL, 1, key);

		HeapTuple	extensionTuple = systable_getnext(extensionScan);

		if (!HeapTupleIsValid(extensionTuple))
			elog(ERROR, "could not find tuple for extension %u", extensionId);

		Form_pg_extension extensionForm = (Form_pg_extension) GETSTRUCT(extensionTuple);

		extension->ownerId = extensionForm->extowner;

		systable_endscan(extensionScan);
		table_close(pgExtension, AccessShareLock);
	}

	return extension->ownerId;
}


/*
 * EnsureExtensionExists throws an error if the given extension does not exist
 * in the current database.
 */
void
EnsureExtensionExists(CachedExtensionIds * extension)
{
	if (IsExtensionCreated(extension))
		return;

	ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					errmsg("extension %s is not created",
						   extension->extensionName),
					errhint("Your administrator may need to run "
							"CREATE EXTENSION %s CASCADE",
							extension->extensionName)));
}
