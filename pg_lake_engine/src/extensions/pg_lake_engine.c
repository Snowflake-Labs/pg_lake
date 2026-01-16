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

#include "postgres.h"

#include "catalog/pg_type.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_extension_base/extension_ids.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * PgLakeEngineIds contains OIDs from the pg_lake_engine extension.
 */
typedef struct PgLakeEngineIds
{
	/* read table placeholder function */
	Oid			readTableFunctionId;

	Oid			inProgressTableId;

	Oid			inProgressTablePkeyId;
}			PgLakeEngineIds;

static void ClearIds(void *queryEngineIds);

/* cached extension IDs for pg_lake_engine */
static PgLakeEngineIds CachedIds;

/* top-level identifier of the extension */
CachedExtensionIds *PgLakeEngine;

/*
 * Set up extension ID caching for pg_lake_engine.
 */
void
InitializePgLakeEngineIdCache(void)
{
	PgLakeEngine = CreateExtensionIdsCache("pg_lake_engine",
										   ClearIds, &CachedIds);
}


/*
 * ClearIds clears all the cached OIDs.
 */
static void
ClearIds(void *queryEngineIds)
{
	Assert(queryEngineIds != NULL);

	memset(queryEngineIds, '\0', sizeof(PgLakeEngineIds));
}


/*
 * InProgressTableId returns the OID of the IN_PROGRESS_FILES_TABLE.
 */
Oid
InProgressTableId(void)
{
	if (CachedIds.inProgressTableId == InvalidOid)
	{
		EnsureExtensionExists(PgLakeEngine);

		Oid			namespaceId = get_namespace_oid(PG_LAKE_ENGINE_NSP, false);

		CachedIds.inProgressTableId = get_relname_relid(IN_PROGRESS_FILES_TABLE, namespaceId);
	}

	return CachedIds.inProgressTableId;
}


/*
* InProgressTablePkeyId returns the OID of the primary key of the IN_PROGRESS_FILES_TABLE.
*/
Oid
InProgressTablePkeyId(void)
{
	if (CachedIds.inProgressTablePkeyId == InvalidOid)
	{
		EnsureExtensionExists(PgLakeEngine);

		Oid			namespaceId = get_namespace_oid(PG_LAKE_ENGINE_NSP, false);

		CachedIds.inProgressTablePkeyId = get_relname_relid(IN_PROGRESS_FILES_TABLE "_pkey", namespaceId);
	}

	return CachedIds.inProgressTablePkeyId;
}


/*
 * ReadTableFunctionId returns the OID of the placeholder function for reading
 * from tables.
 */
Oid
ReadTableFunctionId(void)
{
	if (CachedIds.readTableFunctionId == InvalidOid)
	{
		EnsureExtensionExists(PgLakeEngine);

		Oid			funcParamOid[2] = {TEXTOID, INT4OID};

		List	   *functionName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											  makeString(PG_LAKE_READ_TABLE));

		CachedIds.readTableFunctionId =
			LookupFuncName(functionName, 2, funcParamOid, false);
	}

	return CachedIds.readTableFunctionId;
}
