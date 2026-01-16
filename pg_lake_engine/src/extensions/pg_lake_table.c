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
 * Utility functions for pg_lake_table extension OIDs.
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_extension_base/extension_ids.h"
#include "nodes/pg_list.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


/*
 * PgLakeTableIds contains OIDs from the pg_lake_table extension.
 */
typedef struct PgLakeTableIds
{
	/*
	 * lake_iceberg.table_size function ID (created by pg_lake_table)
	 */
	Oid			tableSizeFunctionId;

	/*
	 * __lake__internal__nsp__.is_object_created_by_lake function ID (created
	 * by any pg_lake extension)
	 */
	Oid			isObjectCreatedByLakeFuncId;

	/*
	 * __pg_lake_table_writes schema ID
	 */
	Oid			pgLakeWritesSchemaId;
}			PgLakeTableIds;

static void ClearIds(void *lakeTableIds);

/* cached extension IDs for pg_lake_table */
static PgLakeTableIds CachedIds;


/*
 * Generic extension state.
 */
CachedExtensionIds *PgLakeTable;

/*
 * Set up extension ID caching for pg_lake_table.
 */
void
InitializePgLakeTableIdCache(void)
{
	PgLakeTable = CreateExtensionIdsCache(PG_LAKE_TABLE,
										  ClearIds, &CachedIds);
}


/*
 * ClearIds clears all the cached OIDs.
 */
static void
ClearIds(void *lakeTableIds)
{
	Assert(lakeTableIds != NULL);

	memset(lakeTableIds, '\0', sizeof(PgLakeTableIds));
}


/*
 * PgLakeTableSizeFunctionId returns the OID of the lake_iceberg.table_size
 * function, or InvalidOid if it does not exist.
 */
Oid
PgLakeTableSizeFunctionId(void)
{
	if (CachedIds.tableSizeFunctionId == InvalidOid)
	{
		EnsureExtensionExists(PgLakeTable);

		Oid			funcParamOid[1] = {REGCLASSOID};

		List	   *functionName = list_make2(makeString("lake_iceberg"),
											  makeString("table_size"));

		bool		missingOk = true;

		CachedIds.tableSizeFunctionId =
			LookupFuncName(functionName, 1, funcParamOid, missingOk);
	}

	return CachedIds.tableSizeFunctionId;
}


/*
 * IsObjectCreatedByPgLakeFuncId returns the OID of
 * the function __lake__internal__nsp__.is_object_created_by_lake
 * or InvalidOid if it does not exist.
 */
Oid
IsObjectCreatedByPgLakeFuncId(void)
{
	if (CachedIds.isObjectCreatedByLakeFuncId == InvalidOid)
	{
		EnsureExtensionExists(PgLakeTable);

		Oid			funcParamOid[2] = {REGCLASSOID, OIDOID};

		List	   *functionName = list_make2(makeString("__lake__internal__nsp__"),
											  makeString("is_object_created_by_lake"));

		bool		missingOk = true;

		CachedIds.isObjectCreatedByLakeFuncId =
			LookupFuncName(functionName, 2, funcParamOid, missingOk);
	}

	return CachedIds.isObjectCreatedByLakeFuncId;
}


/*
 * PgLakeWritesSchemaId returns the OID of the __pg_lake_table_writes schema.
 */
Oid
PgLakeWritesSchemaId(void)
{
	if (CachedIds.pgLakeWritesSchemaId == InvalidOid)
	{
		EnsureExtensionExists(PgLakeTable);

		bool		missingOk = true;

		CachedIds.pgLakeWritesSchemaId =
			get_namespace_oid(REPLICATION_WRITES_SCHEMA, missingOk);
	}

	return CachedIds.pgLakeWritesSchemaId;
}
