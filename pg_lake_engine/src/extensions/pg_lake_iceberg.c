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
 * Utility functions for pg_lake_iceberg extension OIDs.
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/namespace.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_extension_base/extension_ids.h"


/*
 * PgLakeIcebergIds contains OIDs from the pg_lake_iceberg extension.
 */
typedef struct PgLakeIcebergIds
{
	/*
	 * pg_lake_iceberg.tables_internal relation OID
	 */
	Oid			tablesInternalTableId;
}			PgLakeIcebergIds;

static void ClearIds(void *pgLakeIcebergIds);

/* cached extension IDs for pg_lake_iceberg */
static PgLakeIcebergIds CachedIds;

/*
 * Generic extension state.
 */
CachedExtensionIds *PgLakeIceberg;

/*
 * Set up extension ID caching for pg_lake_iceberg.
 */
void
InitializePgLakeIcebergIdCache(void)
{
	PgLakeIceberg = CreateExtensionIdsCache("pg_lake_iceberg",
											ClearIds, &CachedIds);
}



/*
 * ClearIds clears all the cached OIDs.
 */
static void
ClearIds(void *ids)
{
	Assert(ids != NULL);

	memset(ids, '\0', sizeof(PgLakeIcebergIds));
}


/*
 * IcebergTablesInternalTableId returns the OID of the tables_internal
 * metadata table.
 */
Oid
IcebergTablesInternalTableId(void)
{
	if (CachedIds.tablesInternalTableId == InvalidOid)
	{
		bool		missingOk = true;
		Oid			namespaceId = get_namespace_oid(PG_LAKE_ICEBERG_SCHEMA, missingOk);

		if (namespaceId != InvalidOid)
		{
			CachedIds.tablesInternalTableId =
				get_relname_relid(ICEBERG_INTERNAL_CATALOG_TABLE_NAME, namespaceId);
		}
		else
		{
			CachedIds.tablesInternalTableId = InvalidOid;
		}
	}

	return CachedIds.tablesInternalTableId;
}
