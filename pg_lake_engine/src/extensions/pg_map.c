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

#include "access/htup_details.h"
#include "catalog/pg_proc.h"
#include "pg_lake/extensions/pg_map.h"
#include "pg_extension_base/extension_ids.h"
#include "parser/parse_func.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"

/*
 * PgMapIds contains OIDs from the pg_map extension.
 */
typedef struct PgMapIds
{
	Oid			schemaId;
}			PgMapIds;

static void ClearIds(void *queryEngineIds);

/* cached extension IDs for pg_map */
static PgMapIds CachedIds;

/* top-level identifier of the extension */
CachedExtensionIds *PgMap;

/*
 * Set up extension ID caching for pg_map.
 */
void
InitializePgMapIdCache(void)
{
	PgMap = CreateExtensionIdsCache("pg_map",
									ClearIds, &CachedIds);
}


/*
 * ClearIds clears all the cached OIDs.
 */
static void
ClearIds(void *queryEngineIds)
{
	Assert(queryEngineIds != NULL);

	memset(queryEngineIds, '\0', sizeof(PgMapIds));
}


/*
 * Returns the OID of the types schema.
 */
Oid
PgMapSchemaId(void)
{
	if (CachedIds.schemaId == InvalidOid)
	{
		EnsureExtensionExists(PgMap);

		CachedIds.schemaId = get_namespace_oid(MAP_TYPES_SCHEMA, false);
	}

	return CachedIds.schemaId;
}


/*
 * Returns whether a given function is a variant of pg_map's extract
 * function.
 */
extern PGDLLEXPORT bool
IsMapExtractFunction(Oid objectId)
{
	return get_func_namespace(objectId) == PgMapSchemaId() &&
		strcmp(get_func_name(objectId), "extract") == 0;
}
