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
 * Utility functions for pg_lake_spatial extension OIDs.
 */
#include "postgres.h"

#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_extension_base/extension_ids.h"
#include "parser/parse_func.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * Cached extension OIDS for pg_lake_spatial.
 */
typedef struct PgLakeSpatialExtensionIds
{
	/* internal ST_AsWKB function */
	Oid			internalAsWKBFunctionId;

	/* internal ST_Transform(geometry,text,text,bool) function */
	Oid			internalTransformFunctionId;

	/* internal ST_Intersects_Extent(geometry,geometry) function */
	Oid			internalIntersectsExtentFunctionId;

	/* internal ST_GeometryType(geometry) function */
	Oid			internalGeometryTypeFunctionId;
}			PgLakeSpatialExtensionIds;

/*
 * Generic extension state.
 */
CachedExtensionIds *PgLakeSpatial;

/*
 * pg_lake_spatial-specific OIDs.
 */
static PgLakeSpatialExtensionIds CachedIds;

static void ClearIds(void *spatialAnalyticsIds);


/*
 * Set up extension ID caching for pg_lake_spatial.
 */
void
InitializePgLakeSpatialIdCache(void)
{
	PgLakeSpatial = CreateExtensionIdsCache("pg_lake_spatial",
											ClearIds, &CachedIds);
}


/*
 * ClearIds clears all the cached OIDs.
 */
static void
ClearIds(void *spatialAnalyticsIds)
{
	Assert(spatialAnalyticsIds != NULL);
	memset(spatialAnalyticsIds, '\0', sizeof(PgLakeSpatialExtensionIds));
}


/*
 * InternalAsWKBFunctionId returns the internal ST_AsWKB function ID.
 */
Oid
InternalAsWKBFunctionId(void)
{
	if (CachedIds.internalAsWKBFunctionId == InvalidOid)
	{
		ErrorIfPgLakeSpatialNotEnabled();

		List	   *functionName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											  makeString("st_aswkb"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.internalAsWKBFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.internalAsWKBFunctionId;
}


/*
 * InternalTransformFunctionId returns the internal ST_Transform function ID.
 */
Oid
InternalTransformFunctionId(void)
{
	if (CachedIds.internalTransformFunctionId == InvalidOid)
	{
		ErrorIfPgLakeSpatialNotEnabled();

		List	   *functionName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											  makeString("st_transform"));
		Oid			argTypes[] = {GeometryTypeId(), TEXTOID, TEXTOID, BOOLOID};
		int			argCount = 4;

		CachedIds.internalTransformFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.internalTransformFunctionId;
}


/*
 * InternalIntersectsExtentFunctionId returns the internal ST_Intersects_Extent function ID.
 */
Oid
InternalIntersectsExtentFunctionId(void)
{
	if (CachedIds.internalIntersectsExtentFunctionId == InvalidOid)
	{
		ErrorIfPgLakeSpatialNotEnabled();

		List	   *functionName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											  makeString("st_intersects_extent"));
		Oid			argTypes[] = {GeometryTypeId(), GeometryTypeId()};
		int			argCount = 2;

		CachedIds.internalIntersectsExtentFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.internalIntersectsExtentFunctionId;
}


/*
 * InternalGeometryTypeFunctionId returns the internal ST_GeometryType function ID.
 */
Oid
InternalGeometryTypeFunctionId(void)
{
	if (CachedIds.internalGeometryTypeFunctionId == InvalidOid)
	{
		ErrorIfPgLakeSpatialNotEnabled();

		List	   *functionName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											  makeString("st_geometrytype"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.internalGeometryTypeFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.internalGeometryTypeFunctionId;
}


/*
 * ErrorIfPgLakeSpatialNotEnabled throws an error if pg_lake_spatial
 * is not enabled.
 */
void
ErrorIfPgLakeSpatialNotEnabled(void)
{
	if (!IsExtensionCreated(PgLakeSpatial))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_spatial extension is not enabled"),
						errmsg("The pg_lake_spatial extension is required to "
							   "use PostGIS with pg_lake tables."),
						errhint("Run CREATE EXTENSION pg_lake_spatial CASCADE "
								"to enable the extension.")));
}
