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
 * Utility functions for PostGIS extension OIDs.
 */
#include "postgres.h"

#include "catalog/objectaccess.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_type.h"
#include "commands/extension.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/extensions/postgis.h"
#include "parser/parse_func.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"


/*
 * Cached extension OIDS for Postgis.
 */
typedef struct PostgisExtensionIds
{
	/* geometry type */
	Oid			geometryTypeId;

	/* geography type */
	Oid			geographyTypeId;

	/* geometry[] type */
	Oid			geometryArrayTypeId;

	/* geometry_out function */
	Oid			geometryOutFunctionId;

	/* geography cast function */
	Oid			geographyFunctionId;

	/* ST_AsBinary function */
	Oid			stAsBinaryFunctionId;

	/* ST_AsText function */
	Oid			stAsTextFunctionId;

	/* ST_FlipCoordinates(geometry,geometry) function */
	Oid			stFlipCoordinatesFunctionId;

	/* ST_GeomFromText function */
	Oid			stGeomFromTextFunctionId;

	/* ST_Union aggregate function */
	Oid			stUnionAggregateId;

	/* ST_Union_Agg aggregate function */
	Oid			stUnionAggAggregateId;

}			PostgisExtensionIds;


static void ClearIds(void *postgisIds);

/*
 * Generic extension state.
 */
CachedExtensionIds *Postgis;

/*
 * postgis-specific OIDs.
 */
static PostgisExtensionIds CachedIds;


/*
 * Set up extension ID caching for pg_lake_spatial.
 */
void
InitializePostgisIdCache(void)
{
	Postgis = CreateExtensionIdsCache("postgis", ClearIds, &CachedIds);
}


/*
 * ClearIds clears all the cached OIDs.
 */
static void
ClearIds(void *postgisIds)
{
	Assert(postgisIds != NULL);
	memset(postgisIds, '\0', sizeof(CachedIds));
}


/*
 * IsGeometryType determines whether a given PGType is assigned to the
 * PostGIS geometry type.
 */
bool
IsGeometryType(PGType postgresType)
{
	return IsGeometryTypeId(postgresType.postgresTypeOid);
}


/*
 * IsGeometryTypeId determines whether a given type OID is assigned to the
 * PostGIS geometry type.
 */
bool
IsGeometryTypeId(Oid typeId)
{
	if (!IsExtensionCreated(Postgis))
		return false;

	return typeId == GeometryTypeId();
}



/*
 * IsGeographyTypeId determines whether a given type OID is assigned to the
 * PostGIS geometry type.
 */
bool
IsGeographyTypeId(Oid typeId)
{
	if (!IsExtensionCreated(Postgis))
		return false;

	return typeId == GeographyTypeId();
}


/*
 * GeometryTypeId returns the OID of the geometry type.
 */
Oid
GeometryTypeId(void)
{
	if (CachedIds.geometryTypeId == InvalidOid)
	{
		CachedIds.geometryTypeId =
			GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("geometry"),
							ObjectIdGetDatum(ExtensionSchemaId(Postgis)));
	}
	return CachedIds.geometryTypeId;
}


/*
 * GeographyTypeId returns the OID of the geography type.
 */
Oid
GeographyTypeId(void)
{
	if (CachedIds.geographyTypeId == InvalidOid)
	{
		if (!IsExtensionCreated(Postgis))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot use geography type when postgis extension "
								   "is not enabled")));

		CachedIds.geographyTypeId =
			GetSysCacheOid2(TYPENAMENSP, Anum_pg_type_oid, CStringGetDatum("geography"),
							ObjectIdGetDatum(ExtensionSchemaId(Postgis)));
	}

	return CachedIds.geographyTypeId;
}



/*
 * GeometryArrayTypeId returns the OID of the geometry array type.
 */
Oid
GeometryArrayTypeId(void)
{
	if (CachedIds.geometryArrayTypeId == InvalidOid)
	{
		/* error checks happen in GeometryTypeId */
		CachedIds.geometryArrayTypeId = get_array_type(GeometryTypeId());
	}

	return CachedIds.geometryArrayTypeId;
}


/*
 * IsGeometryOutFunctionId returns whether the given function ID belongs
 * the geometry_out function.
 */
bool
IsGeometryOutFunctionId(Oid functionId)
{
	if (!IsExtensionCreated(Postgis))
		return false;

	return functionId == GeometryOutFunctionId();
}


/*
 * GeometryOutFunctionId returns the OID of the geometry_out function.
 */
Oid
GeometryOutFunctionId(void)
{
	if (CachedIds.geometryOutFunctionId == InvalidOid)
	{
		/* error checks happen in GeometryTypeId */
		Oid			geometryTypeId = GeometryTypeId();
		bool		isVarlena = false;

		getTypeOutputInfo(geometryTypeId, &CachedIds.geometryOutFunctionId,
						  &isVarlena);
	}

	return CachedIds.geometryOutFunctionId;
}


/*
 * GeographyFunctionId returns the OID of the geography function.
 */
Oid
GeographyFunctionId(void)
{
	if (CachedIds.geographyFunctionId == InvalidOid)
	{
		/* error checks happen in ExtensionSchemaId(Postgis) */
		char	   *schemaName = get_namespace_name(ExtensionSchemaId(Postgis));

		List	   *functionName = list_make2(makeString(schemaName),
											  makeString("geography"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.geographyFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.geographyFunctionId;
}


/*
 * ST_AsBinaryFunctionId returns the OID of the ST_AsBinary function.
 */
Oid
ST_AsBinaryFunctionId(void)
{
	if (CachedIds.stAsBinaryFunctionId == InvalidOid)
	{
		/* error checks happen in ExtensionSchemaId(Postgis) */
		char	   *schemaName = get_namespace_name(ExtensionSchemaId(Postgis));

		List	   *functionName = list_make2(makeString(schemaName),
											  makeString("st_asbinary"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.stAsBinaryFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.stAsBinaryFunctionId;
}


/*
 * ST_AsTextFunctionId returns the OID of the ST_AsText function.
 */
Oid
ST_AsTextFunctionId(void)
{
	if (CachedIds.stAsTextFunctionId == InvalidOid)
	{
		/* error checks happen in ExtensionSchemaId(Postgis) */
		char	   *schemaName = get_namespace_name(ExtensionSchemaId(Postgis));

		List	   *functionName = list_make2(makeString(schemaName),
											  makeString("st_astext"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.stAsTextFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.stAsTextFunctionId;
}


/*
 * ST_FlipCoordinatesFunctionId returns the OID of the ST_FlipCoordinates(geometry,geometry)
 * function.
 */
Oid
ST_FlipCoordinatesFunctionId(void)
{
	if (CachedIds.stFlipCoordinatesFunctionId == InvalidOid)
	{
		/* error checks happen in ExtensionSchemaId(Postgis) */
		char	   *schemaName = get_namespace_name(ExtensionSchemaId(Postgis));

		List	   *functionName = list_make2(makeString(schemaName),
											  makeString("st_flipcoordinates"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.stFlipCoordinatesFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.stFlipCoordinatesFunctionId;
}


/*
 * ST_GeomFromTextFunctionId returns the OID of the ST_GeomFromText function.
 */
Oid
ST_GeomFromTextFunctionId(void)
{
	if (CachedIds.stGeomFromTextFunctionId == InvalidOid)
	{
		/* error checks happen in ExtensionSchemaId(Postgis) */
		char	   *schemaName = get_namespace_name(ExtensionSchemaId(Postgis));

		List	   *functionName = list_make2(makeString(schemaName),
											  makeString("st_geomfromtext"));
		Oid			argTypes[] = {TEXTOID};
		int			argCount = 1;

		CachedIds.stGeomFromTextFunctionId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.stGeomFromTextFunctionId;
}


/*
 * ST_AsTextFunctionId returns the OID of the ST_AsText function.
 */
Oid
ST_UnionAggregateId(void)
{
	if (CachedIds.stUnionAggregateId == InvalidOid)
	{
		/* error checks happen in ExtensionSchemaId(Postgis) */
		char	   *schemaName = get_namespace_name(ExtensionSchemaId(Postgis));

		List	   *functionName = list_make2(makeString(schemaName),
											  makeString("st_union"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.stUnionAggregateId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.stUnionAggregateId;
}


/*
 * ST_Union_AggAggregateId returns the OID of the internal ST_Union_Agg
 * function.  While this function technically lives in the pg_lake internal
 * namespace, keep lookup here with other postgis-related things.
 */
Oid
ST_Union_AggAggregateId(void)
{
	if (CachedIds.stUnionAggAggregateId == InvalidOid)
	{
		/* error checks happen in ExtensionSchemaId(Postgis) */
		List	   *functionName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											  makeString("st_union_agg"));
		Oid			argTypes[] = {GeometryTypeId()};
		int			argCount = 1;

		CachedIds.stUnionAggAggregateId =
			LookupFuncName(functionName, argCount, argTypes, false);
	}

	return CachedIds.stUnionAggAggregateId;
}
