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
 * Determine which spatial functions are shippable to pgduck_server
 */

#include "postgres.h"

#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

#include "pg_lake/parsetree/const.h"
#include "pg_lake/pgduck/shippable_spatial_functions.h"
#include "pg_lake/extensions/postgis.h"


static bool IsStAsBinaryWithMyEndian(Node *node);
static bool IsStAsGeoJSONWithDefault(Node *node);
static bool IsComparisonWithDefaultGridSize(Node *node);
static bool IsLastArgConst(Node *node);
static bool IsPointDistanceSpheroid(Node *node);
static bool IsPointGeometry(Node *node);


static const PGDuckShippableFunction ShippableSpatialProcs[] =
{
	/*
	 * The following functions have direct duckdb_spatial equivalents.  Note
	 * that type names currently need to be both valid DuckDB types (or
	 * aliases) and postgres types (or aliases), so "double" becomes "float8",
	 * "integer" becomes "int4", etc.
	 *
	 * If you add functions here, you should add validation of the pushdown
	 * signatures to the test_spatial_function_pushdown file.
	 */

	{POSTGIS_SCHEMA "st_area", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_area", 'f', 2, {POSTGIS_SCHEMA "geography", "bool"}, NULL},
	{POSTGIS_SCHEMA "st_asgeojson", 'f', 3, {POSTGIS_SCHEMA "geometry", "int4", "int4"}, IsStAsGeoJSONWithDefault},
	{POSTGIS_SCHEMA "st_astext", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_boundary", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_buffer", 'f', 3, {POSTGIS_SCHEMA "geometry", "float8", "int4"}, NULL},
	{POSTGIS_SCHEMA "st_centroid", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_collect", 'f', 1, {POSTGIS_SCHEMA "_geometry"}, NULL},
	{POSTGIS_SCHEMA "st_collectionextract", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_collectionextract", 'f', 2, {POSTGIS_SCHEMA "geometry", "int4"}, NULL},
	{POSTGIS_SCHEMA "st_contains", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_containsproperly", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_convexhull", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_coveredby", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_covers", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_crosses", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_difference", 'f', 3, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry", "float8"}, IsComparisonWithDefaultGridSize},
	{POSTGIS_SCHEMA "st_dimension", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_disjoint", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_distance", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_distance", 'f', 3, {POSTGIS_SCHEMA "geography", POSTGIS_SCHEMA "geography", "bool"}, IsPointDistanceSpheroid},
	{POSTGIS_SCHEMA "st_dwithin", 'f', 3, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry", "float8"}, NULL},
	{POSTGIS_SCHEMA "st_endpoint", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_envelope", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_equals", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	/* st_extent() is not an aggregate function in duckdb, so not the same */

	/*
	 * {POSTGIS_SCHEMA "st_extent", 'f', 1, {POSTGIS_SCHEMA "geometry"},
	 * NULL},
	 */
	{POSTGIS_SCHEMA "st_exteriorring", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_flipcoordinates", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_force2d", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},

	/*
	 * The 2025-02-24 release broke the 2-argument signature for these:
	 *
	 * {POSTGIS_SCHEMA "st_force3dm", 'f', 2, {POSTGIS_SCHEMA "geometry",
	 * "float8"}, NULL}, {POSTGIS_SCHEMA "st_force3dz", 'f', 2,
	 * {POSTGIS_SCHEMA "geometry", "float8"}, NULL}, {POSTGIS_SCHEMA
	 * "st_force4d", 'f', 3, {POSTGIS_SCHEMA "geometry", "float8", "float8"},
	 * NULL},
	 */
	/* st_geometrytype return value is not supported for return */

	/*
	 * {POSTGIS_SCHEMA "st_geometrytype", 'f', 1, {POSTGIS_SCHEMA "geometry"},
	 * NULL},
	 */
	{POSTGIS_SCHEMA "st_geomfromgeojson", 'f', 1, {"json"}, NULL},
	{POSTGIS_SCHEMA "st_geomfromgeojson", 'f', 1, {"text"}, NULL},
	{POSTGIS_SCHEMA "st_geomfromtext", 'f', 1, {"text"}, NULL},
	{POSTGIS_SCHEMA "st_geometryfromtext", 'f', 1, {"text"}, NULL},
	{POSTGIS_SCHEMA "st_geomfromwkb", 'f', 1, {"bytea"}, NULL},
	{POSTGIS_SCHEMA "st_intersection", 'f', 3, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry", "float8"}, IsComparisonWithDefaultGridSize},
	{POSTGIS_SCHEMA "st_intersects", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_isclosed", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_isempty", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_isring", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_issimple", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_isvalid", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_length", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_length", 'f', 2, {POSTGIS_SCHEMA "geography", "bool"}, IsLastArgConst},
	{POSTGIS_SCHEMA "st_linemerge", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_linemerge", 'f', 2, {POSTGIS_SCHEMA "geometry", "bool"}, NULL},

	/* TODO: consider checking the SRID argument, which is currently truncated */
	{POSTGIS_SCHEMA "st_makeenvelope", 'f', 5, {"float8", "float8", "float8", "float8", "int4"}, NULL},
	{POSTGIS_SCHEMA "st_makeline", 'f', 1, {POSTGIS_SCHEMA "_geometry"}, NULL},
	{POSTGIS_SCHEMA "st_makeline", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_makepolygon", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_makepolygon", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "_geometry"}, NULL},
	{POSTGIS_SCHEMA "st_makevalid", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_m", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_normalize", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_npoints", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_numgeometries", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_numinteriorrings", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_numpoints", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_overlaps", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_perimeter", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_perimeter", 'f', 2, {POSTGIS_SCHEMA "geography", "bool"}, NULL},
	{POSTGIS_SCHEMA "st_point", 'f', 2, {"float8", "float8"}, NULL},
	{POSTGIS_SCHEMA "st_pointn", 'f', 2, {POSTGIS_SCHEMA "geometry", "int4"}, NULL},
	{POSTGIS_SCHEMA "st_pointonsurface", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_reduceprecision", 'f', 2, {POSTGIS_SCHEMA "geometry", "float8"}, NULL},
	{POSTGIS_SCHEMA "st_removerepeatedpoints", 'f', 2, {POSTGIS_SCHEMA "geometry", "float8"}, NULL},
	{POSTGIS_SCHEMA "st_reverse", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_shortestline", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_simplify", 'f', 2, {POSTGIS_SCHEMA "geometry", "float8"}, NULL},
	{POSTGIS_SCHEMA "st_simplifypreservetopology", 'f', 2, {POSTGIS_SCHEMA "geometry", "float8"}, NULL},
	{POSTGIS_SCHEMA "st_srid", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_startpoint", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_touches", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_transform", 'f', 3, {POSTGIS_SCHEMA "geometry", "text", "text"}, NULL},
	{POSTGIS_SCHEMA "st_union", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_within", 'f', 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_x", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_y", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_z", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_zmflag", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},

	/*
	 * Spatial aggregate functions -- these will get rewritten to duckdb
	 * equivalents
	 */


	/*
	 * ST_Intersection does not work due to not finding the underlying
	 * `ST_Intersection()` step function; I am not sure if it is related to
	 * the defined one having 3 args instead of just (geometry,geometry), but
	 * ST_Union has that signature and seems to work fine, so we potentially
	 * need to do something else here.
	 */

	/*
	 * {POSTGIS_SCHEMA "st_intersection", 'a', 1, {POSTGIS_SCHEMA "geometry"},
	 * NULL},
	 */

	{POSTGIS_SCHEMA "st_union", 'a', 1, {POSTGIS_SCHEMA "geometry"}, NULL},

	/*
	 * PostgreSQL has an implicit cast from bytea to geometry. We rewrite it
	 * to ST_AsWKB.
	 */
	{POSTGIS_SCHEMA "bytea", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},

	/*
	 * ST_AsBinary is called ST_AsWKB in DuckDB.
	 */
	{POSTGIS_SCHEMA "st_asbinary", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
	{POSTGIS_SCHEMA "st_asbinary", 'f', 2, {POSTGIS_SCHEMA "geometry", "text"}, IsStAsBinaryWithMyEndian},

	/*
	 * ST_Transform may be rewritten to postgis_transform_geometry by the time
	 * it gets to FDW, but we write it back.
	 */
	{POSTGIS_SCHEMA "postgis_transform_geometry", 'f', 4, {POSTGIS_SCHEMA "geometry", "text", "text", "int4"}, NULL},

	/*
	 * The geography(geometry) cast is allowed through specifically for cases
	 * where the spheroid variant of a function has a geography argument, e.g.
	 * st_area(geography, true). DuckDB does not have a geography type, and
	 * uses geometry for the equivalent spheroid expressions, so we remove the
	 * cast once we get to the rewriter.
	 *
	 * To get to the rewriter, we need to mark the function as shippable here.
	 */
	{POSTGIS_SCHEMA "geography", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},

	/*
	 * geometrytype is rewritten to st_geometrytype
	 */
	{POSTGIS_SCHEMA "geometrytype", 'f', 1, {POSTGIS_SCHEMA "geometry"}, NULL},
};


/*
 * GetShippableSpatialFunctionCount returns the number of shippable spatial functions.
 */
const PGDuckShippableFunction *
GetShippableSpatialFunctions(int *sizePointer)
{
	*sizePointer = ARRAY_SIZE(ShippableSpatialProcs);

	return ShippableSpatialProcs;
}


/*
 * IsStAsBinaryWithMyEndian checks whether the second argument matches our
 * endianness. If so, we can use push down as regular ST_AsWKB.
 */
static bool
IsStAsBinaryWithMyEndian(Node *node)
{
	char	   *myEndian = "NDR";

#ifdef WORDS_BIGENDIAN
	myEndian = "XDR";
#endif

	Const	   *endianConst;

	if (!GetConstArg(node, 1, &endianConst))
		return false;

	if (endianConst->constisnull || endianConst->consttype != TEXTOID)
		return false;

	char	   *endianVal = TextDatumGetCString(endianConst->constvalue);

	return strcmp(endianVal, myEndian) == 0;
}


/*
 * IsStAsGeoJSONWithDefault checks whether the 2nd and 3rd argument
 * match the behaviour of DuckDB spatial (whether written by the user
 * or injected as a default).
 *
 * Note: The arguments may not have been expanded yet at this stage,
 * so we have to check the argument count.
 */
static bool
IsStAsGeoJSONWithDefault(Node *node)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	Assert(list_length(funcExpr->args) >= 1);
	Assert(list_length(funcExpr->args) <= 3);

	if (list_length(funcExpr->args) == 1)
		return true;

	Const	   *maxDecimalDigitsConst;

	if (!GetConstArg(node, 1, &maxDecimalDigitsConst))
		return false;

	if (maxDecimalDigitsConst->constisnull || maxDecimalDigitsConst->consttype != INT4OID)
		return false;

	if (DatumGetInt32(maxDecimalDigitsConst->constvalue) != 9)
		return false;

	if (list_length(funcExpr->args) == 2)
		return true;

	Const	   *optionsConst;

	if (!GetConstArg(node, 2, &optionsConst))
		return false;

	if (optionsConst->constisnull || optionsConst->consttype != INT4OID)
		return false;

	if (DatumGetInt32(optionsConst->constvalue) != 8)
		return false;

	return true;
}


/*
 * IsComparisonWithDefaultGridSize checks whether the given node is a function
 * expression with a default third argument, which makes it safe to push
 * down.
 */
static bool
IsComparisonWithDefaultGridSize(Node *node)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	if (list_length(funcExpr->args) <= 2)
		return true;

	Const	   *gridSizeConst;

	if (!GetConstArg(node, 2, &gridSizeConst))
		return false;

	if (gridSizeConst->constisnull || gridSizeConst->consttype != FLOAT8OID)
		return false;

	if (DatumGetFloat8(gridSizeConst->constvalue) != -1.0)
		return false;

	return true;
}


/*
 * IsLastArgConst determines whether the last argument of a function call
 * is a Const.
 */
static bool
IsLastArgConst(Node *node)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	int			argCount = list_length(funcExpr->args);
	Const	   *lastArg;

	return GetConstArg(node, argCount - 1, &lastArg);
}


/*
 * IsPointDistanceSpheroid determines whether an st_distance(geography,geography,bool)
 * call can be pushed down, which is only the case if the last argument is constant
 * (such that we know how to rewrite) and the typemod restricts the geometry
 * to points.
 */
static bool
IsPointDistanceSpheroid(Node *node)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	Node	   *firstArg = linitial(funcExpr->args);
	Node	   *secondArg = linitial(funcExpr->args);

	/* we expect casts of geometry to geography */
	if (!IsA(firstArg, FuncExpr) || !IsA(secondArg, FuncExpr))
		return false;

	FuncExpr   *firstCast = castNode(FuncExpr, firstArg);
	FuncExpr   *secondCast = castNode(FuncExpr, secondArg);

	/* sanity check that it is actually casting to geography */
	if (firstCast->funcid != GeographyFunctionId() ||
		secondCast->funcid != GeographyFunctionId())
		return false;

	/* check whether the arguments are both points */
	if (!IsPointGeometry(linitial(firstCast->args)) ||
		!IsPointGeometry(linitial(secondCast->args)))
		return false;

	return true;
}


/*
 * IsPointGeometry determines whether the given node is of type geometry(point,...).
 */
static bool
IsPointGeometry(Node *node)
{
	int			typemod = exprTypmod(node);

	return GEOMETRY_GET_TYPE(typemod) == GEOMETRY_TYPE_POINT;
}
