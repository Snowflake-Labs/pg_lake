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

/*-------------------------------------------------------------------------
 *
 * shippable_spatial_operators.c
 *	  List of operators that can be shipped to pgduck_server or rewritten.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_lake/pgduck/shippable_spatial_operators.h"
#include "pg_lake/extensions/postgis.h"

static const PGDuckShippableOperator ShippableSpatialOperators[] = {
	{"=", POSTGIS_SCHEMA, "geometry_eq", 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{"<->", POSTGIS_SCHEMA, "geometry_distance_centroid", 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
	{"&&", POSTGIS_SCHEMA, "geometry_overlaps", 2, {POSTGIS_SCHEMA "geometry", POSTGIS_SCHEMA "geometry"}, NULL},
};


/*
 * GetShippableSpatialOperatorCount
 *		Returns the number of shippable functions
 */
const PGDuckShippableOperator *
GetShippableSpatialOperators(int *sizePointer)
{
	*sizePointer = ARRAY_SIZE(ShippableSpatialOperators);

	return ShippableSpatialOperators;
}
