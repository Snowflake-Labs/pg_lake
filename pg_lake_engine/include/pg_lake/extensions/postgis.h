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

#pragma once

#include "pg_extension_base/extension_ids.h"
#include "pg_lake/pgduck/type.h"

#define POSTGIS_SCHEMA "$postgis."

#define GEOMETRY_TYPE_UNKNOWN      0
#define GEOMETRY_TYPE_POINT        1
#define GEOMETRY_TYPE_LINE         2
#define GEOMETRY_TYPE_POLYGON      3
#define GEOMETRY_TYPE_MULTIPOINT   4
#define GEOMETRY_TYPE_MULTILINE    5
#define GEOMETRY_TYPE_MULTIPOLYGON 6
#define GEOMETRY_TYPE_COLLECTION   7

#define GEOMETRY_GET_TYPE(typmod) ((typmod & 0x000000FC)>>2)
#define GEOMETRY_GET_SRID(typmod) ((((typmod) & 0x0FFFFF00) - ((typmod) & 0x10000000)) >> 8)

extern PGDLLEXPORT CachedExtensionIds * Postgis;

void		InitializePostgisIdCache(void);

extern PGDLLEXPORT bool IsGeometryType(PGType postgresType);
extern PGDLLEXPORT bool IsGeometryTypeId(Oid typeId);
extern PGDLLEXPORT bool IsGeographyTypeId(Oid typeId);
extern PGDLLEXPORT Oid GeometryArrayTypeId(void);
extern PGDLLEXPORT Oid GeometryTypeId(void);
extern PGDLLEXPORT Oid GeographyTypeId(void);
extern PGDLLEXPORT bool IsGeometryOutFunctionId(Oid functionId);
extern PGDLLEXPORT Oid GeographyFunctionId(void);
extern PGDLLEXPORT Oid GeometryOutFunctionId(void);
extern PGDLLEXPORT Oid ST_AsBinaryFunctionId(void);
extern PGDLLEXPORT Oid ST_AsTextFunctionId(void);
extern PGDLLEXPORT Oid ST_FlipCoordinatesFunctionId(void);
extern PGDLLEXPORT Oid ST_GeomFromTextFunctionId(void);
extern PGDLLEXPORT Oid ST_UnionAggregateId(void);
extern PGDLLEXPORT Oid ST_Union_AggAggregateId(void);
