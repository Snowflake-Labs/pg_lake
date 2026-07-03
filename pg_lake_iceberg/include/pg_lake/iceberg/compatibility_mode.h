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

#include "postgres.h"

#include "nodes/pg_list.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/compatibility_mode.h"

/*
 * Name of the per-table iceberg option that selects a compatibility mode.
 * Defined here so option validation (pg_lake_table) and the conversion logic
 * share a single source of truth.
 */
#define ICEBERG_COMPATIBILITY_MODE_OPTION "compatibility_mode"

/*
 * IcebergCompatibilityMode itself is defined in pg_lake_engine
 * (pg_lake/pgduck/compatibility_mode.h) so the write path can key size
 * clamping off it without pg_lake_engine depending on this module.  The
 * helpers below add the iceberg-side parsing and per-field storage mapping on
 * top of that enum.
 */

/*
 * GUC pg_lake_iceberg.default_compatibility_mode: the compatibility_mode a new
 * iceberg table adopts when CREATE does not specify one. Backed by an enum GUC
 * (declared int, as Postgres requires for enum GUCs), so the accepted set is
 * exactly {auto, snowflake}, matched case-insensitively. Defaults to AUTO.
 */
extern PGDLLEXPORT int IcebergDefaultCompatibilityMode;

/* Canonical lowercase name ("auto"/"snowflake") for a mode. */
extern PGDLLEXPORT const char *IcebergCompatibilityModeName(IcebergCompatibilityMode mode);

/* Maps an option string ("auto"/"snowflake"/NULL) to the enum; errors otherwise. */
extern PGDLLEXPORT IcebergCompatibilityMode ParseIcebergCompatibilityMode(const char *optionValue);

/* Reads the option out of a CREATE statement's WITH (...) DefElem list. */
extern PGDLLEXPORT IcebergCompatibilityMode IcebergCompatibilityModeFromCreateOptions(List *options);

/* Reads the option from an existing relation; AUTO for non-iceberg/unset. */
extern PGDLLEXPORT IcebergCompatibilityMode IcebergCompatibilityModeFromRelation(Oid relationId);

/*
 * Rewrites, in place, the storage type of every nested (level > 0) scalar
 * Iceberg field for which the mode's per-leaf policy diverges from the surface
 * type (e.g. snowflake stores a nested "uuid" as "string"), leaving top-level
 * fields untouched. No-op for ICEBERG_COMPAT_AUTO. Maps are NOT descended: a
 * column containing a map is rejected at DDL time under a restrictive mode.
 */
extern PGDLLEXPORT void ApplyCompatibilityStorageMapping(Field * field,
														 IcebergCompatibilityMode mode);

/*
 * True iff typeOid is, or contains at any depth, a pg_map type. Used to reject
 * map columns under compatibility_mode='snowflake' (Snowflake cannot represent
 * them, mirroring snowflake_cdc's restriction).
 */
extern PGDLLEXPORT bool TypeContainsMap(Oid typeOid);
