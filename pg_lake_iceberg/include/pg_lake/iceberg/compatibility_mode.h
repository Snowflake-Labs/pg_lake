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

/*
 * Name of the per-table iceberg option that selects a compatibility mode.
 * Defined here so option validation (pg_lake_table) and the conversion logic
 * share a single source of truth.
 */
#define ICEBERG_COMPATIBILITY_MODE_OPTION "compatibility_mode"

/*
 * IcebergCompatibilityMode selects how a table's Iceberg STORAGE is shaped so
 * the table is consumable by a downstream engine with narrower type support.
 *
 * Unlike a type-rewrite, these modes never change the PostgreSQL column type:
 * the column stays exactly as declared (uuid stays uuid). The intent is that
 * only the physical Iceberg/Parquet storage type and the I/O-boundary
 * conversions differ, with that surface->storage divergence persisted per-leaf
 * in lake_table.field_id_mappings (decided once, at registration).
 *
 * This option layer recognizes and validates the mode and enforces the DDL
 * guards; on its own it records no divergence (a storage no-op). The actual
 * per-leaf storage shaping is layered on top by the surface/storage mapping
 * change.
 *
 *   ICEBERG_COMPAT_AUTO       default (option unset or 'auto'): no storage
 *                             divergence. This is the hook where future
 *                             auto-detection would live.
 *   ICEBERG_COMPAT_SNOWFLAKE  selects the Snowflake-compatible storage shape
 *                             (a uuid nested inside an array/composite is
 *                             stored as Iceberg `string`, since Snowflake
 *                             cannot hold a UUID inside a structured type; a
 *                             top-level uuid column stays native `uuid`).
 */
typedef enum IcebergCompatibilityMode
{
	ICEBERG_COMPAT_AUTO = 0,
	ICEBERG_COMPAT_SNOWFLAKE,
}			IcebergCompatibilityMode;

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
 * True iff typeOid is, or contains at any depth, a pg_map type. Used to reject
 * map columns under compatibility_mode='snowflake' (Snowflake cannot represent
 * them, mirroring snowflake_cdc's restriction).
 */
extern PGDLLEXPORT bool TypeContainsMap(Oid typeOid);
