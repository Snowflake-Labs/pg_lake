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

#include "pg_lake/parquet/field.h"

/*
 * Name of the per-table iceberg option that selects a compatibility mode.
 * Shared by option validation (pg_lake_table) and the conversion logic so the
 * accepted set lives in one place.
 */
#define ICEBERG_COMPATIBILITY_MODE_OPTION "compatibility_mode"

/*
 * IcebergCompatibilityMode selects how a table's Iceberg STORAGE is shaped so
 * the table is consumable by a downstream engine with narrower type support.
 *
 * Unlike a type-rewrite, these modes never change the Postgres column type: the
 * column stays exactly as declared.  Only the physical Iceberg/Parquet
 * representation and the I/O-boundary conversions differ.
 *
 *   ICEBERG_COMPAT_AUTO       default (unset or 'auto'): no storage divergence.
 *   ICEBERG_COMPAT_SNOWFLAKE  a uuid nested inside an array/map/composite is
 *                             stored as Iceberg `string` (Snowflake cannot hold
 *                             a UUID inside a structured type).  A top-level
 *                             uuid column stays native `uuid`.
 */
typedef enum IcebergCompatibilityMode
{
	ICEBERG_COMPAT_AUTO = 0,
	ICEBERG_COMPAT_SNOWFLAKE,
}			IcebergCompatibilityMode;

/* Maps an option string ("auto"/"snowflake"/NULL) to the enum; errors otherwise. */
extern PGDLLEXPORT IcebergCompatibilityMode ParseIcebergCompatibilityMode(const char *optionValue);

/* Reads the compatibility_mode option for relationId; AUTO for non-iceberg/unset. */
extern PGDLLEXPORT IcebergCompatibilityMode GetIcebergCompatibilityModeForTable(Oid relationId);

/*
 * True iff a uuid appears strictly nested (inside an array, map, or composite)
 * within typeOid.  A bare top-level uuid returns false.  Used to decide whether
 * a column participates in the snowflake storage divergence.
 */
extern PGDLLEXPORT bool TypeHasNestedUuid(Oid typeOid);

/*
 * Rewrites, in place, every nested (level > 0) scalar Iceberg field whose type
 * is "uuid" to "string", leaving a top-level uuid untouched.  No-op unless mode
 * is ICEBERG_COMPAT_SNOWFLAKE.
 */
extern PGDLLEXPORT void RewriteNestedUuidFieldsToString(Field * field, IcebergCompatibilityMode mode);
