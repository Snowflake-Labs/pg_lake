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

/*
 * IcebergCompatibilityMode selects how a table's Iceberg STORAGE is shaped so
 * the table is consumable by a downstream engine with narrower type support.
 *
 * Unlike a type-rewrite, these modes never change the PostgreSQL column type:
 * the column stays exactly as declared (uuid stays uuid). Only the physical
 * Iceberg/Parquet storage type and the I/O-boundary conversions differ, and
 * that surface->storage divergence is persisted per-leaf in
 * lake_table.field_id_mappings (decided once, at registration).
 *
 *   ICEBERG_COMPAT_AUTO       default (option unset or 'auto'): no storage
 *                             divergence. This is the hook where future
 *                             auto-detection would live.
 *   ICEBERG_COMPAT_SNOWFLAKE  a uuid nested inside an array/composite is
 *                             stored as Iceberg `string` (Snowflake cannot
 *                             hold a UUID inside a structured type). A
 *                             top-level uuid column stays native `uuid`.
 *
 * The enum lives in pg_lake_engine (the lower layer) so the pgduck write path
 * can key size clamping off the mode without pg_lake_engine having to depend
 * on pg_lake_iceberg's headers.  The parsing/relation helpers that operate on
 * this enum stay in pg_lake_iceberg (pg_lake/iceberg/compatibility_mode.h).
 */
typedef enum IcebergCompatibilityMode
{
	ICEBERG_COMPAT_AUTO = 0,
	ICEBERG_COMPAT_SNOWFLAKE,
}			IcebergCompatibilityMode;
