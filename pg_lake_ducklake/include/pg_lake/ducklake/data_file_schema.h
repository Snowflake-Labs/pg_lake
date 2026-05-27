/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef PG_LAKE_DUCKLAKE_DATA_FILE_SCHEMA_H
#define PG_LAKE_DUCKLAKE_DATA_FILE_SCHEMA_H

#include "postgres.h"
#include "pg_lake/data_file/data_files.h"

/*
 * Build a DataFileSchema for a DuckLake foreign table by reading
 * pg_attribute and lake_ducklake.column. Field IDs come from
 * lake_ducklake.column.column_id so they line up with what DuckDB's
 * ducklake extension embeds in parquet files.
 *
 * Lives here so register_field_ids.c in pg_lake_table can stay free
 * of DuckLake-specific catalog reads.
 */
extern PGDLLEXPORT DataFileSchema * DucklakeBuildDataFileSchema(Oid relationId);

#endif							/* PG_LAKE_DUCKLAKE_DATA_FILE_SCHEMA_H */
