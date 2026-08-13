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

#include "datatype/timestamp.h"
#include "nodes/pg_list.h"
#include "pg_extension_base/extension_ids.h"

#define TIMESERIES_NSP "timeseries"
#define TIMESERIES_TABLES_TABLE "tables"
#define TIMESERIES_TABLES_PKEY "tables_pkey"
#define TIMESERIES_PARTITIONS_TABLE "partitions"
#define TIMESERIES_PARTITIONS_PKEY "partitions_pkey"

/* attribute numbers of timeseries.tables */
#define Anum_tables_relation 1
#define Anum_tables_cold_table 2
#define Anum_tables_time_column 3
#define Anum_tables_partition_interval 4
#define Anum_tables_boundary 5
#define Anum_tables_hot_retention 6
#define Anum_tables_cold_retention 7
#define Anum_tables_precreate_ahead 8
#define Natts_tables 8

/* attribute numbers of timeseries.partitions */
#define Anum_partitions_relation 1
#define Anum_partitions_part_start 2
#define Anum_partitions_part_end 3
#define Anum_partitions_synced_at 4
#define Anum_partitions_sealed_at 5
#define Natts_partitions 5

/*
 * TieredTable is one row of timeseries.tables: a user-facing range-partitioned
 * heap that the planner extends with its Iceberg tier.
 *
 * All of it is cached per backend, which is only sound because none of it changes
 * often: the boundary moves on seal() and nothing else, and per-range sync state
 * (timeseries.partitions) is deliberately not here -- the planner does not read
 * it, see metadata.c.
 */
typedef struct TieredTable
{
	/* the user's own relation, and the hash key of the membership cache */
	Oid			relationId;

	/* Iceberg table holding the same columns */
	Oid			coldTableId;

	/* time (partitioning) column of relationId */
	NameData	timeColumn;

	/* fixed-length partition granularity of relationId */
	Interval	partitionInterval;

	/* authority boundary: relationId owns >= it, coldTableId owns < it */
	TimestampTz boundary;

	/* how much time stays authoritative in PostgreSQL */
	Interval	hotRetention;

	/* how much history Iceberg keeps; coldRetentionIsNull means forever */
	Interval	coldRetention;
	bool		coldRetentionIsNull;

	/* partition intervals pre-created ahead of now() */
	int32		precreateAhead;
}			TieredTable;

/* generic extension state for pg_lake_timeseries */
extern PGDLLEXPORT CachedExtensionIds * PgLakeTimeseries;

extern void InitializePgLakeTimeseriesMetadata(void);

extern Oid	TimeseriesTablesRelationId(void);
extern Oid	TimeseriesTablesPrimaryKeyId(void);
extern Oid	TimeseriesPartitionsRelationId(void);
extern Oid	TimeseriesPartitionsPrimaryKeyId(void);

extern bool AnyTieredTables(void);
extern bool IsTieredTable(Oid relationId);
extern bool GetTieredTable(Oid relationId, TieredTable * result);
extern List *AllTieredTables(void);
extern void ResetTieredTableCache(void);
