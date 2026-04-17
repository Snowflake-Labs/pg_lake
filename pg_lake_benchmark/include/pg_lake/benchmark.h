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

typedef enum BenchmarkType
{
	BENCHMARK_TPCH = 0,
	BENCHMARK_TPCDS = 1
} BenchmarkType;

typedef enum BenchmarkTableType
{
	BENCHMARK_ICEBERG_TABLE = 0,
	BENCHMARK_LAKE_TABLE = 1,
	BENCHMARK_HEAP_TABLE = 2
} BenchmarkTableType;

typedef struct BenchQuery
{
	const char *query;
	int			query_nr;
} BenchQuery;

extern void PgDuckInstallBenchExtension(BenchmarkType benchType);
extern void PgDuckDropBenchTables(const char *tableNames[], int length);
extern void PgDuckGenerateBenchTables(BenchmarkType benchType, float4 scaleFactor, int iterationCount);
extern void PgDuckCopyBenchTablesToRemoteParquet(const char *tableNames[], int length, char *location);
extern void PgDuckGetQueries(BenchmarkType benchType, BenchQuery *queries, int queryCount);
extern void PgLakeDropBenchTables(const char *tableNames[], int length, char *location);
extern void PgLakeCreateBenchTables(const char *tableNames[], char **partitionBys, int length, BenchmarkTableType tableType, char *location);
extern BenchmarkTableType GetBenchTableType(Oid tableTypeId);
