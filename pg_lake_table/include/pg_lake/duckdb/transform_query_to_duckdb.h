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
 * transform_query_to_duckdb.c
 *		  Apply any transformations to the query before sending it to duckdb.
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "pg_lake/fdw/snapshot.h"
#include "nodes/parsenodes.h"

#define EXPLAIN_REQUESTED (1 << 0)
#define SKIP_FULL_MATCH_FILES (1 << 1)

extern char *ReplaceReadTableFunctionCalls(char *query,
										   PgLakeScanSnapshot * snapshot,
										   int scanFlags);
extern char *ReplaceHeapTableReadCalls(char *query, List *heapRteList,
									   bool explainRequested);
