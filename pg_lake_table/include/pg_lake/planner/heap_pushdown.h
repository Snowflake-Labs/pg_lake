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

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

/* pg_lake_table.enable_heap_query_pushdown setting */
extern bool EnableHeapQueryPushdown;

/* pg_lake_table.heap_pushdown_dsn setting */
extern char *HeapPushdownDSN;

extern bool HeapRteIsPushdownable(RangeTblEntry *rte);
extern bool HeapRteIsRelationPushdownable(RangeTblEntry *rte);
extern bool AllInheritorsArePushdownableHeap(Oid parentRelationId);
extern List *ReplaceHeapTableWithReadTableFunc(Node *node);
extern char *ReplaceHeapTableFunctionCalls(char *query, List *heapRteList,
										   bool explainRequested);
