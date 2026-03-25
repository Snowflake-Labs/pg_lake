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

#include "pg_lake/planner/extensible_nodes.h"

/* pg_lake_table.enable_full_query_pushdown setting */
extern bool EnableFullQueryPushdown;

/* pg_lake_table.enable_postgres_scan_in_pushdown setting */
extern bool EnablePostgresScanInPushdown;

void		InitializeFullQueryPushdown(void);
bool		FullQueryIsPushdownable(Query *query);
bool		FullQueryIsPushdownableWithPostgresScan(Query *parse);
bool		HasNotShippableExpression(Node *node);
extern PGDLLEXPORT bool HasLakeRTE(Node *node, void *context);
