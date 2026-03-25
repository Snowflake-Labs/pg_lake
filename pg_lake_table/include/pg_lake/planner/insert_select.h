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

#include "nodes/parsenodes.h"
#include "pg_lake/copy/copy_format.h"

/* pg_lake_table.enable_insert_select_pushdown setting */
extern bool EnableInsertSelectPushdown;

/* set by IsPushdownableInsertSelectQuery when postgres_scan is needed */
extern bool InsertSelectUsesPostgresScan;

bool		IsPushdownableInsertSelectQuery(Query *query);
bool		IsInsertSelectQuery(Query *query);
Oid			GetInsertRelidFromInsertSelect(Query *query);
void		TransformPushdownableInsertSelect(Query *query);

/* logic shared with COPY pushdown */
extern PGDLLEXPORT bool RelationColumnsSuitableForPushdown(Relation relation, CopyDataFormat sourceFormat);
extern PGDLLEXPORT bool RelationSuitableForPushdown(Relation relation, bool allowDefaultConsts);
