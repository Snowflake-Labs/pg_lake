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

#include "pg_lake/ddl/utility_hook.h"

bool		ProcessCreatePgLakeTable(ProcessUtilityParams *params, void *arg);
bool		ProcessCreateAsSelectPgLakeTable(ProcessUtilityParams *params, void *arg);
bool		ErrorUnsupportedCreatePgLakeTableHandler(ProcessUtilityParams *params, void *arg);
void		CreatePgLakeTableCheckUnsupportedFeaturesPostProcess(ProcessUtilityParams *params, void *arg);
bool		ColumnDefIsPseudoSerial(ColumnDef *column);
List	   *GetRestrictedColumnDefList(List *columnDefList);

typedef bool (*PgLakeIsReservedColumnNameHookType) (const char *columnName);

extern PGDLLEXPORT PgLakeIsReservedColumnNameHookType PgLakeIsReservedColumnNameHook;
