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

bool		ProcessAlterTable(ProcessUtilityParams * params, void *arg);
bool		ProcessAlterType(ProcessUtilityParams * params, void *arg);
bool		ProcessEnumStatement(ProcessUtilityParams * params, void *arg);
bool		ProcessAlterTypeAttributeRename(ProcessUtilityParams * processUtilityParams, void *arg);
void		PostProcessRenameWritablePgLakeTable(ProcessUtilityParams * params, void *arg);
void		PostProcessAlterWritablePgLakeTableSchema(ProcessUtilityParams * params, void *arg);

/* allow other hooks */
typedef void (*PgLakeAlterTableHookType) (Oid relationOid, AlterTableStmt *stmt);
typedef void (*PgLakeAlterTableRenameColumnHookType) (Oid relationOid, RenameStmt *stmt);
typedef void (*PgLakeAlterTableSetSchemaHookType) (Oid relationOid, AlterObjectSchemaStmt *stmt);
typedef bool (*PgLakeIsReplicationTargetHookType) (Oid relationOid);

extern PGDLLEXPORT PgLakeAlterTableHookType PgLakeAlterTableHook;
extern PGDLLEXPORT PgLakeAlterTableRenameColumnHookType PgLakeAlterTableRenameColumnHook;
extern PGDLLEXPORT PgLakeAlterTableSetSchemaHookType PgLakeAlterTableSetSchemaHook;
extern PGDLLEXPORT PgLakeIsReplicationTargetHookType PgLakeIsReplicationTargetHook;
