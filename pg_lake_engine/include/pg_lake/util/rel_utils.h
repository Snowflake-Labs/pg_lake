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

#include "nodes/primnodes.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/table_type.h"


#define PG_LAKE_SERVER_NAME "pg_lake"
#define PG_LAKE_ICEBERG_SERVER_NAME "pg_lake_iceberg"

extern PGDLLEXPORT bool IsAnyLakeForeignTableById(Oid foreignTableId);
extern PGDLLEXPORT char *GetQualifiedRelationName(Oid relationId);
extern PGDLLEXPORT char *GetPgLakeForeignServerName(Oid foreignTableId);
extern PGDLLEXPORT PgLakeTableType GetPgLakeTableTypeViaServerName(char *serverName);
extern PGDLLEXPORT bool IsPgLakeForeignTableById(Oid foreignTableId);
extern PGDLLEXPORT bool IsPgLakeIcebergForeignTableById(Oid foreignTableId);
extern PGDLLEXPORT bool IsPgLakeServerName(const char *serverName);
extern PGDLLEXPORT bool IsPgLakeIcebergServerName(const char *serverName);
extern PGDLLEXPORT char *GetWritableTableLocation(Oid relationId, char **queryArguments);
extern PGDLLEXPORT void EnsureTableOwner(Oid relationId);
extern PGDLLEXPORT bool IsAnyLakeForeignTable(RangeTblEntry *rte);
extern PGDLLEXPORT CopyDataFormat GetForeignTableFormat(Oid foreignTableId);
extern PGDLLEXPORT char *GetForeignTablePath(Oid foreignTableId);
extern PGDLLEXPORT void ErrorIfTypeUnsupportedForIcebergTables(Oid typeOid, int32 typmod, char *columnName);
extern PGDLLEXPORT void ErrorIfTypeUnsupportedNumericForIcebergTables(int32 typmod, char *columnName);
extern PGDLLEXPORT PgLakeTableProperties GetPgLakeTableProperties(Oid relationId);

/* range var help */
extern PGDLLEXPORT List *MakeNameListFromRangeVar(const RangeVar *rel);
