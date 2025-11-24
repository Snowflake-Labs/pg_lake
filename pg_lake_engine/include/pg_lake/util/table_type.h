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


/* distinguish tables with "server pg_lake" vs "server pg_lake_iceberg" */
typedef enum PgLakeTableType
{
	PG_LAKE_INVALID_TABLE_TYPE,
	PG_LAKE_TABLE_TYPE,
	PG_LAKE_ICEBERG_TABLE_TYPE
}			PgLakeTableType;


extern PGDLLEXPORT const char *PgLakeTableTypeToName(PgLakeTableType tableType);
extern PGDLLEXPORT PgLakeTableType GetPgLakeTableType(Oid foreignTableId);
extern PGDLLEXPORT bool IsWritablePgLakeTable(Oid relationId);
extern PGDLLEXPORT bool IsIcebergTable(Oid relationId);
extern PGDLLEXPORT bool IsInternalIcebergTable(Oid relationId);
extern PGDLLEXPORT bool IsExternalIcebergTable(Oid relationId);
