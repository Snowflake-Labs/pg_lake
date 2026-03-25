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
#include "pg_lake_log/log_buffer.h"

/*
 * InsertBatchToIceberg writes entries[0..count-1] to the named Iceberg table.
 *
 * Internally it uses the same pipeline as "COPY tbl FROM 'file.csv'":
 *   1. Build a PostgreSQL VALUES query and run it through WriteQueryResultToCSV
 *      to produce a local temp CSV (PostgreSQL handles all type formatting).
 *   2. Pass the CSV to PrepareCSVInsertion, which sends it to pgduck_server;
 *      DuckDB converts it to a single Parquet file in object storage.
 *   3. Call ApplyDataFileModifications to commit the Iceberg snapshot.
 *
 * The caller must be inside an open transaction with an active snapshot.
 */
void		InsertBatchToIceberg(LogEntry *entries, int count,
								 const char *table_name);
