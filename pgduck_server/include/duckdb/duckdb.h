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

#ifndef PGDUCK_DUCKDB_H
#define PGDUCK_DUCKDB_H

#include <duckdb.h>

#include "lib/stringinfo.h"

/* DuckDB's special path for an in-memory database (no on-disk file). */
#define DUCKDB_MEMORY_DB_PATH ":memory:"

/* SQLSTATEs reported to the client for DuckDB errors */
#define PGDUCK_SQLSTATE_FEATURE_NOT_SUPPORTED "0A000"
#define PGDUCK_SQLSTATE_OUT_OF_MEMORY "53200"
#define PGDUCK_SQLSTATE_IO_ERROR "58030"
#define PGDUCK_SQLSTATE_INVALID_PARAMETER "22023"
#define PGDUCK_SQLSTATE_INTERNAL_ERROR "XX000"

struct PGSession;
struct ResponseFormat;

typedef enum
{
	DUCKDB_SUCCESS = 0,
	DUCKDB_INITIALIZATION_ERROR,
	DUCKDB_SESSION_INITIALIZATION_ERROR,
	DUCKDB_QUERY_ERROR,
	DUCKDB_TYPE_CONVERSION_ERROR,
	DUCKDB_PG_COMMUNICATION_ERROR,
	DUCKDB_OUT_OF_MEMORY_ERROR,
	DUCKDB_FATAL_ERROR,
	DUCKDB_INVALID
}			DuckDBStatus;

typedef struct DuckDBSession
{
	struct PGSession *clientSession;
	duckdb_connection connection;
	duckdb_prepared_statement duckPreparedStatement;

	/*
	 * SQLSTATE mapped from the last DuckDB error type, or NULL when the error
	 * did not carry one. Consumed and cleared when the error is reported.
	 */
	const char *errorSqlState;
}			DuckDBSession;

/* global instance of DuckDB that is shared across threads */
extern duckdb_database DuckDB;

extern DuckDBStatus duckdb_global_init(char *databaseFilePath,
									   char *cacheDir,
									   char *extensionsDir,
									   bool allowExtensionInstall,
									   char *memoryLimit,
									   int64_t cacheOnWriteMaxSize,
									   char *initFile);
extern DuckDBStatus duckdb_session_init(DuckDBSession * duckSession,
										struct PGSession *clientSession);
extern DuckDBStatus duckdb_session_run_command(DuckDBSession * duckSession, const char *queryString,
											   struct ResponseFormat *responseFormat,
											   char **errorMessage);
extern DuckDBStatus duckdb_session_prepare(DuckDBSession * duckSession,
										   const char *queryString,
										   char **errorMessage);
extern int	duckdb_session_prepared_nparams(DuckDBSession * session);
extern DuckDBStatus duckdb_session_bind_varchar(DuckDBSession * duckSession,
												int paramNumber, const char *value,
												char **errorMessage);
extern DuckDBStatus duckdb_session_execute_prepared(DuckDBSession * duckSession,
													struct ResponseFormat *responseFormat,
													char **errorMessage);
extern void duckdb_session_destroy_prepare(DuckDBSession * duckSession);
extern void duckdb_session_destroy(DuckDBSession * duckSession);


#endif
