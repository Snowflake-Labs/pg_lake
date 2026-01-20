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

#ifndef PGDUCK_CLIENT_H
#define PGDUCK_CLIENT_H

#include "libpq-fe.h"

#include "nodes/pg_list.h"

#define DEFAULT_PGDUCK_SERVER_CONNINFO "host=/tmp port=5332"

#define DEFAULT_DUCKDB_MAX_LINE_SIZE (2097152)

/* settings */
extern char *PgduckServerConninfo;

typedef struct PGDuckConnection
{
	uint32		connectionId;
	PGconn	   *conn;

}			PGDuckConnection;

extern PGDLLEXPORT PGDuckConnection * GetPGDuckConnection(void);
extern PGDLLEXPORT void ReleasePGDuckConnection(PGDuckConnection * pgDuckConnection);
extern PGDLLEXPORT int64 ExecuteCommandInPGDuck(char *query);
extern PGDLLEXPORT List *ExecuteCommandsInPGDuck(List *commands);
extern PGDLLEXPORT bool ExecuteOptionalCommandInPGDuck(char *command);
extern PGDLLEXPORT PGresult *ExecuteQueryOnPGDuckConnection(PGDuckConnection * pgDuckConnection,
															const char *query);
extern PGDLLEXPORT PGresult *WaitForResult(PGDuckConnection * conn);
extern PGDLLEXPORT PGresult *WaitForLastResult(PGDuckConnection * conn);
extern PGDLLEXPORT void SendQueryToPGDuck(PGDuckConnection * conn, char *query);
extern PGDLLEXPORT void CheckPGDuckResult(PGDuckConnection * conn, PGresult *result);
extern PGDLLEXPORT void ThrowIfPGDuckResultHasError(PGDuckConnection * conn, PGresult *result);
extern PGDLLEXPORT char *GetSingleValueFromPGDuck(char *query);
extern PGDLLEXPORT void SendQueryWithParams(PGDuckConnection * pgduckConn, char *queryString,
											int numParams, const char **parameterValues);

#endif
