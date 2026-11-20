/*
 * Copyright 2026 Snowflake Inc.
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

/*
 * Connecting to and disconnecting from an external query engine.
 *
 * From PostgreSQL 19, libpq-be-fe-helpers.h includes libpq-be-fe.h, which
 * redefines PGresult and the libpq functions that use it in every file that
 * includes it. The rest of pg_lake passes plain PGresults between files, so
 * the helpers are only used here, where no PGresult is handled.
 */
#include "postgres.h"

#include "libpq-fe.h"
#include "libpq/libpq-be-fe-helpers.h"
#include "utils/wait_event.h"

#include "pg_lake/pgduck/client.h"


/*
 * ConnectToExternalQueryEngine opens a connection to an external query
 * engine. Unlike PQconnectdb, a cancel request or statement_timeout
 * interrupts an attempt that hangs, and the connection's file descriptor is
 * reserved, as in postgres_fdw.
 */
PGconn *
ConnectToExternalQueryEngine(const char *conninfo)
{
	return libpqsrv_connect(conninfo, PG_WAIT_EXTENSION);
}


/*
 * DisconnectFromExternalQueryEngine closes a connection opened by
 * ConnectToExternalQueryEngine and releases its file descriptor.
 */
void
DisconnectFromExternalQueryEngine(PGconn *connection)
{
	libpqsrv_disconnect(connection);
}
