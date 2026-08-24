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

/*
 * User-defined functions for testing the pgduck_server connection lifecycle.
 */

#include "postgres.h"
#include "fmgr.h"

#include "pg_lake/pgduck/client.h"


PG_FUNCTION_INFO_V1(release_pgduck_connection_twice);


/*
 * release_pgduck_connection_twice releases the same connection twice, which is
 * what a caller does when it releases both in an error path and in the
 * PG_FINALLY block that follows. The second release must be a no-op.
 */
Datum
release_pgduck_connection_twice(PG_FUNCTION_ARGS)
{
	PGDuckConnection *pgDuckConnection = GetPGDuckConnection();

	ReleasePGDuckConnection(pgDuckConnection);
	ReleasePGDuckConnection(pgDuckConnection);

	PG_RETURN_VOID();
}
