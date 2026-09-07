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
 * functions.c
 * The SQL-callable functions of the extension.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_foreign_server.h"
#include "foreign/foreign.h"
#include "utils/acl.h"
#include "utils/builtins.h"

#include "pg_lake_snowflake/import_schema.h"
#include "pg_lake_snowflake/options.h"
#include "pg_lake_snowflake/sql_api.h"

PG_FUNCTION_INFO_V1(pg_lake_snowflake_execute);


/*
 * pg_lake_snowflake_execute runs one statement on the account behind a foreign
 * server and returns the first column of the first row.
 *
 * The statement runs with the credentials of the user mapping that applies to
 * the caller, so the caller needs the same privilege on the server that reading
 * a foreign table on it needs.
 */
Datum
pg_lake_snowflake_execute(PG_FUNCTION_ARGS)
{
	char	   *serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *statement = text_to_cstring(PG_GETARG_TEXT_PP(1));
	ForeignServer *server = GetForeignServerByName(serverName, false);

	if (!IsSnowflakeForeignServer(server->serverid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("foreign server \"%s\" is not a Snowflake server",
						serverName)));
	}

	AclResult	aclResult = object_aclcheck(ForeignServerRelationId, server->serverid,
											GetUserId(), ACL_USAGE);

	if (aclResult != ACLCHECK_OK)
		aclcheck_error(aclResult, OBJECT_FOREIGN_SERVER, serverName);

	SnowflakeConnection *connection = GetSnowflakeConnection(server->serverid,
															 GetUserId());
	char	   *value = SnowflakeExecuteScalar(connection, statement);

	if (value == NULL)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(value));
}
