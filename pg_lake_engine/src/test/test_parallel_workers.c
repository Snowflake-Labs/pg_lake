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

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "pg_lake/util/parallel_workers.h"
#include "utils/acl.h"
#include "utils/array.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(test_run_commands_in_parallel);


/*
 * test_run_commands_in_parallel is a test function that runs the given
 * commands with RunCommandsInParallel in the current database as the
 * current user.
 */
Datum
test_run_commands_in_parallel(PG_FUNCTION_ARGS)
{
	ArrayType  *commandArray = PG_GETARG_ARRAYTYPE_P(0);
	int			workerCount = PG_GETARG_INT32(1);
	int			maxFailures = PG_GETARG_INT32(2);

	if (workerCount < 1)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("worker count must be at least 1")));

	Datum	   *commandDatums;
	bool	   *commandNulls;
	int			commandCount;

	deconstruct_array_builtin(commandArray, TEXTOID, &commandDatums,
							  &commandNulls, &commandCount);

	List	   *commands = NIL;

	for (int i = 0; i < commandCount; i++)
	{
		if (commandNulls[i])
			ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
							errmsg("commands cannot be NULL")));

		commands = lappend(commands, TextDatumGetCString(commandDatums[i]));
	}

	char	   *databaseName = get_database_name(MyDatabaseId);
	char	   *userName = GetUserNameFromId(GetUserId(), false);

	RunCommandsInParallel(commands, databaseName, userName, workerCount,
						  maxFailures);

	PG_RETURN_VOID();
}
