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
 * sql_api.h
 * Statement execution over the Snowflake SQL API v2.
 */
#pragma once

#include "postgres.h"

#include "pg_lake_snowflake/options.h"
#include "pg_lake_snowflake/type_map.h"
#include "utils/jsonb.h"
#include "utils/palloc.h"

/*
 * SnowflakeStatement is one statement submitted to /api/v2/statements and the
 * cursor over the result set it produced. Results arrive one partition at a
 * time and partitions are fetched as the cursor reaches them.
 */
typedef struct SnowflakeStatement
{
	SnowflakeConnection *connection;
	char	   *sql;
	char	   *handle;

	/*
	 * Whether the statement has finished running on the Snowflake side. While
	 * this is false the statement is a candidate for cancellation.
	 */
	bool		completed;

	SnowflakeResultColumn *columns;
	int			columnCount;

	int			partitionCount;
	int64		totalRowCount;

	/* cursor state */
	int			currentPartition;
	int			rowCountInPartition;
	int			currentRowInPartition;

	/* the "data" array of the partition currently held in partitionContext */
	JsonbValue *partitionData;

	/* lives as long as the statement */
	MemoryContext statementContext;

	/* reset for every partition fetched */
	MemoryContext partitionContext;
}			SnowflakeStatement;

extern SnowflakeStatement * SnowflakeExecute(SnowflakeConnection * connection, const char *sql);
extern bool SnowflakeStatementNextRow(SnowflakeStatement * statement);
extern char *SnowflakeStatementGetValue(SnowflakeStatement * statement, int columnIndex);
extern void SnowflakeStatementClose(SnowflakeStatement * statement);

/* run a statement and return the first column of the first row, or NULL */
extern char *SnowflakeExecuteScalar(SnowflakeConnection * connection, const char *sql);
