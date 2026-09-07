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
 * pg_lake_snowflake.h
 * Settings and declarations shared by the whole extension.
 */
#pragma once

#include "postgres.h"

/* pg_lake_snowflake.default_row_estimate */
extern double SnowflakeDefaultRowEstimate;

/* pg_lake_snowflake.statement_timeout */
extern int	SnowflakeStatementTimeoutSeconds;

/* pg_lake_snowflake.enable_aggregate_pushdown */
extern bool SnowflakeEnableAggregatePushdown;

/* pg_lake_snowflake.log_remote_sql */
extern bool SnowflakeLogRemoteSql;

/* pg_lake_snowflake.allow_plain_http */
extern bool SnowflakeAllowPlainHttp;

/* pg_lake_snowflake.batch_size */
extern int	SnowflakeDefaultBatchSize;

/* pg_lake_snowflake.warn_on_write_in_transaction_block */
extern bool SnowflakeWarnOnWriteInTransactionBlock;

/*
 * A statement is one round trip to a warehouse, which no local cost can
 * compete with, so the startup cost is deliberately far above
 * DEFAULT_FDW_STARTUP_COST: it is what makes the planner prefer one remote
 * query with pushed-down work over repeated scans.
 */
#define SNOWFLAKE_STARTUP_COST 10000.0

/* per-row cost of parsing a JSON row and forming a tuple */
#define SNOWFLAKE_PER_TUPLE_COST 0.01

/* interruptible sleep used while polling a running statement */
extern void SnowflakeSleepMs(int milliseconds);
