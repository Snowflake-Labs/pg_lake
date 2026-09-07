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
 * init.c
 * Extension entry point, settings and the shared interruptible sleep.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/wait_event.h"

#include "pg_lake_snowflake/import_schema.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"

PG_MODULE_MAGIC;

double		SnowflakeDefaultRowEstimate = 1000000.0;
int			SnowflakeStatementTimeoutSeconds = 300;
bool		SnowflakeEnableAggregatePushdown = true;
bool		SnowflakeLogRemoteSql = false;
bool		SnowflakeAllowPlainHttp = false;
int			SnowflakeDefaultBatchSize = 500;
bool		SnowflakeWarnOnWriteInTransactionBlock = true;

void		_PG_init(void);

static void DefineSnowflakeSettings(void);


/*
 * _PG_init is the entry-point for the library.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;

	DefineSnowflakeSettings();
	SnowflakeInstallUtilityHook();
}


/*
 * DefineSnowflakeSettings defines the settings of the extension.
 */
static void
DefineSnowflakeSettings(void)
{
	DefineCustomRealVariable(
							 "pg_lake_snowflake.default_row_estimate",
							 gettext_noop("Number of rows assumed for a Snowflake table "
										  "that has never been analyzed."),
							 gettext_noop("A remote table has no size the planner can "
										  "read, so this is what it works with until "
										  "ANALYZE or the row_estimate option says "
										  "otherwise."),
							 &SnowflakeDefaultRowEstimate,
							 1000000.0, 1.0, 1e15,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable(
							"pg_lake_snowflake.statement_timeout",
							gettext_noop("Seconds Snowflake may spend on one statement."),
							gettext_noop("Zero leaves the limit to the account. The "
										 "statement_timeout option of a server "
										 "overrides this."),
							&SnowflakeStatementTimeoutSeconds,
							300, 0, 604800,
							PGC_USERSET,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_snowflake.enable_aggregate_pushdown",
							 gettext_noop("Let Snowflake evaluate grouping and "
										  "aggregation."),
							 NULL,
							 &SnowflakeEnableAggregatePushdown,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_snowflake.log_remote_sql",
							 gettext_noop("Log every statement sent to Snowflake."),
							 NULL,
							 &SnowflakeLogRemoteSql,
							 false,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_snowflake.allow_plain_http",
							 gettext_noop("Allow an account_url that is not https."),
							 gettext_noop("Credentials travel in request headers, so "
										  "this exists for tests that point a server at "
										  "a local mock of the SQL API."),
							 &SnowflakeAllowPlainHttp,
							 false,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable(
							"pg_lake_snowflake.batch_size",
							gettext_noop("Rows an INSERT sends to Snowflake in one "
										 "statement."),
							gettext_noop("A statement is one round trip, so this is "
										 "what decides the speed of a load. The "
										 "batch_size option of a table or a server "
										 "overrides it."),
							&SnowflakeDefaultBatchSize,
							500, 1, 100000,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_snowflake.warn_on_write_in_transaction_block",
							 gettext_noop("Warn when a Snowflake table is written "
										  "inside a transaction block."),
							 gettext_noop("Snowflake commits each statement on its "
										  "own, so a ROLLBACK cannot undo a write. "
										  "The warning is issued once per "
										  "transaction."),
							 &SnowflakeWarnOnWriteInTransactionBlock,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved("pg_lake_snowflake");
}


/*
 * SnowflakeSleepMs waits without becoming unresponsive to a cancellation, which
 * matters while polling a statement that may run for minutes.
 */
void
SnowflakeSleepMs(int milliseconds)
{
	CHECK_FOR_INTERRUPTS();

	if (milliseconds <= 0)
		return;

	(void) WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					 milliseconds, PG_WAIT_EXTENSION);
	ResetLatch(MyLatch);

	CHECK_FOR_INTERRUPTS();
}
