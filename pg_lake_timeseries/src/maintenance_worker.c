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
 * Maintenance background worker for pg_lake_timeseries.
 *
 * Registered through pg_extension_base's base-worker framework (see
 * extension_base.register_worker in the install script), which gives us one
 * worker per database that has the extension, started on CREATE EXTENSION and
 * on server start, and stopped on DROP EXTENSION / DROP DATABASE.
 *
 * Each pass calls timeseries.maintain() for every enabled table, in its own
 * transaction so that one failing table does not hold back the others. The
 * work performed per table is described in DESIGN.md section 13.8: extend the
 * hot partition frontier, refresh the Iceberg copy of past hot partitions,
 * seal partitions that aged out of the hot window (advancing the authority
 * boundary), repair partitions that were mutated below the boundary, and apply
 * cold retention.
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "executor/spi.h"
#include "nodes/pg_list.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"

#include "pg_extension_base/base_workers.h"

/* pg_lake_timeseries.enable / .maintenance_naptime, defined in init.c */
extern bool EnablePgLakeTimeseries;
extern int	PgLakeTimeseriesNaptimeMs;

/* UDF implementations */
PG_FUNCTION_INFO_V1(pg_lake_timeseries_maintenance_worker);

static void MaintainAllTables(MemoryContext resultContext);
static List *ListEnabledTables(MemoryContext resultContext);
static void MaintainTable(char *tableName);


/*
 * pg_lake_timeseries_maintenance_worker is the entry point of the
 * pg_lake_timeseries maintenance base worker. It is called outside of a
 * transaction and starts its own transactions as needed.
 */
Datum
pg_lake_timeseries_maintenance_worker(PG_FUNCTION_ARGS)
{
	int32		workerId = PG_GETARG_INT32(0);

	ereport(LOG, (errmsg("pg_lake_timeseries maintenance worker %d started", workerId)));

	/* report application_name in pg_stat_activity */
	pgstat_report_appname("pg_lake_timeseries maintenance");

	/* report process name in ps (follows "pg_extension_base worker") */
	set_ps_display(psprintf("(pg_lake_timeseries maintenance for database %d)",
							MyDatabaseId));

	/*
	 * The table list and the SPI plans of a pass are allocated in a context
	 * that outlives the per-table transactions, and reset after every pass.
	 */
	MemoryContext loopContext = AllocSetContextCreate(CacheMemoryContext,
													  "pg_lake_timeseries maintenance",
													  ALLOCSET_DEFAULT_SIZES);

	MemoryContextSwitchTo(loopContext);

	while (!TerminationRequested)
	{
		/*
		 * Transaction start and commit leave CurrentMemoryContext pointing at
		 * a context of their own, so it is reset to the loop context on every
		 * iteration: the table list has to outlive the transaction it was
		 * read in.
		 */
		MemoryContextSwitchTo(loopContext);

		if (EnablePgLakeTimeseries)
			MaintainAllTables(loopContext);

		MemoryContextReset(loopContext);

		LightSleep(PgLakeTimeseriesNaptimeMs);
	}

	PG_RETURN_VOID();
}


/*
 * MaintainAllTables performs one maintenance pass over all enabled tables.
 *
 * The table list is read in its own transaction, and each table is then
 * maintained in a separate transaction: maintenance of one table can take a
 * while (it writes Iceberg files) and an error on one table should not prevent
 * the others from making progress. The list itself is allocated in
 * resultContext, which outlives those transactions.
 */
static void
MaintainAllTables(MemoryContext resultContext)
{
	List	   *tableNames = NIL;

	START_TRANSACTION();
	{
		tableNames = ListEnabledTables(resultContext);
	}
	END_TRANSACTION();

	ListCell   *tableCell = NULL;

	foreach(tableCell, tableNames)
	{
		char	   *tableName = (char *) lfirst(tableCell);

		CHECK_FOR_INTERRUPTS();

		if (TerminationRequested)
			break;

		MaintainTable(tableName);
	}
}


/*
 * ListEnabledTables returns the names of the enabled time-series tables as
 * quoted, schema-qualified strings, allocated in resultContext.
 *
 * Names rather than OIDs, because the table can be dropped between listing and
 * maintaining it; regclass output would then fail to resolve. A name that no
 * longer exists simply makes timeseries.maintain() raise, which is caught and
 * logged per table.
 */
static List *
ListEnabledTables(MemoryContext resultContext)
{
	List	   *tableNames = NIL;

	SPI_connect();

	int			queryResult = SPI_execute("SELECT parent::text FROM timeseries.tables "
										  "WHERE enabled ORDER BY parent", true, 0);

	if (queryResult != SPI_OK_SELECT)
		ereport(ERROR, (errmsg("could not list pg_lake_timeseries tables")));

	for (uint64 rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull = false;
		Datum		nameDatum = SPI_getbinval(SPI_tuptable->vals[rowIndex],
											  SPI_tuptable->tupdesc, 1, &isNull);

		if (isNull)
			continue;

		MemoryContext spiContext = MemoryContextSwitchTo(resultContext);

		tableNames = lappend(tableNames, text_to_cstring(DatumGetTextPP(nameDatum)));

		MemoryContextSwitchTo(spiContext);
	}

	SPI_finish();

	return tableNames;
}


/*
 * MaintainTable runs timeseries.maintain() for one table in its own
 * transaction, downgrading any error to a WARNING so that the worker keeps
 * running and moves on to the next table.
 */
static void
MaintainTable(char *tableName)
{
	StringInfoData command;

	initStringInfo(&command);
	appendStringInfo(&command, "SELECT timeseries.maintain(%s::regclass)",
					 quote_literal_cstr(tableName));

	START_TRANSACTION();
	{
		SPI_connect();

		if (SPI_execute(command.data, false, 0) != SPI_OK_SELECT)
			ereport(ERROR, (errmsg("could not maintain time-series table %s", tableName)));

		SPI_finish();
	}
	END_TRANSACTION_NO_THROW(WARNING);
}
