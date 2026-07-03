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
 * pg_lake_timeseries extension entry-point.
 *
 * pg_lake_timeseries turns an Iceberg table into a live, indexed, mutable
 * time-series table by keeping a small heap "delta" of recent writes and
 * merging it over the Iceberg "base" at query time (merge-on-read, model A).
 *
 * This file is a skeleton: it wires up the module and its GUCs. The background
 * worker that drives frontier maintenance / flushing, and the CustomScan that
 * performs efficient per-partition reconciliation, are specified in DESIGN.md
 * and land in follow-up work.
 */
#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

#define GUC_STANDARD 0

PG_MODULE_MAGIC;

/* GUCs */
bool		EnablePgLakeTimeseries = true;
int			PgLakeTimeseriesNaptimeMs = 10000;

/* function declarations */
void		_PG_init(void);

/*
 * _PG_init is the entry-point for pg_lake_timeseries which is called on
 * postmaster start-up when pg_lake_timeseries is in shared_preload_libraries.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pg_lake_timeseries can only be loaded via shared_preload_libraries"),
						errhint("Add pg_lake_timeseries to shared_preload_libraries configuration "
								"variable in postgresql.conf")));
	}

	DefineCustomBoolVariable(
							 "pg_lake_timeseries.enable",
							 gettext_noop("Enables pg_lake_timeseries background maintenance"),
							 NULL,
							 &EnablePgLakeTimeseries,
							 true,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomIntVariable(
							"pg_lake_timeseries.maintenance_naptime",
							gettext_noop("Time between background maintenance passes, in milliseconds"),
							gettext_noop("Each pass pre-creates the delta frontier, drains the "
										 "DEFAULT partition, and flushes aged delta into Iceberg."),
							&PgLakeTimeseriesNaptimeMs,
							10000, 100, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	/*
	 * TODO(pg_lake_timeseries): register the maintenance background worker here
	 * (RegisterBackgroundWorker). The worker loops every maintenance_naptime,
	 * iterating timeseries.tables and calling timeseries.maintain() per table
	 * via SPI. See DESIGN.md, "Frontier & maintenance".
	 */
}
