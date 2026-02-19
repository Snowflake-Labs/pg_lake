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

/*-------------------------------------------------------------------------
 *
 * PG Extension Base Test Hibernate
 *
 * This test extension demonstrates the hibernate/restart feature.
 * It supports two modes:
 *
 * - Delayed restart: worker runs for 5 seconds, then restarts after 5 seconds
 * - No restart: worker runs for 2 seconds, then stops (can be woken up)
 *
 * The mode is controlled by the pg_extension_base_test_hibernate.no_restart GUC.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

#include "pg_extension_base/base_workers.h"

PG_MODULE_MAGIC;

static bool NoRestart = false;

/* function declarations */
void		_PG_init(void);

/* UDF implementations */
PG_FUNCTION_INFO_V1(pg_extension_base_test_hibernate_main_worker);


/*
 * _PG_init is the entry-point for pg_extension_base_test_hibernate.
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable("pg_extension_base_test_hibernate.no_restart",
							 "Whether to stop without automatic restart",
							 NULL,
							 &NoRestart,
							 false,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);
}


/*
 * pg_extension_base_test_hibernate_main_worker is the main entry-point for the
 * hibernate test worker.
 */
Datum
pg_extension_base_test_hibernate_main_worker(PG_FUNCTION_ARGS)
{
	int32		workerId = PG_GETARG_INT32(0);

	if (NoRestart)
	{
		elog(LOG, "pg_extension_base_test_hibernate worker %d started (no restart mode)", workerId);
		elog(LOG, "pg_extension_base_test_hibernate worker %d sleeping for 2 seconds", workerId);

		pg_usleep(2000000);

		elog(LOG, "pg_extension_base_test_hibernate worker %d stopping", workerId);

		PG_RETURN_INT64(BASE_WORKER_NO_RESTART);
	}
	else
	{
		elog(LOG, "pg_extension_base_test_hibernate worker %d started", workerId);
		elog(LOG, "pg_extension_base_test_hibernate worker %d sleeping for 5 seconds", workerId);

		pg_usleep(5000000);

		elog(LOG, "pg_extension_base_test_hibernate worker %d restarting in 5 seconds", workerId);

		PG_RETURN_INT64(5000);
	}
}
