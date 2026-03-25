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
 * pg_lake_log extension entry-point.
 *
 * Responsibilities:
 *   - Validate that we are loaded via shared_preload_libraries.
 *   - Register all GUCs.
 *   - Hook shmem_request_hook and shmem_startup_hook to set up the ring buffer.
 *   - Install the emit_log_hook.
 *   - Register the background flush worker.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "storage/shmem.h"
#include "utils/elog.h"
#include "utils/guc.h"

#include "pg_lake_log/log_buffer.h"
#include "pg_lake_log/log_worker.h"

PG_MODULE_MAGIC;

/* ----------------------------------------------------------------
 * GUC variables (declared extern in log_worker.h / log_hook.c)
 * ---------------------------------------------------------------- */

/* Target Iceberg table (schema-qualified or unqualified). */
char	   *PgLakeLogTargetTable = NULL;

/* Database the worker connects to. */
char	   *PgLakeLogDatabase = NULL;

/* Maximum rows sent to Iceberg per flush cycle. */
int			PgLakeLogBatchSize = 1000;

/* Milliseconds the worker sleeps between flush cycles. */
int			PgLakeLogFlushIntervalMs = 5000;

/* Master on/off switch. */
bool		PgLakeLogEnabled = true;

/*
 * Minimum PostgreSQL error level to capture.
 * Default WARNING (19).  Users can lower this to LOG (15) or INFO (17)
 * for more verbose capture.
 */
int			PgLakeLogMinElevel = WARNING;

/* ----------------------------------------------------------------
 * Hook state
 * ---------------------------------------------------------------- */
static shmem_request_hook_type PrevShmemRequestHook = NULL;
static shmem_startup_hook_type PrevShmemStartupHook = NULL;

/* Declared in log_hook.c */
extern emit_log_hook_type PrevEmitLogHook;
extern void PgLakeLogHook(ErrorData *edata);

/* ----------------------------------------------------------------
 * Forward declarations
 * ---------------------------------------------------------------- */
void		_PG_init(void);
static void PgLakeLogShmemRequest(void);
static void PgLakeLogShmemStartup(void);

/* Config enum entries for pg_lake_log.min_severity */
static const struct config_enum_entry MinSeverityOptions[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"log", LOG, false},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{NULL, 0, false},
};


/*
 * _PG_init is called when the library is loaded at postmaster startup.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
		ereport(ERROR,
				(errmsg("pg_lake_log must be loaded via shared_preload_libraries"),
				 errhint("Add pg_lake_log to shared_preload_libraries in postgresql.conf.")));

	/* ---- GUCs ---- */

	DefineCustomBoolVariable(
							 "pg_lake_log.enabled",
							 gettext_noop("Enable or disable pg_lake_log log capture."),
							 NULL,
							 &PgLakeLogEnabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "pg_lake_log.target_table",
							   gettext_noop("Schema-qualified name of the Iceberg table "
											"that receives captured log entries."),
							   gettext_noop("The table must have columns: "
											"log_time timestamptz, pid int, severity text, "
											"message text, detail text, context text."),
							   &PgLakeLogTargetTable,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "pg_lake_log.database",
							   gettext_noop("Name of the database the flush worker connects to."),
							   NULL,
							   &PgLakeLogDatabase,
							   "postgres",
							   PGC_POSTMASTER,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable(
							"pg_lake_log.batch_size",
							gettext_noop("Maximum number of log entries written to the "
										 "Iceberg table in a single flush cycle."),
							NULL,
							&PgLakeLogBatchSize,
							1000, 1, 100000,
							PGC_SIGHUP,
							0,
							NULL, NULL, NULL);

	DefineCustomIntVariable(
							"pg_lake_log.flush_interval",
							gettext_noop("How long the flush worker sleeps between "
										 "flush cycles."),
							NULL,
							&PgLakeLogFlushIntervalMs,
							5000, 100, INT_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomEnumVariable(
							 "pg_lake_log.min_severity",
							 gettext_noop("Minimum log severity level to capture. "
										  "Messages below this level are ignored."),
							 NULL,
							 &PgLakeLogMinElevel,
							 WARNING,
							 MinSeverityOptions,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	/* ---- Shared memory ---- */

	PrevShmemRequestHook = shmem_request_hook;
	shmem_request_hook = PgLakeLogShmemRequest;

	PrevShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = PgLakeLogShmemStartup;

	/* ---- Log hook ---- */

	PrevEmitLogHook = emit_log_hook;
	emit_log_hook = PgLakeLogHook;

	/* ---- Background worker ---- */

	StartLogFlushWorker();
}


/*
 * PgLakeLogShmemRequest asks the postmaster to reserve space for our ring
 * buffer before shared memory is allocated.
 */
static void
PgLakeLogShmemRequest(void)
{
	if (PrevShmemRequestHook != NULL)
		PrevShmemRequestHook();

	RequestAddinShmemSpace(LogBufferShmemSize());
}


/*
 * PgLakeLogShmemStartup attaches to the shared-memory segment once it has
 * been allocated by the postmaster.
 */
static void
PgLakeLogShmemStartup(void)
{
	if (PrevShmemStartupHook != NULL)
		PrevShmemStartupHook();

	LogBufferShmemInit();
}
