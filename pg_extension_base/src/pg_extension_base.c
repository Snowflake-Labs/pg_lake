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
 * Pg extension base extension entry-point.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pg_extension_base/base_workers.h"
#include "pg_extension_base/extension_dependencies.h"
#include "utils/guc.h"

#define GUC_STANDARD 0

PG_MODULE_MAGIC;


#if PG_VERSION_NUM < 160000
#error This extension requires Postgres 16 or newer
#endif


/* function declarations */
void		_PG_init(void);

/* initialization function declarations */
void		PgExtensionBasePreloadLibraries(void);
void		InitializeBaseWorkerLauncher(void);

/* settings */
static bool EnablePreloadLibraries;
bool		EnableBaseWorkerLauncher;


/*
 * _PG_init is the entry-point for pg_extension_base which is called on postmaster
 * start-up when pg_extension_base is in shared_preload_libraries.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pg_extension_base can only be loaded via shared_preload_libraries"),
						errhint("Add pg_extension_base to shared_preload_libraries configuration "
								"variable in postgresql.conf")));
	}

	DefineCustomBoolVariable(
							 "pg_extension_base.enable_preload_libraries",
							 gettext_noop("Enables preloading libraries that have the "
										  "#!shared_preload_libraries option in their control file "
										  "to simplify administration (default on)"),
							 NULL,
							 &EnablePreloadLibraries,
							 true,
							 PGC_POSTMASTER,
							 GUC_STANDARD,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_extension_base.enable_extension_dependency_create",
							 gettext_noop("Enables automatic creation of new dependencies "
										  "when updating an extension"),
							 NULL,
							 &EnableExtensionDependencyCreate,
							 true,
							 PGC_SUSET,
							 GUC_STANDARD,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_extension_base.enable_extension_dependency_update",
							 gettext_noop("Enables automatic update of existing dependencies when "
										  "updating an extension"),
							 NULL,
							 &EnableExtensionDependencyUpdate,
							 true,
							 PGC_SUSET,
							 GUC_STANDARD,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_extension_base.enable_base_worker_launcher",
							 gettext_noop("Enables the bae worker launcher, which simplifies "
										  "background worker creation for extensions"),
							 NULL,
							 &EnableBaseWorkerLauncher,
							 true,
							 PGC_POSTMASTER,
							 GUC_STANDARD,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_extension_base.worker_starter_sleep_time",
							"Sets the time in seconds that the worker starter sleeps "
							"in between checking for failed workers.",
							NULL,
							&WorkerStarterSleepTimeSec,
							DEFAULT_WORKER_STARTER_SLEEP_TIME,
							0,
							INT32_MAX,
							PGC_USERSET,
							GUC_UNIT_S | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_extension_base.worker_restart_backoff_initial",
							"Sets the delay before restarting a base worker after its "
							"first failure. The delay doubles on each subsequent "
							"consecutive failure.",
							NULL,
							&WorkerRestartBackoffInitialMs,
							DEFAULT_WORKER_RESTART_BACKOFF_INITIAL_MS,
							0,
							INT32_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_extension_base.worker_restart_backoff_max",
							"Sets the maximum delay before restarting a base worker "
							"that keeps failing.",
							NULL,
							&WorkerRestartBackoffMaxMs,
							DEFAULT_WORKER_RESTART_BACKOFF_MAX_MS,
							0,
							INT32_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_extension_base.worker_restart_healthy_time",
							"Sets how long a base worker must stay up before a later "
							"failure is treated as fresh, resetting the restart "
							"backoff to its initial delay.",
							NULL,
							&WorkerRestartHealthyMs,
							DEFAULT_WORKER_RESTART_HEALTHY_MS,
							0,
							INT32_MAX,
							PGC_SIGHUP,
							GUC_UNIT_MS | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	/*
	 * Install our ProcessUtility hook for ALTER EXTENSION ... UPDATE
	 * dependency walking BEFORE preloading other extensions.  Postgres
	 * ProcessUtility hooks chain LIFO, so when a preloaded extension installs
	 * its own ProcessUtility hook later in PgExtensionBasePreloadLibraries
	 * below, that extension's hook ends up at the head of the chain and runs
	 * first.  That lets a preloaded extension intervene on CREATE / ALTER
	 * EXTENSION (e.g. enforce a permission check or rewrite the statement)
	 * before our hook dispatches to nested CreateExtension /
	 * ExecAlterExtensionStmt calls that would otherwise bypass ProcessUtility
	 * entirely.
	 *
	 * Skipped during pg_upgrade -- the dump-restore script issues the right
	 * ALTER EXTENSION statements itself, and auto-walking dependencies in
	 * that mode would fight with what pg_upgrade is doing.
	 */
	if (!IsBinaryUpgrade)
		InitializeExtensionDependencyInstaller();

	/*
	 * Library preloading runs even during pg_upgrade: pg_upgrade's "checking
	 * for presence of required libraries" phase issues LOAD on every shared
	 * library referenced by the catalog.  Several pg_lake libraries import
	 * symbols from each other (e.g. pg_lake_table from pg_lake_engine), so
	 * the chain-preload has to put pg_lake_engine in memory first or
	 * pg_lake_table's LOAD fails on unresolved symbols.
	 */
	if (EnablePreloadLibraries)
		PgExtensionBasePreloadLibraries();

	/* pg_extension_base functionality below is disabled during upgrade */
	if (IsBinaryUpgrade)
		return;

	if (EnableBaseWorkerLauncher)
	{
		InitializeBaseWorkerLauncher();
	}
}
