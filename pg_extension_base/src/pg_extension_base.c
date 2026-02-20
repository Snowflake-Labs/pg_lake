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


#if PG_VERSION_NUM < 150000
#error This extension requires Postgres 15 or newer
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

	if (EnableBaseWorkerLauncher)
	{
		InitializeBaseWorkerLauncher();
	}

	if (EnablePreloadLibraries)
		PgExtensionBasePreloadLibraries();

	/* pg_extension_base functionality below is disabled during upgrade */
	if (IsBinaryUpgrade)
		return;

	InitializeExtensionDependencyInstaller();
}
