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
 * pg_lake_ducklake extension entry-point.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include <limits.h>

#include "pg_lake/ducklake/catalog.h"

PG_MODULE_MAGIC;

/* GUC variables */
char	   *PGDLLEXPORT DucklakeDefaultLocationPrefix = NULL;
bool		DucklakeAutovacuumEnabled = true;
int			DucklakeAutovacuumNaptime = 10 * 60;	/* 10 minutes */
int			DucklakeMaxSnapshotAge = 30 * 60;	/* 30 minutes */
int			DucklakeLogAutovacuumMinDuration = 600000;	/* 10 minutes in ms */

/*
 * Process-local flag set while the snapshot_changes-based DDL replay
 * trigger (or the ducklake_table_insert rename-replay path) applies
 * CREATE / DROP / ALTER FOREIGN TABLE on the PG side after a
 * DuckDB-driven catalog change. pg_lake_table's hooks read this
 * directly via the catalog.h extern and short-circuit so they don't
 * write the same change BACK to lake_ducklake.* (which would create
 * a duplicate version row). Not a GUC -- no user-tunable knob here.
 */
bool		DucklakeInDDLReplay = false;

/* Hook check functions */
static bool DucklakeDefaultLocationCheckHook(char **newvalue, void **extra,
											 GucSource source);

/* function declarations */
void		_PG_init(void);


/*
 * _PG_init is the entry-point for pg_lake_ducklake.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
	{
		/*
		 * Sneakily allow recreation of pg_catalog.ducklake_tables view.
		 */
		SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
						PGC_S_OVERRIDE);
		return;
	}

	DefineCustomStringVariable("pg_lake_ducklake.default_location_prefix",
							   gettext_noop("Default S3/GCS prefix under which DuckLake "
											"tables are stored when CREATE TABLE USING "
											"ducklake omits the location option."),
							   gettext_noop("On the first PG-side CREATE TABLE -- or at "
											"CREATE EXTENSION when this setting is already "
											"set -- the value is persisted as the catalog's "
											"data_path metadata row. After that the "
											"persisted row wins: changing the GUC will not "
											"redirect new writes (existing relative paths "
											"stay anchored to the original prefix), it only "
											"affects catalogs that have not yet seeded a "
											"data_path. To redirect a catalog that already "
											"has data, UPDATE the data_path row directly "
											"and (if the prefix moved) move the underlying "
											"object-store data."),
							   &DucklakeDefaultLocationPrefix,
							   NULL,
							   PGC_SUSET,
							   0,
							   DucklakeDefaultLocationCheckHook, NULL, NULL);

	DefineCustomBoolVariable("pg_lake_ducklake.autovacuum",
							 gettext_noop("Global on/off switch for the DuckLake autovacuum process."),
							 NULL,
							 &DucklakeAutovacuumEnabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_ducklake.autovacuum_naptime",
							"Time between DuckLake autovacuum runs.",
							NULL,
							&DucklakeAutovacuumNaptime,
							10 * 60, 1, INT_MAX / 1000,
							PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_ducklake.max_snapshot_age",
							gettext_noop("The default maximum age of snapshots in seconds to retain."),
							NULL,
							&DucklakeMaxSnapshotAge,
							30 * 60,	/* 30 minutes */
							0,
							INT32_MAX,
							PGC_SUSET,
							GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_ducklake.log_autovacuum_min_duration",
							"Minimum duration in milliseconds to log autovacuum "
							"operations for DuckLake tables.",
							NULL,
							&DucklakeLogAutovacuumMinDuration,
							600000, -1, INT_MAX,
							PGC_SIGHUP, GUC_UNIT_MS,
							NULL, NULL, NULL);
}


static bool
DucklakeDefaultLocationCheckHook(char **newvalue, void **extra, GucSource source)
{
	char	   *newLocationPrefix = *newvalue;

	if (newLocationPrefix == NULL)
	{
		/* default location not set */
		return true;
	}

	/* Check for supported URL prefixes */
	if (strncmp(newLocationPrefix, "s3://", 5) != 0 &&
		strncmp(newLocationPrefix, "gs://", 5) != 0 &&
		strncmp(newLocationPrefix, "az://", 5) != 0 &&
		strncmp(newLocationPrefix, "abfss://", 8) != 0)
	{
		GUC_check_errdetail("pg_lake_ducklake: only s3://, gs://, az://, and abfss:// URLs are "
							"supported as the default location prefix");
		return false;
	}

	if (strchr(newLocationPrefix, '?') != NULL)
	{
		GUC_check_errdetail("pg_lake_ducklake: configuration parameters are not allowed "
							"in the default location prefix");
		return false;
	}

	return true;
}
