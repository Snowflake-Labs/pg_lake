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
 * Required extension entry-point.
 */
#include <limits.h>

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/extensions/btree_gist.h"
#include "pg_lake/extensions/pg_lake_benchmark.h"
#include "pg_lake/extensions/pg_extension_base.h"
#include "pg_lake/extensions/pg_lake_copy.h"
#include "pg_lake/extensions/pg_lake.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/extensions/pg_map.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/extensions/pg_lake_replication.h"
#include "pg_lake/extensions/pg_parquet.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/extensions/extension_ids.h"
#include "pg_lake/pgduck/cache_worker.h"
#include "pg_lake/pgduck/client.h"
#include "utils/guc.h"

PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);

/* pg_lake_engine.enabled setting */
static bool QueryEngineEnabled = true;

/* pg_lake_engine.enable_heavy_asserts setting */
bool		EnableHeavyAsserts = false;


/*
 * _PG_init is the entry-point for the library.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;

	DefineCustomStringVariable(
							   "pg_lake_engine.host",
							   gettext_noop("Specifies the pg_lake engine host"),
							   NULL,
							   &PgduckServerConninfo,
							   DEFAULT_PGDUCK_SERVER_CONNINFO,
							   PGC_POSTMASTER,
							   GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_engine.enable_cache_manager",
							 gettext_noop("When enabled, a background worker will "
										  "automatically manage the query engine "
										  "cache"),
							 NULL,
							 &EnableCacheManager,
							 true,
							 PGC_SUSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable(
							"pg_lake_engine.max_cache_size",
							gettext_noop("The cache manager will ensure the cache "
										 "remains under this size."),
							NULL,
							&MaxCacheSizeMB,
							MAX_CACHE_SIZE_MB_DEFAULT, 0, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MB,
							NULL, NULL, NULL);

	DefineCustomIntVariable(
							"pg_lake_engine.cache_manager_interval",
							gettext_noop("Configures the frequency with which the "
										 "cache manager runs by specifying the delay "
										 "between runs."),
							NULL,
							&CacheManagerIntervalMs,
							CACHE_MANAGER_INTERVAL_MS_DEFAULT, 1, INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MS,
							NULL, NULL, NULL);


	DefineCustomBoolVariable(
							 "pg_lake_engine.enabled",
							 gettext_noop("Global on/off switch for pg_lake_engine "
										  "hooks"),
							 NULL,
							 &QueryEngineEnabled,
							 true,
							 PGC_POSTMASTER,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_engine.enable_heavy_asserts",
							 gettext_noop("Computationally heavy asserts for the pg_lake. "
										  "This should only be used in "
										  "development and testing while USE_ASSERT_CHECKING "
										  "is enabled."),
							 NULL,
							 &EnableHeavyAsserts,
							 false,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_engine.vacuum_file_remove_max_retries",
							gettext_noop("The maximum number of retries to remove a file "
										 "with vacuum. Once this number of retries is reached, "
										 "the file will be removed from the deletion queue and "
										 "won't be retried to remove."),
							NULL,
							&VacuumFileRemoveMaxRetries,
							145 /* With the default vacuum naptime, we try up
							  * to 1 day */ ,
							0,
							INT32_MAX,
							PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	/*
	 * Currently only applied to pg_lake_iceberg tables, but we want to keep
	 * the GUC here so that we could apply it to other tables in the future.
	 */
	DefineCustomIntVariable("pg_lake_engine.orphaned_file_retention_period",
							gettext_noop("The default maximum age of remote files in seconds to retain on "
										 "the remote storage. This period is applied after the file is "
										 "no longer referenced by any table/snapshot."),
							NULL,
							&OrphanedFileRetentionPeriod,
							60 * 60 * 24 * 10 /* 10 days */ ,
							0,
							INT32_MAX,
							PGC_SUSET,
							GUC_UNIT_S | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	if (QueryEngineEnabled)
	{
		InitializePgLakeEngineIdCache();
		InitializePgMapIdCache();
		InitializePgLakeSpatialIdCache();
		InitializePgLakeBenchmarkIdCache();
		InitializePGExtensionBaseCache();
		InitializePgLakeIdCache();
		InitializePgLakeTableIdCache();
		InitializePgLakeIcebergIdCache();
		InitializePgLakeCopyIdCache();
		InitializePgParquetIdCache();
		InitializePgLakeReplicationIdCache();
		InitializePostgisIdCache();
		InitializeBtreeGistIdCache();
		InitializeUtilityHook();
		StartPGDuckCacheWorker();
	}
}
