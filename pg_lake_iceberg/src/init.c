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
 * pg_lake_iceberg extension entry-point.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"

#include "pg_lake/avro/avro_init.h"
#include "pg_lake/avro/avro_reader.h"
#include "pg_lake/avro/avro_writer.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/iceberg/operations/vacuum.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"

#define GUC_STANDARD 0

PG_MODULE_MAGIC;

/* managed via pg_lake_iceberg.autovacum*/
bool		IcebergAutovacuumEnabled = true;

/* managed via pg_lake_iceberg.autovacuum_naptime, 10 minutes */
int			IcebergAutovacuumNaptime = 10 * 60;

/* managed via pg_lake_iceberg.log_autovacuum_min_duration, 10 minutes */
int			IcebergAutovacuumLogMinDuration = 600000;

static bool IcebergDefaultLocationCheckHook(char **newvalue, void **extra, GucSource source);

/* function declarations */
void		_PG_init(void);


/*
 * _PG_init is the entry-point for pg_lake_iceberg.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
	{
		/*
		 * Sneakily allow recreation of pg_catalog.iceberg_tables view.
		 */
		SetConfigOption("allow_system_table_mods", "true", PGC_POSTMASTER,
						PGC_S_OVERRIDE);
		return;
	}

	DefineCustomBoolVariable(
							 "pg_lake_iceberg.autovacuum",
							 gettext_noop("Similar to Postgres' autovacuum, global on/off switch for the autovacuum process."),
							 NULL,
							 &IcebergAutovacuumEnabled,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_iceberg.autovacuum_naptime",
							"Similar to the autovacuum_naptime but for pg_lake_iceberg tables.",
							NULL,
							&IcebergAutovacuumNaptime,
							10 * 60, 1, INT_MAX / 1000,
							PGC_SIGHUP, GUC_UNIT_S,
							NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_iceberg.enable_object_store_catalog",
							 gettext_noop("Determines whether object storage catalog is enabled."),
							 NULL,
							 &EnableObjectStoreCatalog,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lake_iceberg.external_object_store_catalog_prefix",
							   gettext_noop("Specifies the prefix used for the object store catalog files for external tables."),
							   NULL,
							   &ExternalObjectStorePrefix,
							   "fromsf",
							   PGC_SIGHUP,
							   GUC_NO_SHOW_ALL | GUC_SUPERUSER_ONLY | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lake_iceberg.internal_object_store_catalog_prefix",
							   gettext_noop("Specifies the prefix used for the object store catalog files for internal tables."),
							   NULL,
							   &InternalObjectStorePrefix,
							   "frompg",
							   PGC_SIGHUP,
							   GUC_NO_SHOW_ALL | GUC_SUPERUSER_ONLY | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	/* similar to Postgres' log_autovacuum_min_duration */
	DefineCustomIntVariable("pg_lake_iceberg.log_autovacuum_min_duration",
							"Minimum duration in milliseconds to log autovacuum "
							"operations for pg_lake_iceberg tables.",
							NULL,
							&IcebergAutovacuumLogMinDuration,
							600000, -1, INT_MAX,
							PGC_SIGHUP, GUC_UNIT_MS,
							NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_iceberg.enable_stats_collection_for_nested_types",
							 gettext_noop("When set to true, stats collection is enabled for nested types."
										  "We currently do not support pruning for nested types, but you can "
										  "still get into stats problems with nested types due to parsing "
										  "discrepancies between Postgres and DuckDB."),
							 NULL,
							 &EnableStatsCollectionForNestedTypes,
							 false,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);


	DefineCustomBoolVariable(
							 "pg_lake_iceberg.http_client_trace_traffic",
							 gettext_noop("When set to true, HTTP client logging is enabled."),
							 NULL,
							 &HttpClientTraceTraffic,
							 false,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);


	DefineCustomStringVariable("pg_lake_iceberg.default_location_prefix",
							   gettext_noop("Specifies the default location prefix for "
											"iceberg tables. This is used when the location "
											"option is not specified at \"CREATE FOREIGN "
											"TABLE SERVER pg_lake_iceberg\" statements."),
							   NULL,
							   &IcebergDefaultLocationPrefix,
							   NULL,
							   PGC_SUSET,
							   0,
							   IcebergDefaultLocationCheckHook, NULL, NULL);

	DefineCustomStringVariable("pg_lake_iceberg.object_store_catalog_location_prefix",
							   gettext_noop("Specifies the location prefix for "
											"object store catalog files."),
							   NULL,
							   &ObjectStoreCatalogLocationPrefix,
							   NULL,
							   PGC_SIGHUP,
							   0,
							   NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lake_iceberg.enable_manifest_merge_on_write",
							 "Enables merging manifest files after each data modification.",
							 NULL,
							 &EnableManifestMergeOnWrite,
							 true,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_iceberg.target_manifest_size_kb",
							"Determines the target size of a manifest file during manifest merge.",
							NULL,
							&TargetManifestSizeKB,
							DEFAULT_TARGET_MANIFEST_SIZE_KB,
							0,
							INT_MAX,
							PGC_SUSET,
							GUC_UNIT_KB | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_iceberg.manifest_min_count_to_merge",
							"Determines the minimum number of manifest files in the manifest group "
							"to trigger a merge.",
							NULL,
							&ManifestMinCountToMerge,
							DEFAULT_MANIFEST_MIN_COUNT_TO_MERGE,
							2,
							INT_MAX,
							PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_iceberg.max_snapshot_age",
							gettext_noop("The default maximum age of snapshots in seconds to retain on "
										 "the tables and branches when expiring snapshots."),
							NULL,
							&IcebergMaxSnapshotAge,
							DEFAULT_MAX_SNAPSHOT_AGE,
							0,
							INT32_MAX,
							PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_iceberg.default_avro_writer_block_size_kb",
							"Determines the default block size in kilobytes for the Avro writer.",
							NULL,
							&DefaultAvroWriterBlockSize,
							DEFAULT_AVRO_WRITER_BLOCK_SIZE,
							0,
							INT_MAX,
							PGC_SUSET,
							GUC_UNIT_KB,
							NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lake_iceberg.rest_catalog_host",
							   NULL,
							   NULL,
							   &RestCatalogHost,
							   "http://localhost:8181",
							   PGC_SUSET,
							   GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

    DefineCustomStringVariable("pg_lake_iceberg.rest_catalog_oauth_host",
							   NULL,
							   NULL,
							   &RestCatalogOauthHost,
							   "http://localhost:8181",
							   PGC_SUSET,
							   GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

    DefineCustomStringVariable("pg_lake_iceberg.rest_catalog_oauth_path",
							   NULL,
							   NULL,
							   &RestCatalogOauthPath,
							   "%s/api/catalog/v1/oauth/tokens",
							   PGC_SUSET,
							   GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lake_iceberg.rest_catalog_client_id",
							   NULL,
							   NULL,
							   &RestCatalogClientId,
							   NULL,
							   PGC_SUSET,
							   GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	DefineCustomStringVariable("pg_lake_iceberg.rest_catalog_client_secret",
							   NULL,
							   NULL,
							   &RestCatalogClientSecret,
							   NULL,
							   PGC_SUSET,
							   GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							   NULL, NULL, NULL);

	AvroInit();
}


static bool
IcebergDefaultLocationCheckHook(char **newvalue, void **extra, GucSource source)
{
	char	   *newLocationPrefix = *newvalue;

	if (newLocationPrefix == NULL)
	{
		/* default location not set */
		return true;
	}

	if (!IsSupportedURL(newLocationPrefix))
	{
		GUC_check_errdetail("pg_lake_iceberg: only s3:// and gs:// URLs are currently "
							"supported as the default location prefix");
		return false;
	}

	if (strchr(newLocationPrefix, '?') != NULL)
	{
		GUC_check_errdetail("pg_lake_iceberg: s3 configuration parameters are not allowed "
							"in the default location prefix");
		return false;
	}

	return true;
}
