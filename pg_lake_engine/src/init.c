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
#include "pg_lake/copy/copy_format.h"
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
#include "pg_extension_base/extension_ids.h"
#include "pg_extension_base/pg_extension_base_ids.h"
#include "pg_lake/pgduck/cache_worker.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/iceberg_validation.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "utils/guc.h"
#include "utils/varlena.h"

PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);
static bool PgLakeStageLocationCheckHook(char **newvalue, void **extra, GucSource source);
static bool PgLakeAllowedAzureHostSuffixesCheckHook(char **newvalue, void **extra,
													GucSource source);

/* pg_lake_engine.enabled setting */
static bool QueryEngineEnabled = true;

/* pg_lake_engine.enable_heavy_asserts setting */
bool		EnableHeavyAsserts = false;

/* pg_lake.stage_location setting */
char	   *PgLakeStageLocation = NULL;

/* pg_lake.allowed_azure_host_suffixes setting */
char	   *PgLakeAllowedAzureHostSuffixes = NULL;

/*
 * pg_lake_engine.jsonb_storage setting.
 *
 * Decides how a jsonb value is encoded when we write it and have no
 * per-column decision to follow.  That is the case for a new iceberg column
 * whose table carries no jsonb_storage option, and for COPY TO a Parquet
 * file, which has no table to carry one.
 *
 * Writes into an existing iceberg column follow the storage type persisted
 * for that column at creation, so changing this setting never reinterprets
 * data already written, and one table can hold jsonb columns in both
 * encodings.  Reads always follow the file: a variant column is surfaced as
 * jsonb whatever this is set to.
 *
 * Known deviation from the Iceberg spec: `variant` is a format-version 3
 * type, but tables written here stay at format-version 2. The resulting
 * metadata is therefore not spec-compliant and another engine may reject or
 * misread it, so these tables should be treated as pg_lake-only for now.
 * Emitting format-version 3 is deliberately left out because it pulls in the
 * rest of the v3 surface (deletion vectors above all).
 */
int			DefaultJsonbStorage = JSONB_STORAGE_STRING;

/* pg_lake_engine.jsonb_storage */
static const struct config_enum_entry JsonbStorageOptions[] = {
	{"string", JSONB_STORAGE_STRING, false},
	{"variant", JSONB_STORAGE_VARIANT, false},
	{NULL, 0, false},
};


/*
 * JsonbStorageName returns the canonical lowercase name of a jsonb storage
 * encoding, as accepted by the GUC and the per-table option.
 */
const char *
JsonbStorageName(JsonbStorage storage)
{
	for (int i = 0; JsonbStorageOptions[i].name != NULL; i++)
	{
		if (JsonbStorageOptions[i].val == (int) storage)
			return JsonbStorageOptions[i].name;
	}

	elog(ERROR, "unexpected jsonb storage %d", (int) storage);
}


/*
 * ParseJsonbStorage maps an option string to a JsonbStorage, erroring on
 * anything the GUC would not accept either.  NULL means "unset", which the
 * caller resolves; it is not a valid spelling of the default.
 */
JsonbStorage
ParseJsonbStorage(const char *optionValue)
{
	for (int i = 0; JsonbStorageOptions[i].name != NULL; i++)
	{
		if (pg_strcasecmp(optionValue, JsonbStorageOptions[i].name) == 0)
			return (JsonbStorage) JsonbStorageOptions[i].val;
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid value for jsonb_storage: \"%s\"", optionValue),
			 errhint("Valid values are \"string\" and \"variant\".")));
}


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
							 "pg_lake_engine.log_engine_errors",
							 gettext_noop("Log a canned class for query-engine errors "
										  "(no DuckDB or query text)."),
							 NULL,
							 &LogPGDuckEngineErrors,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	DefineCustomEnumVariable("pg_lake_engine.jsonb_storage",
							 gettext_noop("How jsonb values are encoded in data files we "
										  "write: as `string` (the default) or as the "
										  "Iceberg/Parquet `variant` type."),
							 gettext_noop("Consulted for a new iceberg column whose table "
										  "has no jsonb_storage option, and for COPY TO a "
										  "Parquet file. Writes into an existing iceberg "
										  "column follow the storage type persisted for that "
										  "column, and a variant column is always read back "
										  "as jsonb. WARNING: `variant` is a format-version 3 "
										  "Iceberg type but these tables stay at "
										  "format-version 2, so the metadata is not "
										  "spec-compliant and other engines may reject or "
										  "misread it; treat such tables as pg_lake-only."),
							 &DefaultJsonbStorage,
							 JSONB_STORAGE_STRING,
							 JsonbStorageOptions,
							 PGC_USERSET,
							 0,
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
							gettext_noop("Retries are spaced by "
										 "pg_lake_engine.vacuum_file_remove_retry_interval, so this "
										 "bounds how long we keep trying a file rather than how "
										 "many vacuum passes happen to reach it."),
							&VacuumFileRemoveMaxRetries,
							145 /* At the default retry interval, we try for
							  * at least 1 day */ ,
							0,
							INT32_MAX,
							PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_engine.vacuum_file_remove_retry_interval",
							gettext_noop("The minimum time to wait before vacuum tries to remove "
										 "a file that it failed to remove before."),
							gettext_noop("A file that cannot be removed is otherwise retried by "
										 "every vacuum pass that reaches it, which spends "
										 "pg_lake_engine.vacuum_file_remove_max_retries at "
										 "whatever rate those passes happen to run at. Set to 0 "
										 "to retry on every pass."),
							&VacuumFileRemoveRetryInterval,
							600 /* the default
							  * pg_lake_iceberg.autovacuum_naptime */ ,
							0,
							INT32_MAX,
							PGC_SUSET,
							GUC_UNIT_S | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
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

	DefineCustomIntVariable("pg_lake_engine.max_parallel_file_uploads",
							gettext_noop("Maximum number of concurrent file uploads to "
										 "object storage."),
							NULL,
							&MaxParallelFileUploads,
							DEFAULT_MAX_PARALLEL_FILE_UPLOADS /* default */ ,
							1,
							256,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

	DefineCustomStringVariable(
							   "pg_lake.stage_location",
							   gettext_noop("Base URL for @STAGE/ resolution in paths"),
							   NULL,
							   &PgLakeStageLocation,
							   NULL,
							   PGC_SUSET,
							   0,
							   PgLakeStageLocationCheckHook,
							   NULL, NULL);

	DefineCustomStringVariable(
							   "pg_lake.allowed_azure_host_suffixes",
							   gettext_noop("Comma-separated list of host suffixes a user-supplied "
											"Azure URL may name a storage endpoint under"),
							   gettext_noop("Azure URLs carry the storage endpoint in the host, so "
											"an unrestricted host is an SSRF vector.  An empty list "
											"rejects every URL that names a host; the endpoint from "
											"the Azure secret is always allowed."),
							   &PgLakeAllowedAzureHostSuffixes,
							   DEFAULT_ALLOWED_AZURE_HOST_SUFFIXES,
							   PGC_SUSET,
							   GUC_LIST_INPUT,
							   PgLakeAllowedAzureHostSuffixesCheckHook,
							   NULL, NULL);

	if (QueryEngineEnabled)
	{
		InitializePgLakeEngineIdCache();
		InitializePgMapIdCache();
		InitializePgLakeSpatialIdCache();
		InitializePgLakeBenchmarkIdCache();
		InitializePgExtensionBaseCache();
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


/*
 * PgLakeStageLocationCheckHook validates the pg_lake.stage_location GUC value.
 */
static bool
PgLakeStageLocationCheckHook(char **newvalue, void **extra, GucSource source)
{
	char	   *newStageLocation = *newvalue;

	if (newStageLocation == NULL)
	{
		/* stage location not set */
		return true;
	}

	if (!IsSupportedURL(newStageLocation))
	{
		GUC_check_errdetail("pg_lake.stage_location must be a valid cloud storage URL "
							"(s3://, gs://, gcs://, az://, azure://, or abfss://)");
		return false;
	}

	/* Reject HTTP/HTTPS URLs for stage location (only cloud storage allowed) */
	if (strncmp(newStageLocation, HTTP_URL_PREFIX, strlen(HTTP_URL_PREFIX)) == 0 ||
		strncmp(newStageLocation, HTTPS_URL_PREFIX, strlen(HTTPS_URL_PREFIX)) == 0)
	{
		GUC_check_errdetail("pg_lake.stage_location must be a valid cloud storage URL "
							"(s3://, gs://, gcs://, az://, azure://, or abfss://)");
		return false;
	}

	if (strchr(newStageLocation, '?') != NULL)
	{
		GUC_check_errdetail("pg_lake.stage_location cannot contain query parameters (?)");
		return false;
	}

	return true;
}


/*
 * PgLakeAllowedAzureHostSuffixesCheckHook validates the
 * pg_lake.allowed_azure_host_suffixes GUC value.  The suffix list is read on
 * every user-supplied Azure URL, so reject a list that does not parse here
 * rather than at the first URL that needs it.
 */
static bool
PgLakeAllowedAzureHostSuffixesCheckHook(char **newvalue, void **extra, GucSource source)
{
	if (*newvalue == NULL)
		return true;

	char	   *suffixList = pstrdup(*newvalue);
	List	   *suffixes = NIL;
	bool		parsed = SplitIdentifierString(suffixList, ',', &suffixes);

	if (!parsed)
		GUC_check_errdetail("pg_lake.allowed_azure_host_suffixes must be a "
							"comma-separated list of host suffixes");

	list_free(suffixes);
	pfree(suffixList);

	return parsed;
}
