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
 * pg_lake_copy extension entry-point.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "pg_lake/ddl/alter_table.h"
#include "pg_lake/ddl/create_table.h"
#include "pg_lake/ddl/drop_table.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/ddl/vacuum.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/fdw/data_file_pruning.h"
#include "pg_lake/fdw/shippable.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/multi_data_file_dest.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/partitioning/partitioned_dest_receiver.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/planner/extensible_nodes.h"
#include "pg_lake/planner/insert_select.h"
#include "pg_lake/planner/query_pushdown.h"
#include "pg_lake/util/s3_file_utils.h"
#include "pg_lake/test/hide_lake_objects.h"
#include "pg_lake/transaction/transaction_hooks.h"
#include "utils/guc.h"

#define GUC_STANDARD 0

PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);

static bool CheckTargetFileSizeMB(int *newval, void **extra, GucSource source);


static const struct config_enum_entry LogLevelOptions[] = {
	{"debug5", DEBUG5, false},
	{"debug4", DEBUG4, false},
	{"debug3", DEBUG3, false},
	{"debug2", DEBUG2, false},
	{"debug1", DEBUG1, false},
	{"debug", DEBUG2, true},
	{"info", INFO, false},
	{"notice", NOTICE, false},
	{"warning", WARNING, false},
	{"error", ERROR, false},
	{"log", LOG, false},
	{NULL, 0, false}
};


/* pg_lake_table.default_parquet_version */
static const struct config_enum_entry ParquetVersionOptions[] = {
	{"v1", PARQUET_VERSION_V1, false},
	{"v2", PARQUET_VERSION_V2, false},
	{NULL, 0, false},
};


/*
 * Module load callback
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;

	DefineCustomBoolVariable("pg_lake_table.enable_strict_pushdown",
							 "Enables pg_lake_table extension to "
							 "pushdown only safe operators, functions and types to the pgduck server",
							 NULL,
							 &EnableStrictPushdown,
							 true,
							 PGC_USERSET,
							 GUC_STANDARD,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_lake_table.enable_full_query_pushdown",
							 "Enables pg_lake_table extension to "
							 "push down the full query if all the operators can be "
							 "pushed down and the query contains only "
							 "pg_lake tables.",
							 NULL,
							 &EnableFullQueryPushdown,
							 true,
							 PGC_USERSET,
							 GUC_STANDARD,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_lake_table.enable_insert_select_pushdown",
							 "Enables pg_lake_table extension to "
							 "push down the INSERT..SELECT queries into Iceberg "
							 "if all the operators can be pushed down and the query "
							 "contains only pg_lake tables.",
							 NULL,
							 &EnableInsertSelectPushdown,
							 true,
							 PGC_USERSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_lake_table.enable_data_file_pruning",
							 "Enables data file pruning based on the metadata statistics "
							 "for iceberg tables.",
							 NULL,
							 &EnableDataFilePruning,
							 true,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomBoolVariable("pg_lake_table.enable_partition_pruning",
							 "Enables partition pruning based on the partition values "
							 "for iceberg tables.",
							 NULL,
							 &EnablePartitionPruning,
							 true,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL,
							 NULL,
							 NULL);


	DefineCustomIntVariable("pg_lake_table.copy_on_write_threshold",
							"Determines the percentage of deleted rows in a file "
							"after which we use copy-on-write instead of merge-on-read "
							"for deletion. The default is 20%, the minimum of 0% implies "
							"always using copy-on-write, and 100% implies always using "
							"merge-on-read.",
							NULL,
							&CopyOnWriteThreshold,
							20,
							0,
							100,
							PGC_USERSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_lake_table.target_file_size_mb",
							"Determines the target size of files in writable tables. "
							"Files larger than this size will be split during insertion "
							"or compaction. The default is 512 (MB) and a value of less "
							"than 1 disables splitting.",
							NULL,
							&TargetFileSizeMB,
							DEFAULT_TARGET_FILE_SIZE_MB,
							-1,
							INT_MAX,
							PGC_USERSET,
							GUC_UNIT_MB,
							CheckTargetFileSizeMB,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_lake_table.target_row_group_size_mb",
							"Determines the target size of row groups in writable tables, in MB. "
							"The default is 512; set to 0 to disable. ",
							NULL,
							&TargetRowGroupSizeMB,
							DEFAULT_TARGET_ROW_GROUP_SIZE_MB,
							0,
							INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	DefineCustomEnumVariable("pg_lake_table.default_parquet_version",
							 gettext_noop("Default parquet version when writing the parquet file."),
							 NULL,
							 &DefaultParquetVersion,
							 PARQUET_VERSION_V1,
							 ParquetVersionOptions,
							 PGC_SUSET, 0,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_table.vacuum_compact_min_input_files",
							"Similar to Spark's min-input-files parameter on rewrite_data_files(), "
							"any file group exceeding this number of files will be compacted regardless "
							"of other criteria.",
							NULL,
							&VacuumCompactMinInputFiles,
							DEFAULT_MIN_INPUT_FILES,
							1,
							INT_MAX,
							PGC_USERSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_lake_table.max_write_temp_file_size_mb",
							"Determines the the maximum temporary file size during "
							"writes. If a file exceeds this threshold, it will be "
							"converted to one or more data files in S3 and cache. The "
							"files may be evicted from cache before the write is over.",
							NULL,
							&MaxWriteTempFileSizeMB,
							DEFAULT_MAX_WRITE_TEMP_FILE_SIZE_MB,
							-1,
							INT_MAX,
							PGC_SUSET,
							GUC_UNIT_MB | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_lake_table.max_open_files_for_partitioned_write",
							"Determines the maximum number of open files for "
							"partitioned writes. If this limit is reached, currently the "
							"largest partitioned file will be flushed. Lowering "
							"this value would cause pushing smaller files, but decrease the "
							"amount of the total intermediate data size during writes.",
							NULL,
							&MaxOpenFilesForPartitionedWrite,
							5000,
							1,
							INT_MAX,
							PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_lake_table.max_file_removals_per_vacuum",
							"Determines the maximum number of files to remove "
							"during a single vacuum operation.",
							NULL,
							&MaxFileRemovalsPerVacuum,
							100000,
							0,
							INT_MAX,
							PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_table.max_compactions_per_vacuum",
							"Determines the maximum number of compactions "
							"during a single vacuum operation.",
							NULL,
							&MaxCompactionsPerVacuum,
							100,
							1,
							INT_MAX,
							PGC_SUSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lake_table.enable_delete_file_function",
							 "Determines whether lake_file.delete can be used "
							 "to delete a file.",
							 NULL,
							 &EnableDeleteFileFunction,
							 false,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lake_table.skip_drop_access_hook",
							 "Determines whether to skip the hook when dropping "
							 "an object",
							 NULL,
							 &SkipDropAccessHook,
							 false,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_table.unbounded_numeric_default_precision",
							"Determines the default precision for unbounded numeric types "
							"in pg_lake tables.",
							NULL,
							&UnboundedNumericDefaultPrecision,
							UNBOUNDED_NUMERIC_DEFAULT_PRECISION,
							1,
							DUCKDB_MAX_NUMERIC_PRECISION,
							PGC_USERSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomIntVariable("pg_lake_table.unbounded_numeric_default_scale",
							"Determines the default scale for unbounded numeric types "
							"in pg_lake tables.",
							NULL,
							&UnboundedNumericDefaultScale,
							UNBOUNDED_NUMERIC_DEFAULT_SCALE,
							0,
							DUCKDB_MAX_NUMERIC_SCALE,
							PGC_USERSET,
							GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							NULL, NULL, NULL);

	DefineCustomEnumVariable("pg_lake_table.write_log_level",
							 "Determines the log level for reporting file "
							 "additions/removals.",
							 NULL,
							 &WriteLogLevel,
							 LOG,
							 LogLevelOptions,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable("pg_lake_table.hide_objects_created_by_lake",
							 "Hides some objects, which is created by any pg_lake extension, from queries "
							 "with catalog tables. It is intended to be used only before postgres tests, with "
							 "our extensions created, to not break them.",
							 NULL,
							 &HideObjectsCreatedByLake,
							 false,
							 PGC_USERSET,
							 GUC_SUPERUSER_ONLY | GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	MarkGUCPrefixReserved(PG_LAKE_TABLE);

	RegisterUtilityStatementHandler(ProcessVacuumPgLakeTable, NULL);
	RegisterUtilityStatementHandler(ProcessCreatePgLakeTable, NULL);
	RegisterUtilityStatementHandler(ProcessCreateAsSelectPgLakeTable, NULL);
	RegisterUtilityStatementHandler(ProcessAlterTable, NULL);
	RegisterUtilityStatementHandler(ProcessAlterType, NULL);
	RegisterUtilityStatementHandler(ProcessAlterTypeAttributeRename, NULL);
	RegisterUtilityStatementHandler(ProcessEnumStatement, NULL);
	RegisterUtilityStatementHandler(ProcessDropPgLakeTable, NULL);

	/*
	 * Add as the last handler, execute as the first. Don't worry, you'll have
	 * bunch of errors in the tests if you ever mess with the order.
	 */
	RegisterUtilityStatementHandler(ErrorUnsupportedCreatePgLakeTableHandler, NULL);

	RegisterPostUtilityStatementHandler(CreatePgLakeTableCheckUnsupportedFeaturesPostProcess, NULL);
	RegisterPostUtilityStatementHandler(PostProcessRenameWritablePgLakeTable, NULL);
	RegisterPostUtilityStatementHandler(PostProcessAlterWritablePgLakeTableSchema, NULL);

	InitializeDropTableHandler();
	InitializeFullQueryPushdown();

	RegisterPgLakeCustomNodes();

	IcebergRegisterCallbacks();
}


/*
 * CheckTargetFileSizeMB confirms whether the target file size is at least 16MIB,
 * unless the user is superuser.
 */
static bool
CheckTargetFileSizeMB(int *newval, void **extra, GucSource source)
{
	return *newval <= 0 || *newval > MIN_TARGET_FILE_SIZE_MB ||
		(IsTransactionState() && superuser());
}
