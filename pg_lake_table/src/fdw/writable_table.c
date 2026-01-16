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

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_namespace.h"
#include "commands/defrem.h"
#include "common/hashfn.h"
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/catalog/row_id_mappings.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/row_ids.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/delete_data.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/util/string_utils.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"


/* files are compacted if >180% or <50% of TargetFileSizeMB  */
#define MAX_TARGET_FILE_SIZE_RATIO (1.8)
#define MIN_TARGET_FILE_SIZE_RATIO (0.5)
#define MAX_TARGET_FILE_SIZE (MAX_TARGET_FILE_SIZE_RATIO * TargetFileSizeMB * MB_BYTES)
#define MIN_TARGET_FILE_SIZE (MIN_TARGET_FILE_SIZE_RATIO * TargetFileSizeMB * MB_BYTES)

/*
 * Somewhat arbitrary, but we want to avoid millions of single-row inserts
 * getting compacted in one go.
 */
#define MAX_FILES_PER_COMPACTION (1000)

/* start above the locktag classes used in postgres and Citus */
#define ADV_LOCKTAG_CLASS_PG_LAKE_TABLE_UPDATE 101


/*
 * CompactionDataFileHashEntry is used to compact data files of the same
 * partition spec and partition tuple.
 */
typedef struct CompactionDataFileHashEntry
{
	uint64		partitionHash;	/* combined hash of partition spec + partition
								 * tuple */
	List	   *dataFiles;
}			CompactionDataFileHashEntry;


static List *ApplyInsertFile(Relation rel, char *insertFile, int64 rowCount,
							 int64 reservedRowIdStart, int32 partitionSpecId,
							 Partition * partition, DataFileStats * fileStats);
static List *ApplyDeleteFile(Relation rel, char *sourcePath, int64 sourceRowCount,
							 int64 liveRowCount, char *deleteFile, int64 deletedRowCount);
static List *GetDataFilePathsFromStatsList(List *dataFileStats);
static List *GetNewFileOpsFromFileStats(Oid relationId, List *dataFileStats,
										int32 partitionSpecId, Partition * partition, int64 rowCount,
										bool isVerbose, List **newFiles);
static bool ShouldRewriteAfterDeletions(int64 sourceRowCount, uint64 totalDeletedRowCount);
static CompactionDataFileHashEntry * GetPartitionWithMostEligibleFiles(Oid relationId, TimestampTz compactionStartTime,
																	   bool forceMerge);
static HTAB *CreateCompactionDataFileHash(void);
static HTAB *GroupDataFilesByPartition(List *dataFiles, TimestampTz compactionStartTime, bool forceMerge);
static List *FilterCompactionCandidates(List *dataFiles, TimestampTz compactionStartTime, bool forceMerge);
static List *TryCompactDataFiles(Oid relationId, TupleDesc tupleDescriptor, List *candidates,
								 PgLakeTableType tableType, List *options, bool forceMerge, bool isVerbose);
#ifdef USE_ASSERT_CHECKING
static void AssertAllFilesHaveSamePartition(List *dataFiles);
#endif
static List *PrepareToAddQueryResultToTable(Oid relationId,
											char *readQuery,
											TupleDesc queryTupleDesc,
											int32 partitionSpecId,
											Partition * partition,
											bool queryHasRowId,
											bool allowSplit,
											bool isVerbose);
static List *GetPossiblePositionDeleteFiles(Oid relationId, List *sourcePathList,
											Snapshot snapshot);
static void ApplyMetadataChanges(Oid relationId, List *metadataOperations);


/* pg_lake_table.copy_on_write_threshold */
int			CopyOnWriteThreshold = DEFAULT_COPY_ON_WRITE_THRESHOLD;

/* pg_lake_table.target_file_size_mb */
int			TargetFileSizeMB = DEFAULT_TARGET_FILE_SIZE_MB;

/*
 * Similar to Spark's min-input-files parameter on rewrite_data_files(),
 * any file group exceeding this number of files will be compacted regardless
 * of other criteria.
 */
int			VacuumCompactMinInputFiles = DEFAULT_MIN_INPUT_FILES;

/* pg_lake_table.write_log_level */
int			WriteLogLevel = LOG;


/*
 * Writers can choose to defer applying the modifications to the
 * table.
 *
 * (used to defer delete queue flush modifications to combine them
 *  with insert flush modifications in logical replication)
 */
List	   *DeferredModifications = NIL;


/*
 * ApplyInsertFile prepares to add the given insert file to the table.
 *
 * It returns a list of TableMetadataOperation to apply to the table metadata.
 */
static List *
ApplyInsertFile(Relation rel, char *insertFile, int64 rowCount,
				int64 reservedRowIdStart, int32 partitionSpecId,
				Partition * partition, DataFileStats * dataFileStats)
{
	ereport(WriteLogLevel, (errmsg("adding %s with " INT64_FORMAT " rows ",
								   insertFile, rowCount)));

	Oid			relationId = RelationGetRelid(rel);
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	bool		hasRowIds = GetBoolOption(options, "row_ids", false);

	Assert(dataFileStats != NULL);

	List	   *metadataOperations = NIL;

	/* store the new file in the metadata */
	TableMetadataOperation *addOperation =
		AddDataFileOperation(insertFile, CONTENT_DATA, dataFileStats, partition, partitionSpecId);

	metadataOperations = lappend(metadataOperations, addOperation);

	if (hasRowIds && rowCount > 0)
	{
		/* assign new row ID range */
		RowIdRangeMapping *rowIdRange =
			CreateRowIdRangeForNewFile(relationId, rowCount, reservedRowIdStart);

		TableMetadataOperation *rowIdMappingOp =
			AddRowIdMappingOperation(addOperation->path, list_make1(rowIdRange));

		metadataOperations = lappend(metadataOperations, rowIdMappingOp);

		/* set the row_id_start for the new data file */
		addOperation->dataFileStats.rowIdStart = rowIdRange->rowStartId;
	}

	return metadataOperations;
}


/*
 * PrepareCSVInsertion converts a given CSV file to the table's format (e.g. Parquet)
 * the table's location (e.g. s3://mybucket/mytable/).
 *
 * It returns a list of DataFileModifications to apply to the table metadata.
 */
List *
PrepareCSVInsertion(Oid relationId, char *insertCSV, int64 rowCount,
					int64 reservedRowIdStart, int maximumLineSize,
					DataFileSchema * schema)
{
	Relation	relation = table_open(relationId, RowExclusiveLock);
	ForeignTable *foreignTable = GetForeignTable(relationId);
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);

	List	   *options = foreignTable->options;

	CopyDataFormat format;
	CopyDataCompression compression;
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	FindDataFormatAndCompression(tableType, NULL, options, &format, &compression);

	bool		isPrefix = false;

	/*
	 * We currently only support splitting Parquet files, to not complicate
	 * the row counting.
	 */
	bool		splitFilesBySize =
		TargetFileSizeMB > 0 && format == DATA_FORMAT_PARQUET &&
		reservedRowIdStart == 0;

	/*
	 * When target_file_size_mb is non-0 (512MB by default), we use the
	 * file_size_bytes option in DuckDB COPY to split the file.
	 *
	 * The files may not be exactly the desired size; some guess work is
	 * involved.
	 */
	if (splitFilesBySize)
	{
		options = lappend(options, CreateFileSizeBytesOption(TargetFileSizeMB));

		/* extension will be added automatically when using file_size_bytes */
		isPrefix = true;
	}

	char	   *dataFilePrefix = GenerateDataFileNameForTable(relationId, !isPrefix);

	/* we defer deletion of in-progress data files only for Iceberg tables */
	bool		deferDeletion = IsPgLakeIcebergForeignTableById(relationId);

	InsertInProgressFileRecordExtended(dataFilePrefix, isPrefix, deferDeletion);

	List	   *leafFields = GetLeafFieldsForTable(relationId);

	/* convert insert file to a new file in table format */
	StatsCollector *statsCollector =
		ConvertCSVFileTo(insertCSV,
						 tupleDescriptor,
						 maximumLineSize,
						 dataFilePrefix,
						 format,
						 compression,
						 options,
						 schema,
						 leafFields);

	ApplyColumnStatsModeForAllFileStats(relationId, statsCollector->dataFileStats);

	if (!splitFilesBySize && statsCollector->dataFileStats == NIL)
	{
		DataFileStats *stats = palloc0(sizeof(DataFileStats));

		stats->dataFilePath = dataFilePrefix;
		stats->rowCount = rowCount;
		statsCollector->dataFileStats = list_make1(stats);
	}

	/*
	 * when we defer deletion of in-progress files, we need to replace the
	 * prefix paths with full paths. At precommit hook, we delete persisted
	 * files from in-progress
	 */
	if (isPrefix && deferDeletion)
		ReplaceInProgressPrefixPathWithFullPaths(dataFilePrefix, GetDataFilePathsFromStatsList(statsCollector->dataFileStats));

	/* build a DataFileModification for each new data file */
	List	   *modifications = NIL;
	ListCell   *dataFileStatsCell = NULL;

	foreach(dataFileStatsCell, statsCollector->dataFileStats)
	{
		DataFileStats *stats = lfirst(dataFileStatsCell);

		DataFileModification *modification = palloc0(sizeof(DataFileModification));

		modification->type = ADD_DATA_FILE;
		modification->insertFile = stats->dataFilePath;
		modification->insertedRowCount = stats->rowCount;
		modification->reservedRowIdStart = reservedRowIdStart;
		modification->fileStats = stats;

		modifications = lappend(modifications, modification);
	}

	table_close(relation, NoLock);

	return modifications;
}


/*
 * GetDataFileNamesFromStatsList extracts the data file paths from the given
 * DataFileStats list.
 */
static List *
GetDataFilePathsFromStatsList(List *dataFileStats)
{
	List	   *dataFiles = NIL;
	ListCell   *cell = NULL;

	foreach(cell, dataFileStats)
	{
		DataFileStats *stats = lfirst(cell);

		dataFiles = lappend(dataFiles, stats->dataFilePath);
	}

	return dataFiles;
}


/*
 * GetNewFileOpsFromFileStats gets the list of newly written data files (could
 * be multiple when file_size_bytes is specified) with their file stats
 * and adds them to the metadata operations list to be returned.
 */
static List *
GetNewFileOpsFromFileStats(Oid relationId, List *dataFileStats, int32 partitionSpecId, Partition * partition,
						   int64 rowCount, bool isVerbose, List **newFiles)
{
	*newFiles = NIL;

	List	   *metadataOperations = NIL;

	ListCell   *dataFileStatsCell = NULL;

	foreach(dataFileStatsCell, dataFileStats)
	{
		DataFileStats *dataFileStats = lfirst(dataFileStatsCell);

		ereport(isVerbose ? INFO : WriteLogLevel,
				(errmsg("adding %s with " INT64_FORMAT " rows to %s",
						dataFileStats->dataFilePath, dataFileStats->rowCount, get_rel_name(relationId))));

		*newFiles = lappend(*newFiles, dataFileStats->dataFilePath);
		/* store the new file in the metadata */
		TableMetadataOperation *addOperation =
			AddDataFileOperation(dataFileStats->dataFilePath, CONTENT_DATA, dataFileStats, partition, partitionSpecId);

		metadataOperations = lappend(metadataOperations, addOperation);
	}

	return metadataOperations;
}


/*
 * GenerateDataFileNameForTable generates a new file name for the given relation.
 */
char *
GenerateDataFileNameForTable(Oid relationId, bool withExtension)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;

	CopyDataFormat format;
	CopyDataCompression compression;
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	FindDataFormatAndCompression(tableType, NULL, options, &format, &compression);

	const char *fileExtension = "";

	if (withExtension)
	{
		fileExtension = FormatToFileExtension(format, compression);

		if (fileExtension == NULL)
			ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
							errmsg("unsupported format or compression")));
	}

	char	   *dataFileRelativePath = psprintf("%s%s", GenerateUUID(), fileExtension);

	/* if the location has query arguments, we need to append them to the path */
	char	   *queryArguments = "";

	char	   *location = GetWritableTableLocation(relationId, &queryArguments);
	char	   *dataFilePath = NULL;

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		dataFilePath = psprintf("%s/data/%s%s",
								location,
								dataFileRelativePath,
								queryArguments);
	}
	else
	{
		dataFilePath = psprintf("%s/%s%s",
								location,
								dataFileRelativePath,
								queryArguments);
	}

	return dataFilePath;
}


/*
 * ApplyDeleteFile applies the given deletion file to the source file.
 */
static List *
ApplyDeleteFile(Relation rel, char *sourcePath, int64 sourceRowCount, int64 liveRowCount,
				char *deleteFile, int64 deletedRowCount)
{
	if (deletedRowCount == 0)
		return NIL;

	/* cannot have more live rows than source rows */
	Assert(liveRowCount <= sourceRowCount);

	/* cannot have more deleted rows than live rows */
	Assert(deletedRowCount <= liveRowCount);

	Oid			relationId = RelationGetRelid(rel);
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	bool		hasRowIds = GetBoolOption(options, "row_ids", false);

	CopyDataFormat format;
	CopyDataCompression compression;
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	FindDataFormatAndCompression(tableType, NULL, options, &format, &compression);

	List	   *metadataOperations = NIL;

	if (deletedRowCount == liveRowCount)
	{
		/* no rows remaining */
		ereport(WriteLogLevel, (errmsg("removing %s with " INT64_FORMAT " rows ",
									   sourcePath, sourceRowCount)));

		metadataOperations = lappend(metadataOperations,
									 RemoveDataFileOperation(sourcePath));
	}
	else if (deletedRowCount < liveRowCount)
	{
		/* not all rows deleted */
		uint64		totalDeletedRowCount =
			deletedRowCount + sourceRowCount - liveRowCount;

		/* we don't expect empty source files */
		Assert(sourceRowCount > 0);

		/* should have a delete file with the deleted rows */
		Assert(deleteFile != NULL);

		/*
		 * we only do copy-on-write if more than copy_on_write_threshold
		 * percent of the original file was deleted.
		 *
		 * Copy-on-write with row IDs is not yet supported. We'd want to
		 * preserve the row IDs similar to compaction.
		 */
		if (ShouldRewriteAfterDeletions(sourceRowCount, totalDeletedRowCount) &&
			!hasRowIds)
		{
			/*
			 * Copy-on-write deletion. The FDW produces a CSV file with the
			 * file names and positions of deleted rows. We produce a new file
			 * that excludes the deleted rows by (anti-)joining the source
			 * file with CSV file.
			 *
			 * Since there may already have been merge-on-read deletions, we
			 * also filter out deletions from existing position delete files.
			 */

			List	   *existingPositionDeletes =
				GetPossiblePositionDeleteFiles(relationId, list_make1(sourcePath), NULL);

			/* copy-on-write deletion */
			char	   *newDataFilePath = GenerateDataFileNameForTable(relationId, true);

			ereport(WriteLogLevel, (errmsg("removing %s with " INT64_FORMAT " rows ",
										   sourcePath, sourceRowCount)));

			/* remove the old file from the metadata */
			metadataOperations = lappend(metadataOperations,
										 RemoveDataFileOperation(sourcePath));

			DataFileSchema *schema = GetDataFileSchemaForTable(relationId);

			/*
			 * we defer deletion of in-progress data files only for Iceberg
			 * tables
			 */
			bool		isPrefix = false;
			bool		deferDeletion = IsPgLakeIcebergForeignTableById(relationId);

			InsertInProgressFileRecordExtended(newDataFilePath, isPrefix, deferDeletion);

			/* write the remainder of the file into a new path */
			uint64		existingDeletedRowCount = sourceRowCount - liveRowCount;

			ReadDataStats stats = {sourceRowCount, existingDeletedRowCount};

			List	   *leafFields = GetLeafFieldsForTable(relationId);
			StatsCollector *statsCollector = PerformDeleteFromParquet(sourcePath, existingPositionDeletes,
																	  deleteFile, newDataFilePath, compression,
																	  schema, &stats, leafFields);

			ApplyColumnStatsModeForAllFileStats(relationId, statsCollector->dataFileStats);

			int64		newRowCount = liveRowCount - deletedRowCount;

			ereport(WriteLogLevel, (errmsg("adding %s with " INT64_FORMAT " rows ",
										   newDataFilePath, newRowCount)));

			/*
			 * We are shrinking the data file with the same partition bounds,
			 * but the file might belong to an old partition spec.
			 */
			int32		partitionSpecId = DEFAULT_SPEC_ID;
			List	   *transforms = AllPartitionTransformList(relationId);
			Partition  *partition = GetDataFilePartition(relationId, transforms, sourcePath,
														 &partitionSpecId);

			Assert(list_length(statsCollector->dataFileStats) == 1);

			/*
			 * while deleting from parquet, we do not add file_size_bytes
			 * option to COPY command, so we can assume that we'll have only a
			 * single file.
			 */
			DataFileStats *newFileStats = linitial(statsCollector->dataFileStats);

			/* store the new file in the metadata */
			TableMetadataOperation *addOperation =
				AddDataFileOperation(newDataFilePath, CONTENT_DATA, newFileStats, partition, partitionSpecId);

			metadataOperations = lappend(metadataOperations, addOperation);
		}
		else
		{
			/*
			 * Merge-on-read deletion. The FDW produced a CSV file with the
			 * file names and positions of deleted rows. We convert that file
			 * to Parquet and add it to the metadata, such that
			 * ReadDataSourceQuery can merge them into the source files at
			 * read time.
			 */
			List	   *copyOptions = NIL;
			DataFileSchema *schema = CreatePositionDeleteDataFileSchema();
			TupleDesc	deleteTupleDesc = CreatePositionDeleteTupleDesc();
			char	   *deletionFilePath = GenerateDataFileNameForTable(relationId, true);

			/*
			 * we defer deletion of in-progress data files only for Iceberg
			 * tables
			 */
			bool		isPrefix = false;
			bool		deferDeletion = IsPgLakeIcebergForeignTableById(relationId);

			InsertInProgressFileRecordExtended(deletionFilePath, isPrefix, deferDeletion);

			List	   *leafFields = GetLeafFieldsForTable(relationId);

			/* write the deletion file */
			StatsCollector *statsCollector =
				ConvertCSVFileTo(deleteFile, deleteTupleDesc, -1, deletionFilePath,
								 DATA_FORMAT_PARQUET, compression, copyOptions, schema, leafFields);

			ereport(WriteLogLevel, (errmsg("adding deletion file %s with " INT64_FORMAT " rows ",
										   deletionFilePath, deletedRowCount)));

			/*
			 * ConvertCSVFileTo() does not use file_bytes_size so we can
			 * assume single file
			 */
			Assert(list_length(statsCollector->dataFileStats) == 1);
			DataFileStats *deletionFileStats = linitial(statsCollector->dataFileStats);

			/*
			 * We are adding position delete file with the same partition
			 * bounds as the source file, but the file might belong to an old
			 * partition spec. So, pass AllPartitionTransformList.
			 */
			int32		partitionSpecId = DEFAULT_SPEC_ID;
			List	   *transforms = AllPartitionTransformList(relationId);
			Partition  *partition =
				GetDataFilePartition(relationId, transforms, sourcePath, &partitionSpecId);

			/* store the deletion file in the metadata */
			TableMetadataOperation *addDeletionFileOperation =
				AddDataFileOperation(deletionFilePath, CONTENT_POSITION_DELETES, deletionFileStats, partition, partitionSpecId);

			metadataOperations = lappend(metadataOperations, addDeletionFileOperation);

			/* store the deletion file to data file mapping */
			TableMetadataOperation *delMapOperation =
				AddDeleteMappingOperation(deletionFilePath, sourcePath);

			metadataOperations = lappend(metadataOperations, delMapOperation);

			/* update the total number of deleted rows in the metadata */
			TableMetadataOperation *updateOperation =
				UpdateDeletedRowCountOperation(sourcePath, totalDeletedRowCount);

			metadataOperations = lappend(metadataOperations, updateOperation);
		}
	}
	else
	{
		elog(ERROR, "deleted row count " INT64_FORMAT " exceeds live row count " INT64_FORMAT,
			 deletedRowCount, liveRowCount);
	}

	return metadataOperations;
}


/*
 * ShouldRewriteAfterDeletions decides whether we should copy the source file excluding the
 * deleted rows, based on the percentage of rows that have been deleted so far.
 */
static bool
ShouldRewriteAfterDeletions(int64 sourceRowCount, uint64 totalDeletedRowCount)
{
	return 100. * totalDeletedRowCount / sourceRowCount >= CopyOnWriteThreshold;
}


/*
 * RemoveAllDataFilesFromTable removes all data files from the given table.
 */
void
RemoveAllDataFilesFromTable(Oid relationId)
{
	TableMetadataOperation *truncateOperation = palloc0(sizeof(TableMetadataOperation));

	truncateOperation->type = DATA_FILE_REMOVE_ALL;

	ApplyMetadataChanges(relationId, list_make1(truncateOperation));
}


/*
 * RemoveAllDataFilesFromPgLakeCatalogFromTable removes all data files from
 * the given table, only from the pg_lake catalogs.
 */
void
RemoveAllDataFilesFromPgLakeCatalogFromTable(Oid relationId)
{
	TableMetadataOperation *truncateOperation = palloc0(sizeof(TableMetadataOperation));

	truncateOperation->type = DATA_FILE_DROP_TABLE;

	ApplyMetadataChanges(relationId, list_make1(truncateOperation));
}


/*
 * CompactDataFiles finds a list of small and large files and rewrites them
 * and returns whether any files were rewritten.
 *
 * It compacts only the data files of the same partition spec and partition tuple.
 *
 * 1. Fetches all data files from catalog.
 * 2. Groups them by partition spec and partition tuple into a hash table.
 * 3. Filters the hash table to only include files that are eligible for
 * compaction.
 * 4. It iterates over the hash table and tries to compact the files of the
 * same partition spec and partition tuple. It picks the partition with
 * the most files first to avoid being stuck in recompacting the same
 * partition that always generate big file forever.
 */
bool
CompactDataFiles(Oid relationId, TimestampTz compactionStartTime,
				 bool forceMerge, bool isVerbose)
{
	/* prevent concurrent update/delete which might rewrite files too */
	LockTableForUpdate(relationId);

	/* make sure we see the changes made by flush */
	PushActiveSnapshot(GetLatestSnapshot());

	Relation	rel = table_open(relationId, RowExclusiveLock);
	TupleDesc	tupleDescriptor = RelationGetDescr(rel);

	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;

	CopyDataFormat format;
	CopyDataCompression compression;
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	FindDataFormatAndCompression(tableType, NULL, options, &format, &compression);

	if (TargetFileSizeMB <= 0 || format != DATA_FORMAT_PARQUET)
	{
		/* files are not splittable */
		table_close(rel, RowExclusiveLock);
		PopActiveSnapshot();
		return false;
	}

	CompactionDataFileHashEntry *entry = GetPartitionWithMostEligibleFiles(relationId, compactionStartTime, forceMerge);

	if (entry == NULL)
	{
		/* no files to compact */
		table_close(rel, RowExclusiveLock);
		PopActiveSnapshot();
		return false;
	}

	List	   *newFileOps = TryCompactDataFiles(relationId, tupleDescriptor, entry->dataFiles,
												 tableType, options, forceMerge, isVerbose);

	table_close(rel, NoLock);
	PopActiveSnapshot();

	return newFileOps != NIL;
}


#ifdef USE_ASSERT_CHECKING
static void
AssertAllFilesHaveSamePartition(List *dataFiles)
{
	ListCell   *fileCell = NULL;
	TableDataFile *firstFile = NULL;

	foreach(fileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(fileCell);

		if (firstFile == NULL)
			firstFile = dataFile;
		else
		{
			Assert(firstFile->partitionSpecId == dataFile->partitionSpecId);

			for (int i = 0; i < firstFile->partition->fields_length; i++)
			{
				PartitionField *firstField = &firstFile->partition->fields[i];
				PartitionField *secondField = &dataFile->partition->fields[i];

				Assert(firstField->field_id == secondField->field_id);
			}
		}
	}
}
#endif


/*
 * TryCompactDataFiles tries to compact the given list of data files. Then, it applies
 * the metadata changes to the table. It returns generated metadata operations for
 * compacted files, if any.
 */
static List *
TryCompactDataFiles(Oid relationId, TupleDesc tupleDescriptor, List *candidates,
					PgLakeTableType tableType, List *options, bool forceMerge, bool isVerbose)
{
#ifdef USE_ASSERT_CHECKING
	AssertAllFilesHaveSamePartition(candidates);
#endif

	if (list_length(candidates) == 1)
	{
		TableDataFile *candidate = linitial(candidates);

		/*
		 * We have 1 candidate. If it's a small file then we cannot compact
		 * further, unless it has too many deletions.
		 */
		if (candidate->stats.fileSize < MAX_TARGET_FILE_SIZE &&
			!ShouldRewriteAfterDeletions(candidate->stats.rowCount, candidate->stats.deletedRowCount) &&
			(!forceMerge || candidate->stats.deletedRowCount == 0))
		{
			/* one small file without merges, nothing to do */
			return NIL;
		}
	}

	List	   *filePathsToCompact = NIL;
	List	   *metadataOperations = NIL;

	ListCell   *candidateCell = NULL;

	ReadDataStats stats = {0, 0};
	int64		fileSizeSum = 0;

	foreach(candidateCell, candidates)
	{
		TableDataFile *dataFile = lfirst(candidateCell);

		ereport(isVerbose ? INFO : WriteLogLevel,
				(errmsg("removing %s with " INT64_FORMAT " rows%s from %s",
						dataFile->path,
						dataFile->stats.rowCount,
						dataFile->stats.deletedRowCount > 0 ? psprintf("(" INT64_FORMAT " deleted)", dataFile->stats.deletedRowCount) : "",
						get_rel_name(relationId))));

		/* remove the old file from the metadata (deferred) */
		metadataOperations = lappend(metadataOperations,
									 RemoveDataFileOperation(dataFile->path));


		/*
		 * we only compact files that are writable, which never have negative
		 * row count
		 */
		Assert(dataFile->stats.rowCount != ROW_COUNT_NOT_SET);
		stats.sourceRowCount += (uint64) dataFile->stats.rowCount - dataFile->stats.deletedRowCount;

		fileSizeSum += dataFile->stats.fileSize;

		filePathsToCompact = lappend(filePathsToCompact, dataFile->path);
	}

	/* get all position deletion files for the candidates */
	List	   *positionDeletes = GetPositionDeleteFilesForDataFiles(relationId,
																	 candidates, NULL,
																	 &stats.positionDeleteRowCount);

	/* construct a query that reads all candidates */
	List	   *formatOptions = NIL;
	DataFileSchema *schema = NULL;

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		schema = GetDataFileSchemaForTable(relationId);
	}

	/*
	 * FIXME: we are using READ_DATA_PREFER_VARCHAR here for its side effects
	 * of having no additional casting/special handling for data fields; this
	 * should be promoted to an actual flag or refactor the whole thing to
	 * account for this use case.
	 */
	int			readFlags = READ_DATA_PREFER_VARCHAR;

	/*
	 * We need the row location to compute the row ID.
	 */
	bool		hasRowIds = GetBoolOption(options, "row_ids", false);

	if (hasRowIds)
		readFlags |= READ_DATA_EMIT_ROW_LOCATION | READ_DATA_EMIT_ROW_ID;

	char	   *readFileQuery =
		ReadDataSourceQuery(filePathsToCompact,
							positionDeletes,
							DATA_FORMAT_PARQUET,
	/* compression is not needed for reading Parquet */
							DATA_COMPRESSION_INVALID,
							tupleDescriptor,
							formatOptions,
							schema,
							&stats,
							readFlags);

	if (hasRowIds)
	{
		readFileQuery = AddRowIdMaterializationToReadQuery(readFileQuery, relationId,
														   candidates);

		/*
		 * Sorting by row ID helps ensure that we get contiguous,
		 * monotonically increasing row ID ranges that compress well as
		 * ranges.
		 */
		readFileQuery = psprintf("%s order by _row_id", readFileQuery);
	}

	/*
	 * Enabling file_size_bytes increases memory usage and CPU, even if there
	 * is no actual splitting. Hence, we only enable split when it's required.
	 */
	bool		allowSplit = fileSizeSum > TargetFileSizeMB * MB_BYTES;

	/*
	 * all candidates have the same partition spec and partition tuple during
	 * compaction
	 */
	TableDataFile *firstCandidate = linitial(candidates);
	int32		partitionSpecId = firstCandidate->partitionSpecId;
	Partition  *partition = firstCandidate->partition;

	List	   *newFileOps =
		PrepareToAddQueryResultToTable(relationId, readFileQuery, tupleDescriptor,
									   partitionSpecId, partition,
									   hasRowIds, allowSplit, isVerbose);

	metadataOperations = list_concat(metadataOperations, newFileOps);

	if (hasRowIds)
	{
		/* get the row ID ranges from the compacted files */
		ListCell   *newFileCell = NULL;

		foreach(newFileCell, newFileOps)
		{
			TableMetadataOperation *addOp = lfirst(newFileCell);
			List	   *rowIdRanges = GetRowIdRangesFromFile(addOp->path);

			Assert(GetTotalRowIdRangeRowCount(rowIdRanges) == addOp->dataFileStats.rowCount);

			TableMetadataOperation *rowIdMappingOp =
				AddRowIdMappingOperation(addOp->path, rowIdRanges);

			metadataOperations = lappend(metadataOperations, rowIdMappingOp);
		}
	}

	ApplyMetadataChanges(relationId, metadataOperations);

	/*
	 * If we did a large insertion of row_id_mappings, we do not want to wait
	 * for autovacuum to update statistics.
	 */
	if (hasRowIds)
		AnalyzeRowIdMappings();

	return newFileOps;
}


/*
 * PrepareToAddQueryResultToTable executes a query in pgduck, analyzes
 * the newly generated files, and prepares the metadata operations.
 */
static List *
PrepareToAddQueryResultToTable(Oid relationId, char *readQuery, TupleDesc queryTupleDesc,
							   int32 partitionSpecId, Partition * partition,
							   bool queryHasRowId, bool allowSplit, bool isVerbose)
{
	PgLakeTableProperties properties = GetPgLakeTableProperties(relationId);
	List	   *options = properties.options;

	bool		isPrefix = false;

	/*
	 * We currently only support splitting Parquet files, to not complicate
	 * the row counting.
	 */
	bool		splitFilesBySize =
		allowSplit && TargetFileSizeMB > 0 && properties.format == DATA_FORMAT_PARQUET;

	/*
	 * When target_file_size_mb is non-0 (512MB by default), we use the
	 * file_size_bytes option in DuckDB COPY to split the file.
	 *
	 * The files may not be exactly the desired size; some guess work is
	 * involved.
	 */
	if (splitFilesBySize)
	{
		options = lappend(options, CreateFileSizeBytesOption(TargetFileSizeMB));
		isPrefix = true;
	}

	/* prepare a directory name */
	char	   *newDataFilePath = GenerateDataFileNameForTable(relationId, !isPrefix);
	DataFileSchema *schema = GetDataFileSchemaForTable(relationId);

	/* we defer deletion of in-progress data files only for Iceberg tables */
	bool		deferDeletion = IsPgLakeIcebergForeignTableById(relationId);

	InsertInProgressFileRecordExtended(newDataFilePath, isPrefix, deferDeletion);

	/* perform compaction */
	List	   *leafFields = GetLeafFieldsForTable(relationId);
	StatsCollector *statsCollector =
		WriteQueryResultTo(readQuery,
						   newDataFilePath,
						   properties.format,
						   properties.compression,
						   options,
						   queryHasRowId,
						   schema,
						   queryTupleDesc,
						   leafFields);

	if (statsCollector->totalRowCount == 0)
	{
		TimestampTz orphanedAt = GetCurrentTransactionStartTimestamp();

		/* as a convention, we don't add relationIds for prefixes */
		InsertDeletionQueueRecordExtended(newDataFilePath, isPrefix ? InvalidOid : relationId,
										  orphanedAt, isPrefix);

		return NIL;
	}

	ApplyColumnStatsModeForAllFileStats(relationId, statsCollector->dataFileStats);

	/* find which files were generated */
	List	   *newFiles = NIL;
	List	   *newFileOps = GetNewFileOpsFromFileStats(relationId, statsCollector->dataFileStats,
														partitionSpecId, partition,
														statsCollector->totalRowCount,
														isVerbose, &newFiles);

	/*
	 * when we defer deletion of in-progress files, we need to replace the
	 * prefix paths with full paths. At precommit hook, we delete persisted
	 * files from in-progress
	 */
	if (isPrefix && deferDeletion)
		ReplaceInProgressPrefixPathWithFullPaths(newDataFilePath, newFiles);

	return newFileOps;
}


/*
 * AddQueryResultToTable adds the result of a pgduck query to the table.
 */
int64
AddQueryResultToTable(Oid relationId, char *readQuery, TupleDesc queryTupleDesc)
{
	int64		rowsProcessed = 0;
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	bool		hasRowIds = GetBoolOption(options, "row_ids", false);

	/* verbose option is only used during vacuum */
	bool		isVerbose = false;

	/* we currently do not inject a row ID column into the query result */
	bool		queryHasRowId = false;

	/* query result can potentially be multi-TB, always allow split */
	bool		allowSplit = true;

	List	   *metadataOperations = NIL;

	/*
	 * COPY/INSERT .. SELECT pushdown code-path is never exercised for
	 * partitioned tables, so partition is NULL.
	 */
	Assert(GetIcebergTablePartitionByOption(relationId) == NULL);
	Partition  *partition = NULL;

	/*
	 * Our convention is to use partitionSpecId = 0 for non-partitioned
	 * tables, and for now we don't support partitioning in COPY/INSERT ..
	 * SELECT pushdown.
	 */
	int			partitionSpecId = GetCurrentSpecId(relationId);

	Assert(partitionSpecId == DEFAULT_SPEC_ID);

	List	   *newFileOps =
		PrepareToAddQueryResultToTable(relationId, readQuery, queryTupleDesc,
									   partitionSpecId, partition,
									   queryHasRowId, allowSplit, isVerbose);

	metadataOperations = list_concat(metadataOperations, newFileOps);

	ListCell   *newFileCell = NULL;

	foreach(newFileCell, newFileOps)
	{
		TableMetadataOperation *addOp = lfirst(newFileCell);
		int64		rowCount = addOp->dataFileStats.rowCount;

		if (hasRowIds && rowCount > 0)
		{
			/* assign new row ID ranges */
			RowIdRangeMapping *rowIdRange =
				CreateRowIdRangeForNewFile(relationId, rowCount, 0);

			TableMetadataOperation *rowIdMappingOp =
				AddRowIdMappingOperation(addOp->path, list_make1(rowIdRange));

			metadataOperations = lappend(metadataOperations, rowIdMappingOp);

			/* set the row_id_start for the new data file */
			addOp->dataFileStats.rowIdStart = rowIdRange->rowStartId;
		}

		rowsProcessed += rowCount;
	}

	ApplyMetadataChanges(relationId, metadataOperations);

	return rowsProcessed;
}


/*
 * GetPartitionWithMostEligibleFiles
 *
 * 1. Fetches all data files from catalog.
 * 2. Groups them by partition spec and partition tuple into a hash table.
 * 3. Filters the hash table to only include files that are eligible for
 * compaction.
 * 4. It iterates over the hash table and tries to compact the files of the
 * same partition spec and partition tuple. It picks the partition with
 * the most files first to avoid being stuck in recompacting the same
 * partition that always generate big file forever.
 */
static CompactionDataFileHashEntry *
GetPartitionWithMostEligibleFiles(Oid relationId, TimestampTz compactionStartTime, bool forceMerge)
{
	/* we're going to rewrite files, so lock them */
	bool		forUpdate = true;

	/* compact oldest files first */
	char	   *orderBy = "updated_time";

	Snapshot	snapshot = NULL;
	bool		dataOnly = false;
	bool		newFilesOnly = false;

	/* fetch all data files from catalog */
	List	   *dataFiles = GetTableDataFilesFromCatalog(relationId, dataOnly, newFilesOnly,
														 forUpdate, orderBy, snapshot);

	/* group data files by partition spec and partition tuple */
	HTAB	   *dataFilesPerPartition = GroupDataFilesByPartition(dataFiles, compactionStartTime, forceMerge);

	/* find the partition with the most eligible files after pruning */
	CompactionDataFileHashEntry *mostFileEntry = NULL;

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, dataFilesPerPartition);

	CompactionDataFileHashEntry *entry = NULL;

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		/* filter out files that are not eligible for compaction */
		entry->dataFiles = FilterCompactionCandidates(entry->dataFiles,
													  compactionStartTime,
													  forceMerge);

		if (entry->dataFiles == NIL)
		{
			/* no candidates left after pruning, then remove the entry */
			hash_search(dataFilesPerPartition, &entry->partitionHash, HASH_REMOVE, NULL);
			continue;
		}

		/* set the partition with the most files */
		if (mostFileEntry == NULL || list_length(entry->dataFiles) > list_length(mostFileEntry->dataFiles))
		{
			mostFileEntry = entry;
		}
	}

	return mostFileEntry;
}


/*
 * CompactMetadata expires old snapshots from an Iceberg table via the metadata
 * and merges manifest files.
 */
void
CompactMetadata(Oid relationId, bool isVerbose)
{
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	if (tableType != PG_LAKE_ICEBERG_TABLE_TYPE)
		/* expiring old snapshots is only valid for Iceberg tables */
		return;

	/*
	 * we will change metadata. We should prevent concurrent update/delete
	 * which might rewrite files too.
	 */
	LockTableForUpdate(relationId);

	TrackIcebergMetadataChangesInTx(relationId, list_make2_int(EXPIRE_OLD_SNAPSHOTS, DATA_FILE_MERGE_MANIFESTS));
}


/*
 * ApplyDataFileModifications takes a list of file modifications and applies it
 * to the table.
 *
 * There is significant room for optimization in this function by combining
 * multiple modifications in a single command.
 */
void
ApplyDataFileModifications(Relation rel, List *modifications)
{
	Oid			relationId = RelationGetRelid(rel);
	ListCell   *modificationCell = NULL;
	List	   *metadataOperations = NIL;

	/*
	 * We may have deferred modifications from previous commands to this one.
	 * We primarily do this in logical replication flush.
	 */
	if (DeferredModifications != NIL)
	{
		modifications = list_concat(modifications, DeferredModifications);

		DeferredModifications = NIL;
	}

	foreach(modificationCell, modifications)
	{
		DataFileModification *modification = lfirst(modificationCell);

		List	   *modificationOperations = NIL;

		if (modification->type == ADD_DELETION_FILE_FROM_CSV)
		{
			Assert(modification->sourcePath != NULL);

			modificationOperations =
				ApplyDeleteFile(rel,
								modification->sourcePath,
								modification->sourceRowCount,
								modification->liveRowCount,
								modification->deleteFile,
								modification->deletedRowCount);
		}

		else if (modification->type == ADD_DATA_FILE)
		{
			Assert(modification->insertFile != NULL);

			modificationOperations =
				ApplyInsertFile(rel,
								modification->insertFile,
								modification->insertedRowCount,
								modification->reservedRowIdStart,
								modification->partitionSpecId,
								modification->partition,
								modification->fileStats);
		}

		else
			elog(ERROR, "unrecognized modification type %d", modification->type);

		metadataOperations = list_concat(metadataOperations, modificationOperations);
	}

	ApplyMetadataChanges(relationId, metadataOperations);
}


/*
 * ApplyMetadataChanges applies metadata changes to a given table.
 */
static void
ApplyMetadataChanges(Oid relationId, List *metadataOperations)
{
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	switch (tableType)
	{
		case PG_LAKE_TABLE_TYPE:
			ApplyDataFileCatalogChanges(relationId, metadataOperations);
			break;

		case PG_LAKE_ICEBERG_TABLE_TYPE:
			{
				ApplyDataFileCatalogChanges(relationId, metadataOperations);

				List	   *operationTypes = GetMetadataOperationTypes(metadataOperations);

				TrackIcebergMetadataChangesInTx(relationId, operationTypes);
				break;
			}

		default:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("metadata changes not yet implemented for "
								   "table type %d",
								   tableType)));
	}
}


/*
 * CreateCompactionDataFileHash creates a hash for storing data files of the same
 * partition spec and partition tuple, to be used for compaction.
 */
static HTAB *
CreateCompactionDataFileHash(void)
{
	HASHCTL		hashInfo;

	hashInfo.keysize = sizeof(uint64);
	hashInfo.entrysize = sizeof(CompactionDataFileHashEntry);
	hashInfo.hash = tag_hash;
	hashInfo.hcxt = CurrentMemoryContext;

	uint32		hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	return hash_create("Iceberg partitioned data file hash for compaction",
					   32, &hashInfo, hashFlags);
}


/*
 * GroupDataFilesByPartition creates a hash table with data files
 * grouped by partition spec and partition tuple from the given list of
 * data files.
 */
static HTAB *
GroupDataFilesByPartition(List *dataFiles, TimestampTz compactionStartTime, bool forceMerge)
{
	HTAB	   *dataFilesHash = CreateCompactionDataFileHash();

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(dataFileCell);

		uint64		specPartitionKey = ComputeSpecPartitionKey(dataFile->partitionSpecId,
															   dataFile->partition);

		bool		found = false;
		CompactionDataFileHashEntry *entry = hash_search(dataFilesHash, &specPartitionKey, HASH_ENTER, &found);

		if (!found)
			entry->dataFiles = NIL;

		entry->dataFiles = lappend(entry->dataFiles, dataFile);
	}

	return dataFilesHash;
}


/*
 * FilterCompactionCandidates prunes the list of data files to only include those that
 * are eligible for compaction.
 *
 * It gets a list of compaction candidates from oldest (updated_time)
 * to newest by the following criteria:
 * - Less than 50% of target size
 * - More than 180% of target size
 * - forceMerge is enabled and deletions exist
 *
 * We stop when reaching 4 * TargetFileSizeMB, such that a filled up compaction would write
 * multiple files of roughly the target file size. If some additional bytes remain, they
 * will be rewritten in the next compaction.
 *
 * We also limited to MAX_FILES_PER_COMPACTION files to not overwhelm the compactor.
 */
static List *
FilterCompactionCandidates(List *dataFiles, TimestampTz compactionStartTime,
						   bool forceMerge)
{
#ifdef USE_ASSERT_CHECKING
	AssertAllFilesHaveSamePartition(dataFiles);
#endif

	List	   *candidates = NIL;
	int64		candidatePoolSize = 0;

	ListCell   *dataFileCell = NULL;

	bool		meetCompactionCriteria = forceMerge;

	foreach(dataFileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(dataFileCell);

		/* we only compact data files */
		if (dataFile->content != CONTENT_DATA)
			continue;

		/* do not consider files created after compaction started */
		if (TimestampDifferenceExceeds(compactionStartTime,
									   dataFile->stats.creationTime,
									   0))
			continue;

		/*
		 * We currently use the same compaction criteria as Spark defaults.
		 * https://iceberg.apache.org/docs/1.5.1/spark-procedures/#rewrite_data_files
		 *
		 * Additionally, we merge files that have more than 20% deletions, or
		 * if forceMerge is specified we compact any file that has deletions.
		 */
		if (dataFile->stats.fileSize < MIN_TARGET_FILE_SIZE ||
			dataFile->stats.fileSize > MAX_TARGET_FILE_SIZE ||
			ShouldRewriteAfterDeletions(dataFile->stats.rowCount, dataFile->stats.deletedRowCount) ||
			(forceMerge && dataFile->stats.deletedRowCount > 0))
		{
			candidates = lappend(candidates, dataFile);

			int64		liveRowCount = dataFile->stats.rowCount - dataFile->stats.deletedRowCount;
			int64		estimatedPostMergeSize = 100;

			if (dataFile->stats.rowCount > 0)
				estimatedPostMergeSize = dataFile->stats.fileSize * liveRowCount / dataFile->stats.rowCount;

			candidatePoolSize += estimatedPostMergeSize;

			/*
			 * Our aim is to produce one new file at a time to avoid the
			 * overhead of file_size_bytes, though if one of the source files
			 * is large it will be split by the compaction.
			 */
			if (candidatePoolSize > (0.75 * TargetFileSizeMB * MB_BYTES))
			{
				meetCompactionCriteria = true;
				break;
			}

			/*
			 * Limit the number of candidates per compaction.
			 */
			if (list_length(candidates) >= MAX_FILES_PER_COMPACTION)
			{
				meetCompactionCriteria = true;
				break;
			}
		}
	}

	if (!meetCompactionCriteria && list_length(candidates) < VacuumCompactMinInputFiles)
		/* not enough files to compact */
		return NIL;

	return candidates;
}


/*
 * GetPositionDeleteFilesForDataFiles gets all possible position deletes
 * for a given set of data files.
 *
 * The optional snapshot parameter can be used to get the position deletes
 * as of a specific snapshot.
 *
 * This operation could perhaps be optimized into a single SQL query.
 */
List *
GetPositionDeleteFilesForDataFiles(Oid relationId, List *dataFiles, Snapshot snapshot,
								   uint64 *rowCount)
{
	List	   *positionDeletes = NIL;

	List	   *dataFilePathList = NIL;
	ListCell   *dataFileCell = NULL;

	*rowCount = 0;

	foreach(dataFileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(dataFileCell);

		dataFilePathList = lappend(dataFilePathList, dataFile->path);

		/*
		 * we only compact data files that are writable, so never expect
		 * negative values
		 */
		Assert(dataFile->stats.deletedRowCount != ROW_COUNT_NOT_SET);
		*rowCount += (uint64) dataFile->stats.deletedRowCount;
	}

	/*
	 * Only get position deletes for the data files that are being compacted
	 * and access the catalog once.
	 */
	positionDeletes = GetPossiblePositionDeleteFiles(relationId, dataFilePathList, snapshot);

	return positionDeletes;
}


/*
 * GetPossiblePositionDeleteFilesFromCatalog returns a list of position delete files that
 * may have to be applied to a given source file.
 */
static List *
GetPossiblePositionDeleteFiles(Oid relationId, List *sourcePathList, Snapshot snapshot)
{
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	switch (tableType)
	{
		case PG_LAKE_TABLE_TYPE:
		case PG_LAKE_ICEBERG_TABLE_TYPE:
			return GetPossiblePositionDeleteFilesFromCatalog(relationId, sourcePathList, snapshot);

		default:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("metadata changes not yet implemented for "
								   "table type %d",
								   tableType)));
	}
}


/*
 * CreatePositionDeleteTupleDesc returns a TupleDescriptor that
 * can be used for position delete files.
 */
TupleDesc
CreatePositionDeleteTupleDesc(void)
{
	int			columnCount = 3;
	TupleDesc	tupleDescriptor = CreateTemplateTupleDesc(columnCount);

	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 1, "file_path",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 2, "pos",
					   INT8OID, -1, 0);
	TupleDescInitEntry(tupleDescriptor, (AttrNumber) 3, "row",
					   RECORDOID, -1, 0);

	return tupleDescriptor;
}


/*
 * CreateFileSizeBytesOption creates an option of the form file_size_bytes '512MB'
 */
DefElem *
CreateFileSizeBytesOption(int sizeMb)
{
	char	   *fileSizeStr = psprintf("%dMB", sizeMb);
	DefElem    *fileSizeOption =
		makeDefElem("file_size_bytes", (Node *) makeString(fileSizeStr), -1);

	return fileSizeOption;
}


/*
 * LockTableForUpdate takes an advisory lock that corresponds to the relation
 * ID, which is used to block concurrent updates.
 */
void
LockTableForUpdate(Oid relationId)
{
	LOCKTAG		tag;
	const bool	sessionLock = false;
	const bool	dontWait = false;

	SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, 0, (uint32) relationId,
						 ADV_LOCKTAG_CLASS_PG_LAKE_TABLE_UPDATE);

	(void) LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
}


/*
 * TryLockTableForUpdate takes an advisory lock that corresponds to the relation
 * ID, or returns false if the lock is currently taken.
 */
bool
TryLockTableForUpdate(Oid relationId)
{
	LOCKTAG		tag;
	const bool	sessionLock = false;
	const bool	dontWait = true;

	SET_LOCKTAG_ADVISORY(tag, MyDatabaseId, 0, (uint32) relationId,
						 ADV_LOCKTAG_CLASS_PG_LAKE_TABLE_UPDATE);

	return LockAcquire(&tag, ExclusiveLock, sessionLock, dontWait);
}
