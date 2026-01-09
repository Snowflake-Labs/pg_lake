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
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/util/string_utils.h"

/*
 * AddDataFileOperation creates a TableMetadataOperation for adding a new data
 * file.
 */
TableMetadataOperation *
AddDataFileOperation(const char *path, DataFileContent content, DataFileStats * dataFileStats,
					 struct Partition *partition, int32 partitionSpecId)
{
	TableMetadataOperation *operation = palloc0(sizeof(TableMetadataOperation));

	operation->type = DATA_FILE_ADD;

	/* we never want s3 parameters * in the metadata */
	operation->path = StripFromChar((char *) path, '?');

	operation->content = content;

	operation->dataFileStats = *dataFileStats;
	operation->partition = partition;
	operation->partitionSpecId = partitionSpecId;

	return operation;
}


/*
 * UpdateDeletedRowCountOperation creates a TableMetadataOperation for updating
 * the number of deleted rows for a given data file.
 */
TableMetadataOperation *
UpdateDeletedRowCountOperation(const char *path, int64 deletedRowCount)
{
	TableMetadataOperation *operation = palloc0(sizeof(TableMetadataOperation));

	operation->type = DATA_FILE_UPDATE_DELETED_ROW_COUNT;
	operation->path = path;
	operation->dataFileStats.deletedRowCount = deletedRowCount;

	return operation;
}



/*
 * RemoveDataFileOperation creates a TableMetadataOperation for removing a data
 * file.
 */
TableMetadataOperation *
RemoveDataFileOperation(const char *path)
{
	TableMetadataOperation *operation = palloc0(sizeof(TableMetadataOperation));

	operation->type = DATA_FILE_REMOVE;
	operation->path = path;

	return operation;
}


/*
 * AddDeleteMappingFileOperation creates a TableMetadataOperation for adding a new mapping
 * from a deletion file to a data file.
 */
TableMetadataOperation *
AddDeleteMappingOperation(const char *deleteFilePath, const char *dataFilePath)
{
	TableMetadataOperation *operation = palloc0(sizeof(TableMetadataOperation));

	operation->type = DATA_FILE_ADD_DELETE_MAPPING;
	operation->path = StripFromChar((char *) deleteFilePath, '?');
	operation->deletedFrom = StripFromChar((char *) dataFilePath, '?');

	return operation;
}


/*
 * AddRowIdMappingOperation creates a TableMetadataOperation for adding all row map
 * entries for a given file.
 */
TableMetadataOperation *
AddRowIdMappingOperation(const char *dataFilePath, List *rowIdRanges)
{
	TableMetadataOperation *operation = palloc0(sizeof(TableMetadataOperation));

	operation->type = DATA_FILE_ADD_ROW_ID_MAPPING;
	operation->path = StripFromChar((char *) dataFilePath, '?');
	operation->rowIdRanges = rowIdRanges;

	return operation;
}

/*
 * DeepCopyDataFileStats deep copies a DataFileSchema.
 */
DataFileStats *
DeepCopyDataFileStats(const DataFileStats * stats)
{
	DataFileStats *copiedStats = palloc0(sizeof(DataFileStats));

	copiedStats->dataFilePath = pstrdup(stats->dataFilePath);
	copiedStats->fileSize = stats->fileSize;
	copiedStats->rowCount = stats->rowCount;
	copiedStats->deletedRowCount = stats->deletedRowCount;
	copiedStats->creationTime = stats->creationTime;
	copiedStats->rowIdStart = stats->rowIdStart;

	/* Deep copy column stats list */
	if (stats->columnStats != NULL)
	{
		copiedStats->columnStats = NIL;
		ListCell   *cell = NULL;

		foreach(cell, stats->columnStats)
		{
			DataFileColumnStats *colStats = lfirst(cell);
			DataFileColumnStats *copiedColStats = palloc0(sizeof(DataFileColumnStats));

			copiedColStats->leafField = DeepCopyLeafField(&colStats->leafField);
			copiedColStats->lowerBoundText = colStats->lowerBoundText ? pstrdup(colStats->lowerBoundText) : NULL;
			copiedColStats->upperBoundText = colStats->upperBoundText ? pstrdup(colStats->upperBoundText) : NULL;

			copiedStats->columnStats = lappend(copiedStats->columnStats, copiedColStats);
		}
	}

	return copiedStats;
}
