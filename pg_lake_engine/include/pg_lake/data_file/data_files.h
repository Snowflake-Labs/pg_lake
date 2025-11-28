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

#pragma once

#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/parquet/field.h"

#define ROW_COUNT_NOT_SET (-1)
#define INVALID_ROW_ID (0)

/*
 * Data structure for a row mapping range for a file; this takes some number
 * of abstract rowIds and maps them to the contiguous block starting at
 * rownumOffset.
 *
 * Higher-level structures will track which file this belongs to.
 */
typedef struct RowIdRangeMapping
{
	int64		rowStartId;		/* starting row id for this range */
	int64		rowStartNum;	/* starting row number for this range */
	int64		numRows;		/* how many rows in this chunk */
}			RowIdRangeMapping;


/*
 * DataFileContent describes what type of content a data file contains.
 */
typedef enum DataFileContent
{
	CONTENT_DATA = 0,
	CONTENT_POSITION_DELETES = 1,

	/*
	 * Equality deletes are not yet implemented, but we keep them in the enum
	 * to match the Iceberg spec.
	 */
	CONTENT_EQUALITY_DELETES = 2,
}			DataFileContent;

/*
 * TableDataFile represents a single data file in a writable table.
 */
typedef struct TableDataFile
{
	/* file ID */
	int64		fileId;

	/* path of the data file */
	char	   *path;

	/* content of the file (data, position deletes, equality deletes) */
	DataFileContent content;

	/* stats for the data file */
	DataFileStats stats;

	/* partition info for the data file */
	int32_t		partitionSpecId;
	struct Partition *partition;
}			TableDataFile;


/*
 * TableMetadataOperationType describes a type of metadata
 * modification.
 */
typedef enum TableMetadataOperationType
{
	DATA_FILE_ADD = 0,
	DATA_FILE_REMOVE = 1,
	DATA_FILE_REMOVE_ALL = 2,
	DATA_FILE_DROP_TABLE = 3,
	DATA_FILE_UPDATE_DELETED_ROW_COUNT = 4,
	DATA_FILE_MERGE_MANIFESTS = 5,
	DATA_FILE_ADD_DELETE_MAPPING = 6,
	EXPIRE_OLD_SNAPSHOTS = 7,
	TABLE_DDL = 8,
	DATA_FILE_ADD_ROW_ID_MAPPING = 9,
	TABLE_PARTITION_BY = 10,
	TABLE_CREATE = 11,
}			TableMetadataOperationType;

struct IcebergPartitionSpec;
struct Partition;


/*
* When a schema is set back to an existing schema in iceberg metadata,
* we use DDL_EFFECT_SET_EXISTING_SCHEMA to indicate which effect the
* DDL has. An example would be ALTER TABLE .. ADD COLUMN x; followed by
* ALTER TABLE .. DROP COLUMN x; which results in the same schema as before.
* In all other cases, we use DDL_EFFECT_ADD_SCHEMA as that'd mean there is
* no existing schema to set to, and we should add a new schema.
* ps: This is especially important for REST catalog/Polaris, where it rejects
* identical schema addition.
*/
typedef enum DDLSchemaEffect
{
	DDL_EFFECT_NONE = 0,
	DDL_EFFECT_ADD_SCHEMA = 1,
	DDL_EFFECT_SET_EXISTING_SCHEMA = 2,
}			DDLSchemaEffect;

/*
 * TableMetadataOperation represents an operation on table metadata.
 */
typedef struct TableMetadataOperation
{
	TableMetadataOperationType type;

	/* path of the file to add/remove/update */
	const char *path;

	/* content of the file (add or delete) */
	DataFileContent content;

	/* stats for data file */
	DataFileStats dataFileStats;

	/* partition info for data file */
	int32		partitionSpecId;
	struct Partition *partition;

	/* for a new deletion file, from which data file are we deleting? */
	char	   *deletedFrom;

	/*
	 * newSchema and existingSchemaId are mutually exclusive, set according to
	 * ddlSchemaEffect.
	 */
	DDLSchemaEffect ddlSchemaEffect;
	DataFileSchema *newSchema;
	int32_t		existingSchemaId;

	/* for multi-delete, which files we are deleting from */
	List	   *deleteStats;

	/* for add row ID mapping, list of row ID ranges and their row numbers */
	List	   *rowIdRanges;

	/* for partition specs */
	List	   *partitionSpecs;
	int			defaultSpecId;
}			TableMetadataOperation;


extern PGDLLEXPORT TableMetadataOperation * AddDataFileOperation(const char *path,
																 DataFileContent content,
																 DataFileStats * dataFileStats,
																 struct Partition *partition,
																 int32 partitionSpecId);
extern PGDLLEXPORT TableMetadataOperation * RemoveDataFileOperation(const char *path);
extern PGDLLEXPORT TableMetadataOperation * UpdateDeletedRowCountOperation(const char *path, int64 deletedRowCount);
extern PGDLLEXPORT TableMetadataOperation * AddDeleteMappingOperation(const char *deleteFilePath,
																	  const char *dataFilePath);
extern PGDLLEXPORT TableMetadataOperation * AddRowIdMappingOperation(const char *dataFilePath,
																	 List *rowIdRanges);
