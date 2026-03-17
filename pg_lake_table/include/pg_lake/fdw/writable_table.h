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

#include "nodes/pg_list.h"
#include "utils/relcache.h"
#include "pg_lake/data_file/data_files.h"

/* by default, we switch to copy-on-write if 20% or more of a file is deleted */
#define DEFAULT_COPY_ON_WRITE_THRESHOLD (20)

/* by default, we generate 512 MB files (same as Spark Iceberg) */
#define DEFAULT_TARGET_FILE_SIZE_MB (512)

/* DuckDB is unstable for target file size below 16MB, but allow superuser for testing */
#define MIN_TARGET_FILE_SIZE_MB (16)

/* number of bytes in a megabyte */
#define MB_BYTES ((int64) 1024 * 1024)

#define DEFAULT_MIN_INPUT_FILES (5)

/*
 * DataFileModificationType reflects a type of modification.
 */
typedef enum DataFileModificationType
{
	/* deletions from a single data file (in CSV) */
	ADD_DELETION_FILE_FROM_CSV,

	/* new data file (Parquet) */
	ADD_DATA_FILE,
}			DataFileModificationType;

/*
 * DataFileModification represents a batch of modifications to a source file.
 */
typedef struct DataFileModification
{
	DataFileModificationType type;

	/* file from which we are deleting (NULL for insert only) */
	char	   *sourcePath;
	int64		sourceRowCount;

	/*
	 * number of not-deleted rows, if liveRowCount - deletedRowCount is 0 at
	 * the end of a modification then we can remove the file.
	 */
	int64		liveRowCount;

	/* deletions on the source file (NULL for insert only) */
	char	   *deleteFile;
	int64		deletedRowCount;

	/* insertions (NULL for delete only) */
	char	   *insertFile;
	int64		insertedRowCount;
	int			partitionSpecId;
	struct Partition *partition;

	/* if the caller already reserved a row ID range, where does it start? */
	int64		reservedRowIdStart;
	DataFileStats *fileStats;
}			DataFileModification;


/* pg_lake_table.copy_on_write_threshold */
extern int	CopyOnWriteThreshold;

/* pg_lake_table.target_file_size_mb */
extern int	TargetFileSizeMB;

/* lake_table.compact_min_input_files */
extern int	VacuumCompactMinInputFiles;

/* pg_lake_table.write_log_level */
extern PGDLLEXPORT int WriteLogLevel;

/* list of deferred modifications that should be applied during the next write */
extern PGDLLEXPORT List *DeferredModifications;

extern PGDLLEXPORT void ApplyDataFileModifications(Relation rel, List *modifications);
extern PGDLLEXPORT void RemoveAllDataFilesFromTable(Oid relationId);
extern PGDLLEXPORT void RemoveAllDataFilesFromPgLakeCatalogFromTable(Oid relationId);
extern PGDLLEXPORT bool CompactDataFiles(Oid relaitonId, TimestampTz compactionStartTime,
										 bool forceMerge, bool isVerbose);
extern PGDLLEXPORT void CompactMetadata(Oid relationId, bool isVerbose);
extern PGDLLEXPORT List *GetPositionDeleteFilesForDataFiles(Oid relationId, List *dataFiles,
															Snapshot snapshot, uint64 *rowCount);


extern PGDLLEXPORT char *GenerateDataFileNameForTable(Oid relationId, bool withExtension);
extern PGDLLEXPORT TupleDesc CreatePositionDeleteTupleDesc(void);
extern PGDLLEXPORT DefElem *CreateFileSizeBytesOption(int sizeMb);
extern PGDLLEXPORT void LockTableForUpdate(Oid relationId);
extern PGDLLEXPORT bool TryLockTableForUpdate(Oid relationId);

extern PGDLLEXPORT List *PrepareCSVInsertion(Oid relationId, char *insertCSV, int64 rowCount,
											 int64 reservedRowIdStart, int maximumLineSize,
											 DataFileSchema * schema);

extern PGDLLEXPORT int64 AddQueryResultToTable(Oid relationId, char *readQuery,
											   TupleDesc queryTupleDesc);
