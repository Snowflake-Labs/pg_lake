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

#include "access/tupdesc.h"
#include "nodes/pg_list.h"
#include "utils/relcache.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/data_file/data_files.h"

/* by default, we switch to copy-on-write if 20% or more of a file is deleted */
#define DEFAULT_COPY_ON_WRITE_THRESHOLD (20)

/* by default, switch all remaining files to copy-on-write once 10M rows have been position-deleted */
#define DEFAULT_COPY_ON_WRITE_MAX_DELETE_ROWS (10000000)

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

	/*
	 * Streaming-write variant: deletions from a single data file where the
	 * intermediate CSV was streamed directly into the destination Parquet
	 * position-delete file (no local CSV ever existed). `deleteFile` points
	 * at the precomputed Parquet path on object storage and `fileStats`
	 * carries its DuckDB return_stats. ApplyPrecomputedDeleteFile consumes
	 * this; the file-based ApplyDeleteFile path is bypassed (and so is its
	 * copy-on-write threshold check — see ApplyPrecomputedDeleteFile for
	 * the rationale).
	 */
	ADD_DELETION_FILE_PRECOMPUTED,
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

/* pg_lake_table.copy_on_write_max_delete_rows */
extern int	CopyOnWriteMaxDeleteRows;

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

/*
 * Two-phase variant of PrepareCSVInsertion exposed for the streaming-write
 * path: callers compute the destination prefix / format / options up front
 * (BeginCSVInsertion), drive the CSV bytes themselves (e.g. via
 * OpenCSVStreamWriter), then build modifications from a precomputed
 * StatsCollector (BuildCSVInsertionModifications). EndCSVInsertion releases
 * the relation lock.
 *
 * The struct is opaque to callers; access the destination prefix / format /
 * options via the accessor functions below.
 */
typedef struct CSVInsertionContext CSVInsertionContext;

extern PGDLLEXPORT CSVInsertionContext * BeginCSVInsertion(Oid relationId,
														   int64 reservedRowIdStart,
														   DataFileSchema * schema);
extern PGDLLEXPORT List *BuildCSVInsertionModifications(CSVInsertionContext * ctx,
														StatsCollector * statsCollector,
														int64 rowCount);
extern PGDLLEXPORT void EndCSVInsertion(CSVInsertionContext * ctx);

/* accessors for the streaming-write call sites */
extern PGDLLEXPORT TupleDesc CSVInsertionContextTupleDescriptor(CSVInsertionContext * ctx);
extern PGDLLEXPORT char *CSVInsertionContextDestinationPath(CSVInsertionContext * ctx);
extern PGDLLEXPORT CopyDataFormat CSVInsertionContextFormat(CSVInsertionContext * ctx);
extern PGDLLEXPORT CopyDataCompression CSVInsertionContextCompression(CSVInsertionContext * ctx);
extern PGDLLEXPORT List *CSVInsertionContextOptions(CSVInsertionContext * ctx);
extern PGDLLEXPORT DataFileSchema * CSVInsertionContextSchema(CSVInsertionContext * ctx);
extern PGDLLEXPORT List *CSVInsertionContextLeafFields(CSVInsertionContext * ctx);

extern PGDLLEXPORT int64 AddQueryResultToTable(Oid relationId, char *readQuery,
											   TupleDesc queryTupleDesc,
											   bool wrapNativeTypes);

/*
 * Streaming-write variant of AddQueryResultToTable.
 *
 * StartAddQueryResultToTableStream prepares destination metadata, builds
 *   COPY (readQuery) TO '<parquet>' WITH (...), prefixes "RECEIVE " (with
 *   '@@PG_LAKE_RECV@@' substituted for the read_csv path arg in
 *   readQuery), and opens a libpq COPY-IN stream against pgduck_server.
 *   Returns an opaque handle.
 *
 * AddQueryResultStreamConnection returns the libpq connection the
 *   caller should forward CSV bytes over (e.g. via CopyInputToStream).
 *
 * FinishAddQueryResultToTableStream calls PQputCopyEnd, waits for the
 *   deferred COPY query to complete, applies the resulting metadata
 *   changes to the table, and returns the number of inserted rows.
 *   Releases the underlying pgduck connection (success or failure).
 *
 * Callers of these helpers MUST use '@@PG_LAKE_RECV@@' as the read_csv
 * path arg in readQuery; see write_data.c for why and pgduck_server's
 * pgsession.c for the substitution.
 */
typedef struct AddQueryResultStreamHandle AddQueryResultStreamHandle;

extern PGDLLEXPORT AddQueryResultStreamHandle *
StartAddQueryResultToTableStream(Oid relationId, char *readQuery,
								 TupleDesc queryTupleDesc, bool wrapNativeTypes);

extern PGDLLEXPORT struct pg_conn *AddQueryResultStreamConnection(AddQueryResultStreamHandle * handle);

extern PGDLLEXPORT int64
			FinishAddQueryResultToTableStream(AddQueryResultStreamHandle * handle);
