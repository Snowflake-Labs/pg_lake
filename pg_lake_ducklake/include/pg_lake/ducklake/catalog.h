/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef PG_LAKE_DUCKLAKE_CATALOG_H
#define PG_LAKE_DUCKLAKE_CATALOG_H

#include "postgres.h"
#include "utils/uuid.h"

/* GUC variables */
extern char *DucklakeDefaultLocationPrefix;
extern bool DucklakeAutovacuumEnabled;
extern int	DucklakeAutovacuumNaptime;
extern int	DucklakeMaxSnapshotAge;
extern int	DucklakeLogAutovacuumMinDuration;

/* Schema and table name constants */
#define DUCKLAKE_SCHEMA_NAME "lake_ducklake"
#define DUCKLAKE_METADATA_TABLE "metadata"
#define DUCKLAKE_SNAPSHOT_TABLE "snapshot"
#define DUCKLAKE_SNAPSHOT_CHANGES_TABLE "snapshot_changes"
#define DUCKLAKE_SCHEMA_TABLE "schema"
#define DUCKLAKE_TABLE_TABLE "table"
#define DUCKLAKE_COLUMN_TABLE "column"
#define DUCKLAKE_VIEW_TABLE "view"
#define DUCKLAKE_DATA_FILE_TABLE "data_file"
#define DUCKLAKE_DELETE_FILE_TABLE "delete_file"
#define DUCKLAKE_TABLE_STATS_TABLE "table_stats"

/*
 * DucklakeTableMetadata - represents a DuckLake table's metadata
 */
typedef struct DucklakeTableMetadata
{
	int64		tableId;
	pg_uuid_t	tableUuid;
	int64		schemaId;
	char	   *tableName;
	char	   *schemaName;
	char	   *path;
	bool		pathIsRelative;
	int64		beginSnapshot;
	int64		endSnapshot;
}			DucklakeTableMetadata;

/*
 * DucklakeSnapshot - represents a snapshot
 */
typedef struct DucklakeSnapshot
{
	int64		snapshotId;
	TimestampTz snapshotTime;
	int64		schemaVersion;
	int64		nextCatalogId;
	int64		nextFileId;
}			DucklakeSnapshot;

/*
 * DucklakeDataFile - represents a data file entry
 */
typedef struct DucklakeDataFile
{
	int64		dataFileId;
	int64		tableId;
	int64		beginSnapshot;
	int64		endSnapshot;
	char	   *path;
	bool		pathIsRelative;
	char	   *fileFormat;
	int64		recordCount;
	int64		fileSizeBytes;
	int64		rowIdStart;
	int64		partitionId;
}			DucklakeDataFile;

/*
 * DucklakeDeleteFile - represents a delete file entry
 */
typedef struct DucklakeDeleteFile
{
	int64		deleteFileId;
	int64		tableId;
	int64		beginSnapshot;
	int64		endSnapshot;
	int64		dataFileId;
	char	   *path;
	bool		pathIsRelative;
	int64		deleteCount;
	int64		fileSizeBytes;
}			DucklakeDeleteFile;

/* Catalog operations */
extern PGDLLEXPORT DucklakeSnapshot *DucklakeGetCurrentSnapshot(void);
extern PGDLLEXPORT DucklakeSnapshot *DucklakeCreateSnapshot(const char *changesMade,
															 const char *author,
															 const char *commitMessage);
extern PGDLLEXPORT int64 DucklakeGetNextCatalogId(void);
extern PGDLLEXPORT int64 DucklakeGetNextFileId(void);

/* Table operations */
extern PGDLLEXPORT DucklakeTableMetadata *DucklakeGetTableMetadata(Oid tableOid);
extern PGDLLEXPORT DucklakeTableMetadata *DucklakeGetTableMetadataById(int64 tableId);
extern PGDLLEXPORT int64 DucklakeRegisterTable(const char *schemaName,
											   const char *tableName,
											   const char *path,
											   Oid tableOid);
extern PGDLLEXPORT void DucklakeRegisterTableColumns(Oid tableOid, int64 tableId);
extern PGDLLEXPORT void DucklakeDropTable(int64 tableId);

/* Data file operations */
extern PGDLLEXPORT List *DucklakeGetDataFiles(int64 tableId, int64 snapshotId);
extern PGDLLEXPORT int64 DucklakeAddDataFile(int64 tableId,
											 const char *path,
											 int64 recordCount,
											 int64 fileSizeBytes,
											 int64 rowIdStart);
extern PGDLLEXPORT void DucklakeRemoveDataFile(int64 dataFileId);
extern PGDLLEXPORT void DucklakeRemoveAllDataFiles(int64 tableId);

/* Delete file operations */
extern PGDLLEXPORT List *DucklakeGetDeleteFiles(int64 tableId, int64 snapshotId);
extern PGDLLEXPORT int64 DucklakeAddDeleteFile(int64 tableId,
											   int64 dataFileId,
											   const char *path,
											   int64 deleteCount,
											   int64 fileSizeBytes);

/* File column stats operations */
extern PGDLLEXPORT void DucklakeAddFileColumnStats(int64 dataFileId,
												   int64 tableId,
												   int columnOrder,
												   const char *minValue,
												   const char *maxValue);

/* Column operations for ALTER TABLE support */
extern PGDLLEXPORT void DucklakeAddColumn(Oid tableOid, const char *columnName,
										  const char *columnType, bool nullsAllowed);
extern PGDLLEXPORT void DucklakeDropColumn(Oid tableOid, const char *columnName);
extern PGDLLEXPORT void DucklakeRenameColumn(Oid tableOid, const char *oldName,
											 const char *newName);

#endif							/* PG_LAKE_DUCKLAKE_CATALOG_H */
