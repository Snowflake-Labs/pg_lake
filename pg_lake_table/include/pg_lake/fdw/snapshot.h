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
#include "pg_lake/iceberg/metadata_spec.h"

/*
 * PgLakeFileScan represents a single file to scan.
 */
typedef struct PgLakeFileScan
{
	/* path of the file */
	char	   *path;

	/* number of rows in the file (-1 for unknown) */
	int64		rowCount;

	/*
	 * number of deleted rows in the data file (via merge-on-read deletion
	 * files)
	 */
	uint64		deletedRowCount;

	/* true if we already determined that all rows match our filters */
	bool		allRowsMatch;
}			PgLakeFileScan;

/*
 * PgLakeTableScan represents a single table to scan.
 */
typedef struct PgLakeTableScan
{
	Oid			relationId;
	int			uniqueRelationIdentifier;

	/* list of PgLakeFileScan for data files */
	List	   *fileScans;

	/* list of PgLakeFileScan for position delete files */
	List	   *positionDeleteScans;

	/* if we want to include child tables, a list of PgLakeTableScan */
	List	   *childScans;

	bool		isUpdateDelete;
}			PgLakeTableScan;

/*
 * PgLakeScanSnapshot represents the snapshot of all the tables
 * involved in a scan.
 */
typedef struct PgLakeScanSnapshot
{
	List	   *tableScans;
}			PgLakeScanSnapshot;


PgLakeScanSnapshot *CreatePgLakeScanSnapshot(List *rteList,
											 List *relationRestrictionsList,
											 ParamListInfo externalParams,
											 bool includeChildren,
											 Oid resultRelationId);
PgLakeTableScan *GetTableScanByRelationId(PgLakeScanSnapshot * snapshot, Oid relationId);
extern PGDLLEXPORT List *GetFileScanPathList(List *fileScans, uint64 *rowCount, bool skipFullScans);
void		SnapshotFilesScanned(PgLakeScanSnapshot * scanSnapshot, int *dataFileScans, int *deleteFileScans);
extern PGDLLEXPORT void CreateTableScanForIcebergMetadata(Oid relationId,
														  IcebergTableMetadata * metadata,
														  List *baseRestrictInfoList,
														  List **fileScans,
														  List **positionDeleteScans);
