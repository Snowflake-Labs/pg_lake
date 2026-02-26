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

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/util/s3_reader_utils.h"

#include "nodes/pg_list.h"
#include "utils/dynahash.h"

#define DATA_FILES_TABLE_QUALIFIED \
	PG_LAKE_TABLE_SCHEMA "." PG_LAKE_TABLE_FILES_TABLE_NAME

typedef struct TableDataFileHashEntry
{
	char		filePath[MAX_S3_PATH_LENGTH];
	TableDataFile dataFile;
}			TableDataFileHashEntry;


/* external control of whether to add to in-transaction temp table */
typedef bool (*PgLakeAddDataFileHookType) (void);
extern PGDLLEXPORT PgLakeAddDataFileHookType PgLakeAddDataFileHook;


/* functions to read from files catalog */
extern PGDLLEXPORT List *GetTableDataFilesFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
													  bool forUpdate, char *orderBy, Snapshot snapshot);
HTAB	   *GetTableDataFilesHashFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
											 bool forUpdate, char *orderBy, Snapshot snapshot,
											 List *partitionTransforms);
HTAB	   *GetTableDataFilesByPathHashFromCatalog(Oid relationId, bool dataOnly, bool newFilesOnly,
												   bool forUpdate, char *orderBy, Snapshot snapshot,
												   List *partitionTransforms);
extern PGDLLEXPORT void RemoveAllDataFilesFromCatalog(Oid relationId);
extern PGDLLEXPORT List *GetPossiblePositionDeleteFilesFromCatalog(Oid relationId, List *sourcePathList,
																   Snapshot snapshot);
extern PGDLLEXPORT int64 GetTableSizeFromCatalog(Oid relationId);
bool		DataFilesCatalogExists(void);
bool		DataFilesPartitionValuesCatalogExists(void);
bool		PartitionSpecsCatalogExists(void);
bool		PartitionFieldsCatalogExists(void);

/* functions to write to files catalog */
extern PGDLLEXPORT void ApplyDataFileCatalogChanges(Oid relationId, List *metadataOperations);
int64		GenerateDataFileId(void);

/* when enabling row_ids, we need to explicit update first_row_id */
void		UpdateDataFileFirstRowId(Oid relationId, int64 fileId, int64 firstRowId);
