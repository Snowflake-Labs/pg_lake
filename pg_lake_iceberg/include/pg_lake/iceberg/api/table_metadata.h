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
#include "pg_lake/parquet/field.h"

/* for now we use 30 minutes as the default maximum snapshot age */
#define DEFAULT_MAX_SNAPSHOT_AGE (1800)

extern PGDLLEXPORT int IcebergMaxSnapshotAge;

/* write api */
extern PGDLLEXPORT IcebergTableMetadata * GenerateEmptyTableMetadata(char *location);
extern PGDLLEXPORT char *GenerateInitialIcebergTableMetadataPath(Oid relationId);
extern PGDLLEXPORT IcebergTableMetadata * GenerateInitialIcebergTableMetadata(Oid relationId);
extern PGDLLEXPORT char *GenerateRemoteMetadataFilePath(int version, const char *location, char *queryArguments);
extern PGDLLEXPORT void UploadTableMetadataToURI(IcebergTableMetadata * tableMetadata, char *metadataURI);
extern PGDLLEXPORT void AdjustAndRetainMetadataLogs(IcebergTableMetadata * metadata, char *prevMetadataPath, size_t snapshotLogLength, int64_t prev_last_updated_ms);
extern PGDLLEXPORT void UpdateLatestSnapshot(IcebergTableMetadata * tableMetadata, IcebergSnapshot * newSnapshot);
extern PGDLLEXPORT bool RemoveOldSnapshotsFromMetadata(Oid relationId, IcebergTableMetadata * metadata, bool isVerbose);
extern PGDLLEXPORT void GenerateSnapshotLogEntries(IcebergTableMetadata * metadata);
extern PGDLLEXPORT int FindLargestPartitionFieldId(IcebergPartitionSpec * newSpec);
extern PGDLLEXPORT void AppendCurrentPostgresSchema(Oid relationId, IcebergTableMetadata * metadata,
													DataFileSchema * schema);
extern PGDLLEXPORT void AppendPartitionSpec(IcebergTableMetadata * metadata, IcebergPartitionSpec * partitionSpec);
extern PGDLLEXPORT List *GetAllIcebergPartitionSpecsFromTableMetadata(IcebergTableMetadata * metadata);
