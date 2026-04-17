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

#include "postgres.h"

#include "pg_lake/iceberg/api/snapshot.h"
#include "pg_lake/iceberg/manifest_spec.h"

#define KB_BYTES 1024

/* same as Spark default (8MB) */
#define DEFAULT_TARGET_MANIFEST_SIZE_KB (8 * KB_BYTES)

/* same as Spark default (100) */
#define DEFAULT_MANIFEST_MIN_COUNT_TO_MERGE (100)

/* pg_lake_iceberg.enable_manifest_merge */
extern PGDLLEXPORT bool EnableManifestMergeOnWrite;

/* pg_lake_iceberg.target_manifest_size_kb */
extern int	TargetManifestSizeKB;

/* pg_lake_iceberg.manifest_min_count_to_merge */
extern int	ManifestMinCountToMerge;

extern PGDLLEXPORT List *MergeDataManifests(IcebergSnapshot *currentSnapshot,
											List *allTransforms,
											List *dataManifests,
											const char *metadataLocation,
											const char *snapshotUUID,
											bool isVerbose,
											int *manifestIndex);
extern PGDLLEXPORT bool RemoveDeletedManifestEntries(IcebergSnapshot *currentSnapshot,
													 List *allTransforms,
													 List **manifests,
													 IcebergManifestContentType contentType,
													 const char *metadataLocation,
													 const char *snapshotUUID,
													 bool isVerbose,
													 int *manifestIndex);
extern PGDLLEXPORT bool HasMergeableManifests(List *dataManifests);
