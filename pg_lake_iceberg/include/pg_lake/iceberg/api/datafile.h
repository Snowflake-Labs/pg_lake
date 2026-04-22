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

#include "pg_lake/iceberg/api/manifest.h"
#include "pg_lake/iceberg/api/manifest_entry.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/metadata_spec.h"

/*
 * Predicate function to filter data files.
 *
 * The predicate function should return true if the data file should be included in the result.
 * Otherwise, it should return false.
 *
 * The predicate function can be NULL, in which case all data files are included in the result.
 */
typedef bool (*DataFilePredicateFn) (DataFile *dataFile);

/* read api */
extern PGDLLEXPORT List *FetchDataFilesFromManifestEntry(IcebergManifestEntry *manifestEntry, DataFilePredicateFn dataFilePredicateFn);
extern PGDLLEXPORT List *FetchDataFilesFromSnapshot(IcebergSnapshot *snapshot, ManifestPredicateFn manifestPredicateFn, ManifestEntryPredicateFn manifestEntryPredicateFn, DataFilePredicateFn dataFilePredicateFn);
extern PGDLLEXPORT List *FetchDataFilePathsFromSnapshot(IcebergSnapshot *snapshot, ManifestPredicateFn manifestPredicateFn, ManifestEntryPredicateFn manifestEntryPredicateFn, DataFilePredicateFn dataFilePredicateFn);
extern PGDLLEXPORT void FetchAllDataAndDeleteFilesFromCurrentSnapshot(IcebergTableMetadata *metadata, List **dataFiles, List **deleteFiles);
extern PGDLLEXPORT void FetchAllDataAndDeleteFilePathsFromCurrentSnapshot(IcebergTableMetadata *metadata, List **dataFilePaths, List **deleteFilePaths);
extern PGDLLEXPORT List *FetchDataFilesFromManifest(IcebergManifest *manifest, bool pathOnly, ManifestEntryPredicateFn manifestEntryPredicateFn, DataFilePredicateFn dataFilePredicateFn);
