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
 * Predicate function to filter snapshots.
 *
 * The predicate function should return true if the snapshot should be included in the result.
 * Otherwise, it should return false.
 *
 * The predicate function can be NULL, in which case all snapshots are included in the result.
 */
typedef bool (*SnapshotPredicateFn) (IcebergTableMetadata * metadata, IcebergSnapshot * snapshot);

/*
* Represents IcebergSnapshot->summary->operation value.
*/
typedef enum
{
	SNAPSHOT_OPERATION_INVALID,
	SNAPSHOT_OPERATION_APPEND,
	SNAPSHOT_OPERATION_REPLACE,
	SNAPSHOT_OPERATION_OVERWRITE,
	SNAPSHOT_OPERATION_DELETE
}			SnapshotOperation;


/* predicates */
extern PGDLLEXPORT bool IsCurrentSnapshot(IcebergTableMetadata * metadata, IcebergSnapshot * snapshot);

/* read api */
extern PGDLLEXPORT List *FetchSnapshotsFromTableMetadata(IcebergTableMetadata * metadata, SnapshotPredicateFn snapshotPredicateFn);
extern PGDLLEXPORT IcebergSnapshot * GetCurrentSnapshot(IcebergTableMetadata * metadata, bool missingOk);
extern PGDLLEXPORT IcebergSnapshot * GetIcebergSnapshotViaId(IcebergTableMetadata * metadata, uint64_t snapshotId, bool missingOk);


/* write api */
extern PGDLLEXPORT IcebergSnapshot * CreateNewIcebergSnapshot(IcebergTableMetadata * metadata);
