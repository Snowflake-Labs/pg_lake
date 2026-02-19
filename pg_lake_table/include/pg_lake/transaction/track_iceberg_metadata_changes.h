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
#include "access/hash.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/rest_catalog/rest_catalog.h"

typedef struct TableMetadataOperationTracker
{
	Oid			relationId;

	bool		relationCreated;
	bool		relationAltered;
	bool		relationPartitionByChanged;
	bool		relationDataFileChanged;
	bool		relationManifestMergeRequested;
	bool		relationSnapshotExpirationRequested;

	/*
	 * used to compute diff between internal catalog and the last snapshot
	 * that is synced to rest catalog by us
	 */
	IcebergSnapshot *lastSyncedSnapshot;

	/*
	 * current metadata of the table, cache it at the start of diff
	 * computation
	 */
	IcebergTableMetadata *currentMetadata;
	char	   *currentMetadataLocation;
}			TableMetadataOperationTracker;


extern PGDLLEXPORT void ConsumeTrackedIcebergMetadataChanges(void);
extern PGDLLEXPORT void PostAllRestCatalogRequests(void);
extern PGDLLEXPORT void TrackIcebergMetadataChangesInTx(Oid relationId, List *metadataOperationTypes);
extern PGDLLEXPORT void RecordRestCatalogRequestInTx(Oid relationId, RestCatalogOperationType operationType,
													 const char *body,
													 int64_t currentSnapshotId);
extern PGDLLEXPORT void ResetTrackedIcebergMetadataOperation(void);
extern PGDLLEXPORT void ResetRestCatalogRequests(void);
extern PGDLLEXPORT HTAB *GetTrackedIcebergMetadataOperations(void);
extern PGDLLEXPORT bool HasAnyTrackedIcebergMetadataChanges(void);
extern PGDLLEXPORT bool IsIcebergTableCreatedInCurrentTransaction(Oid relation);
