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
	 * Commit-time fast path state. trackedAddedFileOps accumulates a deep
	 * copy of each DATA_FILE_ADD operation we've already inserted into the
	 * catalog during this transaction, allocated in TopTransactionContext and
	 * pre-loaded with column stats. At commit time
	 * GetDataFileMetadataOperations can return these directly instead of
	 * doing the catalog-vs-iceberg-metadata diff query +
	 * LoadColumnStatsForFiles.
	 *
	 * fastPathDisabled flips to true on any operation we can't represent
	 * cheaply in memory (REMOVE, REMOVE_ALL, UPDATE_DELETED_ROW_COUNT, etc.)
	 * or when a subtransaction aborts after touching this relation. In that
	 * case the diff path is used.
	 */
	List	   *trackedAddedFileOps;
	bool		fastPathDisabled;
}			TableMetadataOperationTracker;


extern PGDLLEXPORT void ConsumeTrackedIcebergMetadataChanges(bool isVerbose);
extern PGDLLEXPORT void PostAllRestCatalogRequests(void);
extern PGDLLEXPORT void TrackIcebergMetadataChangesInTx(Oid relationId, List *metadataOperationTypes);
extern PGDLLEXPORT void TrackAppliedDataFileOperations(Oid relationId, List *operations);
extern PGDLLEXPORT void RecordRestCatalogRequestInTx(Oid relationId, RestCatalogOperationType operationType,
													 const char *body);
extern PGDLLEXPORT void ResetTrackedIcebergMetadataOperation(void);
extern PGDLLEXPORT void ResetRestCatalogRequests(void);
extern PGDLLEXPORT HTAB *GetTrackedIcebergMetadataOperations(void);
extern PGDLLEXPORT bool HasAnyTrackedIcebergMetadataChanges(void);
extern PGDLLEXPORT bool IsIcebergTableCreatedInCurrentTransaction(Oid relation);
