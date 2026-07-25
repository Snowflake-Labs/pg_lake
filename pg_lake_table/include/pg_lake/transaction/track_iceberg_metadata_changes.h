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
	bool		relationDataFilesRemoveAllSeen;

	/*
	 * Number of single-file data-file operations recorded for this relation
	 * in the current transaction (DATA_FILE_ADD + DATA_FILE_REMOVE). Each
	 * such op rewrites the pg_lake catalogs that the commit-time diff joins,
	 * so the count tracks the work the diff will do regardless of direction.
	 * Used by pg_lake_table.commit_time_analyze_threshold.
	 */
	int64		dataFileChangeCount;

	/*
	 * Set when DATA_FILE_REMOVE_ALL was recorded for this relation. That
	 * single op typically maps to thousands of catalog deletes, so we force
	 * commit-time ANALYZE regardless of dataFileChangeCount.
	 */
	bool		forceCommitTimeAnalyze;
}			TableMetadataOperationTracker;

extern PGDLLEXPORT int CommitTimeCatalogAnalyzeThreshold;

extern PGDLLEXPORT void ConsumeTrackedIcebergMetadataChanges(bool isVerbose);
extern PGDLLEXPORT void PostAllRestCatalogRequests(void);
extern PGDLLEXPORT void TrackIcebergMetadataChangesInTx(Oid relationId, List *metadataOperationTypes);
extern PGDLLEXPORT void RecordRestCatalogRequestInTx(Oid relationId, RestCatalogOperationType operationType,
													 const char *body);
extern PGDLLEXPORT void ResetTrackedIcebergMetadataOperation(void);
extern PGDLLEXPORT void ResetRestCatalogRequests(void);
extern PGDLLEXPORT HTAB *GetTrackedIcebergMetadataOperations(void);
extern PGDLLEXPORT bool HasAnyTrackedIcebergMetadataChanges(void);
extern PGDLLEXPORT bool IsIcebergTableCreatedInCurrentTransaction(Oid relation);
extern PGDLLEXPORT void BindRelationToXactRestCatalog(Oid relationId);
extern PGDLLEXPORT void RegisterRestCatalogXactCaptureCallback(void);
