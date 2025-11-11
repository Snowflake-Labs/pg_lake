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

typedef struct TableMetadataOperationTracker
{
	Oid			relationId;

	bool		relationCreated;
	bool		relationAltered;
	bool		relationPartitionByChanged;
	bool		relationDataFileChanged;
	bool		relationManifestMergeRequested;
	bool		relationSnapshotExpirationRequested;
}			TableMetadataOperationTracker;

typedef enum RestCatalogOperationType
{
	REST_CATALOG_CREATE_ICEBERG_TABLE = 0,
	REST_CATALOG_ADD_SNAPSHOT = 1,
	REST_CATALOG_ADD_SCHEMA = 2,
	REST_CATALOG_ADD_PARTITION = 3,
}			RestCatalogOperationType;

extern PGDLLEXPORT void ConsumeTrackedIcebergMetadataChanges(void);
extern PGDLLEXPORT void PostAllRestCatalogRequests(void);
extern PGDLLEXPORT void TrackIcebergMetadataChangesInTx(Oid relationId, List *metadataOperationTypes);
extern PGDLLEXPORT void RecordRestCatalogRequestInTx(Oid relationId, RestCatalogOperationType operationType,
													 const char *catalogName, const char *catalogNamespace,
													 const char *catalogTableName, const char *body);
extern PGDLLEXPORT void ResetTrackedIcebergMetadataOperation(void);
extern PGDLLEXPORT void ResetRestCatalogRequests(void);
extern PGDLLEXPORT HTAB *GetTrackedIcebergMetadataOperations(void);
extern PGDLLEXPORT bool HasAnyTrackedIcebergMetadataChanges(void);
extern PGDLLEXPORT bool IsIcebergTableCreatedInCurrentTransaction(Oid relation);
