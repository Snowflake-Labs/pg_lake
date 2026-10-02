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

#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/metadata_spec.h"


/*
 * Predicate function to filter manifests.
 *
 * The predicate function should return true if the manifest should be included in the result.
 * Otherwise, it should return false.
 *
 * The predicate function can be NULL, in which case all manifests are included in the result.
 */
typedef bool (*ManifestPredicateFn) (IcebergManifest * manifest);

/*
 * IcebergManifestWriteContext holds the table state that the Iceberg spec
 * requires in the Avro key-value metadata of every manifest file.
 */
typedef struct IcebergManifestWriteContext
{
	int			formatVersion;

	/* the table's current schema, as JSON, and its id */
	int32_t		schemaId;
	const char *schemaJson;

	/* partition specs that a manifest may be written for */
	List	   *partitionSpecs;
}			IcebergManifestWriteContext;

/* predicates */
extern PGDLLEXPORT bool IsManifestOfFileContentAdd(IcebergManifest * manifest);
extern PGDLLEXPORT bool IsManifestOfFileContentDeletes(IcebergManifest * manifest);

/* read api */
extern PGDLLEXPORT List *FetchManifestsFromSnapshot(IcebergSnapshot * snapshot, ManifestPredicateFn manifestPredicateFn);

/* write api */
extern PGDLLEXPORT char *GenerateRemoteManifestPath(const char *location, const char *snapshotUUID, int manifestIndex, char *queryArguments);
extern PGDLLEXPORT int64_t UploadIcebergManifestToURI(List *manifestEntries, char *manifestURI,
													  IcebergManifestWriteContext * writeContext,
													  int32_t partitionSpecId,
													  IcebergManifestContentType contentType);
extern PGDLLEXPORT IcebergManifest * CreateNewIcebergManifest(IcebergSnapshot * snapshot,
															  int32_t partitionSpecId,
															  List *allTransforms,
															  int64 manifestFileSize,
															  IcebergManifestContentType contentType,
															  char *manifestPath,
															  List *manifestEntries);
