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

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/iceberg_type_json_serde.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/operations/find_referenced_files.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/string_utils.h"
#include "pg_lake/util/numeric.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/s3_writer_utils.h"

#include "access/relation.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/pg_type.h"
#include "catalog/pg_type_d.h"
#include "commands/comment.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parse_type.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"
#include "utils/wait_event.h"

#define ICEBERG_MAIN_BRANCH "main"

/* controlled via a GUC */
int			IcebergMaxSnapshotAge = 0;	/* seconds */

static void WriteMetadataJsonToTemporaryFile(IcebergTableMetadata * metadata, FILE *localFile);
static void AddIcebergSnapshotToMetadata(IcebergTableMetadata * metadata, IcebergSnapshot * newSnapshot);
static void DeleteUnreferencedFiles(Oid relationId, IcebergTableMetadata * metadata, IcebergSnapshot * expiredSnapshots,
									int expiredSnapshotCount, IcebergSnapshot * nonExpiredSnapshots, int nonExpiredSnapshotCount);
static void SetSnapshotReference(IcebergTableMetadata * metadata, uint64_t snapshotId);
static void GroupExpiredSnapshots(IcebergTableMetadata * metadata, IcebergSnapshot * expiredSnapshots, int *expiredSnapshotCount,
								  IcebergSnapshot * nonExpiredSnapshots, int *nonExpiredSnapshotCount);
static bool SnapshotAgeExceeded(IcebergSnapshot * snapshot, int64_t currentTimeMs, int maxAge);
static int32_t MaxSchemaId(IcebergTableSchema * schemas, size_t schemasLength);

/*
* GenerateEmptyTableMetadata generates an empty iceberg table metadata
* for the given location.
*
* We closely follow the reference implementation (spark) for empty
* table metadata generation.
*/
IcebergTableMetadata *
GenerateEmptyTableMetadata(char *location)
{
	IcebergTableMetadata *metadata = palloc0(sizeof(IcebergTableMetadata));

	metadata->format_version = 2;
	metadata->table_uuid = GenerateUUID();
	metadata->location = pstrdup(location);
	metadata->last_sequence_number = 0;

	/* convert timestamptz to Unix epoch milliseconds */
	metadata->last_updated_ms = PostgresTimestampToIcebergTimestampMs();
	metadata->current_snapshot_id = 0;
	metadata->current_schema_id = 0;

	metadata->schemas = NULL;
	metadata->schemas_length = 0;

	/* currently we only have the owner */
	metadata->properties_length = 1;
	metadata->properties = palloc0(sizeof(Property) * metadata->properties_length);

	Property   *ownerProperty = &metadata->properties[0];

	ownerProperty->key = "owner";
	ownerProperty->value = GetUserNameFromId(GetUserId(), false);

	metadata->partition_specs = NULL;
	metadata->partition_specs_length = 0;

	/* spark requires non-null sort orders, we provide an empty one */
	metadata->sort_orders = palloc0(sizeof(IcebergSortOrder) * 1);
	metadata->sort_orders_length = 1;
	metadata->sort_orders[0].order_id = 0;
	metadata->sort_orders[0].fields_length = 0;

	metadata->current_snapshot_id = -1;
	return metadata;
}


/*
* GenerateSnapshotLogEntries generates the snapshot log entries from the
* snapshots in the metadata.
*/
void
GenerateSnapshotLogEntries(IcebergTableMetadata * metadata)
{
	int			snapshotLogLength = metadata->snapshots_length;

	if (snapshotLogLength == 0)
	{
		metadata->snapshot_log = NULL;
		metadata->snapshot_log_length = 0;
		return;
	}

	metadata->snapshot_log = palloc0(sizeof(IcebergMetadataLogEntry) * snapshotLogLength);
	for (int snapshotIndex = 0; snapshotIndex < snapshotLogLength; snapshotIndex++)
	{
		IcebergSnapshotLogEntry *logEntry = &metadata->snapshot_log[snapshotIndex];
		IcebergSnapshot *snapshot = &metadata->snapshots[snapshotIndex];

		logEntry->timestamp_ms = snapshot->timestamp_ms;
		logEntry->snapshot_id = snapshot->snapshot_id;
	}

	metadata->snapshot_log_length = snapshotLogLength;
}


/*
* SetSnapshotReference set the snapshot reference in the metadata.
* We currently only support a single snapshot reference, so we always
* override the existing snapshot reference.
*/
static void
SetSnapshotReference(IcebergTableMetadata * metadata, uint64_t snapshotId)
{
	if (metadata->refs == NULL)
	{
		metadata->refs = palloc0(sizeof(SnapshotReference) * 1);
	}

	/* we currently only support a single snapshot reference */
	metadata->refs_length = 1;

	SnapshotReference *newEntry = &metadata->refs[metadata->refs_length - 1];

	/* we currently only have main branch snapshot, no branches or tags */
	newEntry->key = ICEBERG_MAIN_BRANCH;
	newEntry->key_length = strlen(newEntry->key);
	newEntry->snapshot_id = snapshotId;
	newEntry->type = SNAPSHOT_REFERENCE_TYPE_BRANCH;
}

/*
* AddIcebergSnapshotToMetadata adds given snapshot in the metadata.
*/
static void
AddIcebergSnapshotToMetadata(IcebergTableMetadata * metadata, IcebergSnapshot * newSnapshot)
{
	int			oldSize = metadata->snapshots_length;
	int			newSize = oldSize + 1;

	/*
	 * Always grow the list by one, we always add the new snapshot at the end
	 * of the list.
	 */
	if (oldSize == 0)
	{
		metadata->snapshots = palloc0(sizeof(IcebergSnapshot) * newSize);
	}
	else
	{
		metadata->snapshots = repalloc0(metadata->snapshots, sizeof(IcebergSnapshot) * oldSize, sizeof(IcebergSnapshot) * newSize);
	}

	/*
	 * Last slot is always reserved for the new snapshot.
	 */
	metadata->snapshots_length = newSize;
	metadata->snapshots[metadata->snapshots_length - 1] = *newSnapshot;
}

/*
 * RemoveOldSnapshotsFromMetadata expires old snapshots based on the retention policy.
 * It only does the in-memory operation, the caller is responsible for
 * persisting the changes.
 */
bool
RemoveOldSnapshotsFromMetadata(Oid relationId, IcebergTableMetadata * metadata, bool isVerbose)
{
	if (metadata->snapshots_length == 0)
	{
		/* no snapshots yet, not possible to trigger, but let's be defensive */
		return false;
	}

	/*
	 * We need to collect the snapshots that are not expired based on the
	 * retention policy. We could at most have snapshots_length snapshots.
	 * This is a bit inefficient to allocate snapshots_length memory for each
	 * snapshot, but it simplifies the code.
	 */
	IcebergSnapshot *expiredSnapshots =
		palloc0(sizeof(IcebergSnapshot) * metadata->snapshots_length);
	int			expiredSnapshotCount = 0;
	IcebergSnapshot *nonExpiredSnapshots =
		palloc0(sizeof(IcebergSnapshot) * metadata->snapshots_length);
	int			nonExpiredSnapshotCount = 0;

	GroupExpiredSnapshots(metadata, expiredSnapshots, &expiredSnapshotCount,
						  nonExpiredSnapshots, &nonExpiredSnapshotCount);

	if (expiredSnapshotCount == 0)
		/* no snapshots to expire */
		return false;

	/* we might expire all snapshots, always retain at least 1 snapshot */
	if (nonExpiredSnapshotCount == 0)
	{
		/* do not expire the remaining snapshot */
		expiredSnapshotCount--;

		if (expiredSnapshotCount == 0)
			/* only have a single snapshot, cannot expired it */
			return false;

		IcebergSnapshot *lastSnapshot = &metadata->snapshots[metadata->snapshots_length - 1];

		/* snapshots are always in time ordered */
		nonExpiredSnapshots[0] = *lastSnapshot;
		nonExpiredSnapshotCount = 1;
	}

	for (int snapshotIndex = 0; snapshotIndex < expiredSnapshotCount; snapshotIndex++)
	{
		ereport(isVerbose ? INFO : DEBUG1,
				(errmsg("expiring snapshot %" PRId64 " from %s",
						expiredSnapshots[snapshotIndex].snapshot_id,
						get_rel_name(relationId))));
	}

	DeleteUnreferencedFiles(relationId, metadata, expiredSnapshots, expiredSnapshotCount, nonExpiredSnapshots, nonExpiredSnapshotCount);

	metadata->snapshots = nonExpiredSnapshots;
	metadata->snapshots_length = nonExpiredSnapshotCount;

	return true;
}


/*
* DeleteUnreferencedFiles firsts finds the unreferenced files for the expired snapshots
* and then deletes them from the remote storage.
*/
static void
DeleteUnreferencedFiles(Oid relationId, IcebergTableMetadata * metadata, IcebergSnapshot * expiredSnapshots, int expiredSnapshotCount,
						IcebergSnapshot * nonExpiredSnapshots, int nonExpiredSnapshotCount)
{
	TimestampTz orphanedAt = GetCurrentTransactionStartTimestamp();

	List	   *unreferencedFiles =
		FindUnreferencedFilesForSnapshots(expiredSnapshots, expiredSnapshotCount,
										  nonExpiredSnapshots, nonExpiredSnapshotCount);
	ListCell   *unreferencedFileCell = NULL;

	foreach(unreferencedFileCell, unreferencedFiles)
	{
		char	   *unreferencedFile = lfirst(unreferencedFileCell);

		InsertDeletionQueueRecord(unreferencedFile, relationId, orphanedAt);
	}
}


/*
* UpdateLatestSnapshot updates the table metadata with the values from the
* new snapshot and also sets the last_updated_ms.
*/
void
UpdateLatestSnapshot(IcebergTableMetadata * tableMetadata, IcebergSnapshot * newSnapshot)
{
	/* add the new snapshot to the metadata */
	AddIcebergSnapshotToMetadata(tableMetadata, newSnapshot);

	/* set the snapshot reference */
	SetSnapshotReference(tableMetadata, newSnapshot->snapshot_id);

	/* version is used provide an ordered history for a given metadata */
	tableMetadata->current_snapshot_id = newSnapshot->snapshot_id;
}


/*
 * GenerateInitialIcebergTableMetadataPath generates the initial iceberg table metadata path.
 */
char *
GenerateInitialIcebergTableMetadataPath(Oid relationId)
{
	char	   *queryArguments = "";
	char	   *location = GetWritableTableLocation(relationId, &queryArguments);

	int			version = 0;

	return GenerateRemoteMetadataFilePath(version, location, queryArguments);
}


/*
 * GenerateInitialIcebergTableMetadata generates an empty iceberg table metadata
 * for the given relation.
 */
IcebergTableMetadata *
GenerateInitialIcebergTableMetadata(Oid relationId)
{
	char	   *queryArguments = "";
	char	   *location = GetWritableTableLocation(relationId, &queryArguments);
	IcebergTableMetadata *metadata = GenerateEmptyTableMetadata(location);

	return metadata;
}

/*
 * GenerateRemoteMetadataFilePath generates a remote metadata file path from given parameters.
 */
char *
GenerateRemoteMetadataFilePath(int version, const char *location, char *queryArguments)
{
	StringInfo	remoteFilePath = makeStringInfo();

	appendStringInfo(remoteFilePath, "%s/metadata/%05d-%s.metadata.json%s",
					 location, version, GenerateUUID(), queryArguments);

	return remoteFilePath->data;
}

/*
* WriteMetadataJsonToTemporaryFile writes the given
* iceberg table metadata to a temporary file.
*/
static void
WriteMetadataJsonToTemporaryFile(IcebergTableMetadata * metadata, FILE *file)
{
	char	   *metadataJson = WriteIcebergTableMetadataToJson(metadata);
	int			contentLength = strlen(metadataJson);
	int			writtenLength = fwrite(metadataJson, 1, contentLength, file);

	if (writtenLength != contentLength)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to write metadata file to temporary file")));
	}
	fflush(file);
}

/*
* UploadTableMetadataToURI writes the given iceberg table metadata to a temporary file
* and uploads it to given uri.
*/
void
UploadTableMetadataToURI(IcebergTableMetadata * tableMetadata, char *metadataURI)
{
	char	   *localFilePath = GenerateTempFileName(PG_LAKE_ICEBERG, true);

	/* open the destination file for writing */
	FILE	   *localFile = AllocateFile(localFilePath, PG_BINARY_W);

	if (localFile == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create file \"%s\": %m",
							   localFilePath)));
	}

	WriteMetadataJsonToTemporaryFile(tableMetadata, localFile);

	CopyLocalFileToS3WithCleanupOnAbort(localFilePath, metadataURI);

	FreeFile(localFile);
}

/*
* AdjustAndRetainMetadataLogs appends a new entry to the metadata log.
* The metadata log is used to track the changes to the metadata and
* the appended entry is previous metadata file path and the timestamp.
*
* The function also applies retention on the metadata logs. We keep at most
* snapshotLogLength entries in the metadata log. The oldest entries are
* removed if the new entry exceeds the limit.
*/
void
AdjustAndRetainMetadataLogs(IcebergTableMetadata * metadata, char *prevMetadataPath,
							size_t snapshotLogLength, int64_t prev_last_updated_ms)
{
	/* no snapshots, nothing to do */
	if (snapshotLogLength == 0)
	{
		metadata->metadata_log = NULL;
		metadata->metadata_log_length = 0;
		return;
	}

	int			oldMetadataLength = metadata->metadata_log_length;
	int			newMetadataLength = oldMetadataLength + 1;

	/* keep at most snapshotLogLength entries */
	if (newMetadataLength > snapshotLogLength)
		newMetadataLength = snapshotLogLength;

	/*
	 * Determine how many entries to copy from the old array, always keep the
	 * last entry for the newly added log.
	 */
	int			entriesToCopy = Min(oldMetadataLength, newMetadataLength - 1);

	IcebergMetadataLogEntry *newMetadataLog = palloc0(sizeof(IcebergMetadataLogEntry) * newMetadataLength);

	if (entriesToCopy > 0)
	{
		/*
		 * If the old array is smaller than the new array, we copy
		 * oldMetadataLength from the old array to the new array. If the old
		 * array and new array size are the same, we copy oldMetadataLength -1
		 * from the old array to the new array. If the old array is larger
		 * than the new array, we copy the last newMetadataLength -1 entries
		 * from the old array to the new array.
		 */
		memcpy(newMetadataLog, &metadata->metadata_log[oldMetadataLength - entriesToCopy],
			   sizeof(IcebergMetadataLogEntry) * entriesToCopy);
	}

	/* Update the metadata with the new log array and length */
	metadata->metadata_log = newMetadataLog;
	metadata->metadata_log_length = newMetadataLength;

	/* Append the new entry at the end of the array */
	IcebergMetadataLogEntry *newEntry = &metadata->metadata_log[newMetadataLength - 1];

	newEntry->timestamp_ms = prev_last_updated_ms;
	newEntry->metadata_file = pstrdup(prevMetadataPath);
}


static void
GroupExpiredSnapshots(IcebergTableMetadata * metadata, IcebergSnapshot * expiredSnapshots, int *expiredSnapshotCount,
					  IcebergSnapshot * nonExpiredSnapshots, int *nonExpiredSnapshotCount)
{
	IcebergSnapshot *allSnapshots = metadata->snapshots;
	int			allSnapshotCount = metadata->snapshots_length;

	int64_t		currentTimeMs = PostgresTimestampToIcebergTimestampMs();

	*expiredSnapshotCount = 0;
	*nonExpiredSnapshotCount = 0;

	for (int snapshotIndex = 0; snapshotIndex < allSnapshotCount; snapshotIndex++)
	{
		if (SnapshotAgeExceeded(&allSnapshots[snapshotIndex], currentTimeMs, IcebergMaxSnapshotAge))
		{
			expiredSnapshots[(*expiredSnapshotCount)++] = allSnapshots[snapshotIndex];
		}
		else
		{
			nonExpiredSnapshots[(*nonExpiredSnapshotCount)++] = allSnapshots[snapshotIndex];
		}
	}
}

/*
* SnapshotAgeExceeded checks whether the snapshot is too old based on the
* current time and the maximum age.
*/
static bool
SnapshotAgeExceeded(IcebergSnapshot * snapshot, int64_t currentTimeMs, int maxAge)
{
	uint64_t	maxAgeMs = (uint64_t) maxAge * 1000;

	return (currentTimeMs - snapshot->timestamp_ms) > maxAgeMs;
}


/*
* FindLargestPartitionFieldId finds the largest partition field ID in the
* given partition spec. These specIds are already assigned in the
* IcebergPartitionSpec, so we just need to find the largest one.
*/
int
FindLargestPartitionFieldId(IcebergPartitionSpec * newSpec)
{
	int			largestFieldId = 0;

	for (int i = 0; i < newSpec->fields_length; i++)
	{
		IcebergPartitionSpecField *field = &newSpec->fields[i];

		if (field->field_id > largestFieldId)
			largestFieldId = field->field_id;
	}

	/*
	 * Sanity check: the largest one should be greater than
	 * ICEBERG_PARTITION_FIELD_ID_START.
	 */
	Assert(largestFieldId > ICEBERG_PARTITION_FIELD_ID_START);

	return largestFieldId;
}


/*
* AppendCurrentPostgresSchema creates a new schema for the given relationId
* and appends it to the metadata.
*/
void
AppendCurrentPostgresSchema(Oid relationId, IcebergTableMetadata * metadata,
							DataFileSchema * schema)
{
	int			currentSchemaLength = metadata->schemas_length;

	IcebergTableSchema *newSchema = RebuildIcebergSchemaFromDataFileSchema(relationId, schema, &metadata->last_column_id);

	/* now, add this newSchema to metadata->schemas */
	if (currentSchemaLength == 0)
	{
		metadata->schemas = palloc0(sizeof(IcebergTableSchema));
	}
	else
	{
		metadata->schemas = repalloc(metadata->schemas, sizeof(IcebergTableSchema) * (currentSchemaLength + 1));
	}

	/* first schema should always start with id=0 */
	newSchema->schema_id = (currentSchemaLength == 0) ? 0 : MaxSchemaId(metadata->schemas, currentSchemaLength) + 1;
	metadata->schemas[currentSchemaLength] = *newSchema;
	metadata->schemas_length = currentSchemaLength + 1;
	metadata->current_schema_id = newSchema->schema_id;
}


/*
* MaxSchemaId finds the maximum schema ID from the given schemas.
*/
static int32_t
MaxSchemaId(IcebergTableSchema * schemas, size_t schemasLength)
{
	int32_t		maxSchemaId = -1;

	for (size_t i = 0; i < schemasLength; i++)
	{
		if (schemas[i].schema_id > maxSchemaId)
		{
			maxSchemaId = schemas[i].schema_id;
		}
	}

	Assert(maxSchemaId != -1);
	return maxSchemaId;
}

/*
* AppendPartitionSpec appends given partition spec to the metadata.
*/
void
AppendPartitionSpec(IcebergTableMetadata * metadata, IcebergPartitionSpec * newSpec)
{
	int			currentPartitionSpecLength = metadata->partition_specs_length;

	if (currentPartitionSpecLength == 0)
	{
		metadata->partition_specs = palloc0(sizeof(IcebergPartitionSpec));
	}
	else
	{
		metadata->partition_specs = repalloc(metadata->partition_specs, sizeof(IcebergPartitionSpec) * (currentPartitionSpecLength + 1));
	}

	metadata->partition_specs[currentPartitionSpecLength] = *newSpec;
	metadata->partition_specs_length = currentPartitionSpecLength + 1;

	if (newSpec->fields_length > 0)
	{
		/* only assigned if we still have a partitioning */
		metadata->last_partition_id = FindLargestPartitionFieldId(newSpec);
	}
}


/*
 * GetAllIcebergPartitionSpecsFromTableMetadata returns all partition specs
 * from the given metadata as a List.
 */
List *
GetAllIcebergPartitionSpecsFromTableMetadata(IcebergTableMetadata * metadata)
{
	List	   *partitionSpecs = NIL;

	for (int specIndex = 0; specIndex < metadata->partition_specs_length; specIndex++)
	{
		IcebergPartitionSpec *spec = &metadata->partition_specs[specIndex];

		partitionSpecs = lappend(partitionSpecs, spec);
	}

	return partitionSpecs;
}
