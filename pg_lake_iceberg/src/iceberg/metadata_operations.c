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
#include "access/xact.h"
#include "common/hashfn.h"
#include "utils/array.h"
#include "utils/builtins.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/iceberg/operations/find_referenced_files.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/string_utils.h"
#include "pg_lake/util/s3_writer_utils.h"

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_enum.h"
#include "catalog/pg_type.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/inval.h"

/*
 * IcebergSnapshotBuilder is used to create a new snapshot from a base
 * snapshot via a series of metadata operations.
 */
typedef struct IcebergSnapshotBuilder
{
	/* snapshot which we're modifying */
	IcebergSnapshot *baseSnapshot;

	/* new snapshot that we're building (generated upfront to obtain new IDs) */
	IcebergSnapshot *newSnapshot;

	/* data files added to the snapshot */
	HTAB	   *dataEntries;

	/* positional delete files */
	HTAB	   *positionalDeleteEntries;

	/* data or delete files to be removed */
	List	   *removedEntries;

	/* remove all data files (truncate) */
	bool		removeAllEntries;

	/* whether to apply manifest compaction */
	bool		applyManifestCompaction;

	/* a DDL has changed the iceberg schema or set to an existing schema */
	bool		regenerateSchema;

	/* a DDL has changed partition specs */
	bool		regeneratePartitionSpec;
	List	   *partitionSpecs;
	int32_t		defaultSpecId;

	/* table is a new one */
	bool		createTable;

	/* whether to expire old snapshots */
	bool		expireOldSnapshots;

	/* new schema */
	DataFileSchema *schema;
	/* a DDL has set to an existing schema */
	int32_t		schemaId;

	/* snapshot operation */
	SnapshotOperation operation;
}			IcebergSnapshotBuilder;


/*
 * PartitionSpecManifestsEntries is hash entry to group manifest entries
 * by partition spec id.
 */
typedef struct PartitionSpecManifestsEntries
{
	int32_t		partitionSpecId;
	List	   *manifestEntries;
}			PartitionSpecManifestsEntries;

static IcebergSnapshotBuilder * CreateIcebergSnapshotBuilder(IcebergTableMetadata * metadata, List *metadataOperations);
static void SetSnapshotOperation(IcebergSnapshot * snapshot, SnapshotOperation operation);
static SnapshotOperation SnapshotOperationSummary(List *metadataOperations);
static void ProcessIcebergMetadataOperations(Oid relationId, List *metadataOperations,
											 IcebergSnapshotBuilder * builder);
static IcebergManifestEntry * CreateIcebergManifestEntryFromMetadataOperation(TableMetadataOperation * operation,
																			  int64_t newSnapshotId,
																			  int64_t sequenceNumber);
static IcebergSnapshot * FinalizeNewSnapshot(IcebergSnapshotBuilder * builder,
											 Oid relationId,
											 const char *metadataLocation,
											 int32_t currentSchemaId,
											 List *allTransforms,
											 bool isVerbose);
static List *CreateNewManifestsForDeletedEntries(List *allManifestEntries, List *deletedManifestEntries,
												 IcebergSnapshot * newSnapshot, const char *metadataLocation,
												 const char *snapshotUUID, int *manifestIndex,
												 int partitionSpecId, List *partitionTransforms,
												 IcebergManifestContentType contentType);
static IcebergSnapshot * CopyIcebergSnapshot(IcebergSnapshot * src);
static Property * CopyPropertiesArray(Property * src, int length);
static HTAB *MakePartitionManifestEntryHash(void);
static void AddManifestEntryToHash(HTAB *hash, int32 partitionSpecId,
								   IcebergManifestEntry * entry);
static bool HasCreateTableOperation(List *metadataOperations);
static void DeleteInProgressManifests(Oid relationId, List *manifests);


/*
 * ApplyIcebergMetadataChanges applies the given metadata operations to the
 * iceberg metadata for the given relation.
 */
void
ApplyIcebergMetadataChanges(Oid relationId, List *metadataOperations, List *allTransforms, bool isVerbose)
{
	Assert(metadataOperations != NIL);

#ifdef USE_ASSERT_CHECKING
	/* we already made sure we should apply the changes at tracking them */
	List	   *metadataOperationTypes = GetMetadataOperationTypes(metadataOperations);

	Assert(!ShouldSkipMetadataChangeToIceberg(metadataOperationTypes));
#endif

	/* read the iceberg metadata for the table */
	bool		forUpdate = true;
	char	   *metadataPath = GetIcebergMetadataLocation(relationId, forUpdate);

	bool		createNewTable = HasCreateTableOperation(metadataOperations);

	IcebergTableMetadata *metadata = (createNewTable) ?
		GenerateInitialIcebergTableMetadata(relationId) :
		ReadIcebergTableMetadata(metadataPath);

	int64_t		prevLastUpdatedMs = metadata->last_updated_ms;

	/*
	 * prepare to use a new sequence number (might not be committed if there's
	 * no change)
	 */
	metadata->last_sequence_number = createNewTable ? 0 : metadata->last_sequence_number + 1;
	metadata->last_updated_ms = PostgresTimestampToIcebergTimestampMs();

	IcebergSnapshotBuilder *builder = CreateIcebergSnapshotBuilder(metadata, metadataOperations);

	ProcessIcebergMetadataOperations(relationId, metadataOperations, builder);

	if (builder->createTable || builder->regenerateSchema)
	{
		if (builder->schema != NULL)
			AppendCurrentPostgresSchema(relationId, metadata, builder->schema);
		else
			metadata->current_schema_id = builder->schemaId;
	}

	if (builder->createTable || builder->regeneratePartitionSpec)
	{
		metadata->default_spec_id = builder->defaultSpecId;

		ListCell   *newSpecCell = NULL;

		foreach(newSpecCell, builder->partitionSpecs)
		{
			IcebergPartitionSpec *newSpec = lfirst(newSpecCell);

			AppendPartitionSpec(metadata, newSpec);
		}
	}

	/* whether to create a new version of the Iceberg table */
	bool		createNewSnapshot = false;

	IcebergSnapshot *newSnapshot = FinalizeNewSnapshot(builder,
													   relationId,
													   metadata->location,
													   metadata->current_schema_id,
													   allTransforms,
													   isVerbose);

	if (newSnapshot != NULL)
	{
		/* update metadata's snapshot */
		UpdateLatestSnapshot(metadata, newSnapshot);

		createNewSnapshot = true;
	}

	/* if we need to expire old snapshots, we do it here */
	if (builder->expireOldSnapshots)
	{
		bool		expiredSnapshots =
			RemoveOldSnapshotsFromMetadata(relationId, metadata, isVerbose);

		if (expiredSnapshots)
			createNewSnapshot = true;
	}

	/* if there were no changes to the Iceberg table, we are done */
	if (!createNewSnapshot && !createNewTable)
		return;

	/* add the new snapshot to the snapshot log */
	GenerateSnapshotLogEntries(metadata);

	/*
	 * append the current metadata log before uploading new one. There is no
	 * metadata log for new tables.
	 */
	if (!createNewTable)
		AdjustAndRetainMetadataLogs(metadata, metadataPath, metadata->snapshots_length, prevLastUpdatedMs);

	int			version = metadata->last_sequence_number;

	/*
	 * For newly created iceberg tables, we have generated the metadata path
	 * and already inserted into iceberg tables. Here, we skip re-generating
	 * another path, simply use the path from the catalog.
	 */
	char	   *newMetadataPath = (createNewTable) ? metadataPath : GenerateRemoteMetadataFilePath(version, metadata->location, "");

	char	   *previousMetadataPath =
		GetIcebergCatalogPreviousMetadataLocation(relationId, forUpdate);

	/* finally, update the table metadata and catalog */
	UploadTableMetadataToURI(metadata, newMetadataPath);
	UpdateInternalCatalogMetadataLocation(relationId, newMetadataPath, (createNewTable) ? NULL : metadataPath);

	if (previousMetadataPath)
	{
		TimestampTz orphanedAt = GetCurrentTransactionStartTimestamp();

		/*
		 * There is (currently) no value in retaining the old metadata.json
		 * files.
		 */
		InsertDeletionQueueRecord(previousMetadataPath, relationId, orphanedAt);
	}

	TriggerCatalogExportIfObjectStoreTable(relationId);
}


/*
 * CreateIcebergSnapshotBuilder prepares a snapshot builder that keeps track
 * of all the changes to the current snapshot.
 */
static IcebergSnapshotBuilder *
CreateIcebergSnapshotBuilder(IcebergTableMetadata * metadata, List *metadataOperations)
{
	IcebergSnapshotBuilder *builder = palloc0(sizeof(IcebergSnapshotBuilder));

	/*
	 * Get the current snapshot, and also prepare new snapshot for adding
	 * files. We take a copy of the current snapshot because there is no
	 * guarantee that we will retain the current snapshot in the metadata.
	 */
	builder->baseSnapshot = CopyIcebergSnapshot(GetCurrentSnapshot(metadata, true));
	builder->newSnapshot = CreateNewIcebergSnapshot(metadata);

	builder->dataEntries = MakePartitionManifestEntryHash();
	builder->positionalDeleteEntries = MakePartitionManifestEntryHash();

	builder->operation = SnapshotOperationSummary(metadataOperations);

	return builder;
}


/*
* MakePartitionManifestEntryHash creates a hash table for the partition
* manifest entries. The hash table is used to keep track of partitionSpecId <->asm
* manifest entries.
*/
static HTAB *
MakePartitionManifestEntryHash(void)
{
	HASHCTL		hashInfo;

	hashInfo.keysize = sizeof(int32_t);
	hashInfo.entrysize = sizeof(PartitionSpecManifestsEntries);
	hashInfo.hash = uint32_hash;
	hashInfo.hcxt = CurrentMemoryContext;

	uint32		hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	return hash_create("Iceberg partitioned manifest entry hash for snapshot builder",
					   32, &hashInfo, hashFlags);
}

/*
* AddManifestEntryToHash adds a manifest entry to the hash table.
*/
static void
AddManifestEntryToHash(HTAB *hash, int32 partitionSpecId, IcebergManifestEntry * entry)
{
	bool		found = false;
	PartitionSpecManifestsEntries *manifestEntries = hash_search(hash, &partitionSpecId, HASH_ENTER, &found);

	if (!found)
	{
		manifestEntries->partitionSpecId = partitionSpecId;
		manifestEntries->manifestEntries = NIL;
	}

	manifestEntries->manifestEntries = lappend(manifestEntries->manifestEntries, entry);
}


/*
* SetSnapshotOperation sets the operation of the snapshot.
*/
static void
SetSnapshotOperation(IcebergSnapshot * snapshot, SnapshotOperation operation)
{
	Property   *summary = palloc0(sizeof(Property));

	summary->key = "operation";

	switch (operation)
	{
		case SNAPSHOT_OPERATION_APPEND:
			summary->value = "append";
			break;
		case SNAPSHOT_OPERATION_REPLACE:
			summary->value = "replace";
			break;
		case SNAPSHOT_OPERATION_OVERWRITE:
			summary->value = "overwrite";
			break;
		case SNAPSHOT_OPERATION_DELETE:
			summary->value = "delete";
			break;
		default:
			ereport(ERROR, (errmsg("Unsupported snapshot operation: %d", operation)));
	}

	snapshot->summary = summary;
	snapshot->summary_length = 1;
}

/*
* SnapshotOperationSummary determines the summary of the given metadata operations.
* As per iceberg spec:
* The snapshot summary's operation field is used by some operations, like snapshot expiration, to skip processing certain snapshots. Possible operation values are:
* - append -- Only data files were added and no files were removed.
* - replace -- Data and delete files were added and removed without changing table data; i.e., compaction, changing the data file format, or relocating data files.
* - overwrite -- Data and delete files were added and removed in a logical overwrite operation.
* - delete -- Data files were removed and their contents logically deleted and/or delete files were added to delete rows.
*/
static SnapshotOperation
SnapshotOperationSummary(List *metadataOperations)
{
	SnapshotOperation currentSummary = SNAPSHOT_OPERATION_INVALID;

	ListCell   *operationCell = NULL;

	foreach(operationCell, metadataOperations)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		switch (operation->type)
		{
			case DATA_FILE_ADD:
				{
					if (operation->content == CONTENT_DATA)
					{
						if (currentSummary == SNAPSHOT_OPERATION_INVALID ||
							currentSummary == SNAPSHOT_OPERATION_APPEND)
						{
							currentSummary = SNAPSHOT_OPERATION_APPEND;
						}
						else
						{
							currentSummary = SNAPSHOT_OPERATION_OVERWRITE;
						}
					}
					else if (operation->content == CONTENT_POSITION_DELETES)
					{
						if (currentSummary == SNAPSHOT_OPERATION_INVALID ||
							currentSummary == SNAPSHOT_OPERATION_DELETE)
						{
							currentSummary = SNAPSHOT_OPERATION_DELETE;
						}
						else
						{
							currentSummary = SNAPSHOT_OPERATION_OVERWRITE;
						}
					}
					else
					{
						ereport(ERROR, (errmsg("Unsupported operation content type: %d", operation->content)));
					}

					break;
				}
			case DATA_FILE_REMOVE_ALL:
			case DATA_FILE_REMOVE:
				{
					if (currentSummary == SNAPSHOT_OPERATION_INVALID ||
						currentSummary == SNAPSHOT_OPERATION_DELETE)
					{
						currentSummary = SNAPSHOT_OPERATION_DELETE;
					}
					else
					{
						currentSummary = SNAPSHOT_OPERATION_OVERWRITE;
					}
					break;
				}
			case DATA_FILE_MERGE_MANIFESTS:
			case EXPIRE_OLD_SNAPSHOTS:
				{
					if (currentSummary == SNAPSHOT_OPERATION_INVALID ||
						currentSummary == SNAPSHOT_OPERATION_REPLACE)
					{
						currentSummary = SNAPSHOT_OPERATION_REPLACE;
					}
					else
					{
						currentSummary = SNAPSHOT_OPERATION_OVERWRITE;
					}
					break;
				}

			case TABLE_DDL:
			case TABLE_PARTITION_BY:
				{
					/*
					 * Spark doesn't push a new snapshot for the schema
					 * changes. But we do and we treat this as an overwrite,
					 * which is the highest level.
					 */
					currentSummary = SNAPSHOT_OPERATION_OVERWRITE;
					break;
				}

				/* these do not affect the Iceberg metadata */
			case DATA_FILE_UPDATE_DELETED_ROW_COUNT:
			case DATA_FILE_ADD_DELETE_MAPPING:
			case DATA_FILE_ADD_ROW_ID_MAPPING:
			case DATA_FILE_DROP_TABLE:
			case TABLE_CREATE:
				{
					break;
				}

			default:
				ereport(ERROR, (errmsg("Unsupported operation type: %d", operation->type)));
		}
	}

	return currentSummary;
}


/*
* ProcessIcebergMetadataOperations creates the manifest entries for the given metadata operations.
*/
static void
ProcessIcebergMetadataOperations(Oid relationId, List *metadataOperations,
								 IcebergSnapshotBuilder * builder)
{
	ListCell   *operationCell = NULL;

	foreach(operationCell, metadataOperations)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		switch (operation->type)
		{
			case DATA_FILE_ADD:
				{
					IcebergManifestEntry *manifestEntry =
						CreateIcebergManifestEntryFromMetadataOperation(operation,
																		builder->newSnapshot->snapshot_id,
																		builder->newSnapshot->sequence_number);

					if (operation->content == CONTENT_DATA)
					{
						AddManifestEntryToHash(builder->dataEntries,
											   operation->partitionSpecId,
											   manifestEntry);
					}
					else if (operation->content == CONTENT_POSITION_DELETES)
					{
						AddManifestEntryToHash(builder->positionalDeleteEntries,
											   operation->partitionSpecId,
											   manifestEntry);
					}
					else
					{
						ereport(ERROR, (errmsg("Unsupported operation content type: %d", operation->content)));
					}

					break;
				}

			case DATA_FILE_REMOVE:
				{
					IcebergManifestEntry *manifestEntry =
						CreateIcebergManifestEntryFromMetadataOperation(operation,
																		builder->newSnapshot->snapshot_id,
																		builder->newSnapshot->sequence_number);

					builder->removedEntries = lappend(builder->removedEntries, manifestEntry);

					break;
				}

			case DATA_FILE_ADD_DELETE_MAPPING:
				{
					/*
					 * Iceberg metadata doesn't track delete mappings, so
					 * skip.
					 */
					break;
				}

			case DATA_FILE_ADD_ROW_ID_MAPPING:
				{
					/*
					 * Iceberg metadata doesn't track row id mappings, so
					 * skip.
					 */
					break;
				}

			case DATA_FILE_UPDATE_DELETED_ROW_COUNT:
				{
					/*
					 * Iceberg metadata doesn't track deleted row counts, so
					 * skip
					 */
					break;
				}

			case DATA_FILE_MERGE_MANIFESTS:
				{
					/*
					 * We are requested to merge manifests, and we'll handle
					 * this operation in TrackIcebergMetadataChangesInTx().
					 */
					builder->applyManifestCompaction = true;

					break;
				}

			case TABLE_CREATE:
				{
					/*
					 * we expect one TABLE_CREATE command for all Postgres
					 * commands that affect iceberg schema and partition spec
					 * if the table is created in the same transaction
					 */
					Assert(!builder->createTable);

					builder->createTable = true;
					builder->schema = operation->newSchema;
					builder->partitionSpecs = operation->partitionSpecs;
					builder->defaultSpecId = operation->defaultSpecId;

					break;
				}

			case TABLE_DDL:
				{
					/*
					 * we expect one TABLE_DDL command for all Postgres
					 * commands that affect iceberg schema during the
					 * transaction
					 */
					Assert(!builder->regenerateSchema);

					builder->regenerateSchema = true;

					/*
					 * We are requested to update the table schema, and we'll
					 * handle this operation in
					 * TrackIcebergMetadataChangesInTx().
					 */

					if (operation->ddlSchemaEffect == DDL_EFFECT_ADD_SCHEMA)
					{
						/* these two are mutually exclusive */
						Assert(operation->existingSchemaId == -1);
						Assert(operation->newSchema != NULL);

						builder->schema = operation->newSchema;
					}
					else if (operation->ddlSchemaEffect == DDL_EFFECT_SET_EXISTING_SCHEMA)
					{
						/* these two are mutually exclusive */
						Assert(operation->existingSchemaId != -1);
						Assert(operation->newSchema == NULL);
						builder->schemaId = operation->existingSchemaId;
					}
					else
					{
						ereport(ERROR, (errmsg("Unsupported DDL schema effect: %d", operation->ddlSchemaEffect)));
					}

					break;
				}

			case TABLE_PARTITION_BY:
				{
					/*
					 * We expect one TABLE_PARTITION_BY command at a time for
					 * all Postgres commands that affect iceberg partition
					 * spec during the transaction. This is already limited by
					 * Postgres, such that even if you provide multiple SET
					 * partition_by, Postgres only keeps the last one.
					 */
					Assert(!builder->regeneratePartitionSpec);

					/*
					 * We are requested to update the partition spec, and
					 * we'll handle this operation in
					 * TrackIcebergMetadataChangesInTx().
					 */
					builder->regeneratePartitionSpec = true;
					builder->partitionSpecs = operation->partitionSpecs;
					builder->defaultSpecId = operation->defaultSpecId;

					break;
				}

			case EXPIRE_OLD_SNAPSHOTS:
				{
					/*
					 * We are requested to expire old snapshots, and we'll
					 * handle this operation in
					 * TrackIcebergMetadataChangesInTx().
					 */
					builder->expireOldSnapshots = true;
					break;
				}

			case DATA_FILE_DROP_TABLE:
				{
					/*
					 * We are not requested to remove all files from the
					 * iceberg metadata, but only from the pg_lake catalog. We
					 * skip this operation.
					 */
					break;
				}

			case DATA_FILE_REMOVE_ALL:
				{
					builder->removeAllEntries = true;
					break;
				}

			default:
				ereport(ERROR, (errmsg("Unsupported operation type: %d", operation->type)));
		}
	}
}


/*
* FinalizeNewSnapshot creates a new snapshot from an IcebergSnapshotBuilder.
*
* It creates new manifest files for the new data files and positional
* delete files and appends them to the existing manifest files in the current
* snapshot. If needed, we also apply manifest compaction.
*/
static IcebergSnapshot *
FinalizeNewSnapshot(IcebergSnapshotBuilder * builder, Oid relationId, const char *metadataLocation,
					int32_t currentSchemaId, List *allTransforms, bool isVerbose)
{
	IcebergSnapshot *currentSnapshot = builder->baseSnapshot;
	IcebergSnapshot *newSnapshot = builder->newSnapshot;
	HTAB	   *newDataManifestEntries = builder->dataEntries;
	HTAB	   *newPositionalDeleteManifestEntries = builder->positionalDeleteEntries;
	List	   *removedEntries = builder->removedEntries;
	bool		removeAllEntries = builder->removeAllEntries;
	bool		applyManifestCompaction = builder->applyManifestCompaction;

	int64_t		snapshotId = newSnapshot->snapshot_id;
	const char *snapshotUUID = GenerateUUID();

	/* within the new same snapshot, we might have multiple manifests */
	int			manifestIndex = 0;
	int			manifestListIndex = 1;

	/* regenerate schema or partition spec should always create new snapshot */
	bool		createNewSnapshot =
		builder->regenerateSchema || builder->regeneratePartitionSpec;

	/*
	 * Always create a new manifest file for the new data and positional
	 * delete file(s) and append it to the existing data files in the current
	 * snapshot.
	 */
	const List *existingDataManifests =
		FetchManifestsFromSnapshot(currentSnapshot, IsManifestOfFileContentAdd);
	const List *existingDeleteManifests =
		FetchManifestsFromSnapshot(currentSnapshot, IsManifestOfFileContentDeletes);

	/* do not modify existing manifests */
	List	   *finalDataManifestList = list_copy(existingDataManifests);
	List	   *finalDeleteManifestList = list_copy(existingDeleteManifests);

	if (applyManifestCompaction || EnableManifestMergeOnWrite)
	{
		/*
		 * RemoveDeletedManifestEntries goes through old snapshot's manifests
		 * and removes any entries that are marked as deleted.
		 */
		bool		anyDataManifestModified = RemoveDeletedManifestEntries(currentSnapshot, allTransforms, &finalDataManifestList,
																		   ICEBERG_MANIFEST_FILE_CONTENT_DATA, metadataLocation,
																		   snapshotUUID, isVerbose, &manifestIndex);

		bool		anyDeleteManifestModified = RemoveDeletedManifestEntries(currentSnapshot, allTransforms, &finalDeleteManifestList,
																			 ICEBERG_MANIFEST_FILE_CONTENT_DELETES, metadataLocation,
																			 snapshotUUID, isVerbose, &manifestIndex);

		if (anyDataManifestModified || anyDeleteManifestModified)
			createNewSnapshot = true;
	}

	/*
	 * Iterate on the newDataManifestEntries HTAB, which is already grouped by
	 * partitionSpecId. For each partitionSpecId, create a new manifest.
	 */
	if (hash_get_num_entries(newDataManifestEntries) > 0)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, newDataManifestEntries);
		PartitionSpecManifestsEntries *entry = NULL;

		while ((entry = hash_seq_search(&status)) != NULL)
		{
			int32		partitionSpecId = entry->partitionSpecId;
			List	   *manifestEntries = entry->manifestEntries;

			char	   *remoteManifestPath =
				GenerateRemoteManifestPath(metadataLocation,
										   snapshotUUID,
										   manifestIndex++, "");

			int64_t		manifestSize = UploadIcebergManifestToURI(manifestEntries, remoteManifestPath);

			IcebergManifest *newDataManifest =
				CreateNewIcebergManifest(newSnapshot, partitionSpecId, allTransforms,
										 manifestSize, ICEBERG_MANIFEST_FILE_CONTENT_DATA,
										 remoteManifestPath, manifestEntries);

			finalDataManifestList = lappend(finalDataManifestList, newDataManifest);

			createNewSnapshot = true;
		}
	}

	/*
	 * Iterate on the newPositionalDeleteManifestEntries HTAB, which is
	 * already grouped by partitionSpecId. For each partitionSpecId, create a
	 * new manifest.
	 */
	if (hash_get_num_entries(newPositionalDeleteManifestEntries) > 0)
	{
		HASH_SEQ_STATUS status;

		hash_seq_init(&status, newPositionalDeleteManifestEntries);
		PartitionSpecManifestsEntries *entry = NULL;

		while ((entry = hash_seq_search(&status)) != NULL)
		{
			int32		partitionSpecId = entry->partitionSpecId;
			List	   *manifestEntries = entry->manifestEntries;

			char	   *remoteManifestPath =
				GenerateRemoteManifestPath(metadataLocation,
										   snapshotUUID,
										   manifestIndex++, "");

			int64_t		manifestSize = UploadIcebergManifestToURI(manifestEntries, remoteManifestPath);

			IcebergManifest *newDeleteManifest =
				CreateNewIcebergManifest(newSnapshot, partitionSpecId, allTransforms, manifestSize,
										 ICEBERG_MANIFEST_FILE_CONTENT_DELETES, remoteManifestPath,
										 manifestEntries);

			finalDeleteManifestList = lappend(finalDeleteManifestList, newDeleteManifest);

			createNewSnapshot = true;
		}
	}

	/*
	 * If we have any files to be marked as removed, we need to update the
	 * manifest entries to mark them as deleted. We do no create anything new
	 * here, we just update the existing manifest entries. If any entry
	 * changes in a manifest, we write a new manifest file.
	 */
	if (removedEntries != NIL || removeAllEntries)
	{
		/* do not modify input lists */
		List	   *manifestList = list_concat_copy(finalDataManifestList,
													finalDeleteManifestList);

		ListCell   *manifestCell = NULL;

		foreach(manifestCell, manifestList)
		{
			IcebergManifest *manifest = lfirst(manifestCell);
			IcebergManifestContentType content = manifest->content;
			List	   *manifestEntries =
				ReadManifestEntries(manifest->manifest_path);

			List	   *deletedManifestEntries =
				FindAndAdjustDeletedManifestEntries(manifest, manifestEntries, removedEntries,
													snapshotId, removeAllEntries);

			if (deletedManifestEntries != NIL)
			{
				List	   *newManifests =
					CreateNewManifestsForDeletedEntries(manifestEntries, deletedManifestEntries,
														newSnapshot, metadataLocation, snapshotUUID,
														&manifestIndex, manifest->partition_spec_id,
														allTransforms, content);

				/* remove the old manifest and add the new ones */
				if (content == ICEBERG_MANIFEST_FILE_CONTENT_DATA)
				{
					finalDataManifestList = list_difference_ptr(finalDataManifestList, list_make1(manifest));
					finalDataManifestList = list_concat(finalDataManifestList, newManifests);
				}
				else if (content == ICEBERG_MANIFEST_FILE_CONTENT_DELETES)
				{
					finalDeleteManifestList = list_difference_ptr(finalDeleteManifestList, list_make1(manifest));
					finalDeleteManifestList = list_concat(finalDeleteManifestList, newManifests);
				}
				else
					pg_unreachable();

				createNewSnapshot = true;
			}
		}
	}

	if (applyManifestCompaction || (EnableManifestMergeOnWrite && HasMergeableManifests(finalDataManifestList)))
	{
		/*
		 * If the manifest compaction is enabled, we merge the existing
		 * manifests with the new manifests. This is done to reduce the number
		 * of manifest files in the snapshot.
		 */
		int			beforeMergeCount = list_length(finalDataManifestList);

		finalDataManifestList = MergeDataManifests(newSnapshot, allTransforms, finalDataManifestList,
												   metadataLocation, snapshotUUID, isVerbose, &manifestIndex);

		int			afterMergeCount = list_length(finalDataManifestList);

		if (afterMergeCount < beforeMergeCount)
			/* for manifest merge, signal that we did some work */
			createNewSnapshot = true;
	}

	if (!createNewSnapshot)
		return NULL;

	/*
	 * Delete in-progress manifests, created in current transaction, that are
	 * about to be committed into new snapshot. Transient manifests, that are
	 * created but not put into snapshot,would be kept in the in-progress
	 * queue to be cleaned up later.
	 */
	List	   *newDataManifestList = list_difference_ptr(finalDataManifestList, existingDataManifests);
	List	   *newDeleteManifestList = list_difference_ptr(finalDeleteManifestList, existingDeleteManifests);
	List	   *newPersistedManifestList = list_concat(newDataManifestList, newDeleteManifestList);

	DeleteInProgressManifests(relationId, newPersistedManifestList);

	SetSnapshotOperation(newSnapshot, builder->operation);

	/*
	 * Write the manifest list file that contains the list of manifest files.
	 */
	char	   *manifestListPath = GenerateRemoteManifestListPath(snapshotId,
																  metadataLocation,
																  snapshotUUID,
																  manifestListIndex, "");

	List	   *finalManifestList = list_concat(finalDataManifestList, finalDeleteManifestList);

	UploadIcebergManifestListToURI(finalManifestList, manifestListPath);
	newSnapshot->manifest_list = manifestListPath;

	/* schema may have been updated by the current operation */
	newSnapshot->schema_id = currentSchemaId;

	return newSnapshot;
}


/*
* CreateNewManifestsForDeletedEntries creates new manifests for the deleted entries.
* It creates a delete manifest for the deleted entries and a new manifest for
* the remaining data entries, if exists.
* In both cases, it returns a list of manifests that should be added to the
* final manifest list.
*/
static List *
CreateNewManifestsForDeletedEntries(List *allManifestEntries, List *deletedManifestEntries, IcebergSnapshot * newSnapshot,
									const char *metadataLocation, const char *snapshotUUID, int *manifestIndex,
									int partitionSpecId, List *partitionTransforms, IcebergManifestContentType contentType)
{
	char	   *deleteManifestPath = GenerateRemoteManifestPath(metadataLocation,
																snapshotUUID, (*manifestIndex)++, "");
	int64_t		manifestSize = UploadIcebergManifestToURI(deletedManifestEntries, deleteManifestPath);

	IcebergManifest *deleteManifest =
		CreateNewIcebergManifest(newSnapshot, partitionSpecId, partitionTransforms, manifestSize,
								 contentType, deleteManifestPath, deletedManifestEntries);

	List	   *remainingManifestEntries = list_difference_ptr(allManifestEntries, deletedManifestEntries);

	if (list_length(remainingManifestEntries) == 0)
	{
		/*
		 * If there are no remaining entries, we can skip creating a new
		 * manifest for existing values, as the delete manifest will cover all
		 * entries.
		 */
		return list_make1(deleteManifest);
	}

	/*
	 * Create a new manifest for the remaining entries, which are not deleted.
	 * This is necessary to ensure that the remaining entries are still
	 * tracked.
	 */
	char	   *remainingManifestPath = GenerateRemoteManifestPath(metadataLocation,
																   snapshotUUID, (*manifestIndex)++, "");
	int64_t		remainingManifestSize = UploadIcebergManifestToURI(remainingManifestEntries, remainingManifestPath);

	SetExistingStatusForOldSnapshotAddedEntries(remainingManifestEntries, newSnapshot->snapshot_id);

	IcebergManifest *remainingManifest =
		CreateNewIcebergManifest(newSnapshot, partitionSpecId, partitionTransforms,
								 remainingManifestSize, contentType,
								 remainingManifestPath, remainingManifestEntries);

	return list_make2(deleteManifest, remainingManifest);
}


/*
* CreateIcebergManifestEntryFromMetadataOperation creates a new Iceberg manifest entry for the given metadata operation.
*/
static IcebergManifestEntry *
CreateIcebergManifestEntryFromMetadataOperation(TableMetadataOperation * operation, int64_t newSnapshotId, int64_t sequenceNumber)
{
	IcebergManifestEntry *manifestEntry = palloc0(sizeof(IcebergManifestEntry));

	char	   *baseUrl = StripFromChar((char *) operation->path, '?');

	manifestEntry->status =
		(operation->type == DATA_FILE_ADD) ? ICEBERG_MANIFEST_ENTRY_STATUS_ADDED : ICEBERG_MANIFEST_ENTRY_STATUS_DELETED;

	manifestEntry->data_file.file_path = pstrdup(baseUrl);
	manifestEntry->data_file.file_format = "PARQUET";
	manifestEntry->data_file.content =
		(operation->content == CONTENT_DATA) ? ICEBERG_DATA_FILE_CONTENT_DATA : ICEBERG_DATA_FILE_CONTENT_POSITION_DELETES;
	if (operation->partition != NULL)
	{
		/* let's give the ownership of the data_file.partition to itself */
		Partition  *copyPartition = CopyPartition(operation->partition);

		manifestEntry->data_file.partition = *copyPartition;
	}

	manifestEntry->has_snapshot_id = true;
	manifestEntry->snapshot_id = newSnapshotId;

	manifestEntry->has_sequence_number = true;
	manifestEntry->sequence_number = sequenceNumber;

	manifestEntry->file_sequence_number = 0;
	manifestEntry->has_file_sequence_number = false;

	SetIcebergDataFileStats(&operation->dataFileStats,
							&manifestEntry->data_file.record_count,
							&manifestEntry->data_file.file_size_in_bytes,
							&manifestEntry->data_file.lower_bounds,
							&manifestEntry->data_file.lower_bounds_length,
							&manifestEntry->data_file.upper_bounds,
							&manifestEntry->data_file.upper_bounds_length);

	return manifestEntry;
}


/*
* ShouldSkipMetadataChangeToIceberg determines whether we should skip applying
* metadata changes to iceberg.
*/
bool
ShouldSkipMetadataChangeToIceberg(List *metadataOperationTypes)
{
	ListCell   *metadataOperationCell = NULL;

	foreach(metadataOperationCell, metadataOperationTypes)
	{
		TableMetadataOperationType opType = lfirst_int(metadataOperationCell);

		switch (opType)
		{
			case DATA_FILE_ADD:
			case DATA_FILE_REMOVE:
			case DATA_FILE_REMOVE_ALL:
			case DATA_FILE_MERGE_MANIFESTS:
			case EXPIRE_OLD_SNAPSHOTS:
			case TABLE_CREATE:
			case TABLE_DDL:
			case TABLE_PARTITION_BY:
				{
					return false;
				}
			default:
				break;
		}
	}

	return true;
}


/*
 * GetMetadataOperationTypes returns a list of operation types from the given
 * list of metadata operations.
 */
List *
GetMetadataOperationTypes(List *metadataOperations)
{
	List	   *operationTypes = NIL;
	ListCell   *operationCell = NULL;

	foreach(operationCell, metadataOperations)
	{
		TableMetadataOperation *operation = lfirst(operationCell);

		operationTypes = lappend_int(operationTypes, operation->type);
	}

	return operationTypes;
}


/*
* CopyPropertiesArray creates a copy of the given Property array.
*/
static Property *
CopyPropertiesArray(Property * src, int length)
{
	if (!src)
		return NULL;
	Property   *dest = palloc0(sizeof(Property) * length);

	for (int i = 0; i < length; i++)
	{
		dest[i].key = pstrdup(src[i].key);
		dest[i].key_length = src[i].key_length;
		dest[i].value = pstrdup(src[i].value);
		dest[i].value_length = src[i].value_length;
	}

	return dest;
}


/*
* CopyIcebergSnapshot creates a copy of the given IcebergSnapshot.
*/
static IcebergSnapshot *
CopyIcebergSnapshot(IcebergSnapshot * src)
{
	if (!src)
		return NULL;
	IcebergSnapshot *dest = palloc0(sizeof(IcebergSnapshot));

	dest->snapshot_id = src->snapshot_id;
	dest->parent_snapshot_id = src->parent_snapshot_id;
	dest->sequence_number = src->sequence_number;
	dest->timestamp_ms = src->timestamp_ms;
	dest->manifest_list = pstrdup(src->manifest_list);
	dest->manifest_list_length = src->manifest_list_length;
	dest->schema_id = src->schema_id;
	dest->schema_id_set = src->schema_id_set;
	dest->summary = CopyPropertiesArray(src->summary, src->summary_length);
	dest->summary_length = src->summary_length;

	return dest;
}


/*
 * HasCreateTableOperation checks if there is a TABLE_CREATE operation in the list.
 */
static bool
HasCreateTableOperation(List *metadataOperations)
{
	ListCell   *cell = NULL;

	foreach(cell, metadataOperations)
	{
		TableMetadataOperation *operation = lfirst(cell);

		if (operation->type == TABLE_CREATE)
			return true;
	}

	return false;
}


/*
 * DeleteInProgressManifests deletes the given manifests from the remote storage.
 * This is used to clean up manifests that were created for a snapshot that was
 * not committed.
 */
static void
DeleteInProgressManifests(Oid relationId, List *manifests)
{
	ListCell   *fileCell = NULL;

	foreach(fileCell, manifests)
	{
		IcebergManifest *manifest = lfirst(fileCell);

		char	   *manifestPath = pstrdup(manifest->manifest_path);

		DeleteInProgressFileRecord(manifestPath);
	}
}
