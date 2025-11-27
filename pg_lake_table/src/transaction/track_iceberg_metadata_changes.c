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
#include "access/xact.h"
#include "common/int.h"
#include "utils/memutils.h"

#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/iceberg/operations/find_referenced_files.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/transaction/transaction_hooks.h"
#include "pg_lake/util/injection_points.h"


static void ApplyTrackedIcebergMetadataChanges(void);
static void RecordIcebergMetadataOperation(Oid relationId, TableMetadataOperationType operationType);
static void InitTableMetadataTrackerHashIfNeeded(void);
static HTAB *CreateDataFilesHashForMetadata(IcebergTableMetadata * metadata);
static void FindChangedFilesSinceMetadata(HTAB *currentFilesMap, IcebergTableMetadata * metadata,
										  List **addedFiles, List **removedFilePaths);
static HTAB *CreatePartitionSpecsHashForMetadata(IcebergTableMetadata * metadata);
static List *FindNewPartitionSpecsSinceMetadata(HTAB *currentSpecs, IcebergTableMetadata * metadata);
static IcebergTableMetadata * GetLastPushedIcebergMetadata(const TableMetadataOperationTracker * opTracker);
static List *GetDataFileMetadataOperations(const TableMetadataOperationTracker * opTracker,
										   List *allTransforms);
static List *GetDDLMetadataOperations(const TableMetadataOperationTracker * opTracker);
static void DeleteInProgressAddedFiles(Oid relationId, List *addedFiles);
static bool AreSchemasEqual(IcebergTableSchema * existingSchema, DataFileSchema * newSchema);
static int32_t GetSchemaIdForIcebergTableIfExists(const TableMetadataOperationTracker * opTracker, DataFileSchema * schema);
static int	ComparePartitionSpecsById(const ListCell *a, const ListCell *b);


/*
 * Hash table to track iceberg metadata operations per relation within a transaction.
 */
static HTAB *TrackedIcebergMetadataOperationsHash = NULL;


/*
 * TrackIcebergMetadataChangesInTx tracks metadata changes for a given relation
 * within a transaction. It acquires the necessary locks before applying the changes
 * here. (might defer locking as well but let's not worry about edge cases now)
 */
void
TrackIcebergMetadataChangesInTx(Oid relationId, List *metadataOperationTypes)
{
	if (ShouldSkipMetadataChangeToIceberg(metadataOperationTypes))
		return;

	/*
	 * we might also defer acquiring locks to precommit hook but let's keep
	 * them here to prevent any subtle bug
	 */
	bool		forUpdate = true;

	GetIcebergMetadataLocation(relationId, forUpdate);

	ListCell   *operationCell = NULL;

	foreach(operationCell, metadataOperationTypes)
	{
		TableMetadataOperationType opType = lfirst_int(operationCell);

		RecordIcebergMetadataOperation(relationId, opType);
	}
}


/*
* Expose the relations that are tracked within a transaction to
* external callers. The HTAB includes which metadata operations
* each table had.
*/
HTAB *
GetTrackedIcebergMetadataOperations(void)
{
	return TrackedIcebergMetadataOperationsHash;
}


/*
 * HasAnyTrackedIcebergMetadataChanges checks if there are any tracked
 * metadata changes in the current transaction.
 */
bool
HasAnyTrackedIcebergMetadataChanges(void)
{
	return TrackedIcebergMetadataOperationsHash != NULL &&
		hash_get_num_entries(TrackedIcebergMetadataOperationsHash) > 0;
}


/*
 * IsIcebergTableCreatedInCurrentTransaction checks if there is any
 * create table operation for given relation in the tracked metadata changes
 * in the current transaction.
 */
bool
IsIcebergTableCreatedInCurrentTransaction(Oid relation)
{
	if (TrackedIcebergMetadataOperationsHash == NULL)
		return false;

	bool		found = false;

	TableMetadataOperationTracker *opTracker =
		hash_search(TrackedIcebergMetadataOperationsHash,
					&relation, HASH_FIND, &found);

	return found && opTracker->relationCreated;
}


/*
 * We simply set the pointer to NULL, given the memory
 * is allocated in the TopTransactionContext and will be
 * freed when the transaction ends.
 */
void
ResetTrackedIcebergMetadataOperation(void)
{
	TrackedIcebergMetadataOperationsHash = NULL;
}


/*
 * RecordIcebergMetadataOperation records a metadata operation for a relation.
 * This is used to track changes to the iceberg metadata during a transaction.
 *
 * Allocate everything in the TopTransactionContext
 * so that it is cleaned up at the end of the transaction.
 */
static void
RecordIcebergMetadataOperation(Oid relationId, TableMetadataOperationType operationType)
{
	InitTableMetadataTrackerHashIfNeeded();

	bool		isFound = false;
	TableMetadataOperationTracker *opTracker =
		hash_search(TrackedIcebergMetadataOperationsHash,
					&relationId, HASH_ENTER, &isFound);

	if (!isFound)
	{
		memset(opTracker, 0, sizeof(TableMetadataOperationTracker));
		opTracker->relationId = relationId;
	}

	/*
	 * flags are not reset in case a subtransaction rollbacks. But this is not
	 * a problem because we calculate the difference between our catalogs and
	 * the last pushed metadata and then apply the difference to the new
	 * metadata. Only exception is TABLE_DDL operations, for which we always
	 * create a snapshot even if the subtransaction rollbacks. In future, we
	 * might want to apply the diff algorithm to see if the schema changes as
	 * well.
	 */
	switch (operationType)
	{
		case TABLE_CREATE:
			opTracker->relationCreated = true;
			break;
		case TABLE_DDL:
			opTracker->relationAltered = true;
			break;
		case TABLE_PARTITION_BY:
			opTracker->relationPartitionByChanged = true;
			break;
		case DATA_FILE_ADD:
		case DATA_FILE_REMOVE:
		case DATA_FILE_REMOVE_ALL:
			opTracker->relationDataFileChanged = true;
			break;
		case DATA_FILE_MERGE_MANIFESTS:
			opTracker->relationManifestMergeRequested = true;
			break;
		case EXPIRE_OLD_SNAPSHOTS:
			opTracker->relationSnapshotExpirationRequested = true;
			break;
		default:
			/* other operations do not affect the flags */
			break;
	}
}


/*
 * InitTableMetadataTrackerHashIfNeeded is a helper function to manage the initialization
 * of the hash. We allocate the hash and entries in TopTransactionContext.
 */
static void
InitTableMetadataTrackerHashIfNeeded(void)
{
	if (TrackedIcebergMetadataOperationsHash == NULL)
	{
		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(TableMetadataOperationTracker);
		ctl.hash = oid_hash;
		ctl.hcxt = TopTransactionContext;

		TrackedIcebergMetadataOperationsHash = hash_create("Tracked Iceberg Metadata Operations",
														   32, &ctl,
														   HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}
}


/*
 * ConsumeTrackedIcebergMetadataChanges consumes the tracked metadata operations and
 * applies them to the Iceberg metadata.
 */
void
ConsumeTrackedIcebergMetadataChanges(void)
{
	ApplyTrackedIcebergMetadataChanges();
	ResetTrackedIcebergMetadataOperation();
}


/*
 * ApplyTrackedIcebergMetadataChanges applies the tracked metadata operations to the
 * Iceberg metadata and pushes the changes to remote catalog.
 */
static void
ApplyTrackedIcebergMetadataChanges(void)
{
	HTAB	   *trackedRelations = GetTrackedIcebergMetadataOperations();

	if (trackedRelations == NULL)
	{
		return;
	}

	HASH_SEQ_STATUS status;
	TableMetadataOperationTracker *opTracker;

	hash_seq_init(&status, trackedRelations);
	while ((opTracker = hash_seq_search(&status)) != NULL)
	{
		Oid			relationId = opTracker->relationId;

		/* relation is dropped */
		if (!RelationExistsInTheIcebergCatalog(relationId))
			continue;

		List	   *allTransforms = AllPartitionTransformList(relationId);

		List	   *metadataOperations = NIL;

		/* apply all ddl operations at once */
		if (opTracker->relationCreated || opTracker->relationAltered || opTracker->relationPartitionByChanged)
		{
			List	   *ddlOps = GetDDLMetadataOperations(opTracker);

			metadataOperations = list_concat(metadataOperations, ddlOps);
		}

		/* apply all data file operations at once */
		if (opTracker->relationDataFileChanged)
		{
			List	   *dataFileOps = GetDataFileMetadataOperations(opTracker, allTransforms);

			metadataOperations = list_concat(metadataOperations, dataFileOps);
		}

		/* explicit manifest merge operation */
		if (opTracker->relationManifestMergeRequested)
		{
			TableMetadataOperation *mergeOp = palloc0(sizeof(TableMetadataOperation));

			mergeOp->type = DATA_FILE_MERGE_MANIFESTS;

			metadataOperations = lappend(metadataOperations, mergeOp);
		}

		/* snapshot expiration operation */
		if (opTracker->relationSnapshotExpirationRequested)
		{
			TableMetadataOperation *expireOp = palloc0(sizeof(TableMetadataOperation));

			expireOp->type = EXPIRE_OLD_SNAPSHOTS;

			metadataOperations = lappend(metadataOperations, expireOp);
		}

		if (metadataOperations != NIL)
			ApplyIcebergMetadataChanges(relationId, metadataOperations, allTransforms, true);
	}

	INJECTION_POINT_COMPAT("after-apply-iceberg-changes");

	ExternalHeavyAssertsOnIcebergMetadataChange();
}


/*
 * CreateDataFilesHashForMetadata creates and populates a hash table of data files
 * from the given Iceberg table metadata.
 */
static HTAB *
CreateDataFilesHashForMetadata(IcebergTableMetadata * metadata)
{
	HTAB	   *dataFilesMap = CreateFilesHash();

	if (metadata == NULL)
		return dataFilesMap;

	IcebergSnapshot *iceSnapshot = GetCurrentSnapshot(metadata, true);
	List	   *dataFiles = FetchDataFilesFromSnapshot(iceSnapshot, NULL, IsManifestEntryStatusScannable, NULL);

	ListCell   *fileCell = NULL;

	foreach(fileCell, dataFiles)
	{
		TableDataFile *dataFile = lfirst(fileCell);

		AppendFileToHash(dataFile->path, dataFilesMap);
	}

	return dataFilesMap;
}


/*
 * FindChangedFilesSinceMetadata identifies added and removed files by comparing
 * the current state of data files with the state recorded in the provided metadata.
 * It populates the addedFiles and removedFilePaths lists with the respective files.
 *
 * addedFiles: file info, wrapped in `TableDataFile` struct, for the files that are added since the metadata
 * removedFilePaths: file paths, which are added before the current tx, that are removed since the metadata
 */
static void
FindChangedFilesSinceMetadata(HTAB *currentFilesMap, IcebergTableMetadata * metadata,
							  List **addedFiles, List **removedFilePaths)
{
	/* create metadata's data files */
	HTAB	   *metadataDataFilesMap = CreateDataFilesHashForMetadata(metadata);

	/* find added files */
	HASH_SEQ_STATUS currentFilesStatus;

	hash_seq_init(&currentFilesStatus, currentFilesMap);

	TableDataFileHashEntry *currentDataFile = NULL;

	while ((currentDataFile = hash_seq_search(&currentFilesStatus)) != NULL)
	{
		if (!hash_search(metadataDataFilesMap, currentDataFile->filePath, HASH_FIND, NULL))
			*addedFiles = lappend(*addedFiles, &currentDataFile->dataFile);
	}

	/* find removed files */
	HASH_SEQ_STATUS metadataFilesStatus;

	hash_seq_init(&metadataFilesStatus, metadataDataFilesMap);

	char	   *metadataDataFilePath = NULL;

	while ((metadataDataFilePath = hash_seq_search(&metadataFilesStatus)) != NULL)
	{
		if (!hash_search(currentFilesMap, metadataDataFilePath, HASH_FIND, NULL))
			*removedFilePaths = lappend(*removedFilePaths, metadataDataFilePath);
	}
}


/*
 * DeleteInProgressAddedFiles deletes the in-progress data file records
 * for the given list of data files.
 */
static void
DeleteInProgressAddedFiles(Oid relationId, List *addedFiles)
{
	ListCell   *fileCell = NULL;

	foreach(fileCell, addedFiles)
	{
		TableDataFile *addedFile = lfirst(fileCell);

		DeleteInProgressFileRecord(addedFile->path);
	}
}


/*
 * CreatePartitionSpecsHashForMetadata creates and populates a hash table of partition specs
 * from the given Iceberg table metadata.
 */
static HTAB *
CreatePartitionSpecsHashForMetadata(IcebergTableMetadata * metadata)
{
	HTAB	   *partitionSpecsMap = CreatePartitionSpecHash();

	if (metadata == NULL)
		return partitionSpecsMap;

	List	   *partitionSpecs = GetAllIcebergPartitionSpecsFromTableMetadata(metadata);

	ListCell   *specCell = NULL;

	foreach(specCell, partitionSpecs)
	{
		IcebergPartitionSpec *spec = lfirst(specCell);

		bool		found = false;

		IcebergPartitionSpecHashEntry *entry =
			(IcebergPartitionSpecHashEntry *) hash_search(partitionSpecsMap, &spec->spec_id,
														  HASH_ENTER, &found);

		if (!found)
		{
			entry->specId = spec->spec_id;
			entry->spec = spec;
		}
	}

	return partitionSpecsMap;
}


/*
 * FindNewPartitionSpecsSinceMetadata identifies new partition specs that have been
 * added since the provided metadata. It compares the current list of partition specs
 * with those in the metadata and returns a list of new partition specs.
 */
static List *
FindNewPartitionSpecsSinceMetadata(HTAB *currentSpecs, IcebergTableMetadata * metadata)
{
	HTAB	   *metadataSpecs = CreatePartitionSpecsHashForMetadata(metadata);

	List	   *newSpecs = NIL;

	HASH_SEQ_STATUS currentSpecsStatus;

	hash_seq_init(&currentSpecsStatus, currentSpecs);
	IcebergPartitionSpecHashEntry *currentSpec = NULL;

	while ((currentSpec = hash_seq_search(&currentSpecsStatus)) != NULL)
	{
		if (!hash_search(metadataSpecs, &currentSpec->specId, HASH_FIND, NULL))
			newSpecs = lappend(newSpecs, currentSpec->spec);
	}

	/* sort the new specs by their spec_id for consistent ordering at metadata */
	list_sort(newSpecs, ComparePartitionSpecsById);

	return newSpecs;
}


/*
 * GetLastPushedIcebergMetadata retrieves the most recently pushed Iceberg metadata
 * for the specified relation. It returns NULL if no metadata is found.
 */
static IcebergTableMetadata *
GetLastPushedIcebergMetadata(const TableMetadataOperationTracker * opTracker)
{
	/* table is just created, no metadata is pushed yet */
	if (opTracker->relationCreated)
		return NULL;

	/* read the most recently pushed iceberg metadata for the table */
	char	   *metadataPath = GetIcebergMetadataLocation(opTracker->relationId, false);

	return ReadIcebergTableMetadata(metadataPath);
}


/*
 * GetDataFileMetadataOperations retrieves the metadata operations for data files
 * in the specified relation.
 */
static List *
GetDataFileMetadataOperations(const TableMetadataOperationTracker * opTracker,
							  List *allTransforms)
{
	/*
	 * get current state of data files, which are not applied to metadata yet,
	 * from catalog
	 */
	bool		dataOnly = false;
	bool		newFilesOnly = false;
	bool		forUpdate = false;
	char	   *orderBy = NULL;
	Snapshot	snapshot = GetTransactionSnapshot();

	HTAB	   *currentFilesMap = GetTableDataFilesByPathHashFromCatalog(opTracker->relationId, dataOnly, newFilesOnly,
																		 forUpdate, orderBy, snapshot, allTransforms);

	/* get last pushed metadata */
	IcebergTableMetadata *lastMetadata = GetLastPushedIcebergMetadata(opTracker);

	/* find added and removed files since metadata */
	List	   *addedFiles = NIL;
	List	   *removedFilePaths = NIL;

	FindChangedFilesSinceMetadata(currentFilesMap, lastMetadata, &addedFiles, &removedFilePaths);

	/*
	 * We have found the new files that are added since the last metadata
	 * push. We can delete them from in-progress files now.
	 *
	 * Transient files, that are added and removed in the same transaction
	 * would still be in-progress queue to be removed later. Files that are
	 * added in rollbacked subtransactions would also be in-progress queue.
	 */
	DeleteInProgressAddedFiles(opTracker->relationId, addedFiles);

	List	   *metadataOperations = NIL;

	/* create operations for added files */
	ListCell   *addedFileCell = NULL;

	foreach(addedFileCell, addedFiles)
	{
		TableDataFile *addedFile = lfirst(addedFileCell);

		TableMetadataOperation *addFileOp =
			AddDataFileOperation(addedFile->path, addedFile->content, &addedFile->stats,
								 addedFile->partition, addedFile->partitionSpecId);

		metadataOperations = lappend(metadataOperations, addFileOp);
	}

	/* create operations for removed files */
	ListCell   *removedFileCell = NULL;

	foreach(removedFileCell, removedFilePaths)
	{
		char	   *removedFilePath = lfirst(removedFileCell);

		TableMetadataOperation *removedFileOp = RemoveDataFileOperation(removedFilePath);

		metadataOperations = lappend(metadataOperations, removedFileOp);
	}

	return metadataOperations;
}


/*
 * GetDDLMetadataOperations creates the metadata operations for ddl changes for
 * the given relation.
 */
static List *
GetDDLMetadataOperations(const TableMetadataOperationTracker * opTracker)
{
	Assert(opTracker->relationCreated || opTracker->relationAltered || opTracker->relationPartitionByChanged);

	int			defaultSpecId = DEFAULT_SPEC_ID;
	List	   *newPartitionSpecs = NIL;
	DataFileSchema *schema = NULL;

	if (opTracker->relationCreated || opTracker->relationAltered)
		schema = GetDataFileSchemaForTable(opTracker->relationId);

	if (opTracker->relationCreated || opTracker->relationPartitionByChanged)
	{
		IcebergTableMetadata *lastMetadata = GetLastPushedIcebergMetadata(opTracker);

		HTAB	   *currentSpecs = GetAllPartitionSpecsFromCatalog(opTracker->relationId);

		newPartitionSpecs = FindNewPartitionSpecsSinceMetadata(currentSpecs, lastMetadata);
		defaultSpecId = GetCurrentSpecId(opTracker->relationId);
	}

	if (opTracker->relationCreated)
	{
		TableMetadataOperation *createOp = palloc0(sizeof(TableMetadataOperation));

		createOp->type = TABLE_CREATE;
		createOp->newSchema = schema;
		createOp->partitionSpecs = newPartitionSpecs;
		createOp->defaultSpecId = defaultSpecId;

		/*
		 * TABLE_CREATE operation with schema and partition spec would already
		 * cover other operations
		 */
		return list_make1(createOp);
	}

	List	   *operations = NIL;

	if (opTracker->relationAltered)
	{
		/*
		 * When a table is altered, its schema has changed. However, it might
		 * have been set to an existing schema in the iceberg metadata, if so,
		 * use that schemaId.
		 */
		TableMetadataOperation *ddlOp = palloc0(sizeof(TableMetadataOperation));

		ddlOp->type = TABLE_DDL;

		int32_t		existingSchemaId =
			GetSchemaIdForIcebergTableIfExists(opTracker, schema);

		if (existingSchemaId != -1)
		{
			ddlOp->ddlSchemaEffect = DDL_EFFECT_SET_EXISTING_SCHEMA;
			ddlOp->existingSchemaId = existingSchemaId;
			ddlOp->newSchema = NULL;
		}
		else
		{
			ddlOp->ddlSchemaEffect = DDL_EFFECT_ADD_SCHEMA;
			ddlOp->newSchema = schema;
			ddlOp->existingSchemaId = -1;
		}

		operations = lappend(operations, ddlOp);
	}

	if (opTracker->relationPartitionByChanged)
	{
		TableMetadataOperation *partitionByOp = palloc0(sizeof(TableMetadataOperation));

		partitionByOp->type = TABLE_PARTITION_BY;
		partitionByOp->partitionSpecs = newPartitionSpecs;
		partitionByOp->defaultSpecId = defaultSpecId;

		operations = lappend(operations, partitionByOp);
	}

	return operations;
}


/*
 * ComparePartitionSpecsById is a comparison function for sorting partition specs by their ID.
 */
int
ComparePartitionSpecsById(const ListCell *a, const ListCell *b)
{
	IcebergPartitionSpec *specA = lfirst(a);
	IcebergPartitionSpec *specB = lfirst(b);

	return pg_cmp_s32(specA->spec_id, specB->spec_id);
}


/*
* GetSchemaIdForIcebergTableIfExists checks if the given schema already exists
 * in the iceberg table metadata. If it exists, it returns the schema ID, otherwise -1.
*/
static int32_t
GetSchemaIdForIcebergTableIfExists(const TableMetadataOperationTracker * opTracker, DataFileSchema * schema)
{
	IcebergTableMetadata *metadata = GetLastPushedIcebergMetadata(opTracker);

	if (metadata == NULL)
		return -1;

	IcebergTableSchema *schemas = metadata->schemas;

	for (int schemaIndex = 0; schemaIndex < metadata->schemas_length; schemaIndex++)
	{
		IcebergTableSchema *existingSchema = &schemas[schemaIndex];

		if (AreSchemasEqual(existingSchema, schema))
			return schemas[schemaIndex].schema_id;

	}

	return -1;
}


/*
* AreSchemasEqual compares two schemas for equality.
*/
static bool
AreSchemasEqual(IcebergTableSchema * existingSchema, DataFileSchema * newSchema)
{
	if (existingSchema->fields_length != newSchema->nfields)
		return false;

	for (size_t i = 0; i < existingSchema->fields_length; i++)
	{
		DataFileSchemaField *existingField = &existingSchema->fields[i];
		DataFileSchemaField *newField = &newSchema->fields[i];

		if (!SchemaFieldsEquivalent(existingField, newField))
			return false;
	}

	return true;
}
