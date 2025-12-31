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
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/transaction/transaction_hooks.h"
#include "pg_lake/util/injection_points.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/util/url_encode.h"

#define ONE_MB (1 * 1024 * 1024)

/*
* Represents the rest catalog requests per table within a transaction.
* We can commit all DDL/DML requests in a single Post-request to the REST
* catalog, except for the create table requests, which need to be done
* first with a different API call.
*/
typedef struct RestCatalogRequestPerTable
{
	Oid			relationId;

	bool		isValid;

	char	   *catalogName;
	char	   *catalogNamespace;
	char	   *catalogTableName;

	/*
	 * We do URL-encoding of the catalog, namespace and table names to
	 * construct the identifiers used in REST API calls.
	 */
	char	   *urlEncodedCatalogName;
	char	   *urlEncodedCatalogNamespace;
	char	   *urlEncodedCatalogTableName;

	char	   *tableRestUrl;
	char	   *tableIdentifier;

	RestCatalogRequest *createTableRequest;
	RestCatalogRequest *dropTableRequest;


	List	   *tableModifyRequests;
}			RestCatalogRequestPerTable;

static void ApplyTrackedIcebergMetadataChanges(void);
static void RecordIcebergMetadataOperation(Oid relationId, TableMetadataOperationType operationType);
static void InitTableMetadataTrackerHashIfNeeded(void);
static void InitRestCatalogRequestsHashIfNeeded(void);
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

static char *IdentifierJson(const char *namespaceFlat, const char *tableName);



/*
 * Hash table to track iceberg metadata operations per relation within a transaction.
 */
static HTAB *TrackedIcebergMetadataOperationsHash = NULL;

/*
* Hash table to track rest catalog requests per relation within a transaction.
*/
static HTAB *RestCatalogRequestsHash = NULL;


/* some pre-allocated memory so we don't palloc() ever in XACT_COMMIT  */
static MemoryContext PgLakeXactCommitContext = NULL;

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
	 * We might also defer acquiring locks to precommit hook but let's keep
	 * them here to prevent any subtle bug. We call
	 * GetIcebergCatalogMetadataLocation() to acquire the necessary locks, not
	 * for the actual metadata location as our serialization of iceberg
	 * metadata changes relies on those locks.
	 */
	bool		forUpdate = true;

	GetIcebergCatalogMetadataLocation(relationId, forUpdate);

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


void
ResetRestCatalogRequests(void)
{
	RestCatalogRequestsHash = NULL;
	PgLakeXactCommitContext = NULL;
}


/*
* PostAllRestCatalogRequests posts all the tracked REST catalog requests
* to the REST catalog at transaction commit time. This is called at post-commit
* hook, meaning that ERRORS here will be FATAL, so not acceptable.
*/
void
PostAllRestCatalogRequests(void)
{
	if (RestCatalogRequestsHash == NULL)
	{
		return;
	}

	/*
	 * Switch to PgLakeXactCommitContext to avoid palloc() in XACT_COMMIT, as
	 * PgLakeXactCommitContext is pre-allocated before.
	 */
	MemoryContext oldContext = MemoryContextSwitchTo(PgLakeXactCommitContext);

	/*
	 * We need to iterate over the RestCatalogRequestsHash twice: 1. First, we
	 * need to post the create table requests to create the iceberg tables in
	 * the rest catalog. 2. Then, we need to post all the other modifications
	 * (like adding snapshots, partition specs, etc.)
	 *
	 * This is because the create table requests need to be completed before
	 * we can add snapshots to the tables. And, REST API does not support
	 * batching requests of create table and anything else.
	 */
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, RestCatalogRequestsHash);
	RestCatalogRequestPerTable *requestPerTable = NULL;

	while ((requestPerTable = hash_seq_search(&status)) != NULL)
	{
		if (!requestPerTable->isValid)
		{
			/*
			 * Might only happen if an OOM happened during adding this request
			 * to the hash table.
			 */
			elog(WARNING, "Skipping invalid REST catalog request for relation %u",
				 requestPerTable->relationId);
			continue;
		}

		RestCatalogRequest *createTableRequest = requestPerTable->createTableRequest;
		RestCatalogRequest *dropTableRequest = requestPerTable->dropTableRequest;

		if (createTableRequest != NULL && dropTableRequest != NULL)
		{
			/*
			 * table is created and dropped in the same transaction, skip both
			 * requests, essentially a no-op.
			 */
			continue;
		}
		else if (createTableRequest != NULL || dropTableRequest != NULL)
		{
			if (createTableRequest != NULL)
			{
				HttpResult	httpResult =
					SendRequestToRestCatalog(HTTP_POST, requestPerTable->tableRestUrl,
											 createTableRequest->body, PostHeadersWithAuth());

				if (httpResult.status != 200)
				{
					ReportHTTPError(httpResult, WARNING);

					/*
					 * Ouch, something failed. Should we stop sending the
					 * requests?
					 */
				}
			}
			else if (dropTableRequest != NULL)
			{
				HttpResult	httpResult =
					SendRequestToRestCatalog(HTTP_DELETE, requestPerTable->tableRestUrl,
											 NULL, DeleteHeadersWithAuth());

				if (httpResult.status != 204)
				{
					ReportHTTPError(httpResult, WARNING);

					/*
					 * Ouch, something failed. Should we stop sending the
					 * requests?
					 */
				}
			}
			else
			{
				pg_unreachable();
			}
		}
	}

	/*
	 * Now that all create table requests have been posted, we can post all
	 * the other modifications. All table modifications are sent in a single
	 * HTTP request to ensure atomicity.
	 */
	char	   *catalogName = NULL;
	bool		hasRestCatalogChanges = false;
	StringInfo	batchRequestBody = makeStringInfo();

	appendStringInfo(batchRequestBody, "{");	/* start msg body  */
	appendJsonKey(batchRequestBody, "table-changes");
	appendStringInfo(batchRequestBody, "[");	/* start array of changes */

	hash_seq_init(&status, RestCatalogRequestsHash);

	while ((requestPerTable = hash_seq_search(&status)) != NULL)
	{
		if (!requestPerTable->isValid)
		{
			/*
			 * Might only happen if an OOM happened during adding this request
			 * to the hash table.
			 */
			elog(WARNING, "Skipping invalid REST catalog request for relation %u",
				 requestPerTable->relationId);
			continue;
		}

		/* TODO: can we ever have multiple catalogs? */
		catalogName = requestPerTable->catalogName;

		if (requestPerTable->createTableRequest != NULL &&
			requestPerTable->dropTableRequest != NULL)
		{
			/*
			 * table is created and dropped in the same transaction, nothing
			 * post to do for this table to the REST catalog.
			 */
			continue;
		}
		else if (requestPerTable->tableModifyRequests == NIL)
		{
			/*
			 * no modifications to send for this table
			 */
			continue;
		}

		if (hasRestCatalogChanges)
		{
			appendStringInfoChar(batchRequestBody, ',');	/* separate previous
															 * table change */
		}

		appendStringInfoChar(batchRequestBody, '{');	/* start per-table json
														 * object */
		appendJsonKey(batchRequestBody, "identifier");
		appendStringInfo(batchRequestBody, "%s", requestPerTable->tableIdentifier);
		appendStringInfoChar(batchRequestBody, ',');
		appendStringInfoString(batchRequestBody, "\"requirements\":[],");
		appendStringInfoString(batchRequestBody, " \"updates\":[");

		ListCell   *requestCell = NULL;

		foreach(requestCell, requestPerTable->tableModifyRequests)
		{
			RestCatalogRequest *request = (RestCatalogRequest *) lfirst(requestCell);

			appendStringInfoString(batchRequestBody, request->body);

			bool		lastRequest = (requestCell == list_tail(requestPerTable->tableModifyRequests));

			if (!lastRequest)
			{
				appendStringInfoChar(batchRequestBody, ',');
			}

			if (message_level_is_interesting(DEBUG2))
			{
				elog(DEBUG2, "REST Catalog Request Body size reached: %d bytes",
					 batchRequestBody->len);
			}
		}

		appendStringInfoChar(batchRequestBody, ']');	/* close updates array */
		appendStringInfoChar(batchRequestBody, '}');	/* close per-table json
														 * object */

		/*
		 * We have at least one change to send for this table
		 */
		hasRestCatalogChanges = true;
	}

	if (hasRestCatalogChanges)
	{
		appendStringInfoChar(batchRequestBody, ']');	/* close table-changes */
		appendStringInfoChar(batchRequestBody, '}');	/* close json body */

		char	   *url = psprintf(REST_CATALOG_TRANSACTION_COMMIT, RestCatalogHost, catalogName);
		HttpResult	httpResult = SendRequestToRestCatalog(HTTP_POST, url, batchRequestBody->data, PostHeadersWithAuth());

		if (httpResult.status != 204)
		{
			ReportHTTPError(httpResult, WARNING);
		}
	}

	/*
	 * Switch back to old context from PgLakeXactCommitContext.
	 */
	MemoryContextSwitchTo(oldContext);
}


/*
 * IdentifierJson creates a JSON representation of an iceberg table identifier
 * given its namespace and table name.
 */
static char *
IdentifierJson(const char *namespaceFlat, const char *tableName)
{
	StringInfoData out;

	initStringInfo(&out);
	appendStringInfoChar(&out, '{');
	appendStringInfoString(&out, "\"namespace\":");
	appendStringInfo(&out, "[\"%s\"]", namespaceFlat);
	appendStringInfoString(&out, ",\"name\":");
	appendStringInfo(&out, "\"%s\"", tableName);
	appendStringInfoChar(&out, '}');
	return out.data;
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
 * InitTableMetadataTrackerHashIfNeeded is a helper function to manage the initialization
 * of the hash. We allocate the hash and entries in TopTransactionContext.
 */
static void
InitRestCatalogRequestsHashIfNeeded(void)
{
	if (RestCatalogRequestsHash == NULL)
	{
		/*
		 * They always updated together.
		 */
		Assert(PgLakeXactCommitContext == NULL);

		/*
		 * First allocate 1MB memory context to avoid palloc() in XACT_COMMIT
		 * as much as possible. Only with very large REST catalog requests we
		 * might need to palloc() in XACT_COMMIT, which is still better than
		 * always palloc()ing in XACT_COMMIT, reducing the risk of OOM
		 * significantly. These very large requests might happen when there
		 * are many tables modified in a single transaction, likely > 100
		 * tables. We allocate in TopTransactionContext to preserve the
		 * context until the end of the transaction, and let it be cleaned up
		 * automatically at transaction end.
		 */
		PgLakeXactCommitContext =
			AllocSetContextCreateInternal(TopTransactionContext,
										  "PgLakeXactCommitContext",
										  ONE_MB, ONE_MB, ONE_MB);
		Assert(MemoryContextMemAllocated(PgLakeXactCommitContext, true) == ONE_MB);

		HASHCTL		ctl;

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = sizeof(Oid);
		ctl.entrysize = sizeof(RestCatalogRequestPerTable);
		ctl.hash = oid_hash;

		/*
		 * We prefer to allocate everything in TopTransactionContext, not in
		 * PgLakeXactCommitContext, because we preserve
		 * PgLakeXactCommitContext mostly for REST API request bodies to avoid
		 * palloc() in XACT_COMMIT.
		 */
		ctl.hcxt = TopTransactionContext;

		RestCatalogRequestsHash = hash_create("Rest Catalog Requests",
											  32, &ctl,
											  HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	}
}


/*
* RecordRestCatalogRequestInTx records a REST catalog request to be sent at post-commit.
*/
void
RecordRestCatalogRequestInTx(Oid relationId, RestCatalogOperationType operationType,
							 const char *body)
{
	InitRestCatalogRequestsHashIfNeeded();

	bool		isFound = false;
	RestCatalogRequestPerTable *requestPerTable =
		hash_search(RestCatalogRequestsHash,
					&relationId, HASH_ENTER, &isFound);

	if (!isFound || !requestPerTable->isValid)
	{
		memset(requestPerTable, 0, sizeof(RestCatalogRequestPerTable));
		requestPerTable->relationId = relationId;

		requestPerTable->catalogName =
			MemoryContextStrdup(TopTransactionContext, GetRestCatalogName(relationId));
		requestPerTable->catalogNamespace =
			MemoryContextStrdup(TopTransactionContext, GetRestCatalogNamespace(relationId));
		requestPerTable->catalogTableName =
			MemoryContextStrdup(TopTransactionContext, GetRestCatalogTableName(relationId));

		requestPerTable->urlEncodedCatalogName =
			MemoryContextStrdup(TopTransactionContext, URLEncodePath(GetRestCatalogName(relationId)));
		requestPerTable->urlEncodedCatalogNamespace =
			MemoryContextStrdup(TopTransactionContext, URLEncodePath(GetRestCatalogNamespace(relationId)));
		requestPerTable->urlEncodedCatalogTableName =
			MemoryContextStrdup(TopTransactionContext, URLEncodePath(GetRestCatalogTableName(relationId)));

		requestPerTable->tableRestUrl =
			MemoryContextStrdup(TopTransactionContext, psprintf(REST_CATALOG_TABLE,
																RestCatalogHost,
																requestPerTable->urlEncodedCatalogName,
																requestPerTable->urlEncodedCatalogNamespace,
																requestPerTable->urlEncodedCatalogTableName));

		requestPerTable->tableIdentifier =
			MemoryContextStrdup(TopTransactionContext,
								IdentifierJson(requestPerTable->catalogNamespace,
											   requestPerTable->catalogTableName));

		requestPerTable->isValid = true;
	}

	/*
	 * Always allocate in TopTransactionContext as we need this in
	 * post-commit.
	 */
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->operationType = operationType;

	if (operationType == REST_CATALOG_CREATE_TABLE)
	{
		request->body = pstrdup(body);
		requestPerTable->createTableRequest = request;
	}
	else if (operationType == REST_CATALOG_DROP_TABLE)
	{
		requestPerTable->dropTableRequest = request;
	}
	else if (operationType == REST_CATALOG_ADD_SNAPSHOT ||
			 operationType == REST_CATALOG_ADD_SCHEMA ||
			 operationType == REST_CATALOG_SET_CURRENT_SCHEMA ||
			 operationType == REST_CATALOG_ADD_PARTITION ||
			 operationType == REST_CATALOG_REMOVE_SNAPSHOT ||
			 operationType == REST_CATALOG_SET_DEFAULT_PARTITION_ID)
	{
		request->body = pstrdup(body);
		requestPerTable->tableModifyRequests = lappend(requestPerTable->tableModifyRequests, request);
	}
	else
	{
		elog(ERROR, "unsupported rest catalog operation type: %d", operationType);
	}

	MemoryContextSwitchTo(oldContext);
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

			if (opTracker->relationCreated &&
				GetIcebergCatalogType(relationId) == REST_CATALOG_READ_WRITE)
			{
				TableMetadataOperation *createOp = linitial(metadataOperations);

				char	   *body =
					FinishStageRestCatalogIcebergTableCreateRestRequest(relationId,
																		createOp->newSchema,
																		createOp->partitionSpecs);

				RecordRestCatalogRequestInTx(relationId, REST_CATALOG_CREATE_TABLE, body);
			}
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
		{
			List	   *restRequests = ApplyIcebergMetadataChanges(relationId, metadataOperations, allTransforms, true);
			ListCell   *requestCell = NULL;

			foreach(requestCell, restRequests)
			{
				RestCatalogRequest *request = lfirst(requestCell);

				RecordRestCatalogRequestInTx(relationId, request->operationType,
											 request->body);
			}

		}
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
