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

/*
 * Writes to external REST catalog tables (external='true').
 *
 * This is pg_lake as one of several Iceberg writers of a table. The REST
 * catalog is the only place the table's state lives, so a write is a commit
 * of a child of the snapshot the transaction read, nothing more. Statements
 * write their data files as usual, and the file writers hand us the
 * TableMetadataOperations they produced. We keep those until pre-commit, and
 * scans in the transaction apply them to the snapshot it read, which the
 * external catalog cache holds for the whole transaction. Nothing goes into
 * lake_table.files, the stats and partition catalogs, or the in-progress
 * files.
 *
 * At pre-commit we apply the operations to that snapshot, write the
 * manifests and manifest list, and send one transactions/commit for every
 * external table the transaction wrote. It requires that each table still
 * has the UUID, main branch snapshot, schema and partition spec we read. If
 * another engine changed the table in between, the catalog answers 409 and
 * the PostgreSQL transaction aborts.
 *
 * The catalog commit makes the changes visible on its own, before the
 * PostgreSQL commit, so a transaction that writes external tables cannot
 * also write tables that commit through PostgreSQL.
 *
 * Left out for now:
 * - Orphan files. Files of a commit the catalog rejected, or of a statement
 *   that failed, stay in object storage. Cleaning them up means listing the
 *   table location and keeping what a retained snapshot references.
 * - Snapshot expiration and compaction, which VACUUM skips for these tables.
 * - Partitioned tables. New files go into spec 0, and the partition values
 *   of an existing file come from our catalogs, which hold nothing here.
 * - Schema changes and row_ids.
 * - Partitioning or a location for a table created from pg_lake, and making
 *   that create atomic: the catalog has the table as soon as CREATE asks for
 *   it, so a later error in the same statement leaves it there.
 * - Copy-on-write deletes, since the rewrite would have to apply position
 *   deletes other engines added. Deletes are merge-on-read.
 * - Vended credentials for writing.
 * - Retrying a 409 or a 5xx with an unknown outcome. Both raise an error.
 */
#include "postgres.h"

#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/pg_class.h"
#include "foreign/foreign.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#include "pg_extension_base/pg_compat.h"
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/fdw/snapshot.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/transaction/external_rest_catalog_table.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/injection_points.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/util/url_encode.h"


/*
 * The data file operations one statement made to an external table, kept
 * until the commit applies them to the snapshot the transaction read.
 */
typedef struct ExternalTableChange
{
	Oid			relationId;

	/* the (sub)transaction that made the change, for savepoint rollback */
	SubTransactionId subXactId;

	/* TableMetadataOperation, in TopTransactionContext */
	List	   *operations;
}			ExternalTableChange;

/* list of ExternalTableChange in the order they were made */
static List *ExternalTableChanges = NIL;

/* whether the transaction writes external tables, set before any file */
static bool XactWritesExternalTables = false;

/* the REST catalog the transaction commits to, see ErrorIfOtherCatalog */
static Oid	XactCatalogServerId = InvalidOid;
static char *XactCatalogName = NULL;

/*
 * Relations the transaction wrote to, checked once it writes an external
 * table. Not undone by ROLLBACK TO SAVEPOINT.
 */
static List *XactWrittenRelationIds = NIL;

static IcebergTableMetadata * ReadExternalTableMetadata(Oid relationId);
static void ErrorIfPartitionedExternalTable(Oid relationId,
											IcebergTableMetadata * metadata);
static void ErrorIfOtherCatalog(Oid relationId);
static bool IsPostgresTableWrite(Oid relationId);
static void ErrorIfXactWrotePostgresTables(Oid relationId);
static TableMetadataOperation * CopyExternalTableOperation(TableMetadataOperation * operation,
														   List *metadataOperations);
static bool ForgetPendingDataFile(List **pendingOperations, const char *path);
static char *TableChangeJson(Oid relationId, IcebergTableMetadata * metadata,
							 List *updates);


/*
 * IsWritableExternalRestCatalogTable returns whether the relation is a REST
 * catalog table with external='true'.
 */
bool
IsWritableExternalRestCatalogTable(Oid relationId)
{
	return IsPgLakeIcebergForeignTableById(relationId) &&
		GetIcebergCatalogType(relationId) == REST_CATALOG_EXTERNAL_READ_WRITE;
}


/*
 * CreateExternalRestCatalogTableInCatalog creates the catalog table of a new
 * external table whose columns the user gave, when the catalog does not have
 * it yet. Field ids are assigned for the request only; the catalog may assign
 * its own, and we read them back from it like for any attached table.
 */
void
CreateExternalRestCatalogTableInCatalog(Oid relationId, List *tableElements)
{
	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot create external REST catalog table \"%s\" in a transaction block",
						get_rel_name(relationId)),
				 errdetail("The REST catalog creates the table on its own, "
						   "so it cannot be part of a larger transaction.")));

	List	   *columnDefs = NIL;

	/* table constraints are PostgreSQL's alone */
	ListCell   *elementCell = NULL;

	foreach(elementCell, tableElements)
	{
		if (IsA(lfirst(elementCell), ColumnDef))
			columnDefs = lappend(columnDefs, lfirst(elementCell));
	}

	bool		forAddColumn = false;
	List	   *columnMappings =
		CreatePostgresColumnMappingsForColumnDefs(relationId, columnDefs, forAddColumn);

	DataFileSchema *schema = palloc0(sizeof(DataFileSchema));

	schema->fields = palloc0(sizeof(DataFileSchemaField) * list_length(columnMappings));

	foreach_ptr(PostgresColumnMapping, columnMapping, columnMappings)
	{
		/*
		 * Every read compares the columns to the catalog's schema, so a type
		 * that comes back from Iceberg as another one would make the new
		 * table unreadable.
		 */
		PGType		icebergType = IcebergFieldToPostgresType(columnMapping->field->type);

		if (!TypesAreCompatible(columnMapping->pgType, icebergType))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("column \"%s\" of type %s is stored in Iceberg as %s",
							columnMapping->attname,
							format_type_with_typemod(columnMapping->pgType.postgresTypeOid,
													 columnMapping->pgType.postgresTypeMod),
							format_type_with_typemod(icebergType.postgresTypeOid,
													 icebergType.postgresTypeMod)),
					 errhint("Use type %s for an external REST catalog table.",
							 format_type_with_typemod(icebergType.postgresTypeOid,
													  icebergType.postgresTypeMod))));

		schema->fields[schema->nfields++] = *columnMapping->field;
	}

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	(void) RegisterNamespaceToRestCatalog(opts, GetRestCatalogName(relationId),
										  GetRestCatalogNamespace(relationId));

	const char *requestedLocation = NULL;
	bool		stageCreate = false;

	(void) CreateRestCatalogIcebergTable(relationId, schema, requestedLocation,
										 stageCreate);
}


/*
 * PrepareExternalRestCatalogTableWrite checks that a statement may write the
 * external table, before it writes any file.
 */
void
PrepareExternalRestCatalogTableWrite(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);

	if (GetBoolOption(foreignTable->options, "row_ids", false))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row_ids are not supported on external REST catalog tables")));

	IcebergTableMetadata *metadata = ReadExternalTableMetadata(relationId);

	/* as a scan would, so that we write the files with the right field ids */
	ErrorIfSchemasDoNotMatch(relationId, metadata);
	ErrorIfPartitionedExternalTable(relationId, metadata);

	ErrorIfOtherCatalog(relationId);

	if (!XactWritesExternalTables)
	{
		ErrorIfXactWrotePostgresTables(relationId);

		/*
		 * The files the transaction writes are live as soon as the catalog
		 * accepts the commit, whatever happens to the transaction after that,
		 * so VACUUM must not take them for files of an aborted transaction.
		 */
		SkipInProgressFileRecords();

		XactWritesExternalTables = true;
	}
}


/*
 * RecordExternalRestCatalogTableChanges keeps the data file operations a
 * statement produced for an external table until the commit.
 */
void
RecordExternalRestCatalogTableChanges(Oid relationId, List *metadataOperations)
{
	List	   *operations = NIL;
	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	foreach_ptr(TableMetadataOperation, operation, metadataOperations)
	{
		TableMetadataOperation *copy =
			CopyExternalTableOperation(operation, metadataOperations);

		if (copy != NULL)
			operations = lappend(operations, copy);
	}

	if (operations != NIL)
	{
		ExternalTableChange *change = palloc0(sizeof(ExternalTableChange));

		change->relationId = relationId;
		change->subXactId = GetCurrentSubTransactionId();
		change->operations = operations;

		ExternalTableChanges = lappend(ExternalTableChanges, change);
	}

	MemoryContextSwitchTo(oldContext);
}


/*
 * CopyExternalTableOperation copies an operation that changes the files of an
 * external table into the current memory context, or returns NULL for one
 * that only concerns our own catalogs.
 */
static TableMetadataOperation *
CopyExternalTableOperation(TableMetadataOperation * operation, List *metadataOperations)
{
	switch (operation->type)
	{
		case DATA_FILE_ADD:
			{
				TableMetadataOperation *copy =
					AddDataFileOperation(pstrdup(operation->path), operation->content,
										 DeepCopyDataFileStats(&operation->dataFileStats),
										 operation->partition != NULL ?
										 CopyPartition(operation->partition) : NULL,
										 operation->partitionSpecId);

				/* for forgetting a deletion file along with its data file */
				foreach_ptr(TableMetadataOperation, mapping, metadataOperations)
				{
					if (mapping->type == DATA_FILE_ADD_DELETE_MAPPING &&
						strcmp(mapping->path, operation->path) == 0)
						copy->deletedFrom = pstrdup(mapping->deletedFrom);
				}

				return copy;
			}

		case DATA_FILE_REMOVE:
			return RemoveDataFileOperation(pstrdup(operation->path));

		case DATA_FILE_REMOVE_ALL:
			{
				TableMetadataOperation *copy = palloc0(sizeof(TableMetadataOperation));

				copy->type = DATA_FILE_REMOVE_ALL;
				return copy;
			}

			/* bookkeeping of our own catalogs */
		case DATA_FILE_ADD_DELETE_MAPPING:
		case DATA_FILE_UPDATE_DELETED_ROW_COUNT:
		case DATA_FILE_DROP_TABLE:
			return NULL;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("only data changes can be made to an external REST catalog table")));
	}
}


/*
 * GetPendingExternalTableOperations returns the changes the transaction made
 * to the files of an external table so far, relative to the snapshot it read:
 * removing all files, removing files of that snapshot, and adding files.
 * Files the transaction added and removed again are left out.
 */
List *
GetPendingExternalTableOperations(Oid relationId)
{
	List	   *pendingOperations = NIL;

	foreach_ptr(ExternalTableChange, change, ExternalTableChanges)
	{
		if (change->relationId != relationId)
			continue;

		foreach_ptr(TableMetadataOperation, operation, change->operations)
		{
			if (operation->type == DATA_FILE_REMOVE_ALL)
				pendingOperations = list_make1(operation);
			else if (operation->type == DATA_FILE_ADD)
				pendingOperations = lappend(pendingOperations, operation);
			else if (!ForgetPendingDataFile(&pendingOperations, operation->path))
				pendingOperations = lappend(pendingOperations, operation);
		}
	}

	return pendingOperations;
}


/*
 * ForgetPendingDataFile removes the deletion files for a data file that is
 * removed from the pending operations, and the data file itself if the
 * transaction added it, in which case it returns true.
 */
static bool
ForgetPendingDataFile(List **pendingOperations, const char *path)
{
	List	   *remainingOperations = NIL;
	bool		forgotDataFile = false;

	foreach_ptr(TableMetadataOperation, operation, *pendingOperations)
	{
		if (operation->type == DATA_FILE_ADD &&
			operation->content == CONTENT_DATA &&
			strcmp(operation->path, path) == 0)
		{
			forgotDataFile = true;
			continue;
		}

		if (operation->type == DATA_FILE_ADD &&
			operation->content == CONTENT_POSITION_DELETES &&
			operation->deletedFrom != NULL &&
			strcmp(operation->deletedFrom, path) == 0)
			continue;

		remainingOperations = lappend(remainingOperations, operation);
	}

	*pendingOperations = remainingOperations;

	return forgotDataFile;
}


/*
 * CommitExternalRestCatalogTableChanges sends the commit of the external
 * tables the transaction wrote to their REST catalog, at pre-commit. An error
 * aborts the transaction.
 */
void
CommitExternalRestCatalogTableChanges(void)
{
	List	   *relationIds = NIL;

	foreach_ptr(ExternalTableChange, change, ExternalTableChanges)
		relationIds = list_append_unique_oid(relationIds, change->relationId);

	List	   *tableChanges = NIL;
	Oid			firstRelationId = InvalidOid;

	foreach_oid(relationId, relationIds)
	{
		/* dropped later in the transaction */
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(relationId)))
			continue;

		List	   *pendingOperations = GetPendingExternalTableOperations(relationId);

		if (pendingOperations == NIL)
			continue;

		/* the snapshot the transaction read and wrote its files for */
		IcebergTableMetadata *metadata = ReadExternalTableMetadata(relationId);

		ErrorIfPartitionedExternalTable(relationId, metadata);

		List	   *updates =
			BuildSnapshotOnExternalTableMetadata(relationId, metadata, pendingOperations,
												 AllPartitionTransformList(relationId));

		if (updates == NIL)
			continue;

		if (!OidIsValid(firstRelationId))
			firstRelationId = relationId;

		tableChanges = lappend(tableChanges, TableChangeJson(relationId, metadata, updates));
	}

	/* whatever the catalog answers, there is nothing left to send */
	ExternalTableChanges = NIL;

	if (tableChanges == NIL)
		return;

	/* the manifests have to be in object storage before the catalog commits */
	FinishAllUploads();

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(firstRelationId);
	StringInfo	body = makeStringInfo();

	appendStringInfoString(body, "{\"table-changes\":[");

	foreach_ptr(char, tableChange, tableChanges)
	{
		if (tableChange != linitial(tableChanges))
			appendStringInfoChar(body, ',');

		appendStringInfoString(body, tableChange);
	}

	appendStringInfoString(body, "]}");

	char	   *url = psprintf(REST_CATALOG_TRANSACTION_COMMIT, opts->baseUri,
							   URLEncodePrefix(XactCatalogName));
	char	   *tableName = get_rel_name(firstRelationId);
	bool		multipleTables = list_length(tableChanges) > 1;

	INJECTION_POINT_COMPAT("before-external-rest-catalog-commit");

	HttpResult	httpResult =
		SendRequestToRestCatalog(opts, REST_CATALOG_OP_PUBLISH_COMMIT, HTTP_POST,
								 url, body->data, PostHeadersWithAuth(opts));

	/* Polaris and Nessie answer 204, Lakekeeper 200 */
	if (httpResult.status == 200 || httpResult.status == 204)
	{
		INJECTION_POINT_COMPAT("after-external-rest-catalog-commit");
		return;
	}

	if (httpResult.status == 409)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 multipleTables ?
				 errmsg("could not commit to external REST catalog tables due to a "
						"concurrent change") :
				 errmsg("could not commit to external REST catalog table \"%s\" due to "
						"a concurrent change", tableName),
				 errdetail("Another writer changed the table after this transaction read it."),
				 errhint("Retry the transaction.")));

	ReportHTTPError(httpResult, ERROR, REST_CATALOG_OP_PUBLISH_COMMIT,
					XactCatalogName, NULL, NULL);
}


/*
 * ResetExternalRestCatalogTableChanges forgets the external table changes and
 * writes of a transaction that ended, whose lists went with
 * TopTransactionContext.
 */
void
ResetExternalRestCatalogTableChanges(void)
{
	ExternalTableChanges = NIL;
	XactWritesExternalTables = false;
	XactCatalogServerId = InvalidOid;
	XactCatalogName = NULL;
	XactWrittenRelationIds = NIL;
}


/*
 * ErrorIfPrepareWithExternalTableChanges errors for PREPARE TRANSACTION in a
 * transaction that wrote external tables, whose commit the catalog decides.
 */
void
ErrorIfPrepareWithExternalTableChanges(void)
{
	if (XactWritesExternalTables)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot prepare a transaction that modified external REST "
						"catalog tables")));
}


/*
 * ExternalRestCatalogTableSubXactCallback forgets the changes of a rolled
 * back savepoint, and hands those of a released one to its parent.
 */
void
ExternalRestCatalogTableSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
										SubTransactionId parentSubid, void *arg)
{
	ListCell   *changeCell = NULL;

	if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		foreach(changeCell, ExternalTableChanges)
		{
			ExternalTableChange *change = lfirst(changeCell);

			if (change->subXactId == mySubid)
				ExternalTableChanges = foreach_delete_current(ExternalTableChanges, changeCell);
		}
	}
	else if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		foreach_ptr(ExternalTableChange, change, ExternalTableChanges)
		{
			if (change->subXactId == mySubid)
				change->subXactId = parentSubid;
		}
	}
}


/*
 * RecordTableWriteInTx records that the transaction writes to a relation, and
 * errors if it also writes external tables and the relation commits through
 * PostgreSQL. The PostgreSQL commit happens after the catalog commit, and if
 * it failed the two would disagree.
 */
void
RecordTableWriteInTx(Oid relationId)
{
	if (XactWritesExternalTables)
	{
		if (IsPostgresTableWrite(relationId))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot write to table \"%s\" in a transaction that modifies "
							"external REST catalog tables", get_rel_name(relationId)),
					 errdetail("The REST catalog commits the changes to external tables, so "
							   "the transaction cannot include writes that commit through "
							   "PostgreSQL."),
					 errhint("Write to the table in a separate transaction.")));
		return;
	}

	if (list_member_oid(XactWrittenRelationIds, relationId))
		return;

	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	XactWrittenRelationIds = lappend_oid(XactWrittenRelationIds, relationId);
	MemoryContextSwitchTo(oldContext);
}


/*
 * IsPostgresTableWrite returns whether a write to the given relation commits
 * through PostgreSQL, as opposed to through the REST catalog.
 */
static bool
IsPostgresTableWrite(Oid relationId)
{
	if (IsWritableExternalRestCatalogTable(relationId))
		return false;

	/* temporary tables do not outlive the session */
	if (get_rel_persistence(relationId) == RELPERSISTENCE_TEMP)
		return false;

	/* bookkeeping of pg_lake itself */
	if (OidIsValid(getExtensionOfObject(RelationRelationId, relationId)))
		return false;

	return true;
}


/*
 * ErrorIfXactWrotePostgresTables errors if the transaction already wrote to
 * a table that commits through PostgreSQL, now that it writes external table
 * relationId.
 */
static void
ErrorIfXactWrotePostgresTables(Oid relationId)
{
	foreach_oid(writtenRelationId, XactWrittenRelationIds)
	{
		/* dropped again later in the transaction */
		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(writtenRelationId)))
			continue;

		if (!IsPostgresTableWrite(writtenRelationId))
			continue;

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot modify external REST catalog table \"%s\" after writing to "
						"table \"%s\" in the same transaction",
						get_rel_name(relationId), get_rel_name(writtenRelationId)),
				 errdetail("The REST catalog commits the changes to external tables, so "
						   "the transaction cannot include writes that commit through "
						   "PostgreSQL."),
				 errhint("Write to the table in a separate transaction.")));
	}
}


/*
 * ErrorIfOtherCatalog errors if the transaction already writes external
 * tables of another REST catalog. One transactions/commit request goes to one
 * catalog.
 */
static void
ErrorIfOtherCatalog(Oid relationId)
{
	Oid			serverId = GetRestCatalogOptionsForRelation(relationId)->serverOid;
	char	   *catalogName = GetRestCatalogName(relationId);

	if (!OidIsValid(XactCatalogServerId))
	{
		XactCatalogServerId = serverId;
		XactCatalogName = MemoryContextStrdup(TopTransactionContext, catalogName);
		return;
	}

	if (serverId != XactCatalogServerId || strcmp(catalogName, XactCatalogName) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot modify external REST catalog tables of different catalogs "
						"in one transaction")));
}


/*
 * ReadExternalTableMetadata returns the metadata of the snapshot the current
 * statement reads, which the external catalog cache holds for it.
 */
static IcebergTableMetadata *
ReadExternalTableMetadata(Oid relationId)
{
	char	   *metadataLocation = GetIcebergMetadataLocation(relationId, false);

	return ReadIcebergTableMetadataForRelation(relationId, metadataLocation);
}


/*
 * ErrorIfPartitionedExternalTable errors for a table with a partition spec
 * that has fields. New files go into spec 0, and the partition values of a
 * file come from our catalogs, which hold nothing for this table.
 */
static void
ErrorIfPartitionedExternalTable(Oid relationId, IcebergTableMetadata * metadata)
{
	bool		unpartitioned = metadata->default_spec_id == DEFAULT_SPEC_ID;

	/* a file of an earlier partitioned spec would also need its partition */
	for (size_t specIndex = 0; specIndex < metadata->partition_specs_length; specIndex++)
	{
		if (metadata->partition_specs[specIndex].fields_length > 0)
			unpartitioned = false;
	}

	if (!unpartitioned)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("writing to partitioned external REST catalog table \"%s\" is not supported",
						get_rel_name(relationId))));
}


/*
 * TableChangeJson returns the table-changes entry that commits updates to the
 * table, on condition that the catalog still has the table, main branch
 * snapshot, schema and partition spec of metadata.
 */
static char *
TableChangeJson(Oid relationId, IcebergTableMetadata * metadata, List *updates)
{
	StringInfo	change = makeStringInfo();

	appendStringInfoString(change, "{\"identifier\":{\"namespace\":[");
	appendStringInfoString(change, EscapeJson(GetRestCatalogNamespace(relationId)));
	appendStringInfoString(change, "],\"name\":");
	appendStringInfoString(change, EscapeJson(GetRestCatalogTableName(relationId)));
	appendStringInfoString(change, "},\"requirements\":[");

	/* the table we read, not one dropped and created again under its name */
	appendStringInfoString(change, "{\"type\":\"assert-table-uuid\",\"uuid\":");
	appendStringInfoString(change, EscapeJson(metadata->table_uuid));
	appendStringInfoString(change, "},");

	/* the snapshot we read, so a commit since then fails the request */
	bool		missingOk = true;
	IcebergSnapshot *snapshot = GetCurrentSnapshot(metadata, missingOk);

	appendStringInfoString(change,
						   "{\"type\":\"assert-ref-snapshot-id\",\"ref\":\"main\",\"snapshot-id\":");
	if (snapshot != NULL)
		appendStringInfo(change, INT64_FORMAT, snapshot->snapshot_id);
	else
		appendStringInfoString(change, "null");
	appendStringInfoString(change, "},");

	/*
	 * A schema or partition change adds no snapshot, but our files are
	 * written for the schema and field ids we read.
	 */
	appendStringInfo(change,
					 "{\"type\":\"assert-current-schema-id\",\"current-schema-id\":%d},"
					 "{\"type\":\"assert-last-assigned-field-id\",\"last-assigned-field-id\":%d},"
					 "{\"type\":\"assert-default-spec-id\",\"default-spec-id\":%d},"
					 "{\"type\":\"assert-last-assigned-partition-id\",\"last-assigned-partition-id\":%d}",
					 metadata->current_schema_id, metadata->last_column_id,
					 metadata->default_spec_id, metadata->last_partition_id);
	appendStringInfoString(change, "],\"updates\":[");

	foreach_ptr(RestCatalogRequest, request, updates)
	{
		if (request != linitial(updates))
			appendStringInfoChar(change, ',');

		appendStringInfoString(change, request->body);
	}

	appendStringInfoString(change, "]}");

	return change->data;
}
