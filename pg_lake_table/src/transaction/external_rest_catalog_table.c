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
 * The REST catalog is the only place such a table's state lives, so a write
 * is a commit of a child of the snapshot the statement read, nothing more.
 * The statement writes its data files as usual, and the file writers hand us
 * the TableMetadataOperations they produced. We apply those to the table
 * metadata the statement read, which the external catalog cache holds for
 * it, write the manifests and manifest list, and keep the catalog updates
 * until pre-commit. Nothing goes into lake_table.files or the stats and
 * partition catalogs.
 *
 * At pre-commit we send one transactions/commit for every external table the
 * transaction wrote, requiring that each table still has the UUID and the
 * main branch snapshot we read. If another engine committed in between, the
 * catalog answers 409 and the PostgreSQL transaction aborts.
 *
 * Only the REST catalog commit makes the write visible, and it cannot be
 * undone, so such a write is not allowed in a transaction block: there would
 * be no way to make it atomic with anything after it, nor to show the
 * statement after it the files of a snapshot that is not committed yet.
 */
#include "postgres.h"

#include "access/xact.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pg_extension_base/pg_compat.h"
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/snapshot.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/transaction/external_rest_catalog_table.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/injection_points.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/util/url_encode.h"


/*
 * The commit of one external table, waiting for pre-commit.
 */
typedef struct ExternalTableCommit
{
	Oid			relationId;

	/* the REST catalog prefix, the same for every table in the transaction */
	char	   *catalogName;

	/* one entry of table-changes in the transactions/commit request */
	char	   *tableChange;
}			ExternalTableCommit;

/* list of ExternalTableCommit, in TopTransactionContext */
static List *ExternalTableCommits = NIL;

static IcebergTableMetadata * ReadExternalTableMetadata(Oid relationId);
static void ErrorIfPartitionedExternalTable(Oid relationId,
											IcebergTableMetadata * metadata);
static char *TableChangeJson(Oid relationId, IcebergTableMetadata * metadata,
							 List *updates);
static ExternalTableCommit * FindExternalTableCommit(Oid relationId);


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
 * PrepareExternalRestCatalogTableWrite checks that a statement may write the
 * external table, before it writes any file.
 */
void
PrepareExternalRestCatalogTableWrite(Oid relationId)
{
	if (IsTransactionBlock())
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				 errmsg("cannot modify external REST catalog table \"%s\" in a transaction block",
						get_rel_name(relationId)),
				 errdetail("The REST catalog commits each change to the table on its own, "
						   "so it cannot be part of a larger transaction.")));

	ForeignTable *foreignTable = GetForeignTable(relationId);

	if (GetBoolOption(foreignTable->options, "row_ids", false))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("row_ids are not supported on external REST catalog tables")));

	IcebergTableMetadata *metadata = ReadExternalTableMetadata(relationId);

	/* as a scan would, so that we write the files with the right field ids */
	ErrorIfSchemasDoNotMatch(relationId, metadata);
	ErrorIfPartitionedExternalTable(relationId, metadata);

	/*
	 * The files this statement writes are live as soon as the catalog accepts
	 * the commit, whatever happens to the transaction after that.
	 */
	DeferInProgressFileRecordDeletes();
}


/*
 * RecordExternalRestCatalogTableChanges builds the snapshot that applies the
 * operations a statement produced to the snapshot it read, and keeps the
 * catalog updates that commit it for pre-commit.
 */
void
RecordExternalRestCatalogTableChanges(Oid relationId, List *metadataOperations)
{
	if (metadataOperations == NIL)
		return;

	/*
	 * A second change would have to be a child of the first, which the
	 * catalog has not seen yet. Without transaction blocks this only happens
	 * when one statement writes the table twice.
	 */
	if (FindExternalTableCommit(relationId) != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot modify external REST catalog table \"%s\" more than once in a transaction",
						get_rel_name(relationId))));

	char	   *catalogName = GetRestCatalogName(relationId);

	/*
	 * One transactions/commit request goes to one catalog. We also check the
	 * server when sending, but there is no point writing the files first.
	 */
	if (ExternalTableCommits != NIL)
	{
		ExternalTableCommit *firstCommit = linitial(ExternalTableCommits);

		if (GetRestCatalogOptionsForRelation(firstCommit->relationId)->serverOid !=
			GetRestCatalogOptionsForRelation(relationId)->serverOid ||
			strcmp(firstCommit->catalogName, catalogName) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot modify external REST catalog tables of different catalogs "
							"in one transaction")));
	}

	/* the same metadata the statement scanned and wrote its files for */
	IcebergTableMetadata *metadata = ReadExternalTableMetadata(relationId);

	ErrorIfPartitionedExternalTable(relationId, metadata);

	List	   *updates =
		BuildSnapshotOnExternalTableMetadata(relationId, metadata, metadataOperations,
											 AllPartitionTransformList(relationId));

	if (updates == NIL)
		return;

	/* the manifests have to be in object storage before the catalog commits */
	FinishAllUploads();

	/*
	 * The data files are live once the catalog commits, so their in-progress
	 * records go with the ones of the manifests.
	 */
	List	   *addedPaths = NIL;

	foreach_ptr(TableMetadataOperation, operation, metadataOperations)
	{
		if (operation->type == DATA_FILE_ADD)
			addedPaths = lappend(addedPaths, (char *) operation->path);
	}

	DeleteInProgressFileRecords(addedPaths);

	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);

	ExternalTableCommit *commit = palloc0(sizeof(ExternalTableCommit));

	commit->relationId = relationId;
	commit->catalogName = pstrdup(catalogName);
	commit->tableChange = TableChangeJson(relationId, metadata, updates);

	ExternalTableCommits = lappend(ExternalTableCommits, commit);

	MemoryContextSwitchTo(oldContext);
}


/*
 * CommitExternalRestCatalogTableChanges sends the commit of the external
 * tables the transaction wrote to their REST catalog, at pre-commit. An error
 * aborts the transaction.
 */
void
CommitExternalRestCatalogTableChanges(void)
{
	if (ExternalTableCommits == NIL)
		return;

	/*
	 * The other tables commit through PostgreSQL, after the catalog has
	 * already made our change visible.
	 */
	if (HasAnyTrackedIcebergMetadataChanges())
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot modify external REST catalog tables and other Iceberg tables "
						"in one transaction")));

	ExternalTableCommit *firstCommit = linitial(ExternalTableCommits);
	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(firstCommit->relationId);
	StringInfo	body = makeStringInfo();

	appendStringInfoString(body, "{\"table-changes\":[");

	foreach_ptr(ExternalTableCommit, commit, ExternalTableCommits)
	{
		if (commit != firstCommit)
			appendStringInfoChar(body, ',');

		appendStringInfoString(body, commit->tableChange);
	}

	appendStringInfoString(body, "]}");

	char	   *url = psprintf(REST_CATALOG_TRANSACTION_COMMIT, opts->baseUri,
							   URLEncodePrefix(firstCommit->catalogName));
	char	   *tableName = get_rel_name(firstCommit->relationId);
	bool		multipleTables = list_length(ExternalTableCommits) > 1;

	/* whatever the catalog answers, there is nothing left to send */
	ExternalTableCommits = NIL;

	/*
	 * From here on, a crash leaves the files of the commit without a record,
	 * which leaks them if the catalog did not take the commit, but does not
	 * remove them if it did.
	 */
	List	   *committedPaths = FlushDeferredInProgressFileRecordDeletes();

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

	/*
	 * A 4xx answer means the catalog did not commit, so the files go back to
	 * cleanup. Without an answer, or with a 5xx one, the commit may have
	 * happened, so they have to stay.
	 */
	if (httpResult.status >= 400 && httpResult.status < 500)
		RestoreInProgressFileRecords(committedPaths);
	else
		ereport(WARNING,
				(errmsg("the REST catalog may have committed the change to \"%s\"",
						tableName),
				 errdetail("The files written for it are not removed.")));

	if (httpResult.status == 409)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 multipleTables ?
				 errmsg("could not commit to external REST catalog tables due to a "
						"concurrent change") :
				 errmsg("could not commit to external REST catalog table \"%s\" due to "
						"a concurrent change", tableName),
				 errdetail("Another writer committed to the table after this statement read it."),
				 errhint("Retry the statement.")));

	ReportHTTPError(httpResult, ERROR, REST_CATALOG_OP_PUBLISH_COMMIT,
					firstCommit->catalogName, NULL, NULL);
}


/*
 * ResetExternalRestCatalogTableChanges forgets the commits of an aborted
 * transaction, whose list went with TopTransactionContext.
 */
void
ResetExternalRestCatalogTableChanges(void)
{
	ExternalTableCommits = NIL;
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
 * ErrorIfPartitionedExternalTable errors for a table whose default partition
 * spec has fields. New files go into spec 0, and the partition values of a
 * file come from our catalogs, which hold nothing for this table.
 */
static void
ErrorIfPartitionedExternalTable(Oid relationId, IcebergTableMetadata * metadata)
{
	bool		unpartitioned = metadata->default_spec_id == DEFAULT_SPEC_ID;

	for (size_t specIndex = 0; specIndex < metadata->partition_specs_length; specIndex++)
	{
		IcebergPartitionSpec *spec = &metadata->partition_specs[specIndex];

		if (spec->spec_id == metadata->default_spec_id && spec->fields_length > 0)
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
 * table, on condition that the catalog still has the table and main branch
 * snapshot of metadata.
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
	appendStringInfoString(change, "}],\"updates\":[");

	foreach_ptr(RestCatalogRequest, request, updates)
	{
		if (request != linitial(updates))
			appendStringInfoChar(change, ',');

		appendStringInfoString(change, request->body);
	}

	appendStringInfoString(change, "]}");

	return change->data;
}


/*
 * FindExternalTableCommit returns the pending commit of the relation, or
 * NULL.
 */
static ExternalTableCommit *
FindExternalTableCommit(Oid relationId)
{
	foreach_ptr(ExternalTableCommit, commit, ExternalTableCommits)
	{
		if (commit->relationId == relationId)
			return commit;
	}

	return NULL;
}
