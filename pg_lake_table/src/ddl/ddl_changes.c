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
#include "miscadmin.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/ddl/ddl_changes.h"
#include "pg_lake/ddl/drop_table.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"

#include "utils/timestamp.h"
#include "utils/inval.h"

static void ApplyDDLCatalogChanges(Oid relationId, List *ddlOperations,
								   List **droppedColumns, char **metadataLocation,
								   IcebergPartitionSpec * *partitionSpec);
static bool DDLsRequireIcebergSchemaChange(List *ddlOperations);


/*
 * ApplyDDLChanges applies given ddl operations to internal catalog tables.
 * Then, it applies a change to iceberg metadata.
 */
void
ApplyDDLChanges(Oid relationId, List *ddlOperations)
{
	List	   *droppedColumns = NIL;

	char	   *metadataLocation = NULL;

	IcebergPartitionSpec *partitionSpec = NULL;

	ApplyDDLCatalogChanges(relationId, ddlOperations, &droppedColumns, &metadataLocation, &partitionSpec);

	bool		isIcebergTableCreated = metadataLocation != NULL;
	bool		isPartitionByChange = partitionSpec != NULL;

	if (isIcebergTableCreated)
		TrackIcebergMetadataChangesInTx(relationId, list_make1_int(TABLE_CREATE));
	else if (isPartitionByChange)
		TrackIcebergMetadataChangesInTx(relationId, list_make1_int(TABLE_PARTITION_BY));

	/*
	 * we apply a single iceberg metadata operation if any of the ddl requires
	 * schema change
	 */
	else if (DDLsRequireIcebergSchemaChange(ddlOperations))
		TrackIcebergMetadataChangesInTx(relationId, list_make1_int(TABLE_DDL));
}



/*
 * ApplyDDLCatalogChanges applies given DDL operations to internal catalog tables.
 * - It fills droppedColumns with dropped column numbers.
 * - It also sets metadataLocation to the location of the iceberg metadata
 *  if an iceberg table is created.
 * - It also sets partitionSpec to the list of partition transforms
 *   if the partition_by option is changed.
 */
static void
ApplyDDLCatalogChanges(Oid relationId, List *ddlOperations,
					   List **droppedColumns, char **metadataLocation,
					   IcebergPartitionSpec * *partitionSpec)
{
	ListCell   *ddlOperationCell = NULL;

	foreach(ddlOperationCell, ddlOperations)
	{
		IcebergDDLOperation *ddlOperation = lfirst(ddlOperationCell);

		if (ddlOperation->type == DDL_TABLE_CREATE)
		{
			if (*metadataLocation != NULL)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("cannot create the same iceberg table multiple times")));
			}

			/* register the table */
			*metadataLocation = GenerateInitialIcebergTableMetadataPath(relationId);
			InsertInternalIcebergCatalogTable(relationId, *metadataLocation, ddlOperation->hasCustomLocation);

			/* register mappings for new columns */
			bool		forAddColumn = false;
			List	   *columnMappings = CreatePostgresColumnMappingsForColumnDefs(relationId,
																				   ddlOperation->columnDefs,
																				   forAddColumn);

			RegisterPostgresColumnMappings(columnMappings);
		}
		else if (ddlOperation->type == DDL_TABLE_DROP)
		{
			/*
			 * This is not an expected case, either user manually messed with
			 * the catalog or we have a bug. Still, we should not fail the
			 * DROP TABLE command.
			 */
			if (!RelationExistsInTheIcebergCatalog(relationId))
			{
				RemoveAllDataFilesFromPgLakeCatalogFromTable(relationId);

				return;
			}

			/*
			 * We intentionally do not call ErrorIfReadOnlyIcebergTable here
			 * because users might want to get rid-of the iceberg table on
			 * forks. If we ever remove the remote files with DROP TABLE, we
			 * should call ErrorIfReadOnlyIcebergTable here.
			 *
			 * Also, for read-only tables, we will not mark the files for
			 * deletion.
			 */
			if (!IsReadOnlyIcebergTable(relationId))
			{
				/*
				 * metadata is not pushed yet if table is created in current
				 * tx. Do not try to delete it. Files created in this
				 * transaction are in in_progress files, so they'll be cleaned
				 * up separately.
				 */
				if (!IsIcebergTableCreatedInCurrentTransaction(relationId))
					TryMarkAllReferencedFilesForDeletion(relationId);

				/*
				 * previous_metadata location is a special file that is not
				 * referenced by the iceberg metadata, but may not have been
				 * deleted when the table was dropped. Normally, when a new
				 * metadata file is written, the previous metadata file is
				 * added to the deletion queue. However, here are not going to
				 * create another metadata file, the table is dropped. So,
				 * this could be the only chance to delete the previous
				 * metadata file.
				 */
				char	   *previousMetadataPath =
					GetIcebergCatalogPreviousMetadataLocation(relationId, false);

				if (previousMetadataPath)
					InsertDeletionQueueRecord(previousMetadataPath, InvalidOid, GetCurrentTimestamp());
			}

			RemoveAllDataFilesFromPgLakeCatalogFromTable(relationId);

			/*
			 * Question - do we need to do anything special with
			 *
			 * ErrorIfIcebergReplicationTarget(relationId)?
			 */

			DeleteInternalIcebergCatalogTable(relationId);
		}
		else if (ddlOperation->type == DDL_COLUMN_ADD)
		{
			/* register mapping for new column */
			bool		forAddColumn = true;
			List	   *columnMappings = CreatePostgresColumnMappingsForColumnDefs(relationId,
																				   ddlOperation->columnDefs,
																				   forAddColumn);

			RegisterPostgresColumnMappings(columnMappings);
		}
		else if (ddlOperation->type == DDL_COLUMN_DROP)
		{
			/*
			 * track dropped column since column might not be dropped yet from
			 * catalog (e.g. when called from event hook)
			 */
			*droppedColumns = lappend_int(*droppedColumns, ddlOperation->attrNumber);
		}
		else if (ddlOperation->type == DDL_COLUMN_SET_DEFAULT ||
				 ddlOperation->type == DDL_COLUMN_DROP_DEFAULT)
		{
			/* update write default of the column */
			AttrNumber	attrNumber = ddlOperation->attrNumber;
			const char *writeDefault = ddlOperation->writeDefault;

			UpdateRegisteredFieldWriteDefaultForAttribute(relationId, attrNumber, writeDefault);
		}
		else if (ddlOperation->type == DDL_TABLE_SET_PARTITION_BY)
		{
			List	   *parsedTransforms = ddlOperation->parsedTransforms;
			List	   *analyzedTransforms = AnalyzeIcebergTablePartitionBy(relationId, parsedTransforms);

			*partitionSpec = GetPartitionSpecIfAlreadyExist(relationId, analyzedTransforms);
			if (*partitionSpec != NULL)
			{
				/* update lake_iceberg.internal_tables specId */
				UpdateDefaultPartitionSpecId(relationId, (*partitionSpec)->spec_id);

				return;
			}

			/*
			 * creatint a new spec, get the largest spec and we'll increment
			 * it if needed when assigning
			 */
			int			largestSpecId = GetLargestSpecId(relationId);

			*partitionSpec =
				BuildPartitionSpecFromPartitionTransforms(relationId, analyzedTransforms, largestSpecId);

			/* update lake_iceberg.internal_tables specId */
			UpdateDefaultPartitionSpecId(relationId, (*partitionSpec)->spec_id);

			/* insert new fields to lake_table.partition_fields */
			InsertPartitionSpecAndPartitionFields(relationId, *partitionSpec);
		}
		else if (ddlOperation->type == DDL_TABLE_DROP_PARTITION_BY)
		{
			IcebergPartitionSpec *defaultSpec = palloc0(sizeof(IcebergPartitionSpec));

			defaultSpec->spec_id = DEFAULT_SPEC_ID;
			defaultSpec->fields = NULL;
			defaultSpec->fields_length = 0;

			*partitionSpec = defaultSpec;

			/* update lake_iceberg.internal_tables specId */
			UpdateDefaultPartitionSpecId(relationId, DEFAULT_SPEC_ID);
		}
		else if (ddlOperation->type == DDL_TABLE_RENAME ||
				 ddlOperation->type == DDL_COLUMN_DROP_NOT_NULL)
		{
			/* nothing to do to catalog */
			continue;
		}
	}
}


/*
 * DDLsRequireIcebergSchemaChange returns whether given DDL operations
 * require a schema change in iceberg metadata.
 */
static bool
DDLsRequireIcebergSchemaChange(List *ddlOperations)
{
	ListCell   *ddlOperationCell = NULL;

	foreach(ddlOperationCell, ddlOperations)
	{
		IcebergDDLOperation *ddlOperation = lfirst(ddlOperationCell);

		if (ddlOperation->type == DDL_TABLE_RENAME ||
			ddlOperation->type == DDL_COLUMN_ADD ||
			ddlOperation->type == DDL_COLUMN_DROP ||
			ddlOperation->type == DDL_COLUMN_SET_DEFAULT ||
			ddlOperation->type == DDL_COLUMN_DROP_DEFAULT ||
			ddlOperation->type == DDL_COLUMN_DROP_NOT_NULL)
		{
			return true;
		}
	}

	return false;
}
