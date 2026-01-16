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
#include "string.h"

#include "commands/tablecmds.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

#include "access/table.h"
#include "access/xact.h"
#include "catalog/namespace.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_class.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "pg_lake/ddl/ddl_changes.h"
#include "pg_lake/ddl/drop_table.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/data_file_stats_catalog.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/iceberg/operations/find_referenced_files.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/parsetree/options.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/inval.h"

/* controlled via GUC */
bool		SkipDropAccessHook = false;

static bool IsDropPgLakeTable(DropStmt *dropStmt);
static void PgLakeTableObjectsInDropStmt(DropStmt *dropStmt,
										 List **pgLakeTableObjects,
										 List **otherTableObjects);
static void DropTableAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
								int subId, void *arg);
static bool MarkAllReferencedFilesForDeletion(Oid relationId);


static object_access_hook_type PreviousObjectAccessHook = NULL;


/*
 * InitializeDropTableHandler sets up the object_access_hook for
 * processing DROP commands for pg_lake tables.
 */
void
InitializeDropTableHandler(void)
{
	PreviousObjectAccessHook = object_access_hook;
	object_access_hook = DropTableAccessHook;
}

/*
 * ProcessDropPgLakeTable processes DROP TABLE statements that
 * drop pg_lake tables.
 */
bool
ProcessDropPgLakeTable(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, DropStmt))
	{
		return false;
	}

	DropStmt   *dropStmt = (DropStmt *) plannedStmt->utilityStmt;

	if (!IsDropPgLakeTable(dropStmt))
	{
		return false;
	}

	if (params->readOnlyTree)
	{
		plannedStmt = copyObject(plannedStmt);
		params->plannedStmt = plannedStmt;

		/*
		 * make sure modifications are reflected in call to
		 * PgLakeCommonProcessUtility
		 */
		dropStmt = (DropStmt *) plannedStmt->utilityStmt;
	}

	List	   *pgLakeTableObjects = NIL;
	List	   *otherTableObjects = NIL;

	PgLakeTableObjectsInDropStmt(dropStmt, &pgLakeTableObjects, &otherTableObjects);

	/* drop lake tables */
	dropStmt->removeType = OBJECT_FOREIGN_TABLE;
	dropStmt->objects = pgLakeTableObjects;
	RemoveRelations(dropStmt);

	/*
	 * remove lake tables from original statement since we already dropped
	 * them
	 */
	dropStmt->removeType = OBJECT_TABLE;
	dropStmt->objects = otherTableObjects;

	/* continue with regular process utility to handle DROP TABLE */
	PgLakeCommonProcessUtility(params);

	return true;
}

/*
 * IsDropPgLakeTable returns whether given DROP TABLE statement
 * will drop a pg_lake table.
 */
static bool
IsDropPgLakeTable(DropStmt *dropStmt)
{
	if (dropStmt->removeType != OBJECT_TABLE)
	{
		return false;
	}

	List	   *pgLakeTableObjects = NIL;
	List	   *otherTableObjects = NIL;

	PgLakeTableObjectsInDropStmt(dropStmt, &pgLakeTableObjects, &otherTableObjects);

	if (list_length(pgLakeTableObjects) == 0)
	{
		return false;
	}

	return true;
}

/*
 * PgLakeTableObjectsInDropStmt sets the list of lake
 * tables and other table objects in the DROP TABLE statement.
 */
static void
PgLakeTableObjectsInDropStmt(DropStmt *dropStmt,
							 List **pgLakeTableObjects,
							 List **otherTableObjects)
{
	ListCell   *droppedTableObjectCell = NULL;

	foreach(droppedTableObjectCell, dropStmt->objects)
	{
		List	   *droppedTableObject = lfirst(droppedTableObjectCell);
		RangeVar   *rel = makeRangeVarFromNameList(droppedTableObject);

		/* if table doesn't exist, let Postgres handle */
		bool		missingOk = true;
		Oid			tableOid = RangeVarGetRelid(rel, NoLock, missingOk);

		if (OidIsValid(tableOid) && IsAnyLakeForeignTableById(tableOid))
		{
			*pgLakeTableObjects = lappend(*pgLakeTableObjects, droppedTableObject);
		}
		else
		{
			*otherTableObjects = lappend(*otherTableObjects, droppedTableObject);
		}
	}
}

/*
 * DropTableAccessHook checks for DROP events on pg_lake tables, including
 * DROP TABLE and DROP COLUMN.
 */
static void
DropTableAccessHook(ObjectAccessType access, Oid classId, Oid objectId,
					int subId, void *arg)
{
	if (PreviousObjectAccessHook)
		PreviousObjectAccessHook(access, classId, objectId, subId, arg);

	if (SkipDropAccessHook)
		return;

	if (access != OAT_DROP)
		return;

	if (classId != RelationRelationId)
		return;

	/* avoid doing stuff when the extension is winding down */
	if (!DataFilesCatalogExists() || !IcebergTablesCatalogExists() || !DataFileColumnStatsCatalogExists() ||
		!DataFilesPartitionValuesCatalogExists() || !PartitionSpecsCatalogExists() || !PartitionFieldsCatalogExists())
		return;

	bool		isColumn = subId != 0;

	if (IsWritablePgLakeTable(objectId))
	{
		if (isColumn)
		{
			/*
			 * We currently do not take action when dropping a column of a
			 * regular writable table.
			 */
		}
		else
		{
			/*
			 * remove files from metadata (unless we're winding down the
			 * extension)
			 */
			RemoveAllDataFilesFromTable(objectId);
		}
	}
	else if (IsWritableIcebergTable(objectId))
	{
		if (isColumn)
		{
			/*
			 * We do not allow dropping columns from iceberg tables that are
			 * used in partition spec. Note that we currently do not support
			 * dropping columns that are even used to be part of the partition
			 * spec but not used in the current partition spec. Note that
			 * Spark crashes in that scenario and it complicates our
			 * implementation.
			 */
			StringInfo	errorDetail = makeStringInfo();

			appendStringInfo(errorDetail,
							 "drop column \"%s\"",
							 get_attname(objectId, subId, false));

			ErrorIfColumnEverUsedInIcebergPartitionSpec(objectId, subId, errorDetail->data);

			/*
			 * When a column gets dropped, we should remove it from the
			 * Iceberg schema.
			 *
			 * Dropping columns could be triggered directly via ALTER TABLE
			 * DROP COLUMN or could be cascading from DROP TYPE / DROP OWNED
			 * BY etc. That's why we prefer to implement this logic in drop
			 * object hook over ALTER TABLE hook.
			 */

			IcebergDDLOperation *ddlOperation = palloc0(sizeof(IcebergDDLOperation));

			ddlOperation->type = DDL_COLUMN_DROP;
			ddlOperation->attrNumber = subId;

			ApplyDDLChanges(objectId, list_make1(ddlOperation));

			/*
			 * When using replication, the replay table is already updated by
			 * the AlterTable DDL hook, so we do not need to update it here,
			 * despite it being where the Iceberg column itself is dropped.
			 */
		}
		else
		{
			IcebergDDLOperation *ddlOperation = palloc0(sizeof(IcebergDDLOperation));

			ddlOperation->type = DDL_TABLE_DROP;

			ApplyDDLChanges(objectId, list_make1(ddlOperation));

			TriggerCatalogExportIfObjectStoreTable(objectId);

			IcebergCatalogType catalogType = GetIcebergCatalogType(objectId);

			if (catalogType == REST_CATALOG_READ_WRITE)
				RecordRestCatalogRequestInTx(objectId, REST_CATALOG_DROP_TABLE, NULL);
		}
	}
	else if (get_rel_type_id(objectId) != InvalidOid && subId != 0)
	{
		/*
		 * Handling ALTER TYPE .. DROP ATTRIBUTE or any other command such as
		 * DROP OWNED BY that drops an attribute from a type.
		 */
		Oid			typeId = get_rel_type_id(objectId);

		if (CheckIfTypeIsUsedInIcebergTable(typeId))
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot alter type \"%s\" because it is used in an iceberg table",
							get_rel_name(objectId))));

	}
}


/*
* PreventCitusTableVisibility sets the citus.override_table_visibility GUC
* to off, so that Citus tables are not visible in the iceberg tables.
* We need this, otherwise DROP EXTENSION citus CASCADE will fail.
*/
static void
PreventCitusTableVisibility(void)
{
	SetConfigOption("citus.override_table_visibility", "off", PGC_USERSET, PGC_S_OVERRIDE);
}

/*
* CheckIfTypeIsUsedInIcebergTable checks if the given type is used in any
* iceberg table.
*
* It also recursively checks if any type that depends on
* the given type is used in any iceberg table.
*/
bool
CheckIfTypeIsUsedInIcebergTable(Oid typeId)
{

	if (!IsExtensionCreated(PgLakeTable))
		return false;

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "WITH RECURSIVE dependent_types(type_oid) AS ( "
					 "   SELECT $1::oid AS type_oid "
					 "UNION "
					 " SELECT t.oid AS type_oid "
					 "    FROM pg_type t "
					 "   JOIN pg_attribute a ON a.attrelid = t.typrelid "
					 "  JOIN dependent_types dt ON a.atttypid = dt.type_oid "
					 " WHERE NOT a.attisdropped "
					 ") "
					 "SELECT EXISTS ( "
					 "    SELECT 1 "
					 "    FROM lake_iceberg.tables_internal tbl "
					 "    JOIN pg_class c ON c.oid = tbl.table_name "
					 "    JOIN pg_attribute attr ON attr.attrelid = c.oid "
					 "    WHERE NOT attr.attisdropped "
					 "      AND attr.atttypid IN (SELECT type_oid FROM dependent_types));");


	/* set the user context */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	SPI_START();

	/* SPI_END will rollback this setting to previous value */
	PreventCitusTableVisibility();

	bool		readOnly = true;

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, typeId, false);

	SPI_EXECUTE(query->data, readOnly);

	bool		isNull = false;
	bool		exists = GET_SPI_VALUE(BOOLOID, 0, 1, &isNull);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return exists;
}


/*
* TryMarkAllReferencedFilesForDeletion tries to mark all files referenced by the
* given iceberg table for deletion.
* In case of failure to access the blob store, it marks the data files in the
* lake_table.files catalog for deletion.
*/
void
TryMarkAllReferencedFilesForDeletion(Oid relationId)
{
	bool		deleted = MarkAllReferencedFilesForDeletion(relationId);

	/*
	 * OK, failed to delete by accessing the blob storage, most likely we
	 * couldn't connect to the blob store or our access is revoked.
	 *
	 * We mark the iceberg table for deletion as the whole location prefix.
	 * That way, we could retry the deletion of the files with VACUUM. We do
	 * not want to interfere with the deletion of the iceberg tables that are
	 * not using the default location prefix. For those, we only warn the
	 * user.
	 *
	 * Note that after we add the files to the deletion queue, VACUUM will
	 * take care of the rest. It'll try to delete the files from the blob
	 * store up to a specified limit. If that fails, VACUUM will bail out for
	 * these files.
	 */
	if (!deleted)
	{
		char	   *queryArguments = "";
		char	   *tableLocation = GetWritableTableLocation(relationId, &queryArguments);

		if (HasCustomLocation(relationId))
		{
			/*
			 * If the table has a custom location prefix, so we don't manage
			 * its storage. It means that the user might have used the same
			 * prefix for multiple tables, and we wouldn't dare to delete
			 * files that might be used in other tables.
			 */
			ereport(WARNING,
					(errmsg("failed to access blob storage location %s for table %s",
							tableLocation, GetQualifiedRelationName(relationId)),
					 errdetail("Files will not be marked for deletion, and may have "
							   "to be removed manually.")));

			return;
		}

		TimestampTz orphanedAt = GetCurrentTransactionStartTimestamp();

		InsertPrefixDeletionRecord(tableLocation, orphanedAt);
	}
}


/*
* MarkAllReferencedFilesForDeletion marks all files referenced by the
* given iceberg table for deletion.
* Returns false if it failed to find the files, likely indicating a
* connection issue with the blob store.
*/
static bool
MarkAllReferencedFilesForDeletion(Oid relationId)
{
	TimestampTz orphanedAt = GetCurrentTransactionStartTimestamp();
	char	   *metadataLocation = GetIcebergMetadataLocation(relationId, true);
	MemoryContext savedContext = CurrentMemoryContext;

	List	   *allFiles = NIL;
	volatile bool success = true;

	PG_TRY();
	{
		allFiles = IcebergFindAllReferencedFiles(metadataLocation);
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		/* continue unless it was a cancellation */
		if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
			edata->elevel = WARNING;

		/* report as warning */
		ThrowErrorData(edata);
		success = false;
	}
	PG_END_TRY();

	if (!success)
	{
		/*
		 * OK, failed to find the files, most likely we couldn't connect to
		 * the blob store or our access is revoked.
		 */
		return false;
	}

	ListCell   *fileCell = NULL;

	foreach(fileCell, allFiles)
	{
		char	   *filePath = lfirst(fileCell);

		/* the table is dropped, so it won't have a valid oid anymore */
		Oid			tableId = InvalidOid;

		InsertDeletionQueueRecord(filePath, tableId, orphanedAt);
	}

	return true;
}
