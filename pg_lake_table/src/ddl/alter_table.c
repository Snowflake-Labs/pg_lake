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

#include "access/heapam.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/heap.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"
#include "utils/inval.h"
#include "utils/fmgroids.h"

#include "pg_lake/access_method/access_method.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/ddl/alter_table.h"
#include "pg_lake/ddl/ddl_changes.h"
#include "pg_lake/ddl/create_table.h"
#include "pg_lake/ddl/drop_table.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/fdw/row_ids.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/util/rel_utils.h"


typedef enum PgLakeDDLType
{
	PG_LAKE_DDL_ALTER_TABLE,
	PG_LAKE_DDL_RENAME_TABLE,
	PG_LAKE_DDL_SET_SCHEMA,
}			PgLakeDDLType;

typedef struct PgLakeDDLTypeInfo
{
	/*
	 * relationId is currently only valid for alter table statements, not for
	 * rename table or set schema.
	 */
	Oid			relationId;
	PgLakeDDLType ddlType;
	union
	{
		AlterTableType alterTableCmdType;
		ObjectType	renameObjectType;
		ObjectType	alterSchemaObjectType;
	}			ddlInfo;
}			PgLakeDDLTypeInfo;

typedef struct PgLakeDDL
{
	PgLakeDDLTypeInfo ddlTypeInfo;
	bool		(*allowedForIceberg) (Node *, Oid relationId);
	bool		(*allowedForWritableLake) (Node *, Oid relationId);
}			PgLakeDDL;

#define ALTER_TABLE_DDL(cmdType, icebergFunc, writableLakeFunc) \
	{ \
		.ddlTypeInfo.ddlType = PG_LAKE_DDL_ALTER_TABLE, \
		.ddlTypeInfo.ddlInfo.alterTableCmdType = cmdType, \
		.allowedForIceberg = icebergFunc, \
		.allowedForWritableLake = writableLakeFunc \
	} \

#define RENAME_TABLE_DDL(objectType, icebergFunc, writableLakeFunc) \
	{ \
		.ddlTypeInfo.ddlType = PG_LAKE_DDL_RENAME_TABLE, \
		.ddlTypeInfo.ddlInfo.renameObjectType = objectType, \
		.allowedForIceberg = icebergFunc, \
		.allowedForWritableLake = writableLakeFunc \
	} \

#define ALTER_SCHEMA_DDL(objectType, icebergFunc, writableLakeFunc) \
	{ \
		.ddlTypeInfo.ddlType = PG_LAKE_DDL_SET_SCHEMA, \
		.ddlTypeInfo.ddlInfo.alterSchemaObjectType = objectType, \
		.allowedForIceberg = icebergFunc, \
		.allowedForWritableLake = writableLakeFunc \
	} \

/* functions used */
static bool Allowed(Node *arg, Oid relationId);
static bool Disallowed(Node *arg, Oid relationId);
static bool DisallowedAddColumnWithUnsupportedConstraints(Node *arg, Oid relationId);
static bool DisallowedForWritableRestRenameTable(Node *arg, Oid relationId);
static bool DisallowedForWritableRestSetSchema(Node *arg, Oid relationId);

PgLakeAlterTableHookType PgLakeAlterTableHook = NULL;
PgLakeAlterTableRenameColumnHookType PgLakeAlterTableRenameColumnHook = NULL;

/*
 * Below is the support matrix for ALTER TABLE commands, which pg_lake
 * should specially handle, for writable and iceberg tables. We basically
 * disabled some of them just because we have not handled them yet.
 *
 * Some of the commands are allowed for both iceberg and writable tables.
 * We are keeping them in the list to make sure we are aware of them.
 *
 * Some of the commands are disallowed for Iceberg tables since they cause
 * metadata change for Iceberg tables.
 *
 * Some of the commands are disallowed for writable tables since they break
 * reading existing parquet files, e.g. (new column, dropped column, changed
 * column type or name).
 * All other commands are already disallowed by PostgreSQL for foreign tables.
 * PG throws its own error before we catch them in our post hooks.
 */
static const PgLakeDDL PgLakeDDLs[] = {
	/* allowed for iceberg and not allowed for writable tables */
	ALTER_TABLE_DDL(AT_AddColumn, DisallowedAddColumnWithUnsupportedConstraints, Disallowed),
	ALTER_TABLE_DDL(AT_DropColumn, Allowed, Disallowed),
	ALTER_TABLE_DDL(AT_DropNotNull, Allowed, Disallowed),
	RENAME_TABLE_DDL(OBJECT_COLUMN, Allowed, Disallowed),

	/* not allowed for both */
	RENAME_TABLE_DDL(OBJECT_ATTRIBUTE, Disallowed, Disallowed),
	ALTER_TABLE_DDL(AT_SetNotNull, Disallowed, Disallowed),
#if PG_VERSION_NUM >= 170000
	ALTER_TABLE_DDL(AT_SetExpression, Disallowed, Disallowed),
#endif
	ALTER_TABLE_DDL(AT_AlterColumnType, Disallowed, Disallowed),

	/* allowed for writable tables, not allowed for iceberg tables */
	ALTER_TABLE_DDL(AT_AddIdentity, Disallowed, Allowed),
	ALTER_TABLE_DDL(AT_SetIdentity, Disallowed, Allowed),
	ALTER_TABLE_DDL(AT_DropIdentity, Disallowed, Allowed),

	/* allowed for both iceberg and writable tables */
	ALTER_TABLE_DDL(AT_ColumnDefault, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_DropExpression, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_SetStatistics, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_GenericOptions, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_SetStorage, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_ChangeOwner, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_AddConstraint, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_ValidateConstraint, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_DropConstraint, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_EnableTrig, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_EnableAlwaysTrig, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_EnableReplicaTrig, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_DisableTrig, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_EnableTrigAll, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_DisableTrigAll, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_EnableTrigUser, Allowed, Allowed),
	ALTER_TABLE_DDL(AT_DisableTrigUser, Allowed, Allowed),
	ALTER_SCHEMA_DDL(OBJECT_TABLE, DisallowedForWritableRestSetSchema, Allowed),
	ALTER_SCHEMA_DDL(OBJECT_FOREIGN_TABLE, DisallowedForWritableRestSetSchema, Allowed),
	RENAME_TABLE_DDL(OBJECT_TABLE, DisallowedForWritableRestRenameTable, Allowed),
	RENAME_TABLE_DDL(OBJECT_FOREIGN_TABLE, DisallowedForWritableRestRenameTable, Allowed),
};

#define N_PG_LAKE_DDLS (sizeof(PgLakeDDLs) / sizeof(PgLakeDDLs[0]))

static bool RequiresNewIcebergSchema(AlterTableStmt *alterStmt);
static const PgLakeDDL *FindPgLakeDDL(PgLakeDDLTypeInfo ddlTypeInfo);
static bool ShouldPgLakeThrowErrorForDDL(PgLakeTableType tableType,
										 PgLakeDDLTypeInfo ddlTypeInfo,
										 Node *arg);
static void ErrorIfUnsupportedSetAccessMethod(AlterTableStmt *alterStmt);
static void ErrorIfUnsupportedAlterWritablePgLakeTableStmt(AlterTableStmt *alterStmt,
														   PgLakeTableType tableType);
static void ErrorIfUnsupportedRenameWritablePgLakeTableStmt(Oid relationId,
															RenameStmt *renameStmt,
															PgLakeTableType tableType);
static void ErrorIfUnsupportedAlterWritablePgLakeTableSchemaStmt(AlterObjectSchemaStmt *alterSchemaStmt,
																 PgLakeTableType tableType);
static const char *PgLakeUnsupportedAlterTableToString(AlterTableCmd *cmd);
static char *AddColumnToString(AlterTableCmd *cmd);
static bool AlterTableHasSerialPseudoType(AlterTableCmd *cmd);
static bool AlterTableAddColumnHasAnyUnsupportedConstraint(AlterTableCmd *cmd);
static bool AlterTableAddColumnHasMutableDefault(AlterTableCmd *cmd);
static bool IsAlterTableAddColumnDefaultForRestTable(Oid relationId, AlterTableCmd *cmd);
static void ErrorIfUnsupportedTypeAddedForIcebergTables(AlterTableStmt *alterStmt);
static List *CreateDDLOperationsForAlterTable(AlterTableStmt *alterStmt);
static void HandleIcebergOptionsChanges(Oid relationId, AlterTableStmt *alterStmt);
static void HasPartitionByChanged(AlterTableStmt *alterStmt, bool *partitionByAdded, bool *partitionByDropped);
static bool HasPartitionByAdded(AlterTableStmt *alterStmt);
static bool HasPartitionByDropped(AlterTableStmt *alterStmt);
static bool HasOnlyCatalogAlterTableOptions(AlterTableStmt *alterStmt);
static void ErrorIfUnsupportedTableOptionChange(AlterTableStmt *alterStmt, List *allowedOptions);
static void ErrorIfAnyRestCatalogTablesInSchema(const char *schemaName);

/*
 * ProcessAlterTable is used in cases where we want to preempt the error
 * message thrown by regular ProcessUtility.
 */
bool
ProcessAlterTable(ProcessUtilityParams * processUtilityParams, void *arg)
{
	PlannedStmt *plannedStmt = processUtilityParams->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, AlterTableStmt))
		return false;

	AlterTableStmt *alterStmt = (AlterTableStmt *) plannedStmt->utilityStmt;

	if (alterStmt->objtype != OBJECT_TABLE &&
		alterStmt->objtype != OBJECT_FOREIGN_TABLE)
	{
		return false;
	}

	Oid			relationId = AlterTableLookupRelation(alterStmt, NoLock);

	if (!IsWritablePgLakeTable(relationId) && !IsIcebergTable(relationId))
	{
		/* for non-pg_lake tables, error when using SET ACCESS METHOD iceberg */
		ErrorIfUnsupportedSetAccessMethod(alterStmt);
		return false;
	}

	/*
	 * For read-only rest catalog iceberg tables, we only allow changing
	 * catalog_table_name option. All other operations or any other DDLs are
	 * disallowed.
	 */
	if (HasOnlyCatalogAlterTableOptions(alterStmt))
	{
		IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

		if (icebergCatalogType == REST_CATALOG_READ_ONLY)
		{
			/*
			 * We currently only allow changing catalog_table_name option for
			 * read-only rest catalog iceberg tables.
			 */
			List	   *allowedOptions = list_make1("catalog_table_name");

			ErrorIfUnsupportedTableOptionChange(alterStmt, allowedOptions);

			return false;
		}
		else if (icebergCatalogType == OBJECT_STORE_READ_ONLY)
		{
			/*
			 * We currently only allow changing catalog_table_name option for
			 * read-only rest catalog iceberg tables.
			 */
			List	   *allowedOptions = list_make3("catalog_table_name", "catalog_namespace", "catalog_name");

			ErrorIfUnsupportedTableOptionChange(alterStmt, allowedOptions);

			return false;
		}

	}

	ErrorIfReadOnlyIcebergTable(relationId);

	ErrorIfUnsupportedTypeAddedForIcebergTables(alterStmt);

	/* check whether we are accepting writes for this table */
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	ErrorIfUnsupportedAlterWritablePgLakeTableStmt(alterStmt, tableType);

	/* if there is an OPTIONS (...) subcommand, perform additional validation */
	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
		HandleIcebergOptionsChanges(relationId, alterStmt);

	/*
	 * Do the actual DDL using internal ProcessUtility. We use
	 * PgLakeCommonParentProcessUtility, which skips other pg_lake_engine
	 * handlers, since we did not make any modifications to the statement yet,
	 * so PgLakeCommonProcessUtility would recurse back into this function.
	 */
	PgLakeCommonParentProcessUtility(processUtilityParams);

	List	   *schemaDDLOperations = NIL;

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE && RequiresNewIcebergSchema(alterStmt))
	{
		/* schema has changed, update the metadata */
		List	   *ddlOperations = CreateDDLOperationsForAlterTable(alterStmt);

		schemaDDLOperations = list_concat(schemaDDLOperations, ddlOperations);
	}

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE && HasPartitionByAdded(alterStmt))
	{
		List	   *parsedTransforms = ParseIcebergTablePartitionBy(relationId);

		IcebergDDLOperation *partitionDDLOp = palloc0(sizeof(IcebergDDLOperation));

		partitionDDLOp->type = DDL_TABLE_SET_PARTITION_BY;
		partitionDDLOp->parsedTransforms = parsedTransforms;

		schemaDDLOperations = lappend(schemaDDLOperations, partitionDDLOp);
	}
	else if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE && HasPartitionByDropped(alterStmt))
	{
		IcebergDDLOperation *partitionDDLOp = palloc0(sizeof(IcebergDDLOperation));

		partitionDDLOp->type = DDL_TABLE_DROP_PARTITION_BY;

		schemaDDLOperations = lappend(schemaDDLOperations, partitionDDLOp);
	}

	ApplyDDLChanges(relationId, schemaDDLOperations);

	if (PgLakeAlterTableHook)
		PgLakeAlterTableHook(relationId, alterStmt);

	/* called parent ProcessUtility, so we are done */
	return true;
}


/*
* CreateDDLOperationsForAlterTable is used to create column operations
* for an ALTER TABLE command. The operation is used to update the column mappings
* for new columns, and to update the initial_default and write_default values
* for columns with default values.
*
* Note that we handle DDL_COLUMN_DROP separately in DropTableAccessHook as a column
* can be dropped not only with ALTER TABLE DROP COLUMN but also with other commands
* like DROP TYPE/DROP OWNED BY etc. So, DropAccess hooks are more suitable for
* handling column drops.
*/
static List *
CreateDDLOperationsForAlterTable(AlterTableStmt *alterStmt)
{
	List	   *ddlOperations = NIL;

	Oid			relationId = AlterTableLookupRelation(alterStmt, NoLock);

	Relation	rel = table_open(relationId, AccessShareLock);

	TupleDesc	tupleDesc = RelationGetDescr(rel);

	ListCell   *subcommandCell = NULL;

	foreach(subcommandCell, alterStmt->cmds)
	{
		AlterTableCmd *subcommand = (AlterTableCmd *) lfirst(subcommandCell);

		if (subcommand->subtype == AT_AddColumn)
		{
			IcebergDDLOperation *ddlOperation = palloc0(sizeof(IcebergDDLOperation));

			ddlOperation->type = DDL_COLUMN_ADD;

			ddlOperation->columnDefs = list_make1((ColumnDef *) subcommand->def);

			ddlOperations = lappend(ddlOperations, ddlOperation);
		}
		else if (subcommand->subtype == AT_ColumnDefault)
		{
			char	   *columnName = subcommand->name;
			AttrNumber	attrNo = get_attnum(relationId, columnName);

			IcebergDDLOperation *ddlOperation = palloc0(sizeof(IcebergDDLOperation));

			ddlOperation->attrNumber = attrNo;

			DataFileSchemaField *field = GetRegisteredFieldForAttribute(relationId, attrNo);

			/*
			 * we need to fetch new default from postgres since we did not
			 * register it in our catalog yet
			 */
			ddlOperation->writeDefault = GetIcebergJsonSerializedDefaultExpr(tupleDesc, attrNo, field);

			if (ddlOperation->writeDefault != NULL)
			{
				ddlOperation->type = DDL_COLUMN_SET_DEFAULT;
			}
			else
			{
				ddlOperation->type = DDL_COLUMN_DROP_DEFAULT;
			}

			ddlOperations = lappend(ddlOperations, ddlOperation);
		}
		else if (subcommand->subtype == AT_DropNotNull)
		{
			IcebergDDLOperation *ddlOperation = palloc0(sizeof(IcebergDDLOperation));

			ddlOperation->type = DDL_COLUMN_DROP_NOT_NULL;

			ddlOperations = lappend(ddlOperations, ddlOperation);
		}
	}

	table_close(rel, NoLock);

	return ddlOperations;
}


/*
* ErrorIfUnsupportedTypeAddedForIcebergTables checks if the column type being added
* to an iceberg table is supported.
*/
static void
ErrorIfUnsupportedTypeAddedForIcebergTables(AlterTableStmt *alterStmt)
{
	ListCell   *subcommandCell = NULL;

	foreach(subcommandCell, alterStmt->cmds)
	{
		AlterTableCmd *subcommand = (AlterTableCmd *) lfirst(subcommandCell);

		if (subcommand->subtype != AT_AddColumn)
		{
			continue;
		}
		else if (AlterTableHasSerialPseudoType(subcommand))
		{
			continue;
		}


		ColumnDef  *columnDef = (ColumnDef *) subcommand->def;

		int32		typmod = 0;
		Oid			typeOid = InvalidOid;

		typenameTypeIdAndMod(NULL, columnDef->typeName, &typeOid, &typmod);

		ErrorIfTypeUnsupportedForIcebergTables(typeOid, typmod, columnDef->colname);
	}
}


bool
ProcessAlterType(ProcessUtilityParams * processUtilityParams, void *arg)
{
	PlannedStmt *plannedStmt = processUtilityParams->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, AlterTableStmt))
		return false;

	AlterTableStmt *stmt = (AlterTableStmt *) plannedStmt->utilityStmt;

	RangeVar   *relation = stmt->relation;
	Oid			relationId = RangeVarGetRelid(relation, NoLock, true);

	/* if the relation doesn't exists, just leave it to Postgres */
	if (!OidIsValid(relationId))
		return false;

	ListCell   *cell;

	foreach(cell, stmt->cmds)
	{
		AlterTableCmd *cmd = (AlterTableCmd *) lfirst(cell);

		/* we are only interested in ALTER TYPE .. ADD ATTRIBUTE */
		if (cmd->subtype != AT_AddColumn)
			continue;

		List	   *nameList = MakeNameListFromRangeVar(stmt->relation);
		TypeName   *typename = makeTypeNameFromNameList(nameList);
		Oid			typeOid = typenameTypeId(NULL, typename);

		if (CheckIfTypeIsUsedInIcebergTable(typeOid))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot alter type %s because it is used in an iceberg table",
							format_type_be(typeOid))));
		}
	}
	return false;
}


/*
* we do not allow ALTER ENUM statements if the enum type is used
* in an iceberg table.
*/
bool
ProcessEnumStatement(ProcessUtilityParams * processUtilityParams, void *arg)
{
	PlannedStmt *plannedStmt = processUtilityParams->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, AlterEnumStmt))
		return false;

	AlterEnumStmt *stmt = (AlterEnumStmt *) plannedStmt->utilityStmt;
	TypeName   *typename = makeTypeNameFromNameList(stmt->typeName);
	Oid			typeOid = typenameTypeId(NULL, typename);

	if (CheckIfTypeIsUsedInIcebergTable(typeOid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter type %s because it is used in an iceberg table",
						format_type_be(typeOid))));
	}

	return false;
}


/*
* ProcessAlterTypeAttributeRename is to prevent renaming of a an attribute of a composite type
* that is used in an iceberg table.
*/
bool
ProcessAlterTypeAttributeRename(ProcessUtilityParams * processUtilityParams, void *arg)
{
	PlannedStmt *plannedStmt = processUtilityParams->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, RenameStmt))
		return false;

	RenameStmt *renameStmt = (RenameStmt *) plannedStmt->utilityStmt;

	if (renameStmt->renameType != OBJECT_ATTRIBUTE)
	{
		/* not ALTER TABLE RENAME ATTRIBUTE */
		return false;
	}

	List	   *nameList = MakeNameListFromRangeVar(renameStmt->relation);
	TypeName   *typename = makeTypeNameFromNameList(nameList);
	Oid			typeOid = typenameTypeId(NULL, typename);

	if (CheckIfTypeIsUsedInIcebergTable(typeOid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot alter type %s because it is used in an iceberg table",
						format_type_be(typeOid))));
	}

	return false;
}



/*
 * RequiresNewIcebergSchema returns whether the given ALTER TABLE statement
 * may affect Iceberg metadata. Note that not all these commands are supported
 * yet (e.g. AT_AlterColumnType).
 */
static bool
RequiresNewIcebergSchema(AlterTableStmt *alterStmt)
{
	ListCell   *subcommandCell = NULL;

	foreach(subcommandCell, alterStmt->cmds)
	{
		AlterTableCmd *subcommand = (AlterTableCmd *) lfirst(subcommandCell);

		switch (subcommand->subtype)
		{
			case AT_AddColumn:
			case AT_AlterColumnType:
			case AT_ColumnDefault:
			case AT_CookedColumnDefault:
			case AT_DropNotNull:
			case AT_SetNotNull:
#if PG_VERSION_NUM >= 170000
			case AT_SetExpression:
#endif
				return true;
			default:
				continue;
		}
	}

	return false;
}

/*
 * PostProcessRenameWritablePgLakeTable postprocesses ALTER TABLE RENAME
 * statements that renames a writable pg_lake table or one of its columns.
 * It checks if the RENAME statement is supported for pg_lake tables and
 * throws an error if it is not.
 */
void
PostProcessRenameWritablePgLakeTable(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, RenameStmt))
	{
		return;
	}

	RenameStmt *renameStmt = (RenameStmt *) plannedStmt->utilityStmt;

	if (renameStmt->renameType == OBJECT_SCHEMA &&
		IsExtensionCreated(PgLakeIceberg))
	{
		/* we are in post-process, use new name */
		ErrorIfAnyRestCatalogTablesInSchema(renameStmt->newname);
		return;
	}

	if (renameStmt->renameType != OBJECT_COLUMN &&
		renameStmt->renameType != OBJECT_ATTRIBUTE &&
		renameStmt->renameType != OBJECT_TABLE &&
		renameStmt->renameType != OBJECT_FOREIGN_TABLE)
	{
		/* not ALTER TABLE RENAME or ALTER FOREIGN TABLE RENAME */
		return;
	}

	Oid			namespaceId =
		RangeVarGetAndCheckCreationNamespace(renameStmt->relation, NoLock, NULL);

	const char *relationName = renameStmt->relation->relname;

	if (renameStmt->renameType == OBJECT_TABLE ||
		renameStmt->renameType == OBJECT_FOREIGN_TABLE)
	{
		/* Table is renamed. When column is renamed, we use actual table name. */
		relationName = renameStmt->newname;
	}

	Oid			relationId = get_relname_relid(relationName, namespaceId);

	if (!IsWritablePgLakeTable(relationId) && !IsIcebergTable(relationId))
	{
		return;
	}

	ErrorIfReadOnlyIcebergTable(relationId);

	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	ErrorIfUnsupportedRenameWritablePgLakeTableStmt(relationId, renameStmt, tableType);

	bool		pgLakeTable = IsPgLakeIcebergForeignTableById(relationId);

	if (pgLakeTable &&
		!(renameStmt->renameType == OBJECT_TABLE ||
		  renameStmt->renameType == OBJECT_FOREIGN_TABLE))
	{
		/* schema has changed, update the metadata */
		IcebergDDLOperation *ddlOperation = palloc0(sizeof(IcebergDDLOperation));

		/* this is a column that has been renamed */
		ddlOperation->type = DDL_TABLE_RENAME;
		ApplyDDLChanges(relationId, list_make1(ddlOperation));

		if (PgLakeAlterTableRenameColumnHook)
			PgLakeAlterTableRenameColumnHook(relationId, renameStmt);
	}
	else if (pgLakeTable &&
			 (renameStmt->renameType == OBJECT_TABLE ||
			  renameStmt->renameType == OBJECT_FOREIGN_TABLE))
	{
		TriggerCatalogExportIfObjectStoreTable(relationId);
	}
}


/*
 * PostProcessAlterWritablePgLakeTableSchema processes ALTER TABLE SET SCHEMA
 * statements that alters the schema of a writable pg_lake table.
 * It checks if the ALTER TABLE SET SCHEMA statement is supported for pg_lake
 * tables and throws an error if it is not.
 */
void
PostProcessAlterWritablePgLakeTableSchema(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, AlterObjectSchemaStmt))
	{
		return;
	}

	AlterObjectSchemaStmt *alterSchemaStmt = (AlterObjectSchemaStmt *) plannedStmt->utilityStmt;

	if (alterSchemaStmt->objectType != OBJECT_TABLE &&
		alterSchemaStmt->objectType != OBJECT_FOREIGN_TABLE)
	{
		/* not ALTER TABLE or ALTER FOREIGN TABLE */
		return;
	}

	Oid			namespaceId = get_namespace_oid(alterSchemaStmt->newschema, alterSchemaStmt->missing_ok);

	/* if not validOid, let Postgres handle */
	if (!OidIsValid(namespaceId))
	{
		return;
	}

	Oid			relationId = get_relname_relid(alterSchemaStmt->relation->relname, namespaceId);

	if (!IsWritablePgLakeTable(relationId) && !IsIcebergTable(relationId))
	{
		return;
	}

	ErrorIfReadOnlyIcebergTable(relationId);

	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	ErrorIfUnsupportedAlterWritablePgLakeTableSchemaStmt(alterSchemaStmt, tableType);

	TriggerCatalogExportIfObjectStoreTable(relationId);

	return;
}


/*
 * FindPgLakeDDL returns the PgLakeDDL for the given DDL type info.
 */
static const PgLakeDDL *
FindPgLakeDDL(PgLakeDDLTypeInfo ddlTypeInfo)
{
	const		PgLakeDDL *pgLakeDDL = NULL;

	int			pgLakeDDLIndex = 0;

	for (pgLakeDDLIndex = 0; pgLakeDDLIndex < N_PG_LAKE_DDLS; pgLakeDDLIndex++)
	{
		const		PgLakeDDL *currentPgLakeDDL = &PgLakeDDLs[pgLakeDDLIndex];

		if (ddlTypeInfo.ddlType != currentPgLakeDDL->ddlTypeInfo.ddlType)
		{
			continue;
		}

		if (ddlTypeInfo.ddlType == PG_LAKE_DDL_ALTER_TABLE &&
			ddlTypeInfo.ddlInfo.alterTableCmdType == currentPgLakeDDL->ddlTypeInfo.ddlInfo.alterTableCmdType)
		{
			pgLakeDDL = currentPgLakeDDL;
			break;
		}
		else if (ddlTypeInfo.ddlType == PG_LAKE_DDL_RENAME_TABLE &&
				 ddlTypeInfo.ddlInfo.renameObjectType == currentPgLakeDDL->ddlTypeInfo.ddlInfo.renameObjectType)
		{
			pgLakeDDL = currentPgLakeDDL;
			break;
		}
		else if (ddlTypeInfo.ddlType == PG_LAKE_DDL_SET_SCHEMA &&
				 ddlTypeInfo.ddlInfo.alterSchemaObjectType == currentPgLakeDDL->ddlTypeInfo.ddlInfo.alterSchemaObjectType)
		{
			pgLakeDDL = currentPgLakeDDL;
			break;
		}
	}

	return pgLakeDDL;
}


/*
 * ShouldPgLakeThrowErrorForDDL returns whether pg_lake should throw error
 * for the given ALTER TABLE command.
 */
static bool
ShouldPgLakeThrowErrorForDDL(PgLakeTableType tableType,
							 PgLakeDDLTypeInfo ddlTypeInfo,
							 Node *arg)
{
	const		PgLakeDDL *pgLakeDDL = FindPgLakeDDL(ddlTypeInfo);

	if (pgLakeDDL == NULL)
	{
		/* we do not hook for this DDL */
		return false;
	}

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE && !pgLakeDDL->allowedForIceberg(arg, ddlTypeInfo.relationId))
	{
		return true;
	}

	if (tableType == PG_LAKE_TABLE_TYPE && !pgLakeDDL->allowedForWritableLake(arg, ddlTypeInfo.relationId))
	{
		return true;
	}

	return false;
}


/*
 * ErrorIfUnsupportedAlterWritablePgLakeTableStmt ensures that the ALTER TABLE
 * statement is supported for pg_lake tables.
 */
static void
ErrorIfUnsupportedAlterWritablePgLakeTableStmt(AlterTableStmt *alterStmt,
											   PgLakeTableType tableType)
{
	List	   *cmds = alterStmt->cmds;

	ListCell   *cmdCell = NULL;

	foreach(cmdCell, cmds)
	{
		AlterTableCmd *cmd = lfirst(cmdCell);

		PgLakeDDLTypeInfo ddlTypeInfo = {
			.relationId = AlterTableLookupRelation(alterStmt, NoLock),
			.ddlType = PG_LAKE_DDL_ALTER_TABLE,
			.ddlInfo.alterTableCmdType = cmd->subtype
		};

		if (ShouldPgLakeThrowErrorForDDL(tableType, ddlTypeInfo, (Node *) cmd))
		{
			const char *tableTypeStr = PgLakeTableTypeToName(tableType);

			const char *cmdTypeStr = PgLakeUnsupportedAlterTableToString(cmd);

			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ALTER TABLE %s command not supported for "
							"%s tables", cmdTypeStr, tableTypeStr)));
		}
	}

	return;
}


/*
 * ErrorIfUnsupportedRenameWritablePgLakeTableStmt ensures that the RENAME TABLE
 * statement is supported for pg_lake tables.
 */
static void
ErrorIfUnsupportedRenameWritablePgLakeTableStmt(Oid relationId,
												RenameStmt *renameStmt,
												PgLakeTableType tableType)
{
	const char *tableTypeStr = PgLakeTableTypeToName(tableType);

	PgLakeDDLTypeInfo ddlTypeInfo = {
		.ddlType = PG_LAKE_DDL_RENAME_TABLE,
		.ddlInfo.renameObjectType = renameStmt->renameType
	};

	if (ShouldPgLakeThrowErrorForDDL(tableType, ddlTypeInfo, (Node *) renameStmt))
	{
		if (renameStmt->renameType == OBJECT_TABLE ||
			renameStmt->renameType == OBJECT_FOREIGN_TABLE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ALTER TABLE RENAME command not supported for "
							"%s tables", tableTypeStr)));
		}
		else if (renameStmt->renameType == OBJECT_COLUMN ||
				 renameStmt->renameType == OBJECT_ATTRIBUTE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("ALTER TABLE RENAME COLUMN command not supported for "
							"%s tables", tableTypeStr)));
		}
		else
		{
			/* we do not hook for other object types */
			pg_unreachable();
		}
	}

	if (renameStmt->renameType == OBJECT_COLUMN)
	{
		/*
		 * we are in the post-process, so use the newname for fetching the
		 * attr no
		 */
		AttrNumber	attrNo = get_attnum(relationId, renameStmt->newname);

		if (attrNo == InvalidAttrNumber)
		{
			/* sanity check, not possible but still let's be defensive */
			ereport(ERROR,
					(errcode(ERRCODE_UNDEFINED_COLUMN),
					 errmsg("column \"%s\" does not exist in table \"%s\"",
							renameStmt->newname, renameStmt->relation->relname)));
		}

		StringInfo	messageDetail = makeStringInfo();

		appendStringInfo(messageDetail, "rename column \"%s\"", renameStmt->subname);

		ErrorIfColumnEverUsedInIcebergPartitionSpec(relationId, attrNo, messageDetail->data);

	}

	return;
}


/*
 * ErrorIfUnsupportedAlterWritablePgLakeTableSchemaStmt ensures that the ALTER TABLE
 * SET SCHEMA statement is supported for pg_lake tables.
 */
static void
ErrorIfUnsupportedAlterWritablePgLakeTableSchemaStmt(AlterObjectSchemaStmt *alterSchemaStmt,
													 PgLakeTableType tableType)
{
	PgLakeDDLTypeInfo ddlTypeInfo = {
		.ddlType = PG_LAKE_DDL_SET_SCHEMA,
		.ddlInfo.alterSchemaObjectType = alterSchemaStmt->objectType
	};

	if (ShouldPgLakeThrowErrorForDDL(tableType, ddlTypeInfo, (Node *) alterSchemaStmt))
	{
		const char *tableTypeStr = PgLakeTableTypeToName(tableType);

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ALTER TABLE SET SCHEMA command not supported for "
						"%s tables", tableTypeStr)));
	}

	return;
}


/*
 * PgLakeUnsupportedAlterTableToString returns the string representation of the AlterTableType.
 * We only need to support a subset of AlterTableType for which we throw error.
 */
static const char *
PgLakeUnsupportedAlterTableToString(AlterTableCmd *cmd)
{
	AlterTableType cmdType = cmd->subtype;

	switch (cmdType)
	{
		case AT_AddColumn:
			return AddColumnToString(cmd);
		case AT_AddColumnToView:
			return "ADD COLUMN";
		case AT_ColumnDefault:
		case AT_CookedColumnDefault:
			return "ALTER COLUMN ... SET DEFAULT";
		case AT_DropNotNull:
			return "ALTER COLUMN ... DROP NOT NULL";
		case AT_SetNotNull:
			return "ALTER COLUMN ... SET NOT NULL";
#if PG_VERSION_NUM >= 170000
		case AT_SetExpression:
			return "ALTER COLUMN ... SET EXPRESSION";
#endif
		case AT_DropColumn:
			return "DROP COLUMN";
		case AT_AlterColumnType:
			return "ALTER COLUMN ... SET DATA TYPE";
		case AT_AddIdentity:
			return "ALTER COLUMN ... ADD IDENTITY";
		case AT_SetIdentity:
			return "ALTER COLUMN ... SET";
		case AT_DropIdentity:
			return "ALTER COLUMN ... DROP IDENTITY";
		default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unexpected ALTER TABLE command type %d", cmdType)));
				return NULL;
			}
	}

	return NULL;
}


/*
 * AddColumnToString returns the string representation of the ALTER TABLE .. ADD COLUMN
 * command.
 */
static char *
AddColumnToString(AlterTableCmd *cmd)
{
	if (AlterTableAddColumnHasAnyUnsupportedConstraint(cmd))
	{
		return "ADD COLUMN with constraints";
	}

	if (AlterTableAddColumnHasMutableDefault(cmd))
	{
		return "ADD COLUMN with default expression";
	}

	if (AlterTableHasSerialPseudoType(cmd))
	{
		return "ADD COLUMN with SERIAL data types";
	}

	return "ADD COLUMN";
}


/*
 * Allowed returns true if the DDL is allowed.
 */
static bool
Allowed(Node *arg, Oid relationId)
{
	return true;
}

/*
 * Disallowed returns false if the DDL is disallowed.
 */
static bool
Disallowed(Node *arg, Oid relationId)
{
	return false;
}


/*
* We have not yet implemented table renames in REST catalog.
*/
static bool
DisallowedForWritableRestRenameTable(Node *arg, Oid relationIdIn)
{
	RenameStmt *renameStmt = (RenameStmt *) arg;

	/* only disallow RENAME TABLE/FOREIGN TABLE for writable rest catalog */
	if (renameStmt->renameType == OBJECT_TABLE ||
		renameStmt->renameType == OBJECT_FOREIGN_TABLE)
	{
		Oid			namespaceId =
			RangeVarGetAndCheckCreationNamespace(renameStmt->relation, NoLock, NULL);

		/* we are in the post-process, so should use new name */
		const char *relationName = renameStmt->newname;

		/* TODO: relationIdIn is not valid for rename statements */
		Oid			relationId = get_relname_relid(relationName, namespaceId);

		IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

		return (icebergCatalogType != REST_CATALOG_READ_WRITE);
	}

	return false;
}

/*
* We have not yet implemented schema changes in REST catalog.
*/
static bool
DisallowedForWritableRestSetSchema(Node *arg, Oid relationIdIn)
{
	AlterObjectSchemaStmt *alterSchemaStmt = (AlterObjectSchemaStmt *) arg;

	Oid			namespaceId = get_namespace_oid(alterSchemaStmt->newschema, alterSchemaStmt->missing_ok);

	/* if not validOid, let Postgres handle */
	if (!OidIsValid(namespaceId))
	{
		return false;
	}

	/* TODO: relationIdIn is not valid for set schema statements */
	Oid			relationId = get_relname_relid(alterSchemaStmt->relation->relname, namespaceId);
	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	return (icebergCatalogType != REST_CATALOG_READ_WRITE);
}


/*
 * ErrorIfUnsupportedSetAccessMethod checks ALTER TABLE on non-pg_lake
 * tables for possible issues involving Iceberg.
 */
static void
ErrorIfUnsupportedSetAccessMethod(AlterTableStmt *alterStmt)
{
	List	   *cmds = alterStmt->cmds;

	ListCell   *cmdCell = NULL;

	foreach(cmdCell, cmds)
	{
		AlterTableCmd *cmd = lfirst(cmdCell);

		/*
		 * We currently do not support SET ACCESS METHOD pg_lake_iceberg.
		 */
		if (cmd->subtype != AT_SetAccessMethod)
			continue;

		char	   *accessMethodName = cmd->name;

		if (accessMethodName == NULL)
			accessMethodName = default_table_access_method;

		if (IsPgLakeIcebergAccessMethod(accessMethodName))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("setting the access method to %s is currently "
								   "not supported",
								   accessMethodName),
							errhint("You can use CREATE TABLE new_table USING %s "
									"AS SELECT * FROM %s",
									accessMethodName,
									alterStmt->relation->relname)));
		}
	}
}

/*
 * DisallowedAddColumnWithUnsupportedConstraints returns false when ALTER TABLE .. ADD COLUMN
 * has any unsupported constraint. The unsupported constraints are: [NOT] NULL, CHECK,
 * PRIMARY KEY, UNIQUE, REFERENCES, EXCLUDE, and GENERATED. Some of these constraints are already
 * not supported for foreign tables.
 */
static bool
DisallowedAddColumnWithUnsupportedConstraints(Node *arg, Oid relationId)
{
	AlterTableCmd *cmd = (AlterTableCmd *) arg;

	/* first, assert the subtype to prevent programming errors */
	Assert(cmd->subtype == AT_AddColumn);

	if (AlterTableAddColumnHasAnyUnsupportedConstraint(cmd))
	{
		return false;
	}

	if (IsAlterTableAddColumnDefaultForRestTable(relationId, cmd))
	{
		return false;
	}

	if (AlterTableAddColumnHasMutableDefault(cmd))
	{
		return false;
	}

	if (AlterTableHasSerialPseudoType(cmd))
	{
		return false;
	}

	return true;
}


/*
* IsAlterTableAddColumnDefaultForRestTable returns true if ALTER TABLE .. ADD COLUMN
* has a DEFAULT constraint and the table is a REST catalog iceberg table.
*/
static bool
IsAlterTableAddColumnDefaultForRestTable(Oid relationId, AlterTableCmd *cmd)
{
	Assert(cmd->subtype == AT_AddColumn);

	ColumnDef  *column = castNode(ColumnDef, cmd->def);

	List	   *constraints = column->constraints;

	ListCell   *constraintCell = NULL;

	foreach(constraintCell, constraints)
	{
		Constraint *constraint = lfirst(constraintCell);

		if (constraint->contype != CONSTR_DEFAULT)
		{
			continue;
		}

		IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

		if (icebergCatalogType == REST_CATALOG_READ_WRITE)
		{
			return true;
		}
	}

	return false;
}

/*
 * AlterTableAddColumnHasMutableDefault returns true if ALTER TABLE .. ADD COLUMN
 * has a mutable default value. e.g. ALTER TABLE .. ADD COLUMN .. DEFAULT now().
 */
static bool
AlterTableAddColumnHasMutableDefault(AlterTableCmd *cmd)
{
	Assert(cmd->subtype == AT_AddColumn);

	ColumnDef  *column = castNode(ColumnDef, cmd->def);

	List	   *constraints = column->constraints;

	ListCell   *constraintCell = NULL;

	foreach(constraintCell, constraints)
	{
		Constraint *constraint = lfirst(constraintCell);

		if (constraint->contype != CONSTR_DEFAULT)
		{
			continue;
		}

		int32		typeMod = 0;
		Oid			typeOid = InvalidOid;

		typenameTypeIdAndMod(NULL, column->typeName, &typeOid, &typeMod);

		/* create a dummy parse state */
		ParseState *pstate = make_parsestate(NULL);

		Node	   *expr = cookDefault(pstate, constraint->raw_expr, typeOid, typeMod, column->colname, column->generated);

		if (contain_mutable_functions(expr))
		{
			return true;
		}
	}

	return false;
}

/*
 * AlterTableAddColumnHasAnyUnsupportedConstraint returns true if ALTER TABLE .. ADD COLUMN
 * has any unsupported constraint, such as [NOT] NULL, CHECK, PRIMARY KEY, UNIQUE, REFERENCES,
 * EXCLUDE, or GENERATED.
 */
static bool
AlterTableAddColumnHasAnyUnsupportedConstraint(AlterTableCmd *cmd)
{
	/* first, assert the subtype to prevent programming errors */
	Assert(cmd->subtype == AT_AddColumn);
	ColumnDef  *column = castNode(ColumnDef, cmd->def);

	ListCell   *constraintCell = NULL;

	foreach(constraintCell, column->constraints)
	{
		Constraint *constraint = lfirst(constraintCell);

		/* we support only DEFAULT */
		if (constraint->contype != CONSTR_DEFAULT)
		{
			return true;
		}
	}

	return false;
}


/*
 * AlterTableHasSerialPseudoType returns true if ALTER TABLE .. ADD COLUMN
 * has SERIAL pseudo-types.
 */
static bool
AlterTableHasSerialPseudoType(AlterTableCmd *cmd)
{
	ColumnDef  *column = castNode(ColumnDef, cmd->def);

	return ColumnDefIsPseudoSerial(column);
}


/*
 * HandleIcebergOptionsChanges handles options changes for foreign
 * tables, which require additional checks or steps that the regular
 * validator cannot perform.
 */
static void
HandleIcebergOptionsChanges(Oid relationId, AlterTableStmt *alterStmt)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	bool		currentlyHasRowIds = GetBoolOption(options, "row_ids", false);

	ListCell   *subcommandCell = NULL;

	foreach(subcommandCell, alterStmt->cmds)
	{
		AlterTableCmd *subcommand = (AlterTableCmd *) lfirst(subcommandCell);

		if (subcommand->subtype != AT_GenericOptions)
			continue;

		List	   *newOptions = (List *) subcommand->def;
		ListCell   *optionCell = NULL;

		bool		newHasRowIds = currentlyHasRowIds;
		bool		foundRowIdsOption = false;

		foreach(optionCell, newOptions)
		{
			DefElem    *newOption = lfirst(optionCell);

			if (strcmp(newOption->defname, "catalog") == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("Changing option \"%s\" is not supported",
								newOption->defname)));

			/* we currently only care about row_ids */
			if (strcmp(newOption->defname, "row_ids") != 0)
				continue;

			if (foundRowIdsOption)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("option \"row_ids\" provided more than once")));

			switch (newOption->defaction)
			{
				case DEFELEM_DROP:
				case DEFELEM_UNSPEC:
					newHasRowIds = false;
					foundRowIdsOption = true;
					break;

				case DEFELEM_SET:
				case DEFELEM_ADD:
					newHasRowIds = defGetBoolean(newOption);
					foundRowIdsOption = true;
					break;

				default:
					elog(ERROR, "unrecognized action %d on option \"%s\"",
						 (int) newOption->defaction, newOption->defname);
					break;
			}
		}

		/* enabling row_ids for the first time */
		if (newHasRowIds && !currentlyHasRowIds)
			EnableRowIdsOnTable(relationId);

		/* trying to disable row_ids */
		else if (!newHasRowIds && currentlyHasRowIds)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("disabling row_ids is currently not supported")));
		}
	}
}


/*
 * HasPartitionByChanged checks if partition_by option is added or dropped.
 */
static void
HasPartitionByChanged(AlterTableStmt *alterStmt, bool *partitionByAdded, bool *partitionByDropped)
{
	ListCell   *subcommandCell = NULL;

	foreach(subcommandCell, alterStmt->cmds)
	{
		AlterTableCmd *subcommand = (AlterTableCmd *) lfirst(subcommandCell);

		if (subcommand->subtype != AT_GenericOptions)
			continue;

		List	   *newOptions = (List *) subcommand->def;
		ListCell   *optionCell = NULL;

		foreach(optionCell, newOptions)
		{
			DefElem    *newOption = lfirst(optionCell);

			if (strcmp(newOption->defname, "partition_by") == 0)
			{
				*partitionByAdded = (newOption->defaction == DEFELEM_SET || newOption->defaction == DEFELEM_ADD);
				*partitionByDropped = (newOption->defaction == DEFELEM_DROP);
				return;
			}
		}
	}
}

/*
 * HasOnlyCatalogAlterTableOptions checks if only catalog_table_name option is changed.
 */
static bool
HasOnlyCatalogAlterTableOptions(AlterTableStmt *alterStmt)
{
	ListCell   *subcommandCell = NULL;

	foreach(subcommandCell, alterStmt->cmds)
	{
		AlterTableCmd *subcommand = (AlterTableCmd *) lfirst(subcommandCell);

		if (subcommand->subtype != AT_GenericOptions)
			return false;
	}

	return true;
}


/*
 * ErrorIfUnsupportedTableOptionChange checks if only catalog_table_name option is changed.
 */
static void
ErrorIfUnsupportedTableOptionChange(AlterTableStmt *alterStmt, List *allowedOptions)
{
	StringInfo	allowedOptionsStr = makeStringInfo();
	ListCell   *subcommandCell = NULL;

	foreach(subcommandCell, alterStmt->cmds)
	{
		AlterTableCmd *subcommand = (AlterTableCmd *) lfirst(subcommandCell);

		Assert(subcommand->subtype == AT_GenericOptions);

		List	   *newOptions = (List *) subcommand->def;
		ListCell   *optionCell = NULL;
		bool		allowedOptionFound = false;

		foreach(optionCell, newOptions)
		{
			DefElem    *newOption = lfirst(optionCell);
			ListCell   *allowedOptionCell = NULL;

			foreach(allowedOptionCell, allowedOptions)
			{
				char	   *allowedOption = (char *) lfirst(allowedOptionCell);

				if (strcmp(newOption->defname, allowedOption) == 0)
				{
					allowedOptionFound = true;
					break;
				}
				appendStringInfo(allowedOptionsStr, "%s%s",
								 allowedOptionsStr->len > 0 ? ", " : "",
								 allowedOption);
			}
		}

		if (!allowedOptionFound)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("The following table options can be changed: %s",
							allowedOptionsStr->data)));
		}
	}
}


/*
 * HasPartitionByAdded checks if new partition_by option is set or added.
 */
static bool
HasPartitionByAdded(AlterTableStmt *alterStmt)
{
	bool		partitionByAdded = false;
	bool		partitionByDropped = false;

	HasPartitionByChanged(alterStmt, &partitionByAdded, &partitionByDropped);

	return partitionByAdded;
}


/*
 * HasPartitionByDropped checks if partition_by option is dropped.
 */
static bool
HasPartitionByDropped(AlterTableStmt *alterStmt)
{
	bool		partitionByAdded = false;
	bool		partitionByDropped = false;

	HasPartitionByChanged(alterStmt, &partitionByAdded, &partitionByDropped);

	return partitionByDropped;
}


/*
* ErrorIfAnyRestCatalogTablesInSchema throws an error if there are any
* iceberg tables with REST catalog in the specified schema.
*/
static void
ErrorIfAnyRestCatalogTablesInSchema(const char *schemaName)
{
	Oid			namespaceId = get_namespace_oid(schemaName, false);

	Relation	classRel = table_open(RelationRelationId, AccessShareLock);
	HeapTuple	tuple;

	ScanKeyData key[1];

	ScanKeyInit(&key[0],
				Anum_pg_class_relnamespace,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(namespaceId));

	/* get all the relations present in the specified schema */
	TableScanDesc scan = table_beginscan_catalog(classRel, 1, key);

	while ((tuple = heap_getnext(scan, ForwardScanDirection)) != NULL)
	{
		Form_pg_class relForm = (Form_pg_class) GETSTRUCT(tuple);
		Oid			relid = relForm->oid;

		IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relid);

		if (icebergCatalogType == REST_CATALOG_READ_WRITE)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot rename schema \"%s\" because it contains "
							"an iceberg table with rest catalog \"%s\"", schemaName, get_rel_name(relid))));
		}
	}

	table_endscan(scan);
	table_close(classRel, AccessShareLock);
}
