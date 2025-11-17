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

#include "access/table.h"
#include "access/tableam.h"
#include "access/relation.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "common/string.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "nodes/nodes.h"
#include "nodes/value.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"
#include "utils/typcache.h"
#include "utils/inval.h"

#include "pg_lake/access_method/access_method.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/ddl/alter_table.h"
#include "pg_lake/ddl/ddl_changes.h"
#include "pg_lake/ddl/create_table.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/describe/describe.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/fdw/row_ids.h"
#include "pg_lake/fdw/utils.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/util/numeric.h"
#include "pg_lake/util/url_encode.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/region.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/planner/dbt.h"
#include "pg_lake/query/execute.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"


/* reserved column hook */
PgLakeIsReservedColumnNameHookType PgLakeIsReservedColumnNameHook = NULL;


static bool IsCreateLakeTable(CreateForeignTableStmt *createStmt);
static void AddLakeTableColumnDefinitions(CreateForeignTableStmt *createStmt);
static bool IsForeignTableStmtWithUnboundedNumericColumns(CreateForeignTableStmt *createStmt);
static void SetDefaultPrecisionAndScaleForUnboundedNumericColumns(List *columnDefList);
static bool IsJsonOrCSVBackedTable(PgLakeTableType tableType, List *options);
static void ErrorIfUnsupportedColumnTypeForJsonOrCSVTables(List *columnDefList);
static void ErrorIfUsingGeometryWithoutSpatialAnalytics(List *columnDefList);
static void ErrorIfUnsupportedLakeTable(CreateForeignTableStmt *createStmt);
static void ErrorIfWritableTableWithReservedColumnName(List *columnDefList, PgLakeTableType tableType);
static void ErrorIfInvalidFilenameColumn(List *columnDefList);
static bool IsConflictingColumnNameForReadParquet(const char *columnName);
static CreateForeignTableStmt *GetCreateIcebergForeignTableStmtFromCreateStmt(CreateStmt *createStmt);
static void EnsureCreateIcebergTableColumnOptions(CreateStmt *createStmt);
static void EnsureCreateIcebergTableSupported(CreateStmt *createStmt);
static List *ExpandTableElements(List *tableElements);
static List *ExpandTableLikeClause(TableLikeClause *table_like_clause);

static bool ProcessCreateLakeTable(ProcessUtilityParams * params);
static bool ProcessCreateIcebergTableFromForeignTableStmt(ProcessUtilityParams * params);
static bool ProcessCreateIcebergTableFromCreateStmt(ProcessUtilityParams * params);
static void ErrorIfNotInManagedStorageRegion(char *location);
static void ErrorIfTypeUnsupportedForIcebergTablesInternal(Oid typeOid, int32 typmod, int level, char *columnName);
static void ErrorIfLocationIsNotEmpty(const char *location);
static void EnsureSupportedIcebergTableColumnDefinitions(List *columnDefList);
#if PG_VERSION_NUM >= 180000
static void ErrorIfTableContainsVirtualColumns(List *columnDefList);
#endif
static void ErrorIfTableContainsUnsupportedTypes(List *columnDefList);
static char *SetIcebergTableLocationOptionFromDefaultPrefix(Oid relationId,
															const char *defaultLocationPrefix,
															char *databaseName,
															char *schemaName, char *tableName);

/*
* CreatePgLakeTableCheckUnsupportedFeaturesPostProcess is a utility statement handler
* for checking unsupported features in CREATE FOREIGN TABLE statements that have to be called
* after the table has been created.
*
* We currently check for unsupported column types in CSV backed tables. It is much simpler
* to check for these features after the table has been created, such that Postgres has
* already done the heavy lifting of parsing the column definitions (e.g., bigserial is already
* converted to int8, etc.)
*/
void
CreatePgLakeTableCheckUnsupportedFeaturesPostProcess(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, CreateForeignTableStmt))
	{
		/* not a foreign table */
		return;
	}

	CreateForeignTableStmt *createStmt =
		(CreateForeignTableStmt *) plannedStmt->utilityStmt;

	if (!IsCreateLakeTable(createStmt))
	{
		/* not a lake table */
		return;
	}

	PgLakeTableType tableType =
		GetPgLakeTableTypeViaServerName(createStmt->servername);
	List	   *options = createStmt->options;

	/* Parquet and JSON/CSV have different rules */
	if (IsJsonOrCSVBackedTable(tableType, options))
	{
		ErrorIfUnsupportedColumnTypeForJsonOrCSVTables(createStmt->base.tableElts);
	}

	/* cannot use geometry without pg_lake_spatial */
	ErrorIfUsingGeometryWithoutSpatialAnalytics(createStmt->base.tableElts);
}


/*
* IsJsonOrCSVBackedTable returns whether the given table is backed by a JSON
* or CSV file. It supports both the regular pg_lake tables
* and the writable pg_lake tables.
*/
static bool
IsJsonOrCSVBackedTable(PgLakeTableType tableType, List *options)
{
	DefElem    *pathOption = GetOption(options, "path");
	char	   *path = NULL;

	if (pathOption != NULL)
	{
		path = defGetString(pathOption);
	}

	CopyDataFormat format = DATA_FORMAT_INVALID;
	CopyDataCompression compression = DATA_COMPRESSION_INVALID;

	FindDataFormatAndCompression(tableType, path, options, &format, &compression);

	return (format == DATA_FORMAT_CSV || format == DATA_FORMAT_JSON);

}


/*
* ErrorIfUnsupportedColumnTypeForJsonOrCSVTables checks whether the given column
* definitions are supported for JSON/CSV backed pg_lake tables.
*/
static void
ErrorIfUnsupportedColumnTypeForJsonOrCSVTables(List *columnDefList)
{
	ListCell   *columnDefCell;

	List	   *restrictedColumnDefList =
		GetRestrictedColumnDefList(columnDefList);

	foreach(columnDefCell, restrictedColumnDefList)
	{

		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);
		int32		typmod = 0;
		Oid			typeOid = InvalidOid;

		typenameTypeIdAndMod(NULL, columnDef->typeName, &typeOid, &typmod);

		/*
		 * We prevent arrays, bytea, and structs because we perform pushdown
		 * operations on them. This breaks in CSV/JSON as we can't yet parse
		 * PostgreSQL array/composite/bytea syntax from DuckDB. Other types
		 * are either not pushed down or are treated as text.
		 */
		if (type_is_array(typeOid))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("array types are not "
								   "supported for JSON/CSV backed pg_lake tables")));

		if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("composite types are not "
								   "supported for JSON/CSV backed pg_lake tables")));

		if (typeOid == BYTEAOID)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("bytea type is not "
								   "supported for JSON/CSV backed pg_lake tables")));
	}
}


/*
* GetRestrictedColumnDefList returns a list of column definitions that are not
* pseudo-serial columns, LIKE or Constraint clauses.
*
* These are the column definitions that shows up in pre-utility hooks.
* However, due to a bug in other extension (e.g., Citus), we might see
* these pseudo-serial columns in the column definitions. See #863 for the
* details.
*
* So, we filter out these pseudo-serial columns, LIKE and Constraint clauses
* to get the actual column definitions.
*/
List *
GetRestrictedColumnDefList(List *columnDefList)
{
	List	   *restrictedColumnDefList = NIL;
	ListCell   *columnDefCell;

	foreach(columnDefCell, columnDefList)
	{
		if (!IsA(lfirst(columnDefCell), ColumnDef))
		{
			/* could be LIKE clause or constraint clause */
			continue;
		}

		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		/*
		 * might be null for partitions when specified constraint for parent
		 * column. e.g. CREATE TABLE child_table PARTITION OF parent_table (a
		 * unique) FOR VALUES FROM (0) TO (10);
		 */
		if (columnDef->typeName == NULL)
		{
			continue;
		}

		if (ColumnDefIsPseudoSerial(columnDef))
		{
			/*
			 * serial etc. is supported for iceberg tables, but
			 * typenameTypeIdAndMod() cannot resolve the type for these
			 * pseudo-types. We skip these.
			 */
			continue;
		}

		restrictedColumnDefList = lappend(restrictedColumnDefList, columnDef);
	}

	return restrictedColumnDefList;
}


/*
 * ErrorIfUsingGeometryWithoutSpatialAnalytics throws an error if there is
 * a geometry column, but pg_lake_spatial does not exist.
 *
 * We rely on pg_lake_spatial to push down certain PostGIS functions.
 * Users will get errors for queries that involve those function at some point,
 * perhaps in future releases, so we prefer to error out aggressively at
 * CREATE TABLE time.
 */
static void
ErrorIfUsingGeometryWithoutSpatialAnalytics(List *columnDefList)
{
	ListCell   *columnDefCell;

	List	   *restrictedColumnDefList =
		GetRestrictedColumnDefList(columnDefList);

	foreach(columnDefCell, restrictedColumnDefList)
	{
		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);
		int32		typmod = 0;
		Oid			typeOid = InvalidOid;

		typenameTypeIdAndMod(NULL, columnDef->typeName, &typeOid, &typmod);

		if (IsGeometryTypeId(typeOid))
			ErrorIfPgLakeSpatialNotEnabled();
	}
}



/*
* ErrorUnsupportedCreatePgLakeTableHandler is a utility statement handler for handling
* CREATE FOREIGN TABLE statements that are pg_lake tables.
*
* We check for unsupported features in the table definition, such as unsupported URLs or unsupported
* combinations such as writable tables without column definitions.
*/
bool
ErrorUnsupportedCreatePgLakeTableHandler(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, CreateForeignTableStmt))
	{
		/* not a foreign table */
		return false;
	}

	CreateForeignTableStmt *createStmt =
		(CreateForeignTableStmt *) plannedStmt->utilityStmt;

	if (!IsCreateLakeTable(createStmt))
	{
		/* not a lake table */
		return false;
	}

	ErrorIfUnsupportedLakeTable(createStmt);

	return false;
}


/*
* ErrorIfUnsupportedLakeTable is a helper function for checking unsupported features
* in CREATE FOREIGN TABLE statements that are pg_lake tables.
*/
static void
ErrorIfUnsupportedLakeTable(CreateForeignTableStmt *createStmt)
{
	List	   *options = createStmt->options;
	DefElem    *pathOption = GetOption(options, "path");
	char	   *path = pathOption != NULL ? defGetString(pathOption) : "";
	DefElem    *locationOption = GetOption(options, "location");
	char	   *location = locationOption != NULL ? defGetString(locationOption) : "";

	bool		isWritable = GetBoolOption(createStmt->options, "writable", false);

	if (isWritable && createStmt->base.tableElts == NIL &&
		createStmt->base.partbound == NULL)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("column list cannot be empty for writable "
							   "pg_lake tables")));

	if (!isWritable && pathOption == NULL)
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("\"path\" option is required for regular "
							   "pg_lake tables")));

	if (!isWritable && !IsSupportedURL(path))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: only s3:// and gs:// URLs are "
							   "currently supported")));
	}
	else if (isWritable && !IsSupportedURL(location))
	{

		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: only s3:// and gs:// URLs are "
							   "currently supported")));
	}

	if (isWritable)
		ErrorIfWritableTableWithReservedColumnName(createStmt->base.tableElts, PG_LAKE_TABLE_TYPE);

#if PG_VERSION_NUM >= 180000
	ErrorIfTableContainsVirtualColumns(createStmt->base.tableElts);
#endif
}

/*
 * ErrorIfWritableTableWithReservedColumnName errors if column list contains a column
 * with a reserved column name, which is also returned by duckdb's "read_parquet"
 * function.
 */
static void
ErrorIfWritableTableWithReservedColumnName(List *columnDefList, PgLakeTableType tableType)
{
	ListCell   *columnDefCell = NULL;

	foreach(columnDefCell, columnDefList)
	{
		/* could be LIKE clause or constraint clause */
		if (!IsA(lfirst(columnDefCell), ColumnDef))
		{
			continue;
		}

		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		if (IsConflictingColumnNameForReadParquet(columnDef->colname))
		{
			const char *tableTypeStr = PgLakeTableTypeToName(tableType);

			ereport(ERROR, (errcode(ERRCODE_RESERVED_NAME),
							errmsg("pg_lake_table: column name \"%s\" is reserved for "
								   "%s tables", columnDef->colname, tableTypeStr),
							errhint("Please use a different column name.")));
		}
	}
}

/*
 * ErrorIfInvalidFilenameColumn errors if the _filename column is not in the last place.
 */
static void
ErrorIfInvalidFilenameColumn(List *columnDefList)
{
	ListCell   *columnDefCell = NULL;
	bool		hasFilenameColumn = false;

	foreach(columnDefCell, columnDefList)
	{
		/* could be LIKE clause or constraint clause */
		if (!IsA(lfirst(columnDefCell), ColumnDef))
			continue;

		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		if (strcmp(columnDef->colname, "_filename") != 0)
			continue;

		int32		typmod = 0;
		Oid			typeOid = InvalidOid;
		bool		missingOK = true;

		Type		typeTuple = LookupTypeName(NULL, columnDef->typeName, &typmod, missingOK);

		if (typeTuple != NULL)
		{
			typeOid = typeTypeId(typeTuple);

			ReleaseSysCache(typeTuple);
		}

		if (typeOid != TEXTOID)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("_filename column must have type text")));
		}

		hasFilenameColumn = true;
	}

	if (!hasFilenameColumn)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("no _filename column found"),
						errdetail("When using the filename option, the last column "
								  "must be _filename text")));
	}
}


/*
 * IsConflictingColumnNameForReadParquet returns true if given column name
 * is a column name which is also returned by duckdb's "read_parquet" calls.
 *
 * We rely on Duckdb's file_row_number and filename columns during
 * read_parquet calls.
 */
bool
IsConflictingColumnNameForReadParquet(const char *columnName)
{
	const char *reservedColumnNames[] = {"file_row_number", INTERNAL_FILENAME_COLUMN_NAME};
	const int	numReservedColumnNames = sizeof(reservedColumnNames) / sizeof(reservedColumnNames[0]);

	for (int reservedNameIndex = 0; reservedNameIndex < numReservedColumnNames; reservedNameIndex++)
	{
		if (pg_strcasecmp(columnName, reservedColumnNames[reservedNameIndex]) == 0)
		{
			return true;
		}
	}

	/* if we have a hook to check additional columns, call it now */
	if (PgLakeIsReservedColumnNameHook)
		return PgLakeIsReservedColumnNameHook(columnName);

	return false;
}

/*
 * ProcessPgLakeTable is a utility statement handler for handling
 * statements for creating pg_lake tables.
 *
 * Currently this code implements:
 * 1. Adding columns definitions to lake tables:
 *   - CREATE FOREIGN TABLE name () SERVER pg_lake OPTIONS (path 's3://...')
 *
 * 2. CREATE TABLE USING syntax for iceberg tables:
 *   - CREATE TABLE name (<col_defs>) USING pg_lake_iceberg WITH (location = <>)
 */
bool
ProcessCreatePgLakeTable(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (IsA(plannedStmt->utilityStmt, CreateForeignTableStmt))
	{
		CreateForeignTableStmt *createStmt =
			(CreateForeignTableStmt *) plannedStmt->utilityStmt;

		if (IsPgLakeIcebergServerName(createStmt->servername))
		{
			return ProcessCreateIcebergTableFromForeignTableStmt(params);
		}
		else if (IsPgLakeServerName(createStmt->servername))
		{
			return ProcessCreateLakeTable(params);
		}
	}
	else if (IsA(plannedStmt->utilityStmt, CreateStmt))
	{
		return ProcessCreateIcebergTableFromCreateStmt(params);
	}

	return false;
}


/*
 * ProcessCreateLakeTable handles CREATE FOREIGN TABLE statements
 * that creates pg_lake tables.
 */
static bool
ProcessCreateLakeTable(ProcessUtilityParams * params)
{
	Assert(IsA(params->plannedStmt->utilityStmt, CreateForeignTableStmt));

	CreateForeignTableStmt *createStmt =
		(CreateForeignTableStmt *) params->plannedStmt->utilityStmt;

	if (!IsPgLakeServerName(createStmt->servername))
	{
		/* not a lake table */
		return false;
	}

	/* when creating a partition we always inherit columns from parent */
	if (createStmt->base.partbound != NULL)
		return false;

	/*
	 * If the column list is empty, we automatically fill it in.
	 */
	if (createStmt->base.tableElts == NIL)
	{
		/* we will adjust column list in the parse tree */
		if (params->readOnlyTree)
			createStmt = (CreateForeignTableStmt *) CopyUtilityStmt(params);

		AddLakeTableColumnDefinitions(createStmt);

		/*
		 * Rerun all DDL handlers. We will not re-enter this path since the
		 * tableElts is no longer NIL.
		 */
		PgLakeCommonProcessUtility(params);
		return true;
	}

	/*
	 * If there are unbounded numerics, we assign them a scale and precision.
	 */
	if (IsForeignTableStmtWithUnboundedNumericColumns(createStmt))
	{
		/* we will adjust numerics in the parse tree */
		if (params->readOnlyTree)
			createStmt = (CreateForeignTableStmt *) CopyUtilityStmt(params);

		SetDefaultPrecisionAndScaleForUnboundedNumericColumns(createStmt->base.tableElts);

		/*
		 * Note: we do not explicitly rerun handlers from the start, since we
		 * expect the statement to be ready for execution.
		 */
	}

	/*
	 * If there is a filename option, check whether the _filename column is in
	 * the right place.
	 */
	bool		hasFilename = GetBoolOption(createStmt->options, "filename", false);

	if (hasFilename)
	{
		bool		isWritable = GetBoolOption(createStmt->options, "writable", false);

		if (isWritable)
			/* filename option is never allowed for writable tables */
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("\"filename\" option is not allowed for writable pg_lake tables")));

		ErrorIfInvalidFilenameColumn(createStmt->base.tableElts);
	}

	return false;
}


/*
 * ProcessCreateIcebergTableFromForeignTableStmt handles CREATE FOREIGN TABLE
 * statements that are pg_lake_iceberg tables.
 */
static bool
ProcessCreateIcebergTableFromForeignTableStmt(ProcessUtilityParams * params)
{
	Assert(IsA(params->plannedStmt->utilityStmt, CreateForeignTableStmt));

	CreateForeignTableStmt *createStmt =
		(CreateForeignTableStmt *) params->plannedStmt->utilityStmt;

	if (!IsPgLakeIcebergServerName(createStmt->servername))
	{
		/* not an iceberg table */
		return false;
	}

	/* we might adjust the parse tree */
	if (params->readOnlyTree)
		createStmt = (CreateForeignTableStmt *) CopyUtilityStmt(params);

	bool		hasRestCatalogOption = HasRestCatalogTableOption(createStmt->options);
	bool		hasObjectStoreCatalogOption = HasObjectStoreCatalogTableOption(createStmt->options);

	if (hasObjectStoreCatalogOption || hasRestCatalogOption)
	{
		Oid			namespaceId = RangeVarGetAndCheckCreationNamespace(createStmt->base.relation, NoLock, NULL);

		/*
		 * Read-only external catalog tables are a special case of Iceberg
		 * tables. They are recognized as Iceberg tables, but are not
		 * registered in any internal catalogs (e.g., lake_iceberg.tables).
		 * Instead, the table is created only in PostgreSQL’s system
		 * catalogs. When the table is queried, its metadata is fetched on
		 * demand from the external catalog.
		 */
		bool		hasExternalCatalogReadOnlyOption = HasReadOnlyOption(createStmt->options);

		char	   *metadataLocation = NULL;
		char	   *catalogNamespace = NULL;
		char	   *catalogTableName = NULL;
		char	   *catalogName = NULL;

		char	   *catalogNamespaceProvided = GetStringOption(createStmt->options, "catalog_namespace", false);

		/*
		 * Always provide catalog_namespace and catalog_table_name options for
		 * REST catalog iceberg tables. If not provided by user, we set them
		 * to default values. The default values are the table's schema name
		 * and table name.
		 */
		if (catalogNamespaceProvided == NULL && hasExternalCatalogReadOnlyOption)
		{
			catalogNamespace = get_namespace_name(namespaceId);

			/* add catalog_namespace table options */
			createStmt->options =
				lappend(createStmt->options,
						makeDefElem("catalog_namespace", (Node *) makeString(catalogNamespace), -1));
		}
		else
		{
			catalogNamespace = catalogNamespaceProvided;
		}

		char	   *catalogTableNameProvided = GetStringOption(createStmt->options, "catalog_table_name", false);

		if (catalogTableNameProvided == NULL && hasExternalCatalogReadOnlyOption)
		{
			catalogTableName = pstrdup(createStmt->base.relation->relname);
			createStmt->options =
				lappend(createStmt->options,
						makeDefElem("catalog_table_name", (Node *) makeString(catalogTableName), -1));
		}
		else
		{
			catalogTableName = catalogTableNameProvided;
		}

		char	   *catalogNameProvided = GetStringOption(createStmt->options, "catalog_name", false);

		if (catalogNameProvided == NULL && hasExternalCatalogReadOnlyOption)
		{
			catalogName = get_database_name(MyDatabaseId);
			createStmt->options =
				lappend(createStmt->options,
						makeDefElem("catalog_name", (Node *) makeString(catalogName), -1));
		}
		else
		{
			catalogName = catalogNameProvided;
		}

		if (hasRestCatalogOption && hasExternalCatalogReadOnlyOption)
		{
			ErrorIfRestNamespaceDoesNotExist(catalogName, catalogNamespace);

			metadataLocation =
				GetMetadataLocationFromRestCatalog(catalogName, catalogNamespace, catalogTableName);
		}
		else if (hasObjectStoreCatalogOption && hasExternalCatalogReadOnlyOption)
		{
			ErrorIfExternalObjectStoreCatalogDoesNotExist(catalogName);

			metadataLocation =
				GetTableMetadataLocationFromExternalObjectStoreCatalog(catalogName,
																	   catalogNamespace,
																	   catalogTableName);
		}

		if (!hasExternalCatalogReadOnlyOption)
		{
			/*
			 * For writable object store catalog tables, we need to continue
			 * with the regular iceberg table creation process. We only fill
			 * in the catalog options here. Other than that, we simply check
			 * if user provided any catalog options. That's not allowed,
			 * writable tables only inherit from the database name, schema
			 * name, and table name.
			 */
			if (catalogNamespaceProvided != NULL ||
				catalogTableNameProvided != NULL ||
				catalogNameProvided != NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("writable %s catalog iceberg tables do not "
									   "allow explicit catalog options", hasObjectStoreCatalogOption ? "object store" : "REST")));
			}
		}
		else if (createStmt->base.tableElts == NIL && hasExternalCatalogReadOnlyOption)
		{
			List	   *dataFileColumns =
				DescribeColumnsFromIcebergMetadataURI(metadataLocation, false);

			createStmt->base.tableElts = dataFileColumns;
			EnsureSupportedIcebergTableColumnDefinitions(createStmt->base.tableElts);

			/*
			 * Rerun all DDL handlers. We will not re-enter this path since
			 * the tableElts is no longer NIL.
			 */
			PgLakeCommonProcessUtility(params);

			return true;
		}
		else
		{
			EnsureSupportedIcebergTableColumnDefinitions(createStmt->base.tableElts);

			PgLakeCommonParentProcessUtility(params);

			return true;
		}
	}

	EnsureSupportedIcebergTableColumnDefinitions(createStmt->base.tableElts);

	Oid			namespaceId =
		RangeVarGetAndCheckCreationNamespace(createStmt->base.relation, NoLock, NULL);

	createStmt->base.tableElts = ExpandTableElements(createStmt->base.tableElts);

	if (createStmt->base.relation->schemaname == NULL)
	{
		/*
		 * Fix the schema name to be robust to search_path changes, since we
		 * rely on the schema name in PostProcessCreateIcebergTable.
		 */
		createStmt->base.relation->schemaname = get_namespace_name(namespaceId);
	}

	/*
	 * If there are unbounded numerics, we assign them a scale and precision.
	 */
	if (createStmt->base.partbound == NULL && IsForeignTableStmtWithUnboundedNumericColumns(createStmt))
	{
		SetDefaultPrecisionAndScaleForUnboundedNumericColumns(createStmt->base.tableElts);
	}

	DefElem    *catalogOption = GetOption(createStmt->options, "catalog");

	if (catalogOption == NULL)
	{
		DefElem    *defaultCatalog = makeDefElem("catalog", (Node *) makeString("postgres"), -1);

		createStmt->options = lappend(createStmt->options, defaultCatalog);
	}

	DefElem    *locationOption = GetOption(createStmt->options, "location");
	const char *defaultLocationPrefix = GetIcebergDefaultLocationPrefix();

	/*
	 * We will set the location by using the default location prefix when user
	 * does not specify the location but already set default locatipn prefix.
	 * We append the "database_name/schema_name/table_name/relation_id" to the
	 * default location prefix. Since the relation_id is available only after
	 * table is created at post hook, we set the location as a placeholder for
	 * now. We will replace the placeholder with the actual location at post
	 * hook.
	 */
	if (locationOption == NULL && defaultLocationPrefix != NULL)
	{
		DefElem    *defaultPlaceholder = makeDefElem("location", (Node *) makeString(DEFAULT_ICEBERG_LOCATION_PLACEHOLDER), -1);

		createStmt->options = lappend(createStmt->options, defaultPlaceholder);
	}


	if (hasObjectStoreCatalogOption)
	{
		const char *objectStoreCatalogLocationPrefix = GetObjectStoreDefaultLocationPrefix();

		if (objectStoreCatalogLocationPrefix == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("object store catalog iceberg tables require "
								   "pg_lake_iceberg.object_store_catalog_location_prefix "
								   "to be set")));
		}
	}

	/*
	 * Our CREATE FOREIGN TABLE statement is fully ready for execution, so we
	 * go to the parent ProcessUtility.
	 */
	PgLakeCommonParentProcessUtility(params);

	/*
	 * Repeat the type checks for CREATE FOREIGN TABLE .. PARTITION OF .. now
	 * that the column list is populated.
	 */
	if (createStmt->base.partbound != NULL)
		ErrorIfTableContainsUnsupportedTypes(createStmt->base.tableElts);

	/* the table is now created, get its OID */
	Oid			relationId = RangeVarGetRelid(createStmt->base.relation, NoLock, false);

	char	   *location;

	if (locationOption != NULL)
	{
		/*
		 * We use GetWritableTableLocation rather than looking at the option
		 * directly because it cleans up the separator.
		 */
		char	   *queryArguments = "";

		location = GetWritableTableLocation(relationId, &queryArguments);
	}
	else
	{
		/*
		 * replace default placeholder uri with the actual default uri for the
		 * table
		 */
		Assert(defaultLocationPrefix != NULL);

		char	   *databaseName = get_database_name(MyDatabaseId);
		char	   *tableName = createStmt->base.relation->relname;
		char	   *schemaName = createStmt->base.relation->schemaname;

		location = SetIcebergTableLocationOptionFromDefaultPrefix(relationId,
																  defaultLocationPrefix,
																  databaseName,
																  schemaName, tableName);
	}

	/* we do not allow non-empty locations */
	ErrorIfLocationIsNotEmpty(location);

	/* we currently only allow Iceberg tables in the managed storage region */
	ErrorIfNotInManagedStorageRegion(location);


	if (hasRestCatalogOption)
	{
		/* here we only deal with writable rest catalog iceberg tables */
		Assert(!HasReadOnlyOption(createStmt->options));

		/*
		 * For writable rest catalog iceberg tables, we register the namespace
		 * in the rest catalog. We do that later in the command processing so
		 * that any previous errors (e.g., table creation failures) prevents
		 * us from registering the namespace.
		 *
		 * Note that registering a namespace is not a transactional operation
		 * from pg_lake's perspective. If the subsequent table creation fails,
		 * the namespace registration will remain. We accept that tradeoff for
		 * simplicity as re-registering an existing namespace is a no-op. For
		 * a writable rest catalog iceberg table, the namespace is always the
		 * table's schema name. Similarly, the catalog name is always the
		 * database name. We normally encode that in GetRestCatalogName()
		 * etc., but here we need to do it early before the table is created.
		 */
		RegisterNamespaceToRestCatalog(get_database_name(MyDatabaseId),
									   get_namespace_name(namespaceId));
	}

	bool		hasRowIds = GetBoolOption(createStmt->options, "row_ids", false);

	/* when a table has row_ids, we need to create a sequence */
	if (hasRowIds)
		CreateRelationRowIdSequence(relationId);

	List	   *columnDefList = createStmt->base.tableElts;

	/*
	 * This function should run after StandardProcessUtility, such that the
	 * columnDefList is already the restricted list.
	 */
	Assert(list_length(GetRestrictedColumnDefList(columnDefList)) == list_length(columnDefList));

	List	   *ddlOps = NIL;

	IcebergDDLOperation *createDDLOp = palloc0(sizeof(IcebergDDLOperation));

	createDDLOp->type = DDL_TABLE_CREATE;
	createDDLOp->columnDefs = columnDefList;
	createDDLOp->hasCustomLocation = (locationOption != NULL);

	ddlOps = lappend(ddlOps, createDDLOp);

	/* make sure partition_by option is well formed */
	List	   *parsedTransforms = ParseIcebergTablePartitionBy(relationId);

	/* make sure adding default spec if table is created with partitioning */
	if (!IsIcebergTableWithDefaultPartitionSpec(relationId))
	{
		IcebergDDLOperation *defaultPartitionDDLOp = palloc0(sizeof(IcebergDDLOperation));

		defaultPartitionDDLOp->type = DDL_TABLE_SET_PARTITION_BY;
		defaultPartitionDDLOp->parsedTransforms = NIL;

		ddlOps = lappend(ddlOps, defaultPartitionDDLOp);
	}

	IcebergDDLOperation *partitionDDLOp = palloc0(sizeof(IcebergDDLOperation));

	partitionDDLOp->type = DDL_TABLE_SET_PARTITION_BY;
	partitionDDLOp->parsedTransforms = parsedTransforms;

	ddlOps = lappend(ddlOps, partitionDDLOp);

	ApplyDDLChanges(relationId, ddlOps);

	/* signal that we already executed parent process utility */
	return true;
}

static void
EnsureSupportedIcebergTableColumnDefinitions(List *columnDefList)
{
#if PG_VERSION_NUM >= 180000
	ErrorIfTableContainsVirtualColumns(columnDefList);
#endif
	ErrorIfTableContainsUnsupportedTypes(columnDefList);
	ErrorIfWritableTableWithReservedColumnName(columnDefList, PG_LAKE_ICEBERG_TABLE_TYPE);
}


/*
 * ErrorIfLocationIsNotEmpty errors if the given location is not empty.
 */
static void
ErrorIfLocationIsNotEmpty(const char *location)
{
	Assert(location != NULL);

	List	   *metadataFiles = ListRemoteFileNames(psprintf("%s/**", location));

	if (metadataFiles != NIL)
	{
		ereport(ERROR, (errcode(ERRCODE_DUPLICATE_FILE),
						errmsg("location \"%s\" is not empty", location),
						errhint("Please use an empty location.")));
	}
}

/*
 * ProcessCreateIcebergTableFromCreateStmt handles "CREATE TABLE USING pg_lake_iceberg"
 * statements. Behind the scenes, we convert "CREATE TABLE USING pg_lake_iceberg" statement
 * to "CREATE FOREIGN TABLE SERVER pg_lake_iceberg" statement for ease of use.
 *
 * We need to ensure that the CREATE TABLE statement does not contain anything
 * (e.g. temporary table, unlogged table) that Postgres ignores during conversion
 * to foreign tables.
 */
static bool
ProcessCreateIcebergTableFromCreateStmt(ProcessUtilityParams * params)
{
	Assert(IsA(params->plannedStmt->utilityStmt, CreateStmt));

	CreateStmt *createStmt =
		(CreateStmt *) params->plannedStmt->utilityStmt;

	if (creating_extension)
	{
		/*
		 * We don't want to break any extensions that create its own metadata
		 * tables during create/alter extension update commands by using
		 * pg_lake_iceberg access method. This includes our own extensions
		 * such as pg_lake_iceberg as well as other extensions that use
		 * metadata tables, such as pg_cron.
		 */
		if (createStmt->accessMethod == NULL &&
			IsPgLakeIcebergAccessMethod(default_table_access_method))
		{
			createStmt->accessMethod = DEFAULT_TABLE_ACCESS_METHOD;
		}

		return false;
	}

	PgLakeTableType tableType = GetPgLakeTableTypeViaAccessMethod(createStmt->accessMethod);

	if (tableType != PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		/* not an iceberg table */
		return false;
	}

	/*
	 * We replace the CreateStmt, but still copy the underlying utility
	 * statement.
	 */
	if (params->readOnlyTree)
		createStmt = (CreateStmt *) CopyUtilityStmt(params);

	if (IsDBTTempTable(createStmt->relation))
	{
		/*
		 * replace the default access method to heap for temp tables when dbt
		 * runs to prevent errors during materiazed = incremental mode. We
		 * must replace the access method since it throws error at its
		 * am_handler as it is a placeholder access method.
		 */
		createStmt->accessMethod = DEFAULT_TABLE_ACCESS_METHOD;
		return false;
	}

	createStmt->tableElts = ExpandTableElements(createStmt->tableElts);
	EnsureCreateIcebergTableSupported(createStmt);

	CreateForeignTableStmt *createIcebergTableStmt =
		GetCreateIcebergForeignTableStmtFromCreateStmt(createStmt);

	/* replace the utility statement */
	params->plannedStmt->utilityStmt = (Node *) createIcebergTableStmt;

	/*
	 * Rerun all DDL handlers. We will not re-enter this path since the
	 * statement is no longer a CreateStmt.
	 */
	PgLakeCommonProcessUtility(params);
	return true;
}


/*
 * GetCreateIcebergForeignTableStmtFromCreateStmt returns a statement
 * "CREATE FOREIGN TABLE SERVER pg_lake_iceberg" from a statement
 * "CREATE TABLE USING pg_lake_iceberg".
 */
static CreateForeignTableStmt *
GetCreateIcebergForeignTableStmtFromCreateStmt(CreateStmt *createStmt)
{
	CreateForeignTableStmt *foreignTableStmt = makeNode(CreateForeignTableStmt);

	/* we use the same names for foreign server and access method names  */
	foreignTableStmt->servername = PG_LAKE_ICEBERG_SERVER_NAME;
	foreignTableStmt->options = createStmt->options;
	foreignTableStmt->base = *createStmt;
	foreignTableStmt->base.accessMethod = NULL;
	foreignTableStmt->base.type = T_CreateForeignTableStmt;

	/* remove WITH options from the original CREATE statement */
	foreignTableStmt->base.options = NIL;

	return foreignTableStmt;
}


/*
 * EnsureCreateIcebergTableColumnOptions ensures that the given CREATE TABLE statement
 * does not have any column storage or compression options that are not supported.
 *
 * We do not need to check for column constraints since Postgres will throw error
 * if it is not supported for foreign tables.
 */
static void
EnsureCreateIcebergTableColumnOptions(CreateStmt *createStmt)
{
	ListCell   *columnDefCell = NULL;

	foreach(columnDefCell, createStmt->tableElts)
	{
		/*
		 * This might be Constraint, e.g. (a int, check (a > 10)) or
		 * TableLikeClause, e.g. (LIKE other_table).
		 *
		 * When it is constraint, we don't need to check anything. Postgres
		 * will throw error if it is not supported for foreign tables.
		 *
		 * When it is TableLikeClause, we don't need to check anything.
		 * Postgres will throw error LIKE clause is not supported for foreign
		 * tables.
		 */
		if (!IsA(lfirst(columnDefCell), ColumnDef))
		{
			continue;
		}

		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		if (columnDef->storage_name != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_table: column storage is not allowed for "
								   "pg_lake_iceberg tables")));
		}

		if (columnDef->compression != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_table: column compression is not allowed for "
								   "pg_lake_iceberg tables")));
		}
	}

	return;
}

/*
 * EnsureCreateIcebergTableSupported checks whether the given
 * "CREATE TABLE USING pg_lake_iceberg" statement is valid to convert to
 * "CREATE FOREIGN TABLE SERVER pg_lake_iceberg" statement.
 *
 * We make sure "CREATE TABLE" syntax has no part which would not be valid for
 * foreign tables. We need to check most of these since Postgres does not
 * throw error during the conversion and the parts are silently ignored.
 *
 * We intentionally does not check "constraints" and "table options" since Postgres
 * validates and throws errors for these as they are also accepted by foreign tables.
 */
static void
EnsureCreateIcebergTableSupported(CreateStmt *createStmt)
{
	if (createStmt->relation->relpersistence == RELPERSISTENCE_TEMP)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: temporary tables are not allowed "
							   "for pg_lake_iceberg tables")));
	}

	if (createStmt->relation->relpersistence == RELPERSISTENCE_UNLOGGED)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: unlogged tables are not allowed "
							   "for pg_lake_iceberg tables")));
	}

	if (createStmt->partspec != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: partitioned tables are not allowed "
							   "for pg_lake_iceberg tables")));
	}

	if (createStmt->tablespacename != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: tablespace is not allowed "
							   "for pg_lake_iceberg tables")));
	}

	if (createStmt->ofTypename != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: CREATE TABLE OF is not allowed "
							   "for pg_lake_iceberg tables")));
	}

	EnsureCreateIcebergTableColumnOptions(createStmt);

	return;
}


/*
 * IsCreateLakeTable returns whether the given CREATE FOREIGN TABLE statement
 * will create a lake table.
 */
static bool
IsCreateLakeTable(CreateForeignTableStmt *createStmt)
{
	bool		missingOK = true;
	ForeignServer *foreignServer =
		GetForeignServerByName(createStmt->servername, missingOK);

	if (foreignServer == NULL)
	{
		/* server does not exist, let postgres handle it */
		return false;
	}

	ForeignDataWrapper *foreignDataWrapper =
		GetForeignDataWrapper(foreignServer->fdwid);

	if (strncasecmp(foreignDataWrapper->fdwname, PG_LAKE_TABLE, strlen(PG_LAKE_TABLE)) != 0)
	{
		/* not our FDW */
		return false;
	}

	return true;
}


/*
 * AddLakeTableColumnDefinitions handles
 * - CREATE FOREIGN TABLE name () SERVER pg_lake OPTIONS (path 's3://...')
 *
 * by filling in the column definitions based on the columns of the file.
 */
static void
AddLakeTableColumnDefinitions(CreateForeignTableStmt *createStmt)
{
	if (createStmt->base.tableElts != NIL)
	{
		/* user already decided on columns */
		return;
	}

	List	   *options = createStmt->options;

	/* set the new column definition list */
	CopyDataFormat format = DATA_FORMAT_INVALID;
	CopyDataCompression compression = DATA_COMPRESSION_INVALID;
	PgLakeTableType tableType = GetPgLakeTableTypeViaServerName(createStmt->servername);

	DefElem    *pathOption = GetOption(options, "path");
	char	   *path = pathOption != NULL ? defGetString(pathOption) : "";

	Assert(IsSupportedURL(path));

	/* determine format & compression to make DESCRIBE more precise */
	FindDataFormatAndCompression(tableType, path, options, &format, &compression);

	List	   *dataFileColumns = DescribeColumnsForURL(path,
														format, compression,
														createStmt->options);

	createStmt->base.tableElts = dataFileColumns;

	if (format == DATA_FORMAT_CSV)
	{
		createStmt->options = SniffCSVOptions(path, compression, createStmt->options);
	}
}


/*
 * Transform a list of base table elements into a list of ColumnDefs (to the
 * extent that we can).
 *
 * Right now we just expand out TableLikeClause into the corresponding
 * ColumnDef elements, but it is possible there would be additional types that
 * we don't handle in this case.  These will just get handed post-transform to
 * the underlying CreateStmt utility handler, so will presumably just error if
 * we don't know what to do.
 *
 * While Iceberg tables can conceivably support more complicated things like
 * constraints, we are only considering column definitions for now.
 */
static List *
ExpandTableElements(List *tableElements)
{
	List	   *expandedTableElements = NIL;
	ListCell   *listCell;

	foreach(listCell, tableElements)
	{
		Node	   *node = (Node *) lfirst(listCell);

		if (IsA(node, TableLikeClause))
		{
			/* expand our LIKE into any number of columns, including 0 */
			expandedTableElements = list_concat(expandedTableElements, ExpandTableLikeClause((TableLikeClause *) node));
		}
		else
		{
			/*
			 * Anything else (ColumnDef, check constraints, etc) just gets
			 * persisted as-is.
			 */
			expandedTableElements = lappend(expandedTableElements, node);
		}
	}
	return expandedTableElements;
}


/*
 * Expand the TableLikeClause into a list of underlying ColumnDefs. This is
 * intended to allow you to transform the data to replace the column defs to
 * support LIKE in our Iceberg tables.
 *
 * Heavily borrows from transformTableLikeClause().
 */
static List *
ExpandTableLikeClause(TableLikeClause *table_like_clause)
{
	AttrNumber	parent_attno;
	Relation	relation;
	TupleDesc	tupleDesc;
	List	   *newColumns = NIL;
	AclResult	aclresult;

	/* Sanity-check that we don't have any unsupported options here */
	if (table_like_clause->options != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("only basic \"LIKE table\" is supported; no \"INCLUDING\" clause support for iceberg tables")));
	}

	/* Open the relation referenced by the LIKE clause */
	relation = relation_openrv(table_like_clause->relation, AccessShareLock);

	if (relation->rd_rel->relkind != RELKIND_RELATION &&
		relation->rd_rel->relkind != RELKIND_VIEW &&
		relation->rd_rel->relkind != RELKIND_MATVIEW &&
		relation->rd_rel->relkind != RELKIND_COMPOSITE_TYPE &&
		relation->rd_rel->relkind != RELKIND_FOREIGN_TABLE &&
		relation->rd_rel->relkind != RELKIND_PARTITIONED_TABLE)
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is invalid in LIKE clause",
						RelationGetRelationName(relation)),
				 errdetail_relkind_not_supported(relation->rd_rel->relkind)));

	/*
	 * Check for privileges
	 */
	if (relation->rd_rel->relkind == RELKIND_COMPOSITE_TYPE)
	{
		aclresult = object_aclcheck(TypeRelationId, relation->rd_rel->reltype, GetUserId(),
									ACL_USAGE);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, OBJECT_TYPE,
						   RelationGetRelationName(relation));
	}
	else
	{
		aclresult = pg_class_aclcheck(RelationGetRelid(relation), GetUserId(),
									  ACL_SELECT);
		if (aclresult != ACLCHECK_OK)
			aclcheck_error(aclresult, get_relkind_objtype(relation->rd_rel->relkind),
						   RelationGetRelationName(relation));
	}

	tupleDesc = RelationGetDescr(relation);

	/*
	 * Insert the copied attributes into the cxt for the new table definition.
	 * We must do this now so that they appear in the table in the relative
	 * position where the LIKE clause is, as required by SQL99.
	 */
	for (parent_attno = 1; parent_attno <= tupleDesc->natts;
		 parent_attno++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDesc,
													parent_attno - 1);
		ColumnDef  *def;

		/*
		 * Ignore dropped columns in the parent.
		 */
		if (attribute->attisdropped)
			continue;

		/*
		 * Create a new column, which is marked as NOT inherited.
		 *
		 * For constraints, ONLY the not-null constraint is inherited by the
		 * new column definition per SQL99.
		 */
		def = makeColumnDef(NameStr(attribute->attname), attribute->atttypid,
							attribute->atttypmod, attribute->attcollation);
		def->is_not_null = attribute->attnotnull;

		/*
		 * Add to column list
		 */
		newColumns = lappend(newColumns, def);

		def->storage = 0;
		def->compression = NULL;
	}

	/*
	 * Close the parent rel, but keep our AccessShareLock on it until xact
	 * commit.  That will prevent someone else from deleting or ALTERing the
	 * parent before we can finish creating.
	 */
	table_close(relation, NoLock);

	return newColumns;
}


/*
 * IsForeignTableStmtWithUnboundedNumericColumns checks whether the given
 * CreateForeignTableStmt has unbounded numeric columns.
 */
static bool
IsForeignTableStmtWithUnboundedNumericColumns(CreateForeignTableStmt *createStmt)
{
	ListCell   *columnDefCell = NULL;

	foreach(columnDefCell, createStmt->base.tableElts)
	{
		/* could be LIKE clause or constraint clause */
		if (!IsA(lfirst(columnDefCell), ColumnDef))
		{
			continue;
		}

		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		int32		typmod = 0;
		bool		missingOK = true;
		Type		typeTuple = LookupTypeName(NULL, columnDef->typeName, &typmod, missingOK);

		if (typeTuple == NULL)
		{
			/*
			 * type not found, could be serial. But we are sure it is not
			 * numeric
			 */
			continue;
		}

		Oid			typeOid = typeTypeId(typeTuple);

		ReleaseSysCache(typeTuple);

		if (IsUnboundedNumeric(typeOid, typmod))
		{
			return true;
		}
	}

	return false;
}


/*
 * SetDefaultPrecisionAndScaleForUnboundedNumericColumns sets the default
 * precision and scale for unbounded numeric columns.
 */
static void
SetDefaultPrecisionAndScaleForUnboundedNumericColumns(List *columnDefList)
{
	ListCell   *columnDefCell = NULL;

	foreach(columnDefCell, columnDefList)
	{
		/* could be LIKE clause or constraint clause */
		if (!IsA(lfirst(columnDefCell), ColumnDef))
		{
			continue;
		}

		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		int32		typmod = 0;
		bool		missingOK = true;
		Type		typeTuple = LookupTypeName(NULL, columnDef->typeName, &typmod, missingOK);

		if (typeTuple == NULL)
		{
			/*
			 * type not found, could be serial. But we are sure it is not
			 * numeric
			 */
			continue;
		}

		Oid			typeOid = typeTypeId(typeTuple);

		if (IsUnboundedNumeric(typeOid, typmod))
		{
			int			newTypMod = make_numeric_typmod(UnboundedNumericDefaultPrecision,
														UnboundedNumericDefaultScale);

			columnDef->typeName = makeTypeNameFromOid(typeOid, newTypMod);

			ereport(NOTICE, (errmsg("setting default precision and scale for unbounded numeric column \"%s\" to (%d, %d)",
									columnDef->colname, UnboundedNumericDefaultPrecision,
									UnboundedNumericDefaultScale),
							 errdetail("Iceberg tables do not fully support unbounded numeric columns.")));
		}

		ReleaseSysCache(typeTuple);
	}
}


/*
 * SetIcebergTableLocationOptionFromDefaultPrefix sets the location option for
 * given iceberg table via internal api. Location will be in the format of
 * "<database>/<schema>/<table>/<relationId>"
 */
static char *
SetIcebergTableLocationOptionFromDefaultPrefix(Oid relationId,
											   const char *defaultLocationPrefix,
											   char *databaseName,
											   char *schemaName, char *tableName)
{
	Assert(IsPgLakeIcebergForeignTableById(relationId));

	StringInfo	location = makeStringInfo();

	appendStringInfo(location, "%s/%s/%s/%s/%u", defaultLocationPrefix,
					 URLEncodePath(databaseName), URLEncodePath(schemaName),
					 URLEncodePath(tableName), relationId);

	DefElem    *locationOption = makeDefElem("location", (Node *) makeString(location->data), -1);

	locationOption->defaction = DEFELEM_SET;

	AlterTableCmd *setLocationOptionCmd = makeNode(AlterTableCmd);

	setLocationOptionCmd->subtype = AT_GenericOptions;
	setLocationOptionCmd->def = (Node *) list_make1(locationOption);

	AlterTableStmt *alterTable = makeNode(AlterTableStmt);

	alterTable->relation = makeRangeVar(schemaName, tableName, -1);
	alterTable->objtype = OBJECT_FOREIGN_TABLE;
	alterTable->cmds = list_make1(setLocationOptionCmd);

	PlannedStmt *plannedStmt = makeNode(PlannedStmt);

	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = (Node *) alterTable;

	/*
	 * Call regular ProcessUtility, because our hook is not yet ready to
	 * process Iceberg table DDL, since the table is not yet in the Iceberg
	 * catalog.
	 */
	standard_ProcessUtility(plannedStmt, "ALTER FOREIGN TABLE", false,
							PROCESS_UTILITY_QUERY, NULL, NULL, None_Receiver, NULL);

	return location->data;
}

#if PG_VERSION_NUM >= 180000
/*
 * ErrorIfTableContainsVirtualColumns throws an error if the given column definitions
 */
static void
ErrorIfTableContainsVirtualColumns(List *columnDefList)
{
	List	   *restrictedColumnDefList =
		GetRestrictedColumnDefList(columnDefList);

	ListCell   *columnDefCell = NULL;

	foreach(columnDefCell, restrictedColumnDefList)
	{
		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		List	   *constraints = columnDef->constraints;

		ListCell   *constraintCell = NULL;

		foreach(constraintCell, constraints)
		{
			Constraint *constraint = lfirst(constraintCell);

			if (constraint->contype == CONSTR_GENERATED && constraint->generated_kind == ATTRIBUTE_GENERATED_VIRTUAL)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("virtual generated columns are not supported"),
								errdetail("Iceberg tables do not support virtual generated columns.")));
			}
		}
	}
}
#endif

/*
* ErrorIfTableContainsUnsupportedTypes throws an error if the given column definitions
* contain unsupported types for Iceberg tables.
*/
static void
ErrorIfTableContainsUnsupportedTypes(List *columnDefList)
{
	ListCell   *columnDefCell;

	List	   *restrictedColumnDefList =
		GetRestrictedColumnDefList(columnDefList);

	foreach(columnDefCell, restrictedColumnDefList)
	{
		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);

		int32		typmod = 0;
		Oid			typeOid = InvalidOid;

		typenameTypeIdAndMod(NULL, columnDef->typeName, &typeOid, &typmod);

		char	   *columnName = columnDef->colname;

		ErrorIfTypeUnsupportedForIcebergTables(typeOid, typmod, columnName);
	}
}


/*
* ColumnDefIsPseudoSerial returns true if the ColumnDef has a SERIAL pseudo-type.
*/
bool
ColumnDefIsPseudoSerial(ColumnDef *column)
{
	/*
	 * Check for SERIAL pseudo-types, inspired from
	 * transformColumnDefinition().
	 */
	if (column->typeName
		&& list_length(column->typeName->names) == 1
		&& !column->typeName->pct_type)
	{
		char	   *typname = strVal(linitial(column->typeName->names));

		if (strcmp(typname, "smallserial") == 0 ||
			strcmp(typname, "serial2") == 0 ||
			strcmp(typname, "serial") == 0 ||
			strcmp(typname, "serial4") == 0 ||
			strcmp(typname, "bigserial") == 0 ||
			strcmp(typname, "serial8") == 0)
		{
			return true;
		}
	}

	return false;
}

/*
* ErrorIfTypeUnsupportedForIcebergTables throws an error if the given type is
* unsupported for Iceberg tables.
* It is mainly a wrapper around ErrorIfTypeUnsupportedForIcebergTablesInternal()
*/
void
ErrorIfTypeUnsupportedForIcebergTables(Oid typeOid, int32 typmod, char *columnName)
{
	int			level = 0;

	ErrorIfTypeUnsupportedForIcebergTablesInternal(typeOid, typmod, level, columnName);
}


/*
* We do not support:
* - numeric with precision > 38
* - table types
* - geometry types that are in arrays or composite types
*
* We use level to track the depth of the type. We do not support geometry types
* inside arrays or composite types.
*
* We pass top level column name to provide better error messages.
*/
static void
ErrorIfTypeUnsupportedForIcebergTablesInternal(Oid typeOid, int32 typmod, int level, char *columnName)
{
	/*
	 * Error if type is numeric and precision > 38 or scale > 38.
	 */
	if (typeOid == NUMERICOID)
	{
		ErrorIfTypeUnsupportedNumericForIcebergTables(typmod, columnName);
	}

	/*
	 * Iceberg does not have a native interval type, but Parquet does. We
	 * would like to store interval as the native Parquet type, but then we
	 * need to then somehow tell BuildParquetSchema that we want a (possibly
	 * nested) interval. For now, this is not yet implemented, and we want to
	 * avoid storing the intervals as strings.
	 */
	if (typeOid == INTERVALOID)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_iceberg: interval types in columns are not yet supported"),
						errdetail("column name triggering the error: \"%s\"", columnName)));
	}

	Oid			typrelid = get_typ_typrelid(typeOid);
	char		typtype = get_typtype(typeOid);

	if (typrelid != InvalidOid)
	{
		char		relkind = get_rel_relkind(typrelid);

		if (relkind != RELKIND_COMPOSITE_TYPE)
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_iceberg: table types are not supported as columns"),
							errdetail("column name triggering the error: \"%s\"", columnName ? columnName : "null")));
	}

	if (level >= 1 && IsGeometryTypeId(typeOid))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_iceberg: geometry types are not supported inside arrays, composite types or maps"),
						errdetail("column name triggering the error: \"%s\"", columnName ? columnName : "null")));
	}
	else if (level >= 1 && (typtype == TYPTYPE_MULTIRANGE || typtype == TYPTYPE_RANGE))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_iceberg: range types are not supported inside arrays, composite types or maps"),
						errdetail("column name triggering the error: \"%s\"", columnName ? columnName : "null")));
	}

	/* now, recurse into arrays & composite types */
	if (type_is_array(typeOid))
	{
		Oid			elemType = get_element_type(typeOid);


		ErrorIfTypeUnsupportedForIcebergTablesInternal(elemType, typmod, level + 1, columnName);
	}
	else if (typtype == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, typmod);
		int			i;

		for (i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			ErrorIfTypeUnsupportedForIcebergTablesInternal(attr->atttypid, attr->atttypmod, level + 1, columnName);
		}

		ReleaseTupleDesc(tupdesc);
	}
	else if (IsMapTypeOid(typeOid))
	{
		Oid			postgresTypeId = getBaseType(typeOid);

		/* Find the underlying element type from the array type */
		postgresTypeId = get_element_type(postgresTypeId);

		TypeCacheEntry *typEntry = lookup_type_cache(postgresTypeId, TYPECACHE_TUPDESC);

		/*
		 * If for some reason we aren't able to find the type in the typecache
		 * it's an error.
		 */
		if (!typEntry)
			return;
		TupleDesc	tupDesc = typEntry->tupDesc;

		if (!tupDesc)
			return;

		/*
		 * A map type is composed of an array of keys and an array of values,
		 * so a 2-column composite type.
		 */
		Assert(tupDesc->natts == 2);

		Form_pg_attribute keysAttribute = TupleDescAttr(tupDesc, 0);
		Form_pg_attribute valuesAttribute = TupleDescAttr(tupDesc, 1);

		Assert(strcmp(NameStr(keysAttribute->attname), "key") == 0);
		Assert(strcmp(NameStr(valuesAttribute->attname), "val") == 0);

		/*
		 * TODO: currently pg_map doesn't keep track of the typemods, so we
		 * cannot detect numerics in the map. See
		 * https://github.com/CrunchyData/priv-crunchy-extensions/issues/38.
		 */
		ErrorIfTypeUnsupportedForIcebergTablesInternal(keysAttribute->atttypid, keysAttribute->atttypmod, level + 1, columnName);
		ErrorIfTypeUnsupportedForIcebergTablesInternal(valuesAttribute->atttypid, valuesAttribute->atttypmod, level + 1, columnName);
	}
}

/*
 * ErrorIfTypeUnsupportedNumericForIcebergTables throws an error if the given
 * numeric type is unsupported for Iceberg tables.
 */
void
ErrorIfTypeUnsupportedNumericForIcebergTables(int32 typmod, char *columnName)
{
	/* check precision */
	int			precision = -1;
	int			scale = -1;

	GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(typmod, &precision, &scale);

	if (precision > DUCKDB_MAX_NUMERIC_PRECISION)
	{
		/*
		 * we don't expect columnName to be null, but let's be defensive
		 * anyway..
		 */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("numeric types with precision > %d are not supported on Iceberg tables", DUCKDB_MAX_NUMERIC_PRECISION),
						errdetail("column name triggering the error: \"%s\", precision: %d", columnName ? columnName : "null", precision)));
	}

	if (scale > DUCKDB_MAX_NUMERIC_SCALE)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("numeric types with scale > %d are not supported on Iceberg tables", DUCKDB_MAX_NUMERIC_SCALE),
						errdetail("column name triggering the error: \"%s\", scale: %d", columnName ? columnName : "null", scale)));
	}
}

/*
 * ErrorIfNotInManagedStorageRegion throws an error if the given location is not in
 * the managed storage region.
 */
static void
ErrorIfNotInManagedStorageRegion(char *location)
{
	char	   *managedStorageRegion = GetManagedStorageRegion();

	/* if no managed storage region is specified, allow all regions */
	if (managedStorageRegion == NULL || *managedStorageRegion == '\0')
		return;

	char	   *bucketRegion = GetBucketRegion(location);

	/* if no bucket region can be detected, allow */
	if (bucketRegion == NULL || *bucketRegion == '\0')
		return;

	/* finally, check whether the bucket is in the managed storage region */
	if (strcmp(managedStorageRegion, bucketRegion) == 0)
		return;

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("can only create Iceberg tables in region %s", managedStorageRegion),
					errdetail("bucket is in region %s", bucketRegion)));
}
