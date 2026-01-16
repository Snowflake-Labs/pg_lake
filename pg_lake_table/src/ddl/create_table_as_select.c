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
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/createas.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "commands/tablecmds.h"
#include "common/string.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "parser/parse_utilcmd.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/ruleutils.h"

#include "pg_lake/access_method/access_method.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/ddl/create_table.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/describe/describe.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/parsetree/columns.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/planner/dbt.h"


static bool IsCreateAsSelectIcebergTable(CreateTableAsStmt *createAsStmt);
static void EnsureCreateAsSelectIcebergTableSupported(CreateTableAsStmt *createAsStmt);
static CreateForeignTableStmt *GetCreateIcebergForeignTableStmtFromCreateAsSelect(CreateTableAsStmt *createAsStmt);
static void InsertIntoIcebergTable(Query *selectQuery, char *qualifiedTableName);


/*
 * ProcessCreateAsSelectPgLakeTable is a utility statement handler for handling
 * statements for creating iceberg tables via CREATE AS SELECT syntax. Normally,
 * iceberg tables are stored as foreign tables internally. But, for ease of use,
 * we let users to create iceberg tables using "CREATE TABLE USING pg_lake_iceberg"
 * syntax. Behind the scenes, we convert the "CREATE TABLE USING pg_lake_iceberg AS SELECT"
 * statement to "CREATE FOREIGN TABLE SERVER pg_lake_iceberg" + "INSERT SELECT" statements.
 *
 * Currently this code handles:
 * - CREATE TABLE name USING pg_lake_iceberg WITH (location = <>) AS SELECT ...
 * - CREATE TABLE name USING pg_lake_iceberg WITH (location = <>) AS TABLE ...
 * - CREATE TABLE name USING pg_lake_iceberg WITH (location = <>) AS VALUES ( ... )
 */
bool
ProcessCreateAsSelectPgLakeTable(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	/*
	 * EXPLAIN ANALYZE CREATE TABLE .. AS SELECT .. is a bit of a quirky case
	 * that bypasses our hooks and then hits the fake access method. We throw
	 * a nicer error here.
	 */
	if (IsA(plannedStmt->utilityStmt, ExplainStmt))
	{
		ExplainStmt *explainStmt = (ExplainStmt *) plannedStmt->utilityStmt;
		Query	   *query = (Query *) explainStmt->query;

		if (query->utilityStmt != NULL &&
			IsA(query->utilityStmt, CreateTableAsStmt) &&
			HasOption(explainStmt->options, "analyze"))
		{
			CreateTableAsStmt *createAsStmt = (CreateTableAsStmt *) query->utilityStmt;

			if (IsCreateAsSelectIcebergTable(createAsStmt))
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("EXPLAIN ANALYZE CREATE TABLE .. USING "
									   "pg_lake_iceberg AS SELECT .. statements are "
									   "currently not supported")));
			}
		}

		return false;
	}

	if (!IsA(plannedStmt->utilityStmt, CreateTableAsStmt))
	{
		return false;
	}

	CreateTableAsStmt *createAsStmt = (CreateTableAsStmt *) plannedStmt->utilityStmt;

	if (!IsCreateAsSelectIcebergTable(createAsStmt))
	{
		return false;
	}

	/*
	 * Check if table already exists. We need to short cut here to not insert
	 * into existing table.
	 */
	if (CreateTableAsRelExists(createAsStmt))
	{
		return true;
	}

	/* we will adjust the schema and also replace the PlannedStmt */
	if (params->readOnlyTree)
		createAsStmt = (CreateTableAsStmt *) CopyUtilityStmt(params);

	if (IsDBTTempTable(createAsStmt->into->rel))
	{
		/*
		 * replace the default access method to heap for temp tables when dbt
		 * runs to prevent errors during materiazed = incremental mode. We
		 * must replace the access method since it throws error at its
		 * am_handler as it is a placeholder access method.
		 */
		createAsStmt->into->accessMethod = DEFAULT_TABLE_ACCESS_METHOD;
		return false;
	}

	/*
	 * If we are creating an extension, we want to ensure that we don't use
	 * the user-provided default_table_access_method and just override
	 * ourselves if we need to.  This does mean you can't use CTAS in an
	 * extension script and create an iceberg table (at least using
	 * default_table_access_method), but seems better than breaking extension
	 * upgrades.
	 */
	if (creating_extension)
	{
		if (createAsStmt->into->accessMethod == NULL &&
			IsPgLakeIcebergAccessMethod(default_table_access_method))
		{
			createAsStmt->into->accessMethod = DEFAULT_TABLE_ACCESS_METHOD;
		}

		return false;
	}

	EnsureCreateAsSelectIcebergTableSupported(createAsStmt);

	char	   *tableName = createAsStmt->into->rel->relname;
	char	   *schemaName = createAsStmt->into->rel->schemaname;

	if (schemaName == NULL)
	{
		/* fix the schema name to be robust to search_path changes */
		Oid			namespaceId =
			RangeVarGetAndCheckCreationNamespace(createAsStmt->into->rel, NoLock, NULL);

		schemaName = get_namespace_name(namespaceId);
	}

	CreateForeignTableStmt *createIcebergTableStmt =
		GetCreateIcebergForeignTableStmtFromCreateAsSelect(createAsStmt);

	params->plannedStmt->utilityStmt = (Node *) createIcebergTableStmt;

	/*
	 * Run the other handlers and the internal ProcessUtility. We will not
	 * re-enter this path since the statement is now CREATE FOREIGN TABLE.
	 */
	PgLakeCommonProcessUtility(params);

	if (!createAsStmt->into->skipData)
	{
		Query	   *selectQuery = (Query *) createAsStmt->query;

		char	   *qualifiedTableName = quote_qualified_identifier(schemaName, tableName);

		InsertIntoIcebergTable(selectQuery, qualifiedTableName);
	}

	/* signal that we already ran the ProcessUtility */
	return true;
}

/*
 * InsertIntoIcebergTable inserts the result of a SELECT statement into given
 * iceberg table.
 */
static void
InsertIntoIcebergTable(Query *selectQuery, char *qualifiedTableName)
{
	char	   *selectQueryStr = pg_get_querydef(selectQuery, false);

	StringInfo	insertSelectSql = makeStringInfo();

	appendStringInfo(insertSelectSql, "INSERT INTO %s %s",
					 qualifiedTableName,
					 selectQueryStr);

	SPI_START();

	SPI_exec(insertSelectSql->data, 0);

	SPI_END();
}


/*
 * IsCreateAsSelectIcebergTable returns whether given
 * CREATE TABLE AS SELECT statement will create a pg_lake_iceberg table.
 */
static bool
IsCreateAsSelectIcebergTable(CreateTableAsStmt *createAsStmt)
{
	char	   *accessMethod = createAsStmt->into->accessMethod;
	PgLakeTableType tableType = GetPgLakeTableTypeViaAccessMethod(accessMethod);

	return tableType == PG_LAKE_ICEBERG_TABLE_TYPE;
}


/*
 * EnsureCreateAsSelectIcebergTableSupported checks whether the given
 * CREATE TABLE USING pg_lake_iceberg AS SELECT statement contains any
 * query parts that is not valid for foreign tables. We need to check
 * this here because Postgres does not check these parts when converting
 * the statement to a foreign table.
 */
static void
EnsureCreateAsSelectIcebergTableSupported(CreateTableAsStmt *createAsStmt)
{
	if (createAsStmt->objtype == OBJECT_MATVIEW)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: materialized views are not allowed "
							   "as pg_lake_iceberg tables")));
	}

	if (createAsStmt->into->rel->relpersistence == RELPERSISTENCE_TEMP)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: temporary tables are not allowed "
							   "as pg_lake_iceberg tables")));
	}

	if (createAsStmt->into->rel->relpersistence == RELPERSISTENCE_UNLOGGED)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: unlogged tables are not allowed "
							   "as pg_lake_iceberg tables")));
	}

	Query	   *selectQuery = (Query *) createAsStmt->query;

	if (selectQuery->utilityStmt != NULL)
	{
		/* set when SELECT AS EXECUTE */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: CREATE TABLE AS EXECUTE is not supported "
							   "for pg_lake_iceberg tables")));
	}

	if (createAsStmt->into->tableSpaceName != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: tablespace is not supported "
							   "with pg_lake_iceberg tables")));
	}

	return;
}


/*
 * GetCreateIcebergForeignTableStmtFromCreateAsSelect converts given
 * "CREATE TABLE USING pg_lake_iceberg AS SELECT" statement into
 * "CREATE FOREIGN TABLE SERVER pg_lake_iceberg" statement.
 */
static CreateForeignTableStmt *
GetCreateIcebergForeignTableStmtFromCreateAsSelect(CreateTableAsStmt *createAsStmt)
{
	/* set when AS SELECT, AS TABLE and AS VALUES */
	Query	   *selectQuery = (Query *) createAsStmt->query;
	List	   *targetList = selectQuery->targetList;
	bool		ifNotExists = createAsStmt->if_not_exists;
	RangeVar   *rel = createAsStmt->into->rel;
	List	   *options = createAsStmt->into->options;

	TupleDesc	tupledesc = BuildTupleDescriptorForTargetList(targetList);
	List	   *columnDefs = BuildColumnDefListForTupleDesc(tupledesc);

	CreateForeignTableStmt *icebergForeignTableStmt = makeNode(CreateForeignTableStmt);

	icebergForeignTableStmt->base.accessMethod = NULL;
	icebergForeignTableStmt->base.if_not_exists = ifNotExists;
	icebergForeignTableStmt->base.relation = rel;
	icebergForeignTableStmt->base.tableElts = columnDefs;
	icebergForeignTableStmt->servername = PG_LAKE_ICEBERG_SERVER_NAME;
	icebergForeignTableStmt->options = options;

	return icebergForeignTableStmt;
}
