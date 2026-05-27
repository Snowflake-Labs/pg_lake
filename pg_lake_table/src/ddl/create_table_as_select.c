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
#include "pg_lake/util/table_type.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/planner/dbt.h"


static bool IsCreateAsSelectPgLakeForeignTable(CreateTableAsStmt *createAsStmt,
											   PgLakeTableType * tableType);
static void EnsureCreateAsSelectPgLakeTableSupported(CreateTableAsStmt *createAsStmt,
													 PgLakeTableType tableType);
static CreateForeignTableStmt *GetCreatePgLakeForeignTableStmtFromCreateAsSelect(
																				 CreateTableAsStmt *createAsStmt,
																				 PgLakeTableType tableType);
static uint64 InsertIntoPgLakeForeignTable(Query *selectQuery, char *qualifiedTableName);


/*
 * ProcessCreateAsSelectPgLakeTable is a utility statement handler for handling
 * CREATE TABLE ... AS SELECT statements that target an iceberg or ducklake
 * table. Both end up as foreign tables internally, so we convert the
 * "CREATE TABLE ... USING <am> AS SELECT" statement to a
 * "CREATE FOREIGN TABLE SERVER <server>" + "INSERT SELECT" pair.
 *
 * Currently this code handles:
 * - CREATE TABLE name USING pg_lake_iceberg WITH (...) AS SELECT|TABLE|VALUES ...
 * - CREATE TABLE name USING ducklake          WITH (...) AS SELECT|TABLE|VALUES ...
 */
bool
ProcessCreateAsSelectPgLakeTable(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;
	PgLakeTableType tableType = PG_LAKE_INVALID_TABLE_TYPE;

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
			PgLakeTableType explainTableType;

			if (IsCreateAsSelectPgLakeForeignTable(createAsStmt, &explainTableType))
			{
				const char *am = (explainTableType == PG_LAKE_ICEBERG_TABLE_TYPE)
					? "pg_lake_iceberg" : "ducklake";

				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("EXPLAIN ANALYZE CREATE TABLE .. USING "
									   "%s AS SELECT .. statements are "
									   "currently not supported", am)));
			}
		}

		return false;
	}

	if (!IsA(plannedStmt->utilityStmt, CreateTableAsStmt))
	{
		return false;
	}

	CreateTableAsStmt *createAsStmt = (CreateTableAsStmt *) plannedStmt->utilityStmt;

	if (!IsCreateAsSelectPgLakeForeignTable(createAsStmt, &tableType))
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
	 * ourselves if we need to.
	 *
	 * However, if the extension explicitly specifies USING iceberg or USING
	 * ducklake, we allow the table creation.
	 */
	if (creating_extension)
	{
		bool		amIsLake = (createAsStmt->into->accessMethod != NULL) &&
			(IsPgLakeIcebergAccessMethod(createAsStmt->into->accessMethod) ||
			 IsPgLakeDucklakeAccessMethod(createAsStmt->into->accessMethod));
		bool		defaultIsLake = IsPgLakeIcebergAccessMethod(default_table_access_method) ||
			IsPgLakeDucklakeAccessMethod(default_table_access_method);

		if (createAsStmt->into->accessMethod == NULL && defaultIsLake)
		{
			createAsStmt->into->accessMethod = DEFAULT_TABLE_ACCESS_METHOD;
			return false;
		}
		else if (createAsStmt->into->accessMethod != NULL && !amIsLake)
		{
			return false;
		}
	}

	EnsureCreateAsSelectPgLakeTableSupported(createAsStmt, tableType);

	char	   *tableName = createAsStmt->into->rel->relname;
	char	   *schemaName = createAsStmt->into->rel->schemaname;

	if (schemaName == NULL)
	{
		/* fix the schema name to be robust to search_path changes */
		Oid			namespaceId =
			RangeVarGetAndCheckCreationNamespace(createAsStmt->into->rel, NoLock, NULL);

		schemaName = get_namespace_name(namespaceId);
	}

	CreateForeignTableStmt *foreignTableStmt =
		GetCreatePgLakeForeignTableStmtFromCreateAsSelect(createAsStmt, tableType);

	params->plannedStmt->utilityStmt = (Node *) foreignTableStmt;

	/*
	 * Run the other handlers and the internal ProcessUtility. We will not
	 * re-enter this path since the statement is now CREATE FOREIGN TABLE.
	 */
	PgLakeCommonProcessUtility(params);

	if (!createAsStmt->into->skipData)
	{
		Query	   *selectQuery = (Query *) createAsStmt->query;

		char	   *qualifiedTableName = quote_qualified_identifier(schemaName, tableName);
		uint64		rowCount = InsertIntoPgLakeForeignTable(selectQuery, qualifiedTableName);

		if (params->completionTag)
			SetQueryCompletion(params->completionTag, CMDTAG_SELECT, rowCount);
	}

	/* signal that we already ran the ProcessUtility */
	return true;
}

/*
 * InsertIntoPgLakeForeignTable inserts the result of a SELECT statement into
 * the given iceberg or ducklake foreign table.
 */
static uint64
InsertIntoPgLakeForeignTable(Query *selectQuery, char *qualifiedTableName)
{
	char	   *selectQueryStr = pg_get_querydef(selectQuery, false);

	StringInfo	insertSelectSql = makeStringInfo();

	appendStringInfo(insertSelectSql, "INSERT INTO %s %s",
					 qualifiedTableName,
					 selectQueryStr);

	SPI_START();

	SPI_exec(insertSelectSql->data, 0);

	uint64		rowCount = SPI_processed;

	SPI_END();

	return rowCount;
}


/*
 * IsCreateAsSelectPgLakeForeignTable returns true when the CREATE TABLE AS
 * SELECT statement targets an iceberg or ducklake table, and writes the
 * concrete table type back through *tableType.
 */
static bool
IsCreateAsSelectPgLakeForeignTable(CreateTableAsStmt *createAsStmt,
								   PgLakeTableType * tableType)
{
	char	   *accessMethod = createAsStmt->into->accessMethod;
	PgLakeTableType resolved = GetPgLakeTableTypeViaAccessMethod(accessMethod);

	if (resolved == PG_LAKE_ICEBERG_TABLE_TYPE ||
		resolved == PG_LAKE_DUCKLAKE_TABLE_TYPE)
	{
		*tableType = resolved;
		return true;
	}

	*tableType = PG_LAKE_INVALID_TABLE_TYPE;
	return false;
}


/*
 * EnsureCreateAsSelectPgLakeTableSupported checks whether the given CREATE
 * TABLE ... USING (iceberg|ducklake) AS SELECT statement contains any query
 * parts that aren't valid for foreign tables. We need to check this here
 * because Postgres does not check these parts when converting the statement
 * to a foreign table.
 */
static void
EnsureCreateAsSelectPgLakeTableSupported(CreateTableAsStmt *createAsStmt,
										 PgLakeTableType tableType)
{
	const char *amLabel = (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
		? "pg_lake_iceberg" : "ducklake";

	if (createAsStmt->objtype == OBJECT_MATVIEW)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: materialized views are not allowed "
							   "as %s tables", amLabel)));
	}

	if (createAsStmt->into->rel->relpersistence == RELPERSISTENCE_TEMP)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: temporary tables are not allowed "
							   "as %s tables", amLabel)));
	}

	if (createAsStmt->into->rel->relpersistence == RELPERSISTENCE_UNLOGGED)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: unlogged tables are not allowed "
							   "as %s tables", amLabel)));
	}

	Query	   *selectQuery = (Query *) createAsStmt->query;

	if (selectQuery->utilityStmt != NULL)
	{
		/* set when SELECT AS EXECUTE */
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: CREATE TABLE AS EXECUTE is not supported "
							   "for %s tables", amLabel)));
	}

	if (createAsStmt->into->tableSpaceName != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_table: tablespace is not supported "
							   "with %s tables", amLabel)));
	}

	return;
}


/*
 * GetCreatePgLakeForeignTableStmtFromCreateAsSelect converts the given
 * "CREATE TABLE USING (iceberg|ducklake) AS SELECT" statement into a
 * "CREATE FOREIGN TABLE SERVER <pg_lake_iceberg|pg_lake_ducklake>" statement.
 */
static CreateForeignTableStmt *
GetCreatePgLakeForeignTableStmtFromCreateAsSelect(CreateTableAsStmt *createAsStmt,
												  PgLakeTableType tableType)
{
	/* set when AS SELECT, AS TABLE and AS VALUES */
	Query	   *selectQuery = (Query *) createAsStmt->query;
	List	   *targetList = selectQuery->targetList;
	bool		ifNotExists = createAsStmt->if_not_exists;
	RangeVar   *rel = createAsStmt->into->rel;
	List	   *options = createAsStmt->into->options;

	TupleDesc	tupledesc = BuildTupleDescriptorForTargetList(targetList);
	List	   *columnDefs = BuildColumnDefListForTupleDesc(tupledesc);

	CreateForeignTableStmt *foreignTableStmt = makeNode(CreateForeignTableStmt);

	foreignTableStmt->base.accessMethod = NULL;
	foreignTableStmt->base.if_not_exists = ifNotExists;
	foreignTableStmt->base.relation = rel;
	foreignTableStmt->base.tableElts = columnDefs;
	foreignTableStmt->servername = (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
		? PG_LAKE_ICEBERG_SERVER_NAME
		: PG_LAKE_DUCKLAKE_SERVER_NAME;
	foreignTableStmt->options = options;

	return foreignTableStmt;
}
