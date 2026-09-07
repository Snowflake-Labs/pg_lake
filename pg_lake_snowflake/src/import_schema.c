/*
 * Copyright 2026 Snowflake Inc.
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
 * import_schema.c
 *
 * Attaching a Snowflake table without spelling out its columns, the way an
 * external Iceberg table is attached in pg_lake:
 *
 *   CREATE FOREIGN TABLE orders () SERVER sf OPTIONS (table_name 'ORDERS');
 *   IMPORT FOREIGN SCHEMA "PUBLIC" FROM SERVER sf INTO sf_public;
 *
 * Both go through the same column inference, which asks Snowflake to describe
 * the result of "SELECT * FROM <table> WHERE 1 = 0". Reading the shape of a
 * query rather than the catalog means one code path covers standard tables,
 * hybrid tables, dynamic tables, Iceberg tables and views, and that the types
 * arrive in exactly the form the scans will see them in.
 */

#include "postgres.h"
#include "miscadmin.h"

#include "catalog/namespace.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "parser/parse_func.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"

#include "pg_lake_snowflake/import_schema.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"
#include "pg_lake_snowflake/sql_api.h"
#include "pg_lake_snowflake/type_map.h"

/* SnowflakeAttachedColumn is one column of a table being attached */
typedef struct SnowflakeAttachedColumn
{
	char	   *columnName;		/* the Postgres name */
	char	   *snowflakeName;	/* the name as Snowflake reports it */
	bool		needsNameOption;
	Oid			typeId;
	int32		typeMod;
}			SnowflakeAttachedColumn;

static ProcessUtility_hook_type PreviousProcessUtilityHook = NULL;

static void SnowflakeProcessUtility(PlannedStmt *plannedStatement,
									const char *queryString, bool readOnlyTree,
									ProcessUtilityContext context, ParamListInfo params,
									QueryEnvironment *queryEnvironment,
									DestReceiver *destination,
									QueryCompletion *completion);
static void AddInferredColumns(CreateForeignTableStmt *createStatement);
static List *DescribeSnowflakeColumns(SnowflakeConnection * connection,
									  SnowflakeTable * table);
static List *ListSnowflakeTables(SnowflakeConnection * connection,
								 const char *quotedDatabase, const char *schemaName,
								 bool schemaNameFromOption);
static void ErrorIfSchemaIsMissing(SnowflakeConnection * connection,
								   const char *quotedDatabase, const char *schemaName,
								   bool schemaNameFromOption);
static char *QuotedDatabaseName(SnowflakeConnection * connection, const char *database);
static char *PostgresNameForSnowflakeName(const char *snowflakeName,
										  bool *needsNameOption);
static bool IsPlainUpperCaseIdentifier(const char *identifier);
static char *SnowflakeStringLiteral(const char *value);


/*
 * SnowflakeInstallUtilityHook installs the utility hook that fills in the
 * columns of a foreign table created without any.
 */
void
SnowflakeInstallUtilityHook(void)
{
	PreviousProcessUtilityHook = ProcessUtility_hook;
	ProcessUtility_hook = SnowflakeProcessUtility;
}


/*
 * SnowflakeProcessUtility infers the columns of a Snowflake foreign table that
 * is created without a column list.
 */
static void
SnowflakeProcessUtility(PlannedStmt *plannedStatement, const char *queryString,
						bool readOnlyTree, ProcessUtilityContext context,
						ParamListInfo params, QueryEnvironment *queryEnvironment,
						DestReceiver *destination, QueryCompletion *completion)
{
	Node	   *parseTree = plannedStatement->utilityStmt;

	if (parseTree != NULL && IsA(parseTree, CreateForeignTableStmt))
	{
		CreateForeignTableStmt *createStatement = (CreateForeignTableStmt *) parseTree;

		if (createStatement->base.tableElts == NIL &&
			createStatement->base.inhRelations == NIL &&
			createStatement->servername != NULL)
		{
			ForeignServer *server = GetForeignServerByName(createStatement->servername,
														   true);

			if (server != NULL && IsSnowflakeForeignServer(server->serverid))
			{
				/*
				 * The tree may be a cached one that we are not allowed to
				 * change, so it is copied before the columns are added.
				 */
				if (readOnlyTree)
				{
					plannedStatement = copyObject(plannedStatement);
					createStatement =
						(CreateForeignTableStmt *) plannedStatement->utilityStmt;
					readOnlyTree = false;
				}

				AddInferredColumns(createStatement);
			}
		}
	}

	if (PreviousProcessUtilityHook != NULL)
	{
		PreviousProcessUtilityHook(plannedStatement, queryString, readOnlyTree, context,
								   params, queryEnvironment, destination, completion);
	}
	else
	{
		standard_ProcessUtility(plannedStatement, queryString, readOnlyTree, context,
								params, queryEnvironment, destination, completion);
	}
}


/*
 * IsSnowflakeForeignServer returns whether a foreign server is served by this
 * extension, which is decided by the handler rather than by the name of the
 * wrapper.
 */
bool
IsSnowflakeForeignServer(Oid serverId)
{
	static Oid	snowflakeHandlerId = InvalidOid;

	if (!OidIsValid(snowflakeHandlerId))
	{
		List	   *handlerName = list_make1(makeString("pg_lake_snowflake_handler"));

		snowflakeHandlerId = LookupFuncName(handlerName, 0, NULL, true);

		if (!OidIsValid(snowflakeHandlerId))
			return false;
	}

	ForeignServer *server = GetForeignServerExtended(serverId, FSV_MISSING_OK);

	if (server == NULL)
		return false;

	ForeignDataWrapper *wrapper = GetForeignDataWrapper(server->fdwid);

	return wrapper->fdwhandler == snowflakeHandlerId;
}


/*
 * AddInferredColumns replaces the empty column list of a CREATE FOREIGN TABLE
 * with the columns of the Snowflake table it points at.
 */
static void
AddInferredColumns(CreateForeignTableStmt *createStatement)
{
	ForeignServer *server = GetForeignServerByName(createStatement->servername, false);
	SnowflakeConnection *connection = GetSnowflakeConnection(server->serverid,
															 GetUserId());
	char	   *database = NULL;
	char	   *schemaName = NULL;
	char	   *tableName = NULL;
	ListCell   *optionCell = NULL;

	foreach(optionCell, createStatement->options)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, "database") == 0)
			database = defGetString(option);
		else if (strcmp(option->defname, "schema_name") == 0)
			schemaName = defGetString(option);
		else if (strcmp(option->defname, "table_name") == 0)
			tableName = defGetString(option);
	}

	bool		schemaFromOption = schemaName != NULL;

	if (schemaName == NULL && connection->schemaName == NULL)
	{
		/*
		 * The relation does not exist yet, so the Postgres schema a scan
		 * would read from the catalog is resolved the same way the create is
		 * about to resolve it.
		 */
		Oid			namespaceId =
			RangeVarGetCreationNamespace(createStatement->base.relation);

		schemaName = get_namespace_name(namespaceId);
	}

	SnowflakeTable *table = MakeSnowflakeTable(connection,
											   database, database != NULL,
											   schemaName, schemaFromOption,
											   tableName != NULL ? tableName :
											   createStatement->base.relation->relname,
											   tableName != NULL);

	createStatement->base.tableElts = SnowflakeInferColumnDefinitions(connection, table);
}


/*
 * SnowflakeInferColumnDefinitions returns the columns of a Snowflake table as
 * Postgres column definitions.
 */
List *
SnowflakeInferColumnDefinitions(SnowflakeConnection * connection, SnowflakeTable * table)
{
	List	   *columns = DescribeSnowflakeColumns(connection, table);
	List	   *columnDefinitions = NIL;
	ListCell   *columnCell = NULL;

	foreach(columnCell, columns)
	{
		SnowflakeAttachedColumn *column = (SnowflakeAttachedColumn *) lfirst(columnCell);
		ColumnDef  *columnDefinition = makeColumnDef(column->columnName, column->typeId,
													 column->typeMod, InvalidOid);

		if (column->needsNameOption)
		{
			columnDefinition->fdwoptions =
				list_make1(makeDefElem("column_name",
									   (Node *) makeString(column->snowflakeName), -1));
		}

		columnDefinitions = lappend(columnDefinitions, columnDefinition);
	}

	return columnDefinitions;
}


/*
 * DescribeSnowflakeColumns asks Snowflake for the shape of a table.
 *
 * A column keeps its Snowflake name in an option unless the Postgres name folds
 * back to it, which keeps the catalog readable in the ordinary all-upper-case
 * case and correct in every other one.
 */
static List *
DescribeSnowflakeColumns(SnowflakeConnection * connection, SnowflakeTable * table)
{
	char	   *qualifiedTableName = SnowflakeQualifiedTableName(table);
	char	   *describeStatement = psprintf("SELECT * FROM %s WHERE 1 = 0",
											 qualifiedTableName);
	SnowflakeStatement *statement = SnowflakeExecute(connection, describeStatement);
	List	   *columns = NIL;

	PG_TRY();
	{
		if (statement->columnCount == 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_TABLE_NOT_FOUND),
					 errmsg("Snowflake table %s has no columns", qualifiedTableName)));
		}

		for (int columnIndex = 0; columnIndex < statement->columnCount; columnIndex++)
		{
			SnowflakeResultColumn *resultColumn = &statement->columns[columnIndex];
			SnowflakeAttachedColumn *column = palloc0(sizeof(SnowflakeAttachedColumn));

			column->snowflakeName = pstrdup(resultColumn->name);
			column->columnName = PostgresNameForSnowflakeName(resultColumn->name,
															  &column->needsNameOption);

			SnowflakeColumnPostgresType(resultColumn, &column->typeId, &column->typeMod);

			columns = lappend(columns, column);
		}
	}
	PG_FINALLY();
	{
		SnowflakeStatementClose(statement);
	}
	PG_END_TRY();

	return columns;
}


/*
 * SnowflakeImportForeignSchema returns the CREATE FOREIGN TABLE commands for the
 * tables and views of a Snowflake schema.
 */
List *
SnowflakeImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverId)
{
	SnowflakeConnection *connection = GetSnowflakeConnection(serverId, GetUserId());

	if (stmt->options != NIL)
	{
		DefElem    *option = (DefElem *) linitial(stmt->options);

		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
				 errmsg("invalid option \"%s\"", option->defname),
				 errdetail("IMPORT FOREIGN SCHEMA from Snowflake takes no options.")));
	}

	/*
	 * The remote schema may be written as "DB.SCHEMA", which is how a server
	 * without a database option reaches more than one database.
	 */
	char	   *remoteSchema = stmt->remote_schema;
	char	   *database = NULL;
	char	   *separator = strchr(remoteSchema, '.');

	if (separator != NULL)
	{
		database = pnstrdup(remoteSchema, separator - remoteSchema);
		remoteSchema = separator + 1;
	}

	char	   *quotedDatabase = QuotedDatabaseName(connection, database);
	List	   *snowflakeTableNames = ListSnowflakeTables(connection, quotedDatabase,
														  remoteSchema, false);
	List	   *commands = NIL;
	ListCell   *tableNameCell = NULL;

	foreach(tableNameCell, snowflakeTableNames)
	{
		char	   *snowflakeTableName = (char *) lfirst(tableNameCell);
		bool		needsNameOption = false;
		char	   *localTableName = PostgresNameForSnowflakeName(snowflakeTableName,
																  &needsNameOption);

		if (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO ||
			stmt->list_type == FDW_IMPORT_SCHEMA_EXCEPT)
		{
			ListCell   *listedTableCell = NULL;
			bool		isListed = false;

			foreach(listedTableCell, stmt->table_list)
			{
				RangeVar   *listedTable = (RangeVar *) lfirst(listedTableCell);

				if (strcmp(listedTable->relname, localTableName) == 0)
				{
					isListed = true;
					break;
				}
			}

			if (isListed != (stmt->list_type == FDW_IMPORT_SCHEMA_LIMIT_TO))
				continue;
		}

		SnowflakeTable *table = MakeSnowflakeTable(connection,
												   database, database != NULL,
												   remoteSchema, false,
												   snowflakeTableName, true);
		List	   *columns = DescribeSnowflakeColumns(connection, table);
		StringInfo	command = makeStringInfo();
		ListCell   *columnCell = NULL;
		bool		isFirstColumn = true;

		appendStringInfo(command, "CREATE FOREIGN TABLE %s.%s (",
						 quote_identifier(stmt->local_schema),
						 quote_identifier(localTableName));

		foreach(columnCell, columns)
		{
			SnowflakeAttachedColumn *column =
				(SnowflakeAttachedColumn *) lfirst(columnCell);

			appendStringInfo(command, "%s\n  %s %s",
							 isFirstColumn ? "" : ",",
							 quote_identifier(column->columnName),
							 format_type_extended(column->typeId, column->typeMod,
												  FORMAT_TYPE_TYPEMOD_GIVEN |
												  FORMAT_TYPE_FORCE_QUALIFY));
			isFirstColumn = false;

			if (column->needsNameOption)
			{
				appendStringInfo(command, " OPTIONS (column_name %s)",
								 SnowflakeStringLiteral(column->snowflakeName));
			}
		}

		appendStringInfo(command, "\n) SERVER %s OPTIONS (",
						 quote_identifier(stmt->server_name));

		if (database != NULL)
		{
			appendStringInfo(command, "database %s, ",
							 SnowflakeStringLiteral(SnowflakeIdentifierName(database,
																			true)));
		}

		appendStringInfo(command, "schema_name %s, table_name %s)",
						 SnowflakeStringLiteral(SnowflakeIdentifierName(remoteSchema,
																		false)),
						 SnowflakeStringLiteral(snowflakeTableName));

		commands = lappend(commands, command->data);
	}

	return commands;
}


/*
 * QuotedDatabaseName returns the quoted Snowflake database a schema is looked up
 * in, falling back to the database of the server.
 */
static char *
QuotedDatabaseName(SnowflakeConnection * connection, const char *database)
{
	/*
	 * A database named in IMPORT FOREIGN SCHEMA arrives folded by the
	 * Postgres parser, so it is treated like any other catalog name; one from
	 * a server option was written by hand and is used as spelled.
	 */
	if (database != NULL)
		return SnowflakeQuoteIdentifier(database, false);

	if (connection->database == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("no Snowflake database to import from"),
				 errhint("Set a \"database\" option on server \"%s\", or name the "
						 "schema as \"database.schema\".", connection->serverName)));
	}

	return SnowflakeQuoteIdentifier(connection->database, true);
}


/*
 * ListSnowflakeTables returns the names of the tables and views of a Snowflake
 * schema. Hybrid, dynamic and Iceberg tables are all BASE TABLE here, which is
 * why the type is not filtered any further.
 */
static List *
ListSnowflakeTables(SnowflakeConnection * connection, const char *quotedDatabase,
					const char *schemaName, bool schemaNameFromOption)
{
	char	   *listStatement =
		psprintf("SELECT table_name FROM %s.INFORMATION_SCHEMA.TABLES "
				 "WHERE table_schema = %s AND table_type IN ('BASE TABLE', 'VIEW') "
				 "ORDER BY table_name",
				 quotedDatabase,
				 SnowflakeStringLiteral(SnowflakeIdentifierName(schemaName,
																schemaNameFromOption)));
	SnowflakeStatement *statement = SnowflakeExecute(connection, listStatement);
	List	   *tableNames = NIL;

	PG_TRY();
	{
		while (SnowflakeStatementNextRow(statement))
		{
			char	   *tableName = SnowflakeStatementGetValue(statement, 0);

			if (tableName != NULL)
				tableNames = lappend(tableNames, tableName);
		}
	}
	PG_FINALLY();
	{
		SnowflakeStatementClose(statement);
	}
	PG_END_TRY();

	if (tableNames == NIL)
	{
		/*
		 * An empty result is either an empty schema or a schema name that
		 * does not exist, and the second one is what a case mistake looks
		 * like.
		 */
		ErrorIfSchemaIsMissing(connection, quotedDatabase, schemaName,
							   schemaNameFromOption);

		ereport(NOTICE,
				(errmsg("Snowflake schema %s contains no tables or views",
						SnowflakeIdentifierName(schemaName, schemaNameFromOption))));
	}

	return tableNames;
}


/*
 * ErrorIfSchemaIsMissing reports a schema that Snowflake does not have, rather
 * than letting it look like an empty one.
 */
static void
ErrorIfSchemaIsMissing(SnowflakeConnection * connection, const char *quotedDatabase,
					   const char *schemaName, bool schemaNameFromOption)
{
	char	   *resolvedSchemaName = SnowflakeIdentifierName(schemaName,
															 schemaNameFromOption);
	char	   *existsStatement =
		psprintf("SELECT COUNT(*) FROM %s.INFORMATION_SCHEMA.SCHEMATA "
				 "WHERE schema_name = %s",
				 quotedDatabase, SnowflakeStringLiteral(resolvedSchemaName));
	char	   *schemaCount = SnowflakeExecuteScalar(connection, existsStatement);

	if (schemaCount != NULL && strcmp(schemaCount, "0") == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
				 errmsg("schema \"%s\" does not exist in Snowflake database %s",
						resolvedSchemaName, quotedDatabase),
				 errhint("Snowflake names are case sensitive and an unquoted name is "
						 "upper-cased. Write IMPORT FOREIGN SCHEMA \"%s\" to keep a "
						 "name as it is.", schemaName)));
	}
}


/*
 * PostgresNameForSnowflakeName returns the Postgres name for a Snowflake name,
 * and whether the Snowflake spelling has to be kept in an option because the
 * Postgres name does not fold back to it.
 */
static char *
PostgresNameForSnowflakeName(const char *snowflakeName, bool *needsNameOption)
{
	if (IsPlainUpperCaseIdentifier(snowflakeName))
	{
		*needsNameOption = false;

		return str_tolower(snowflakeName, strlen(snowflakeName), DEFAULT_COLLATION_OID);
	}

	*needsNameOption = true;

	return pstrdup(snowflakeName);
}


/*
 * IsPlainUpperCaseIdentifier returns whether a name is what Snowflake produces
 * by folding an unquoted identifier, so that lower-casing it is reversible.
 */
static bool
IsPlainUpperCaseIdentifier(const char *identifier)
{
	if (identifier == NULL || identifier[0] == '\0')
		return false;

	if (!(identifier[0] >= 'A' && identifier[0] <= 'Z') && identifier[0] != '_')
		return false;

	for (const char *cursor = identifier; *cursor != '\0'; cursor++)
	{
		bool		isUpper = (*cursor >= 'A' && *cursor <= 'Z');
		bool		isDigit = (*cursor >= '0' && *cursor <= '9');

		if (!isUpper && !isDigit && *cursor != '_' && *cursor != '$')
			return false;
	}

	return true;
}


/*
 * SnowflakeStringLiteral quotes a string for a statement we generate, whether it
 * goes to Snowflake or to Postgres. Doubling a quote is understood by both.
 */
static char *
SnowflakeStringLiteral(const char *value)
{
	StringInfo	literal = makeStringInfo();

	appendStringInfoChar(literal, '\'');

	for (const char *cursor = value; *cursor != '\0'; cursor++)
	{
		if (*cursor == '\'')
			appendStringInfoChar(literal, '\'');

		appendStringInfoChar(literal, *cursor);
	}

	appendStringInfoChar(literal, '\'');

	return literal->data;
}
