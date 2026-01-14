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
#include "catalog/namespace.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/copy/copy.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/copy/create_table.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/describe/describe.h"
#include "pg_lake/parsetree/columns.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/util/rel_utils.h"
#include "parser/parse_utilcmd.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


#define DEFINITION_FROM_OPTION_NAME "definition_from"
#define LOAD_FROM_OPTION_NAME "load_from"

static bool IsCreateFromFile(CreateStmt *createStmt);
static void ProcessCreateFromFile(CreateStmt *createStmt,
								  ProcessUtilityParams * params);
static void ErrorIfTableExists(CreateStmt *createStmt);
static List *ExtractDataImportOptions(CopyDataFormat format, List *options);


/*
 * CreateTableFromFileHandler is a utility statement handler for handling
 * statements of the form:
 * - CREATE TABLE name () WITH (load_from = 's3://...')
 * - CREATE TABLE name () WITH (definition_from = 's3://...')
 */
bool
CreateTableFromFileHandler(ProcessUtilityParams * params, void *arg)
{
	PlannedStmt *plannedStmt = params->plannedStmt;

	if (!IsA(plannedStmt->utilityStmt, CreateStmt))
	{
		return false;
	}

	CreateStmt *createStmt = (CreateStmt *) plannedStmt->utilityStmt;

	if (!IsCreateFromFile(createStmt))
	{
		return false;
	}

	if (params->readOnlyTree)
		createStmt = (CreateStmt *) CopyUtilityStmt(params);

	ProcessCreateFromFile(createStmt, params);
	return true;
}


/*
 * IsCreateFromFile returns whether the given CREATE TABLE statement
 * should be handled by ProcessCreateFromFile.
 */
static bool
IsCreateFromFile(CreateStmt *createStmt)
{
	if (!EnablePgLakeCopy)
	{
		/* disabled by the administrator */
		return false;
	}

	if (HasOption(createStmt->options, DEFINITION_FROM_OPTION_NAME))
	{
		/* definition_from option specified */
		return true;
	}

	if (HasOption(createStmt->options, LOAD_FROM_OPTION_NAME))
	{
		/* load_from specified, but no columns: infer the schema */
		return true;
	}

	return false;
}


/*
 * ProcessCreateFromFile handles CREATE TABLE .. () WITH
 * (definition_from='url') and WITH (load_from='url') statements by adding
 * column definitions that match the file at the URL and loading the data (in
 * case of load_from).
 */
static void
ProcessCreateFromFile(CreateStmt *createStmt,
					  ProcessUtilityParams * utilityParams)
{
	char	   *definitionFromURL = NULL;
	char	   *loadFromURL = NULL;
	CopyDataFormat format = DATA_FORMAT_INVALID;
	CopyDataCompression compression = DATA_COMPRESSION_INVALID;
	List	   *copyOptions = NIL;

	/*
	 * We cannot determine the table type in this command, but we also do not
	 * need. You cannot load from an iceberg table, so we can ignore this.
	 */
	PgLakeTableType tableType = PG_LAKE_INVALID_TABLE_TYPE;

	List	   *options = createStmt->options;
	DefElem    *definitionFromOption = GetOption(options, DEFINITION_FROM_OPTION_NAME);
	DefElem    *loadFromOption = GetOption(options, LOAD_FROM_OPTION_NAME);

	Oid			namespaceId =
		RangeVarGetAndCheckCreationNamespace(createStmt->relation, NoLock, NULL);

	if (createStmt->relation->schemaname == NULL)
	{
		/* fix the schema name to be robust to search_path changes */
		createStmt->relation->schemaname = get_namespace_name(namespaceId);
	}

	ErrorIfTableExists(createStmt);

	if (definitionFromOption != NULL && loadFromOption != NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_copy: cannot specify both " LOAD_FROM_OPTION_NAME
							   " and " DEFINITION_FROM_OPTION_NAME " table options")));
	}
	else if (definitionFromOption != NULL)
	{
		if (createStmt->partbound)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: cannot infer the column definitions "
								   "from a URL when creating partitions")));
		}

		if (createStmt->tableElts != NIL)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: cannot infer the column definitions "
								   "from a URL when specifying columns")));
		}

		definitionFromURL = defGetString(definitionFromOption);

		if (!IsSupportedURL(definitionFromURL))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: only s3:// and gs:// URLs are "
								   "currently supported")));
		}

		/* determine format & compression to make DESCRIBE more precise */
		FindDataFormatAndCompression(tableType, definitionFromURL, options, &format, &compression);

		/*
		 * Remove the definition_from option, since regular CREATE TABLE would
		 * processing would error if we kept it.
		 */
		createStmt->options = list_delete(createStmt->options, definitionFromOption);
	}
	else if (loadFromOption != NULL)
	{
		loadFromURL = defGetString(loadFromOption);

		if (!IsSupportedURL(loadFromURL))
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("pg_lake_copy: only s3:// and gs:// URLs are "
								   "currently supported")));
		}

		/* determine format & compression to make DESCRIBE more precise */
		FindDataFormatAndCompression(tableType, loadFromURL, options, &format, &compression);

		/*
		 * Remove the load_from option, since regular CREATE TABLE would
		 * processing would error if we kept it.
		 */
		createStmt->options = list_delete(createStmt->options, loadFromOption);

		if (createStmt->tableElts == NIL && !createStmt->partbound)
		{
			Assert(definitionFromURL == NULL);

			/*
			 * When no columns and no infer schema option are specified, we
			 * infer the schema from load_from.
			 */
			definitionFromURL = loadFromURL;
		}
	}
	else
	{
		/* should have at least one option */
		Assert(false);
	}

	/* permission checks */
	CheckURLReadAccess();

	/*
	 * Extract the options we need for DescribeColumnsForURL and COPY.
	 */
	copyOptions = ExtractDataImportOptions(format, createStmt->options);

	/*
	 * Remove custom options, otherwise regular CREATE TABLE processing would
	 * error.
	 */
	createStmt->options = list_difference(createStmt->options, copyOptions);

	/* now do actual work */

	if (definitionFromURL != NULL)
	{
		/* set the new column definition list */
		List	   *dataFileColumns = DescribeColumnsForURL(definitionFromURL,
															format, compression,
															copyOptions);

		createStmt->tableElts = dataFileColumns;
	}

	/* continue with regular process utility to handle CREATE TABLE */
	PgLakeCommonProcessUtility(utilityParams);

	if (loadFromURL != NULL)
	{
		/* load the data via our custom COPY implementation */
		CopyStmt   *copyStmt = makeNode(CopyStmt);

		copyStmt->relation = createStmt->relation;
		copyStmt->is_from = true;
		copyStmt->filename = loadFromURL;

		if (format == DATA_FORMAT_CSV)
		{
			/*
			 * Add auto_detect by default for CSV (different from COPY).
			 * Without it, we would have to specify a columns map and we have
			 * no idea.
			 */
			DefElem    *autoDetectOption =
				makeDefElem("auto_detect", (Node *) makeBoolean(true), -1);

			copyOptions = lappend(copyOptions, autoDetectOption);
		}

		copyStmt->options = copyOptions;

		ParseState *pstate = make_parsestate(NULL);

		pstate->p_sourcetext = utilityParams->queryString;
		pstate->p_queryEnv = utilityParams->queryEnv;

		/*
		 * Construct a new PlannedStmt for the CopyStmt.
		 */
		PlannedStmt *plannedStmt = makeNode(PlannedStmt);

		plannedStmt->commandType = CMD_UTILITY;
		plannedStmt->utilityStmt = (Node *) copyStmt;

		uint64		rowsProcessed = 0;

		ProcessPgLakeCopy(pstate, plannedStmt, utilityParams->queryString,
						  &rowsProcessed);
	}
}


/*
 * ErrorIfTableExists throws an error if the table the user tries to create
 * already exists.
 *
 * We also throw an error in the IF NOT EXISTS case, because who knows what
 * the user is trying to do.
 */
static void
ErrorIfTableExists(CreateStmt *createStmt)
{
	/* do some early error checks */
	char	   *schemaName = createStmt->relation->schemaname;
	char	   *tableName = createStmt->relation->relname;

	Oid			namespaceId = get_namespace_oid(schemaName, false);
	Oid			existingRelationId = get_relname_relid(tableName, namespaceId);

	if (existingRelationId != InvalidOid)
	{
		DefElem    *loadFromOption = GetOption(createStmt->options, LOAD_FROM_OPTION_NAME);

		if (!createStmt->if_not_exists)
		{
			ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
							errmsg("relation \"%s\" already exists", tableName)));
		}
		else if (loadFromOption != NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
							errmsg("relation \"%s\" already exists", tableName),
							errdetail("CREATE TABLE IF NOT EXISTS cannot be used with "
									  "the " LOAD_FROM_OPTION_NAME " option"),
							errhint("Use COPY to load into an existing table.")));
		}
		else
		{
			ereport(ERROR, (errcode(ERRCODE_DUPLICATE_TABLE),
							errmsg("relation \"%s\" already exists", tableName),
							errdetail("CREATE TABLE IF NOT EXISTS cannot be used with "
									  "the " DEFINITION_FROM_OPTION_NAME " option")));
		}
	}
}


/*
 * ExtractDataImportOptions returns the list of options from the CREATE TABLE .. WITH (...)
 * statement to pass to the DESCRIBE and COPY FROM statements, and errors if an invalid
 * option is encountered.
 */
static List *
ExtractDataImportOptions(CopyDataFormat format, List *options)
{
	List	   *copyOptions = NULL;

	ListCell   *optionCell = NULL;

	foreach(optionCell, options)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, LOAD_FROM_OPTION_NAME) == 0)
		{
			/* extract load_from */
			continue;
		}
		else if (strcmp(option->defname, "format") == 0 ||
				 strcmp(option->defname, "compression") == 0 ||
				 strcmp(option->defname, "freeze") == 0 ||
				 strcmp(option->defname, "filename") == 0)
		{
			copyOptions = lappend(copyOptions, option);
			continue;
		}
		else if (format == DATA_FORMAT_CSV)
		{
			if (strcmp(option->defname, "header") == 0 ||
				strcmp(option->defname, "quote") == 0 ||
				strcmp(option->defname, "escape") == 0 ||
				strcmp(option->defname, "delimiter") == 0 ||
				strcmp(option->defname, "null") == 0 ||
				strcmp(option->defname, "null_padding") == 0)
			{
				copyOptions = lappend(copyOptions, option);
				continue;
			}
		}
		else if (format == DATA_FORMAT_GDAL)
		{
			if (strcmp(option->defname, "zip_path") == 0 ||
				strcmp(option->defname, "layer") == 0)
			{
				copyOptions = lappend(copyOptions, option);
				continue;
			}
		}
		else if (format == DATA_FORMAT_JSON)
		{
			if (strcmp(option->defname, "maximum_object_size") == 0)
			{
				copyOptions = lappend(copyOptions, option);
				continue;
			}
		}
	}

	return copyOptions;
}
