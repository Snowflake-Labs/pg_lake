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

#include "access/htup_details.h"
#include "catalog/pg_foreign_table.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/lsyscache.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "commands/defrem.h"
#include "utils/acl.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/rel_utils.h"


PgLakeTableType
GetPgLakeTableTypeViaServerName(char *serverName)
{
	if (IsPgLakeIcebergServerName(serverName))
	{
		return PG_LAKE_ICEBERG_TABLE_TYPE;
	}
	else if (IsPgLakeServerName(serverName))
	{
		return PG_LAKE_TABLE_TYPE;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("unexpected state: foreign server %s is not a "
						"pg_lake table", serverName)));
	}
}



/*
* GetPgLakeForeignServerName - get the server name for the foreign table.
* Returns NULL if the foreign table is not a pg_lake table.
*/
char *
GetPgLakeForeignServerName(Oid foreignTableId)
{
	bool		isPgLakeTable = IsAnyLakeForeignTableById(foreignTableId);

	if (!isPgLakeTable)
	{
		return NULL;
	}

	ForeignTable *foreignTable = GetForeignTable(foreignTableId);
	ForeignServer *foreignServer = GetForeignServer(foreignTable->serverid);

	return foreignServer->servername;
}


/*
* IsAnyLakeForeignTableById - check if the table is a lake table.
*/
bool
IsAnyLakeForeignTableById(Oid foreignTableId)
{
	return IsPgLakeForeignTableById(foreignTableId) ||
		IsPgLakeIcebergForeignTableById(foreignTableId);
}

/*
* Similar to IsPgLakeForeignTable, by using the foreign table id.
*/
bool
IsPgLakeForeignTableById(Oid foreignTableId)
{
	bool		IsPgLakeForeignTable = false;

	/*
	 * We do not call GetForeignTable directly, since it errors for
	 * non-foreign tables.
	 */
	HeapTuple	foreignTableTup = SearchSysCache1(FOREIGNTABLEREL,
												  ObjectIdGetDatum(foreignTableId));

	if (HeapTupleIsValid(foreignTableTup))
	{
		Form_pg_foreign_table tableForm =
			(Form_pg_foreign_table) GETSTRUCT(foreignTableTup);

		ForeignServer *foreignServer =
			GetForeignServer(tableForm->ftserver);

		if (IsPgLakeServerName(foreignServer->servername))
			IsPgLakeForeignTable = true;

		ReleaseSysCache(foreignTableTup);
	}

	return IsPgLakeForeignTable;
}

/*
 * Similar to IsPgLakeForeignTableById, but for iceberg.
 */
bool
IsPgLakeIcebergForeignTableById(Oid foreignTableId)
{
	bool		isPgLakeIcebergForeignTable = false;

	/*
	 * We do not call GetForeignTable directly, since it errors for
	 * non-foreign tables.
	 */
	HeapTuple	foreignTableTup = SearchSysCache1(FOREIGNTABLEREL,
												  ObjectIdGetDatum(foreignTableId));

	if (HeapTupleIsValid(foreignTableTup))
	{
		Form_pg_foreign_table tableForm =
			(Form_pg_foreign_table) GETSTRUCT(foreignTableTup);

		ForeignServer *foreignServer =
			GetForeignServer(tableForm->ftserver);

		if (IsPgLakeIcebergServerName(foreignServer->servername))
			isPgLakeIcebergForeignTable = true;

		ReleaseSysCache(foreignTableTup);
	}

	return isPgLakeIcebergForeignTable;
}


bool
IsPgLakeServerName(const char *serverName)
{
	if (strlen(serverName) != strlen(PG_LAKE_SERVER_NAME))
		return false;
	return strncasecmp(serverName, PG_LAKE_SERVER_NAME, strlen(PG_LAKE_SERVER_NAME)) == 0;
}

bool
IsPgLakeIcebergServerName(const char *serverName)
{
	if (strlen(serverName) != strlen(PG_LAKE_ICEBERG_SERVER_NAME))
		return false;

	return strncasecmp(serverName, PG_LAKE_ICEBERG_SERVER_NAME, strlen(PG_LAKE_ICEBERG_SERVER_NAME)) == 0;
}

/*
 * GetQualifiedRelationname generates the quoted and qualified name for a given
 * relation id.
 */
char *
GetQualifiedRelationName(Oid relationId)
{
	char	   *relationName = get_rel_name(relationId);

	if (!relationName)
	{
		elog(ERROR, "cache lookup failed for relation %u", relationId);
	}

	Oid			relNameSpaceOid = get_rel_namespace(relationId);

	if (relNameSpaceOid == InvalidOid)
	{
		elog(ERROR, "cache lookup failed for namespace %u", relationId);
	}

	char	   *namespaceName = get_namespace_name(relNameSpaceOid);

	if (!namespaceName)
	{
		elog(ERROR, "cache lookup failed for namespace %u", relationId);
	}

	return quote_qualified_identifier(namespaceName, relationName);
}


/*
* GetForeignTablePath - get the path option for the foreign table.
*/
char *
GetForeignTablePath(Oid foreignTableId)
{
	ForeignTable *fTable = GetForeignTable(foreignTableId);
	ListCell   *cell;

	foreach(cell, fTable->options)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "path") == 0)
		{
			return defGetString(defel);
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("path option not found for foreign table %u", foreignTableId)));
}


/*
 * GetWritableTableLocation returns the location of a writable table.
 */
char *
GetWritableTableLocation(Oid relationId, char **queryArguments)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	DefElem    *locationOption = GetOption(foreignTable->options, "location");

	if (locationOption == NULL)
		ereport(ERROR, (errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
						errmsg("location option not found for writable foreign table %u",
							   relationId)));

	char	   *location = pstrdup(defGetString(locationOption));

	char	   *queryParamSeparator = strchr(location, '?');

	if (queryParamSeparator != NULL)
	{
		*queryParamSeparator = '\0';

		if (queryArguments != NULL)
			*queryArguments = psprintf("?%s", queryParamSeparator + 1);
	}

	int			locationLength = strlen(location);

	/* normalize prefix to not have a trailing slash */
	if (location[locationLength - 1] == '/')
		location[locationLength - 1] = '\0';

	return location;
}

/*
 * Ensure that the current is the owner of the input relation, error out if
 * not. Superusers bypass this check.
 */
void
EnsureTableOwner(Oid relationId)
{
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER, OBJECT_TABLE,
					   get_rel_name(relationId));
	}
}


/*
 * MakeNameListFromRangeVar makes a namelist from a RangeVar. Its behaviour
 * should be the exact opposite of postgres' makeRangeVarFromNameList.
 */
List *
MakeNameListFromRangeVar(const RangeVar *rel)
{
	if (rel->catalogname != NULL)
	{
		Assert(rel->schemaname != NULL);
		Assert(rel->relname != NULL);
		return list_make3(makeString(rel->catalogname),
						  makeString(rel->schemaname),
						  makeString(rel->relname));
	}
	else if (rel->schemaname != NULL)
	{
		Assert(rel->relname != NULL);
		return list_make2(makeString(rel->schemaname),
						  makeString(rel->relname));
	}
	else
	{
		Assert(rel->relname != NULL);
		return list_make1(makeString(rel->relname));
	}
}


bool
IsAnyLakeForeignTable(RangeTblEntry *rte)
{
	if (rte->rtekind != RTE_RELATION ||
		rte->relkind != RELKIND_FOREIGN_TABLE)
	{
		return false;
	}

	return IsAnyLakeForeignTableById(rte->relid);
}


/*
* GetForeignTableFormat - get the underlying file format for the foreign table.
*/
CopyDataFormat
GetForeignTableFormat(Oid foreignTableId)
{
	PgLakeTableType tableType = GetPgLakeTableType(foreignTableId);

	if (tableType == PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		/* iceberg tables are always parquet */
		return DATA_FORMAT_PARQUET;
	}

	ForeignTable *fTable = GetForeignTable(foreignTableId);
	ListCell   *cell;

	foreach(cell, fTable->options)
	{
		DefElem    *defel = (DefElem *) lfirst(cell);

		if (strcmp(defel->defname, "format") == 0)
		{
			return NameToCopyDataFormat(defGetString(defel));
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
			 errmsg("format option not found for foreign table %u", foreignTableId)));
}


/*
 * GetPgLakeTableProperties returns the format, compression, options and
 * table type of a pg_lake table.
 */
PgLakeTableProperties
GetPgLakeTableProperties(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;

	CopyDataFormat format;
	CopyDataCompression compression;
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	DefElem    *pathOption = GetOption(options, "path");
	char	   *path = NULL;

	if (pathOption != NULL)
	{
		path = defGetString(pathOption);
	}

	FindDataFormatAndCompression(tableType, path, options, &format, &compression);

	PgLakeTableProperties result = {
		.tableType = tableType,
		.format = format,
		.compression = compression,
		.options = options
	};

	return result;
}
