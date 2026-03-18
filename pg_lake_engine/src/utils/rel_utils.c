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
#include "access/xact.h"
#include "commands/defrem.h"
#include "commands/typecmds.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "catalog/pg_type.h"
#include "parser/parser.h"
#include "parser/parse_type.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/relcache.h"
#include "utils/syscache.h"
#include "utils/typcache.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/type.h"
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
		/*
		 * iceberg data files are parquet, but use a separate format for type
		 * handling
		 */
		return DATA_FORMAT_ICEBERG;
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


/*
 * LookupTypeInSchema returns the OID of a type by name and schema, or
 * InvalidOid if it does not exist.
 */
static Oid
LookupTypeInSchema(const char *schemaName, const char *typeName)
{
	List	   *names = list_make2(makeString(pstrdup(schemaName)),
								   makeString(pstrdup(typeName)));
	TypeName   *tn = makeTypeNameFromNameList(names);
	int32		typmod_out;
	Type		tup = LookupTypeName(NULL, tn, &typmod_out, true);

	if (!HeapTupleIsValid(tup))
		return InvalidOid;

	Oid			result = ((Form_pg_type) GETSTRUCT(tup))->oid;

	ReleaseSysCache(tup);
	return result;
}


/*
 * MaybeConvertType recursively converts a type that contains unsupported
 * numerics.  Returns a PGType with the replacement OID, or with InvalidOid
 * when no conversion is needed.
 *
 *   numeric (unsupported)       -> FLOAT8OID
 *   array of X                  -> array of MaybeConvertType(X)
 *   map (domain over array)     -> new map via CreatePGMapTypeFromOids
 *   domain (non-map)            -> unwrap and recurse into base type
 *   composite containing any    -> new composite via DefineCompositeType
 */
PGType
MaybeConvertType(PGType type, char *columnName)
{
	Oid			typeOid = type.postgresTypeOid;
	int32		typmod = type.postgresTypeMod;

	/* unsupported numeric -> float8 */
	if (IsUnsupportedNumericForIceberg(typeOid, typmod))
		return MakePGTypeOid(FLOAT8OID);

	/* array: recurse into element type */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		PGType		converted = MaybeConvertType(MakePGType(elemType, typmod),
												 columnName);

		if (OidIsValid(converted.postgresTypeOid))
			return MakePGTypeOid(get_array_type(converted.postgresTypeOid));
		return MakePGTypeOid(InvalidOid);
	}

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);
		PGType		newKey = MaybeConvertType(keyType, columnName);
		PGType		newVal = MaybeConvertType(valType, columnName);

		if (!OidIsValid(newKey.postgresTypeOid) &&
			!OidIsValid(newVal.postgresTypeOid))
			return MakePGTypeOid(InvalidOid);

		return MakePGTypeOid(CreatePGMapTypeFromOids(
													 OidIsValid(newKey.postgresTypeOid) ? newKey.postgresTypeOid : keyType.postgresTypeOid,
													 OidIsValid(newVal.postgresTypeOid) ? newVal.postgresTypeOid : valType.postgresTypeOid));
	}

	char		typeType = get_typtype(typeOid);

	/* domain (non-map): unwrap and recurse */
	if (typeType == TYPTYPE_DOMAIN)
	{
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &typmod);

		return MaybeConvertType(MakePGType(baseType, typmod), columnName);
	}

	/* composite: check each attribute */
	if (typeType == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		List	   *coldeflist = NIL;
		bool		needsConversion = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			Oid			targetType = attr->atttypid;
			int32		targetTypmod = attr->atttypmod;
			PGType		converted = MaybeConvertType(
													 MakePGType(targetType, targetTypmod), columnName);

			if (OidIsValid(converted.postgresTypeOid))
			{
				targetType = converted.postgresTypeOid;
				targetTypmod = converted.postgresTypeMod;
				needsConversion = true;
			}

			ColumnDef  *colDef = makeNode(ColumnDef);

			colDef->colname = pstrdup(NameStr(attr->attname));
			colDef->typeName = makeTypeNameFromOid(targetType, targetTypmod);
			colDef->is_local = true;
			coldeflist = lappend(coldeflist, colDef);
		}

		ReleaseTupleDesc(tupdesc);

		if (!needsConversion)
			return MakePGTypeOid(InvalidOid);

		char	   *newTypeName = psprintf("converted_%u_f8", typeOid);
		Oid			existingOid = LookupTypeInSchema(STRUCT_TYPES_SCHEMA,
													 newTypeName);

		if (OidIsValid(existingOid))
			return MakePGTypeOid(existingOid);

		ereport(NOTICE,
				(errmsg("creating type \"%s\".\"%s\" with unsupported numeric "
						"attributes replaced by double precision for Iceberg "
						"column \"%s\"",
						STRUCT_TYPES_SCHEMA, newTypeName, columnName)));

		RangeVar   *typevar = makeRangeVar(STRUCT_TYPES_SCHEMA, newTypeName,
										   -1);
		ObjectAddress newType = DefineCompositeType(typevar, coldeflist);

		CommandCounterIncrement();

		return MakePGTypeOid(newType.objectId);
	}

	return MakePGTypeOid(InvalidOid);
}


/*
 * MaybeConvertUnsupportedNumericColumnsToDouble converts numeric columns that
 * cannot be represented as Iceberg decimals (unbounded or precision > 38) to
 * float8, when pg_lake_iceberg.unsupported_numeric_as_double is enabled.
 * Does nothing when the GUC is off.
 *
 * For top-level numeric and numeric arrays the ColumnDef's typeName is
 * replaced directly.  For composite types and map types a *new* type is
 * created (in lake_struct / map_type schemas) and the column definition
 * is pointed to the new type; the original user-defined type is never
 * modified.
 */
void
MaybeConvertUnsupportedNumericColumnsToDouble(List *columnDefList)
{
	ListCell   *cell;

	if (!UnsupportedNumericAsDouble)
		return;

	foreach(cell, columnDefList)
	{
		if (!IsA(lfirst(cell), ColumnDef))
			continue;

		ColumnDef  *columnDef = (ColumnDef *) lfirst(cell);

		if (columnDef->typeName == NULL)
			continue;

		int32		typmod = 0;
		Oid			typeOid = InvalidOid;

		/*
		 * Use missing_ok lookup because pseudo-types like serial/bigserial
		 * are not resolvable before transformColumnDefinition() runs.
		 */
		Type		tup = LookupTypeName(NULL, columnDef->typeName, &typmod, true);

		if (!HeapTupleIsValid(tup))
			continue;

		typeOid = ((Form_pg_type) GETSTRUCT(tup))->oid;
		ReleaseSysCache(tup);

		PGType		converted = MaybeConvertType(MakePGType(typeOid, typmod),
												 columnDef->colname);

		if (!OidIsValid(converted.postgresTypeOid))
			continue;

		ereport(NOTICE,
				(errmsg("column \"%s\" has type that cannot be stored as an "
						"Iceberg decimal, converting to double precision",
						columnDef->colname),
				 errhint("Use numeric(P,S) with precision <= %d to preserve "
						 "exact decimal semantics.",
						 DUCKDB_MAX_NUMERIC_PRECISION)));

		columnDef->typeName = makeTypeNameFromOid(converted.postgresTypeOid, converted.postgresTypeMod);
	}
}
