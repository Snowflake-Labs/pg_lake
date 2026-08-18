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
#include "pg_lake/util/string_utils.h"


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

	bool		inPlace = true;

	return StripTrailingSlash(location, inPlace);
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
 * ConvertTypeTree recursively rewrites a (typeOid, typeMod) by applying a
 * caller-supplied leaf rule to every scalar leaf, while handling the container
 * structure (array / map / domain / composite) itself.  Returns true and fills
 * *outTypeOid / *outTypeMod when anything was rewritten; false (outputs
 * untouched) otherwise.
 *
 * The caller supplies the leaf rule via a TypeLeafConverter callback, which is
 * invoked on every node and must return false for container types (arrays,
 * maps, domains, composites) -- those are handled structurally here; returning
 * true and filling *outOid / *outMod requests a rewrite of that scalar leaf.
 * `level` is 0 for a top-level table column and increments by one for every
 * array element, map key/value, or composite field descended into; `context`
 * is passed through untouched.
 *
 * This is the single place that knows how pg_lake types nest, so independent
 * passes (unsupported numeric -> double, snowflake compatibility, ...) share
 * one structural traversal and cannot drift out of coverage.  The container
 * rules are:
 *
 *   array of X                  -> array of ConvertTypeTree(X) at level + 1
 *   map (domain over array)     -> key/value visited at level + 1, never
 *                                  rewritten
 *   domain (non-map)            -> unwrap and recurse into base at same level
 *   composite                   -> fields visited at level + 1 (dropped
 *                                  attributes skipped), never rewritten
 *
 * A composite or map field is only visited so the leaf rule sees every leaf;
 * the container itself is always returned unchanged.  Rewriting a field means
 * declaring a different composite or map type, and a column of that type is no
 * longer assignable from the type the user wrote, so callers that cannot store
 * a leaf as it is have to handle that on the Iceberg side.
 */
bool
ConvertTypeTree(Oid typeOid, int32 typeMod, int level,
				TypeLeafConverter leafConv, void *context,
				Oid *outTypeOid, int32 *outTypeMod)
{
	/* leaf rule first; the callback returns false for container types */
	if (leafConv(typeOid, typeMod, level, context, outTypeOid, outTypeMod))
		return true;

	/* array: recurse into the element type one level deeper */
	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		Oid			rewrittenElementOid;
		int32		rewrittenElementMod;

		if (ConvertTypeTree(elemType, typeMod, level + 1, leafConv, context,
							&rewrittenElementOid, &rewrittenElementMod))
		{
			*outTypeOid = get_array_type(rewrittenElementOid);
			*outTypeMod = -1;
			return true;
		}

		return false;
	}

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valueType = GetMapValueType(typeOid);
		Oid			visitedOid;
		int32		visitedMod;

		/* visit both sides for the leaf rule, keep the map type as it is */
		ConvertTypeTree(keyType.postgresTypeOid, keyType.postgresTypeMod,
						level + 1, leafConv, context, &visitedOid, &visitedMod);
		ConvertTypeTree(valueType.postgresTypeOid, valueType.postgresTypeMod,
						level + 1, leafConv, context, &visitedOid, &visitedMod);

		return false;
	}

	char		typeType = get_typtype(typeOid);

	/* domain (non-map): unwrap and recurse at the same level */
	if (typeType == TYPTYPE_DOMAIN)
	{
		int32		baseMod = typeMod;
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &baseMod);

		return ConvertTypeTree(baseType, baseMod, level, leafConv, context,
							   outTypeOid, outTypeMod);
	}

	/* composite: visit every field for the leaf rule, keep the type as it is */
	if (typeType == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
			Oid			visitedOid;
			int32		visitedMod;

			/* dropped columns must be ignored, never visited */
			if (attr->attisdropped)
				continue;

			ConvertTypeTree(attr->atttypid, attr->atttypmod, level + 1,
							leafConv, context, &visitedOid, &visitedMod);
		}

		ReleaseTupleDesc(tupdesc);

		return false;
	}

	return false;
}


/*
 * NumericLeafToDouble is the ConvertTypeTree leaf rule for the unsupported
 * numeric -> float8 pass.  Numeric is never a container, so it applies at any
 * nesting level; level and context are unused.
 */
static bool
NumericLeafToDouble(Oid typeOid, int32 typeMod, int level, void *context,
					Oid *outOid, int32 *outMod)
{
	if (IsUnsupportedNumericForIceberg(typeOid, typeMod))
	{
		*outOid = FLOAT8OID;
		*outMod = -1;
		return true;
	}

	return false;
}


/*
 * UnsupportedNumericLeafProbe is the ConvertTypeTree leaf rule that only
 * records whether an unsupported numeric leaf exists, without rewriting
 * anything.  context points at the bool to set.
 */
static bool
UnsupportedNumericLeafProbe(Oid typeOid, int32 typeMod, int level, void *context,
							Oid *outOid, int32 *outMod)
{
	if (IsUnsupportedNumericForIceberg(typeOid, typeMod))
		*((bool *) context) = true;

	return false;
}


/*
 * TypeContainsUnsupportedNumeric returns true when any leaf of the type cannot
 * be stored as an Iceberg decimal, at any nesting level.
 */
static bool
TypeContainsUnsupportedNumeric(PGType type)
{
	bool		found = false;
	Oid			unusedOid;
	int32		unusedMod;

	ConvertTypeTree(type.postgresTypeOid, type.postgresTypeMod, 0,
					UnsupportedNumericLeafProbe, &found,
					&unusedOid, &unusedMod);

	return found;
}


/*
 * MaybeConvertType recursively converts a type that contains unsupported
 * numerics.  Returns a PGType with the replacement OID, or with InvalidOid
 * when no conversion is needed.  Thin wrapper over ConvertTypeTree with the
 * numeric leaf rule; columnName is retained for call-site readability.
 */
PGType
MaybeConvertType(PGType type, char *columnName)
{
	Oid			convOid;
	int32		convMod;

	if (ConvertTypeTree(type.postgresTypeOid, type.postgresTypeMod, 0,
						NumericLeafToDouble, NULL, &convOid, &convMod))
		return MakePGType(convOid, convMod);

	return MakePGTypeOid(InvalidOid);
}


/*
 * MaybeConvertUnsupportedNumericColumnsToDouble converts numeric columns that
 * cannot be represented as Iceberg decimals (unbounded or precision > 38) to
 * float8, when pg_lake_iceberg.unsupported_numeric_as_double is enabled.
 * Does nothing when the GUC is off.
 *
 * Only a numeric column and an array of numeric are converted, since Postgres
 * has a cast for both.  A numeric inside a composite type or a map keeps its
 * declared type, because converting it would mean giving the column a composite
 * type the user never declared; the Iceberg side stores it as a double anyway
 * (see PostgresBaseTypeIdToIcebergTypeName), which a NOTICE points out because
 * the declared type no longer shows it.
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
		{
			/*
			 * The column keeps its declared type, but a numeric nested in it
			 * still cannot be stored as an Iceberg decimal, so it is stored
			 * as a double.  Say so at DDL time: \d keeps showing numeric, and
			 * nothing later in the life of the table points this out.
			 */
			if (TypeContainsUnsupportedNumeric(MakePGType(typeOid, typmod)))
				ereport(NOTICE,
						(errmsg("column \"%s\" contains a nested numeric that "
								"cannot be stored as an Iceberg decimal, "
								"storing it as double precision",
								columnDef->colname),
						 errhint("The column keeps its declared type. Values "
								 "that do not fit double precision lose digits. "
								 "Use numeric(P,S) with precision <= %d inside "
								 "the type to store exact decimals.",
								 DUCKDB_MAX_NUMERIC_PRECISION)));

			continue;
		}

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
