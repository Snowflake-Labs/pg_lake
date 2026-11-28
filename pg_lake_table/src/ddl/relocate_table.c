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
#include "fmgr.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/table.h"
#include "access/xact.h"
#include "catalog/dependency.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "commands/defrem.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/access_method/access_method.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/string_utils.h"

PG_FUNCTION_INFO_V1(iceberg_relocate_table);

static void ErrorIfRelocationUnsupported(Oid relationId);
static void GetColumnLists(Oid relationId, char **columnDefinitions,
						   char **insertColumns);
static char *GetReplacementOptions(Oid relationId, char *newLocation);
static void RunGeneratedStatements(const char *query, Oid relationId,
								   char *qualifiedReplacementName);
static void CopyRelationAcls(Oid sourceRelationId, Oid targetRelationId);
static Oid	GetRelationOwner(Oid relationId);
static void SetCatalogAcl(Relation catalogRelation, HeapTuple tuple,
						  int aclAttributeNumber, Acl *acl, Oid relationId,
						  int32 objectSubId, Oid ownerId);

static uint32 relocationTableCounter = 0;

/* the table comment, the column comments, and ownership of serial sequences */
static const char *const PropertyStatementsQuery =
"SELECT pg_catalog.format('COMMENT ON FOREIGN TABLE %s IS %L', $2, d.description) "
"FROM pg_catalog.pg_description d "
"WHERE d.objoid = $1 AND d.classoid = 'pg_catalog.pg_class'::pg_catalog.regclass "
"AND d.objsubid = 0 "
"UNION ALL "
"SELECT pg_catalog.format('COMMENT ON COLUMN %s.%I IS %L', $2, a.attname, d.description) "
"FROM pg_catalog.pg_description d "
"JOIN pg_catalog.pg_attribute a ON a.attrelid = d.objoid AND a.attnum = d.objsubid "
"WHERE d.objoid = $1 AND d.classoid = 'pg_catalog.pg_class'::pg_catalog.regclass "
"AND d.objsubid > 0 "
"UNION ALL "
"SELECT pg_catalog.format('ALTER SEQUENCE %s OWNED BY %s.%I', "
"d.objid::pg_catalog.regclass, $2, a.attname) "
"FROM pg_catalog.pg_depend d "
"JOIN pg_catalog.pg_attribute a ON a.attrelid = d.refobjid AND a.attnum = d.refobjsubid "
"JOIN pg_catalog.pg_class s ON s.oid = d.objid "
"WHERE d.refobjid = $1 AND d.refobjsubid > 0 AND d.deptype = 'a' "
"AND d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass "
"AND d.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass "
"AND s.relkind = 'S'";


/*
 * iceberg_relocate_table rebuilds the visible rows at a new location, then
 * replaces the source foreign table with the rebuilt table.
 *
 * SQL signature:
 *   lake_iceberg.relocate_table(table_name regclass, new_location text)
 *   RETURNS text  -- new table location
 */
Datum
iceberg_relocate_table(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	char	   *newLocation = text_to_cstring(PG_GETARG_TEXT_P(1));
	char	   *queryArguments = NULL;
	char	   *oldLocation;
	char	   *schemaName;
	char	   *tableName;
	char	   *replacementName;
	char	   *qualifiedRelationName;
	char	   *qualifiedReplacementName;
	char	   *columnDefinitions;
	char	   *insertColumns;
	Oid			namespaceId;
	Oid			ownerId;
	Oid			replacementId;
	bool		inPlace = false;

	if (!IsPgLakeIcebergForeignTableById(relationId))
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("relation \"%s\" is not a pg_lake Iceberg table",
						GetQualifiedRelationName(relationId))));

	EnsureTableOwner(relationId);

	if (GetIcebergCatalogType(relationId) != POSTGRES_CATALOG)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("relocate_table only supports Iceberg tables in the postgres catalog")));

	ErrorIfReadOnlyIcebergTable(relationId);

	newLocation = StripTrailingSlash(newLocation, inPlace);

	if (!IsSupportedURL(newLocation))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported URL for new location: \"%s\"", newLocation)));

	CheckURLWriteAccess(newLocation);

	oldLocation = GetWritableTableLocation(relationId, &queryArguments);
	oldLocation = StripTrailingSlash(oldLocation, inPlace);

	if (strcmp(oldLocation, newLocation) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("new location is the same as the current location")));

	namespaceId = get_rel_namespace(relationId);
	schemaName = get_namespace_name(namespaceId);
	tableName = get_rel_name(relationId);
	qualifiedRelationName = quote_qualified_identifier(schemaName, tableName);

	do
	{
		replacementName = psprintf("pg_lake_relocate_%d_%u",
								   MyProcPid, ++relocationTableCounter);
	}
	while (OidIsValid(get_relname_relid(replacementName, namespaceId)));

	qualifiedReplacementName = quote_qualified_identifier(schemaName, replacementName);

	SPI_START();

	ErrorIfRelocationUnsupported(relationId);
	GetColumnLists(relationId, &columnDefinitions, &insertColumns);

	/*
	 * This is what CTAS does internally, but with the source column
	 * definitions, since CTAS would drop NOT NULL, defaults and generated
	 * expressions. Reading the source applies its deletes.
	 */
	SPI_exec(psprintf("CREATE TABLE %s (%s) USING pg_lake_iceberg WITH (%s)",
					  qualifiedReplacementName, columnDefinitions,
					  GetReplacementOptions(relationId, newLocation)), 0);

	SPI_exec(psprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
					  qualifiedReplacementName, insertColumns, insertColumns,
					  qualifiedRelationName), 0);

	replacementId = get_relname_relid(replacementName, namespaceId);
	ownerId = GetRelationOwner(relationId);

	if (GetRelationOwner(replacementId) != ownerId)
		SPI_exec(psprintf("ALTER FOREIGN TABLE %s OWNER TO %s",
						  qualifiedReplacementName,
						  quote_identifier(GetUserNameFromId(ownerId, false))), 0);

	CopyRelationAcls(relationId, replacementId);
	RunGeneratedStatements(PropertyStatementsQuery, relationId, qualifiedReplacementName);

	SPI_exec(psprintf("DROP TABLE %s", qualifiedRelationName), 0);
	SPI_exec(psprintf("ALTER TABLE %s RENAME TO %s",
					  qualifiedReplacementName, quote_identifier(tableName)), 0);

	SPI_END();

	PG_RETURN_TEXT_P(cstring_to_text(newLocation));
}


/*
 * ErrorIfRelocationUnsupported errors out for table properties that the
 * replacement table would not get and that DROP TABLE would silently remove.
 */
static void
ErrorIfRelocationUnsupported(Oid relationId)
{
	const char *query =
		"SELECT CASE "
		"WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_attribute WHERE attrelid = $1 "
		"AND attnum > 0 AND NOT attisdropped AND attidentity <> '') "
		"THEN 'identity columns' "
		"WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_trigger WHERE tgrelid = $1 "
		"AND NOT tgisinternal) THEN 'triggers' "
		"WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_policy WHERE polrelid = $1) "
		"OR (SELECT relrowsecurity FROM pg_catalog.pg_class WHERE oid = $1) "
		"THEN 'row level security' "
		"WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_inherits WHERE inhrelid = $1 "
		"OR inhparent = $1) THEN 'inheritance or partitioning' "
		"WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_statistic_ext WHERE stxrelid = $1) "
		"THEN 'extended statistics' "
		"WHEN EXISTS (SELECT 1 FROM pg_catalog.pg_publication_rel WHERE prrelid = $1) "
		"THEN 'publications' "
		"END";
	bool		isNull = false;
	char	   *unsupportedFeature;

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_EXECUTE(query, true);

	GET_SPI_DATUM(0, 1, &isNull);
	if (isNull)
		return;

	unsupportedFeature = GET_SPI_VALUE(TEXTOID, 0, 1, &isNull);
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("relocate_table does not support tables with %s",
					unsupportedFeature)));
}


/*
 * GetColumnLists returns the column definitions for the replacement table,
 * and the columns to copy, which leaves out generated columns.
 */
static void
GetColumnLists(Oid relationId, char **columnDefinitions, char **insertColumns)
{
	const char *query =
		"SELECT pg_catalog.string_agg("
		"pg_catalog.format('%I %s', a.attname, "
		"pg_catalog.format_type(a.atttypid, a.atttypmod)) "
		"|| CASE WHEN a.attcollation <> t.typcollation "
		"THEN ' COLLATE ' || pg_catalog.quote_ident(cn.nspname) || '.' "
		"|| pg_catalog.quote_ident(co.collname) ELSE '' END "
		"|| CASE WHEN a.attgenerated = 's' THEN ' GENERATED ALWAYS AS (' "
		"|| pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) || ') STORED' "
		"WHEN ad.adbin IS NOT NULL THEN ' DEFAULT ' "
		"|| pg_catalog.pg_get_expr(ad.adbin, ad.adrelid) ELSE '' END "
		"|| CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END, "
		"', ' ORDER BY a.attnum), "
		"pg_catalog.string_agg(pg_catalog.quote_ident(a.attname), ', ' "
		"ORDER BY a.attnum) FILTER (WHERE a.attgenerated = '') "
		"FROM pg_catalog.pg_attribute a "
		"JOIN pg_catalog.pg_type t ON t.oid = a.atttypid "
		"LEFT JOIN pg_catalog.pg_collation co ON co.oid = a.attcollation "
		"LEFT JOIN pg_catalog.pg_namespace cn ON cn.oid = co.collnamespace "
		"LEFT JOIN pg_catalog.pg_attrdef ad ON ad.adrelid = a.attrelid "
		"AND ad.adnum = a.attnum "
		"WHERE a.attrelid = $1 AND a.attnum > 0 AND NOT a.attisdropped";
	bool		isNull = false;

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_EXECUTE(query, true);

	*columnDefinitions = GET_SPI_VALUE(TEXTOID, 0, 1, &isNull);
	*insertColumns = GET_SPI_VALUE(TEXTOID, 0, 2, &isNull);
}


/*
 * GetReplacementOptions returns the source table options with the new
 * location, pinned to the postgres catalog in case the default differs.
 */
static char *
GetReplacementOptions(Oid relationId, char *newLocation)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	StringInfoData options;
	ListCell   *optionCell;

	initStringInfo(&options);
	appendStringInfo(&options, "catalog = 'postgres', location = %s",
					 quote_literal_cstr(newLocation));

	foreach(optionCell, foreignTable->options)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, "location") == 0 ||
			strcmp(option->defname, "catalog") == 0)
			continue;

		appendStringInfo(&options, ", %s = %s", quote_identifier(option->defname),
						 quote_literal_cstr(defGetString(option)));
	}

	return options.data;
}


/*
 * RunGeneratedStatements runs each statement returned by a query that takes
 * the source relation as $1 and the qualified replacement name as $2.
 */
static void
RunGeneratedStatements(const char *query, Oid relationId,
					   char *qualifiedReplacementName)
{
	List	   *statements = NIL;
	ListCell   *statementCell;

	DECLARE_SPI_ARGS(2);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, qualifiedReplacementName, false);
	SPI_EXECUTE(query, true);

	for (uint64 rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull = false;

		statements = lappend(statements,
							 GET_SPI_VALUE(TEXTOID, rowIndex, 1, &isNull));
	}

	foreach(statementCell, statements)
		SPI_exec((char *) lfirst(statementCell), 0);
}


/*
 * CopyRelationAcls copies the table and column privileges of the source to
 * the target, which has the same owner. Copying the ACL arrays keeps grantors
 * and grant options that GRANT statements could not reproduce.
 */
static void
CopyRelationAcls(Oid sourceRelationId, Oid targetRelationId)
{
	Relation	classRelation = table_open(RelationRelationId, RowExclusiveLock);
	Relation	attributeRelation = table_open(AttributeRelationId, RowExclusiveLock);
	Oid			ownerId = GetRelationOwner(targetRelationId);
	HeapTuple	sourceTuple;
	HeapTuple	targetTuple;
	bool		isNull = false;
	Datum		aclDatum;
	int16		attributeCount;

	sourceTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(sourceRelationId));
	if (!HeapTupleIsValid(sourceTuple))
		elog(ERROR, "cache lookup failed for relation %u", sourceRelationId);

	attributeCount = ((Form_pg_class) GETSTRUCT(sourceTuple))->relnatts;

	/* also replaces any ACL that default privileges gave the target */
	aclDatum = SysCacheGetAttr(RELOID, sourceTuple, Anum_pg_class_relacl, &isNull);
	targetTuple = SearchSysCacheCopy1(RELOID, ObjectIdGetDatum(targetRelationId));
	if (!HeapTupleIsValid(targetTuple))
		elog(ERROR, "cache lookup failed for relation %u", targetRelationId);

	SetCatalogAcl(classRelation, targetTuple, Anum_pg_class_relacl,
				  isNull ? NULL : DatumGetAclPCopy(aclDatum), targetRelationId, 0,
				  ownerId);

	ReleaseSysCache(sourceTuple);

	for (AttrNumber attributeNumber = 1; attributeNumber <= attributeCount;
		 attributeNumber++)
	{
		HeapTuple	sourceAttributeTuple;
		HeapTuple	targetAttributeTuple;
		Form_pg_attribute sourceAttribute;

		sourceAttributeTuple = SearchSysCache2(ATTNUM,
											   ObjectIdGetDatum(sourceRelationId),
											   Int16GetDatum(attributeNumber));
		if (!HeapTupleIsValid(sourceAttributeTuple))
			continue;

		sourceAttribute = (Form_pg_attribute) GETSTRUCT(sourceAttributeTuple);
		aclDatum = SysCacheGetAttr(ATTNUM, sourceAttributeTuple,
								   Anum_pg_attribute_attacl, &isNull);

		if (sourceAttribute->attisdropped || isNull)
		{
			ReleaseSysCache(sourceAttributeTuple);
			continue;
		}

		targetAttributeTuple = SearchSysCacheCopyAttName(targetRelationId,
														 NameStr(sourceAttribute->attname));
		if (!HeapTupleIsValid(targetAttributeTuple))
			elog(ERROR, "cache lookup failed for column \"%s\" of relation %u",
				 NameStr(sourceAttribute->attname), targetRelationId);

		SetCatalogAcl(attributeRelation, targetAttributeTuple, Anum_pg_attribute_attacl,
					  DatumGetAclPCopy(aclDatum), targetRelationId,
					  ((Form_pg_attribute) GETSTRUCT(targetAttributeTuple))->attnum,
					  ownerId);

		ReleaseSysCache(sourceAttributeTuple);
	}

	table_close(attributeRelation, RowExclusiveLock);
	table_close(classRelation, RowExclusiveLock);

	CommandCounterIncrement();
}


/*
 * GetRelationOwner returns the owner of a relation.
 */
static Oid
GetRelationOwner(Oid relationId)
{
	HeapTuple	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(relationId));
	Oid			ownerId;

	if (!HeapTupleIsValid(classTuple))
		elog(ERROR, "cache lookup failed for relation %u", relationId);

	ownerId = ((Form_pg_class) GETSTRUCT(classTuple))->relowner;
	ReleaseSysCache(classTuple);

	return ownerId;
}


/*
 * SetCatalogAcl replaces the ACL in a pg_class or pg_attribute tuple, NULL
 * meaning default privileges, and updates the shared dependencies on roles.
 */
static void
SetCatalogAcl(Relation catalogRelation, HeapTuple tuple, int aclAttributeNumber,
			  Acl *acl, Oid relationId, int32 objectSubId, Oid ownerId)
{
	TupleDesc	tupleDesc = RelationGetDescr(catalogRelation);
	Datum	   *values = palloc0(tupleDesc->natts * sizeof(Datum));
	bool	   *nulls = palloc0(tupleDesc->natts * sizeof(bool));
	bool	   *replaces = palloc0(tupleDesc->natts * sizeof(bool));
	HeapTuple	newTuple;
	Datum		oldAclDatum;
	bool		oldAclIsNull = false;
	Oid		   *oldMemberIds = NULL;
	int			oldMemberCount = 0;
	Oid		   *newMemberIds = NULL;
	int			newMemberCount = 0;

	oldAclDatum = heap_getattr(tuple, aclAttributeNumber, tupleDesc, &oldAclIsNull);
	if (!oldAclIsNull)
		oldMemberCount = aclmembers(DatumGetAclP(oldAclDatum), &oldMemberIds);

	if (acl != NULL)
	{
		values[aclAttributeNumber - 1] = PointerGetDatum(acl);
		newMemberCount = aclmembers(acl, &newMemberIds);
	}
	else
		nulls[aclAttributeNumber - 1] = true;

	replaces[aclAttributeNumber - 1] = true;

	newTuple = heap_modify_tuple(tuple, tupleDesc, values, nulls, replaces);
	CatalogTupleUpdate(catalogRelation, &newTuple->t_self, newTuple);

	updateAclDependencies(RelationRelationId, relationId, objectSubId, ownerId,
						  oldMemberCount, oldMemberIds,
						  newMemberCount, newMemberIds);
}
