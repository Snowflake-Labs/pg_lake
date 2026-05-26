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

/*
* register_field_ids.c
*
* This file contains functions to register and extract field IDs for Iceberg tables
* from/to catalog lake_table.field_id_mappings.
*
* In order to unify all the field ID logic in the code, we also provide functions to
* read field IDs from external Iceberg tables via the Iceberg metadata.
*/
#include "postgres.h"
#include "miscadmin.h"

#include "access/relation.h"
#include "access/table.h"
#include "commands/defrem.h"
#include "commands/comment.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"
#include "parser/parse_type.h"

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/ducklake/catalog.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_json_serde.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/util/table_type.h"


static DataFileSchema * GetDataFileSchemaForTableInternal(Oid relationId);

/*
* RegisterPostgresColumnMappings adds entries to the
* lake_table.field_id_mappings table for the given list of
* PostgresColumnMapping.
*/
void
RegisterPostgresColumnMappings(List *pgColumnMappingList)
{
	ListCell   *lc;

	foreach(lc, pgColumnMappingList)
	{
		PostgresColumnMapping *pgColumnMapping = lfirst(lc);

		Oid			relationId = pgColumnMapping->relationId;

		/* top-level column doesn't have a parent */
		int			parentFieldId = INVALID_FIELD_ID;

		/* find attribute number from attrName */
		AttrNumber	attrNo = get_attnum(relationId, pgColumnMapping->attname);

		DataFileSchemaField *field = pgColumnMapping->field;

		/* recursively traverse the type, and register all (sub)fields */
		RegisterIcebergColumnMapping(relationId, field->type,
									 attrNo, parentFieldId, pgColumnMapping->pgType,
									 field->id,
									 field->writeDefault,
									 field->initialDefault);
	}
}


/*
* GetDataFileSchemaForTableWithExclusion returns a table schema for the given
* relationId after filtering out excludedColumns.
*/
DataFileSchema *
GetDataFileSchemaForTableWithExclusion(Oid relationId, List *excludedColumns)
{
	/*
	 * Iterate on all the columns of the relation, skip dropped ones and the
	 * excluded ones, then create PostgresColumnMapping per column.
	 */
	Relation	rel = table_open(relationId, AccessShareLock);

	TupleDesc	tupDesc = RelationGetDescr(rel);

	DataFileSchema *schema = palloc0(sizeof(DataFileSchema));

	schema->fields = palloc0(sizeof(DataFileSchemaField) * tupDesc->natts);

	size_t		nonExcludedColumnCount = 0;

	for (int idx = 0; idx < tupDesc->natts; idx++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupDesc, idx);

		AttrNumber	attrNo = attr->attnum;

		/* skip dropped or excluded attributes */
		if (attr->attisdropped || list_member_int(excludedColumns, attrNo))
		{
			continue;
		}

		DataFileSchemaField *field = GetRegisteredFieldForAttribute(relationId, attrNo);

		schema->fields[nonExcludedColumnCount] = *field;

		nonExcludedColumnCount++;
	}

	schema->nfields = nonExcludedColumnCount;

	table_close(rel, NoLock);

	return schema;
}


/*
* CreatePostgresColumnMappingsForColumnDefs returns a list of PostgresColumnMapping
* for the given relationId and columnDefList.
*/
List *
CreatePostgresColumnMappingsForColumnDefs(Oid relationId, List *columnDefList, bool forAddColumn)
{
	List	   *pgColumnMappingList = NIL;
	ListCell   *columnDefCell = NULL;
	int			fieldId = GetLargestRegisteredFieldId(relationId) + 1;

	Relation	rel = table_open(relationId, AccessShareLock);

	TupleDesc	tupleDesc = RelationGetDescr(rel);

	foreach(columnDefCell, columnDefList)
	{
		ColumnDef  *columnDef = (ColumnDef *) lfirst(columnDefCell);
		TypeName   *columnTypeName = columnDef->typeName;
		char	   *columnName = columnDef->colname;

		DataFileSchemaField *field = palloc0(sizeof(DataFileSchemaField));

		field->id = fieldId;
		field->name = pstrdup(columnName);

		/*
		 * we never expect this, still better than crash in case of unforeseen
		 * scenarios
		 */
		if (!columnName)
			elog(ERROR, "column name is required");

		AttrNumber	attrNo = get_attnum(relationId, columnName);

		int32		typmod = 0;
		Oid			typeOid = InvalidOid;

		typenameTypeIdAndMod(NULL, columnTypeName, &typeOid, &typmod);

		int			subFieldIndex = fieldId;

		PGType		pgType = MakePGType(typeOid, typmod);

		field->type =
			PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

		field->required = columnDef->is_not_null;

		/*
		 * Postgres doesn't have a syntax to define comment for create
		 * table/add column statements.
		 */
		field->doc = NULL;

		field->writeDefault =
			GetIcebergJsonSerializedDefaultExpr(tupleDesc, attrNo, field);

		if (forAddColumn && field->writeDefault != NULL)
		{
			field->initialDefault = field->writeDefault;
			field->duckSerializedInitialDefault =
				GetDuckSerializedIcebergFieldInitialDefault(field->initialDefault, field->type);
		}
		else
		{
			field->initialDefault = NULL;
			field->duckSerializedInitialDefault = NULL;
		}

		/* now create the PostgresColumnMapping */
		PostgresColumnMapping *columnMapping = palloc0(sizeof(PostgresColumnMapping));

		columnMapping->field = field;
		columnMapping->relationId = relationId;
		columnMapping->attname = pstrdup(columnName);
		columnMapping->pgType = MakePGType(typeOid, typmod);
		columnMapping->attrNum = attrNo;

		pgColumnMappingList = lappend(pgColumnMappingList, columnMapping);

		fieldId = subFieldIndex + 1;
	}

	table_close(rel, NoLock);

	return pgColumnMappingList;
}


/*
* CreatePostgresColumnMappingsForIcebergTableFromExternalMetadata is designed for a very
* specific use case, where we want to create PostgresColumnMapping for an Iceberg table
* from the external metadata. Normally, you'd expect to use GetPostgresColumnMappingsForTable()
* for this purpose.
* The reason we have this function is that in case an iceberg table is created before the
* field_id mapping is implemented (in earlier versions), we need to be able to register
* the field IDs from the external metadata.
*/
List *
CreatePostgresColumnMappingsForIcebergTableFromExternalMetadata(Oid relationId)
{
	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	/*
	 * we extract column mappings to make sure remote catalog schema matches
	 * the schema in our catalog for external tables. Otherwise, we prepare
	 * for creating field id mappings for internal tables.
	 */
	bool		forUpdate = (IsInternalIcebergTable(relationId)) ? true : false;

	char	   *currentMetadataPath = GetIcebergMetadataLocation(relationId, forUpdate);

	DataFileSchema *schema = GetDataFileSchemaForExternalIcebergTable(currentMetadataPath);

	Relation	rel = RelationIdGetRelation(relationId);
	TupleDesc	tupDesc = RelationGetDescr(rel);

	List	   *pgColumnMappingList = NIL;

	for (size_t fieldIdx = 0; fieldIdx < schema->nfields; fieldIdx++)
	{
		DataFileSchemaField *field = &schema->fields[fieldIdx];

		PostgresColumnMapping *columnMapping = palloc0(sizeof(PostgresColumnMapping));

		columnMapping->relationId = relationId;
		columnMapping->field = field;

		columnMapping->attrNum = get_attnum(relationId, field->name);
		if (icebergCatalogType == REST_CATALOG_READ_ONLY && columnMapping->attrNum == InvalidAttrNumber)
		{
			/*
			 * If no such column exists, skip.
			 */
			continue;
		}

		columnMapping->attname = pstrdup(field->name);
		columnMapping->attrNum = get_attnum(relationId, field->name);
		Form_pg_attribute attr = TupleDescAttr(tupDesc, columnMapping->attrNum - 1);

		columnMapping->pgType = MakePGType(attr->atttypid, attr->atttypmod);
		columnMapping->attNotNull = attr->attnotnull;
		columnMapping->attHasDef = attr->atthasdef;

		pgColumnMappingList = lappend(pgColumnMappingList, columnMapping);
	}

	RelationClose(rel);

	return pgColumnMappingList;
}


/*
 * GetDataFileSchemaForDucklakeTable gets a table schema for a DuckLake table
 * by reading pg_attribute. Field IDs are sourced from
 * lake_ducklake.column.column_id so they line up with what DuckDB's
 * ducklake extension embeds in parquet files (DuckDB writes the catalog
 * column_id as the parquet field_id). initial_default is sourced from
 * lake_ducklake.column so reads of older parquet files (written before
 * an ADD COLUMN ... DEFAULT) backfill the right value rather than NULL.
 */
static DataFileSchema *
GetDataFileSchemaForDucklakeTable(Oid relationId)
{
	Relation	rel = relation_open(relationId, AccessShareLock);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	DataFileSchema *schema = palloc0(sizeof(DataFileSchema));

	/* Count non-dropped columns first */
	int			nonDroppedCount = 0;

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (!attr->attisdropped && attr->attnum > 0)
			nonDroppedCount++;
	}

	schema->fields = palloc0(sizeof(DataFileSchemaField) * nonDroppedCount);
	schema->nfields = 0;

	/*
	 * Build per-attnum lookups for (column_id, initial_default) from
	 * lake_ducklake.column for the live (end_snapshot IS NULL) rows of
	 * this table. We pull this once into palloc'd arrays indexed by
	 * attnum so the per-attribute loop below can attach both without an
	 * SPI call per column.
	 */
	int64	   *columnIdsByAttnum = NULL;
	char	  **defaultStrings = NULL;
	int			maxAttnum = 0;
	{
		/*
		 * SPI requires an active snapshot, but this function is reached
		 * from query planning where the planner may not have pushed
		 * one. Push a snapshot if missing so DucklakeGetTableMetadata
		 * and our SPI block below can run.
		 */
		bool		pushedSnapshot = false;

		if (!ActiveSnapshotSet())
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			pushedSnapshot = true;
		}

		DucklakeTableMetadata *metadata = DucklakeGetTableMetadata(relationId);

		if (metadata != NULL)
		{
			for (int i = 0; i < tupdesc->natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

				if (!attr->attisdropped && attr->attnum > maxAttnum)
					maxAttnum = attr->attnum;
			}

			if (maxAttnum > 0)
			{
				columnIdsByAttnum = palloc0(sizeof(int64) * (maxAttnum + 1));
				defaultStrings = palloc0(sizeof(char *) * (maxAttnum + 1));

				/*
				 * Capture the caller's memory context BEFORE SPI_connect
				 * so we can extract default expressions into a context
				 * that survives SPI_finish.
				 */
				MemoryContext callerContext = CurrentMemoryContext;

				StringInfoData q;

				/*
				 * Match catalog rows to pg_attribute by NAME, not by
				 * column_order. After DROP+ADD column the new live
				 * row's column_order (allocated sequentially in the
				 * catalog) no longer matches pg_attribute.attnum (which
				 * skips dropped slots), so order-based lookup would
				 * miss the column entirely and stamp field_id=0 on it.
				 */
				initStringInfo(&q);
				appendStringInfo(&q,
								 "SELECT a.attnum, c.column_id, c.initial_default "
								 "  FROM pg_attribute a "
								 "  JOIN lake_ducklake.column c "
								 "    ON c.column_name = a.attname::text "
								 " WHERE a.attrelid = %u "
								 "   AND a.attnum > 0 AND NOT a.attisdropped "
								 "   AND c.table_id = %ld "
								 "   AND c.end_snapshot IS NULL "
								 "   AND c.parent_column IS NULL",
								 relationId, metadata->tableId);

				SPI_connect();
				int			ret = SPI_exec(q.data, 0);

				if (ret == SPI_OK_SELECT)
				{
					for (uint64 row = 0; row < SPI_processed; row++)
					{
						bool		isnull;
						int32		attno = DatumGetInt32(SPI_getbinval(
													SPI_tuptable->vals[row],
													SPI_tuptable->tupdesc, 1, &isnull));

						if (isnull || attno <= 0 || attno > maxAttnum)
							continue;

						int64		colId = DatumGetInt64(SPI_getbinval(
													SPI_tuptable->vals[row],
													SPI_tuptable->tupdesc, 2, &isnull));

						if (!isnull)
							columnIdsByAttnum[attno] = colId;

						Datum		d = SPI_getbinval(SPI_tuptable->vals[row],
													  SPI_tuptable->tupdesc, 3, &isnull);

						if (!isnull)
						{
							MemoryContext oldcxt =
								MemoryContextSwitchTo(callerContext);

							defaultStrings[attno] = TextDatumGetCString(d);
							MemoryContextSwitchTo(oldcxt);
						}
					}
				}

				SPI_finish();
			}

			if (metadata->tableName)
				pfree(metadata->tableName);
			if (metadata->schemaName)
				pfree(metadata->schemaName);
			if (metadata->path)
				pfree(metadata->path);
			pfree(metadata);
		}

		if (pushedSnapshot)
			PopActiveSnapshot();
	}

	for (int i = 0; i < tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		/* Skip dropped columns and system columns */
		if (attr->attisdropped || attr->attnum <= 0)
			continue;

		DataFileSchemaField *field = &schema->fields[schema->nfields];

		/*
		 * For DuckLake tables, the parquet field_id is the catalog
		 * column_id so files written by DuckDB's ducklake extension and
		 * by pg_lake share a single ID space. If we couldn't find a
		 * matching catalog row (e.g. the table just got registered and
		 * the column row hasn't landed yet), fall back to attnum to
		 * keep the read path working at all.
		 */
		int64		columnId = (columnIdsByAttnum != NULL && attr->attnum <= maxAttnum)
									? columnIdsByAttnum[attr->attnum]
									: 0;

		field->id = (columnId > 0) ? (int) columnId : attr->attnum;
		field->name = pstrdup(NameStr(attr->attname));
		field->required = attr->attnotnull;
		field->doc = NULL;
		field->writeDefault = NULL;
		field->initialDefault = NULL;
		field->duckSerializedInitialDefault = NULL;

		PGType		pgType = MakePGType(attr->atttypid, attr->atttypmod);
		bool		forAddColumn = false;
		int			subFieldIndex = field->id;

		field->type = PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

		if (defaultStrings != NULL && attr->attnum <= maxAttnum &&
			defaultStrings[attr->attnum] != NULL)
		{
			/*
			 * Our DucklakeAddColumn writes the postgres-formatted default
			 * expression (e.g. "99" or "'foo'::text") into
			 * lake_ducklake.column.initial_default. The Iceberg JSON
			 * serializer used for iceberg tables doesn't apply here, so
			 * pass the expression through verbatim — read_parquet's
			 * default_value will handle simple constants.
			 */
			field->initialDefault = defaultStrings[attr->attnum];
			field->duckSerializedInitialDefault = defaultStrings[attr->attnum];
		}

		schema->nfields++;
	}

	relation_close(rel, AccessShareLock);

	return schema;
}


/*
 * GetDataFileSchemaForTableInternal is helper function to get the schema for a given table.
 * It is used by GetDataFileSchemaForTable.
 */
static DataFileSchema *
GetDataFileSchemaForTableInternal(Oid relationId)
{
	if (IsDucklakeTable(relationId))
		return GetDataFileSchemaForDucklakeTable(relationId);

	if (!IsIcebergTable(relationId))
		return NULL;

	if (IsInternalIcebergTable(relationId))
	{
		return GetDataFileSchemaForInternalIcebergTable(relationId);
	}
	else
	{
		Assert(IsExternalIcebergTable(relationId));

		char	   *path = GetIcebergMetadataLocation(relationId, false);

		return GetDataFileSchemaForExternalIcebergTable(path);
	}
}


/*
 * GetDataFileSchemaForTable gets the schema for a given table.
 */
DataFileSchema *
GetDataFileSchemaForTable(Oid relationId)
{
	return GetDataFileSchemaForTableInternal(relationId);
}


/*
 * GetDataFileSchemaForExternalIcebergTable gets a table schema field based
 * on the current Iceberg metadata.
 */
DataFileSchema *
GetDataFileSchemaForExternalIcebergTable(char *metadataPath)
{
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataPath);
	IcebergTableSchema *icebergSchema = GetCurrentIcebergTableSchema(metadata);

	DataFileSchema *schema = palloc0(sizeof(DataFileSchema));

	schema->fields = icebergSchema->fields;
	schema->nfields = icebergSchema->fields_length;

	return schema;
}


/*
 * GetLeafFieldsForExternalIcebergTable gets the leaf fields for the external
 * Iceberg table.
 */
List *
GetLeafFieldsForExternalIcebergTable(char *metadataPath)
{
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataPath);

	return GetLeafFieldsFromIcebergMetadata(metadata);
}


/*
 * GetLeafFieldsForTable gets the leaf fields for the given table.
 */
List *
GetLeafFieldsForTable(Oid relationId)
{
	if (IsDucklakeTable(relationId))
		return GetLeafFieldsForDucklakeTable(relationId);

	if (!IsIcebergTable(relationId))
		return NULL;

	if (IsInternalIcebergTable(relationId))
	{
		return GetLeafFieldsForInternalIcebergTable(relationId);
	}
	else
	{
		Assert(IsExternalIcebergTable(relationId));

		char	   *path = GetIcebergMetadataLocation(relationId, false);

		return GetLeafFieldsForExternalIcebergTable(path);
	}
}


/*
 * GetDuckSerializedIcebergFieldInitialDefault first deserialize the initial default
 * value, that is in Iceberg JSON format, to Postgres datum. Then, serialize the
 * Postgres datum to Duckdb serialized string.
 *
 * This is necessary to pass the default values for fields to Duckdb during read_parquet.
 */
const char *
GetDuckSerializedIcebergFieldInitialDefault(const char *initialDefault,
											Field * field)
{
	EnsureIcebergField(field);

	if (initialDefault == NULL)
	{
		return NULL;
	}

	PGType		pgType = IcebergFieldToPostgresType(field);

	bool		isNull = false;
	Datum		initialDefaultDatum = PGIcebergJsonDeserialize(initialDefault,
															   field, pgType,
															   &isNull);

	if (isNull)
	{
		return "NULL";
	}

	FmgrInfo	outFunc;
	Oid			outFuncId = InvalidOid;
	bool		isvarlena = false;

	getTypeOutputInfo(pgType.postgresTypeOid, &outFuncId, &isvarlena);
	fmgr_info(outFuncId, &outFunc);

	return PGDuckSerialize(&outFunc, pgType.postgresTypeOid, initialDefaultDatum,
						   DATA_FORMAT_INVALID);
}
