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
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "parser/parse_type.h"

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/format_version.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_json_serde.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/serialize.h"


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

		/*
		 * Per-column `iceberg_type` foreign-table option (Tier-1 lossy POC,
		 * see commit message and pg_lake_engine.allow_lossy_ns_timestamp).
		 *
		 * Why post-process instead of plumbing through PostgresTypeToIcebergField:
		 *
		 *   - The override only applies at CREATE / ALTER FOREIGN TABLE
		 *     time, when columnDef->fdwoptions is populated. Every other
		 *     caller of PostgresTypeToIcebergField (catalog re-read,
		 *     partition transforms, ...) reconstructs Field from already-
		 *     persisted data, where the typeName is already the override.
		 *
		 *   - The override is intentionally restricted to top-level scalar
		 *     fields (no array / composite / map nesting), so we can
		 *     post-process the returned Field by replacing only
		 *     field->field.scalar.typeName.
		 *
		 * Validation order is important: we want a useful error if the GUC
		 * is off, before we surface the bare PostgreSQL "type mismatch"
		 * message.
		 */
		const char *icebergTypeOverride = NULL;

		if (columnDef->fdwoptions != NIL)
		{
			ListCell   *opt;

			foreach(opt, columnDef->fdwoptions)
			{
				DefElem    *def = (DefElem *) lfirst(opt);

				if (strcmp(def->defname, "iceberg_type") == 0)
				{
					icebergTypeOverride = defGetString(def);
					break;
				}
			}
		}

		if (icebergTypeOverride != NULL)
		{
			if (strcmp(icebergTypeOverride, "timestamp_ns") == 0)
			{
				/*
				 * timestamp_ns is v3-only in the Iceberg spec
				 * (https://iceberg.apache.org/spec/#nanosecond-precision-timestamps).
				 * v2 readers don't know the type and would fail; emitting it
				 * on a v2 table would also leak out-of-spec metadata.
				 */
				IcebergFormatVersion formatVersion =
					GetIcebergFormatVersionFromTableOptions(relationId);

				if (!IcebergFormatVersionSupportsNanoTimestamp(formatVersion))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("iceberg_type 'timestamp_ns' requires Iceberg format-version 3"),
							 errhint("Recreate the table with WITH (format_version = 3) "
									 "or drop the iceberg_type column option.")));

				if (!AllowLossyNsTimestamp)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("iceberg_type 'timestamp_ns' is not enabled"),
							 errdetail("The PostgreSQL TIMESTAMP <-> Iceberg "
									   "timestamp_ns mapping is *lossy*: nanosecond "
									   "precision is truncated on read and padded "
									   "with zeros on write."),
							 errhint("SET pg_lake_engine.allow_lossy_ns_timestamp = on "
									 "before CREATE / ALTER FOREIGN TABLE if you "
									 "accept the lossy round-trip.")));

				if (typeOid != TIMESTAMPOID)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("iceberg_type 'timestamp_ns' is only supported on "
									"PostgreSQL TIMESTAMP columns"),
							 errdetail("Column \"%s\" has PostgreSQL type %s.",
									   columnName, format_type_be(typeOid))));

				if (field->type->type != FIELD_TYPE_SCALAR)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("iceberg_type 'timestamp_ns' is only supported on "
									"top-level scalar columns")));

				field->type->field.scalar.typeName = pstrdup("timestamp_ns");
			}
			else
			{
				/* The validator already rejects unknown values, but be defensive. */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid iceberg_type column option: \"%s\"",
								icebergTypeOverride)));
			}
		}

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
			/*
			 * Stage 15 of the v3 rollout: ``ADD COLUMN ... DEFAULT`` is a
			 * v3-only Iceberg feature (the ``initial-default`` /
			 * ``write-default`` schema-field keys land in the spec at
			 * format-version 3). On v2 we historically emitted these keys
			 * anyway, which is tolerated by v2 readers but is out-of-spec and
			 * can confuse strict consumers. We accept the intentional
			 * regression here -- existing v2 tables can no longer grow a
			 * defaulted column via ALTER TABLE -- in exchange for a clean,
			 * spec-compliant metadata.json on both v2 and v3.
			 *
			 * The error fires from the ALTER TABLE codepath only
			 * (forAddColumn is set in ddl_changes.c for DDL_COLUMN_ADD). The
			 * companion writer-side gate in write_table_metadata.c
			 * additionally drops any stale write-default still hanging on
			 * legacy v2 catalog rows so they never leak into a fresh
			 * metadata.json.
			 */
			IcebergFormatVersion formatVersion =
				GetIcebergFormatVersionFromTableOptions(relationId);

			if (!IcebergFormatVersionSupportsColumnDefaults(formatVersion))
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("ALTER TABLE ADD COLUMN ... DEFAULT requires Iceberg format-version 3"),
						 errhint("Recreate the table with WITH (format_version = 3), or "
								 "ADD COLUMN without DEFAULT and follow up with an "
								 "UPDATE to populate existing rows.")));

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
 * GetDataFileSchemaForTableInternal is helper function to get the schema for a given table.
 * It is used by GetDataFileSchemaForTable.
 */
static DataFileSchema *
GetDataFileSchemaForTableInternal(Oid relationId)
{
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
