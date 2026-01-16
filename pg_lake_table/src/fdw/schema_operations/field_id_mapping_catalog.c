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
* field_id_mapping_catalog.c
*
* This file contains functions to register and extract field IDs for Iceberg tables
* from/to catalog lake_table.field_id_mappings.
*/
#include "postgres.h"
#include "miscadmin.h"

#include "access/relation.h"
#include "access/table.h"
#include "common/int.h"
#include "commands/comment.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"


#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/util/array_utils.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_extension_base/spi_helpers.h"

static DataFileSchemaField * CreateRegisteredFieldForAttribute(Oid relationId, int spiIndex);
static void InsertFieldMapping(Oid relationId, int attrIcebergFieldId,
							   AttrNumber pg_attnum, PGType pgType,
							   const char *writeDefault, const char *initialDefault,
							   int parentFieldId);
static AttrNumber GetAttributeForFieldIdForInternalIcebergTable(Oid relationId, int fieldId);
static AttrNumber GetAttributeForFieldIdForExternalIcebergTable(char *metadataPath, Oid relationId, int fieldId);

#ifdef USE_ASSERT_CHECKING
static List *GetAllRegisteredAttnumsForTopLevelColumns(Oid relationId);
static void AssertAllNonDroppedColumnsHaveRegisteredFieldIds(Oid relationId);
#endif


/*
* GetRegisteredFieldForAttributes returns a DataFileSchemaField
* for the given column. Returns NULL if the field ID is not found for a given column.
* The function guarantees the the returned list is in the same order (and same length)
* as the input list.
*/
List *
GetRegisteredFieldForAttributes(Oid relationId, List *attrNos)
{
	MemoryContext currentContext = CurrentMemoryContext;

	List	   *fields = NIL;

	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Now, with SPI select from field_id_mappings (relation_id, pg_attnum)
	 */
	StringInfo	query = makeStringInfo();

	/*
	 * Make sure that the input $2 matches the order of the pg_attnum in
	 * output, we do that by using unnest with ordinality.
	 */
	appendStringInfo(query, "SELECT pg_attnum, field_id, field_pg_type, field_pg_typemod, initial_default, write_default FROM "
					 MAPPING_TABLE_NAME ", pg_catalog.unnest($2) WITH ORDINALITY AS input(attnum, sort_order) "
					 "WHERE table_name OPERATOR(pg_catalog.=) $1 "
					 "AND pg_attnum = input.attnum AND parent_field_id IS NULL ORDER BY sort_order");

	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT2ARRAYOID, INT16ListToArray(attrNos), false);

	SPI_START();

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	if (SPI_processed != list_length(attrNos))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("missing field for column")));
	}

	/* populate some info from catalog */
	MemoryContext spiContext = MemoryContextSwitchTo(currentContext);

	for (int spiIndex = 0; spiIndex < SPI_processed; spiIndex++)
	{
		DataFileSchemaField *field = CreateRegisteredFieldForAttribute(relationId, spiIndex);

		fields = lappend(fields, field);
	}

	MemoryContextSwitchTo(spiContext);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);


	return fields;

}


/*
* CreateRegisteredFieldForAttribute creates a DataFileSchemaField for the given SPI Index.
* This function is currently only intended to be used by GetRegisteredFieldForAttributes().
*/
static DataFileSchemaField *
CreateRegisteredFieldForAttribute(Oid relationId, int spiIndex)
{

	DataFileSchemaField *field = palloc0(sizeof(DataFileSchemaField));

	bool		isNull = false;

	AttrNumber	attrNo = GET_SPI_VALUE(INT2OID, spiIndex, 1, &isNull);

	field->id = GET_SPI_VALUE(INT4OID, spiIndex, 2, &isNull);

	if (field->id == INVALID_FIELD_ID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("invalid field ID for column")));
	}

	Oid			fieldType = GET_SPI_VALUE(OIDOID, spiIndex, 3, &isNull);
	int32		fieldTypeMod = GET_SPI_VALUE(INT4OID, spiIndex, 4, &isNull);

	bool		initialDefaultValueIsNull = false;
	Datum		initialFefaultValueDatum =
		GET_SPI_DATUM(spiIndex, 5, &initialDefaultValueIsNull);

	field->initialDefault =
		!initialDefaultValueIsNull ? TextDatumGetCString(initialFefaultValueDatum) : NULL;

	bool		writeDefaultValueIsNull = false;
	Datum		writeDefaultValueDatum =
		GET_SPI_DATUM(spiIndex, 6, &writeDefaultValueIsNull);

	field->writeDefault =
		!writeDefaultValueIsNull ? TextDatumGetCString(writeDefaultValueDatum) : NULL;


	/* populate some info from relation attribute */
	Relation	relation = RelationIdGetRelation(relationId);

	TupleDesc	tupleDesc = RelationGetDescr(relation);

	Form_pg_attribute attr = TupleDescAttr(tupleDesc, attrNo - 1);

	field->name = NameStr(attr->attname);

	field->required = attr->attnotnull;

	field->doc = GetComment(relationId, RelationRelationId, attrNo);

	RelationClose(relation);

	/*
	 * we possibly find the sub field idx wrong here but we are not interested
	 * in the computed field id since we already found it from catalog.
	 */
	bool		forAddColumn = false;
	int			subFieldIndex = field->id;

	PGType		pgType = MakePGType(fieldType, fieldTypeMod);

	field->type = PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

	field->duckSerializedInitialDefault =
		!initialDefaultValueIsNull ?
		GetDuckSerializedIcebergFieldInitialDefault(field->initialDefault, field->type) :
		NULL;

	return field;
}


/*
* Wrapper around GetRegisteredFieldForAttributes() for a single attribute.
* If you are looking for multiple attributes, use GetRegisteredFieldForAttributes() as
* it is more efficient.
*/
DataFileSchemaField *
GetRegisteredFieldForAttribute(Oid relationId, AttrNumber attrNo)
{
	List	   *fields = GetRegisteredFieldForAttributes(relationId, list_make1_int(attrNo));

	if (list_length(fields) != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("missing field for column")));
	}

	return linitial(fields);
}

/*
* GetAttributeForFieldId gets the attribute number for a given field ID.
*/
AttrNumber
GetAttributeForFieldId(Oid relationId, int fieldId)
{
	if (IsInternalIcebergTable(relationId))
	{
		return GetAttributeForFieldIdForInternalIcebergTable(relationId, fieldId);
	}
	else
	{
		Assert(IsExternalIcebergTable(relationId));

		char	   *currentMetadataPath = GetIcebergMetadataLocation(relationId, false);

		return GetAttributeForFieldIdForExternalIcebergTable(currentMetadataPath, relationId, fieldId);
	}
}


/*
* GetAttributeForFieldIdForInternalIcebergTable gets the attribute number for a given field ID
* for internal Iceberg tables from catalog.
*/
static AttrNumber
GetAttributeForFieldIdForInternalIcebergTable(Oid relationId, int fieldId)
{
	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT4OID, fieldId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE("SELECT pg_attnum FROM " MAPPING_TABLE_NAME
				" WHERE table_name OPERATOR(pg_catalog.=) $1"
				" AND field_id OPERATOR(pg_catalog.=) $2", readOnly);

	/* there is a primary key on these filters */
	Assert(SPI_processed == 1);

	bool		isNull = false;
	AttrNumber	attrNo = GET_SPI_VALUE(INT2OID, 0, 1, &isNull);

	Assert(!isNull);

	SPI_END();

	return attrNo;
}


/*
* GetAttributeForFieldIdForExternalIcebergTable gets the attribute number for a given field ID
* for external Iceberg tables from iceberg metadata.
 */
static AttrNumber
GetAttributeForFieldIdForExternalIcebergTable(char *metadataPath, Oid relationId, int fieldId)
{
	DataFileSchema *schema = GetDataFileSchemaForExternalIcebergTable(metadataPath);

	DataFileSchemaField *schemaField = GetDataFileSchemaFieldById(schema, fieldId);

	/*
	 * searching attribute number by name is safe here as we do not allow
	 * modifications to external tables
	 */
	char	   *attrName = pstrdup(schemaField->name);

	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, attrName, false);

	SPI_START();

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE("SELECT attnum FROM pg_attribute "
				"WHERE attrelid OPERATOR(pg_catalog.=) $1 "
				"AND attname OPERATOR(pg_catalog.=) $2 "
				"AND NOT attisdropped", readOnly);

	/* there is a primary key on these filters */
	Assert(SPI_processed == 1);

	bool		isNull = false;
	AttrNumber	attrNo = GET_SPI_VALUE(INT2OID, 0, 1, &isNull);

	Assert(!isNull);

	SPI_END();

	return attrNo;
}


/*
 * GetDataFileSchemaForInternalIcebergTable gets a table schema based on the MAPPING_TABLE_NAME.
 *
 * The function also asserts that the field IDs in the MAPPING_TABLE_NAME match
 * the field IDs in the iceberg metadata.
 */
DataFileSchema *
GetDataFileSchemaForInternalIcebergTable(Oid relationId)
{
	/* iterate on the attributes of the relation */
	Relation	rel = RelationIdGetRelation(relationId);

	TupleDesc	tupDesc = RelationGetDescr(rel);

	DataFileSchema *schema = palloc0(sizeof(DataFileSchema));

	schema->fields = palloc0(sizeof(DataFileSchemaField) * tupDesc->natts);

	size_t		nonDroppedColumnCount = 0;
	List	   *attrNos = NIL;

	for (int attrIdx = 0; attrIdx < tupDesc->natts; attrIdx++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupDesc, attrIdx);

		/* skip dropped attributes */
		if (attr->attisdropped)
		{
			continue;
		}

		attrNos = lappend_int(attrNos, attr->attnum);
		nonDroppedColumnCount++;
	}

	/*
	 * Go to the catalog once per table, it is guaranteed that the fields are
	 * returned in the same order as the input attrNos.
	 */
	List	   *fields = GetRegisteredFieldForAttributes(relationId, attrNos);

	Assert(list_length(fields) == nonDroppedColumnCount);

	for (int fieldIndex = 0; fieldIndex < list_length(fields); fieldIndex++)
	{
		DataFileSchemaField *field = list_nth(fields, fieldIndex);

		schema->fields[fieldIndex] = *field;

#ifdef USE_ASSERT_CHECKING
		if (EnableHeavyAsserts)
		{
			/*
			 * It is guaranteed that fields and attrNos are in the same order.
			 */
			int			attrNo = list_nth_int(attrNos, fieldIndex);
			DataFileSchemaField *fieldAssert = GetRegisteredFieldForAttribute(relationId, attrNo);

			Assert(field->id == fieldAssert->id);
		}
#endif
	}

	schema->nfields = nonDroppedColumnCount;

	RelationClose(rel);

#ifdef USE_ASSERT_CHECKING
	if (EnableHeavyAsserts)
		AssertAllNonDroppedColumnsHaveRegisteredFieldIds(relationId);
#endif

	return schema;
}


/*
 * GetLeafFieldsForInternalIcebergTable gets a list of leaf fields from
 * the MAPPING_TABLE_NAME.
 */
List *
GetLeafFieldsForInternalIcebergTable(Oid relationId)
{
	List	   *leafFields = NIL;

	MemoryContext currentContext = CurrentMemoryContext;

	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "WITH RECURSIVE field_hierarchy AS ( "
	/* -- Base case: Start with fields at top level of not-dropped columns */
					 "	SELECT "
					 "		table_name,"
					 "		field_id,"
					 "		field_pg_type,"
					 "		field_pg_typemod,"
					 "		parent_field_id,"
					 "		pg_attnum AS top_level_pg_attnum,"
					 "		1 AS level"
					 "	FROM " MAPPING_TABLE_NAME " f JOIN pg_attribute attr "
					 "    ON (attr.attrelid OPERATOR(pg_catalog.=) f.table_name AND attr.attnum OPERATOR(pg_catalog.=) f.pg_attnum) "
					 "	WHERE parent_field_id IS NULL"
					 "    AND NOT attr.attisdropped"
					 "	  AND table_name OPERATOR(pg_catalog.=) $1"

					 "	UNION ALL"

	/* -- Recursive case: Find sub-fields */
					 "	SELECT "
					 "		f.table_name,"
					 "		f.field_id,"
					 "		f.field_pg_type,"
					 "		f.field_pg_typemod,"
					 "		f.parent_field_id,"
					 "		fh.top_level_pg_attnum,"
					 "		fh.level + 1 as level"
					 "	FROM " MAPPING_TABLE_NAME " f JOIN field_hierarchy fh"
					 "	  ON f.parent_field_id OPERATOR(pg_catalog.=) fh.field_id AND f.table_name OPERATOR(pg_catalog.=) fh.table_name"
					 ") "

	/* base query */
					 "SELECT field_id, field_pg_type, field_pg_typemod, level "
					 "FROM field_hierarchy fh "
					 "WHERE NOT EXISTS ("
	/* If a field never appears as a parent, it's a leaf */
					 "	SELECT 1"
					 "	FROM " MAPPING_TABLE_NAME " f"
					 "	WHERE f.parent_field_id OPERATOR(pg_catalog.=) fh.field_id"
					 "	  AND f.table_name OPERATOR(pg_catalog.=) fh.table_name"
					 ") "
					 "ORDER BY field_id;");

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(currentContext);

		bool		isNull = false;

		int			fieldId = GET_SPI_VALUE(INT4OID, rowIndex, 1, &isNull);
		Oid			typeOid = GET_SPI_VALUE(OIDOID, rowIndex, 2, &isNull);
		int32		typmod = GET_SPI_VALUE(INT4OID, rowIndex, 3, &isNull);
		int32		level = GET_SPI_VALUE(INT4OID, rowIndex, 4, &isNull);

		PGType		pgType = MakePGType(typeOid, typmod);

		bool		forAddColumn = false;
		int			subFieldIndex = fieldId;
		Field	   *field = PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

		Assert(field != NULL && field->type == FIELD_TYPE_SCALAR);

		LeafField  *leafField = palloc0(sizeof(LeafField));

		leafField->fieldId = fieldId;
		leafField->field = field;
		leafField->pgType = pgType;
		leafField->duckTypeName = IcebergTypeNameToDuckdbTypeName(field->field.scalar.typeName);
		leafField->level = level;

		leafFields = lappend(leafFields, leafField);

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);


	return leafFields;
}


/*
 * UpdateRegisteredFieldWriteDefaultForAttribute updates the write default value for a given column.
 */
void
UpdateRegisteredFieldWriteDefaultForAttribute(Oid relationId, AttrNumber attNum, const char *writeDefault)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Now, with SPI select from field_id_mappings (relation_id, pg_attnum)
	 */
	StringInfo	query = makeStringInfo();

	appendStringInfo(query, "UPDATE " MAPPING_TABLE_NAME " SET write_default = $1 "
					 "WHERE table_name OPERATOR(pg_catalog.=) $2 AND "
					 "pg_attnum OPERATOR(pg_catalog.=) $3 AND "
	/* we should update top level field's write default */
					 "parent_field_id IS NULL "
					 "RETURNING field_id");

	DECLARE_SPI_ARGS(3);

	SPI_ARG_VALUE(1, TEXTOID, writeDefault, writeDefault == NULL);
	SPI_ARG_VALUE(2, OIDOID, relationId, false);
	SPI_ARG_VALUE(3, INT2OID, attNum, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	if (SPI_processed != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to update write default for column %d in relation %u", attNum, relationId)));
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}

/*
* GetLargestRegisteredFieldId returns the largest field ID for a given relation.
*/
int
GetLargestRegisteredFieldId(Oid relationId)
{
	int			fieldId = INVALID_FIELD_ID;

	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Now, with SPI select from field_id_mappings (relation_id, pg_attnum)
	 */
	StringInfo	query = makeStringInfo();

	appendStringInfo(query, "SELECT MAX(field_id) FROM " MAPPING_TABLE_NAME " WHERE "
					 "table_name OPERATOR(pg_catalog.=) $1");

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	if (SPI_processed > 0)
	{
		bool		isNull = false;

		/* enforced in the catalog */
		Assert(!isNull);

		fieldId = GET_SPI_VALUE(INT4OID, 0, 1, &isNull);
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return fieldId;
}


/*
* RegisterIcebergColumnMapping inserts field mapping for a relation column.
*/
void
RegisterIcebergColumnMapping(Oid relationId, Field * field,
							 AttrNumber attNo, int parentFieldId, PGType pgType,
							 int fieldId, const char *writeDefault, const char *initialDefault)
{
	EnsureIcebergField(field);

	/*
	 * we always insert given field mapping before recursing into its
	 * subfields
	 */
	InsertFieldMapping(relationId, fieldId, attNo, pgType,
					   writeDefault, initialDefault,
					   parentFieldId);

	/* update parent field id before recursing into subfields */
	parentFieldId = fieldId;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			{
				/* no subfields for scalar field */
				break;
			}

		case FIELD_TYPE_LIST:
			{
				FieldList  *listField = &field->field.list;

				Oid			elementOid = get_element_type(pgType.postgresTypeOid);
				PGType		elementPGType = MakePGType(elementOid, pgType.postgresTypeMod);

				/* we register defaults only for top level fields */
				const char *elementWriteDefault = NULL;
				const char *elementInitialDefault = NULL;

				int			elementFieldId = listField->elementId;

				Field	   *elementField = listField->element;

				RegisterIcebergColumnMapping(relationId, elementField,
											 attNo, parentFieldId, elementPGType,
											 elementFieldId, elementWriteDefault,
											 elementInitialDefault);

				break;
			}

		case FIELD_TYPE_MAP:
			{
				FieldMap   *mapField = &field->field.map;

				PGType		keyPGType = GetMapKeyType(pgType.postgresTypeOid);

				/* we register defaults only for top level fields */
				const char *keyWriteDefault = NULL;
				const char *keyInitialDefault = NULL;

				int			keyFieldId = mapField->keyId;

				Field	   *keyField = mapField->key;

				RegisterIcebergColumnMapping(relationId, keyField, attNo,
											 parentFieldId, keyPGType, keyFieldId,
											 keyWriteDefault, keyInitialDefault);

				PGType		valuePGType = GetMapValueType(pgType.postgresTypeOid);

				/* we register defaults only for top level fields */
				const char *valueWriteDefault = NULL;
				const char *valueInitialDefault = NULL;

				int			valueFieldId = mapField->valueId;

				Field	   *valueField = mapField->value;

				RegisterIcebergColumnMapping(relationId, valueField, attNo,
											 parentFieldId, valuePGType, valueFieldId,
											 valueWriteDefault, valueInitialDefault);

				break;

			}

		case FIELD_TYPE_STRUCT:
			{
				TupleDesc	tupDesc = lookup_rowtype_tupdesc(pgType.postgresTypeOid, pgType.postgresTypeMod);

				for (int fieldIndex = 0; fieldIndex < tupDesc->natts; fieldIndex++)
				{
					/*
					 * we are sure that the struct element order matches the
					 * tupledesc order. there cannot be dropped columns in
					 * type. (we disallow altering type in iceberg table)
					 */
					Form_pg_attribute attr = TupleDescAttr(tupDesc, fieldIndex);

					PGType		subFieldPGType = MakePGType(attr->atttypid, attr->atttypmod);

					FieldStructElement *structElementField = &field->field.structType.fields[fieldIndex];

					/* we register defaults only for top level fields */
					const char *subFieldWriteDefault = NULL;
					const char *subFieldInitialDefault = NULL;

					int			subFieldId = structElementField->id;

					Field	   *subField = structElementField->type;

					RegisterIcebergColumnMapping(relationId, subField, attNo,
												 parentFieldId, subFieldPGType, subFieldId,
												 subFieldWriteDefault, subFieldInitialDefault);
				}

				ReleaseTupleDesc(tupDesc);

				break;
			}

		default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unsupported field type %d", field->type)));
			}
	}
}


/*
* InsertFieldMapping inserts a field ID mapping for a given column. This is a low-level
* function and should not be called directly as it does not check if the field ID already exists.
*/
static void
InsertFieldMapping(Oid relationId, int fieldId, AttrNumber attrNo, PGType pgType,
				   const char *writeDefault, const char *initialDefault, int parentFieldId)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	/*
	 * Now, with SPI select from field_id_mappings (relation_id, pg_attnum)
	 */
	StringInfo	query = makeStringInfo();

	appendStringInfo(query, "INSERT INTO " MAPPING_TABLE_NAME ""
					 "(table_name, field_id, pg_attnum, "
					 "parent_field_id, field_pg_type, "
					 "field_pg_typemod, initial_default, "
					 "write_default) VALUES "
					 "($1, $2, $3, $4, $5, $6, $7, $8)");

	DECLARE_SPI_ARGS(8);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT4OID, fieldId, false);
	SPI_ARG_VALUE(3, INT2OID, attrNo, false);
	SPI_ARG_VALUE(4, INT4OID, parentFieldId, parentFieldId == INVALID_FIELD_ID);
	SPI_ARG_VALUE(5, OIDOID, pgType.postgresTypeOid, false);
	SPI_ARG_VALUE(6, INT4OID, pgType.postgresTypeMod, false);
	SPI_ARG_VALUE(7, TEXTOID, initialDefault, initialDefault == NULL);
	SPI_ARG_VALUE(8, TEXTOID, writeDefault, writeDefault == NULL);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


#ifdef USE_ASSERT_CHECKING
/*
* Read all the registered field ids for a given relationId, and make sure that all the
* non-dropped columns have field ids.
*/
static void
AssertAllNonDroppedColumnsHaveRegisteredFieldIds(Oid relationId)
{
	/* read all the fieldIds for the given table from the MAPPING TABLE */
	List	   *registeredAttnums = GetAllRegisteredAttnumsForTopLevelColumns(relationId);

	/* iterate on the attributes of the relation */
	Relation	rel = RelationIdGetRelation(relationId);

	for (int tupleDescIndex = 0; tupleDescIndex < rel->rd_att->natts; tupleDescIndex++)
	{
		Form_pg_attribute attr = TupleDescAttr(rel->rd_att, tupleDescIndex);

		/* skip dropped attributes */
		if (attr->attisdropped)
		{
			continue;
		}

		ListCell   *registeredAttnumCell;
		bool		found = false;

		foreach(registeredAttnumCell, registeredAttnums)
		{
			int			registeredAttnum = lfirst_int(registeredAttnumCell);

			if (attr->attnum == registeredAttnum)
			{
				found = true;
				break;
			}
		}

		if (!found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("column %s does not have a fieldId", NameStr(attr->attname))));
		}
	}

	RelationClose(rel);
}



static List *
GetAllRegisteredAttnumsForTopLevelColumns(Oid relationId)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	MemoryContext currentContext = CurrentMemoryContext;

	/*
	 * with SPI read the MAPPING table for the given table, return a list of
	 * postgres attribute nums
	 */
	StringInfo	query = makeStringInfo();

	appendStringInfo(query, "SELECT pg_attnum FROM " MAPPING_TABLE_NAME " WHERE table_name "
					 "OPERATOR(pg_catalog.=) $1 AND parent_field_id IS NULL");

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	List	   *fieldIds = NIL;

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull = false;
		int			fieldId = GET_SPI_VALUE(INT4OID, rowIndex, 1, &isNull);

		Assert(!isNull);

		/* append in the currentContext */

		MemoryContext spiContext = MemoryContextSwitchTo(currentContext);

		fieldIds = lappend_int(fieldIds, fieldId);
		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return fieldIds;
}

#endif
