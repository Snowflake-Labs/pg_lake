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
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_json_serde.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/util/catalog_type.h"
#include "parser/scansup.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static IcebergTableSchema * GetIcebergTableSchemaById(IcebergTableMetadata * metadata, int schemaId);
static List *GetLeafFieldsForField(Field * field, int fieldId, int *level);
static void LowercaseStructElementNames(FieldStructElement * elements, size_t elementCount);
static void LowercaseFieldNames(Field * field);

/*
 * GetIcebergTableSchemaByIdFromTableMetadata gets the schema by id from given table metadata.
 */
IcebergTableSchema *
GetIcebergTableSchemaByIdFromTableMetadata(IcebergTableMetadata * metadata, int schemaId)
{
	if (metadata == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table metadata is NULL")));
	}

	IcebergTableSchema *schema = NULL;

	int			schemaIdx = 0;

	for (schemaIdx = 0; schemaIdx < metadata->schemas_length; schemaIdx++)
	{
		IcebergTableSchema *currentSchema = &metadata->schemas[schemaIdx];

		if (currentSchema->schema_id == schemaId)
		{
			schema = currentSchema;
			break;
		}
	}

	if (schema == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("schema not found for schema id %d", schemaId)));
	}

	return schema;
}


/*
 * GetCurrentIcebergTableSchema gets the current Iceberg schema from the table metadata.
 */
IcebergTableSchema *
GetCurrentIcebergTableSchema(IcebergTableMetadata * metadata)
{
	int32_t		schemaId = metadata->current_schema_id;

	if (schemaId == -1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("current schema id is not set")));
	}

	return GetIcebergTableSchemaById(metadata, schemaId);
}

/*
 * GetIcebergTableSchemaById gets the schema by id from given table metadata.
 */
static IcebergTableSchema *
GetIcebergTableSchemaById(IcebergTableMetadata * metadata, int schemaId)
{
	if (metadata == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table metadata is NULL")));
	}

	IcebergTableSchema *schema = NULL;

	int			schemaIdx = 0;

	for (schemaIdx = 0; schemaIdx < metadata->schemas_length; schemaIdx++)
	{
		IcebergTableSchema *currentSchema = &metadata->schemas[schemaIdx];

		if (currentSchema->schema_id == schemaId)
		{
			schema = currentSchema;
			break;
		}
	}

	if (schema == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("schema not found for schema id %d", schemaId)));
	}

	return schema;
}


/*
 * GetLeafFieldsFromIcebergMetadata returns all leaf fields for the current schema
 * from the iceberg metadata.
 */
List *
GetLeafFieldsFromIcebergMetadata(IcebergTableMetadata * metadata)
{
	IcebergTableSchema *schema = GetCurrentIcebergTableSchema(metadata);

	return GetLeafFieldsForIcebergSchema(schema);
}


/*
 * GetLeafFieldsForIcebergSchema returns all leaf fields for the given iceberg schema.
 */
List *
GetLeafFieldsForIcebergSchema(IcebergTableSchema * schema)
{
	List	   *leafFields = NIL;

	for (int fieldIndex = 0; fieldIndex < schema->fields_length; fieldIndex++)
	{
		DataFileSchemaField *topLevelField = &schema->fields[fieldIndex];

		/* in our convention, the level starts from 1 */
		int			level = 1;

		List	   *leafFieldsForField = GetLeafFieldsForField(topLevelField->type,
															   topLevelField->id,
															   &level);

		leafFields = list_concat(leafFields, leafFieldsForField);
	}

	return leafFields;
}


/*
 * GetDataFileSchemaFieldById gets the DataFileSchemaField for the given
 * iceberg field id.
 */
DataFileSchemaField *
GetDataFileSchemaFieldById(DataFileSchema * schema, int fieldId)
{
	DataFileSchemaField *schemaField = NULL;

	for (size_t fieldIdx = 0; fieldIdx < schema->nfields; fieldIdx++)
	{
		DataFileSchemaField *field = &schema->fields[fieldIdx];

		if (field->id == fieldId)
		{
			schemaField = field;
			break;
		}
	}

	if (schemaField == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("field ID %d not found",
						fieldId)));
	}

	return schemaField;
}


/*
 * LowercaseIcebergTableMetadataNames folds the column and struct field names
 * of every schema in the metadata to lowercase, in place. Field ids are left
 * alone, so data is still bound by id.
 */
void
LowercaseIcebergTableMetadataNames(IcebergTableMetadata * metadata)
{
	for (size_t schemaIdx = 0; schemaIdx < metadata->schemas_length; schemaIdx++)
	{
		IcebergTableSchema *schema = &metadata->schemas[schemaIdx];

		LowercaseStructElementNames(schema->fields, schema->fields_length);
	}
}


/*
 * LowercaseStructElementNames folds the names of the given sibling fields and
 * of everything nested below them. Two siblings that differ only in case
 * would become one name, which neither Postgres nor DuckDB can represent.
 */
static void
LowercaseStructElementNames(FieldStructElement * elements, size_t elementCount)
{
	for (size_t elementIdx = 0; elementIdx < elementCount; elementIdx++)
	{
		FieldStructElement *element = &elements[elementIdx];

		if (element->name != NULL)
		{
			const char *originalName = element->name;
			char	   *foldedName = downcase_identifier(originalName, strlen(originalName),
														 false, false);

			for (size_t previousIdx = 0; previousIdx < elementIdx; previousIdx++)
			{
				const char *previousName = elements[previousIdx].name;

				if (previousName != NULL && strcmp(previousName, foldedName) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_DUPLICATE_COLUMN),
							 errmsg("Iceberg field \"%s\" collides with another field "
									"when lowercased to \"%s\"",
									originalName, foldedName),
							 errhint("Create the table without the %s option.",
									 LOWERCASE_COLUMN_NAMES_OPTION)));
			}

			element->name = foldedName;
		}

		LowercaseFieldNames(element->type);
	}
}


/*
 * LowercaseFieldNames folds the struct field names nested in the given type.
 */
static void
LowercaseFieldNames(Field * field)
{
	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			break;

		case FIELD_TYPE_LIST:
			LowercaseFieldNames(field->field.list.element);
			break;

		case FIELD_TYPE_MAP:
			LowercaseFieldNames(field->field.map.key);
			LowercaseFieldNames(field->field.map.value);
			break;

		case FIELD_TYPE_STRUCT:
			LowercaseStructElementNames(field->field.structType.fields,
										field->field.structType.nfields);
			break;

		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unsupported field type %d", field->type)));
	}
}


/*
 * GetLeafFieldsForField returns the leaf fields for the given field.
 * It recursively traverses the field tree and collects all leaf fields, and
 * "level" is the current level of the field in the tree.
 * The level starts from 1 for the top-level field.
 */
static List *
GetLeafFieldsForField(Field * field, int fieldId, int *level)
{
	List	   *leafFields = NIL;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			{
				LeafField  *leafField = palloc0(sizeof(LeafField));

				leafField->fieldId = fieldId;
				leafField->field = field;
				leafField->duckTypeName = IcebergTypeNameToDuckdbTypeName(field->field.scalar.typeName);
				leafField->pgType = IcebergFieldToPostgresType(field);
				leafField->level = *level;

				leafFields = lappend(leafFields, leafField);

				break;
			}

		case FIELD_TYPE_LIST:
			{
				(*level)++;
				leafFields = GetLeafFieldsForField(field->field.list.element,
												   field->field.list.elementId,
												   level);

				(*level)--;
				break;
			}

		case FIELD_TYPE_MAP:
			{
				(*level)++;
				leafFields = GetLeafFieldsForField(field->field.map.key,
												   field->field.map.keyId,
												   level);

				List	   *leafFieldsForValue = GetLeafFieldsForField(field->field.map.value,
																	   field->field.map.valueId,
																	   level);

				(*level)--;
				leafFields = list_concat(leafFields, leafFieldsForValue);

				break;
			}

		case FIELD_TYPE_STRUCT:
			{
				(*level)++;
				for (int fieldIndex = 0; fieldIndex < field->field.structType.nfields; fieldIndex++)
				{
					FieldStructElement *structElement = &field->field.structType.fields[fieldIndex];

					List	   *leafFieldsForStructElement = GetLeafFieldsForField(structElement->type,
																				   structElement->id,
																				   level);

					leafFields = list_concat(leafFields, leafFieldsForStructElement);
				}
				(*level)--;

				break;
			}

		default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("unsupported field type %d", field->type)));
			}
	}

	return leafFields;
}


/*
* RebuildIcebergSchemaFromDataFileSchema rebuilds the iceberg schema from the
* given data file schema.
*/
IcebergTableSchema *
RebuildIcebergSchemaFromDataFileSchema(Oid foreignTableOid, DataFileSchema * schema,
									   int *last_column_id)
{
	size_t		totalFields = schema->nfields;
	IcebergTableSchema *newSchema = palloc0(sizeof(IcebergTableSchema));

	/*
	 * A schema's type is always struct, individual fields could be base types
	 * (e.g., int), list (e.g., array), struct (e.g., composite keys) or map.
	 */
	newSchema->type = "struct";
	newSchema->identifier_field_ids = NULL;
	newSchema->identifier_field_ids_length = 0;

	/* get the ownership of DataFileSchema by copying it */
	DataFileSchema *copiedDataFileSchema = DeepCopyDataFileSchema(schema);

	newSchema->fields = copiedDataFileSchema->fields;
	newSchema->fields_length = copiedDataFileSchema->nfields;

	if (totalFields > 0)
		*last_column_id = schema->fields[totalFields - 1].id;

	return newSchema;
}
