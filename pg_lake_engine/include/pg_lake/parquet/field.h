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
 * field.h stores the field definitions for generic data file schema.
 * It is used to represent the schema of an Iceberg table according to
 * the Iceberg specification. Iceberg spec supports parquet, avro, and orc
 * data files. That is why the structs in this file are generic over
 * these data formats.
 *
 * 4 types of fields are supported:
 *  1. Scalar (Corresponds to scalar types in Postgres.)
 *  2. List (Corresponds to arrays in Postgres.)
 *  3. Map (Corresponds to pg_map in Postgres.)
 *  4. Struct (Corresponds to table row type or composite type in Postgres.)
 *     The schema of a parquet file can be represented as a struct. Similarly,
 *     a field in the schema can be represented as a struct element.
 */

#pragma once

#include "nodes/pg_list.h"

#include "pg_lake/pgduck/type.h"

/*
 * Reserved _row_id field ID used for Iceberg
 * See: https://iceberg.apache.org/spec/#reserved-field-ids
 */
#define ICEBERG_ROWID_FIELD_ID 2147483540

/*
 * FieldType represents the type of a field.
 */
typedef enum FieldType
{
	FIELD_TYPE_SCALAR,
	FIELD_TYPE_LIST,
	FIELD_TYPE_MAP,
	FIELD_TYPE_STRUCT
}			FieldType;

/* forward declaration */
struct Field;

/*
 * FieldScalar represents a primitive type field. They are the leaf nodes of the schema.
 * Their parent can be either a list, map, or struct.
 * Corresponds to scalar types in Postgres.
 */
typedef struct FieldScalar
{
	const char *typeName;
}			FieldScalar;

/*
 * FieldList represents a list is a collection of values with some element type.
 * The element field has an integer id that is unique in the table schema.
 * Elements can be either optional or required. Element types may be any type.
 * Corresponds to arrays in Postgres.
 */
typedef struct FieldList
{
	int			elementId;
	bool		elementRequired;
	struct Field *element;
}			FieldList;

/*
 * FieldStructElement represents an element of a struct field.
 * See FieldStruct for more details.
 *
 * Corresponds to attributes (table column or composite attribute)
 * in Postgres.
 */
typedef struct FieldStructElement
{
	int			id;
	const char *name;
	bool		required;
	const char *doc;
	const char *writeDefault;
	const char *initialDefault;
	const char *duckSerializedInitialDefault;
	struct Field *type;
}			FieldStructElement;

/*
 * FieldStruct is a tuple of typed values. Each field in the tuple is named
 * and has an integer id that is unique in the table schema. Each field can be
 * either optional or required, meaning that values can (or cannot) be null.
 * Fields may be any type. Fields may have an optional comment or doc string.
 * Fields can have default values.
 *
 * Corresponds to table row type or composite type in Postgres.
 */
typedef struct FieldStruct
{
	FieldStructElement *fields;
	size_t		nfields;
}			FieldStruct;

/*
 * FieldMap represents is a collection of key-value pairs with a key type and a value type.
 * Both the key field and value field each have an integer id that is unique in the table schema.
 * Map keys are required and map values can be either optional or required. Both map keys and map
 * values may be any type, including nested types.
 *
 * Corresponds to pg_map in Postgres.
 */
typedef struct FieldMap
{
	int			keyId;
	struct Field *key;
	int			valueId;
	bool		valueRequired;
	struct Field *value;
}			FieldMap;

/*
 * Field represents a field.
 * It can be a scalar, list, map, or struct.
 */
typedef struct Field
{
	FieldType	type;

	union
	{
		FieldScalar scalar;
		FieldList	list;
		FieldMap	map;
		FieldStruct structType;
	}			field;
}			Field;

/*
 * A data file schema is represented by FieldStruct. To make the context
 * (composite or schema) clearer, we have aliases for the schema context.
 */
typedef FieldStruct DataFileSchema;
typedef FieldStructElement DataFileSchemaField;

extern PGDLLEXPORT DataFileSchema * DeepCopyDataFileSchema(const DataFileSchema * schema);
extern PGDLLEXPORT Field * DeepCopyField(const Field * field);
extern PGDLLEXPORT bool PGTypeRequiresConversionToIcebergString(Field * field, PGType pgType);
