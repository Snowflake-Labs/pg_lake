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
 * iceberg_field.c
 *  Contains functions for converting between Iceberg fields and Postgres types
 *  and some utility functions for Iceberg fields.
 *
 * `PostgresTypeToIcebergField`: converts Postgres type to corresponding
 *  Iceberg type.
 *
 * `IcebergFieldToPostgresType`: converts Iceberg field to corresponding
 *  Postgres type.
 *
 * Note that Iceberg field is a logical concept. The physical field type is
 * a generic struct `Field`. That is why we have `EnsureIcebergField` in
 * critical places to ensure that the field is a valid Iceberg field.
 */

#include "postgres.h"
#include "fmgr.h"

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_json_serde.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/util/string_utils.h"

#include "access/table.h"
#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "optimizer/optimizer.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/typcache.h"

typedef enum IcebergType
{
	ICEBERG_TYPE_INVALID,
	ICEBERG_TYPE_BOOLEAN,
	ICEBERG_TYPE_INT,
	ICEBERG_TYPE_LONG,
	ICEBERG_TYPE_FLOAT,
	ICEBERG_TYPE_DOUBLE,
	ICEBERG_TYPE_DECIMAL,
	ICEBERG_TYPE_DATE,
	ICEBERG_TYPE_TIME,
	ICEBERG_TYPE_TIMESTAMP,
	ICEBERG_TYPE_TIMESTAMPTZ,
	ICEBERG_TYPE_STRING,
	ICEBERG_TYPE_UUID,
	ICEBERG_TYPE_FIXED_BINARY,
	ICEBERG_TYPE_BINARY,
	ICEBERG_TYPE_LIST,
	ICEBERG_TYPE_MAP,
	ICEBERG_TYPE_STRUCT,
}			IcebergType;

typedef struct IcebergTypeInfo
{
	IcebergType type;

	/* additional context for decimal */
	int			precision;
	int			scale;
}			IcebergTypeInfo;

typedef struct IcebergToDuckDBType
{
	const char *icebergTypeName;
	IcebergType icebergType;
	DuckDBType	duckdbType;
}			IcebergToDuckDBType;


static IcebergToDuckDBType IcebergToDuckDBTypes[] =
{
	{
		"boolean", ICEBERG_TYPE_BOOLEAN, DUCKDB_TYPE_BOOLEAN,
	},
	{
		"int", ICEBERG_TYPE_INT, DUCKDB_TYPE_INTEGER
	},
	{
		"long", ICEBERG_TYPE_LONG, DUCKDB_TYPE_BIGINT
	},
	{
		"float", ICEBERG_TYPE_FLOAT, DUCKDB_TYPE_FLOAT
	},
	{
		"double", ICEBERG_TYPE_DOUBLE, DUCKDB_TYPE_DOUBLE
	},
	{
		"decimal", ICEBERG_TYPE_DECIMAL, DUCKDB_TYPE_DECIMAL
	},
	{
		"date", ICEBERG_TYPE_DATE, DUCKDB_TYPE_DATE
	},
	{
		"time", ICEBERG_TYPE_TIME, DUCKDB_TYPE_TIME
	},
	{
		"timestamp", ICEBERG_TYPE_TIMESTAMP, DUCKDB_TYPE_TIMESTAMP
	},
	{
		"timestamptz", ICEBERG_TYPE_TIMESTAMPTZ, DUCKDB_TYPE_TIMESTAMP_TZ
	},
	{
		"string", ICEBERG_TYPE_STRING, DUCKDB_TYPE_VARCHAR
	},
	{
		"uuid", ICEBERG_TYPE_UUID, DUCKDB_TYPE_UUID
	},
	{
		"fixed", ICEBERG_TYPE_FIXED_BINARY, DUCKDB_TYPE_BLOB
	},
	{
		"binary", ICEBERG_TYPE_BINARY, DUCKDB_TYPE_BLOB
	},
	{
		"list", ICEBERG_TYPE_LIST, DUCKDB_TYPE_LIST
	},
	{
		"map", ICEBERG_TYPE_MAP, DUCKDB_TYPE_MAP
	},
	{
		"struct", ICEBERG_TYPE_STRUCT, DUCKDB_TYPE_STRUCT
	},
};

static DuckDBType GetDuckDBTypeFromIcebergType(IcebergType icebergType);
static char *PostgresBaseTypeIdToIcebergTypeName(PGType pgType);
static IcebergTypeInfo * GetIcebergTypeInfoFromTypeName(const char *typeName);
static const char *GetIcebergJsonSerializedConstDefaultIfExists(const char *attrName, Field * field, Node *defaultExpr);


/*
 * PostgresTypeToIcebergField converts a PostgreSQL type ID and typemod
 * to an Iceberg Field.
 *
 * 2 use cases:
 * 1. When registering new fields from Postgres columns when CREATE TABLE
 *    or ALTER TABLE ADD COLUMN,
 * 2. When reading fields from internal catalog tables, we need to create
 *    fields from catalog info.
 *
 * Based on https://iceberg.apache.org/spec/#schemas
 */
Field *
PostgresTypeToIcebergField(PGType pgType, bool forAddColumn, int *subFieldIndex)
{
	Oid			typeId = pgType.postgresTypeOid;
	int32		typeMod = pgType.postgresTypeMod;

	Field	   *field = palloc0(sizeof(Field));

	if (type_is_array(typeId))
	{
		field->type = FIELD_TYPE_LIST;
		field->field.list.elementRequired = false;
		field->field.list.elementId = *subFieldIndex + 1;

		*subFieldIndex = field->field.list.elementId;
		PGType		elementPGType = MakePGType(get_element_type(typeId), typeMod);

		field->field.list.element = PostgresTypeToIcebergField(elementPGType, forAddColumn, subFieldIndex);
	}
	else if (get_typtype(typeId) == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupleDesc = lookup_rowtype_tupdesc(typeId, typeMod);
		int			fieldCount = tupleDesc->natts;

		field->type = FIELD_TYPE_STRUCT;
		field->field.structType.nfields = fieldCount;
		field->field.structType.fields = palloc0(sizeof(FieldStructElement) * fieldCount);

		for (int fieldIndex = 0; fieldIndex < fieldCount; ++fieldIndex)
		{
			Form_pg_attribute attr = TupleDescAttr(tupleDesc, fieldIndex);

			FieldStructElement *structElementField = &field->field.structType.fields[fieldIndex];

			structElementField->id = *subFieldIndex + 1;
			*subFieldIndex = structElementField->id;

			structElementField->name = pstrdup(NameStr(attr->attname));

			structElementField->required = attr->attnotnull;

			PGType		subFieldPGType = MakePGType(attr->atttypid, attr->atttypmod);

			structElementField->type = PostgresTypeToIcebergField(subFieldPGType, forAddColumn, subFieldIndex);

			structElementField->writeDefault = GetIcebergJsonSerializedDefaultExpr(tupleDesc, attr->attnum, structElementField);

			if (structElementField->writeDefault && forAddColumn)
			{
				structElementField->initialDefault = structElementField->writeDefault;
			}

			/* Postgres does not allow comment on a struct field */
			structElementField->doc = NULL;
		}

		ReleaseTupleDesc(tupleDesc);
	}
	else if (IsMapTypeOid(typeId))
	{
		field->type = FIELD_TYPE_MAP;

		PGType		keyPgType = GetMapKeyType(typeId);

		field->field.map.keyId = *subFieldIndex + 1;
		*subFieldIndex = field->field.map.keyId;

		field->field.map.key = PostgresTypeToIcebergField(keyPgType, forAddColumn, subFieldIndex);

		PGType		valuePgType = GetMapValueType(typeId);

		field->field.map.valueId = *subFieldIndex + 1;
		*subFieldIndex = field->field.map.valueId;
		field->field.map.value = PostgresTypeToIcebergField(valuePgType, forAddColumn, subFieldIndex);
		field->field.map.valueRequired = false;
	}
	else
	{
		char	   *icebergTypeName = PostgresBaseTypeIdToIcebergTypeName(pgType);

		field->type = FIELD_TYPE_SCALAR;
		field->field.scalar.typeName = pstrdup(icebergTypeName);
	}

	EnsureIcebergField(field);

	return field;
}


/*
 * IcebergFieldToPostgresType returns PGType from the given Iceberg
 * field.
 *
 * We make use of DuckDB types as an intermediate step to get the corresponding
 * PostgreSQL type. We first get the DuckDB type from the Iceberg type and then
 * get the corresponding PostgreSQL type from the DuckDB type. This works since
 * all Iceberg types can be mapped to DuckDB types.
 *
 * 1 use case:
 * 1. Get Postgres type from Field to serialize the Postgres type in duck format.
 */
PGType
IcebergFieldToPostgresType(Field * field)
{
	EnsureIcebergField(field);

	PGType		pgType = {InvalidOid, -1};

	const char *duckDBTypeName = NULL;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			{
				duckDBTypeName = IcebergTypeNameToDuckdbTypeName(field->field.scalar.typeName);

				int			pgTypeMod = -1;
				Oid			pgTypeOid = GetOrCreatePGTypeForDuckDBTypeName(duckDBTypeName, &pgTypeMod);

				pgType.postgresTypeOid = pgTypeOid;
				pgType.postgresTypeMod = pgTypeMod;

				break;
			}
		case FIELD_TYPE_LIST:
			{
				PGType		elementPGType =
					IcebergFieldToPostgresType(field->field.list.element);

				const char *elementDuckDBTypeName = GetFullDuckDBTypeNameForPGType(elementPGType);

				StringInfo	listTypeName = makeStringInfo();

				appendStringInfo(listTypeName, "%s[]", elementDuckDBTypeName);

				duckDBTypeName = listTypeName->data;

				int			arrayTypmod = -1;

				Oid			arrayTypeOid =
					GetOrCreatePGTypeForDuckDBTypeName(duckDBTypeName, &arrayTypmod);

				pgType.postgresTypeOid = arrayTypeOid;
				pgType.postgresTypeMod = arrayTypmod;

				break;
			}
		case FIELD_TYPE_MAP:
			{
				PGType		keyPGType =
					IcebergFieldToPostgresType(field->field.map.key);

				const char *keyDuckDBTypeName = GetFullDuckDBTypeNameForPGType(keyPGType);

				PGType		valuePGType =
					IcebergFieldToPostgresType(field->field.map.value);

				const char *valueDuckDBTypeName = GetFullDuckDBTypeNameForPGType(valuePGType);

				StringInfo	mapTypeName = makeStringInfo();

				appendStringInfo(mapTypeName, "%s(%s, %s)",
								 DUCKDB_MAP_TYPE_PREFIX,
								 keyDuckDBTypeName,
								 valueDuckDBTypeName);

				duckDBTypeName = mapTypeName->data;

				int			mapTypmod = -1;

				Oid			mapTypeOid = GetOrCreatePGTypeForDuckDBTypeName(duckDBTypeName,
																			&mapTypmod);

				pgType.postgresTypeOid = mapTypeOid;
				pgType.postgresTypeMod = mapTypmod;

				break;
			}
		case FIELD_TYPE_STRUCT:
			{
				StringInfo	structTypeName = makeStringInfo();

				appendStringInfo(structTypeName, "%s(", DUCKDB_STRUCT_TYPE_PREFIX);

				size_t		totalFields = field->field.structType.nfields;

				for (size_t fieldIdx = 0; fieldIdx < totalFields; fieldIdx++)
				{
					FieldStructElement *structElementField = &field->field.structType.fields[fieldIdx];

					const char *fieldName = structElementField->name;
					const char *quotedFieldName = QuoteDuckDBFieldName(pstrdup(fieldName));

					PGType		fieldPGType =
						IcebergFieldToPostgresType(structElementField->type);

					const char *fieldDuckDBTypeName = GetFullDuckDBTypeNameForPGType(fieldPGType);

					appendStringInfo(structTypeName, "%s %s",
									 quotedFieldName,
									 fieldDuckDBTypeName);

					if (fieldIdx < totalFields - 1)
					{
						appendStringInfo(structTypeName, ", ");
					}
				}

				appendStringInfo(structTypeName, ")");

				duckDBTypeName = structTypeName->data;

				int			structTypmod = -1;

				Oid			structTypeOid = GetOrCreatePGTypeForDuckDBTypeName(duckDBTypeName,
																			   &structTypmod);

				pgType.postgresTypeOid = structTypeOid;
				pgType.postgresTypeMod = structTypmod;

				break;
			}
		default:
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("pg_lake_copy: unrecognized iceberg field type %d",
									   field->type)));
				break;
			}
	}

	if (pgType.postgresTypeOid == InvalidOid)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_copy: type %s is currently not supported",
							   duckDBTypeName)));
	}

	return pgType;
}


/*
 * GetDuckDBTypeNameFromIcebergTypeName returns corresponding DuckDB type for
 * given Iceberg type.
 */
static DuckDBType
GetDuckDBTypeFromIcebergType(IcebergType icebergType)
{
	DuckDBType	duckdbType = DUCKDB_TYPE_INVALID;

	int			totalTypes = sizeof(IcebergToDuckDBTypes) / sizeof(IcebergToDuckDBTypes[0]);

	int			typeIndex = 0;

	for (typeIndex = 0; typeIndex < totalTypes; typeIndex++)
	{
		if (IcebergToDuckDBTypes[typeIndex].icebergType == icebergType)
		{
			duckdbType = IcebergToDuckDBTypes[typeIndex].duckdbType;
			break;
		}
	}

	if (duckdbType == DUCKDB_TYPE_INVALID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unrecognized iceberg type id %d", icebergType)));
	}

	return duckdbType;
}

/*
 * IcebergTypeNameToDuckdbTypeName returns corresponding DuckDB type name for
 * given Iceberg type name.
 */
const char *
IcebergTypeNameToDuckdbTypeName(const char *icebergTypeName)
{
	IcebergTypeInfo *icebergTypeInfo = GetIcebergTypeInfoFromTypeName(icebergTypeName);

	switch (icebergTypeInfo->type)
	{
		case ICEBERG_TYPE_DECIMAL:
			{
				/*
				 * Decimal needs to append precision and scale to the duckdb
				 * typename. For all other iceberg types, we can directly pass
				 * the corresponding duckdb typename.
				 */
				StringInfo	decimalTypeName = makeStringInfo();

				appendStringInfo(decimalTypeName, "decimal(%d,%d)",
								 icebergTypeInfo->precision, icebergTypeInfo->scale);

				return decimalTypeName->data;
			}

		default:
			{
				DuckDBType	duckDBType = GetDuckDBTypeFromIcebergType(icebergTypeInfo->type);

				return GetDuckDBTypeName(duckDBType);
			}
	}
}


/*
 * GetIcebergTypeInfoFromTypeName returns corresponding Iceberg type info for
 * given Iceberg type name.
 */
static IcebergTypeInfo *
GetIcebergTypeInfoFromTypeName(const char *typeName)
{
	IcebergTypeInfo *icebergTypeInfo = palloc0(sizeof(IcebergTypeInfo));

	icebergTypeInfo->type = ICEBERG_TYPE_INVALID;

	int			totalTypes = sizeof(IcebergToDuckDBTypes) / sizeof(IcebergToDuckDBTypes[0]);

	int			typeIndex = 0;

	int			longestPrefixLen = 0;

	for (typeIndex = 0; typeIndex < totalTypes; typeIndex++)
	{
		const char *currentTypeName = IcebergToDuckDBTypes[typeIndex].icebergTypeName;

		int			currentTypeNameLen = strlen(IcebergToDuckDBTypes[typeIndex].icebergTypeName);

		/*
		 * we need to prefix search for handling type names with dynamic
		 * arguments. e.g. decimal with arbitrary precision and scale
		 * (decimal(10,2))
		 *
		 * We need to find the longest prefix match since we have types like
		 * timestamp and timestamptz.
		 */
		if (currentTypeNameLen > longestPrefixLen &&
			strncasecmp(currentTypeName, typeName, currentTypeNameLen) == 0)
		{
			icebergTypeInfo->type = IcebergToDuckDBTypes[typeIndex].icebergType;
			longestPrefixLen = currentTypeNameLen;

			if (icebergTypeInfo->type == ICEBERG_TYPE_DECIMAL)
			{
				/*
				 * decimal type has precision and scale as arguments. We need
				 * to extract them from the type name.
				 */
				if (sscanf(typeName, "decimal(%d,%d)", &icebergTypeInfo->precision,
						   &icebergTypeInfo->scale) != 2)
				{
					ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
									errmsg("could not parse decimal type modifier from %s",
										   typeName)));
				}
			}
		}
	}

	if (icebergTypeInfo->type == ICEBERG_TYPE_INVALID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg type %s is not supported", typeName)));
	}

	return icebergTypeInfo;
}


/*
 * PostgresTypeIdToIcebergTypeName converts a PostgreSQL type ID and typemod
 * to an Iceberg type name.
 *
 * Based on https://iceberg.apache.org/spec/#schemas
 */
static char *
PostgresBaseTypeIdToIcebergTypeName(PGType pgType)
{
	switch (pgType.postgresTypeOid)
	{
		case BOOLOID:
			return "boolean";
		case INT4OID:
		case INT2OID:
			return "int";
		case INT8OID:
			return "long";
		case FLOAT4OID:
			return "float";
		case FLOAT8OID:
			return "double";
		case DATEOID:
			return "date";
		case TIMEOID:
			return "time";
		case TIMETZOID:
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("TIMETZ is not supported by Iceberg")));
				/* silence compiler */
				return NULL;
			}
		case TIMESTAMPOID:
			return "timestamp";
		case TIMESTAMPTZOID:
			return "timestamptz";
		case TEXTOID:
		case BPCHAROID:
		case VARCHAROID:
			return "string";
		case UUIDOID:
			return "uuid";
		case BYTEAOID:
			return "binary";
		case NUMERICOID:
			{
				/*
				 * Follow similar logic as in ChooseCompatibleDuckDBType
				 */
				int			precision = -1;
				int			scale = -1;

				GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(pgType.postgresTypeMod,
																	 &precision, &scale);

				if (CanPushdownNumericToDuckdb(precision, scale))
				{
					/*
					 * happy case: we can map to DECIMAL(precision, scale)
					 */
					return psprintf("decimal(%d,%d)", precision, scale);
				}
				else
				{
					/* explicit precision which is too big for us */
					return "string";
				}
			}
		default:

			/*
			 * We need to handle the case where the type is a PostGIS type.
			 */
			if (IsGeometryTypeId(pgType.postgresTypeOid))
			{
				ErrorIfPgLakeSpatialNotEnabled();
				return "binary";
			}

			/*
			 * By default, we fallback to string type for any unknown type. In
			 * majority of the cases, given the type is not known, we pull the
			 * data as string from pgduck_server. Then, the fdw converts it to
			 * the appropriate type.
			 */
			return "string";
	}
}


/*
 * CreatePositionDeleteDataFileSchema creates schema for position delete files.
 * See https://iceberg.apache.org/spec/#reserved-field-ids
 */
DataFileSchema *
CreatePositionDeleteDataFileSchema(void)
{
	int			totalFields = 3;

	DataFileSchema *schema = palloc0(sizeof(DataFileSchema));

	schema->fields = palloc0(totalFields * sizeof(DataFileSchemaField));
	schema->nfields = totalFields;

	DataFileSchemaField *filePathField = &schema->fields[0];

	filePathField->name = "file_path";
	filePathField->id = 2147483546;
	filePathField->type = palloc0(sizeof(Field));
	filePathField->type->type = FIELD_TYPE_SCALAR;
	filePathField->type->field.scalar.typeName = "string";

	EnsureIcebergField(filePathField->type);

	DataFileSchemaField *posField = &schema->fields[1];

	posField->name = "pos";
	posField->id = 2147483545;
	posField->type = palloc0(sizeof(Field));
	posField->type->type = FIELD_TYPE_SCALAR;
	posField->type->field.scalar.typeName = "long";

	EnsureIcebergField(posField->type);

	DataFileSchemaField *rowField = &schema->fields[2];

	rowField->name = "row";
	rowField->id = 2147483544;
	rowField->type = palloc0(sizeof(Field));
	rowField->type->type = FIELD_TYPE_STRUCT;
	rowField->type->field.structType.fields = NULL;
	rowField->type->field.structType.nfields = 0;

	EnsureIcebergField(rowField->type);

	return schema;
}


#if PG_VERSION_NUM < 170000

/*
 * Get default expression (or NULL if none) for the given attribute number.
 * The same function exists in Postgres17+.
 */
static Node *
TupleDescGetDefault(TupleDesc tupdesc, AttrNumber attnum)
{
	Node	   *result = NULL;

	if (tupdesc->constr)
	{
		AttrDefault *attrdef = tupdesc->constr->defval;

		for (int i = 0; i < tupdesc->constr->num_defval; i++)
		{
			if (attrdef[i].adnum == attnum)
			{
				result = stringToNode(attrdef[i].adbin);
				break;
			}
		}
	}

	return result;
}

#endif


/*
* GetIcebergJsonSerializedDefaultExpr returns the json serialized default expression for a
* given column per iceberg spec. columnFieldId is contained in the serialized value. e.g. {"1": "value"}
*/
const char *
GetIcebergJsonSerializedDefaultExpr(TupleDesc tupdesc, AttrNumber attnum,
									FieldStructElement * structElementField)
{
	const char *attrName = structElementField->name;
	Field	   *field = structElementField->type;
	Node	   *defaultExpr = TupleDescGetDefault(tupdesc, attnum);

	return GetIcebergJsonSerializedConstDefaultIfExists(attrName, field, defaultExpr);
}


static const char *
GetIcebergJsonSerializedConstDefaultIfExists(const char *attrName, Field * field, Node *defaultExpr)
{
	EnsureIcebergField(field);

	if (defaultExpr == NULL)
	{
		return NULL;
	}

	if (contain_mutable_functions(defaultExpr))
	{
		/*
		 * We cannot serialize expressions with mutable functions, e.g. now()
		 * and random(), to Iceberg schema but ww still let users to set them
		 * as default to not prevent inserts.
		 */
		ereport(DEBUG1,
				(errmsg("default expression for column \"%s\" contains mutable functions",
						attrName),
				 errhint("Default expression will not be serialized to Iceberg schema.")));
		return NULL;
	}

	defaultExpr = eval_const_expressions(NULL, defaultExpr);

	if (IsA(defaultExpr, Const))
	{
		Const	   *defaultConst = (Const *) defaultExpr;

		if (defaultConst->constisnull)
			return NULL;

		PGType		pgType = MakePGType(defaultConst->consttype, defaultConst->consttypmod);

		bool		isNull = defaultConst->constisnull;
		Datum		constDatum = defaultConst->constvalue;

		return PGIcebergJsonSerialize(constDatum, field, pgType, &isNull);
	}

	return NULL;
}


/*
 * EnsureIcebergField ensures that the given field is valid Iceberg field.
 */
void
EnsureIcebergField(Field * field)
{
#ifdef USE_ASSERT_CHECKING

	if (!EnableHeavyAsserts)
		return;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			{
				if (field->field.scalar.typeName == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("missing scalar type name in iceberg field")));
				}

				IcebergTypeInfo *icebergTypeInfo PG_USED_FOR_ASSERTS_ONLY =
					GetIcebergTypeInfoFromTypeName(field->field.scalar.typeName);

				Assert(icebergTypeInfo->type != ICEBERG_TYPE_INVALID);
				break;
			}
		case FIELD_TYPE_LIST:
			{
				if (field->field.list.element == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("missing element field in iceberg list field")));
				}

				EnsureIcebergField(field->field.list.element);
				break;
			}
		case FIELD_TYPE_MAP:
			{
				if (field->field.map.key == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("missing key field in iceberg map field")));
				}

				EnsureIcebergField(field->field.map.key);

				if (field->field.map.value == NULL)
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("missing value field in iceberg map field")));
				}

				EnsureIcebergField(field->field.map.value);
				break;
			}
		case FIELD_TYPE_STRUCT:
			{
				size_t		totalFields = field->field.structType.nfields;

				for (size_t fieldIdx = 0; fieldIdx < totalFields; fieldIdx++)
				{
					FieldStructElement *structElementField = &field->field.structType.fields[fieldIdx];

					if (structElementField->name == NULL)
					{
						ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
										errmsg("missing name in iceberg struct field")));
					}

					if (structElementField->type == NULL)
					{
						ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
										errmsg("missing type in iceberg struct field")));
					}

					EnsureIcebergField(structElementField->type);
				}
				break;
			}
		default:
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("invalid field type %d", field->type)));
				break;
			}
	}

#endif
}
