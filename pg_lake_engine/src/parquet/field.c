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
#include "catalog/pg_type_d.h"

#include "common/int.h"

#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/string_utils.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

static FieldStructElement * DeepCopyFieldStructElement(FieldStructElement * structElementField);

/*
 * DeepCopyField deep copies a Field.
 */
Field *
DeepCopyField(const Field * field)
{
	Field	   *fieldCopy = palloc0(sizeof(Field));

	fieldCopy->type = field->type;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			{
				fieldCopy->field.scalar.typeName = pstrdup(field->field.scalar.typeName);
				break;
			}
		case FIELD_TYPE_LIST:
			{
				fieldCopy->field.list.element = DeepCopyField(field->field.list.element);
				fieldCopy->field.list.elementId = field->field.list.elementId;
				fieldCopy->field.list.elementRequired = field->field.list.elementRequired;
				break;
			}
		case FIELD_TYPE_MAP:
			{
				fieldCopy->field.map.key = DeepCopyField(field->field.map.key);
				fieldCopy->field.map.keyId = field->field.map.keyId;

				fieldCopy->field.map.value = DeepCopyField(field->field.map.value);
				fieldCopy->field.map.valueId = field->field.map.valueId;
				fieldCopy->field.map.valueRequired = field->field.map.valueRequired;
				break;
			}
		case FIELD_TYPE_STRUCT:
			{
				fieldCopy->field.structType.fields = palloc0(field->field.structType.nfields * sizeof(FieldStructElement));
				fieldCopy->field.structType.nfields = field->field.structType.nfields;

				for (size_t i = 0; i < field->field.structType.nfields; i++)
				{
					FieldStructElement *structElementField = &field->field.structType.fields[i];
					FieldStructElement *structElementFieldCopy = DeepCopyFieldStructElement(structElementField);

					fieldCopy->field.structType.fields[i] = *structElementFieldCopy;
				}

				break;
			}
		default:
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("invalid field type")));
			}
	}

	return fieldCopy;
}


/*
 * DeepCopyFieldStructElement deep copies a FieldStructElement.
 */
static FieldStructElement *
DeepCopyFieldStructElement(FieldStructElement * structElementField)
{
	FieldStructElement *copiedStructElementField = palloc0(sizeof(FieldStructElement));

	copiedStructElementField->id = structElementField->id;
	copiedStructElementField->name = pstrdup(structElementField->name);
	copiedStructElementField->required = structElementField->required;
	copiedStructElementField->doc = (structElementField->doc) ? pstrdup(structElementField->doc) : NULL;
	copiedStructElementField->writeDefault = (structElementField->writeDefault) ? pstrdup(structElementField->writeDefault) : NULL;
	copiedStructElementField->initialDefault = (structElementField->initialDefault) ? pstrdup(structElementField->initialDefault) : NULL;
	copiedStructElementField->duckSerializedInitialDefault = (structElementField->duckSerializedInitialDefault) ? pstrdup(structElementField->duckSerializedInitialDefault) : NULL;
	copiedStructElementField->type = DeepCopyField(structElementField->type);

	return copiedStructElementField;
}


/*
 * DeepCopyDataFileSchema deep copies a DataFileSchema.
 */
DataFileSchema *
DeepCopyDataFileSchema(const DataFileSchema * schema)
{
	DataFileSchema *copiedSchema = palloc0(sizeof(DataFileSchema));

	copiedSchema->fields = palloc0(schema->nfields * sizeof(DataFileSchemaField));
	copiedSchema->nfields = schema->nfields;

	for (size_t i = 0; i < schema->nfields; i++)
	{
		DataFileSchemaField *field = &schema->fields[i];
		DataFileSchemaField *fieldCopy = DeepCopyFieldStructElement(field);

		copiedSchema->fields[i] = *fieldCopy;
	}

	return copiedSchema;
}

int
LeafFieldCompare(const ListCell *a, const ListCell *b)
{
	LeafField  *fieldA = lfirst(a);
	LeafField  *fieldB = lfirst(b);

	return pg_cmp_s32(fieldA->fieldId, fieldB->fieldId);
}

#if PG_VERSION_NUM < 170000

int
pg_cmp_s32(int32 a, int32 b)
{
	return (a > b) - (a < b);
}
#endif



/*
* SchemaFieldsEquivalent compares two DataFileSchemaField structs for equivalence.
* It returns true if they are equivalent, false otherwise.
* Note that we do not compare the field->type here, as we do not allow changing
* the type of any field in the schema, including nested types.
*/
bool
SchemaFieldsEquivalent(DataFileSchemaField * fieldA, DataFileSchemaField * fieldB)
{
	if (fieldA->id != fieldB->id)
		return false;

	if (!PgStrcasecmpNullable(fieldA->name, fieldB->name))
		return false;

	if (fieldA->required != fieldB->required)
		return false;

	if (!PgStrcasecmpNullable(fieldA->doc, fieldB->doc))
		return false;

	if (!PgStrcasecmpNullable(fieldA->writeDefault, fieldB->writeDefault))
		return false;

	if (!PgStrcasecmpNullable(fieldA->initialDefault, fieldB->initialDefault))
		return false;

	/*
	 * We don't allow changing any of the types of the fields in the schema,
	 * including the fields of nested types. So we don't need to compare
	 * anything about the field->type here.
	 */
	return true;
}


/*
 * PGTypeRequiresConversionToIcebergString returns true if the given Postgres type
 * requires conversion to Iceberg string.
 * Some of the Postgres types cannot be directly mapped to an Iceberg type.
 * e.g. custom types like hstore
 */
bool
PGTypeRequiresConversionToIcebergString(Field * field, PGType pgType)
{
	/*
	 * We treat geometry as binary within the Iceberg schema, which is encoded
	 * as a hexadecimal string according to the spec. As it happens, the
	 * Postgres output function of geometry produces a hexadecimal WKB string,
	 * so we can use the regular text output function to convert to an Iceberg
	 * value.
	 */
	if (IsGeometryTypeId(pgType.postgresTypeOid))
	{
		return true;
	}

	return strcmp(field->field.scalar.typeName, "string") == 0 && pgType.postgresTypeOid != TEXTOID;
}


/*
 * PostgresBaseTypeIdToIcebergTypeName returns the Iceberg scalar type name a
 * PostgreSQL base (non-container) type maps to, e.g. uuid -> "uuid", bytea ->
 * "binary", an oversized numeric or any unknown type -> "string".
 *
 * This is the single source of truth for the surface PostgreSQL -> Iceberg
 * scalar name mapping. It lives in the engine so both the Iceberg field
 * builder (which shapes the stored schema) and the read/write codecs (which
 * decide whether a leaf's storage type diverges from its surface type) agree
 * on the mapping without re-implementing it.
 *
 * Domains are unwrapped to their base type so a domain over e.g. bytea maps to
 * "binary" rather than falling through to the "string" default. The field
 * builder already resolves domains before reaching here, so this is a no-op for
 * that caller, but the codecs hand us raw attribute type ids and rely on it.
 */
char *
PostgresBaseTypeIdToIcebergTypeName(PGType pgType)
{
	pgType.postgresTypeOid = ResolveDomainBaseType(pgType.postgresTypeOid);

	switch (pgType.postgresTypeOid)
	{
		case BOOLOID:
			return "boolean";
		case INT4OID:
		case INT2OID:
			return "int";
		case INT8OID:
		case OIDOID:
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
			return "time";
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
 * StorageFieldForColumn returns the top-level storage field for a column of the
 * given name within a DataFileSchema, or NULL.
 */
Field *
StorageFieldForColumn(DataFileSchema * storageSchema, const char *name)
{
	if (storageSchema == NULL)
		return NULL;

	for (size_t i = 0; i < storageSchema->nfields; i++)
	{
		FieldStructElement *element = &storageSchema->fields[i];

		if (element->name != NULL && strcmp(element->name, name) == 0)
			return element->type;
	}

	return NULL;
}


/*
 * StorageStructFieldByName returns the child storage field of a struct storage
 * field whose name matches, or NULL. Navigation is by name so dropped
 * attributes (absent from the storage struct) are skipped in lockstep.
 */
Field *
StorageStructFieldByName(Field * storageStruct, const char *name)
{
	if (storageStruct == NULL || storageStruct->type != FIELD_TYPE_STRUCT)
		return NULL;

	for (size_t i = 0; i < storageStruct->field.structType.nfields; i++)
	{
		FieldStructElement *element = &storageStruct->field.structType.fields[i];

		if (element->name != NULL && strcmp(element->name, name) == 0)
			return element->type;
	}

	return NULL;
}


/*
 * ScalarLeafStorageDiverges returns true when a scalar surface leaf is
 * physically persisted as a different Iceberg type than the surface type would
 * naturally map to (e.g. a uuid surface leaf stored as iceberg string under a
 * compatibility mode). It compares the surface type's natural Iceberg name
 * against the persisted storage name, so it is type-agnostic and matches the
 * registration-time divergence decision exactly: intrinsic representation
 * differences that keep the same Iceberg name (geometry/bytea -> "binary",
 * oversized numeric -> "string") are NOT divergences, while a genuine
 * compatibility remap (uuid -> "string") is.
 */
bool
ScalarLeafStorageDiverges(Field * storageField, Oid surfaceOid, int32 surfaceTypmod)
{
	if (storageField == NULL ||
		storageField->type != FIELD_TYPE_SCALAR ||
		storageField->field.scalar.typeName == NULL)
		return false;

	char	   *surfaceName =
		PostgresBaseTypeIdToIcebergTypeName(MakePGType(surfaceOid, surfaceTypmod));

	return strcmp(surfaceName, storageField->field.scalar.typeName) != 0;
}


/*
 * TypeHasStorageDivergentLeaf recursively checks whether a surface type has any
 * scalar leaf whose persisted storage type diverges from the surface type
 * (e.g. a nested uuid stored as iceberg string under compatibility_mode).
 *
 * It navigates the surface type tree and the storage field tree in parallel
 * (by name for composites, by element for arrays, unwrapping domains) so each
 * surface leaf is matched to its storage field. The check is fully
 * type-agnostic; see ScalarLeafStorageDiverges.
 *
 * Both the read path (which must cast diverging leaves back to the surface type
 * on read) and the write path (which casts them to the storage type on write)
 * use this single predicate so the two directions can never disagree on what
 * diverges.
 */
bool
TypeHasStorageDivergentLeaf(Oid typeOid, int32 typmod, Field * storageField)
{
	if (storageField == NULL)
		return false;

	if (ScalarLeafStorageDiverges(storageField, typeOid, typmod))
		return true;

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		Field	   *elemStorage =
			(storageField->type == FIELD_TYPE_LIST) ?
			storageField->field.list.element : NULL;

		/* an array column's typmod applies to its element type */
		return TypeHasStorageDivergentLeaf(elemType, typmod, elemStorage);
	}

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
	{
		int32		baseTypmod = typmod;
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &baseTypmod);

		return TypeHasStorageDivergentLeaf(baseType, baseTypmod, storageField);
	}

	if (typtype == TYPTYPE_COMPOSITE && storageField->type == FIELD_TYPE_STRUCT)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		found = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			Field	   *childStorage =
				StorageStructFieldByName(storageField, NameStr(attr->attname));

			if (TypeHasStorageDivergentLeaf(attr->atttypid, attr->atttypmod,
											childStorage))
			{
				found = true;
				break;
			}
		}

		ReleaseTupleDesc(tupdesc);
		return found;
	}

	return false;
}
