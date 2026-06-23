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
 * compatibility_mode.c
 *  Per-table Iceberg "compatibility mode" STORAGE shaping.
 *
 * Some engines that read pg_lake's Iceberg tables have narrower type support
 * than Iceberg itself.  The `compatibility_mode` table option opts a table into
 * a storage shape consumable by such an engine WITHOUT changing the Postgres
 * column type the user declared.
 *
 * The only mode today is 'snowflake'.  Snowflake cannot store a UUID inside a
 * semi-structured/structured type, so a uuid nested inside an array, map, or
 * composite is stored physically as Iceberg `string`; a top-level uuid column
 * stays native `uuid`.  The Postgres column type is left as uuid in every case
 * -- conversion happens at the I/O boundary (uuid->varchar on write,
 * varchar->uuid on read), so `\d` and DEFAULT/GENERATED clauses are unaffected.
 *
 * This module supplies the type predicate (TypeHasNestedUuid), the per-table
 * option accessor, and the Iceberg Field-tree rewrite
 * (RewriteNestedUuidFieldsToString).  The write-side value cast lives in
 * iceberg_query_validation.c and the read-side cast in read_data.c; both take a
 * plain bool so they need no knowledge of the option machinery.
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/compatibility_mode.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/util/table_type.h"


static bool TypeContainsUuid(Oid typeOid);
static bool AnyCompositeFieldContainsUuid(Oid typeOid);
static void RewriteNestedUuidFieldsToStringInternal(Field * field, int level);


IcebergCompatibilityMode
ParseIcebergCompatibilityMode(const char *optionValue)
{
	if (optionValue == NULL || pg_strcasecmp(optionValue, "auto") == 0)
		return ICEBERG_COMPAT_AUTO;

	if (pg_strcasecmp(optionValue, "snowflake") == 0)
		return ICEBERG_COMPAT_SNOWFLAKE;

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid compatibility_mode option: \"%s\"", optionValue),
			 errhint("Valid values are 'auto' and 'snowflake'.")));
}


IcebergCompatibilityMode
GetIcebergCompatibilityModeForTable(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		return ICEBERG_COMPAT_AUTO;

	ForeignTable *foreignTable = GetForeignTable(relationId);
	char	   *value = GetStringOption(foreignTable->options,
										ICEBERG_COMPATIBILITY_MODE_OPTION, false);

	return ParseIcebergCompatibilityMode(value);
}


/*
 * AnyCompositeFieldContainsUuid returns true if any non-dropped attribute of
 * the composite type contains a uuid at any depth.
 */
static bool
AnyCompositeFieldContainsUuid(Oid typeOid)
{
	TupleDesc	tupleDesc = lookup_rowtype_tupdesc(typeOid, -1);
	bool		found = false;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (TypeContainsUuid(attr->atttypid))
		{
			found = true;
			break;
		}
	}

	ReleaseTupleDesc(tupleDesc);
	return found;
}


/*
 * TypeContainsUuid returns true if typeOid is a uuid or contains a uuid at any
 * nesting depth (array element, composite field, or map key/value).
 */
static bool
TypeContainsUuid(Oid typeOid)
{
	typeOid = ResolveDomainBaseType(typeOid);

	if (typeOid == UUIDOID)
		return true;

	if (type_is_array(typeOid))
		return TypeContainsUuid(get_element_type(typeOid));

	if (IsMapTypeOid(typeOid))
		return TypeContainsUuid(GetMapKeyType(typeOid).postgresTypeOid) ||
			TypeContainsUuid(GetMapValueType(typeOid).postgresTypeOid);

	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
		return AnyCompositeFieldContainsUuid(typeOid);

	return false;
}


bool
TypeHasNestedUuid(Oid typeOid)
{
	typeOid = ResolveDomainBaseType(typeOid);

	if (type_is_array(typeOid))
		return TypeContainsUuid(get_element_type(typeOid));

	if (IsMapTypeOid(typeOid))
		return TypeContainsUuid(GetMapKeyType(typeOid).postgresTypeOid) ||
			TypeContainsUuid(GetMapValueType(typeOid).postgresTypeOid);

	if (get_typtype(typeOid) == TYPTYPE_COMPOSITE)
		return AnyCompositeFieldContainsUuid(typeOid);

	/* bare scalar (including a top-level uuid) is not "nested" */
	return false;
}


/*
 * RewriteNestedUuidFieldsToStringInternal flips every nested (level > 0) scalar
 * "uuid" field to "string".  level 0 is the column itself, so a top-level uuid
 * is left as-is.
 */
static void
RewriteNestedUuidFieldsToStringInternal(Field * field, int level)
{
	if (field == NULL)
		return;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			if (level > 0 && field->field.scalar.typeName != NULL &&
				strcmp(field->field.scalar.typeName, "uuid") == 0)
				field->field.scalar.typeName = pstrdup("string");
			break;

		case FIELD_TYPE_LIST:
			RewriteNestedUuidFieldsToStringInternal(field->field.list.element, level + 1);
			break;

		case FIELD_TYPE_MAP:
			RewriteNestedUuidFieldsToStringInternal(field->field.map.key, level + 1);
			RewriteNestedUuidFieldsToStringInternal(field->field.map.value, level + 1);
			break;

		case FIELD_TYPE_STRUCT:
			for (size_t i = 0; i < field->field.structType.nfields; i++)
				RewriteNestedUuidFieldsToStringInternal(field->field.structType.fields[i].type,
														level + 1);
			break;
	}
}


void
RewriteNestedUuidFieldsToString(Field * field, IcebergCompatibilityMode mode)
{
	if (mode != ICEBERG_COMPAT_SNOWFLAKE)
		return;

	RewriteNestedUuidFieldsToStringInternal(field, 0);
}
