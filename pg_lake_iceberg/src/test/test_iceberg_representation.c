/*
 * Copyright 2026 Snowflake Inc.
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

#include "lib/stringinfo.h"
#include "parser/parse_type.h"
#include "utils/builtins.h"

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_representation.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/type.h"

PG_FUNCTION_INFO_V1(pg_lake_iceberg_storage_type);
PG_FUNCTION_INFO_V1(pg_lake_same_iceberg_representation);

static Field * StorageFieldForTypeString(const char *typeString,
										 IcebergCompatibilityMode mode);
static IcebergCompatibilityMode CompatibilityModeArg(FunctionCallInfo fcinfo,
													 int argIndex);
static void AppendFieldTypeString(StringInfo buffer, Field * field);

/*
 * pg_lake_iceberg_storage_type(type text, compatibility_mode text) -> text
 *
 * Renders the Iceberg type the create path stores for `type`, e.g. 'string',
 * 'list<double>', 'struct<a:double,b:int>'.  Tests assert on this directly so a
 * derivation that is wrong in the same way on both sides of a comparison cannot
 * hide behind an "equal" verdict.
 */
Datum
pg_lake_iceberg_storage_type(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		PG_RETURN_NULL();

	Field	   *field =
		StorageFieldForTypeString(text_to_cstring(PG_GETARG_TEXT_PP(0)),
								  CompatibilityModeArg(fcinfo, 1));

	StringInfo	buffer = makeStringInfo();

	AppendFieldTypeString(buffer, field);

	PG_RETURN_TEXT_P(cstring_to_text(buffer->data));
}

/*
 * pg_lake_same_iceberg_representation(old_type text, new_type text,
 *									   compatibility_mode text) -> bool
 *
 * Composes the exported primitives the way a consumer with no persisted field to
 * compare against would (see iceberg_representation.h on why that is the weaker
 * question), so the type-pair expectations can be pinned without a real table.
 *
 * pg_lake_iceberg.unsupported_numeric_as_double is read live, so a test pins it
 * with SET rather than passing it in.
 */
Datum
pg_lake_same_iceberg_representation(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		PG_RETURN_NULL();

	char	   *oldTypeString = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *newTypeString = text_to_cstring(PG_GETARG_TEXT_PP(1));
	IcebergCompatibilityMode mode = CompatibilityModeArg(fcinfo, 2);

	Oid			oldTypeOid;
	int32		oldTypeMod;
	Oid			newTypeOid;
	int32		newTypeMod;

	parseTypeString(oldTypeString, &oldTypeOid, &oldTypeMod, NULL);
	parseTypeString(newTypeString, &newTypeOid, &newTypeMod, NULL);

	/*
	 * Caller policy, not a property of the primitives: with
	 * unsupported_numeric_as_double off, CREATE rejects such a numeric at any
	 * level instead of storing it, so it has no stored form to compare.
	 * Report "not the same" rather than letting both sides fall through to
	 * the `string` default and match.  (A caller comparing against a
	 * persisted field does not need this: a persisted field can never be an
	 * unsupported numeric.)
	 */
	if (!UnsupportedNumericAsDouble &&
		(TypeHasUnrepresentableLeaf(MakePGType(oldTypeOid, oldTypeMod),
									false) ||
		 TypeHasUnrepresentableLeaf(MakePGType(newTypeOid, newTypeMod),
									false)))
		PG_RETURN_BOOL(false);

	Field	   *oldField = StorageFieldForTypeString(oldTypeString, mode);
	Field	   *newField = StorageFieldForTypeString(newTypeString, mode);

	PG_RETURN_BOOL(IcebergFieldsEquivalent(oldField, newField));
}

/*
 * StorageFieldForTypeString parses a Postgres type string (e.g. 'varchar(50)',
 * 'numeric(50,2)[]', 'uuid[]') and returns the field the create path stores for
 * it, applying the same two steps registration does.
 */
static Field *
StorageFieldForTypeString(const char *typeString,
						  IcebergCompatibilityMode mode)
{
	Oid			typeOid;
	int32		typeMod;
	int			subFieldIndex = 0;

	parseTypeString(typeString, &typeOid, &typeMod, NULL);

	PGType		storedType =
		IcebergStoredPostgresType(MakePGType(typeOid, typeMod));

	return IcebergStorageFieldForColumnType(storedType, mode, false,
											&subFieldIndex, NULL);
}

/*
 * CompatibilityModeArg reads an optional compatibility_mode text argument; a
 * NULL argument means AUTO, i.e. no compatibility storage mapping.
 */
static IcebergCompatibilityMode
CompatibilityModeArg(FunctionCallInfo fcinfo, int argIndex)
{
	if (PG_ARGISNULL(argIndex))
		return ICEBERG_COMPAT_AUTO;

	return ParseIcebergCompatibilityMode(
										 text_to_cstring(PG_GETARG_TEXT_PP(argIndex)));
}

/*
 * AppendFieldTypeString renders a Field tree in a compact Iceberg-ish notation.
 * Field ids and defaults are omitted: they are per-derivation, and including
 * them would make every expected value in a test depend on id allocation.
 */
static void
AppendFieldTypeString(StringInfo buffer, Field * field)
{
	if (field == NULL)
	{
		appendStringInfoString(buffer, "<none>");
		return;
	}

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			appendStringInfoString(buffer, field->field.scalar.typeName);
			break;

		case FIELD_TYPE_LIST:
			appendStringInfoString(buffer, "list<");
			AppendFieldTypeString(buffer, field->field.list.element);
			appendStringInfoChar(buffer, '>');
			break;

		case FIELD_TYPE_MAP:
			appendStringInfoString(buffer, "map<");
			AppendFieldTypeString(buffer, field->field.map.key);
			appendStringInfoChar(buffer, ',');
			AppendFieldTypeString(buffer, field->field.map.value);
			appendStringInfoChar(buffer, '>');
			break;

		case FIELD_TYPE_STRUCT:
			appendStringInfoString(buffer, "struct<");

			for (size_t i = 0; i < field->field.structType.nfields; i++)
			{
				FieldStructElement *element =
					&field->field.structType.fields[i];

				if (i > 0)
					appendStringInfoChar(buffer, ',');

				appendStringInfo(buffer, "%s:", element->name);
				AppendFieldTypeString(buffer, element->type);
			}

			appendStringInfoChar(buffer, '>');
			break;
	}
}
