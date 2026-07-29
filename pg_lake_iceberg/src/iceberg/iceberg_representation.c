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

#include "catalog/pg_type_d.h"

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_representation.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/type.h"


static PGType IcebergCreatePathLeafConversion(PGType leafType, void *context);


/*
 * IcebergFieldsEquivalent - see iceberg_representation.h.
 *
 * Recursively compare two Iceberg Fields for type equality, ignoring field ids
 * and default values (which are assigned per derivation and are irrelevant to
 * the stored representation).
 */
bool
IcebergFieldsEquivalent(Field * a, Field * b)
{
	if (a->type != b->type)
		return false;

	switch (a->type)
	{
		case FIELD_TYPE_SCALAR:
			return strcmp(a->field.scalar.typeName, b->field.scalar.typeName) == 0;

		case FIELD_TYPE_LIST:
			return a->field.list.elementRequired == b->field.list.elementRequired &&
				IcebergFieldsEquivalent(a->field.list.element, b->field.list.element);

		case FIELD_TYPE_MAP:
			return a->field.map.valueRequired == b->field.map.valueRequired &&
				IcebergFieldsEquivalent(a->field.map.key, b->field.map.key) &&
				IcebergFieldsEquivalent(a->field.map.value, b->field.map.value);

		case FIELD_TYPE_STRUCT:
			{
				if (a->field.structType.nfields != b->field.structType.nfields)
					return false;

				for (size_t i = 0; i < a->field.structType.nfields; i++)
				{
					FieldStructElement *ea = &a->field.structType.fields[i];
					FieldStructElement *eb = &b->field.structType.fields[i];

					if (ea->required != eb->required ||
						strcmp(ea->name, eb->name) != 0 ||
						!IcebergFieldsEquivalent(ea->type, eb->type))
						return false;
				}
				return true;
			}
	}

	return false;
}


void
InitIcebergCreatePathContext(IcebergCreatePathContext * context,
							 bool unsupportedNumericAsDouble,
							 IcebergCompatibilityMode compatibilityMode)
{
	context->unsupportedNumericAsDouble = unsupportedNumericAsDouble;
	context->compatibilityMode = compatibilityMode;
}


void
InitIcebergCreatePathContextFromGUC(IcebergCreatePathContext * context)
{
	context->unsupportedNumericAsDouble = UnsupportedNumericAsDouble;
	context->compatibilityMode = ICEBERG_COMPAT_AUTO;
}


/*
 * DeriveIcebergStoredField - see iceberg_representation.h.
 *
 * Reproduces the full create-path stored field for a single type: derive the
 * Iceberg field applying the unsupported-numeric leaf conversion, then apply
 * the compatibility storage mapping over the derived tree (nested uuid ->
 * string, etc.).  Returns NULL when the numeric conversion reports an
 * unrepresentable leaf (an unsupported numeric with the GUC off), so a caller
 * comparing against a persisted field can never treat that as a match.
 */
Field *
DeriveIcebergStoredField(PGType type, const IcebergCreatePathContext * context)
{
	int			subFieldIndex = 0;

	/* the leaf conversion callback takes a non-const void *context */
	IcebergCreatePathContext leafContext = *context;

	Field	   *field = PostgresTypeToIcebergFieldConverted(type, false,
															&subFieldIndex,
															IcebergCreatePathLeafConversion,
															&leafContext);

	if (field == NULL)
		return NULL;

	ApplyCompatibilityStorageMapping(field, context->compatibilityMode);

	return field;
}


/*
 * SameIcebergStoredRepresentation - see iceberg_representation.h.
 *
 * Derives the stored field for each type (see DeriveIcebergStoredField) and
 * compares the two.  A NULL field means the numeric conversion reported an
 * unrepresentable leaf, so the types are not equal.  Callers that hold the
 * actually-stored field (e.g. from field_id_mappings) should instead derive
 * only the new type and compare it against that persisted field, which is
 * immune to GUC/derivation drift on the old side.
 */
bool
SameIcebergStoredRepresentation(PGType oldType, PGType newType,
								const IcebergCreatePathContext * context)
{
	Field	   *oldField = DeriveIcebergStoredField(oldType, context);
	Field	   *newField = DeriveIcebergStoredField(newType, context);

	if (oldField == NULL || newField == NULL)
		return false;

	return IcebergFieldsEquivalent(oldField, newField);
}


/*
 * IcebergCreatePathLeafConversion - leaf conversion mirroring the Iceberg create
 * path's unsupported-numeric handling, applied at every scalar leaf while
 * deriving the stored field tree.  The unsupported-numeric rule is uniform
 * across nesting levels; the depth-dependent compatibility mapping is applied
 * separately, as a Field-tree pass, by DeriveIcebergStoredField (see
 * ApplyCompatibilityStorageMapping).
 */
static PGType
IcebergCreatePathLeafConversion(PGType leafType, void *context)
{
	IcebergCreatePathContext *createContext = (IcebergCreatePathContext *) context;

	if (!IsUnsupportedNumericForIceberg(leafType.postgresTypeOid,
										leafType.postgresTypeMod))
		return leafType;

	if (createContext->unsupportedNumericAsDouble)
		return MakePGTypeOid(FLOAT8OID);

	/*
	 * With the GUC off, CREATE errors on an unsupported numeric at any level,
	 * so there is no faithful stored representation: report the leaf as
	 * unrepresentable (invalid Oid) rather than pretending it is `string`.
	 */
	return MakePGTypeOid(InvalidOid);
}
