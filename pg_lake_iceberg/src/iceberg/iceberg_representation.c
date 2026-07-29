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

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_representation.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/type.h"


static bool SameIcebergFieldType(Field * a, Field * b);
static PGType IcebergCreatePathLeafConversion(PGType leafType, void *context);


/*
 * SameIcebergFieldType - Recursively compare two Iceberg Fields for type
 * equality, ignoring field ids and default values (which are assigned per
 * derivation and are irrelevant to the stored representation).
 */
static bool
SameIcebergFieldType(Field * a, Field * b)
{
	if (a->type != b->type)
		return false;

	switch (a->type)
	{
		case FIELD_TYPE_SCALAR:
			return strcmp(a->field.scalar.typeName, b->field.scalar.typeName) == 0;

		case FIELD_TYPE_LIST:
			return a->field.list.elementRequired == b->field.list.elementRequired &&
				SameIcebergFieldType(a->field.list.element, b->field.list.element);

		case FIELD_TYPE_MAP:
			return a->field.map.valueRequired == b->field.map.valueRequired &&
				SameIcebergFieldType(a->field.map.key, b->field.map.key) &&
				SameIcebergFieldType(a->field.map.value, b->field.map.value);

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
						!SameIcebergFieldType(ea->type, eb->type))
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
 * SameIcebergStoredRepresentation - see iceberg_representation.h.
 *
 * Reproduces the full create-path stored schema: derive each type applying the
 * unsupported-numeric leaf conversion, then apply the compatibility storage
 * mapping over the derived field tree (nested uuid -> string, etc.), then
 * compare.  A NULL field means the numeric conversion reported an
 * unrepresentable leaf, so the types are not equal.
 */
bool
SameIcebergStoredRepresentation(PGType oldType, PGType newType,
								const IcebergCreatePathContext * context)
{
	int			oldSubFieldIndex = 0;
	int			newSubFieldIndex = 0;

	/* the leaf conversion callback takes a non-const void *context */
	IcebergCreatePathContext leafContext = *context;

	Field	   *oldField = PostgresTypeToIcebergFieldConverted(oldType, false,
															   &oldSubFieldIndex,
															   IcebergCreatePathLeafConversion,
															   &leafContext);
	Field	   *newField = PostgresTypeToIcebergFieldConverted(newType, false,
															   &newSubFieldIndex,
															   IcebergCreatePathLeafConversion,
															   &leafContext);

	if (oldField == NULL || newField == NULL)
		return false;

	ApplyCompatibilityStorageMapping(oldField, context->compatibilityMode);
	ApplyCompatibilityStorageMapping(newField, context->compatibilityMode);

	return SameIcebergFieldType(oldField, newField);
}


/*
 * IcebergCreatePathLeafConversion - leaf conversion mirroring the Iceberg create
 * path's unsupported-numeric handling, applied at every scalar leaf while
 * deriving the stored field tree.  The unsupported-numeric rule is uniform
 * across nesting levels; the depth-dependent compatibility mapping is applied
 * separately, as a Field-tree pass, by SameIcebergStoredRepresentation (see
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
