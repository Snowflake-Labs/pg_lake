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

/*
 * Common Iceberg write-validation helpers shared by query-level and
 * datum-level validation: out-of-range policy resolution, temporal type
 * classification, and TypeNeedsIcebergValidation which determines
 * whether a type requires any validation (temporal boundaries,
 * multidimensional array rejection, or bounded numeric NaN).
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/iceberg_validation.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/table_type.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"


static IcebergOutOfRangePolicy GetIcebergOutOfRangePolicyFromOptions(List *options);


/*
 * GetIcebergOutOfRangePolicyFromOptions reads the "out_of_range_values" option
 * from a list of DefElem options (table options).
 *
 * Returns ICEBERG_OOR_CLAMP if the option is set to "clamp",
 * ICEBERG_OOR_ERROR otherwise (including when not present).
 */
static IcebergOutOfRangePolicy
GetIcebergOutOfRangePolicyFromOptions(List *options)
{
	char	   *value = GetStringOption(options, "out_of_range_values", false);

	if (value != NULL && strcmp(value, "clamp") == 0)
		return ICEBERG_OOR_CLAMP;

	return ICEBERG_OOR_ERROR;
}


/*
 * GetIcebergOutOfRangePolicyForTable returns the IcebergOutOfRangePolicy
 * for the given relation.  Returns NONE when the table is not an Iceberg table.
 */
IcebergOutOfRangePolicy
GetIcebergOutOfRangePolicyForTable(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		return ICEBERG_OOR_NONE;

	ForeignTable *foreignTable = GetForeignTable(relationId);

	return GetIcebergOutOfRangePolicyFromOptions(foreignTable->options);
}


/*
 * IsTemporalType returns true for date, timestamp, or timestamptz.
 */
bool
IsTemporalType(Oid typeOid)
{
	return typeOid == DATEOID ||
		typeOid == TIMESTAMPOID ||
		typeOid == TIMESTAMPTZOID;
}


/*
 * TypeNeedsIcebergValidation recursively checks whether a type contains
 * any component that needs Iceberg write validation, including inside
 * arrays, composites, maps, and domains.
 *
 * Validation covers: temporal boundaries (date/timestamp/timestamptz),
 * multidimensional array rejection (any array type), and bounded
 * numeric NaN (non-pushdown only, since numeric blocks pushdown).
 * Unbounded and large-precision numerics are mapped to float8 on
 * Iceberg tables, so NaN is valid for those and no validation is needed.
 */
bool
TypeNeedsIcebergValidation(Oid typeOid, int32 typmod, bool isPushdown)
{
	if (IsTemporalType(typeOid))
		return true;

	if (!isPushdown && typeOid == NUMERICOID &&
		!IsUnsupportedNumericForIceberg(typeOid, typmod))
		return true;

	Oid			elemType = get_element_type(typeOid);

	if (OidIsValid(elemType))
	{
		/*
		 * Any array column needs validation because PostgreSQL allows
		 * multidimensional values in a plain array type (e.g. int[]) and
		 * those must be nullified (clamped to NULL) or raise an error.  On
		 * the non-pushdown path this is handled by IcebergErrorOrClampDatum;
		 * on the pushdown path by pg_nullify_nested_list() or
		 * pg_error_nested_list() in the query wrapper.
		 */
		return true;
	}

	/* map check must precede the generic domain unwrap (maps are domains) */
	if (IsMapTypeOid(typeOid))
	{
		PGType		keyType = GetMapKeyType(typeOid);
		PGType		valType = GetMapValueType(typeOid);

		return TypeNeedsIcebergValidation(keyType.postgresTypeOid,
										  keyType.postgresTypeMod, isPushdown) ||
			TypeNeedsIcebergValidation(valType.postgresTypeOid,
									   valType.postgresTypeMod, isPushdown);
	}

	char		typtype = get_typtype(typeOid);

	if (typtype == TYPTYPE_DOMAIN)
	{
		int32		baseTypmod = typmod;
		Oid			baseType = getBaseTypeAndTypmod(typeOid, &baseTypmod);

		return TypeNeedsIcebergValidation(baseType, baseTypmod, isPushdown);
	}

	if (typtype == TYPTYPE_COMPOSITE)
	{
		TupleDesc	tupdesc = lookup_rowtype_tupdesc(typeOid, -1);
		bool		found = false;

		for (int i = 0; i < tupdesc->natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
				continue;

			if (TypeNeedsIcebergValidation(attr->atttypid, attr->atttypmod,
										   isPushdown))
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
