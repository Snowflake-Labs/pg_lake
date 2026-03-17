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
 * Common Iceberg validation helpers shared by query-level and datum-level
 * validation: out-of-range policy resolution, temporal type classification,
 * and temporal boundary constants.
 */
#include "postgres.h"

#include "access/relation.h"
#include "access/tupdesc.h"
#include "utils/rel.h"
#include "catalog/pg_type.h"
#include "foreign/foreign.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/iceberg_validation.h"
#include "pg_lake/util/table_type.h"


static IcebergOutOfRangePolicy GetIcebergOutOfRangePolicyFromOptions(List *options);
static bool TupleDescNeedsIcebergValidation(TupleDesc tupleDesc);


/*
 * GetIcebergOutOfRangePolicyFromOptions reads the "out_of_range_values" option
 * from a list of DefElem options (table options).
 *
 * Returns ICEBERG_OOR_ERROR if the option is set to "error",
 * ICEBERG_OOR_CLAMP otherwise (including when not present).
 */
static IcebergOutOfRangePolicy
GetIcebergOutOfRangePolicyFromOptions(List *options)
{
	char	   *value = GetStringOption(options, "out_of_range_values", false);

	if (value != NULL && strcmp(value, "error") == 0)
		return ICEBERG_OOR_ERROR;

	return ICEBERG_OOR_CLAMP;
}


/*
 * GetIcebergOutOfRangePolicyForTable returns the IcebergOutOfRangePolicy
 * for the given relation.  Returns NONE when:
 *   - the table is not an Iceberg table, or
 *   - no column is temporal or numeric (nothing to validate).
 */
IcebergOutOfRangePolicy
GetIcebergOutOfRangePolicyForTable(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		return ICEBERG_OOR_NONE;

	Relation	rel = RelationIdGetRelation(relationId);
	TupleDesc	tupleDesc = RelationGetDescr(rel);
	bool		needsValidation = TupleDescNeedsIcebergValidation(tupleDesc);

	RelationClose(rel);

	if (!needsValidation)
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
 * TupleDescNeedsIcebergValidation returns true if any non-dropped column
 * in the tuple descriptor is temporal or numeric (i.e. requires
 * IcebergErrorOrClampDatum processing).
 */
static bool
TupleDescNeedsIcebergValidation(TupleDesc tupleDesc)
{
	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (IsTemporalType(attr->atttypid) || attr->atttypid == NUMERICOID)
			return true;
	}

	return false;
}
