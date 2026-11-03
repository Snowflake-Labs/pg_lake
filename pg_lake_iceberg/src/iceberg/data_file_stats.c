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

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/parquet/leaf_field.h"

#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"

static void SetColumnBoundsFromDataFileStats(const DataFileStats * dataFileStats,
											 ColumnBound * *lowerBounds,
											 size_t *nLowerBounds,
											 ColumnBound * *upperBounds,
											 size_t *nUpperBounds);
static ColumnBound * CreateColumnBoundForLeafField(LeafField * leafField, char *columnBoundText);

/*
 * SetIcebergDataFileStats sets the record count, file size, lower and upper bounds
 * for the given data file stats.
 */
void
SetIcebergDataFileStats(const DataFileStats * dataFileStats,
						int64_t *recordCount,
						int64_t *fileSizeInBytes,
						ColumnBound * *lowerBounds,
						size_t *nLowerBounds,
						ColumnBound * *upperBounds,
						size_t *nUpperBounds)
{
	*recordCount = dataFileStats->rowCount;
	*fileSizeInBytes = dataFileStats->fileSize;

	SetColumnBoundsFromDataFileStats(dataFileStats, lowerBounds, nLowerBounds, upperBounds, nUpperBounds);
}


/*
 * SetColumnBoundsFromDataFileStats sets the lower and upper bounds for each leaf field
 * in the given data file stats.
 */
static void
SetColumnBoundsFromDataFileStats(const DataFileStats * dataFileStats,
								 ColumnBound * *lowerBounds,
								 size_t *nLowerBounds,
								 ColumnBound * *upperBounds,
								 size_t *nUpperBounds)
{
	List	   *columnStats = dataFileStats->columnStats;

	int			totalColumnStats = list_length(columnStats);

	ColumnBound *lBounds = palloc0(sizeof(ColumnBound) * totalColumnStats);

	ColumnBound *uBounds = palloc0(sizeof(ColumnBound) * totalColumnStats);

	int			totalLowerBounds = 0;
	int			totalUpperBounds = 0;

	for (int columnStatIdx = 0; columnStatIdx < totalColumnStats; columnStatIdx++)
	{
		DataFileColumnStats *columnStat = list_nth(columnStats, columnStatIdx);

		LeafField  *leafField = &columnStat->leafField;

		char	   *lowerBoundText = columnStat->lowerBoundText;

		char	   *upperBoundText = columnStat->upperBoundText;

		ColumnBound *lowerBound = NULL;
		ColumnBound *upperBound = NULL;

		if (lowerBoundText != NULL)
			lowerBound = CreateColumnBoundForLeafField(leafField, lowerBoundText);

		if (upperBoundText != NULL)
		{
			/*
			 * a non-empty upper bound requires a non-empty lower bound. But
			 * reverse is not always true when stats mode is truncate and
			 * overflow occurs while truncating upper bound.
			 */
			Assert(lowerBoundText != NULL);

			upperBound = CreateColumnBoundForLeafField(leafField, upperBoundText);
		}

		/*
		 * Add bounds only when both serialize successfully.  Special values
		 * like NaN/Infinity can cause one bound to serialize while the other
		 * cannot (e.g. min=0.0 serializes but max=NaN does not).  Truncation
		 * overflow can also set upperBoundText to NULL.  In both cases we
		 * skip the column to keep lower_bounds and upper_bounds in sync.
		 */
		if (lowerBound != NULL && upperBound != NULL)
		{
			lBounds[totalLowerBounds] = *lowerBound;
			totalLowerBounds++;

			uBounds[totalUpperBounds] = *upperBound;
			totalUpperBounds++;
		}
	}

	*lowerBounds = lBounds;
	*nLowerBounds = totalLowerBounds;

	*upperBounds = uBounds;
	*nUpperBounds = totalUpperBounds;
}


/*
 * CreateColumnBoundForLeafField converts bound text representation to ColumnBound
 * with Iceberg binary serialized value for the given leaf field.
 */
static ColumnBound *
CreateColumnBoundForLeafField(LeafField * leafField, char *columnBoundText)
{
	size_t		valueLen = 0;
	unsigned char *icebergSerializedValue = IcebergSerializeColumnBoundText(columnBoundText,
																			leafField->field,
																			&valueLen);

	if (icebergSerializedValue == NULL)
	{
		/*
		 * even if bound text is valid in Postgres, we return null for special
		 * types e.g. NaN
		 */
		Assert(!CanSerializeColumnBound(columnBoundText, leafField->field));
		return NULL;
	}

	Assert(CanSerializeColumnBound(columnBoundText, leafField->field));

	ColumnBound *columnBound = palloc0(sizeof(ColumnBound));

	columnBound->column_id = leafField->fieldId;
	columnBound->value = icebergSerializedValue;
	columnBound->value_length = valueLen;

	return columnBound;
}


/*
 * IcebergSerializeColumnBoundText serializes the given column bound in Postgres text
 * to Iceberg binary format.
 */
unsigned char *
IcebergSerializeColumnBoundText(char *columnBoundText, Field * field, size_t *binaryLen)
{
	PGType		pgType = IcebergFieldToPostgresType(field);

	Datum		boundDatum = ColumnBoundDatum(columnBoundText, pgType);

	/* serialize the bound value to binary */
	unsigned char *binaryValue =
		PGIcebergBinarySerializeBoundValue(boundDatum, field, pgType, binaryLen);

	return binaryValue;
}


/*
 * CanSerializeColumnBound returns true if the given bound text can be
 * serialized to Iceberg binary format for the given field.  Returns
 * false for NULL text or values that have no Iceberg binary
 * representation (e.g. NaN/Inf floats).
 */
bool
CanSerializeColumnBound(char *boundText, Field * field)
{
	if (boundText == NULL)
		return false;

	size_t		boundLen = 0;

	return IcebergSerializeColumnBoundText(pstrdup(boundText),
										   field, &boundLen) != NULL;
}


/*
 * ColumnBoundDatum converts the given column bound text representation to Datum.
 */
Datum
ColumnBoundDatum(char *columnBoundText, PGType pgType)
{
	/* datumize bound value from its text representation */
	Oid			typoinput;
	Oid			typioparam;

	getTypeInputInfo(pgType.postgresTypeOid, &typoinput, &typioparam);

	Datum		boundDatum = OidInputFunctionCall(typoinput, columnBoundText, typioparam, pgType.postgresTypeMod);

	return boundDatum;
}


/* FindColumnBound rejects absent or duplicated optional metric IDs. */
static ColumnBound *
FindColumnBound(ColumnBound * bounds, size_t count, int fieldId)
{
	ColumnBound *result = NULL;

	for (size_t i = 0; bounds != NULL && i < count; i++)
	{
		if (bounds[i].column_id != fieldId)
			continue;
		if (result != NULL)
			return NULL;
		result = &bounds[i];
	}
	return result;
}

/* DecodeColumnBound returns false for unsupported or malformed metrics. */
static bool
DecodeColumnBound(Field * field, PGType pgType, ColumnBound * bound, Datum *value, int64 *integer)
{
	if (field->type != FIELD_TYPE_SCALAR || bound->value == NULL)
		return false;

	const char *name = field->field.scalar.typeName;
	Oid			type = pgType.postgresTypeOid;
	size_t		length = bound->value_length;
	int			precision,
				scale,
				consumed = 0;

	/* A promoted long may retain four-byte int bounds in older files. */
	if ((type == INT4OID && strcmp(name, "int") == 0 && length == 4) ||
		(type == INT8OID && strcmp(name, "long") == 0 && (length == 4 || length == 8)) ||
		(type == DATEOID && strcmp(name, "date") == 0 && length == 4))
	{
		*value = PGIcebergBinaryDeserialize(bound->value, length, field, pgType);
		*integer = type == INT8OID ? DatumGetInt64(*value) :
			type == DATEOID ? DatumGetDateADT(*value) : DatumGetInt32(*value);
		return type != DATEOID || IS_VALID_DATE(*integer);
	}
	if (type != NUMERICOID || length < 1 || length > 16 ||
		sscanf(name, "decimal(%d,%d)%n", &precision, &scale, &consumed) != 2 ||
		consumed == 0 || name[consumed] != '\0' ||
		precision < 1 || precision > 38 || scale < 0 || scale > precision ||
		pgType.postgresTypeMod != ((precision << 16) | scale) + VARHDRSZ)
		return false;

	/*
	 * All signed128 values fit numeric(39,S); then check the declared
	 * precision.
	 */
	PGType		temporaryType = pgType;

	temporaryType.postgresTypeMod = ((39 << 16) | scale) + VARHDRSZ;
	*value = PGIcebergBinaryDeserialize(bound->value, length, field, temporaryType);
	Datum		limit = DirectFunctionCall3(numeric_in,
											CStringGetDatum(psprintf("1e%d", precision - scale)),
											ObjectIdGetDatum(InvalidOid), Int32GetDatum(-1));
	Datum		absolute = DirectFunctionCall1(numeric_abs, *value);

	return DatumGetInt32(DirectFunctionCall2(numeric_cmp, absolute, limit)) < 0;
}

/*
 * GetIcebergColumnBounds returns ordered non-NULL bounds for supported columns.
 * Missing, ambiguous or invalid metrics return false. NULL counts are separate;
 * successful decoding does not establish whether the column contains NULLs.
 */
bool
GetIcebergColumnBounds(DataFile * dataFile, int fieldId, Field * field, PGType pgType,
					   Datum *lowerValue, Datum *upperValue)
{
	ColumnBound *lower = FindColumnBound(dataFile->lower_bounds,
										 dataFile->lower_bounds_length, fieldId);
	ColumnBound *upper = FindColumnBound(dataFile->upper_bounds,
										 dataFile->upper_bounds_length, fieldId);
	int64		lowerInteger = 0,
				upperInteger = 0;
	Oid			type = pgType.postgresTypeOid;

	if (lower == NULL || upper == NULL ||
		((type == INT4OID || type == INT8OID || type == DATEOID) &&
		 lower->value_length != upper->value_length) ||
		!DecodeColumnBound(field, pgType, lower, lowerValue, &lowerInteger) ||
		!DecodeColumnBound(field, pgType, upper, upperValue, &upperInteger))
		return false;

	return type == NUMERICOID ?
		DatumGetInt32(DirectFunctionCall2(numeric_cmp, *lowerValue, *upperValue)) <= 0 :
		lowerInteger <= upperInteger;
}
