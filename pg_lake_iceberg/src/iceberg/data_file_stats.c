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

#include <math.h>

#include "mb/pg_wchar.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

static void SetColumnBoundsFromDataFileStats(const DataFileStats * dataFileStats,
											 ColumnBound * *lowerBounds,
											 size_t *nLowerBounds,
											 ColumnBound * *upperBounds,
											 size_t *nUpperBounds,
											 ColumnStat * *nanValueCounts,
											 size_t *nNanValueCounts);
static ColumnBound * CreateColumnBoundForLeafField(LeafField * leafField, char *columnBoundText);
static ColumnBound * FindColumnBound(ColumnBound * bounds, size_t boundCount, int fieldId);
static bool DataFileHasNoNaN(DataFile * dataFile, int fieldId);
static char *ColumnBoundText(ColumnBound * bound, LeafField * leafField);

/*
 * SetIcebergDataFileStats sets the record count, file size, lower and upper bounds
 * and NaN counts for the given data file stats.
 */
void
SetIcebergDataFileStats(const DataFileStats * dataFileStats,
						int64_t *recordCount,
						int64_t *fileSizeInBytes,
						ColumnBound * *lowerBounds,
						size_t *nLowerBounds,
						ColumnBound * *upperBounds,
						size_t *nUpperBounds,
						ColumnStat * *nanValueCounts,
						size_t *nNanValueCounts)
{
	*recordCount = dataFileStats->rowCount;
	*fileSizeInBytes = dataFileStats->fileSize;

	SetColumnBoundsFromDataFileStats(dataFileStats, lowerBounds, nLowerBounds,
									 upperBounds, nUpperBounds,
									 nanValueCounts, nNanValueCounts);
}


/*
 * SetColumnBoundsFromDataFileStats sets the lower and upper bounds for each leaf field
 * in the given data file stats.
 *
 * A float or double column only has bounds when the file has no NaN (see
 * ExtractMinMaxForColumn), so it also gets a NaN count of zero. Readers need
 * that count to trust its bounds, since Iceberg bounds leave out NaN.
 */
static void
SetColumnBoundsFromDataFileStats(const DataFileStats * dataFileStats,
								 ColumnBound * *lowerBounds,
								 size_t *nLowerBounds,
								 ColumnBound * *upperBounds,
								 size_t *nUpperBounds,
								 ColumnStat * *nanValueCounts,
								 size_t *nNanValueCounts)
{
	List	   *columnStats = dataFileStats->columnStats;

	int			totalColumnStats = list_length(columnStats);

	ColumnBound *lBounds = palloc0(sizeof(ColumnBound) * totalColumnStats);

	ColumnBound *uBounds = palloc0(sizeof(ColumnBound) * totalColumnStats);

	ColumnStat *nanCounts = palloc0(sizeof(ColumnStat) * totalColumnStats);

	int			totalLowerBounds = 0;
	int			totalUpperBounds = 0;
	int			totalNanCounts = 0;

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

			if (LeafFieldIsFloatingPoint(leafField))
			{
				nanCounts[totalNanCounts].column_id = leafField->fieldId;
				nanCounts[totalNanCounts].value = 0;
				totalNanCounts++;
			}
		}
	}

	*lowerBounds = lBounds;
	*nLowerBounds = totalLowerBounds;

	*upperBounds = uBounds;
	*nUpperBounds = totalUpperBounds;

	*nanValueCounts = nanCounts;
	*nNanValueCounts = totalNanCounts;
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


/*
 * GetIcebergDataFileColumnStats returns the column stats of a data file as the
 * manifest entry records them: the lower and upper bounds, matched to the leaf
 * fields by field id. The bounds are rendered in the text form of each type,
 * the same form the Parquet footer path produced, so callers can use the result
 * in its place.
 *
 * A field without both bounds, or one whose statistics are never used for
 * pruning (ShouldSkipStatistics), gets no entry and is therefore unprunable.
 * The Iceberg spec allows a writer to record only one bound, for instance a
 * truncated string upper bound it cannot increment, but that is rare enough
 * not to be worth a one-sided constraint.
 *
 * The manifest may come from any writer, and pruning is only an optimization,
 * so a bound we cannot use safely is dropped rather than raising an error; see
 * ColumnBoundText.
 */
List *
GetIcebergDataFileColumnStats(DataFile * dataFile, List *leafFields)
{
	List	   *columnStatsList = NIL;
	ListCell   *leafFieldCell = NULL;

	foreach(leafFieldCell, leafFields)
	{
		LeafField  *leafField = lfirst(leafFieldCell);

		if (ShouldSkipStatistics(leafField))
			continue;

		ColumnBound *lowerBound = FindColumnBound(dataFile->lower_bounds,
												  dataFile->lower_bounds_length,
												  leafField->fieldId);
		ColumnBound *upperBound = FindColumnBound(dataFile->upper_bounds,
												  dataFile->upper_bounds_length,
												  leafField->fieldId);

		if (lowerBound == NULL || upperBound == NULL)
			continue;

		/*
		 * Iceberg bounds leave out NaN, which Postgres orders above every
		 * other value, so float bounds only cover the file if the writer says
		 * it has no NaN.
		 */
		if (LeafFieldIsFloatingPoint(leafField) &&
			!DataFileHasNoNaN(dataFile, leafField->fieldId))
			continue;

		char	   *lowerBoundText = ColumnBoundText(lowerBound, leafField);
		char	   *upperBoundText = ColumnBoundText(upperBound, leafField);

		if (lowerBoundText == NULL || upperBoundText == NULL)
			continue;

		DataFileColumnStats *columnStats = palloc0(sizeof(DataFileColumnStats));

		columnStats->leafField = *leafField;
		columnStats->lowerBoundText = lowerBoundText;
		columnStats->upperBoundText = upperBoundText;

		columnStatsList = lappend(columnStatsList, columnStats);
	}

	return columnStatsList;
}


/*
 * FindColumnBound returns the bound for the given field id, or NULL if the
 * data file has none.
 */
static ColumnBound *
FindColumnBound(ColumnBound * bounds, size_t boundCount, int fieldId)
{
	for (size_t boundIndex = 0; boundIndex < boundCount; boundIndex++)
	{
		if (bounds[boundIndex].column_id == fieldId)
			return &bounds[boundIndex];
	}

	return NULL;
}


/*
 * DataFileHasNoNaN returns whether the manifest entry records a NaN count of
 * zero for the given field. A missing count means the writer did not say.
 */
static bool
DataFileHasNoNaN(DataFile * dataFile, int fieldId)
{
	for (size_t countIndex = 0; countIndex < dataFile->nan_value_counts_length; countIndex++)
	{
		if (dataFile->nan_value_counts[countIndex].column_id == fieldId)
			return dataFile->nan_value_counts[countIndex].value == 0;
	}

	return false;
}


/*
 * ColumnBoundText deserializes a binary column bound and returns it in the
 * text form of the leaf field's type, or NULL if the bound cannot be used:
 * its length does not fit the type, it is outside the range of the Postgres
 * type, it is NaN, or it is a string Postgres cannot hold. Using such a bound
 * would either fail the query or, worse, prune a file that has matching rows.
 */
static char *
ColumnBoundText(ColumnBound * bound, LeafField * leafField)
{
	PGType		pgType = leafField->pgType;
	Oid			typeOid = pgType.postgresTypeOid;

	if (bound->value == NULL ||
		!IcebergBinaryLengthIsValid(bound->value_length, leafField->field, pgType))
		return NULL;

	/*
	 * Postgres text cannot hold a NUL byte, and deserializing would cut the
	 * string there, making an upper bound too small. Strings in another
	 * encoding cannot be compared the way the writer ordered them.
	 */
	if (typeOid == TEXTOID &&
		(memchr(bound->value, '\0', bound->value_length) != NULL ||
		 !pg_verifymbstr((char *) bound->value, bound->value_length, true)))
		return NULL;

	Datum		boundDatum = PGIcebergBinaryDeserialize(bound->value, bound->value_length,
														leafField->field, pgType);

	switch (typeOid)
	{
		case FLOAT4OID:
			{
				float4		value = DatumGetFloat4(boundDatum);

				if (isnan(value))
					return NULL;

				/*
				 * Print every digit: the output function rounds according to
				 * extra_float_digits, and a rounded bound may not contain the
				 * file's values.
				 */
				if (isinf(value))
					return pstrdup(value > 0 ? "Infinity" : "-Infinity");

				return psprintf("%.9g", value);
			}

		case FLOAT8OID:
			{
				float8		value = DatumGetFloat8(boundDatum);

				if (isnan(value))
					return NULL;

				if (isinf(value))
					return pstrdup(value > 0 ? "Infinity" : "-Infinity");

				return psprintf("%.17g", value);
			}

		case DATEOID:
			if (!IS_VALID_DATE(DatumGetDateADT(boundDatum)))
				return NULL;
			break;

		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				/*
				 * Moving an Iceberg timestamp to the Postgres epoch wraps
				 * around for the earliest ones, and the result can look like
				 * a valid far-future timestamp, so check the value before the
				 * shift too.
				 */
				int64		unixMicros = 0;

				for (int byteIndex = sizeof(int64) - 1; byteIndex >= 0; byteIndex--)
					unixMicros = (int64) (((uint64) unixMicros << 8) | bound->value[byteIndex]);

				if (unixMicros < PG_INT64_MIN +
					(int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY ||
					!IS_VALID_TIMESTAMP(DatumGetTimestamp(boundDatum)))
					return NULL;
				break;
			}

		case TIMEOID:
			if (DatumGetTimeADT(boundDatum) < 0 ||
				DatumGetTimeADT(boundDatum) > USECS_PER_DAY)
				return NULL;
			break;

		case TIMETZOID:
			{
				TimeTzADT  *timetz = DatumGetTimeTzADTP(boundDatum);

				if (timetz->time < 0 || timetz->time > USECS_PER_DAY)
					return NULL;
				break;
			}

		default:
			break;
	}

	Oid			typoutput;
	bool		typIsVarlena;

	getTypeOutputInfo(typeOid, &typoutput, &typIsVarlena);

	return OidOutputFunctionCall(typoutput, boundDatum);
}
