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

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/parquet/leaf_field.h"

#include "utils/lsyscache.h"

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
						int64_t * recordCount,
						int64_t * fileSizeInBytes,
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

		if (lowerBoundText != NULL)
		{
			ColumnBound *lowerBound = CreateColumnBoundForLeafField(leafField, lowerBoundText);

			if (lowerBound)
			{
				lBounds[totalLowerBounds] = *lowerBound;
				totalLowerBounds++;
			}
		}

		if (upperBoundText != NULL)
		{
			/*
			 * a non-empty upper bound requires a non-empty lower bound. But
			 * reverse is not always true when stats mode is truncate and
			 * overflow occurs while truncating upper bound.
			 */
			Assert(lowerBoundText != NULL);

			ColumnBound *upperBound = CreateColumnBoundForLeafField(leafField, upperBoundText);

			if (upperBound)
			{
				uBounds[totalUpperBounds] = *upperBound;
				totalUpperBounds++;
			}
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
		return NULL;
	}

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
