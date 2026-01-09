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

#include "pg_lake/fdw/data_file_stats.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/util/rel_utils.h"

#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"


static ColumnStatsConfig GetColumnStatsConfig(Oid relationId);
static void ApplyColumnStatsModeForType(ColumnStatsConfig columnStatsConfig,
										PGType pgType, char **lowerBoundText,
										char **upperBoundText);
static char *TruncateStatsMinForText(char *lowerBound, size_t truncateLen);
static char *TruncateStatsMaxForText(char *upperBound, size_t truncateLen);
static bytea *TruncateStatsMinForBinary(bytea *lowerBound, size_t truncateLen);
static bytea *TruncateStatsMaxForBinary(bytea *upperBound, size_t truncateLen);
static Datum ColumnStatsTextToDatum(char *text, PGType pgType);
static char *DatumToColumnStatsText(Datum datum, PGType pgType, bool isNull);

/*
 * CreateDataFileStatsForTable creates the data file stats for the given table's
 * data file. It uses already calculated file level stats. And sends remote queries
 * to the file to extract the column level stats.
 */
DataFileStats *
CreateDataFileStatsForTable(Oid relationId, char *dataFilePath, int64 rowCount,
							int64 deletedRowCount, DataFileContent content)
{
	PgLakeTableProperties properties = GetPgLakeTableProperties(relationId);

	List	   *columnStats;

	if (properties.tableType == PG_LAKE_ICEBERG_TABLE_TYPE && content == CONTENT_DATA)
	{
		List	   *leafFields = GetLeafFieldsForTable(relationId);

		columnStats = GetRemoteParquetColumnStats(dataFilePath, leafFields);

		ApplyColumnStatsMode(relationId, columnStats);
	}
	else
	{
		columnStats = NIL;
	}

	int64		fileSize = GetRemoteFileSize(dataFilePath);

	DataFileStats *dataFileStats = palloc0(sizeof(DataFileStats));

	dataFileStats->fileSize = fileSize;
	dataFileStats->rowCount = rowCount;
	dataFileStats->deletedRowCount = deletedRowCount;
	dataFileStats->columnStats = columnStats;

	return dataFileStats;
}


/*
 * CreateDataFileColumnStats creates a new DataFileColumnStats from the given
 * parameters.
 */
DataFileColumnStats *
CreateDataFileColumnStats(int fieldId, PGType pgType, char *lowerBoundText, char *upperBoundText)
{
	DataFileColumnStats *columnStats = palloc0(sizeof(DataFileColumnStats));

	columnStats->leafField.fieldId = fieldId;
	columnStats->lowerBoundText = lowerBoundText;
	columnStats->upperBoundText = upperBoundText;
	columnStats->leafField.pgType = pgType;

	bool		forAddColumn = false;
	int			subFieldIndex = fieldId;

	Field	   *field = PostgresTypeToIcebergField(pgType, forAddColumn, &subFieldIndex);

	Assert(field->type == FIELD_TYPE_SCALAR);

	columnStats->leafField.field = field;

	const char *duckTypeName = IcebergTypeNameToDuckdbTypeName(field->field.scalar.typeName);

	columnStats->leafField.duckTypeName = duckTypeName;

	return columnStats;
}


/*
 * ApplyColumnStatsMode applies the column stats mode to the given lower and upper
 * bound text.
 *
 * e.g. with "truncate(3)"
 * "abcdef" -> lowerbound: "abc" upperbound: "abd"
 * "\x010203040506" -> lowerbound: "\x010203" upperbound: "\x010204"
 *
 * e.g. with "full"
 * "abcdef" -> lowerbound: "abcdef" upperbound: "abcdef"
 * "\x010203040506" -> lowerbound: "\x010203040506" upperbound: "\x010203040506"
 *
 * e.g. with "none"
 * "abcdef" -> lowerbound: NULL upperbound: NULL
 * "\x010203040506" -> lowerbound: NULL upperbound: NULL
 */
void
ApplyColumnStatsMode(Oid relationId, List *columnStats)
{
	ColumnStatsConfig columnStatsConfig = GetColumnStatsConfig(relationId);

	ListCell   *columnStatsCell = NULL;

	foreach(columnStatsCell, columnStats)
	{
		DataFileColumnStats *columnStats = lfirst(columnStatsCell);

		char	  **lowerBoundText = &columnStats->lowerBoundText;
		char	  **upperBoundText = &columnStats->upperBoundText;

		ApplyColumnStatsModeForType(columnStatsConfig, columnStats->leafField.pgType, lowerBoundText, upperBoundText);
	}
}


void
ApplyColumnStatsModeForAllFileStats(Oid relationId, List *dataFileStats)
{
	ColumnStatsConfig columnStatsConfig = GetColumnStatsConfig(relationId);

	ListCell   *dataFileStatsCell = NULL;

	foreach(dataFileStatsCell, dataFileStats)
	{
		DataFileStats *dataFileStats = lfirst(dataFileStatsCell);

		ListCell   *columnStatsCell = NULL;

		foreach(columnStatsCell, dataFileStats->columnStats)
		{
			DataFileColumnStats *columnStats = lfirst(columnStatsCell);
			char	  **lowerBoundText = &columnStats->lowerBoundText;
			char	  **upperBoundText = &columnStats->upperBoundText;

			ApplyColumnStatsModeForType(columnStatsConfig, columnStats->leafField.pgType, lowerBoundText, upperBoundText);
		}
	}
}


/*
 * GetColumnStatsConfig returns the column stats config for the given
 * relation.
 */
static ColumnStatsConfig
GetColumnStatsConfig(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	DefElem    *columnStatsModeOption = GetOption(options, "column_stats_mode");

	ColumnStatsConfig config;

	/* default to truncate mode */
	if (columnStatsModeOption == NULL)
	{
		config.mode = COLUMN_STATS_MODE_TRUNCATE;
		config.truncateLen = 16;

		return config;
	}

	char	   *columnStatsMode = ToLowerCase(defGetString(columnStatsModeOption));

	if (sscanf(columnStatsMode, "truncate(%zu)", &config.truncateLen) == 1)
	{
		config.mode = COLUMN_STATS_MODE_TRUNCATE;
		if (config.truncateLen > 256)
			ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							errmsg("truncate() cannot exceed 256")));
	}
	else if (strcmp(columnStatsMode, "full") == 0)
	{
		config.mode = COLUMN_STATS_MODE_TRUNCATE;
		config.truncateLen = 256;
	}
	else if (strcmp(columnStatsMode, "none") == 0)
	{
		config.mode = COLUMN_STATS_MODE_NONE;
	}
	else
	{
		/* iceberg fdw validator already validated */
		pg_unreachable();
	}

	return config;
}


/*
 * ApplyColumnStatsModeForType applies the column stats mode to the given lower and upper
 * bound text for the given pgType.
 */
static void
ApplyColumnStatsModeForType(ColumnStatsConfig columnStatsConfig,
							PGType pgType, char **lowerBoundText,
							char **upperBoundText)
{
	if (*lowerBoundText == NULL)
	{
		return;
	}

	Assert(*upperBoundText != NULL);

	if (columnStatsConfig.mode == COLUMN_STATS_MODE_TRUNCATE)
	{
		size_t		truncateLen = columnStatsConfig.truncateLen;

		/* only text and binary types can be truncated */
		if (pgType.postgresTypeOid == TEXTOID ||
			pgType.postgresTypeOid == VARCHAROID ||
			pgType.postgresTypeOid == BPCHAROID)
		{
			*lowerBoundText = TruncateStatsMinForText(*lowerBoundText, truncateLen);
			Assert(*lowerBoundText != NULL);

			/* could be null if overflow occurred */
			*upperBoundText = TruncateStatsMaxForText(*upperBoundText, truncateLen);
		}
		else if (pgType.postgresTypeOid == BYTEAOID)
		{
			/*
			 * convert from text repr (e.g. '\x0102ef') to bytea to apply
			 * truncate
			 */
			Datum		lowerBoundDatum = ColumnStatsTextToDatum(*lowerBoundText, pgType);
			Datum		upperBoundDatum = ColumnStatsTextToDatum(*upperBoundText, pgType);

			/* truncate bytea */
			bytea	   *truncatedLowerBoundBinary = TruncateStatsMinForBinary(DatumGetByteaP(lowerBoundDatum),
																			  truncateLen);
			bytea	   *truncatedUpperBoundBinary = TruncateStatsMaxForBinary(DatumGetByteaP(upperBoundDatum),
																			  truncateLen);

			/* convert bytea back to text representation */
			Assert(truncatedLowerBoundBinary != NULL);
			*lowerBoundText = DatumToColumnStatsText(PointerGetDatum(truncatedLowerBoundBinary),
													 pgType, false);

			/* could be null if overflow occurred */
			*upperBoundText = DatumToColumnStatsText(PointerGetDatum(truncatedUpperBoundBinary),
													 pgType, truncatedUpperBoundBinary == NULL);
		}
	}
	else if (columnStatsConfig.mode == COLUMN_STATS_MODE_NONE)
	{
		*lowerBoundText = NULL;
		*upperBoundText = NULL;
	}
	else
	{
		Assert(false);
	}
}


/*
 * TruncateStatsMinForText truncates the given lower bound text to the given length.
 */
static char *
TruncateStatsMinForText(char *lowerBound, size_t truncateLen)
{
	if (strlen(lowerBound) <= truncateLen)
	{
		return lowerBound;
	}

	lowerBound[truncateLen] = '\0';

	return lowerBound;
}


/*
 * TruncateStatsMaxForText truncates the given upper bound text to the given length.
 */
static char *
TruncateStatsMaxForText(char *upperBound, size_t truncateLen)
{
	if (strlen(upperBound) <= truncateLen)
	{
		return upperBound;
	}

	upperBound[truncateLen] = '\0';

	/*
	 * increment the last byte of the upper bound, which does not overflow. If
	 * not found, return null.
	 */
	for (int i = truncateLen - 1; i >= 0; i--)
	{
		/* check if overflows max ascii char */
		/* todo: how to handle utf8 or different encoding? */
		if (upperBound[i] != INT8_MAX)
		{
			upperBound[i]++;
			return upperBound;
		}
	}

	return NULL;
}


/*
 * TruncateStatsMinForBinary truncates the given lower bound binary to the given length.
 */
static bytea *
TruncateStatsMinForBinary(bytea *lowerBound, size_t truncateLen)
{
	size_t		lowerBoundLen = VARSIZE_ANY_EXHDR(lowerBound);

	if (lowerBoundLen <= truncateLen)
	{
		return lowerBound;
	}

	bytea	   *truncatedLowerBound = palloc0(truncateLen + VARHDRSZ);

	SET_VARSIZE(truncatedLowerBound, truncateLen + VARHDRSZ);
	memcpy(VARDATA_ANY(truncatedLowerBound), VARDATA_ANY(lowerBound), truncateLen);

	return truncatedLowerBound;
}


/*
 * TruncateStatsMaxForBinary truncates the given upper bound binary to the given length.
 */
static bytea *
TruncateStatsMaxForBinary(bytea *upperBound, size_t truncateLen)
{
	size_t		upperBoundLen = VARSIZE_ANY_EXHDR(upperBound);

	if (upperBoundLen <= truncateLen)
	{
		return upperBound;
	}

	bytea	   *truncatedUpperBound = palloc0(truncateLen + VARHDRSZ);

	SET_VARSIZE(truncatedUpperBound, truncateLen + VARHDRSZ);
	memcpy(VARDATA_ANY(truncatedUpperBound), VARDATA_ANY(upperBound), truncateLen);

	/*
	 * increment the last byte of the upper bound, which does not overflow. If
	 * not found, return null.
	 */
	for (int i = truncateLen - 1; i >= 0; i--)
	{
		/* check if overflows max byte */
		if ((unsigned char) VARDATA_ANY(truncatedUpperBound)[i] != UINT8_MAX)
		{
			VARDATA_ANY(truncatedUpperBound)[i]++;
			return truncatedUpperBound;
		}
	}

	return NULL;
}


/*
 * ColumnStatsTextToDatum converts the given text to Datum for the given pgType.
 */
static Datum
ColumnStatsTextToDatum(char *text, PGType pgType)
{
	Oid			typoinput;
	Oid			typioparam;

	getTypeInputInfo(pgType.postgresTypeOid, &typoinput, &typioparam);

	return OidInputFunctionCall(typoinput, text, typioparam, -1);
}


/*
 * DatumToColumnStatsText converts the given datum to text for the given pgType.
 */
static char *
DatumToColumnStatsText(Datum datum, PGType pgType, bool isNull)
{
	if (isNull)
	{
		return NULL;
	}

	Oid			typoutput;
	bool		typIsVarlena;

	getTypeOutputInfo(pgType.postgresTypeOid, &typoutput, &typIsVarlena);

	return OidOutputFunctionCall(typoutput, datum);
}
