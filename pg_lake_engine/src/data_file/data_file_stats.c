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

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/util/rel_utils.h"

#include "commands/defrem.h"
#include "executor/executor.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"


static void ApplyColumnStatsModeForType(ColumnStatsConfig columnStatsConfig,
										PGType pgType, char **lowerBoundText,
										char **upperBoundText);
static char *TruncateStatsMinForText(char *lowerBound, size_t truncateLen);
static char *TruncateStatsMaxForText(char *upperBound, size_t truncateLen);
static bytea *TruncateStatsMinForBinary(bytea *lowerBound, size_t truncateLen);
static bytea *TruncateStatsMaxForBinary(bytea *upperBound, size_t truncateLen);
static Datum ColumnStatsTextToDatum(char *text, PGType pgType);
static char *DatumToColumnStatsText(Datum datum, PGType pgType, bool isNull);
static void ParseDuckdbColumnMinMaxFromText(const char *input, List **names, List **mins, List **maxs);
static void ExtractMinMaxForAllColumns(Datum map, List **names, List **mins, List **maxs);
static void ExtractMinMaxForColumn(Datum map, const char *colName, List **names, List **mins, List **maxs);
static const char *UnescapeDoubleQuotes(const char *s);
static List *GetDataFileColumnStatsList(List *names, List *mins, List *maxs, List *leafFields, DataFileSchema * schema);
static int	FindIndexInStringList(List *names, const char *targetName);


/*
 * GetDataFileStatsFromCopyWithReturnStatsResult extracts DataFileStats list from the
 * given PGresult of COPY .. TO ... WITH (return_stats).
 *
 * It also returns the total row count via totalRowCount output parameter.
 */
List *
GetDataFileStatsFromCopyWithReturnStatsResult(PGresult *result, List *leafFields, DataFileSchema * schema, int64 *totalRowCount)
{
	List	   *statsList = NIL;

	int			resultRowCount = PQntuples(result);
	int			resultColumnCount = PQnfields(result);

	*totalRowCount = 0;

	for (int resultRowIndex = 0; resultRowIndex < resultRowCount; resultRowIndex++)
	{
		DataFileStats *fileStats = palloc0(sizeof(DataFileStats));

		for (int resultColIndex = 0; resultColIndex < resultColumnCount; resultColIndex++)
		{
			char	   *resultColName = PQfname(result, resultColIndex);
			char	   *resultValue = PQgetvalue(result, resultRowIndex, resultColIndex);

			if (schema != NULL && leafFields != NIL && strcmp(resultColName, "column_statistics") == 0)
			{
				List	   *names = NIL;
				List	   *mins = NIL;
				List	   *maxs = NIL;

				ParseDuckdbColumnMinMaxFromText(resultValue, &names, &mins, &maxs);
				fileStats->columnStats = GetDataFileColumnStatsList(names, mins, maxs, leafFields, schema);
			}
			else if (strcmp(resultColName, "file_size_bytes") == 0)
			{
				fileStats->fileSize = atoll(resultValue);
			}
			else if (strcmp(resultColName, "count") == 0)
			{
				fileStats->rowCount = atoll(resultValue);
				*totalRowCount += fileStats->rowCount;
			}
			else if (strcmp(resultColName, "filename") == 0)
			{
				fileStats->dataFilePath = pstrdup(resultValue);
			}
		}

		statsList = lappend(statsList, fileStats);
	}

	return statsList;
}


/*
* ShouldSkipStatistics returns true if the statistics should be skipped for the
* given leaf field.
*/
bool
ShouldSkipStatistics(LeafField * leafField)
{
	Field	   *field = leafField->field;
	PGType		pgType = leafField->pgType;

	Oid			pgTypeOid = pgType.postgresTypeOid;

	if (PGTypeRequiresConversionToIcebergString(field, pgType))
	{
		if (!(pgTypeOid == VARCHAROID || pgTypeOid == BPCHAROID ||
			  pgTypeOid == CHAROID))
		{
			/*
			 * Although there are no direct equivalents of these types on
			 * Iceberg, it is pretty safe to support pruning on these types.
			 */
			return true;
		}
	}
	else if (pgTypeOid == BYTEAOID)
	{
		/*
		 * parquet_metadata function sometimes returns a varchar repr of blob,
		 * which cannot be properly deserialized by Postgres. (when there is
		 * "\" or nonprintable chars in the blob ) See issue Old repo:
		 * issues/957
		 */
		return true;
	}
	else if (pgTypeOid == UUIDOID)
	{
		/*
		 * DuckDB does not keep statistics for UUID type. We should skip
		 * statistics for UUID type.
		 */
		return true;
	}
	else if (leafField->level != 1)
	{
		/*
		 * We currently do not support pruning on array, map and composite
		 * types. So there's no need to collect stats for them. Note that in
		 * the past we did collect, and have some tests commented out, such as
		 * skippedtest_pg_lake_iceberg_table_complex_values.
		 */
		return true;
	}

	return false;
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
ApplyColumnStatsMode(ColumnStatsConfig columnStatsConfig, List *dataFileStats)
{
	ListCell   *dataFileStatCell = NULL;

	foreach(dataFileStatCell, dataFileStats)
	{
		DataFileStats *fileStats = lfirst(dataFileStatCell);

		ListCell   *columnStatsCell = NULL;

		foreach(columnStatsCell, fileStats->columnStats)
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
ColumnStatsConfig *
GetColumnStatsConfig(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;
	DefElem    *columnStatsModeOption = GetOption(options, "column_stats_mode");

	ColumnStatsConfig *config = palloc0(sizeof(ColumnStatsConfig));

	/* default to truncate mode */
	if (columnStatsModeOption == NULL)
	{
		config->mode = COLUMN_STATS_MODE_TRUNCATE;
		config->truncateLen = 16;

		return config;
	}

	char	   *columnStatsMode = ToLowerCase(defGetString(columnStatsModeOption));

	if (sscanf(columnStatsMode, "truncate(%zu)", &(config->truncateLen)) == 1)
	{
		config->mode = COLUMN_STATS_MODE_TRUNCATE;
		if (config->truncateLen > 256)
			ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
							errmsg("truncate() cannot exceed 256")));
	}
	else if (strcmp(columnStatsMode, "full") == 0)
	{
		config->mode = COLUMN_STATS_MODE_TRUNCATE;
		config->truncateLen = 256;
	}
	else if (strcmp(columnStatsMode, "none") == 0)
	{
		config->mode = COLUMN_STATS_MODE_NONE;
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


/*
 * ParseDuckdbColumnMinMaxFromText parses COPY .. TO .parquet WITH (return_stats)
 * output text to map(text, map(text,text)).
 * e.g. { 'id_col' => {'min' => '12', 'max' => 23, ...},
 * 		  'name_col' => {'min' => 'aykut', 'max' => 'onder', ...},
 *         ...
 * 		}
 */
static void
ParseDuckdbColumnMinMaxFromText(const char *input, List **names, List **mins, List **maxs)
{
	/*
	 * e.g. { 'id_col' => {'min' => '12', 'max' => 23, ...}, 'name_col' =>
	 * {'min' => 'aykut', 'max' => 'onder', ...}, ... }
	 */
	Oid			returnStatsMapId = GetOrCreatePGMapType("MAP(TEXT,MAP(TEXT,TEXT))");

	if (returnStatsMapId == InvalidOid)
		ereport(ERROR, (errcode(ERRCODE_INVALID_TEXT_REPRESENTATION),
						errmsg("unexpected return_stats result %s", input)));

	/* parse result into map above */
	Oid			typinput;
	Oid			typioparam;

	getTypeInputInfo(returnStatsMapId, &typinput, &typioparam);

	Datum		statsMapDatum = OidInputFunctionCall(typinput, pstrdup(input), typioparam, -1);

	/*
	 * extract min and max for each column: iterate the underlying map datum
	 * directly to avoid invoking the set-returning `entries()` function in a
	 * non-SRF context.
	 */
	ExtractMinMaxForAllColumns(statsMapDatum, names, mins, maxs);
}


/*
 * ExtractMinMaxFromStatsMapDatum extracts min and max values from given stats map
 * of type map(varchar,varchar).
 */
static void
ExtractMinMaxForColumn(Datum map, const char *colName, List **names, List **mins, List **maxs)
{
	ArrayType  *elementsArray = DatumGetArrayTypeP(map);

	if (elementsArray == NULL)
		return;

	uint32		numElements = ArrayGetNItems(ARR_NDIM(elementsArray), ARR_DIMS(elementsArray));

	if (numElements == 0)
		return;

	char	   *minText = NULL;
	char	   *maxText = NULL;

	ArrayIterator arrayIterator = array_create_iterator(elementsArray, 0, NULL);
	Datum		elemDatum;
	bool		isNull = false;

	while (array_iterate(arrayIterator, &elemDatum, &isNull))
	{
		if (isNull)
			continue;

		HeapTupleHeader tupleHeader = DatumGetHeapTupleHeader(elemDatum);
		bool		statsKeyIsNull = false;
		bool		statsValIsNull = false;

		Datum		statsKeyDatum = GetAttributeByNum(tupleHeader, 1, &statsKeyIsNull);
		Datum		statsValDatum = GetAttributeByNum(tupleHeader, 2, &statsValIsNull);

		/* skip entries without a key or value */
		if (statsKeyIsNull || statsValIsNull)
			continue;

		char	   *statsKey = TextDatumGetCString(statsKeyDatum);

		if (strcmp(statsKey, "min") == 0)
		{
			Assert(minText == NULL);
			minText = TextDatumGetCString(statsValDatum);
		}
		else if (strcmp(statsKey, "max") == 0)
		{
			Assert(maxText == NULL);
			maxText = TextDatumGetCString(statsValDatum);
		}
	}

	if (minText != NULL && maxText != NULL)
	{
		*names = lappend(*names, pstrdup(colName));
		*mins = lappend(*mins, minText);
		*maxs = lappend(*maxs, maxText);
	}

	array_free_iterator(arrayIterator);
}


/*
 * UnescapeDoubleQuotes unescapes any doubled quotes.
 * e.g. "ab\"\"cd\"\"ee" becomes "ab\"cd\"ee"
 */
static const char *
UnescapeDoubleQuotes(const char *s)
{
	if (s == NULL)
		return NULL;

	char		doubleQuote = '"';

	int			len = strlen(s);

	if (len >= 2 && (s[0] == doubleQuote && s[len - 1] == doubleQuote))
	{
		/* Allocate worst-case length (without surrounding quotes) + 1 */
		char	   *out = palloc((len - 1) * sizeof(char));
		int			oi = 0;

		for (int i = 1; i < len - 1; i++)
		{
			/* Handle "" */
			if (s[i] == doubleQuote && i + 1 < len - 1 && s[i + 1] == doubleQuote)
			{
				out[oi++] = doubleQuote;
				i++;			/* skip the doubled quote */
			}
			else
			{
				out[oi++] = s[i];
			}
		}

		out[oi] = '\0';
		return out;
	}

	return s;
}


/*
 * ExtractMinMaxFromStatsMapDatum extracts min and max values from given stats map
 * of type map(text,text).
 */
static void
ExtractMinMaxForAllColumns(Datum map, List **names, List **mins, List **maxs)
{
	ArrayType  *elementsArray = DatumGetArrayTypeP(map);

	if (elementsArray == NULL)
		return;

	uint32		numElements = ArrayGetNItems(ARR_NDIM(elementsArray), ARR_DIMS(elementsArray));

	if (numElements == 0)
		return;

	ArrayIterator arrayIterator = array_create_iterator(elementsArray, 0, NULL);
	Datum		elemDatum;
	bool		isNull = false;

	while (array_iterate(arrayIterator, &elemDatum, &isNull))
	{
		if (isNull)
			continue;

		HeapTupleHeader tupleHeader = DatumGetHeapTupleHeader(elemDatum);
		bool		colNameIsNull = false;
		bool		colStatsIsNull = false;

		Datum		colNameDatum = GetAttributeByNum(tupleHeader, 1, &colNameIsNull);
		Datum		colStatsDatum = GetAttributeByNum(tupleHeader, 2, &colStatsIsNull);

		/* skip entries without a key or value */
		if (colNameIsNull || colStatsIsNull)
			continue;

		char	   *colName = TextDatumGetCString(colNameDatum);

		/*
		 * pg_map text key is escaped for double quotes. We need to unescape
		 * them.
		 */
		const char *unescapedColName = UnescapeDoubleQuotes(colName);

		ExtractMinMaxForColumn(colStatsDatum, unescapedColName, names, mins, maxs);
	}

	array_free_iterator(arrayIterator);
}


/*
 * GetDataFileColumnStatsList builds DataFileColumnStats list from given
 * names, mins, maxs lists and schema.
 */
static List *
GetDataFileColumnStatsList(List *names, List *mins, List *maxs, List *leafFields, DataFileSchema * schema)
{
	List	   *columnStatsList = NIL;

	Assert(schema != NULL);
	for (int fieldIndex = 0; fieldIndex < schema->nfields; fieldIndex++)
	{
		DataFileSchemaField *field = &schema->fields[fieldIndex];
		const char *fieldName = field->name;
		int			fieldId = field->id;

		int			nameIndex = FindIndexInStringList(names, fieldName);

		if (nameIndex == -1)
		{
			ereport(DEBUG3, (errmsg("field with name %s not found in stats output, skipping", fieldName)));
			continue;
		}

		LeafField  *leafField = FindLeafField(leafFields, fieldId);

		if (leafField == NULL)
		{
			ereport(DEBUG3, (errmsg("leaf field with id %d not found in leaf fields, skipping", fieldId)));
			continue;
		}
		else if (ShouldSkipStatistics(leafField))
		{
			ereport(DEBUG3, (errmsg("skipping statistics for field with id %d", fieldId)));
			continue;
		}

		char	   *minStr = list_nth(mins, nameIndex);
		char	   *maxStr = list_nth(maxs, nameIndex);

		DataFileColumnStats *colStats = palloc0(sizeof(DataFileColumnStats));

		colStats->leafField = *leafField;
		colStats->lowerBoundText = pstrdup(minStr);
		colStats->upperBoundText = pstrdup(maxStr);
		columnStatsList = lappend(columnStatsList, colStats);
	}

	return columnStatsList;
}


/*
 * FindIndexInStringList finds the index of targetName in names list.
 * Returns -1 if not found.
 */
static int
FindIndexInStringList(List *names, const char *targetName)
{
	for (int index = 0; index < list_length(names); index++)
	{
		if (strcmp(list_nth(names, index), targetName) == 0)
		{
			return index;
		}
	}

	return -1;
}
