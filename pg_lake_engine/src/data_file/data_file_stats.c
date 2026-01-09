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

#include "executor/executor.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/map.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"

static void ParseDuckdbColumnMinMaxFromText(char *input, List **names, List **mins, List **maxs);
static void ExtractMinMaxForAllColumns(Datum map, List **names, List **mins, List **maxs);
static void ExtractMinMaxForColumn(Datum map, const char *colName, List **names, List **mins, List **maxs);
static const char *UnescapeDoubleQuotes(const char *s);
static List *GetDataFileColumnStatsList(List *names, List *mins, List *maxs, List *leafFields, DataFileSchema * schema);


/*
 * ExecuteCopyCommandOnPGDuckConnection executes the given COPY command on
 * a PGDuck connection and returns a ColumnStatsCollector.
 */
ColumnStatsCollector *
ExecuteCopyCommandOnPGDuckConnection(char *copyCommand,
									 List *leafFields,
									 DataFileSchema * schema,
									 bool disablePreserveInsertionOrder,
									 CopyDataFormat destinationFormat)
{
	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result;
	ColumnStatsCollector *statsCollector = NULL;

	PG_TRY();
	{
		if (disablePreserveInsertionOrder)
		{
			result = ExecuteQueryOnPGDuckConnection(pgDuckConn, "SET preserve_insertion_order TO 'false';");
			CheckPGDuckResult(pgDuckConn, result);
			PQclear(result);
		}

		result = ExecuteQueryOnPGDuckConnection(pgDuckConn, copyCommand);
		CheckPGDuckResult(pgDuckConn, result);

		if (destinationFormat == DATA_FORMAT_PARQUET)
		{
			/* DuckDB returns COPY 0 when return_stats is used. */
			statsCollector = GetDataFileStatsListFromPGResult(result, leafFields, schema);
		}
		else
		{
			char	   *commandTuples = PQcmdTuples(result);

			statsCollector = palloc0(sizeof(ColumnStatsCollector));
			statsCollector->totalRowCount = atoll(commandTuples);
			statsCollector->dataFileStats = NIL;
		}

		PQclear(result);

		if (disablePreserveInsertionOrder)
		{
			result = ExecuteQueryOnPGDuckConnection(pgDuckConn, "RESET preserve_insertion_order;");
			CheckPGDuckResult(pgDuckConn, result);
			PQclear(result);
		}
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(pgDuckConn);
	}
	PG_END_TRY();

	return statsCollector;
}


/*
 * GetDataFileStatsListFromPGResult extracts DataFileStats list from the
 * given PGresult of COPY .. TO ... WITH (return_stats).
 *
 * It returns the collector object that contains the total row count and data file statistics.
 */
ColumnStatsCollector *
GetDataFileStatsListFromPGResult(PGresult *result, List *leafFields, DataFileSchema * schema)
{
	List	   *statsList = NIL;

	int			resultRowCount = PQntuples(result);
	int			resultColumnCount = PQnfields(result);
	int64		totalRowCount = 0;

	for (int resultRowIndex = 0; resultRowIndex < resultRowCount; resultRowIndex++)
	{
		DataFileStats *fileStats = palloc0(sizeof(DataFileStats));

		for (int resultColIndex = 0; resultColIndex < resultColumnCount; resultColIndex++)
		{
			char	   *resultColName = PQfname(result, resultColIndex);
			char	   *resultValue = PQgetvalue(result, resultRowIndex, resultColIndex);

			if (schema != NULL && strcmp(resultColName, "column_statistics") == 0)
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
				totalRowCount += fileStats->rowCount;
			}
			else if (strcmp(resultColName, "filename") == 0)
			{
				fileStats->dataFilePath = pstrdup(resultValue);
			}
		}

		statsList = lappend(statsList, fileStats);
	}

	ColumnStatsCollector *statsCollector = palloc0(sizeof(ColumnStatsCollector));

	statsCollector->totalRowCount = totalRowCount;
	statsCollector->dataFileStats = statsList;

	return statsCollector;
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
 * ParseDuckdbColumnMinMaxFromText parses COPY .. TO .parquet WITH (return_stats)
 * output text to map(text, map(text,text)).
 * e.g. { 'id_col' => {'min' => '12', 'max' => 23, ...},
 * 		  'name_col' => {'min' => 'aykut', 'max' => 'onder', ...},
 *         ...
 * 		}
 */
static void
ParseDuckdbColumnMinMaxFromText(char *input, List **names, List **mins, List **maxs)
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

	Datum		statsMapDatum = OidInputFunctionCall(typinput, input, typioparam, -1);

	/*
	 * extract min and max for each column: iterate the underlying map datum
	 * directly to avoid invoking the set-returning `entries()` function in a
	 * non-SRF context.
	 */
	ExtractMinMaxForAllColumns(statsMapDatum, names, mins, maxs);
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

		int			nameIndex = fieldIndex;

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
