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
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/serialize.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"

static void ParseDuckdbColumnMinMaxFromText(char *input, List **names, List **mins, List **maxs);
static void ExtractMinMaxForAllColumns(Datum returnStatsMap, List **names, List **mins, List **maxs);
static void ExtractMinMaxForColumn(Datum map, char *colName, List **names, List **mins, List **maxs);
static char *UnescapeDoubleQuotes(char *s);
static List *GetDataFileColumnStatsList(List *names, List *mins, List *maxs, List *leafFields, DataFileSchema * schema);
static int	FindIndexInStringList(List *names, const char *targetName);
static List *FetchRowGroupStats(PGDuckConnection * pgDuckConn, List *fieldIdList, char *path);
static char *PrepareRowGroupStatsMinMaxQuery(List *rowGroupStatList);
static char *SerializeTextArrayTypeToPgDuck(ArrayType *array);
static ArrayType *ReadArrayFromText(char *arrayText);
static List *GetFieldMinMaxStats(PGDuckConnection * pgDuckConn, List *rowGroupStatsList);
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
* The output is in the format of:
*     field_id, ARRAY[val1, val2, val3.., valN]
*
* The array values are NOT yet sorted, they are the stats_min and stats_max values
* from the parquet metadata. We put min and max values in the same array to because
* we want the global ordering of the values, not per row group.
*
* Also note that the values are in string format, and need to be converted to the
* appropriate type before being sorted.
*/
typedef struct RowGroupStats
{
	LeafField  *leafField;
	ArrayType  *minMaxArray;
}			RowGroupStats;


/*
 * ExecuteCopyToCommandOnPGDuckConnection executes the given COPY TO command on
 * a PGDuck connection and returns a StatsCollector.
 */
StatsCollector *
ExecuteCopyToCommandOnPGDuckConnection(char *copyCommand,
									   List *leafFields,
									   DataFileSchema * schema,
									   char *destinationPath,
									   CopyDataFormat destinationFormat)
{
	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result;
	StatsCollector *statsCollector = NULL;

	PG_TRY();
	{
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
			int64		totalRowCount = atoll(commandTuples);

#ifdef USE_ASSERT_CHECKING
			if (EnableHeavyAsserts)
			{
				List	   *remoteFiles = ListRemoteFileNames(destinationPath);

				if (list_length(remoteFiles) != 1)
				{
					ereport(ERROR, (errmsg("expected exactly one file at %s, found %d files",
										   destinationPath, list_length(remoteFiles))));
				}
			}
#endif

			DataFileStats *fileStats = CreateDataFileStatsForDataFile(destinationPath,
																	  totalRowCount,
																	  0,
																	  leafFields);

			statsCollector = palloc0(sizeof(StatsCollector));
			statsCollector->totalRowCount = totalRowCount;
			statsCollector->dataFileStats = list_make1(fileStats);
		}

		PQclear(result);
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
StatsCollector *
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

	StatsCollector *statsCollector = palloc0(sizeof(StatsCollector));

	statsCollector->totalRowCount = totalRowCount;
	statsCollector->dataFileStats = statsList;

	return statsCollector;
}


/*
 * ExtractMinMaxFromStatsMapDatum extracts min and max values from given stats map
 * of type map(varchar,varchar).
 */
static void
ExtractMinMaxForColumn(Datum map, char *colName, List **names, List **mins, List **maxs)
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
		*names = lappend(*names, colName);
		*mins = lappend(*mins, minText);
		*maxs = lappend(*maxs, maxText);
	}

	array_free_iterator(arrayIterator);
}


/*
 * UnescapeDoubleQuotes unescapes any doubled quotes.
 * e.g. "ab\"\"cd\"\"ee" becomes "ab\"cd\"ee"
 */
static char *
UnescapeDoubleQuotes(char *s)
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
ExtractMinMaxForAllColumns(Datum returnStatsMap, List **names, List **mins, List **maxs)
{
	ArrayType  *elementsArray = DatumGetArrayTypeP(returnStatsMap);

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
		char	   *unescapedColName = UnescapeDoubleQuotes(colName);

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
						errmsg("cannot find required map type for parsing return stats")));

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

		int			nameIndex = FindIndexInStringList(names, fieldName);

		if (nameIndex == -1)
		{
			ereport(DEBUG3, (errmsg("field with name %s not found in stats output, skipping", fieldName)));
			continue;
		}

		LeafField  *leafField = FindLeafField(leafFields, fieldId);

		if (leafField == NULL)
		{
			ereport(DEBUG3, (errmsg("leaf field with name %s not found in leaf fields, skipping", fieldName)));
			continue;
		}
		else if (ShouldSkipStatistics(leafField))
		{
			ereport(DEBUG3, (errmsg("skipping statistics for field with name %s", fieldName)));
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
  * GetRemoteParquetColumnStats gets the stats for each leaf field
  * in a remote Parquet file.
  */
List *
GetRemoteParquetColumnStats(char *path, List *leafFields)
{
	if (list_length(leafFields) == 0)
	{
		/*
		 * short circuit for empty list, otherwise need to adjust the below
		 * query
		 */
		return NIL;
	}

	/*
	 * Sort the leaf fields by fieldId, and then use ORDER BY in the query to
	 * ensure that the results are in the same order as the input list.
	 */
	List	   *leafFieldsCopy = list_copy(leafFields);

	list_sort(leafFieldsCopy, LeafFieldCompare);

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();

	List	   *rowGroupStatsList = FetchRowGroupStats(pgDuckConn, leafFieldsCopy, path);

	if (list_length(rowGroupStatsList) == 0)
	{
		/* no stats available */
		ReleasePGDuckConnection(pgDuckConn);
		return NIL;
	}

	List	   *columnStatsList = GetFieldMinMaxStats(pgDuckConn, rowGroupStatsList);

	ReleasePGDuckConnection(pgDuckConn);
	return columnStatsList;
}


/*
* FetchRowGroupStats fetches the statistics for the given leaf fields.
* The output is in the format of:
*     field_id, ARRAY[val1, val2, val3.., valN]
*     field_id, ARRAY[val1, val2, val3.., valN]
*    ...
* The array values are NOT yet sorted, they are the stats_min and stats_max values
* from the parquet metadata. We put min and max values in the same array to because
* we want the global ordering of the values, not per row group.
*
* Also note that the values are in string format, and need to be converted to the
* appropriate type before being sorted.
*
* The output is sorted by the input fieldIdList.
*/
static List *
FetchRowGroupStats(PGDuckConnection * pgDuckConn, List *fieldIdList, char *path)
{
	List	   *rowGroupStatsList = NIL;

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,

	/*
	 * column_id_field_id_mapping: maps the column_id to the field_id for all
	 * the leaf fields. We come up with this mapping by checking the DuckDB
	 * source code, we should be careful if they ever break this assumption.
	 */
					 "WITH column_id_field_id_mapping AS ( "
					 "	SELECT row_number() OVER () - 1 AS column_id, field_id "
					 "	FROM parquet_schema(%s)   "
					 "	WHERE num_children IS NULL and field_id <> "
					 PG_LAKE_TOSTRING(ICEBERG_ROWID_FIELD_ID)
					 "), "

	/*
	 * Fetch the parquet metadata per column_id. For each column_id, we may
	 * get multiple row groups, and we need to aggregate the stats_min and
	 * stats_max values for each column_id.
	 */
					 "parquet_metadata AS ( "
					 "		SELECT column_id, stats_min, stats_min_value, stats_max, stats_max_value "
					 "		FROM parquet_metadata(%s)), "

	/*
	 * Now, we aggregate the stats_min and stats_max values for each
	 * column_id. Note that we use the coalesce function to handle the case
	 * where stats_min is NULL, and we use the stats_min_value instead. We
	 * currently don't have a good grasp on when DuckDB uses stats_min vs
	 * stats_min_value, so we use both. Typically both is set to the same
	 * value, but we want to be safe. We use the array_agg function to collect
	 * all the min/max values into an array, and values are not casted to the
	 * appropriate type yet, we create a text array. Finding min/max values
	 * for different data types in the same query is tricky as there is no
	 * support for casting to a type with a dynamic type name. So, doing it in
	 * two queries is easier to understand/maintain.
	 */
					 "row_group_aggs AS ( "
					 "SELECT c.field_id,  "
					 "       array_agg(CAST(coalesce(m.stats_min, m.stats_min_value) AS TEXT)) "
					 "                 FILTER (WHERE m.stats_min IS NOT NULL OR m.stats_min_value IS NOT NULL) || "
					 "       array_agg(CAST(coalesce(m.stats_max, m.stats_max_value) AS TEXT)) "
					 "                  FILTER (WHERE m.stats_max IS NOT NULL OR m.stats_max_value IS NOT NULL)  AS values "
					 "FROM column_id_field_id_mapping c "
					 "JOIN parquet_metadata m USING (column_id) "
					 "GROUP BY c.field_id) "
					 "SELECT field_id, values FROM row_group_aggs ORDER BY field_id;",
					 quote_literal_cstr(path), quote_literal_cstr(path));

	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query->data);

	/* throw error if anything failed  */
	CheckPGDuckResult(pgDuckConn, result);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		int			rowCount = PQntuples(result);

		for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			if (PQgetisnull(result, rowIndex, 0))
			{
				/* the data file doesn't have field id */
				continue;
			}

			int			fieldId = atoi(PQgetvalue(result, rowIndex, 0));
			LeafField  *leafField = FindLeafField(fieldIdList, fieldId);

			if (leafField == NULL)
				/* dropped column for external iceberg tables */
				continue;

			if (ShouldSkipStatistics(leafField))
				continue;

			char	   *minMaxArrayText = NULL;

			if (!PQgetisnull(result, rowIndex, 1))
			{
				minMaxArrayText = pstrdup(PQgetvalue(result, rowIndex, 1));
			}

			RowGroupStats *rowGroupStats = palloc0(sizeof(RowGroupStats));

			rowGroupStats->leafField = leafField;
			rowGroupStats->minMaxArray = minMaxArrayText ? ReadArrayFromText(minMaxArrayText) : NULL;

			rowGroupStatsList = lappend(rowGroupStatsList, rowGroupStats);
		}
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	PQclear(result);

	return rowGroupStatsList;
}


/*
* For the given rowGroupStatList, prepare the query to get the min and max values
* for each field. In the end, we will have a query like:
* 		SELECT 1,
*			   list_aggregate(CAST(min_max_array AS type[]), 'min') as field_1_min,
*			   list_aggregate(CAST(min_max_array AS type[]), 'max') as field_1_max,
*			   2,
*			   list_aggregate(CAST(min_max_array AS type[]), 'min') as field_2_min,
*			   list_aggregate(CAST(min_max_array AS type[]), 'max') as field_2_max,
*			   ...
* We are essentially aggregating the min and max values for each field in the same query. This scales
* better than UNION ALL queries for each field.
*/
static char *
PrepareRowGroupStatsMinMaxQuery(List *rowGroupStatList)
{
	StringInfo	query = makeStringInfo();

	ListCell   *lc;

	appendStringInfo(query, "SELECT ");

	foreach(lc, rowGroupStatList)
	{
		RowGroupStats *rowGroupStats = lfirst(lc);
		LeafField  *leafField = rowGroupStats->leafField;
		int			fieldId = leafField->fieldId;

		if (rowGroupStats->minMaxArray != NULL)
		{
			char	   *reserializedArray = SerializeTextArrayTypeToPgDuck(rowGroupStats->minMaxArray);

			appendStringInfo(query, " %d, list_aggregate(CAST(%s AS %s[]), 'min') as field_%d_min, "
							 "list_aggregate(CAST(%s AS %s[]), 'max')  as field_%d_min, ",
							 fieldId,
							 quote_literal_cstr(reserializedArray), leafField->duckTypeName, fieldId,
							 quote_literal_cstr(reserializedArray), leafField->duckTypeName, fieldId);
		}
		else
		{
			appendStringInfo(query, " %d, NULL  as field_%d_min, NULL  as field_%d_min, ", fieldId, fieldId, fieldId);
		}
	}

	return query->data;
}


/*
* The input array is in the format of {val1, val2, val3, ..., valN},
* and element type is text. Serialize it to text in DuckDB format.
*/
static char *
SerializeTextArrayTypeToPgDuck(ArrayType *array)
{
	Datum		arrayDatum = PointerGetDatum(array);

	FmgrInfo	outFunc;
	Oid			outFuncId = InvalidOid;
	bool		isvarlena = false;

	getTypeOutputInfo(TEXTARRAYOID, &outFuncId, &isvarlena);
	fmgr_info(outFuncId, &outFunc);

	return PGDuckSerialize(&outFunc, TEXTARRAYOID, arrayDatum);
}


/*
* ReadArrayFromText reads the array from the given text.
*/
static ArrayType *
ReadArrayFromText(char *arrayText)
{
	Oid			funcOid = F_ARRAY_IN;

	FmgrInfo	flinfo;

	fmgr_info(funcOid, &flinfo);

	/* array in has 3 arguments */
	LOCAL_FCINFO(fcinfo, 3);

	InitFunctionCallInfoData(*fcinfo,
							 &flinfo,
							 3,
							 InvalidOid,
							 NULL,
							 NULL);

	fcinfo->args[0].value = CStringGetDatum(arrayText);
	fcinfo->args[0].isnull = false;

	fcinfo->args[1].value = ObjectIdGetDatum(TEXTOID);
	fcinfo->args[1].isnull = false;

	fcinfo->args[2].value = Int32GetDatum(-1);
	fcinfo->args[2].isnull = false;

	Datum		result = FunctionCallInvoke(fcinfo);

	if (fcinfo->isnull)
	{
		/* not expected given we only call this for non-null text */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("could not reserialize text array")));
	}

	return DatumGetArrayTypeP(result);
}

/*
* GetFieldMinMaxStats gets the min and max values for each field in the given rowGroupedStatList.
* In this function, we create a query where we first cast the minMaxArray to the appropriate type
* and then aggregate the min and max values for each field.
*/
static List *
GetFieldMinMaxStats(PGDuckConnection * pgDuckConn, List *rowGroupStatList)
{
	char	   *query = PrepareRowGroupStatsMinMaxQuery(rowGroupStatList);

	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query);

	/* throw error if anything failed  */
	CheckPGDuckResult(pgDuckConn, result);

	List	   *columnStatsList = NIL;

#ifdef USE_ASSERT_CHECKING

	/*
	 * We never omit any entries from the rowGroupStatList, and for each
	 * rowGroupStatList entry, we have 3 columns: fieldId, minValue and
	 * maxValue.
	 */
	int			rowGroupLength = list_length(rowGroupStatList);

	Assert(PQnfields(result) == rowGroupLength * 3);
#endif

	PG_TRY();
	{
		for (int columnIndex = 0; columnIndex < PQnfields(result); columnIndex = columnIndex + 3)
		{
			DataFileColumnStats *columnStats = palloc0(sizeof(DataFileColumnStats));
			int			rowGroupIndex = columnIndex / 3;

			RowGroupStats *rowGroupStats = list_nth(rowGroupStatList, rowGroupIndex);
			LeafField  *leafField = rowGroupStats->leafField;

#ifdef USE_ASSERT_CHECKING
			/* we use a sorted rowGroupStatList, so should be */
			int			fieldId = atoi(PQgetvalue(result, 0, columnIndex));

			Assert(leafField->fieldId == fieldId);
#endif

			columnStats->leafField = *leafField;

			int			lowerBoundIndex = columnIndex + 1;

			if (!PQgetisnull(result, 0, lowerBoundIndex))
			{
				/* the data file doesn't have field id */
				columnStats->lowerBoundText = pstrdup(PQgetvalue(result, 0, lowerBoundIndex));
			}
			else
				columnStats->lowerBoundText = NULL;

			int			upperBoundIndex = columnIndex + 2;

			if (!PQgetisnull(result, 0, upperBoundIndex))
			{
				/* the data file doesn't have field id */
				columnStats->upperBoundText = pstrdup(PQgetvalue(result, 0, upperBoundIndex));
			}
			else
				columnStats->upperBoundText = NULL;

			columnStatsList = lappend(columnStatsList, columnStats);
		}
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	PQclear(result);
	return columnStatsList;
}


/*
 * CreateDataFileStatsForDataFile creates the data file stats for the given data file.
 * It uses already calculated file level stats. And sends remote queries
 * to the file to extract the column level stats if leafFields is not NIL.
 */
DataFileStats *
CreateDataFileStatsForDataFile(char *dataFilePath, int64 rowCount, int64 deletedRowCount,
							   List *leafFields)
{

	List	   *columnStats;

	if (leafFields != NIL)
		columnStats = GetRemoteParquetColumnStats(dataFilePath, leafFields);
	else
		columnStats = NIL;

	int64		fileSize = GetRemoteFileSize(dataFilePath);

	DataFileStats *dataFileStats = palloc0(sizeof(DataFileStats));

	dataFileStats->dataFilePath = dataFilePath;
	dataFileStats->fileSize = fileSize;
	dataFileStats->rowCount = rowCount;
	dataFileStats->deletedRowCount = deletedRowCount;
	dataFileStats->columnStats = columnStats;

	return dataFileStats;
}


/*
 * ApplyColumnStatsModeForAllFileStats applies the column stats mode to the given
 * lower and upper bound text for all file stats.
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
