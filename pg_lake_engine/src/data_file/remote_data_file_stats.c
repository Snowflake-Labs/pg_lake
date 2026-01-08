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

#include "pg_lake/data_file/remote_data_file_stats.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/serialize.h"


#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"


static List *FetchRowGroupStats(PGDuckConnection * pgDuckConn, List *fieldIdList, char *path);
static char *PrepareRowGroupStatsMinMaxQuery(List *rowGroupStatList);
static List *GetFieldMinMaxStats(PGDuckConnection * pgDuckConn, List *rowGroupStatList);
static char *SerializeTextArrayTypeToPgDuck(ArrayType *array);
static ArrayType *ReadArrayFromText(char *arrayText);


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
 * GetRemoteDataFileStatsForTable creates the data file stats for the given table's
 * data file. It uses already calculated file level stats. And sends remote queries
 * to the file to extract the column level stats.
 */
DataFileStats *
GetRemoteDataFileStatsForTable(char *dataFilePath, CopyDataFormat format,
							   CopyDataCompression compression,
							   List *formatOptions,
							   List *leafFields)
{
	List	   *columnStats = NIL;

	if (leafFields != NIL)
		columnStats = GetRemoteParquetColumnStats(dataFilePath, leafFields);

	DataFileStats *dataFileStats = palloc0(sizeof(DataFileStats));

	dataFileStats->dataFilePath = pstrdup(dataFilePath);
	dataFileStats->fileSize = GetRemoteFileSize(dataFilePath);
	dataFileStats->rowCount = GetRemoteFileRowCount(dataFilePath, format, compression, formatOptions);
	dataFileStats->deletedRowCount = 0;
	dataFileStats->columnStats = columnStats;

	return dataFileStats;
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
