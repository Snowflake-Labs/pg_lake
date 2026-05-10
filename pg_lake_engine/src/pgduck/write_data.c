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

/*
 * Functions for generating query for writing data via pgduck server.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/csv/csv_writer.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/parquet/geoparquet.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/pgduck/iceberg_query_validation.h"
#include "pg_lake/pgduck/write_data.h"
#include "pg_lake/util/numeric.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "pg_lake/pgduck/parse_struct.h"
#include "utils/lsyscache.h"

static DuckDBTypeInfo ChooseDuckDBEngineTypeForWrite(PGType postgresType,
													 CopyDataFormat destinationFormat);
static void AppendFieldIdValue(StringInfo map, Field * field, int fieldId);
static const char *ParquetVersionToString(ParquetVersion version);
static char *BuildSelectFromCSVQuery(const char *csvFilePath, TupleDesc csvTupleDesc,
									 int maxLineSize, CopyDataFormat destinationFormat);
/* BuildCopyToCommandString is exposed via write_data.h for the streaming path. */

static DuckDBTypeInfo VARCHAR_TYPE =
{
	DUCKDB_TYPE_VARCHAR, false, "VARCHAR",
};

int			TargetRowGroupSizeMB = DEFAULT_TARGET_ROW_GROUP_SIZE_MB;
int			DefaultParquetVersion = PARQUET_VERSION_V1;

/*
 * pg_lake_engine.streaming_writes — opt-in libpq COPY-IN streaming for
 * bulk-write paths (INSERT/COPY/DELETE). Defined here so the variable
 * lives next to the other write-side state. The DefineCustomBoolVariable
 * call is in init.c.
 *
 * When false, the existing file-based path is used: rows are written to a
 * local CSV under $PGDATA/pgsql_tmp and pgduck reads it via read_csv().
 *
 * When true, rows are streamed directly to pgduck_server's RECEIVE sink
 * over libpq, decoupling pgduck_server's filesystem from PGDATA. This is
 * required when pgduck_server runs on a different host than the postgres
 * backend (e.g. as a separate Kubernetes pod).
 */
bool		StreamingWritesEnabled = false;

/* PG_LAKE_RECV_PATH_PLACEHOLDER is exposed via write_data.h. */

/*
 * CSVStreamWriter is the streaming counterpart of a CSV temp file +
 * deferred ConvertCSVFileTo: it owns a libpq COPY-IN stream and a CSV
 * DestReceiver feeding bytes into it. The destination path / format /
 * options used to build the deferred RECEIVE query are kept here so we
 * can reconstruct the StatsCollector once the stream finishes.
 */
struct CSVStreamWriter
{
	PGDuckConnection *conn;
	DestReceiver *dest;
	char	   *destinationPath;
	CopyDataFormat destinationFormat;
	DataFileSchema *schema;
	List	   *leafFields;
};


/*
 * ConvertCSVFileTo copies and converts a CSV file at source path to
 * the destinationPath.
 *
 * The CSV was generated using COPY ... TO '<csvFilePath>'
 */
StatsCollector *
ConvertCSVFileTo(char *csvFilePath, TupleDesc csvTupleDesc, int maxLineSize,
				 char *destinationPath,
				 CopyDataFormat destinationFormat,
				 CopyDataCompression destinationCompression,
				 List *formatOptions,
				 DataFileSchema * schema,
				 List *leafFields)
{
	char	   *innerSelect = BuildSelectFromCSVQuery(csvFilePath, csvTupleDesc,
													  maxLineSize, destinationFormat);
	bool		queryHasRowIds = false;

	/*
	 * CSV data is already clamped by WriteInsertRecord and converted to
	 * struct for Iceberg
	 */
	return WriteQueryResultTo(innerSelect,
							  destinationPath,
							  destinationFormat,
							  destinationCompression,
							  formatOptions,
							  queryHasRowIds,
							  schema,
							  csvTupleDesc,
							  leafFields,
							  ICEBERG_OOR_NONE,
							  false /* wrapNativeTypes */ );
}


/*
 * BuildSelectFromCSVQuery builds the inner `SELECT ... FROM read_csv(<path>, ...)`
 * query that ConvertCSVFileTo wraps with COPY (...) TO ... WITH (...).
 *
 * Pulled out so the streaming path (OpenCSVStreamWriter) can build the
 * same query with the RECEIVE sink-path placeholder substituted for
 * csvFilePath.
 */
static char *
BuildSelectFromCSVQuery(const char *csvFilePath, TupleDesc csvTupleDesc,
						int maxLineSize, CopyDataFormat destinationFormat)
{
	StringInfoData command;

	initStringInfo(&command);

	/* project columns into target format */
	appendStringInfo(&command, "SELECT %s FROM ",
					 TupleDescToProjectionListForWrite(csvTupleDesc, destinationFormat));

	/* build the read_csv(...) clause */
	char	   *columnsMap = NULL;

	if (csvTupleDesc != NULL && csvTupleDesc->natts > 0)
		columnsMap = TupleDescToColumnMapForWrite(csvTupleDesc, destinationFormat);

	bool		includeHeader = true;

	AppendReadCSVClause(&command, csvFilePath, maxLineSize, columnsMap,
						InternalCSVOptions(includeHeader));

	return command.data;
}


/*
 * WriteQueryResultTo takes the result of a query and writes to
 * destinationPath. There may be multiple files if file_size_bytes
 * is specified in formatOptions.
 */
StatsCollector *
WriteQueryResultTo(char *query,
				   char *destinationPath,
				   CopyDataFormat destinationFormat,
				   CopyDataCompression destinationCompression,
				   List *formatOptions,
				   bool queryHasRowId,
				   DataFileSchema * schema,
				   TupleDesc queryTupleDesc,
				   List *leafFields,
				   IcebergOutOfRangePolicy outOfRangePolicy,
				   bool wrapNativeTypes)
{
	char	   *command = BuildCopyToCommandString(query, destinationPath,
												   destinationFormat,
												   destinationCompression,
												   formatOptions, queryHasRowId,
												   schema, queryTupleDesc,
												   outOfRangePolicy, wrapNativeTypes);

	return ExecuteCopyToCommandOnPGDuckConnection(command,
												  leafFields,
												  schema,
												  destinationPath,
												  destinationFormat);
}


/*
 * BuildCopyToCommandString wraps `query` in `COPY (...) TO 'destinationPath'
 * WITH (...)` and returns the command string. Pulled out of
 * WriteQueryResultTo so the streaming paths (OpenCSVStreamWriter and
 * StartAddQueryResultToTableStream) can build the same command but with
 * '@@PG_LAKE_RECV@@' substituted for the read_csv path arg, and then
 * prefix it with "RECEIVE " before sending.
 */
char *
BuildCopyToCommandString(char *query,
						 char *destinationPath,
						 CopyDataFormat destinationFormat,
						 CopyDataCompression destinationCompression,
						 List *formatOptions,
						 bool queryHasRowId,
						 DataFileSchema * schema,
						 TupleDesc queryTupleDesc,
						 IcebergOutOfRangePolicy outOfRangePolicy,
						 bool wrapNativeTypes)
{
	if (outOfRangePolicy != ICEBERG_OOR_NONE)
	{
		query = IcebergWrapQueryWithErrorOrClampChecks(query, queryTupleDesc,
													   outOfRangePolicy,
													   queryHasRowId);
	}

	if (wrapNativeTypes && destinationFormat == DATA_FORMAT_ICEBERG)
	{
		query = IcebergWrapQueryWithNativeTypeConversion(query, queryTupleDesc,
														 queryHasRowId);
	}

	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "COPY (%s) TO %s",
					 query,
					 quote_literal_cstr(destinationPath));

	/* start WITH options */
	appendStringInfoString(&command, " WITH (");

	/*
	 * Iceberg data files are Parquet, so use "parquet" as the DuckDB format
	 * name for both DATA_FORMAT_PARQUET and DATA_FORMAT_ICEBERG.
	 */
	const char *formatName = (destinationFormat == DATA_FORMAT_ICEBERG) ?
		"parquet" : CopyDataFormatToName(destinationFormat);

	appendStringInfo(&command, "format %s",
					 quote_literal_cstr(formatName));

	switch (destinationFormat)
	{
		case DATA_FORMAT_ICEBERG:
		case DATA_FORMAT_PARQUET:
			{
				if (destinationCompression == DATA_COMPRESSION_NONE)
				{
					/* Parquet format uses uncompressed instead of none */
					appendStringInfo(&command, ", compression 'uncompressed'");
					break;
				}
				else
				{

					const char *compressionName =
						CopyDataCompressionToName(destinationCompression);

					appendStringInfo(&command, ", compression %s",
									 quote_literal_cstr(compressionName));
				}

				ListCell   *optionCell = NULL;

				foreach(optionCell, formatOptions)
				{
					DefElem    *option = lfirst(optionCell);

					if (strcmp(option->defname, "file_size_bytes") == 0)
					{
						char	   *fileSizeStr = defGetString(option);

						appendStringInfo(&command, ", file_size_bytes %s",
										 quote_literal_cstr(fileSizeStr));
					}
				}

				if (schema != NULL)
				{
					appendStringInfoString(&command, ", field_ids {");
					AppendFields(&command, schema);

					if (queryHasRowId)
						appendStringInfo(&command, ", '_row_id' : %d", ICEBERG_ROWID_FIELD_ID);

					appendStringInfoString(&command, "}");
				}

				if (queryTupleDesc != NULL)
				{
					char	   *geoParquetMeta =
						GetGeoParquetMetadataForTupleDesc(queryTupleDesc);

					if (geoParquetMeta != NULL)
					{
						appendStringInfo(&command, ", kv_metadata { geo: %s }",
										 quote_literal_cstr(geoParquetMeta));
					}
				}

				if (TargetRowGroupSizeMB > 0)
				{
					/*
					 * When writing Parquet files, a single row group per
					 * thread must fit in memory uncompressed. Hence, set
					 * row_group_size_bytes to 128MB.
					 * https://github.com/duckdb/duckdb/issues/16078#issuecomment-2644985411
					 *
					 * duckdb also uses row_group_size which is set to 122880
					 * rows by default. If row_group_size hits the limit
					 * before row_group_size_bytes, it will be used instead.
					 *
					 * row_group_size_bytes also requires
					 * preserve_insertion_order=false.
					 */
					appendStringInfo(&command, ", row_group_size_bytes '%dMB'", TargetRowGroupSizeMB);
				}

				appendStringInfo(&command, ", parquet_version '%s'",
								 ParquetVersionToString(DefaultParquetVersion));

				appendStringInfo(&command, ", return_stats");

				break;
			}

		case DATA_FORMAT_JSON:
			{
				if (destinationCompression == DATA_COMPRESSION_SNAPPY)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("pg_lake_copy: snappy compression is not "
										   "supported for JSON format")));
				}

				const char *compressionName =
					CopyDataCompressionToName(destinationCompression);

				appendStringInfo(&command, ", compression %s",
								 quote_literal_cstr(compressionName));
				break;
			}

		case DATA_FORMAT_CSV:
			{
				if (destinationCompression == DATA_COMPRESSION_SNAPPY)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("pg_lake_copy: snappy compression is not "
										   "supported for CSV format")));
				}

				const char *compressionName =
					CopyDataCompressionToName(destinationCompression);

				appendStringInfo(&command, ", compression %s",
								 quote_literal_cstr(compressionName));

				/*
				 * We normalize the list of options to include default values
				 * for all options, unless auto_detect is on, in which case we
				 * only include the explicitly defined ones.
				 */
				List	   *csvOptions = NormalizedExternalCSVOptions(formatOptions);

				ListCell   *optionCell = NULL;

				foreach(optionCell, csvOptions)
				{
					DefElem    *option = lfirst(optionCell);

					if (strcmp(option->defname, "header") == 0)
					{
						CopyHeaderChoice choice =
							GetCopyHeaderChoice(option, true);

						appendStringInfo(&command, ", header %s",
										 choice == COPY_HEADER_FALSE ? "false" : "true");
					}
					else if (strcmp(option->defname, "delimiter") == 0)
					{
						char	   *delimiter = defGetString(option);

						appendStringInfo(&command, ", delim %s",
										 quote_literal_cstr(delimiter));
					}
					else if (strcmp(option->defname, "quote") == 0)
					{
						char	   *quote = defGetString(option);

						appendStringInfo(&command, ", quote %s",
										 quote_literal_cstr(quote));
					}
					else if (strcmp(option->defname, "escape") == 0)
					{
						char	   *escape = defGetString(option);

						appendStringInfo(&command, ", escape %s",
										 quote_literal_cstr(escape));
					}
					else if (strcmp(option->defname, "null") == 0)
					{
						char	   *null = defGetString(option);

						appendStringInfo(&command, ", nullstr %s",
										 quote_literal_cstr(null));
					}
					else if (strcmp(option->defname, "force_quote") == 0)
					{
						if (option->arg && IsA(option->arg, A_Star))
						{
							appendStringInfoString(&command, ", force_quote *");
						}
						else if (option->arg && IsA(option->arg, List))
						{
							appendStringInfoString(&command, ", force_quote (");

							List	   *columnNameList = castNode(List, option->arg);;
							ListCell   *columnNameCell = NULL;
							int			columnIndex = 0;

							foreach(columnNameCell, columnNameList)
							{
								char	   *columnName = strVal(lfirst(columnNameCell));

								/* add comma after first column */
								appendStringInfo(&command, "%s%s",
												 columnIndex > 0 ? ", " : "",
												 quote_identifier(columnName));

								columnIndex++;
							}

							appendStringInfoString(&command, ")");
						}
					}
				}

				break;
			}

		default:
			elog(ERROR, "unexpected format: %s", formatName);
	}

	/* end WITH options */
	appendStringInfoString(&command, ")");

	return command.data;
}


/*
 * OpenCSVStreamWriter is the streaming counterpart of ConvertCSVFileTo:
 * instead of reading bytes back through a local CSV file, it opens a
 * libpq COPY-IN stream to pgduck_server's RECEIVE handler with the same
 * COPY ... TO destination query baked in (with '@@PG_LAKE_RECV@@' as the
 * read_csv path placeholder), and returns a writer whose ->dest is a CSV
 * DestReceiver feeding bytes into that stream.
 *
 * Caller pattern:
 *
 *   writer = OpenCSVStreamWriter(...);
 *   dest = CSVStreamWriterDestReceiver(writer);
 *   <drive `dest` from a producer query>
 *   dest->rShutdown(dest);
 *   stats = FinishCSVStreamWriter(writer);
 *
 * The writer takes ownership of a PGDuckConnection for its lifetime and
 * releases it in FinishCSVStreamWriter (success or failure).
 */
CSVStreamWriter *
OpenCSVStreamWriter(TupleDesc csvTupleDesc, int maxLineSize,
					char *destinationPath,
					CopyDataFormat destinationFormat,
					CopyDataCompression destinationCompression,
					List *formatOptions,
					DataFileSchema * schema,
					List *leafFields)
{
	/* Build the same query as ConvertCSVFileTo, with the RECEIVE placeholder. */
	char	   *innerSelect = BuildSelectFromCSVQuery(PG_LAKE_RECV_PATH_PLACEHOLDER,
													  csvTupleDesc,
													  maxLineSize,
													  destinationFormat);

	/*
	 * Wrap with COPY (...) TO 'destinationPath' WITH (...). We use
	 * outOfRangePolicy = ICEBERG_OOR_NONE / wrapNativeTypes = false to
	 * mirror ConvertCSVFileTo: the CSV bytes coming from the DestReceiver
	 * are already clamped / native-converted upstream.
	 */
	bool		queryHasRowIds = false;
	char	   *copyCommand = BuildCopyToCommandString(innerSelect, destinationPath,
													   destinationFormat,
													   destinationCompression,
													   formatOptions,
													   queryHasRowIds,
													   schema, csvTupleDesc,
													   ICEBERG_OOR_NONE,
													   false /* wrapNativeTypes */ );

	char	   *receiveQuery = psprintf("RECEIVE %s", copyCommand);

	CSVStreamWriter *writer = palloc0(sizeof(CSVStreamWriter));

	writer->destinationPath = destinationPath;
	writer->destinationFormat = destinationFormat;
	writer->schema = schema;
	writer->leafFields = leafFields;
	writer->conn = GetPGDuckConnection();

	PG_TRY();
	{
		OpenCopyInStreamToPGDuck(writer->conn, receiveQuery);

		/*
		 * The dest receiver uses the same CSV options as ConvertCSVFileTo's
		 * caller: header included so AppendReadCSVClause's auto-detect /
		 * columns map matches the bytes we emit.
		 */
		bool		includeHeader = true;
		List	   *copyOptions = InternalCSVOptions(includeHeader);

		writer->dest = CreateCSVStreamDestReceiver(writer->conn->conn,
												   copyOptions,
												   destinationFormat);
	}
	PG_CATCH();
	{
		ReleasePGDuckConnection(writer->conn);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return writer;
}


/*
 * CSVStreamWriterDestReceiver returns the DestReceiver that the caller
 * should drive with rows. The receiver is owned by the writer; do not
 * call rDestroy on it (FinishCSVStreamWriter handles that).
 */
DestReceiver *
CSVStreamWriterDestReceiver(CSVStreamWriter * writer)
{
	return writer->dest;
}


/*
 * FinishCSVStreamWriter closes the COPY-IN stream, waits for pgduck to
 * complete the deferred COPY ... TO query, and returns the resulting
 * StatsCollector built from its result rows (Parquet/Iceberg) or
 * CommandComplete tag (CSV/JSON), exactly like
 * ExecuteCopyToCommandOnPGDuckConnection does for the file-based path.
 *
 * The caller MUST have called writer->dest->rShutdown(writer->dest)
 * before invoking this so the per-row buffer is flushed and any binary
 * trailer is emitted.
 *
 * The DestReceiver and the underlying PGDuckConnection are released here
 * (in both success and failure paths).
 */
StatsCollector *
FinishCSVStreamWriter(CSVStreamWriter * writer)
{
	StatsCollector *statsCollector = NULL;
	PGresult   *result = NULL;

	PG_TRY();
	{
		result = FinishCopyInStreamToPGDuck(writer->conn);

		if (writer->destinationFormat == DATA_FORMAT_PARQUET ||
			writer->destinationFormat == DATA_FORMAT_ICEBERG)
		{
			/* DuckDB's COPY ... return_stats returns one row per file. */
			statsCollector = GetDataFileStatsListFromPGResult(result,
															  writer->leafFields,
															  writer->schema);
		}
		else
		{
			char	   *commandTuples = PQcmdTuples(result);
			int64		totalRowCount = atoll(commandTuples);

			statsCollector = palloc0(sizeof(StatsCollector));
			statsCollector->totalRowCount = totalRowCount;

			/* no file is created when 0 rows are copied */
			if (totalRowCount > 0)
			{
				DataFileStats *fileStats =
					CreateDataFileStatsForDataFile(writer->destinationPath,
												   totalRowCount, 0,
												   writer->leafFields);

				statsCollector->dataFileStats = list_make1(fileStats);
			}
		}
	}
	PG_FINALLY();
	{
		if (result != NULL)
			PQclear(result);
		if (writer->dest != NULL)
		{
			writer->dest->rDestroy(writer->dest);
			writer->dest = NULL;
		}
		ReleasePGDuckConnection(writer->conn);
	}
	PG_END_TRY();

	return statsCollector;
}


/*
 * TupleDescToProjectionList converts a PostgreSQL tuple descriptor to
 * projection list in string form that can be used for writes.
 */
char *
TupleDescToProjectionListForWrite(TupleDesc tupleDesc, CopyDataFormat destinationFormat)
{
	Assert(tupleDesc != NULL);

	StringInfoData projection;

	initStringInfo(&projection);

	bool		hasColumns = false;

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
			continue;

		char	   *columnName = NameStr(column->attname);
		Oid			columnTypeId = column->atttypid;

		if (hasColumns)
			appendStringInfoString(&projection, ", ");

		/*
		 * TimeTZ is stored as TIME (UTC-normalized) in Iceberg. We convert to
		 * UTC in PGDuckSerialize, so DuckDB should parse as TIME.
		 */
		if (columnTypeId == TIMETZOID && destinationFormat == DATA_FORMAT_ICEBERG)
			appendStringInfo(&projection, "CAST(%s AS TIME) AS ",
							 quote_identifier(columnName));

		/*
		 * In case of geometry, we write WKT in csv_writer.c and parse it as
		 * GEOMETRY via read_csv. Just before writing to the destination, we
		 * convert to a form that makes sense for the destination format,
		 * namely WKB blob in Parquet and GeoJSON in JSON.
		 *
		 * In case of CSV we preserve the WKT as written by csv_writer.c
		 */
		if (IsGeometryTypeId(columnTypeId))
		{
			if (destinationFormat == DATA_FORMAT_PARQUET ||
				destinationFormat == DATA_FORMAT_ICEBERG)
				appendStringInfo(&projection, "ST_AsWKB(%s) AS ",
								 quote_identifier(columnName));

			else if (destinationFormat == DATA_FORMAT_JSON)
				appendStringInfo(&projection, "ST_AsGeoJSON(%s) AS ",
								 quote_identifier(columnName));
		}
		appendStringInfo(&projection, "%s",
						 quote_identifier(columnName));

		hasColumns = true;
	}

	if (!hasColumns)
		/* no columns, fall back to SELECT * */
		return "*";

	return projection.data;
}


/*
 * TupleDescToColumnMapForWrite converts a PostgreSQL tuple descriptor to
 * a DuckDB columns map in string form.
 */
char *
TupleDescToColumnMapForWrite(TupleDesc tupleDesc, CopyDataFormat destinationFormat)
{
	StringInfoData map;

	initStringInfo(&map);

	bool		hasColumns = false;

	appendStringInfoString(&map, "{");

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
			continue;

		char	   *columnName = NameStr(column->attname);
		Oid			columnTypeId = column->atttypid;
		int			columnTypeMod = column->atttypmod;
		DuckDBTypeInfo duckdbType = ChooseDuckDBEngineTypeForWrite(
																   MakePGType(columnTypeId, columnTypeMod),
																   destinationFormat);

		appendStringInfo(&map, "%s%s:%s",
						 hasColumns ? "," : "",
						 quote_literal_cstr(columnName),
						 quote_literal_cstr(duckdbType.typeName));

		hasColumns = true;
	}

	appendStringInfoString(&map, "}");

	return map.data;
}


/*
 * AppendFields appends comma-separated mappings from
 * a field name to a field ID to a DuckDB map in string form.
 */
void
AppendFields(StringInfo map, DataFileSchema * schema)
{
	bool		addComma = false;

	for (size_t fieldIdx = 0; fieldIdx < schema->nfields; fieldIdx++)
	{
		DataFileSchemaField *field = &schema->fields[fieldIdx];
		const char *fieldName = field->name;

		appendStringInfo(map, "%s%s: ",
						 addComma ? ", " : "",
						 quote_literal_cstr(fieldName));

		AppendFieldIdValue(map, field->type, field->id);

		addComma = true;
	}
}


/*
 * AppendFieldIdValue appends a field ID to a DuckDB map in string form.
 * The field ID is either a number or another map containing the field
 * ID for the current field and the subfields.
 *
 * https://duckdb.org/docs/sql/statements/copy
 */
static void
AppendFieldIdValue(StringInfo fieldIdsStr, Field * field, int fieldId)
{
#define CURRENT_FIELD_ID "__duckdb_field_id"

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			appendStringInfo(fieldIdsStr, "%d", fieldId);
			break;

		case FIELD_TYPE_LIST:
			appendStringInfo(fieldIdsStr, "{" CURRENT_FIELD_ID ": %d", fieldId);

			FieldList  *listField = &field->field.list;

			appendStringInfoString(fieldIdsStr, ", element: ");
			AppendFieldIdValue(fieldIdsStr, listField->element, listField->elementId);

			appendStringInfoString(fieldIdsStr, "}");

			break;

		case FIELD_TYPE_MAP:
			appendStringInfo(fieldIdsStr, "{" CURRENT_FIELD_ID ": %d", fieldId);

			FieldMap   *mapField = &field->field.map;

			appendStringInfoString(fieldIdsStr, ", key: ");
			AppendFieldIdValue(fieldIdsStr, mapField->key, mapField->keyId);
			appendStringInfoString(fieldIdsStr, ", value: ");
			AppendFieldIdValue(fieldIdsStr, mapField->value, mapField->valueId);

			appendStringInfoString(fieldIdsStr, "}");

			break;

		case FIELD_TYPE_STRUCT:
			appendStringInfo(fieldIdsStr, "{" CURRENT_FIELD_ID ": %d", fieldId);

			DataFileSchema *structField = &field->field.structType;

			appendStringInfoString(fieldIdsStr, ", ");
			AppendFields(fieldIdsStr, structField);

			appendStringInfoString(fieldIdsStr, "}");
			break;
	}
}


/*
 * ParquetVersionToString converts a ParquetVersion to a string.
 */
static const char *
ParquetVersionToString(ParquetVersion version)
{
	switch (version)
	{
		case PARQUET_VERSION_V1:
			return "V1";

		case PARQUET_VERSION_V2:
			return "V2";

		default:
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("unexpected parquet version: %d", version)));
	}

	return NULL;
}


/*
 * ChooseDuckDBEngineTypeForWrite obtains a DuckDB type name for a given postgres
 * type, and codifies some of our limitations around arrays and decimals.
 *
 * NOTE: This function should stay in sync with ShouldUseDuckSerialization where
 * we decide how to write the values to the intermediate CSV. Here we decide how
 * DuckDB should parse those values. In particular, the format emitted by csv_writer.c
 * should be parseable by read_csv() when using the type decided by this function.
 */
static DuckDBTypeInfo
ChooseDuckDBEngineTypeForWrite(PGType postgresType,
							   CopyDataFormat destinationFormat)
{
	/*
	 * We prefer to treat all fields as text when writing CSV to preserve
	 * PostgreSQL serialization format.
	 */
	if (destinationFormat == DATA_FORMAT_CSV)
		return VARCHAR_TYPE;

	int32		postgresTypeMod = postgresType.postgresTypeMod;
	Oid			elementTypeId = get_element_type(postgresType.postgresTypeOid);
	bool		isArrayType = OidIsValid(elementTypeId);
	char	   *typeModifier = "";

	/*
	 * We can handle an array by treating the element type like the type that
	 * was passed in from here on out an add [] to the type name in the end.
	 */
	if (isArrayType)
		postgresType.postgresTypeOid = elementTypeId;

	DuckDBType	duckTypeId = GetDuckDBTypeForPGType(postgresType);

	if (duckTypeId == DUCKDB_TYPE_INVALID)
	{
		/*
		 * Treat any type that does not have a DuckDB equivalent as text.
		 */
		duckTypeId = DUCKDB_TYPE_VARCHAR;
	}
	else if (duckTypeId == DUCKDB_TYPE_DECIMAL)
	{
		/*
		 * PostgreSQL supports up to 1000 digits in numeric fields, while
		 * DuckDB supports up to 38.
		 *
		 * To make sure we do not break the limit, emit large numeric as text.
		 * Other systems might not understand that as numeric, but PostgreSQL
		 * can still parse it.
		 *
		 * https://duckdb.org/docs/sql/data_types/overview
		 * https://www.postgresql.org/docs/current/datatype-numeric.html#DATATYPE-NUMERIC-DECIMAL
		 */
		int			precision = -1;
		int			scale = -1;

		GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(postgresTypeMod, &precision, &scale);

		if (CanPushdownNumericToDuckdb(precision, scale))
		{
			/*
			 * happy case: we can map to DECIMAL(precision, scale)
			 */
			typeModifier = psprintf("(%d,%d)", precision, scale);
			duckTypeId = DUCKDB_TYPE_DECIMAL;
		}
		else
		{
			/* explicit precision which is too big for us */
			duckTypeId = DUCKDB_TYPE_VARCHAR;
		}
	}
	else if (duckTypeId == DUCKDB_TYPE_TIME_TZ && destinationFormat == DATA_FORMAT_ICEBERG)
	{
		/*
		 * Iceberg only has a "time" type (no timezone). We convert timetz
		 * values to UTC in PGDuckSerialize, so DuckDB should parse as TIME.
		 */
		duckTypeId = DUCKDB_TYPE_TIME;
	}
	else if (duckTypeId == DUCKDB_TYPE_BLOB && destinationFormat == DATA_FORMAT_JSON)
	{
		/*
		 * We map bytea to text in JSON, because DuckDB's bytea text format is
		 * subtly different from PostgreSQL. It needs a separate \x for every
		 * 2 hex characters, otherwise it interprets the characters as ASCII
		 * bytes, so something like \xabab would be interpreted differently
		 * between PG and DuckDB
		 *
		 * This corresponds to ShouldUseDuckSerialization in csv_writer.c
		 */
		duckTypeId = DUCKDB_TYPE_VARCHAR;
		isArrayType = false;
	}
	else if (duckTypeId == DUCKDB_TYPE_INTERVAL && destinationFormat == DATA_FORMAT_ICEBERG)
	{
		/*
		 * Iceberg does not have a native interval type. We store intervals as
		 * struct(months BIGINT, days BIGINT, microseconds BIGINT) in both the
		 * Iceberg metadata and Parquet data files. For plain Parquet files,
		 * DuckDB uses its native INTERVAL type.
		 */
		char	   *intervalTypeName =
			psprintf("STRUCT(months BIGINT, days BIGINT, microseconds BIGINT)%s",
					 isArrayType ? "[]" : "");
		DuckDBTypeInfo typeInfo = {
			.typeId = DUCKDB_TYPE_STRUCT,
			.typeName = intervalTypeName,
			.isArrayType = isArrayType,
		};

		return typeInfo;
	}

	/*
	 * In case of both JSON and Parquet, composites/arrays/maps are serialized
	 * in a DuckDB- compatible format by csv_writer.c and parsed into the
	 * native DuckDB struct/list/map types by read_csv(). When writing to JSON
	 * they become JSON objects/array, and when writing to Parquet they are
	 * converted to native Parquet structures. That behaviour seems desirable
	 * for us as well, so we do not do any special processing other than
	 * emitting the appropriate type name/definition.
	 */

	char	   *typeName;

	if (duckTypeId == DUCKDB_TYPE_STRUCT || duckTypeId == DUCKDB_TYPE_MAP)
	{
		/* generate field names for struct/map */
		const char *structDef =
			GetFullDuckDBTypeNameForPGType(postgresType, destinationFormat);

		typeName = psprintf("%s%s", structDef, isArrayType ? "[]" : "");
	}
	else
		typeName = psprintf("%s%s%s",
							GetDuckDBTypeName(duckTypeId),
							typeModifier,
							isArrayType ? "[]" : "");

	DuckDBTypeInfo typeInfo = {
		.typeId = duckTypeId,
		.typeName = typeName,
		.isArrayType = isArrayType
	};

	return typeInfo;
}
