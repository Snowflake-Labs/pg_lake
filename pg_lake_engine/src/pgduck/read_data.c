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
 * Functions for generating query for reading data from pgduck server.
 */
#include "postgres.h"

#include <inttypes.h>

#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/gdal.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/util/numeric.h"
#include "nodes/parsenodes.h"
#include "nodes/makefuncs.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


static char *ReadDataSourceFunction(List *sourcePaths,
									uint64 sourceRowCount,
									CopyDataFormat sourceFormat,
									CopyDataCompression sourceCompression,
									TupleDesc expectedDesc,
									List *formatOptions,
									DataFileSchema * schema,
									bool preferVarchar,
									ReadRowLocationMode emitRowLocation,
									bool emitRowId);
static char *BuildParquetSchema(DataFileSchema * schema, bool emitRowId);
static char *GetSchemaType(Field * mapping);
static char *ReadEmptyDataSource(TupleDesc tupleDesc, CopyDataFormat format,
								 bool preferVarchar,
								 ReadRowLocationMode emitRowLocation);
static char *TupleDescToDuckDBColumnsMap(TupleDesc tupleDesc,
										 CopyDataFormat sourceFormat,
										 bool preferVarchar,
										 bool skipFilename);
static char *TupleDescToDuckDBColumnsArray(TupleDesc tupleDesc, bool skipFilename);
static char *TupleDescToAliasList(TupleDesc tupleDesc);
static DuckDBTypeInfo ChooseCompatibleDuckDBType(Oid postgresTypeId, int postgresTypeMod,
												 CopyDataFormat sourceFormat,
												 bool preferVarchar);
static DuckDBTypeInfo GuessStorageType(DuckDBTypeInfo engineType,
									   CopyDataFormat sourceFormat);
static char *BuildColumnProjection(char *expression,
								   DuckDBTypeInfo engineType,
								   CopyDataFormat sourceFormat,
								   List *formatOptions,
								   bool addAlias);
static char *PathListToString(List *paths);
static char *GetLogFormatRegex(List *options);
static char *GetLogTimestampFormat(List *options);


static DuckDBTypeInfo VARCHAR_TYPE =
{
	DUCKDB_TYPE_VARCHAR, false, "VARCHAR",
};


/*
 * ReadDataSourceQuery constructs a query to read from an external data
 * source.
 */
char *
ReadDataSourceQuery(List *sourcePaths,
					List *positionDeletePaths,
					CopyDataFormat sourceFormat,
					CopyDataCompression sourceCompression,
					TupleDesc expectedDesc,
					List *formatOptions,
					DataFileSchema * schema,
					ReadDataStats * stats,
					int flags)
{
	/* whether to prepare output for TRANSMIT */
	bool		isTransmit = (flags & READ_DATA_TRANSMIT) != 0;

	/* whether to read all types as varchar */
	bool		preferVarchar = (flags & READ_DATA_PREFER_VARCHAR) != 0;

	/*
	 * Determine whether to read filename and file_row_number and whether to
	 * include them in the query result (projection).
	 */
	ReadRowLocationMode readRowLocationMode = NO_ROW_LOCATION;

	if ((flags & READ_DATA_EMIT_ROW_LOCATION) != 0)
		readRowLocationMode = EMIT_ROW_LOCATION;
	else if ((flags & READ_DATA_READ_ROW_LOCATION) != 0)
		readRowLocationMode = READ_ROW_LOCATION;

	/* whether to emit _row_id from in read_parquet */
	bool		emitRowId = (flags & READ_DATA_EMIT_ROW_ID) != 0;

	/* whether to add explicit casts to the projection list */
	bool		addCast = (flags & READ_DATA_EXPLICIT_CAST) != 0;

	/* check whether we know the source row count */
	uint64		sourceRowCount = stats != NO_STATISTICS ? stats->sourceRowCount : 0;

	/*
	 * always read filename and file_row_number when there are position
	 * deletes.
	 */
	if (positionDeletePaths != NIL && readRowLocationMode == NO_ROW_LOCATION)
		readRowLocationMode = READ_ROW_LOCATION;

	StringInfoData command;

	initStringInfo(&command);

	if (isTransmit)
	{
		appendStringInfoString(&command, "TRANSMIT ");
	}

	appendStringInfoString(&command, "SELECT ");

	/* create an explicit projection if requested (incl. type casting) */
	if (expectedDesc != NULL)
	{
		char	   *projection = TupleDescToProjectionList(expectedDesc,
														   sourceFormat,
														   formatOptions,
														   readRowLocationMode,
														   emitRowId,
														   addCast);

		appendStringInfo(&command, "%s ", projection);
	}
	else
	{
		/* by default select all columns returned by the read function */
		appendStringInfoString(&command, "* ");

		/*
		 * When there are position delete files, we request filename and row
		 * number from read_parquet, but if the caller did not ask for them we
		 * should exclude them from the query result.
		 *
		 * When there is an expectedDesc, this is handled by not including
		 * them in the target list.
		 */
		if (readRowLocationMode == READ_ROW_LOCATION)
			appendStringInfoString(&command, "EXCLUDE(" INTERNAL_FILENAME_COLUMN_NAME ", file_row_number) ");
	}

	char	   *readFunctionCall = ReadDataSourceFunction(sourcePaths,
														  sourceRowCount,
														  sourceFormat,
														  sourceCompression,
														  expectedDesc,
														  formatOptions,
														  schema,
														  preferVarchar,
														  readRowLocationMode,
														  emitRowId);

	appendStringInfo(&command, "FROM %s", readFunctionCall);

	if (expectedDesc != NULL)
	{
		char	   *aliasList = TupleDescToAliasList(expectedDesc);

		appendStringInfo(&command, " res %s", aliasList);
	}

	if (sourcePaths != NIL && positionDeletePaths != NIL)
	{
		/*
		 * Add an anti-join with the position delete files to filter out
		 * deleted rows. The anti-join is reasonably efficient in DuckDB.
		 */

		appendStringInfo(&command,
						 " WHERE (" INTERNAL_FILENAME_COLUMN_NAME ", file_row_number)"
						 " NOT IN (SELECT (file_path, pos) FROM read_parquet(%s",
						 PathListToString(positionDeletePaths));

		if (stats != NO_STATISTICS && stats->positionDeleteRowCount > 0)
		{
			/*
			 * We also add explicit_cardinality to the read_parquet call to
			 * ensure that the query planner has an accurate row count
			 * estimate.
			 */
			appendStringInfo(&command, ", explicit_cardinality=" UINT64_FORMAT, stats->positionDeleteRowCount);
		}

		appendStringInfoString(&command, "))");
	}

	return command.data;
}


/*
 * ReadDataSourceFunction constructs a query to read_csv/parquet/json from an external data
 * source.
 */
static char *
ReadDataSourceFunction(List *sourcePaths,
					   uint64 sourceRowCount,
					   CopyDataFormat sourceFormat,
					   CopyDataCompression sourceCompression,
					   TupleDesc expectedDesc,
					   List *formatOptions,
					   DataFileSchema * schema,
					   bool preferVarchar,
					   ReadRowLocationMode readRowLocationMode,
					   bool emitRowId)
{
	bool		emitFilename = GetBoolOption(formatOptions, "filename", false);

	if (list_length(sourcePaths) == 0)
	{
		return ReadEmptyDataSource(expectedDesc, sourceFormat, preferVarchar,
								   readRowLocationMode);
	}

	StringInfoData command;

	initStringInfo(&command);

	switch (sourceFormat)
	{
		case DATA_FORMAT_PARQUET:
		case DATA_FORMAT_ICEBERG:
			{
				appendStringInfo(&command, "read_parquet(%s",
								 PathListToString(sourcePaths));

				if (schema && schema->nfields > 0)
				{
					char	   *schemaOptions = BuildParquetSchema(schema, emitRowId);

					if (schemaOptions != NULL)
					{
						appendStringInfo(&command, ", %s", schemaOptions);
					}
				}

				if (sourceRowCount > 0)
					appendStringInfo(&command, ", explicit_cardinality=" UINT64_FORMAT, sourceRowCount);

				if (readRowLocationMode != NO_ROW_LOCATION)
					appendStringInfoString(&command,
										   ", filename='" INTERNAL_FILENAME_COLUMN_NAME "', file_row_number=true");
				else if (emitFilename)
					appendStringInfoString(&command,
										   ", filename='_filename'");

				appendStringInfoString(&command, ")");

				break;
			}

		case DATA_FORMAT_DELTA:
			{
				if (list_length(sourcePaths) != 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("multiple paths are not supported for Delta")));
				}

				appendStringInfo(&command, "delta_scan(%s",
								 PathListToString(sourcePaths));

				if (readRowLocationMode != NO_ROW_LOCATION || emitFilename)
					elog(ERROR, "querying delta tables with filename is currently unsupported");

				appendStringInfoString(&command, ")");

				break;
			}

		case DATA_FORMAT_JSON:
			{
				if (sourceCompression == DATA_COMPRESSION_SNAPPY)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("pg_lake_copy: snappy compression is "
										   "not supported for JSON format")));
				}

				appendStringInfo(&command,
								 "read_json_auto(%s",
								 PathListToString(sourcePaths));

				/* read_json does not react well to compression='none' */
				if (sourceCompression != DATA_COMPRESSION_NONE)
				{
					const char *compressionName =
						CopyDataCompressionToName(sourceCompression);

					appendStringInfo(&command,
									 ", compression=%s",
									 quote_literal_cstr(compressionName));
				}

				if (expectedDesc != NULL && expectedDesc->natts > 0)
				{
					/*
					 * if read_json_auto emits a filename, skip it in the
					 * columns map
					 */
					bool		skipFilename = emitFilename;

					/*
					 * We prefer DuckDB to interpret all fields in JSON as
					 * text, such that cast failures (e.g. trying to write
					 * text into an int column) happen in PostgreSQL when
					 * parsing the intermediate CSV.
					 *
					 * We should revise this if we stop using CSV as an
					 * intermediate format.
					 */
					char	   *columnsMap = TupleDescToDuckDBColumnsMap(expectedDesc,
																		 sourceFormat,
																		 preferVarchar,
																		 skipFilename);

					appendStringInfo(&command, ", columns=%s", columnsMap);

				}

				if (emitFilename)
					appendStringInfoString(&command,
										   ", filename='_filename'");

				/* add other options as needed */
				DefElem    *maximumObjectSizeOption = GetOption(formatOptions, "maximum_object_size");

				if (maximumObjectSizeOption != NULL)
				{
					int64		maximumObjectSize = defGetInt64(maximumObjectSizeOption);

					if (maximumObjectSize < 0)
						ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
										errmsg("option value \"maximum_object_size\" must be non-negative")));

					appendStringInfo(&command,
									 ", maximum_object_size=" INT64_FORMAT,
									 maximumObjectSize);
				}

				appendStringInfo(&command, ")");
				break;
			}

		case DATA_FORMAT_CSV:
			{
				/*
				 * Defaults to false if auto_detect is not present in
				 * formatOptions
				 */
				bool		autoDetect = HasAutoDetect(formatOptions);

				if (sourceCompression == DATA_COMPRESSION_SNAPPY)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("pg_lake_copy: snappy compression is "
										   "not supported for CSV format")));
				}

				appendStringInfo(&command,
								 "read_csv(%s",
								 PathListToString(sourcePaths));

				/* read_csv does not react well to compression='none' */
				if (sourceCompression != DATA_COMPRESSION_NONE)
				{
					const char *compressionName =
						CopyDataCompressionToName(sourceCompression);

					appendStringInfo(&command,
									 ", compression=%s",
									 quote_literal_cstr(compressionName));
				}

				/*
				 * Specify columns, which is required when auto_detect is
				 * false.
				 */
				if (expectedDesc != NULL && expectedDesc->natts > 0)
				{
					/*
					 * DuckDB 0.10.1+ skips automatic header detection if we
					 * specify both columns and auto_detect, so we omit
					 * columns when auto_detect is enabled.
					 */
					if (!autoDetect)
					{
						/*
						 * if read_csv emits a filename, skip it in the
						 * columns map
						 */
						bool		skipFilename = emitFilename;

						char	   *columnsMap = TupleDescToDuckDBColumnsMap(expectedDesc,
																			 sourceFormat,
																			 preferVarchar,
																			 skipFilename);

						appendStringInfo(&command, ", columns=%s", columnsMap);
					}
				}
				else
				{
					/*
					 * when there are 0 columns, we need to at specify
					 * auto-detect
					 */
					autoDetect = true;
				}

				/* propagate user-defined auto_detect value (false by default) */
				appendStringInfo(&command,
								 ", auto_detect=%s",
								 autoDetect ? "true" : "false");

				/*
				 * We normalize the list of options to include default values
				 * for all options, unless auto_detect is on, in which case we
				 * only include the explicitly defined ones.
				 */
				List	   *csvOptions = NormalizedExternalCSVOptions(formatOptions);

				appendStringInfoString(&command, CopyOptionsToReadCSVParams(csvOptions));

				if (emitFilename)
					appendStringInfoString(&command,
										   ", filename='_filename'");

				/* close read_csv( */
				appendStringInfo(&command, ")");
				break;
			}

		case DATA_FORMAT_GDAL:
			{
				if (list_length(sourcePaths) != 1)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("multiple paths are not supported for GDAL")));
				}

				char	   *path = (char *) linitial(sourcePaths);
				char	   *stReadCall =
					GDALReadFunctionCall(path, sourceCompression, formatOptions);

				appendStringInfoString(&command, stReadCall);
				break;
			}

		case DATA_FORMAT_LOG:
			{
				char	   *regex = GetLogFormatRegex(formatOptions);

				if (sourceCompression == DATA_COMPRESSION_SNAPPY)
				{
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
									errmsg("pg_lake_copy: snappy compression is "
										   "not supported for log format")));
				}

				/* open log_struct expand query  */
				appendStringInfoString(&command,
									   "(SELECT log_struct.*");

				if (emitFilename)
					appendStringInfoString(&command,
										   ", _filename");

				/*
				 * When we have the filename option, we skip it in
				 * TupleDescToDuckDBColumnsArray since it does not appear in
				 * the log file.
				 */
				bool		skipFilename = emitFilename;

				/* open regex query */
				appendStringInfo(&command,
								 " FROM (SELECT regexp_extract(log_line, %s, %s) AS log_struct",
								 quote_literal_cstr(regex),
								 TupleDescToDuckDBColumnsArray(expectedDesc, skipFilename));

				if (emitFilename)
					appendStringInfoString(&command,
										   ", _filename");

				/* open read_csv( */
				appendStringInfo(&command,
								 " FROM read_csv(%s, header=False",
								 PathListToString(sourcePaths));

				const char *compressionName =
					CopyDataCompressionToName(sourceCompression);

				appendStringInfo(&command,
								 ", compression=%s",
								 quote_literal_cstr(compressionName));

				appendStringInfo(&command, ", columns={'log_line':'TEXT'}");
				appendStringInfo(&command, ", delim=E'\\n'");

				if (emitFilename)
					appendStringInfoString(&command,
										   ", filename='_filename'");
				/* close read_csv( */
				appendStringInfo(&command, ")");

				/* close regex query */
				appendStringInfo(&command, ")");

				/* close log_struct expand query */
				appendStringInfo(&command, ")");
				break;
			}



		default:
			elog(ERROR, "unexpected format: %s", CopyDataFormatToName(sourceFormat));
	}

	return command.data;
}


/*
* BuildParquetSchema builds a parquet schema from a schema
* field. The return value is passed to
* read_parquet(.. schema=...) to read the parquet file.
* This option is an undocumented feature of the read_parquet()
* function, but it is necessary to read parquet files with
* different schemas.
*
* This is especially important for supporting schema
* evolution for iceberg tables. Different parquet files
* may have different schemas, and we need to be able to
* read them all. The
*/
static char *
BuildParquetSchema(DataFileSchema * schema, bool emitRowId)
{
	StringInfoData schemaString;

	initStringInfo(&schemaString);

	appendStringInfoString(&schemaString, "schema=map {");

	bool		addComma = false;

	for (size_t fieldIdx = 0; fieldIdx < schema->nfields; fieldIdx++)
	{
		DataFileSchemaField *field = &schema->fields[fieldIdx];

		if (addComma)
			appendStringInfoString(&schemaString, ", ");

		const char *defaultValue = "NULL";

		if (field->initialDefault != NULL)
		{
			defaultValue = quote_literal_cstr(field->duckSerializedInitialDefault);
		}

		/*
		 * add this to the schema 0: {name: 'renamed_i', type: 'BIGINT',
		 * default_value: '<default>'}
		 */
		appendStringInfo(&schemaString, "%d: {name: %s, type: %s, default_value: %s}",
						 field->id,
						 quote_literal_cstr(field->name),
						 quote_literal_cstr(GetSchemaType(field->type)),
						 defaultValue);

		addComma = true;
	}

	if (emitRowId)
		appendStringInfo(&schemaString, "%s %d: {name: '_row_id', type: 'BIGINT', default_value: NULL}",
						 addComma ? ", " : "",
						 ICEBERG_ROWID_FIELD_ID);

	appendStringInfoString(&schemaString, "}");

	return schemaString.data;
}


/*
* GetSchemaType returns the schema type for a given field.
*/
static char *
GetSchemaType(Field * field)
{
	StringInfoData str;

	initStringInfo(&str);

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			/* For scalar types, simply append the type name */
			appendStringInfoString(&str, field->field.scalar.typeName);
			break;

		case FIELD_TYPE_LIST:
			{
				/* Recursively get the element type and append '[]' */
				char	   *elemType = GetSchemaType(field->field.list.element);

				appendStringInfo(&str, "%s[]", elemType);
				break;
			}

		case FIELD_TYPE_MAP:
			{
				/* Recursively get the key and value types */
				char	   *keyType = GetSchemaType(field->field.map.key);
				char	   *valueType = GetSchemaType(field->field.map.value);

				appendStringInfo(&str, "MAP(%s,%s)", keyType, valueType);
				break;
			}

		case FIELD_TYPE_STRUCT:
			{
				appendStringInfoString(&str, "STRUCT(");
				bool		first = true;

				/* Iterate over the list of fields in the struct */
				size_t		totalFields = field->field.structType.nfields;

				for (size_t fieldIdx = 0; fieldIdx < totalFields; fieldIdx++)
				{
					FieldStructElement *structElementField = &field->field.structType.fields[fieldIdx];

					if (!first)
						appendStringInfoString(&str, ", ");
					else
						first = false;

					/* Recursively get the field type */
					char	   *fieldType = GetSchemaType(structElementField->type);

					appendStringInfo(&str, "%s %s", quote_identifier(structElementField->name), fieldType);
				}
				appendStringInfoChar(&str, ')');
				break;
			}

		default:
			/* Handle unknown kinds */
			ereport(ERROR, (errmsg("Unknown FieldType: %d", field->type)));
	}

	/* Return the constructed type string */
	return str.data;
}



/*
 * ReadEmptyDataSource generates a query that returns 0 results, but with
 * the types specified in tupleDesc.
 */
static char *
ReadEmptyDataSource(TupleDesc tupleDesc, CopyDataFormat sourceFormat, bool preferVarchar,
					ReadRowLocationMode readRowLocationMode)
{
	StringInfoData query;

	initStringInfo(&query);
	appendStringInfoString(&query, "(SELECT ");

	bool		addComma = false;

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
		{
			continue;
		}

		char	   *columnName = NameStr(column->attname);
		Oid			columnTypeId = column->atttypid;
		int			columnTypeMod = column->atttypmod;

		DuckDBTypeInfo duckdbTypeInfo = ChooseCompatibleDuckDBType(columnTypeId,
																   columnTypeMod,
																   sourceFormat,
																   preferVarchar);

		appendStringInfo(&query, "%s NULL::%s AS %s",
						 addComma ? "," : "",
						 duckdbTypeInfo.typeName,
						 quote_identifier(columnName));

		addComma = true;
	}

	if (readRowLocationMode != NO_ROW_LOCATION)
		appendStringInfo(&query,
						 ", NULL::bigint AS file_row_number, NULL::text AS " INTERNAL_FILENAME_COLUMN_NAME);

	appendStringInfoString(&query, " WHERE false)");

	return query.data;
}


/*
 * TupleDescToColumnMap converts a PostgreSQL tuple descriptor to
 * a DuckDB columns map in string form.
 */
static char *
TupleDescToDuckDBColumnsMap(TupleDesc tupleDesc, CopyDataFormat sourceFormat, bool preferVarchar,
							bool skipFilename)
{
	StringInfoData map;

	initStringInfo(&map);

	bool		addComma = false;

	appendStringInfoString(&map, "{");

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
		{
			continue;
		}

		char	   *columnName = NameStr(column->attname);

		if (skipFilename && strcmp(columnName, "_filename") == 0)
		{
			/*
			 * We exit the loop because the only columns we expect after
			 * _filename are hive partitioning columns and those should not
			 * appears in the columns map which is meant for parsing.
			 */
			break;
		}

		Oid			columnTypeId = column->atttypid;
		int			columnTypeMod = column->atttypmod;
		DuckDBTypeInfo duckdbType = ChooseCompatibleDuckDBType(columnTypeId,
															   columnTypeMod,
															   sourceFormat,
															   preferVarchar);
		DuckDBTypeInfo storageType = GuessStorageType(duckdbType, sourceFormat);

		char	   *readTypeName = duckdbType.typeName;

		if (storageType.typeId != duckdbType.typeId)
		{
			/*
			 * read using the storage type name, we'll cast it in the
			 * projection
			 */
			readTypeName = storageType.typeName;
		}

		/* use the storage type name to read CSV/JSON */
		appendStringInfo(&map, "%s%s:%s",
						 addComma ? "," : "",
						 quote_literal_cstr(columnName),
						 quote_literal_cstr(readTypeName));

		addComma = true;
	}

	appendStringInfoString(&map, "}");

	return map.data;
}


/*
 * TupleDescToDuckDBColumnsArray converts a PostgreSQL tuple descriptor to
 * a DuckDB column names array.
 */
static char *
TupleDescToDuckDBColumnsArray(TupleDesc tupleDesc, bool skipFilename)
{
	StringInfoData array;

	initStringInfo(&array);

	bool		addComma = false;

	appendStringInfoString(&array, "[");

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
			continue;

		char	   *columnName = NameStr(column->attname);

		if (skipFilename && strcmp(columnName, "_filename") == 0)
			continue;

		appendStringInfo(&array, "%s%s",
						 addComma ? "," : "",
						 quote_literal_cstr(columnName));

		addComma = true;
	}

	appendStringInfoString(&array, "]");

	return array.data;
}


/*
 * TupleDescToAliasList converts a PostgreSQL tuple descriptor to
 * an alias list in string form.
 */
static char *
TupleDescToAliasList(TupleDesc tupleDesc)
{
	StringInfoData alias;

	initStringInfo(&alias);

	bool		hasColumns = false;

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
			continue;

		char	   *columnName = NameStr(column->attname);

		appendStringInfo(&alias, "%s%s",
						 hasColumns ? "," : "(",
						 quote_identifier(columnName));

		hasColumns = true;
	}

	if (hasColumns)
		appendStringInfoString(&alias, ")");

	return alias.data;
}


/*
 * TupleDescToProjectionList converts a PostgreSQL tuple descriptor to
 * projection list in string form.
 *
 * We add explicit casts for types that do not have an equivalent in
 * the source format.
 */
char *
TupleDescToProjectionList(TupleDesc tupleDesc, CopyDataFormat sourceFormat,
						  List *formatOptions,
						  ReadRowLocationMode readRowLocationMode,
						  bool emitRowId, bool addCast)
{
	StringInfoData projection;

	initStringInfo(&projection);

	bool		hasColumns = false;

	for (int attnum = 1; attnum <= tupleDesc->natts; attnum++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, attnum - 1);

		if (column->attisdropped)
			continue;

		/* we never want to read as varchar in the SELECT clause */
		bool		preferVarchar = false;

		char	   *columnName = NameStr(column->attname);
		Oid			columnTypeId = column->atttypid;
		int			columnTypeMod = column->atttypmod;
		DuckDBTypeInfo duckdbType = ChooseCompatibleDuckDBType(columnTypeId,
															   columnTypeMod,
															   sourceFormat,
															   preferVarchar);

		/*
		 * We probably want to add an alias, but only if we are not going to
		 * add our own later.
		 */
		char	   *columnProjection = BuildColumnProjection(columnName,
															 duckdbType,
															 sourceFormat,
															 formatOptions,
															 addCast);

		if (hasColumns)
			appendStringInfoString(&projection, ", ");

		appendStringInfo(&projection, "%s", columnProjection);

		hasColumns = true;
	}

	if (emitRowId)
	{
		appendStringInfo(&projection, "%s_row_id",
						 hasColumns ? ", " : "");
		hasColumns = true;
	}

	if (readRowLocationMode == EMIT_ROW_LOCATION)
	{
		appendStringInfo(&projection, "%s file_row_number, " INTERNAL_FILENAME_COLUMN_NAME,
						 hasColumns ? "," : "");
		hasColumns = true;
	}

	if (!hasColumns)
		/* no columns, fall back to SELECT * */
		return "*";

	return projection.data;
}


/*
 * ChooseCompatibleDuckDBType obtains a DuckDB type name for a given postgres
 * type, and codifies some of our limitations around arrays and decimals.
 *
 * Some postgres types map into a DuckDB type that is not directly available
 * in the storage format. In those cases we set the storageTypeName to the
 * type that can be read from the source, and return the desired DuckDB type.
 */
static DuckDBTypeInfo
ChooseCompatibleDuckDBType(Oid postgresTypeId, int postgresTypeMod,
						   CopyDataFormat sourceFormat, bool preferVarchar)
{
	Oid			elementTypeId = get_element_type(postgresTypeId);
	bool		isArrayType = OidIsValid(elementTypeId);

	char	   *typeModifier = "";

	/*
	 * We can handle an arrayby treating the element type like the type that
	 * was passed in from here on out an add [] to the type name in the end.
	 */
	if (isArrayType)
		postgresTypeId = elementTypeId;

	DuckDBType	duckTypeId = GetDuckDBTypeForPGType(MakePGType(postgresTypeId, postgresTypeMod));

	if (duckTypeId == DUCKDB_TYPE_INVALID)
	{
		/* treat unmappable types as text, maybe PostgreSQL can parse them */
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
		}
		else
		{
			/* explicit precision which is too big for us */
			duckTypeId = DUCKDB_TYPE_VARCHAR;
		}
	}
	else if (duckTypeId == DUCKDB_TYPE_BLOB && sourceFormat != DATA_FORMAT_PARQUET)
	{
		/*
		 * We currently treat bytea as text in JSON/CSV, because DuckDB's
		 * bytea text format is subtly different from PostgreSQL's text
		 * format. In particular, It needs a separate \x for every 2 hex
		 * characters, otherwise it interprets the characters as ASCII bytes,
		 * so something like \xabab would be interpreted differently between
		 * PG and DuckDB.
		 *
		 * For Parquet we can handle bytea directly, since there is no parsing
		 * we need to do.
		 *
		 * We should find a way to remove this, because it can cause issues
		 * when pushing down operations on bytea: Old repo: issues/337
		 */
		duckTypeId = DUCKDB_TYPE_VARCHAR;
	}
	else if (duckTypeId == DUCKDB_TYPE_STRUCT || duckTypeId == DUCKDB_TYPE_MAP || isArrayType)
	{
		if (sourceFormat == DATA_FORMAT_CSV)
		{
			/*
			 * In CSV, all values are encoded as text. It is theoretically
			 * possible to convert text into a nested type, but we would
			 * prefer to only support the PostgreSQL serialization format and
			 * we cannot currently parse that from DuckDB. We therefore tread
			 * nested types as pure text. If they are returned from DuckDB to
			 * PostgreSQL, then PostgreSQL can still parse them.
			 *
			 * We should find a way to remove this, because it can cause
			 * issues when pushing down operations on struct/map/arrays: Old
			 * repo: issues/337
			 */
			duckTypeId = DUCKDB_TYPE_VARCHAR;
		}
		else if (sourceFormat == DATA_FORMAT_JSON)
		{
			/*
			 * In JSON, nested types are encoded as JSON. We therefore prefer
			 * to always treat nested types as nested types.
			 */
			preferVarchar = false;
		}
		else
		{
			/* Parquet can handle STRUCT/MAP directly */
		}
	}

	/*
	 * Caller asked for VARCHAR only (presumably does not do any pushdown),
	 * and we did not find a reason to disagree.
	 */
	if (preferVarchar)
		return VARCHAR_TYPE;

	char	   *typeName;

	if (duckTypeId == DUCKDB_TYPE_STRUCT || duckTypeId == DUCKDB_TYPE_MAP)
		typeName = psprintf("%s%s",
							GetFullDuckDBTypeNameForPGType(MakePGType(postgresTypeId, postgresTypeMod)),
							isArrayType ? "[]" : "");
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


/*
 * GuessStorageType makes an informed guess on what we expect the storage type
 * to be for a given engine type in the source format.
 *
 * Ideally, we'd know exactly how a field is stored in the data source,
 * such that we can introduce the appropriate cast.
 */
static DuckDBTypeInfo
GuessStorageType(DuckDBTypeInfo engineType, CopyDataFormat sourceFormat)
{
	DuckDBTypeInfo storageType = engineType;

	if (engineType.typeId == DUCKDB_TYPE_GEOMETRY)
	{
		if (sourceFormat == DATA_FORMAT_PARQUET ||
			sourceFormat == DATA_FORMAT_ICEBERG)
		{
			/*
			 * Geometry is stored as a WKB blob in Parquet, we ask for it as
			 * BLOB such that we can call ST_GeomFromWKB
			 */
			storageType.typeId = DUCKDB_TYPE_BLOB;
			storageType.typeName = engineType.isArrayType ? "BLOB[]" : "BLOB";
		}
		else if (sourceFormat == DATA_FORMAT_JSON ||
				 sourceFormat == DATA_FORMAT_CSV)
		{
			/*
			 * Geometry is stored as a GeoJSON in JSON, we ask for it as
			 * VARCHAR such that we can call ST_GeomFromGeoJSON.
			 *
			 * Geometry is stored as WKT in CSV, we ask for it as VARCHAR such
			 * that we can call ST_GeomFromText
			 */
			storageType.typeId = DUCKDB_TYPE_VARCHAR;
			storageType.typeName = engineType.isArrayType ? "VARCHAR[]" : "VARCHAR";
		}
	}

	return storageType;
}


/*
 * BuildColumnProjection creates a string to retrieve the given column from a
 * read function like read_parquet. For certain types, we may apply an extra
 * conversion to cast to the engine type.
 *
 * If addCast is set to false, we append the name of the target column after any
 * interpolated function call.  This is required because if caller is doing to
 * do its own casting or alias then we end up with invalid/weird expressions
 * here, such as:
 *
 * ST_AsWKB(geom :: BLOB) AS geom :: geometry AS geom
 *
 * Since only the caller knows whether it will be doing that, it will tell us
 * if we need to add our own column aliasing in this case.
 */
static char *
BuildColumnProjection(char *columnName,
					  DuckDBTypeInfo engineType,
					  CopyDataFormat sourceFormat,
					  List *formatOptions,
					  bool addCast)
{
	char	   *columnAliasString = !addCast ? psprintf(" AS %s", quote_identifier(columnName)) : "";

	if (engineType.typeId == DUCKDB_TYPE_GEOMETRY && !engineType.isArrayType)
	{
		/*
		 * Geometry requires special casts using spatial functions
		 */
		if (sourceFormat == DATA_FORMAT_PARQUET || sourceFormat == DATA_FORMAT_ICEBERG)
			/* assume geometry in Parquet is stored as WKB blob */
			return psprintf("ST_GeomFromWKB(%s::blob)%s",
							quote_identifier(columnName),
							columnAliasString);

		if (sourceFormat == DATA_FORMAT_CSV)
			/* assume geometry in JSON is stored as WKT */
			return psprintf("ST_GeomFromText(%s)%s", quote_identifier(columnName),
							columnAliasString);

		if (sourceFormat == DATA_FORMAT_JSON)
			/* assume geometry in JSON is stored as GeoJSON */
			return psprintf("ST_GeomFromGeoJSON(%s)%s", quote_identifier(columnName),
							columnAliasString);
	}

	if (sourceFormat == DATA_FORMAT_LOG)
	{
		if (engineType.typeId == DUCKDB_TYPE_TIMESTAMP_TZ ||
			engineType.typeId == DUCKDB_TYPE_TIMESTAMP)
		{
			char	   *timestampFormat = GetLogTimestampFormat(formatOptions);

			if (timestampFormat != NULL)
				return psprintf("strptime(%s, %s)%s",
								quote_identifier(columnName),
								quote_literal_cstr(timestampFormat),
								columnAliasString);
		}

		if (engineType.typeId != DUCKDB_TYPE_VARCHAR)
			return psprintf("try_cast(%s AS %s)%s",
							quote_identifier(columnName),
							engineType.typeName,
							columnAliasString);
	}

	if (addCast)
	{
		/*
		 * Sometimes we want to enforce (coerce or error) types in DuckDB. In
		 * particular, during a COPY FROM that gets pushed down, we need to
		 * make sure we produce Parquet files with the correct types, even if
		 * the source file does not 100% match.
		 */
		return psprintf("%s::%s AS %s",
						quote_identifier(columnName),
						engineType.typeName,
						quote_identifier(columnName));

	}
	else
	{
		/* no cast needed */
		return psprintf("%s", quote_identifier(columnName));
	}
}


/*
 * PathListToString converts a list of paths to a single string that can
 * be used in a pgduck SQL query.
 */
static char *
PathListToString(List *paths)
{
	if (list_length(paths) == 1)
		return quote_literal_cstr(linitial(paths));

	StringInfoData result;

	initStringInfo(&result);

	appendStringInfoChar(&result, '[');

	bool		addComma = false;

	ListCell   *pathCell = NULL;

	foreach(pathCell, paths)
	{
		char	   *path = lfirst(pathCell);

		appendStringInfo(&result, "%s %s",
						 addComma ? "," : "",
						 quote_literal_cstr(path));

		addComma = true;
	}

	appendStringInfoChar(&result, ']');

	return result.data;
}


/*
 * CopyOptionsToReadCSVParams converts a list of COPY options to read_csv
 * arguments to be used in a query string.
 */
char *
CopyOptionsToReadCSVParams(List *copyOptions)
{
	StringInfoData command;

	initStringInfo(&command);

	ListCell   *optionCell = NULL;

	foreach(optionCell, copyOptions)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, "header") == 0)
		{
			CopyHeaderChoice choice =
				GetCopyHeaderChoice(option, true);

			appendStringInfo(&command, ", header=%s",
							 choice == COPY_HEADER_FALSE ? "false" : "true");
		}
		else if (strcmp(option->defname, "delimiter") == 0)
		{
			char	   *delimiter = defGetString(option);

			appendStringInfo(&command, ", delim=%s",
							 quote_literal_cstr(delimiter));
		}
		else if (strcmp(option->defname, "quote") == 0)
		{
			char	   *quote = defGetString(option);

			appendStringInfo(&command, ", quote=%s",
							 quote_literal_cstr(quote));
		}
		else if (strcmp(option->defname, "escape") == 0)
		{
			char	   *escape = defGetString(option);

			appendStringInfo(&command, ", escape=%s",
							 quote_literal_cstr(escape));
		}
		else if (strcmp(option->defname, "null") == 0)
		{
			char	   *null = defGetString(option);

			appendStringInfo(&command, ", nullstr=%s",
							 quote_literal_cstr(null));
		}
		else if (strcmp(option->defname, "new_line") == 0)
		{
			char	   *new_line = defGetString(option);

			appendStringInfo(&command, ", new_line=%s",
							 quote_literal_cstr(new_line));
		}
		else if (strcmp(option->defname, "skip") == 0)
		{
			int64		skip = defGetInt64(option);

			appendStringInfo(&command, ", skip=" INT64_FORMAT, skip);
		}
		else if (strcmp(option->defname, "null_padding") == 0)
		{
			bool		null_padding = defGetBoolean(option);

			appendStringInfo(&command, ", null_padding=%s",
							 null_padding ? "true" : "false");
		}
	}

	return command.data;
}


/*
 * GetLogFormatRegex returns the regular expression to use for the given
 * log format.
 */
static char *
GetLogFormatRegex(List *options)
{
	DefElem    *logFormatOption = GetOption(options, "log_format");

	if (logFormatOption == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("no log_format option specified in log table")));

	char	   *logFormat = defGetString(logFormatOption);

	if (strcmp(logFormat, "s3") == 0)
	{
		return					/* bucket_owner */
			"^([^ ]+) "
		/* bucket_name */
			"([^ ]+) "
		/* timestamp */
			"\\[([0-9/A-Za-z: +]+)\\] "
		/* remote_ip */
			"([^ ]+) "
		/* requester */
			"([^ ]+) "
		/* request_id */
			"([^ ]+) "
		/* operation */
			"([^ ]+) "
		/* s3_key */
			"([^ ]+) "
		/* request_uri */
			"\"?([^\"]*|-)\"? "
		/* http_status */
			"([^ ]+) "
		/* s3_errorcode */
			"([^ ]+) "
		/* bytes_sent */
			"(\\d+|-) "
		/* object_size */
			"(\\d+|-) "
		/* total_time */
			"(\\d+|-) "
		/* turn_around_time */
			"(\\d+|-) "
		/* referer */
			"\"?([^\"]*|-)\"? "
		/* user_agent */
			"\"?([^\"]*|-)\"? "
		/* version_id */
			"([^ ]+) "
		/* host_id */
			"([^ ]+) "
		/* sigver */
			"([^ ]+) "
		/* ciphersuite */
			"([^ ]+) "
		/* auth_type */
			"([^ ]+) "
		/* host_header */
			"([^ ]+) "
		/* tls_version */
			"([^ ]+) "
		/* access_point_arn */
			"([^ ]+) "
		/* acl_required */
			"([^ ]+)"
		/* extra */
			"(.*)$";
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("unrecognized log_format value: %s", logFormat)));
		return NULL;
	}
}


/*
 * GetLogTimestampFormat returns the format to use for timestamps.
 */
static char *
GetLogTimestampFormat(List *options)
{
	DefElem    *logFormatOption = GetOption(options, "log_format");

	if (logFormatOption == NULL)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("no log_format option specified in log table")));

	char	   *logFormat = defGetString(logFormatOption);

	if (strcmp(logFormat, "s3") == 0)
		return "%d/%b/%Y:%H:%M:%S %z";
	else
		return NULL;
}
