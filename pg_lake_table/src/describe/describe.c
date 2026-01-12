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
#include "libpq-fe.h"

#include "commands/defrem.h"
#include "common/string.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/describe/describe.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/gdal.h"
#include "pg_lake/pgduck/geometry.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/sniff_csv.h"
#include "pg_lake/pgduck/type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/lsyscache.h"


static char *DescribeColumnsQueryForURL(char *url,
										CopyDataFormat format,
										CopyDataCompression compression,
										List *copyOptions);
static bool IsStructOrIsArray(const char *name);
static void ResolveGeoParquetColumns(char *url, CopyDataFormat format, List *blobColumns);
static bool HasAllSniffCSVOptions(List *options);
static List *DescribeColumnsForLogFormat(List *options);


/*
 * DescribeColumnsForURL retrieves the columns for a given URL by doing
 * a DESCRIBE operation (this can take some time).
 */
List *
DescribeColumnsForURL(char *url,
					  CopyDataFormat format,
					  CopyDataCompression compression,
					  List *copyOptions)
{
	/*
	 * we describe iceberg tables from the current schema instead of duckdb's
	 * read_parquet function since there might be no datafiles in the table
	 * and the datafile might not be generated from the current schema.
	 */
	if (format == DATA_FORMAT_ICEBERG)
	{
		bool		emitFilename = GetBoolOption(copyOptions, "filename", false);

		return DescribeColumnsFromIcebergMetadataURI(url, emitFilename);
	}

	/*
	 * Log format tables use a pre-defined set of columns.
	 */
	else if (format == DATA_FORMAT_LOG)
	{
		return DescribeColumnsForLogFormat(copyOptions);
	}

	List	   *volatile columns = NIL;
	List	   *volatile blobColumns = NIL;

	char	   *describeQuery = DescribeColumnsQueryForURL(url, format, compression,
														   copyOptions);

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, describeQuery);

	CheckPGDuckResult(pgDuckConn, result);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		int			columnCount = PQntuples(result);

		for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{
			char	   *columnName = PQgetvalue(result, columnIndex, 0);
			char	   *columnType = PQgetvalue(result, columnIndex, 1);

			int			typeMod = -1;
			Oid			typeId = InvalidOid;

			/*
			 * In the context of JSON format, it is unnecessary to generate a
			 * composite type for STRUCT or Array types. We could just use
			 * JSONB.
			 */
			if (format == DATA_FORMAT_JSON && IsStructOrIsArray(columnType))
				typeId = JSONBOID;
			else
				typeId = GetOrCreatePGTypeForDuckDBTypeName(columnType, &typeMod);

			if (typeId == InvalidOid)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("pg_lake_copy: type %s is currently not supported",
									   columnType)));
			}

			columnName = MakeSimpleColumnName(columnName, format);

			Oid			collationId = InvalidOid;
			ColumnDef  *column = makeColumnDef(columnName, typeId, typeMod, collationId);

			columns = lappend(columns, column);

			/*
			 * We store a reference here to the same column to avoid needing
			 * to iterate all columns in our ResolveGeoParquetColumns() check.
			 */

			if (typeId == BYTEAOID)
				blobColumns = lappend(blobColumns, column);
		}

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleasePGDuckConnection(pgDuckConn);

	if (format == DATA_FORMAT_PARQUET && blobColumns != NIL)
	{
		ResolveGeoParquetColumns(url, format, blobColumns);
	}

	return columns;
}

/*
 * DescribeColumnsFromIcebergMetadataURI retrieves the columns for a given
 * Iceberg metadata URI.
 */
List *
DescribeColumnsFromIcebergMetadataURI(char *uri, bool emitFilename)
{
	List	   *columns = NIL;

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(uri);

	if (metadata == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table metadata is NULL")));
	}

	int32_t		schemaId = metadata->current_schema_id;

	IcebergTableSchema *schema = GetIcebergTableSchemaByIdFromTableMetadata(metadata, schemaId);

	size_t		totalFields = schema->fields_length;

	for (size_t fieldIdx = 0; fieldIdx < totalFields; fieldIdx++)
	{
		DataFileSchemaField *field = &schema->fields[fieldIdx];

		char	   *columnName = pstrdup(field->name);

		columnName = MakeSimpleColumnName(columnName, DATA_FORMAT_ICEBERG);

		PGType		pgType = IcebergFieldToPostgresType(field->type);

		Oid			typeOid = pgType.postgresTypeOid;
		int			typemod = pgType.postgresTypeMod;
		Oid			collationId = InvalidOid;

		ColumnDef  *column = makeColumnDef(columnName, typeOid, typemod, collationId);

		columns = lappend(columns, column);
	}

	if (emitFilename)
		columns = lappend(columns, makeColumnDef("_filename", TEXTOID, -1, InvalidOid));

	return columns;
}


/*
 * ResolveGeoParquetColumns checks whether any of the columns
 * are marked as geometry columns with WKB encoding via the
 * Parquet metadata.
 *
 * See the GeoParquet spec for details:
 * https://geoparquet.org/releases/v1.1.0/
 */
static void
ResolveGeoParquetColumns(char *url, CopyDataFormat format, List *blobColumns)
{
	/*
	 * The following query will return the list of the geometry type columns
	 * and the data encoding.  If the encoding is 'WKB', we can convert a
	 * matching column name into a geometry column, otherwise let's just leave
	 * it alone.
	 */

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	char	   *describeQuery = GetGeomColumnsMetadataQuery(url);
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, describeQuery);

	CheckPGDuckResult(pgDuckConn, result);

	PG_TRY();
	{
		int			columnCount = PQntuples(result);

		for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{
			char	   *columnName = PQgetvalue(result, columnIndex, 0);
			char	   *columnEncoding = PQgetvalue(result, columnIndex, 1);

			columnName = MakeSimpleColumnName(columnName, format);

			/* check the encoding */
			if (strcasecmp(columnEncoding, "WKB") == 0)
			{
				/*
				 * This is a candidate for conversion to geometry; let's
				 * iterate and compare column names to match.  (The list of
				 * columns should be small, so effective O(n^2) is fine here.)
				 */

				ListCell   *cell;
				bool		found = false;

				foreach(cell, blobColumns)
				{
					ColumnDef  *coldef = (ColumnDef *) lfirst(cell);

					if (strcmp(coldef->colname, columnName) == 0)
					{
						ErrorIfPgLakeSpatialNotEnabled();

						/* This is a bytea column, so just change here. */

						found = true;
						coldef->typeName->typeOid = GeometryTypeId();
						coldef->typeName->typemod = -1;
						break;
					}
				}

				if (!found)
				{
					ereport(WARNING,
							(errcode(ERRCODE_INTERNAL_ERROR),
							 errmsg("could not find metadata-referenced geometry column: %s", columnName)));
				}
			}
		}

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleasePGDuckConnection(pgDuckConn);
}

/*
 * IsStructOrIsArray returns true if the given type name is a STRUCT or an
 * array type.
 */
static bool
IsStructOrIsArray(const char *typeName)
{
	return IsArrayType(typeName) || IsStructType(typeName);
}

/*
 * DescribeColumnsQueryForURL returns the query we can use to get the column
 * names and types of a given URL.
 */
static char *
DescribeColumnsQueryForURL(char *url,
						   CopyDataFormat format,
						   CopyDataCompression compression,
						   List *copyOptions)
{
	bool		emitFilename = GetBoolOption(copyOptions, "filename", false);

	StringInfoData command;

	initStringInfo(&command);

	appendStringInfoString(&command, "SELECT column_name, column_type FROM (DESCRIBE ");

	/* add explicit compression options where needed */
	if (format == DATA_FORMAT_CSV)
	{
		appendStringInfo(&command, "FROM read_csv(%s%s",
						 quote_literal_cstr(url),
						 CopyOptionsToReadCSVParams(copyOptions));

		if (compression != DATA_COMPRESSION_INVALID)
			appendStringInfo(&command, ", compression=%s",
							 quote_literal_cstr(CopyDataCompressionToName(compression)));

		if (emitFilename)
			appendStringInfo(&command, ", filename='_filename'");

		appendStringInfoString(&command, ")");
	}
	else if (format == DATA_FORMAT_JSON)
	{
		appendStringInfo(&command, "FROM read_json_auto(%s",
						 quote_literal_cstr(url));

		/* read_json_auto does not support compression='none' */
		if (compression != DATA_COMPRESSION_INVALID &&
			compression != DATA_COMPRESSION_NONE)
			appendStringInfo(&command, ", compression=%s",
							 quote_literal_cstr(CopyDataCompressionToName(compression)));

		if (emitFilename)
			appendStringInfo(&command, ", filename='_filename'");

		appendStringInfoString(&command, ")");
	}
	else if (format == DATA_FORMAT_DELTA)
	{
		appendStringInfo(&command, "FROM delta_scan(%s",
						 quote_literal_cstr(url));

		if (emitFilename)
			appendStringInfo(&command, ", filename='_filename'");

		appendStringInfoString(&command, ")");
	}
	else if (format == DATA_FORMAT_PARQUET)
	{
		/* Parquet does not need explicit compression option */
		appendStringInfo(&command, "FROM read_parquet(%s",
						 quote_literal_cstr(url));

		if (emitFilename)
			appendStringInfo(&command, ", filename='_filename'");

		appendStringInfoString(&command, ")");
	}
	else if (format == DATA_FORMAT_GDAL)
	{
		appendStringInfo(&command, "FROM %s",
						 GDALReadFunctionCall(url, compression, copyOptions));
	}
	else
	{
		appendStringInfo(&command, "FROM %s", quote_literal_cstr(url));
	}

	appendStringInfoString(&command, ")");

	return command.data;
}

/*
 * This function transforms a column name as detected in the source file into
 * something a little nicer to use in the `psql` terminal.  Currently we only
 * downcase field names to avoid needing to quote mixed-case names, but we could
 * add more things here.
 *
 * Note that since the JSON format requires fixed keys for the mapping itself,
 * we cannot perform this translation in this case.
 */
char *
MakeSimpleColumnName(char *columnName, CopyDataFormat format)
{
	/*
	 * In case of ascii column names, we prefer to convert them to not require
	 * users to use double quotes in all their queries
	 *
	 * However, we don't want to do this for JSON format, as the column names
	 * are used as keys in the JSON object and we want to preserve the
	 * original case.
	 */
	if (pg_is_ascii(columnName) && format != DATA_FORMAT_JSON)
		columnName = asc_tolower(columnName, strlen(columnName));
	else
		columnName = pstrdup(columnName);

	return columnName;
}


/*
 * SniffCSVOptions suggests a list of options to use for a given CSV file.
 */
List *
SniffCSVOptions(char *url, CopyDataCompression compression, List *options)
{
	if (HasAllSniffCSVOptions(options))
	{
		/* no options to detect */
		return options;
	}

	char	   *delimiter = NULL;
	char	   *quote = NULL;
	char	   *escape = NULL;
	bool		header = NULL;
	char	   *newLine = NULL;

	SniffCSV(url, compression, options,
			 &delimiter, &quote, &escape, &header, &newLine);

	if (!HasOption((List *) options, "delimiter") && *delimiter != '\0')
	{
		options = lappend(options, makeDefElem("delimiter",
											   (Node *) makeString(delimiter),
											   -1));
	}

	if (!HasOption((List *) options, "quote") && *quote != '\0')
	{
		options = lappend(options, makeDefElem("quote",
											   (Node *) makeString(quote),
											   -1));
	}

	if (!HasOption((List *) options, "escape") && *escape != '\0')
	{
		options = lappend(options, makeDefElem("escape",
											   (Node *) makeString(escape),
											   -1));
	}

	if (!HasOption((List *) options, "header"))
	{
		options = lappend(options, makeDefElem("header",
											   (Node *) makeBoolean(header),
											   -1));
	}

	if (!HasOption((List *) options, "new_line"))
	{
		options = lappend(options, makeDefElem("new_line",
											   (Node *) makeString(newLine),
											   -1));
	}


	return options;
}


/*
 * HasAllCSVOptions returns whether the given list of options contains all
 * CSV options that we can detect.
 */
static bool
HasAllSniffCSVOptions(List *options)
{
	return HasOption(options, "delimiter") &&
		HasOption(options, "new_line") &&
		HasOption(options, "quote") &&
		HasOption(options, "escape") &&
		HasOption(options, "header");
}


/*
 * DescribeColumnsForLogFormat returns the list of ColumnDefs for a log format
 * option that appears in the options list.
 *
 * In the future, we may want to get this from a table to make it more configurable.
 */
static List *
DescribeColumnsForLogFormat(List *options)
{
	DefElem    *logFormatOption = GetOption(options, "log_format");

	if (logFormatOption == NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("\"log_format\" option is required for log format")));

	char	   *logFormat = defGetString(logFormatOption);
	bool		emitFilename = GetBoolOption(options, "filename", false);

	if (strcmp(logFormat, "s3") == 0)
	{
		List	   *columns = NIL;

		columns = lappend(columns, makeColumnDef("bucket_owner", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("bucket", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("request_time", TIMESTAMPTZOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("remote_ip", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("requester", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("request_id", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("operation", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("s3_key", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("request_uri", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("http_status", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("s3_errorcode", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("bytes_sent", INT8OID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("object_size", INT8OID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("total_time", INT8OID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("turn_around_time", INT8OID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("referer", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("user_agent", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("version_id", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("host_id", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("sigver", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("cypersuite", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("auth_type", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("host_header", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("tls_version", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("access_point_arn", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("acl_required", TEXTOID, -1, InvalidOid));
		columns = lappend(columns, makeColumnDef("extra", TEXTOID, -1, InvalidOid));

		if (emitFilename)
			columns = lappend(columns, makeColumnDef("_filename", TEXTOID, -1, InvalidOid));

		return columns;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("unrecognized log_format value: %s", logFormat)));
	}
}
