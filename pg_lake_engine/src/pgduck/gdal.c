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

#include "commands/defrem.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/gdal.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"

static void ValidateGDALLayer(char *pathExpression, char *layerName);


/*
 * ValidateGDALLayer errors out when the layer option does not name a layer in
 * the file. st_read terminates pgduck_server on a layer that does not exist,
 * so ask st_read_meta for the layer list first.
 */
static void
ValidateGDALLayer(char *pathExpression, char *layerName)
{
	char	   *layerExists =
		GetSingleValueFromPGDuck(psprintf("SELECT list_contains("
										  "list_transform(layers, l -> l.name), %s) "
										  "FROM st_read_meta(%s)",
										  quote_literal_cstr(layerName),
										  pathExpression));

	if (layerExists != NULL && strcmp(layerExists, "t") == 0)
		return;

	char	   *layerNames =
		GetSingleValueFromPGDuck(psprintf("SELECT list_aggregate("
										  "list_transform(layers, l -> l.name), "
										  "'string_agg', ', ') "
										  "FROM st_read_meta(%s)",
										  pathExpression));

	ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			 errmsg("layer \"%s\" could not be found", layerName),
			 errdetail("Available layers are: %s",
					   layerNames != NULL ? layerNames : "none")));
}


/*
 * GDALReadFunctionCall constructs the st_read function call for the given
 * path and compression.
 *
 * GDAL sources can hold curve types (CIRCULARSTRING, MULTICURVE) that DuckDB's
 * GEOMETRY cannot represent, so scans pass forceKeepWKB to keep geometry as raw
 * WKB and cast it in Postgres; DESCRIBE clears it so the column comes back typed
 * geometry rather than bytea.
 *
 * See https://gdal.org/user/virtual_file_systems.html for compression syntax.
 */
char *
GDALReadFunctionCall(char *path, CopyDataCompression compression, List *options,
					 bool forceKeepWKB)
{
	DefElem    *filenameOption = GetOption(options, "filename");
	bool		emitFilename =
		filenameOption != NULL ? defGetBoolean(filenameOption) : false;

	StringInfoData stRead;
	StringInfoData pathExpression;

	initStringInfo(&stRead);
	initStringInfo(&pathExpression);

	/*
	 * Add an extra _filename column if specified. Not particularly useful,
	 * since there can only be 1 file, but done for consistency.
	 */
	if (emitFilename)
		appendStringInfo(&stRead, "(SELECT *, %s AS _filename FROM ",
						 quote_literal_cstr(path));

	appendStringInfoString(&stRead, "st_read(");

	if (compression == DATA_COMPRESSION_ZIP)
	{
		/* add vsizip for .zip files with { } syntax */
		appendStringInfo(&pathExpression,
						 "'/vsizip/{'||pg_lake_cache_file_local_path(%s)||'}'",
						 quote_literal_cstr(path));

		/* add path within zipfile */
		DefElem    *zipPathOption = GetOption(options, "zip_path");

		if (zipPathOption != NULL)
		{
			char	   *zipPath = defGetString(zipPathOption);

			/* ensure the string starts with / */
			if (zipPath[0] != '/')
				zipPath = psprintf("/%s", zipPath);

			appendStringInfo(&pathExpression, "||%s",
							 quote_literal_cstr(zipPath));
		}
	}
	else if (compression == DATA_COMPRESSION_GZIP)
	{
		/* vsigzip for .gz */
		appendStringInfo(&pathExpression,
						 "'/vsigzip/'||pg_lake_cache_file_local_path(%s)",
						 quote_literal_cstr(path));
	}
	else
	{
		/* just use the local path */
		appendStringInfo(&pathExpression,
						 "pg_lake_cache_file_local_path(%s)",
						 quote_literal_cstr(path));
	}

	appendStringInfoString(&stRead, pathExpression.data);

	/* whether to preserve WKB as a raw byte array */
	DefElem    *keepWKBOption = GetOption(options, "keep_wkb");

	if (forceKeepWKB || (keepWKBOption != NULL && defGetBoolean(keepWKBOption)))
		appendStringInfoString(&stRead, ", keep_wkb=true");

	/* select a specific layer */
	DefElem    *layerOption = GetOption(options, "layer");

	if (layerOption != NULL)
	{
		char	   *layerName = defGetString(layerOption);

		ValidateGDALLayer(pathExpression.data, layerName);

		appendStringInfo(&stRead, ", layer=%s",
						 quote_literal_cstr(layerName));
	}

	/* close st_read */
	appendStringInfoString(&stRead, ")");

	if (emitFilename)
		/* close subquery */
		appendStringInfoString(&stRead, ")");

	return stRead.data;
}
