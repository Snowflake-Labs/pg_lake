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
 * Functions for managing the pgduck cache.
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "pg_lake/pgduck/keywords.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/cache_control.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"


/*
 * AddFileToCache adds a file to the pgduck cache.
 */
int64
AddFileToCache(char *url, bool refresh)
{
	char	   *query = psprintf("CALL pg_lake_cache_file(%s,%s)",
								 duckdb_quote_literal(url),
								 refresh ? "true" : "false");
	char	   *fileSizeStr = GetSingleValueFromPGDuck(query);
	int64		fileSize = pg_strtoint64(fileSizeStr);

	return fileSize;
}


/*
 * RemoveFileFromCache removes a single file from the pgduck cache and
 * returns true if a file was removed.
 */
bool
RemoveFileFromCache(char *url)
{
	char	   *query = psprintf("CALL pg_lake_uncache_file(%s)",
								 duckdb_quote_literal(url));
	char	   *removedString = GetSingleValueFromPGDuck(query);

	return removedString[0] == 't';
}


/*
 * ListFilesInCache retrieves a list of files in cache.
 */
List *
ListFilesInCache(void)
{
	List	   *files = NIL;

	const char *listQuery = "CALL pg_lake_list_cache()";

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, listQuery);

	CheckPGDuckResult(pgDuckConn, result);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		int			rowCount = PQntuples(result);
		int			columnCount = PQnfields(result);

		if (columnCount < 3)
			ereport(ERROR, (errmsg("unexpected column count %d", columnCount)));

		for (int rowIndex = 0; rowIndex < rowCount; rowIndex++)
		{
			char	   *url = PQgetvalue(result, rowIndex, 0);
			char	   *fileSizeStr = PQgetvalue(result, rowIndex, 1);
			char	   *lastAccessStr = PQgetvalue(result, rowIndex, 2);

			/* parse the last access timestamp */
			Datum		lastAccessTimeDatum =
				DirectFunctionCall3(timestamp_in, CStringGetDatum(lastAccessStr), 0, -1);

			FileInCache *fileInCache = palloc0(sizeof(FileInCache));

			fileInCache->url = pstrdup(url);
			fileInCache->fileSize = pg_strtoint64(fileSizeStr);
			fileInCache->lastAccessTime = DatumGetTimestamp(lastAccessTimeDatum);

			files = lappend(files, fileInCache);
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

	return files;
}
