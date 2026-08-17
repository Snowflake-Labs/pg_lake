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
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/permissions/roles.h"

#include "utils/builtins.h"
#include "utils/timestamp.h"


/*
 * GetRemoteFileSize gets the size of a remote file.
 */
int64
GetRemoteFileSize(char *path)
{
	char	   *query = psprintf("SELECT pg_lake_file_size(%s)",
								 quote_literal_cstr(path));

	char	   *fileSizeStr = GetSingleValueFromPGDuck(query);
	int64		fileSize = pg_strtoint64(fileSizeStr);

	return fileSize;
}


/*
 * GetRemoteFileRowCount gets the number of rows in a remote Parquet file.
 */
int64
GetRemoteParquetFileRowCount(char *path)
{
	char	   *query = psprintf("SELECT count(*) FROM read_parquet(%s)",
								 quote_literal_cstr(path));

	char	   *rowCountStr = GetSingleValueFromPGDuck(query);
	int64		rowCount = pg_strtoint64(rowCountStr);

	return rowCount;
}


/*
 * ListRemoteFileDescriptions gets a list of remote file descriptions.
 */
List *
ListRemoteFileDescriptions(char *pattern)
{
	List	   *fileList = NIL;

	char	   *query = psprintf("SELECT url, file_size, last_modified_time, etag "
								 "FROM pg_lake_list_files(%s)",
								 quote_literal_cstr(pattern));

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query);

	/* throw error if anything failed  */
	CheckPGDuckResult(pgDuckConn, result);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		for (int rowIndex = 0; rowIndex < PQntuples(result); rowIndex++)
		{
			if (PQgetisnull(result, rowIndex, 0))
			{
				ereport(DEBUG1, errmsg("unexpected NULL value in result set"));
				continue;
			}

			RemoteFileDesc *fileDesc = palloc0(sizeof(RemoteFileDesc));

			fileDesc->path = pstrdup(PQgetvalue(result, rowIndex, 0));

			if (!PQgetisnull(result, rowIndex, 1))
			{
				fileDesc->hasFileSize = true;
				fileDesc->fileSize = pg_strtoint64(PQgetvalue(result, rowIndex, 1));
			}

			if (!PQgetisnull(result, rowIndex, 2))
			{
				char	   *lastModifiedTimeStr = PQgetvalue(result, rowIndex, 2);
				Datum		lastModifiedTimeDatum =
					DirectFunctionCall3(timestamp_in, CStringGetDatum(lastModifiedTimeStr), 0, -1);

				fileDesc->hasLastModifiedTime = true;
				fileDesc->lastModifiedTime = DatumGetTimestampTz(lastModifiedTimeDatum);
			}

			if (!PQgetisnull(result, rowIndex, 3))
				fileDesc->etag = pstrdup(PQgetvalue(result, rowIndex, 3));

			fileList = lappend(fileList, fileDesc);
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

	return fileList;

}


/*
 * ListRemoteFileNames gets a list of remote file names.
 */
List *
ListRemoteFileNames(char *pattern)
{
	List	   *descriptionList = ListRemoteFileDescriptions(pattern);
	ListCell   *descriptionCell = NULL;
	List	   *nameList = NIL;

	foreach(descriptionCell, descriptionList)
	{
		RemoteFileDesc *fileDescription = lfirst(descriptionCell);

		nameList = lappend(nameList, fileDescription->path);
	}

	return nameList;
}


/*
 * RemoteFileExists returns whether the given file exists in the remote storage.
 */
bool
RemoteFileExists(char *path)
{
	char	   *query = psprintf("SELECT pg_lake_file_exists(%s)",
								 quote_literal_cstr(path));

	char	   *fileExistsStr = GetSingleValueFromPGDuck(query);

	bool		fileExists = false;

	if (!parse_bool(fileExistsStr, &fileExists))
		ereport(ERROR, (errmsg("could not parse fileExists response: %s", fileExistsStr)));

	return fileExists;
}


/*
* DeleteRemotePrefix lists all the files in the given path and deletes them.
* It recurses into subdirectories/prefixes.
*
* pg_lake_remove_file gets a whole vector of file names at a time and, for S3,
* deletes them in batched DeleteObjects requests (up to 1000 keys each) rather
* than one request per file.
*
* The call goes in WHERE rather than the select list so that we get a single
* count back instead of a row per file, without DuckDB pruning a projection
* that nothing reads.
*/
bool
DeleteRemotePrefix(char *path)
{
	StringInfo	recursivePath = makeStringInfo();

	appendStringInfo(recursivePath, "%s/**", path);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "SELECT count(*) FROM glob(%s) WHERE pg_lake_remove_file(file)",
					 quote_literal_cstr(recursivePath->data));

	return ExecuteOptionalCommandInPGDuck(query->data);
}

/*
 * DeleteRemoteFile deletes a remote file via pg_lake_remove_file.
 */
bool
DeleteRemoteFile(char *path)
{
	char	   *query = psprintf("SELECT pg_lake_remove_file(%s)",
								 quote_literal_cstr(path));

	return ExecuteOptionalCommandInPGDuck(query);
}
