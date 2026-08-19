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
#include "miscadmin.h"

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/permissions/roles.h"

#include "utils/builtins.h"
#include "utils/timestamp.h"

static void AttributeFailedDeletion(List *paths, List **deletedPaths,
									List **failedPaths);


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
* that nothing reads. DeleteRemoteFiles writes the same call in the select list
* because there the paths going out dwarf a boolean per path coming back. Here
* nothing goes out but the pattern, so a row per object under the prefix is the
* whole response.
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


/*
 * DeleteRemoteFiles deletes the given files in a single pgduck request and
 * reports whether the request succeeded.
 *
 * Deleting an arbitrary set of files used to cost one request per file, which
 * made cleanup of a large backlog take as long as the backlog was long. One
 * statement over a VALUES list arrives as a single vector instead, so
 * pg_lake_remove_file collapses it into one bulk delete against the object
 * store.
 *
 * Callers are expected to keep a batch within FILE_DELETION_BATCH_SIZE: the
 * point of batching is to bound the work behind one request, not to move an
 * unbounded delete from Postgres into pgduck.
 */
bool
DeleteRemoteFiles(List *paths)
{
	if (paths == NIL)
		return true;

	/* keep the single-file case on the plain, cheaper statement */
	if (list_length(paths) == 1)
		return DeleteRemoteFile((char *) linitial(paths));

	StringInfo	query = makeStringInfo();

	appendStringInfoString(query, "SELECT pg_lake_remove_file(file) FROM (VALUES ");

	ListCell   *pathCell = NULL;

	foreach(pathCell, paths)
	{
		char	   *path = lfirst(pathCell);

		if (pathCell != list_head(paths))
			appendStringInfoChar(query, ',');

		appendStringInfo(query, "(%s)", quote_literal_cstr(path));
	}

	appendStringInfoString(query, ") AS batch(file)");

	return ExecuteOptionalCommandInPGDuck(query->data);
}


/*
 * DeleteRemoteFileBatch deletes the given files in one pgduck request and
 * records the per-path outcome: paths that were removed are appended to
 * *deletedPaths and paths that were not to *failedPaths. Either output list
 * may be NULL when the caller does not track that outcome.
 *
 * pgduck reports one status for the whole request and does not say which path
 * was at fault, so a failed batch has to be narrowed down. Without that, a
 * single unreachable object would charge its failure to every path that shared
 * its request, so a caller that retires paths by failure count (such as the
 * deletion queue) would eventually retire a whole batch of healthy ones.
 * Removing a file twice is a no-op, so replaying paths the failed batch had
 * already deleted is safe.
 *
 * The narrowing bisects rather than walking the batch, because a path that
 * cannot be removed is claimed again by every following pass, so whatever a
 * failed batch costs is paid once per pass until retry_count retires the row.
 * Walking makes that a request per path; bisecting finds k bad paths out of n
 * in O(k log n) requests, so the usual case of one poison path in a full batch
 * costs about 20 requests instead of 1000.
 *
 * A caller that tracks neither outcome has nothing to tell apart, so it skips
 * the narrowing entirely: producing a verdict nobody reads is the per-file cost
 * this batching exists to remove.
 */
void
DeleteRemoteFileBatch(List *paths, List **deletedPaths, List **failedPaths)
{
	if (paths == NIL)
		return;

	if (DeleteRemoteFiles(paths))
	{
		if (deletedPaths != NULL)
			*deletedPaths = list_concat(*deletedPaths, paths);

		return;
	}

	if (deletedPaths == NULL && failedPaths == NULL)
	{
		/* no outcome to record, so no reason to find out which path failed */
		return;
	}

	AttributeFailedDeletion(paths, deletedPaths, failedPaths);
}


/*
 * AttributeFailedDeletion works out which of the paths of a failed batch could
 * not be removed, by halving the batch and re-issuing each half. A half that
 * succeeds accounts for all of its paths in one request; only a half that fails
 * is split further, so the requests spent follow the number of bad paths rather
 * than the size of the batch.
 */
static void
AttributeFailedDeletion(List *paths, List **deletedPaths, List **failedPaths)
{
	if (list_length(paths) == 1)
	{
		char	   *path = linitial(paths);

		/*
		 * A single path is its own verdict: the batch we just failed was this
		 * one request, so there is nothing left to re-issue.
		 */
		if (failedPaths != NULL)
			*failedPaths = lappend(*failedPaths, path);

		return;
	}

	int			halfLength = list_length(paths) / 2;
	List	   *halves[] = {
		list_truncate(list_copy(paths), halfLength),
		list_copy_tail(paths, halfLength)
	};

	for (int halfIndex = 0; halfIndex < 2; halfIndex++)
	{
		List	   *half = halves[halfIndex];

		if (DeleteRemoteFiles(half))
		{
			if (deletedPaths != NULL)
				*deletedPaths = list_concat(*deletedPaths, half);
		}
		else
			AttributeFailedDeletion(half, deletedPaths, failedPaths);

		/* narrowing a batch is a sequence of requests, so stay cancellable */
		CHECK_FOR_INTERRUPTS();
	}
}
