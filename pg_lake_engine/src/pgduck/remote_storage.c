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

static void DeleteRemoteFilesReportingOutcomes(List *paths, List **deletedPaths,
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
 * A batch of paths that have nothing to do with each other needs an outcome per
 * path. Without one, a single unreachable object charges its failure to every
 * path that shared its request, so a caller that retires paths by failure count
 * (such as the deletion queue) would eventually retire a whole batch of healthy
 * ones. pg_lake_try_remove_file reports the outcome per row rather than failing
 * the statement, so one request answers for the whole batch.
 *
 * A caller that tracks neither outcome has nothing to attribute, so it takes the
 * cheaper statement that reports a single status: a boolean per path is more than
 * it needs coming back, and a verdict nobody reads is the per-file cost this
 * batching exists to remove.
 */
void
DeleteRemoteFileBatch(List *paths, List **deletedPaths, List **failedPaths)
{
	if (paths == NIL)
		return;

	if (deletedPaths == NULL && failedPaths == NULL)
	{
		DeleteRemoteFiles(paths);
		return;
	}

	MemoryContext savedContext = CurrentMemoryContext;
	volatile bool outcomesReported = false;

	PG_TRY();
	{
		DeleteRemoteFilesReportingOutcomes(paths, deletedPaths, failedPaths);
		outcomesReported = true;
	}
	PG_CATCH();
	{
		/*
		 * continue with a warning unless it was a cancellation, as
		 * ExecuteOptionalCommandInPGDuck does
		 */
		MemoryContextSwitchTo(savedContext);

		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
			edata->elevel = WARNING;

		ThrowErrorData(edata);
	}
	PG_END_TRY();

	if (outcomesReported)
		return;

	/*
	 * A request that did not complete says nothing about any of its paths, so
	 * none of them is accounted for. Report them all as failed: a path the
	 * request did remove is claimed again by the next pass, and removing a
	 * file twice is a no-op, while the other way around loses a file that is
	 * still there.
	 */
	if (failedPaths != NULL)
		*failedPaths = list_concat(*failedPaths, paths);
}


/*
 * DeleteRemoteFilesReportingOutcomes deletes the given files in a single pgduck
 * request and reads back what became of each one: pg_lake_try_remove_file returns
 * NULL for a path that is gone and the reason for a path that could not be
 * removed. Throws if the request itself did not complete, in which case the
 * output lists are left untouched.
 *
 * The reason is only reported here, since the deletion queue records how often a
 * path failed and not why. It is the same reason a failed removal used to raise,
 * so a batch that partially fails still says as much about it as one that failed
 * outright.
 */
static void
DeleteRemoteFilesReportingOutcomes(List *paths, List **deletedPaths,
								   List **failedPaths)
{
	StringInfo	query = makeStringInfo();

	appendStringInfoString(query,
						   "SELECT file, pg_lake_try_remove_file(file) AS error "
						   "FROM (VALUES ");

	ListCell   *pathCell = NULL;

	foreach(pathCell, paths)
	{
		char	   *path = lfirst(pathCell);

		if (pathCell != list_head(paths))
			appendStringInfoChar(query, ',');

		appendStringInfo(query, "(%s)", quote_literal_cstr(path));
	}

	appendStringInfoString(query, ") AS batch(file)");

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();

	List	   *volatile removedPaths = NIL;
	List	   *volatile unremovedPaths = NIL;
	char	   *volatile firstError = NULL;
	char	   *volatile firstFailedPath = NULL;

	/*
	 * Release the connection whichever way we leave, because our caller turns
	 * an error here into a warning and carries on: a connection left behind
	 * would be one more per pass.
	 */
	PGresult   *volatile resultToClear = NULL;

	PG_TRY();
	{
		PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query->data);

		/* throws, having cleared the result, if the request failed */
		CheckPGDuckResult(pgDuckConn, result);

		resultToClear = result;

		if (PQntuples(result) != list_length(paths))
		{
			ereport(ERROR,
					(errmsg("query engine reported %d outcomes for a batch of %d files",
							PQntuples(result), list_length(paths))));
		}

		for (int rowIndex = 0; rowIndex < PQntuples(result); rowIndex++)
		{
			char	   *path = pstrdup(PQgetvalue(result, rowIndex, 0));

			if (PQgetisnull(result, rowIndex, 1))
			{
				removedPaths = lappend(removedPaths, path);
				continue;
			}

			char	   *error = pstrdup(PQgetvalue(result, rowIndex, 1));

			unremovedPaths = lappend(unremovedPaths, path);

			ereport(DEBUG1, (errmsg("could not remove %s: %s", path, error)));

			if (firstFailedPath == NULL)
			{
				firstFailedPath = path;
				firstError = error;
			}
		}
	}
	PG_FINALLY();
	{
		if (resultToClear != NULL)
			PQclear(resultToClear);

		ReleasePGDuckConnection(pgDuckConn);
	}
	PG_END_TRY();

	if (unremovedPaths != NIL)
	{
		ereport(WARNING,
				(errmsg("could not remove %d of %d files from object storage",
						list_length(unremovedPaths), list_length(paths)),
				 errdetail("%s: %s", firstFailedPath, firstError)));
	}

	if (deletedPaths != NULL)
		*deletedPaths = list_concat(*deletedPaths, removedPaths);

	if (failedPaths != NULL)
		*failedPaths = list_concat(*failedPaths, unremovedPaths);
}
