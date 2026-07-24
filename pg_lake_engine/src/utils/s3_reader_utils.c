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
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "utils/builtins.h"

static char *ReadTextContent(const char *command);
static char *ReadBlobContent(const char *command, size_t *contentLength);
static char *ReadTextFileCommand(const char *textFileUri);
static char *ReadValidJsonFileCommand(const char *jsonFileUri);
static char *ReadBlobFileCommand(const char *blobFileUri);
static const char *ResolveReadPath(const char *fileUri, bool bypassCache);

/* bounded retries for a racy fresh read of a fixed-path JSON file */
#define NO_CACHE_JSON_READ_MAX_ATTEMPTS 10

/*
 * GetTextFromURI reads the content of a text file at a supported cloud URL.
 * Rejects local filesystem paths and other unsupported schemes.
 */
char *
GetTextFromURI(const char *textFileUri)
{
	return ReadTextContent(ReadTextFileCommand(ResolveReadPath(textFileUri, false)));
}

/*
 * GetJsonFromURINoCache reads a fixed-path JSON file (e.g. an object store
 * catalog.json) fresh from object storage, bypassing the file cache, and
 * returns its content.
 *
 * A fixed-path file can be rewritten concurrently (our own writer or another
 * cluster), so a fresh read may race the writer: DuckDB aborts a read whose S3
 * ETag changes mid-read (an error result), and an append-blob overwrite can be
 * read torn (incomplete, so the json_valid filter drops it to zero rows). Both
 * are transient, so we inspect the result and retry a bounded number of times
 * on a fresh connection. We deliberately do NOT catch an ereport to retry: that
 * would resume mid-query with unwound executor/snapshot state and crash the
 * backend. The last attempt reads without validation so a genuinely invalid
 * document surfaces its real error downstream instead of looping.
 */
char *
GetJsonFromURINoCache(const char *jsonFileUri)
{
	const char *readPath = ResolveReadPath(jsonFileUri, true);
	char	   *validatingCommand = ReadValidJsonFileCommand(readPath);
	char	   *plainCommand = ReadTextFileCommand(readPath);

	for (int attempt = 1;; attempt++)
	{
		bool		lastAttempt = attempt >= NO_CACHE_JSON_READ_MAX_ATTEMPTS;
		const char *command = lastAttempt ? plainCommand : validatingCommand;

		PGDuckConnection *pgDuckConn = GetPGDuckConnection();
		PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, command);

		/*
		 * a racing writer shows up as an error result or (torn read) zero
		 * rows
		 */
		if (!lastAttempt &&
			(PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) != 1))
		{
			PQclear(result);
			ReleasePGDuckConnection(pgDuckConn);

			/* brief, increasing backoff to let the concurrent writer finish */
			pg_usleep(attempt * 25L * 1000L);
			continue;
		}

		char	   *content;

		/*
		 * make sure we PQclear the result (throws on a real error / bad row
		 * count)
		 */
		PG_TRY();
		{
			ThrowIfPGDuckResultHasError(pgDuckConn, result);

			int			rowCount = PQntuples(result);

			if (rowCount != 1)
				elog(ERROR, "Expected 1 row while reading JSON file, got %d",
					 rowCount);

			content = pstrdup(PQgetvalue(result, 0, 0));
		}
		PG_FINALLY();
		{
			PQclear(result);
			ReleasePGDuckConnection(pgDuckConn);
		}
		PG_END_TRY();

		return content;
	}
}

/*
 * ResolveReadPath returns the path ReadTextContent should read for a cloud URL:
 * a pending upload's local file if any (authoritative), otherwise the URL,
 * optionally "nocache"-prefixed to bypass the file cache. The prefix is applied
 * here, after IsSupportedURL, which only accepts bare schemes.
 */
static const char *
ResolveReadPath(const char *fileUri, bool bypassCache)
{
	if (!IsSupportedURL(fileUri))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_lake: unsupported URL: \"%s\"", fileUri),
				 errhint("Paths must use a supported cloud storage scheme "
						 "(s3://, azure://, gcs://, etc.).")));

	const char *localPath = GetPendingUploadLocalPath(fileUri);

	if (localPath != NULL)
		return localPath;

	if (bypassCache)
		return psprintf("%s%s", NO_CACHE_URL_PREFIX, fileUri);

	return fileUri;
}

/*
 * GetBlobFromURI reads the content of a blob file at a supported cloud URL.
 * Rejects local filesystem paths and other unsupported schemes.
 *
 * !!NOTE!!: Caller is responsible for freeing the returned content.
 */
char *
GetBlobFromURI(const char *blobFileUri, size_t *contentLength)
{
	if (!IsSupportedURL(blobFileUri))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("pg_lake: unsupported URL: \"%s\"", blobFileUri),
				 errhint("Paths must use a supported cloud storage scheme "
						 "(s3://, azure://, gcs://, etc.).")));

	const char *localPath = GetPendingUploadLocalPath(blobFileUri);
	const char *readPath = localPath != NULL ? localPath : blobFileUri;

	return ReadBlobContent(ReadBlobFileCommand(readPath), contentLength);
}

/*
* ReadTextContent reads the content of a text file from the PGDuck server.
* The input command should be read_text(), and the
* result should be a single row with a single column.
*/
static char *
ReadTextContent(const char *command)
{
	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result =
		ExecuteQueryOnPGDuckConnection(pgDuckConn, command);

	char	   *content;

	/* make sure we PQclear the result */
	PG_TRY();
	{
		ThrowIfPGDuckResultHasError(pgDuckConn, result);

		int			rowCount = PQntuples(result);

		if (rowCount != 1)
			elog(ERROR, "Expected 1 row while reading text file, got %d", rowCount);

		content = pstrdup(PQgetvalue(result, 0, 0));
	}
	PG_FINALLY();
	{
		PQclear(result);
		ReleasePGDuckConnection(pgDuckConn);
	}
	PG_END_TRY();

	return content;
}

/*
* ReadBlobContent reads the content of a blob file from the PGDuck server.
* The input command should be read_blob(), and the result should be a single
* row with a single column.
*
* !!NOTE!!: Caller is responsible for freeing the returned content.
*/
static char *
ReadBlobContent(const char *command, size_t *contentLength)
{
	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result =
		ExecuteQueryOnPGDuckConnection(pgDuckConn, command);

	char	   *blobContent = NULL;
	char	   *blobContentCopy = NULL;

	/* make sure we PQclear the result */
	PG_TRY();
	{
		ThrowIfPGDuckResultHasError(pgDuckConn, result);

		int			rowCount = PQntuples(result);

		if (rowCount != 1)
			elog(ERROR, "Expected 1 row while reading blob file, got %d", rowCount);

		blobContent = PQgetvalue(result, 0, 0);
		blobContent = (char *) PQunescapeBytea((unsigned char *) blobContent, contentLength);

		if (blobContent == NULL)
		{
			elog(ERROR, "Failed to unescape bytea data");
		}

		blobContentCopy = palloc0(*contentLength);
		memcpy(blobContentCopy, blobContent, *contentLength);

		PQfreemem(blobContent);
	}
	PG_FINALLY();
	{
		PQclear(result);
		ReleasePGDuckConnection(pgDuckConn);
	}
	PG_END_TRY();

	return blobContentCopy;
}

/*
* ReadTextFileCommand returns the SQL command to read the content of
* a text file.
*/
static char *
ReadTextFileCommand(const char *textFileUri)
{
	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "SELECT content FROM read_text(%s)",
					 quote_literal_cstr(textFileUri));


	return command.data;
}

/*
 * ReadValidJsonFileCommand is like ReadTextFileCommand but returns no rows if
 * the file is not valid JSON, so a torn read of a concurrently rewritten file
 * fails instead of returning incomplete content.
 */
static char *
ReadValidJsonFileCommand(const char *jsonFileUri)
{
	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command,
					 "SELECT content FROM read_text(%s) WHERE json_valid(content)",
					 quote_literal_cstr(jsonFileUri));

	return command.data;
}

/*
* ReadBlobFileCommand returns the SQL command to read the content of
* a blob file.
*/
static char *
ReadBlobFileCommand(const char *blobFileUri)
{
	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "SELECT content FROM read_blob(%s)",
					 quote_literal_cstr(blobFileUri));

	return command.data;
}
