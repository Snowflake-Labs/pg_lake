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

#include "pg_lake/pgduck/client.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "utils/builtins.h"

static char *ReadTextContent(const char *command);
static char *ReadBlobContent(const char *command, size_t *contentLength);
static char *ReadTextFileCommand(const char *textFileUri);
static char *ReadBlobFileCommand(const char *blobFileUri);

/*
* GetTextFromURI reads the content of a text file.
*/
char *
GetTextFromURI(const char *textFileUri)
{
	const char *localPath = GetPendingUploadLocalPath(textFileUri);
	const char *readPath = localPath != NULL ? localPath : textFileUri;

	return ReadTextContent(ReadTextFileCommand(readPath));
}

/*
 * GetBlobFromURI reads the content of a blob file.
 *
 * !!NOTE!!: Caller is responsible for freeing the returned content.
 */
char *
GetBlobFromURI(const char *blobFileUri, size_t *contentLength)
{
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
