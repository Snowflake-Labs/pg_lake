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

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/pgduck/sniff_csv.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"


/*
 * SniffCSV calls the DuckDB sniff_csv function to determine the
 * properties of the CSV file.
 */
void
SniffCSV(char *url, CopyDataCompression compression, List *options,
		 char **delimiter, char **quote, char **escape, bool *header, char **newLine)
{
	StringInfoData command;

	initStringInfo(&command);

	List	   *outputFiles = ListRemoteFileNames(url);

	if (!outputFiles || list_length(outputFiles) == 0)
		ereport(ERROR, (errmsg("couldn't find files at %s", url)));

	appendStringInfo(&command,
					 "SELECT Delimiter, Quote, Escape, HasHeader, NewLineDelimiter FROM sniff_csv(%s",
					 quote_literal_cstr((char *) linitial(outputFiles)));

	if (compression != DATA_COMPRESSION_INVALID)
	{
		appendStringInfo(&command, ", compression=%s",
						 quote_literal_cstr(CopyDataCompressionToName(compression)));
	}

	if (options != NIL)
	{
		/* include user-defined options */
		appendStringInfoString(&command, CopyOptionsToReadCSVParams(options));
	}

	appendStringInfoString(&command, ")");

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, command.data);

	CheckPGDuckResult(pgDuckConn, result);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		if (PQntuples(result) != 1 && PQnfields(result) != 4)
		{
			ereport(ERROR, (errmsg("unexpected CSV detection result")));
		}

		char	   *rawValue;

		rawValue = PQgetvalue(result, 0, 0);
		if (strcmp(rawValue, "(empty)") == 0)
			rawValue = ",";
		*delimiter = pstrdup(rawValue);

		rawValue = PQgetvalue(result, 0, 1);
		if (strcmp(rawValue, "(empty)") == 0)
			rawValue = "\"";
		*quote = pstrdup(rawValue);

		rawValue = PQgetvalue(result, 0, 2);
		if (strcmp(rawValue, "(empty)") == 0)
			rawValue = *quote;	/* use same value for quote as escape if
								 * unprovided */
		*escape = pstrdup(rawValue);

		*header = strcasecmp(PQgetvalue(result, 0, 3), "t") == 0;

		rawValue = PQgetvalue(result, 0, 4);
		if (strcmp(rawValue, "") == 0)
			rawValue = "\\n";

		*newLine = pstrdup(rawValue);

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
