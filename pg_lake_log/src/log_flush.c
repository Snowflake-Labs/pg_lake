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
 * Batched write of LogEntry rows into an Iceberg table via the CSV pipeline.
 *
 * InsertBatchToIceberg writes log entries directly to a local temp CSV file
 * using plain C I/O — no SQL parsing, no executor, no VALUES query.
 * The CSV matches the format produced by InternalCSVOptions (header=true,
 * delimiter=',', quote='"', escape='"').
 *
 * The CSV file is then fed through the same pipeline as
 * "COPY iceberg_table FROM 'file.csv'":
 *
 *   1. PrepareCSVInsertion sends the file to pgduck_server via DuckDB's
 *      read_csv(), which converts it to Parquet in object storage.
 *
 *   2. ApplyDataFileModifications registers the new Parquet file and
 *      commits an Iceberg snapshot.
 *
 * The temp file is registered for cleanup at transaction end.
 */
#include "postgres.h"

#include "access/table.h"
#include "catalog/namespace.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/rel.h"
#include "utils/timestamp.h"

#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/storage/local_storage.h"

#include "pg_lake_log/log_buffer.h"
#include "pg_lake_log/log_flush.h"

#define LOG_TEMP_FILE_PATTERN "pg_lake_log_XXXXXX"

/* Forward declarations */
static const char *ELevelToString(int elevel);
static int	WriteCSVFile(const char *path, LogEntry *entries, int count);
static void AppendCSVQuoted(StringInfo buf, const char *str);


/*
 * InsertBatchToIceberg writes entries[0..count-1] to the named Iceberg table.
 *
 * Caller must be inside an open transaction with an active snapshot.
 */
void
InsertBatchToIceberg(LogEntry *entries, int count, const char *table_name)
{
	if (count == 0)
		return;

	/* Resolve the target table OID. */
	List	   *names = stringToQualifiedNameList(table_name, NULL);
	RangeVar   *rv = makeRangeVarFromNameList(names);
	Oid			relationId = RangeVarGetRelid(rv, RowExclusiveLock, false);

	/* ---- Step 1: write a local CSV file directly from C ---- */

	char	   *csvPath = GenerateTempFileName(LOG_TEMP_FILE_PATTERN, true);
	int			maxLineSize = WriteCSVFile(csvPath, entries, count);

	/* ---- Step 2: convert CSV → Parquet via DuckDB ---- */

	DataFileSchema *schema = GetDataFileSchemaForTable(relationId);
	List	   *modifications = PrepareCSVInsertion(relationId, csvPath,
												   count, 0,
												   maxLineSize, schema);

	/* ---- Step 3: commit the new Parquet file to Iceberg metadata ---- */

	Relation	rel = table_open(relationId, NoLock);

	ApplyDataFileModifications(rel, modifications);

	table_close(rel, NoLock);
}


/*
 * WriteCSVFile writes entries to path in the InternalCSVOptions format:
 *
 *   - Header row
 *   - Comma delimiter
 *   - Double-quote quoting / escaping (RFC 4180)
 *   - Empty unquoted field for NULL
 *
 * Returns the maximum line size (bytes before the newline) for DuckDB's
 * read_csv max_line_size parameter.
 */
static int
WriteCSVFile(const char *path, LogEntry *entries, int count)
{
	FILE	   *fp;
	StringInfoData line;
	int			maxLineSize = 0;

	fp = fopen(path, "w");
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\" for writing: %m", path)));

	initStringInfo(&line);

	/* header row */
	fprintf(fp, "log_time,pid,severity,message,detail,context\n");

	for (int i = 0; i < count; i++)
	{
		LogEntry   *e = &entries[i];

		resetStringInfo(&line);

		/* log_time — use PostgreSQL's own output function for correct format */
		char	   *tsStr = DatumGetCString(
									DirectFunctionCall1(timestamptz_out,
														TimestampTzGetDatum(e->log_time)));

		AppendCSVQuoted(&line, tsStr);
		pfree(tsStr);

		/* pid — integer, no quoting needed */
		appendStringInfo(&line, ",%d", e->pid);

		/* severity — always non-NULL */
		appendStringInfoChar(&line, ',');
		AppendCSVQuoted(&line, ELevelToString(e->elevel));

		/* message — always non-NULL */
		appendStringInfoChar(&line, ',');
		AppendCSVQuoted(&line, e->message);

		/* detail — empty unquoted field for NULL */
		appendStringInfoChar(&line, ',');
		if (e->detail[0] != '\0')
			AppendCSVQuoted(&line, e->detail);

		/* context — empty unquoted field for NULL */
		appendStringInfoChar(&line, ',');
		if (e->context[0] != '\0')
			AppendCSVQuoted(&line, e->context);

		if (line.len > maxLineSize)
			maxLineSize = line.len;

		fprintf(fp, "%s\n", line.data);
	}

	pfree(line.data);

	if (fclose(fp) != 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", path)));

	return maxLineSize;
}


/*
 * AppendCSVQuoted appends a double-quoted CSV field to buf.
 * Internal double-quote characters are escaped by doubling (RFC 4180).
 */
static void
AppendCSVQuoted(StringInfo buf, const char *str)
{
	appendStringInfoChar(buf, '"');

	for (const char *p = str; *p != '\0'; p++)
	{
		if (*p == '"')
			appendStringInfoChar(buf, '"');
		appendStringInfoChar(buf, *p);
	}

	appendStringInfoChar(buf, '"');
}


/*
 * ELevelToString maps a PostgreSQL error level integer to its name string.
 */
static const char *
ELevelToString(int elevel)
{
	switch (elevel)
	{
		case DEBUG5:
			return "DEBUG5";
		case DEBUG4:
			return "DEBUG4";
		case DEBUG3:
			return "DEBUG3";
		case DEBUG2:
			return "DEBUG2";
		case DEBUG1:
			return "DEBUG1";
		case LOG:
			return "LOG";
		case INFO:
			return "INFO";
		case NOTICE:
			return "NOTICE";
		case WARNING:
			return "WARNING";
		case ERROR:
			return "ERROR";
		case FATAL:
			return "FATAL";
		case PANIC:
			return "PANIC";
		default:
			return "UNKNOWN";
	}
}
