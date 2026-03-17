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
 * The functions in this file deals with running queries on duckdb.
 *
 * We rely on duckdb APIs for executing the queries. The
 * main goal is to execute queries on duckdb and
 * return the results back to PG-wire compatible format.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */

/* need PRIu64 to print idx_t across platforms */
#define __STDC_FORMAT_MACROS
#include <inttypes.h>

#include "c.h"

#include <stdio.h>
#include <stdlib.h>

#include "access/attnum.h"
#include "mb/pg_wchar.h"
#include "lib/stringinfo.h"
#include "catalog/pg_type_d.h"
#include "common/fe_memutils.h"
#include "fe_utils/string_utils.h"

#include "duckdb.h"
#include "duckdb/duckdb.h"
#include "duckdb/type_conversion.h"
#include "duckdb/duckdb_pglake.h"
#include "pgsession/pgsession.h"
#include "pgsession/pgsession_io.h"
#include "pgsession/pqformat.h"
#include "utils/pgduck_log_utils.h"
#include "utils/numutils.h"
#include "utils/pg_log_utils.h"

#define PG_WIRE_TEXT_FORMAT (0)
#define PG_WIRE_BINARY_FORMAT (1)
#define VECTOR_VALUE(Data,Type,RowIndex) ((Type *) Data)[row]

#define COMPLETION_TAG_BUFSIZE  (64)

/*
 * Start a new transmit message after the previous one reaches 1MB
 * to do batching, but also prevent large allocations.
 */
#define TRANSMIT_MESSAGE_SIZE_THRESHOLD (1024*1024)

/* format options */
#define TRANSMIT_NULL_STRING "\\N"
#define TRANSMIT_END_OF_FILE_STRING "\\."
#define TRANSMIT_COLUMN_SEPARATOR_CHAR ','
#define TRANSMIT_ESCAPE_CHAR '"'
#define TRANSMIT_QUOTE_CHAR '"'
#define TRANSMIT_END_OF_LINE_CHAR '\n'

#define IS_FATAL_DUCKDB_ERROR(error) \
	((error) == DUCKDB_ERROR_FATAL || \
	 (error) == DUCKDB_ERROR_INTERNAL || \
	 (oom_is_fatal && (error) == DUCKDB_ERROR_OUT_OF_MEMORY))

typedef struct DuckDBResultColumn
{
	duckdb_type duckType;
	duckdb_logical_type logicalType;
}			DuckDBResultColumn;

typedef struct DuckDBQueryResult
{
	StringInfoData buf;
	DuckDBResultColumn *resultColumns;
	idx_t		columnCount;
	idx_t		chunkCount;

	duckdb_result *duckResult;
}			DuckDBQueryResult;

typedef duckdb_state(*QueryResultCallback) (duckdb_result * duckResult, void *arg);

/* global instance of DuckDB that is shared across threads */
duckdb_database DuckDB;
static bool IsDuckDBInitialized = false;


static DuckDBStatus print_duckdb_version(void);
static DuckDBStatus ensure_jsonb_cast_created(void);
static duckdb_state run_command_on_duckdb(const char *command);
static duckdb_state run_command_on_duckdb_with_callback(const char *command,
														QueryResultCallback callback,
														void *callbackArg);
static DuckDBStatus return_query_result_to_pgsession(DuckDBSession * duckSession,
													 duckdb_result duckResult,
													 ResponseFormat * responseFormat,
													 char **errorMessage);
static duckdb_state process_single_result_as_text(duckdb_result * result,
												  void *callbackArg);
static DuckDBStatus return_command_completion_pgsession(DuckDBSession * duckSession,
														duckdb_result duckResult);
static void duckdb_query_result_init(DuckDBQueryResult * duckdb_query_result,
									 duckdb_result * duckResult);
static DuckDBStatus duckdb_query_result_send_column_metadata(DuckDBQueryResult * duckdb_query_result,
															 PGSession * clientSession,
															 ResponseFormat * responseFormat);
static DuckDBStatus process_and_send_data_chunks(DuckDBQueryResult * duckdb_query_result,
												 PGSession * clientSession,
												 ResponseFormat * responseFormat,
												 idx_t * rowsReturned,
												 char **errorMessage);
static DuckDBStatus process_data_chunk(duckdb_data_chunk chunk, StringInfoData *buf,
									   PGSession * clientSession,
									   DuckDBResultColumn * resultColumns,
									   idx_t columnCount,
									   ResponseFormat * responseFormat);
static void create_result_column_state(DuckDBResultColumn * resultColumn,
									   duckdb_data_chunk chunk,
									   idx_t columnCount);
static void destroy_result_column_state(DuckDBResultColumn * resultColumns, idx_t columnCount);
static void duckdb_query_result_destroy(DuckDBQueryResult * duckdb_query_result);
static DuckDBStatus duckdb_vector_to_pg_wire(duckdb_vector vector, duckdb_type duckType,
											 duckdb_logical_type logicalType,
											 int row,
											 ResponseFormat * responseFormat,
											 StringInfo output);
static bool is_set_command(const char *command);
static void append_escaped_csv(StringInfo buffer, char *value);
static void append_completion_tag(char *buffer, duckdb_result duckResult, idx_t rowsReturned);
static const char *command_tag(duckdb_statement_type statementType);



/*
 * duckdb_global_init initializes the DuckDB database, which can be used
 * by different threads simulteously.
 */
DuckDBStatus
duckdb_global_init(char *databaseFilePath,
				   char *cacheDir,
				   char *extensionsDir,
				   bool allowExtensionInstall,
				   char *memoryLimit,
				   int64_t cacheOnWriteMaxSize,
				   char *initFilePath)
{
	if (IsDuckDBInitialized)
	{
		return DUCKDB_SUCCESS;
	}

	duckdb_config duckConfig;

	if (duckdb_create_config(&duckConfig) == DuckDBError)
	{
		PGDUCK_SERVER_ERROR("could not create DuckDB configuration");
		return DUCKDB_INITIALIZATION_ERROR;
	}

	/* always follow Postgres' sort order */
	duckdb_set_config(duckConfig, "default_null_order", "postgres");

	/* we still auto-load already installed extensions */
	duckdb_set_config(duckConfig, "autoload_known_extensions", "true");

	/*
	 * We allow unsigned extensions only if extension installation is
	 * disabled.
	 */
	if (allowExtensionInstall)
	{
		/* do not allow installing unsigned extensions */
		duckdb_set_config(duckConfig, "allow_unsigned_extensions", "false");

		/* auto-install extensions (mainly delta) */
		duckdb_set_config(duckConfig, "autoinstall_known_extensions", "true");
	}
	else
	{
		/*
		 * If we do not allow installing extensions at runtime, it means we
		 * can only use already-installed extensions in the
		 * extension_directory. We allow them to be unsigned in that case,
		 * since we'll build them ourselves.
		 */
		duckdb_set_config(duckConfig, "allow_unsigned_extensions", "true");

		/* do not allow automatic extension installation */
		duckdb_set_config(duckConfig, "autoinstall_known_extensions", "false");

		/* disable manual INSTALL as well by setting a non-existent repo */
		duckdb_set_config(duckConfig, "custom_extension_repository", "disabled");
	}

	if (extensionsDir != NULL)
		duckdb_set_config(duckConfig, "extension_directory", extensionsDir);

	char	   *duckError = NULL;

	/* disable the file cache; we have our own */
	duckdb_set_config(duckConfig, "enable_external_file_cache", "false");


	if (duckdb_open_ext(databaseFilePath, &DuckDB, duckConfig, &duckError) == DuckDBError)
	{
		if (duckError != NULL)
		{
			PGDUCK_SERVER_ERROR("error initialization DuckDB: %s", duckError);
			duckdb_free(duckError);
		}

		duckdb_destroy_config(&duckConfig);
		duckdb_close(&DuckDB);
		return DUCKDB_INITIALIZATION_ERROR;
	}

	duckdb_destroy_config(&duckConfig);

	if (print_duckdb_version() != DUCKDB_SUCCESS)
		return DUCKDB_INITIALIZATION_ERROR;

	/*
	 * We assume these modules are already installed and loading them verifies
	 * that.
	 */
	if (run_command_on_duckdb("LOAD parquet") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	if (run_command_on_duckdb("LOAD httpfs") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	if (run_command_on_duckdb("LOAD json") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	/* create jsonb alias */
	ensure_jsonb_cast_created();

	if (run_command_on_duckdb("LOAD icu") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	/*
	 * aws is not a built-in module
	 */
	if (allowExtensionInstall && run_command_on_duckdb("INSTALL aws") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	if (run_command_on_duckdb("LOAD aws") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	/*
	 * spatial is not a built-in module and does not work to be autoinstalled
	 */
	if (allowExtensionInstall && run_command_on_duckdb("INSTALL spatial") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	if (run_command_on_duckdb("LOAD spatial") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

#if PG_LAKE_DELTA_SUPPORT == 1

	/*
	 * Load delta explicitly on start-up if extension install is disabled. We
	 * cannot install it later so it must be there already.
	 */
	if (!allowExtensionInstall && run_command_on_duckdb("LOAD delta") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;
#endif

	/*
	 * Load function aliases/macros for pushdown. XXX If we get more than a
	 * couple, turn into a lookup table?
	 */

	/* This handles case-insensitive match based on third arg */
	if (run_command_on_duckdb("CREATE OR REPLACE MACRO lake_regexp_matches(a,b,c) "
							  "AS CASE WHEN c THEN regexp_matches(a,b,'i') "
							  "ELSE regexp_matches(a,b) END") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;


	/* This handles generate_series(int, int) */
	if (run_command_on_duckdb("CREATE OR REPLACE MACRO generate_series_int "
							  "	(start, stop, step := 1) "
							  "  AS TABLE "
							  "	SELECT CAST(x AS INT) AS generate_series "
							  "	FROM   UNNEST(generate_series(start, stop, step)) AS t(x);") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	/* This handles generate_series(int, int, step) */
	if (run_command_on_duckdb("CREATE OR REPLACE MACRO generate_series_int_step(start, stop, step) "
							  "AS TABLE "
							  "SELECT CAST(x AS INT) AS generate_series "
							  "FROM   UNNEST(generate_series(start, stop, step)) AS t(x);") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	/*
	 * Add a default S3 secret from the AWS credentials chain using the
	 * default region in ~/.aws/confi.g.
	 */
	const char *createS3Secret =
		"CREATE SECRET s3default ("
		"TYPE S3, PROVIDER CREDENTIAL_CHAIN, "
		"VALIDATION 'none', "
		"ENDPOINT 's3.amazonaws.com'"
		")";

	if (run_command_on_duckdb(createS3Secret) == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	const char *createGCSSecret =
		"CREATE SECRET gcsdefault ("
		"TYPE GCS, PROVIDER CREDENTIAL_CHAIN, "
		"VALIDATION 'none', "
		"ENDPOINT 'storage.googleapis.com'"
		")";

	if (run_command_on_duckdb(createGCSSecret) == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	char		setCommand[1024];

	if (cacheDir != NULL)
	{
		if (snprintf(setCommand, 1024, "SET GLOBAL pg_lake_cache_dir TO '%s'", cacheDir) < 0)
		{
			PGDUCK_SERVER_ERROR("pg_lake_cache_dir value is too long");
			return DUCKDB_INITIALIZATION_ERROR;
		}

		if (run_command_on_duckdb(setCommand) == DuckDBError)
			return DUCKDB_INITIALIZATION_ERROR;
	}

	{
		if (snprintf(setCommand, 1024, "SET GLOBAL pg_lake_cache_on_write_max_size TO '%" PRIu64 "'", cacheOnWriteMaxSize) < 0)
		{
			PGDUCK_SERVER_ERROR("pg_lake_cache_on_write_max_size value is too long");
			return DUCKDB_INITIALIZATION_ERROR;
		}

		if (run_command_on_duckdb(setCommand) == DuckDBError)
			return DUCKDB_INITIALIZATION_ERROR;
	}

	{
		if (snprintf(setCommand, 1024, "SET GLOBAL enable_geoparquet_conversion TO 'false'") < 0)
		{
			return DUCKDB_INITIALIZATION_ERROR;
		}

		if (run_command_on_duckdb(setCommand) == DuckDBError)
			return DUCKDB_INITIALIZATION_ERROR;
	}

	{
		if (snprintf(setCommand, 1024, "SET GLOBAL enable_object_cache TO true") < 0)
		{
			return DUCKDB_INITIALIZATION_ERROR;
		}

		if (run_command_on_duckdb(setCommand) == DuckDBError)
			return DUCKDB_INITIALIZATION_ERROR;
	}

	{
		if (snprintf(setCommand, 1024, "SET GLOBAL preserve_insertion_order TO false") < 0)
		{
			return DUCKDB_INITIALIZATION_ERROR;
		}

		if (run_command_on_duckdb(setCommand) == DuckDBError)
			return DUCKDB_INITIALIZATION_ERROR;
	}

	if (memoryLimit != NULL)
	{
		if (snprintf(setCommand, 1024, "SET GLOBAL memory_limit TO '%s'", memoryLimit) < 0)
		{
			PGDUCK_SERVER_ERROR("memory_limit value is too long");
			return DUCKDB_INITIALIZATION_ERROR;
		}

		if (run_command_on_duckdb(setCommand) == DuckDBError)
			return DUCKDB_INITIALIZATION_ERROR;
	}

	if (initFilePath != NULL)
	{
		FILE	   *initFile = fopen(initFilePath, "r");

		if (initFile == NULL)
		{
			PGDUCK_SERVER_ERROR("could not open init file '%s': %s", initFilePath, strerror(errno));
			return DUCKDB_INITIALIZATION_ERROR;
		}

		fseek(initFile, 0, SEEK_END);
		long		fileSize = ftell(initFile);

		rewind(initFile);

		char	   *commands = palloc(fileSize + 1);
		int64_t		bytesRead = fread(commands, 1, fileSize, initFile);

		if (bytesRead < fileSize)
		{
			fclose(initFile);
			pg_free(commands);
			PGDUCK_SERVER_ERROR("could not read init file '%s': %s", initFilePath, strerror(errno));
			return DUCKDB_INITIALIZATION_ERROR;
		}

		fclose(initFile);

		/* NUL-terminate the string */
		commands[fileSize] = '\0';

		if (run_command_on_duckdb(commands) == DuckDBError)
		{
			pg_free(commands);
			return DUCKDB_INITIALIZATION_ERROR;
		}

		pg_free(commands);
	}

	duckdb_pglake_set_output_verbose(IsOutputVerbose);

	IsDuckDBInitialized = true;

	return DUCKDB_SUCCESS;
}


/*
 * print_duckdb_version gets the version of DuckDB via SQL API to be
 * able to print it, and simultaneously do a sanity check of DuckDB.
 */
static DuckDBStatus
print_duckdb_version(void)
{
	char		buffer[STACK_ALLOCATED_OUTPUT_BUFFER_SIZE];
	TextOutputBuffer versionBuffer = {
		.buffer = buffer,
		.needsFree = false
	};

	if (run_command_on_duckdb_with_callback("SELECT version()",
											process_single_result_as_text,
											&versionBuffer) == DuckDBError)
	{
		return DUCKDB_INITIALIZATION_ERROR;
	}

	PGDUCK_SERVER_LOG("using DuckDB version: %s",
					  versionBuffer.buffer != NULL ? versionBuffer.buffer : "<none>");

	if (versionBuffer.needsFree)
	{
		/*
		 * probably never happens because version strings are short, but we
		 * don't control them so let's be defensive.
		 */
		pg_free(versionBuffer.buffer);
	}

	return DUCKDB_SUCCESS;
}


/*
* ensure_jsonb_cast_created creates JSONB cast exist in DuckDB. Note that DuckDB
* does not have JSONB type, but it has JSON type. So, we send queries where val::jsonb
* casts are replaced with jsonb(val); And, the macro we create on DuckDB ensures that
* the value can be casted (e.g., verified to be) a JSON(B) value.
*/
static DuckDBStatus
ensure_jsonb_cast_created(void)
{
	if (run_command_on_duckdb("CREATE OR REPLACE MACRO jsonb(txt) AS (CAST(txt AS JSON))") == DuckDBError)
		return DUCKDB_INITIALIZATION_ERROR;

	return DUCKDB_SUCCESS;
}


/*
 * A helper function for connecting to the global DuckDB,
 * and executing the command. The function discards the
 * results of the command, only returns duckdb_state.
 *
 * The callers are responsible for handling DuckDBError.
 */
static duckdb_state
run_command_on_duckdb(const char *command)
{
	return run_command_on_duckdb_with_callback(command, NULL, NULL);
}


/*
 * run_command_on_duckdb_with_string_result runs a command on DuckDB which
 * returns a string and sets the result.
 */
static duckdb_state
run_command_on_duckdb_with_callback(const char *command,
									QueryResultCallback callback,
									void *callbackArg)
{
	PGDUCK_SERVER_DEBUG("executing command on DuckDB: %s", command);

	duckdb_connection connection;

	if (duckdb_connect(DuckDB, &connection) == DuckDBError)
	{
		PGDUCK_SERVER_ERROR("could not connect to DuckDB while running "
							"command %s", command);
		return DuckDBError;
	}

	duckdb_result duckResult;
	duckdb_state duckState =
		duckdb_query(connection, command, &duckResult);

	if (duckState == DuckDBError)
	{
		const char *errorMessage = duckdb_result_error(&duckResult);

		PGDUCK_SERVER_ERROR("command failed with %s", errorMessage);
		duckdb_destroy_result(&duckResult);
		duckdb_disconnect(&connection);
		return DuckDBError;
	}

	if (callback != NULL)
	{
		duckState = callback(&duckResult, callbackArg);
	}

	duckdb_destroy_result(&duckResult);
	duckdb_disconnect(&connection);

	return duckState;
}


/*
 * process_single_result_as_text processes the result of a DuckDB query that
 * returns a single value and returns it as a string.
 */
static duckdb_state
process_single_result_as_text(duckdb_result * duckResult, void *callbackArg)
{
	TextOutputBuffer *toTextBuffer = (TextOutputBuffer *) callbackArg;

	idx_t		columnCount = duckdb_column_count(duckResult);

	if (columnCount != 1)
	{
		PGDUCK_SERVER_ERROR("command returned %" PRIu64 "columns, expected 1",
							columnCount);
		return DuckDBError;
	}

	idx_t		chunkCount = duckdb_result_chunk_count(*duckResult);

	if (chunkCount != 1)
	{
		PGDUCK_SERVER_ERROR("command returned %" PRIu64 "chunks, expected 1",
							chunkCount);
		return DuckDBError;
	}

	idx_t		chunkIndex = 0;
	duckdb_data_chunk chunk = duckdb_result_get_chunk(*duckResult, chunkIndex);

	idx_t		chunkSize = duckdb_data_chunk_get_size(chunk);

	if (chunkSize != 1)
	{
		PGDUCK_SERVER_ERROR("command returned %" PRIu64 " rows, expected 1",
							chunkSize);
		duckdb_destroy_data_chunk(&chunk);
		return DuckDBError;
	}

	idx_t		columnIndex = 0;
	duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, columnIndex);
	uint64_t   *validity = duckdb_vector_get_validity(vector);

	idx_t		rowInChunk = 0;

	if (validity != NULL && !duckdb_validity_row_is_valid(validity, rowInChunk))
	{
		toTextBuffer->buffer = NULL;
		duckdb_destroy_data_chunk(&chunk);
		return DuckDBSuccess;
	}

	duckdb_type duckType = duckdb_column_type(duckResult, columnIndex);
	duckdb_logical_type logicalType = duckdb_vector_get_column_type(vector);
	DuckDBTypeInfo *typeMap = find_duck_type_info(duckType);

	typeMap->to_text(vector, logicalType, rowInChunk, toTextBuffer);

	duckdb_destroy_logical_type(&logicalType);
	duckdb_destroy_data_chunk(&chunk);

	return DuckDBSuccess;
}


/*
 * duckdb_session_init initializes a DuckDB session for a specific
 * PostgreSQL client session.
 */
DuckDBStatus
duckdb_session_init(DuckDBSession * duckSession, PGSession * clientSession)
{
	if (!IsDuckDBInitialized)
	{
		PGDUCK_SERVER_ERROR("unexpected state while initializing DuckDB session: "
							"DuckDB is not yet initialized");
		return DUCKDB_SESSION_INITIALIZATION_ERROR;
	}

	if (duckdb_connect(DuckDB, &duckSession->connection) == DuckDBError)
	{
		PGDUCK_SERVER_ERROR("could not connect to DuckDB");
		return DUCKDB_SESSION_INITIALIZATION_ERROR;
	}

	duckdb_pglake_init_connection(duckSession->connection,
								  clientSession->pgClient->clientSocket);

	duckSession->clientSession = clientSession;

	return DUCKDB_SUCCESS;
}


/*
 * duckdb_session_destroy terminates a DuckDB session.
 *
 * We currently do not call duckdb_session_destroy_prepare because it
 * is done separately by pgsession.
 */
void
duckdb_session_destroy(DuckDBSession * duckSession)
{
	duckdb_disconnect(&duckSession->connection);
}


/*
 * duckdb_vector_to_pg_wire writes a specific value from a DuckDB query
 * result to the output buffer in PG wire text format.
 */
static DuckDBStatus
duckdb_vector_to_pg_wire(duckdb_vector vector, duckdb_type duckType,
						 duckdb_logical_type logicalType,
						 int row,
						 ResponseFormat * responseFormat,
						 StringInfo output)
{
	DuckDBTypeInfo *typeMap = find_duck_type_info(duckType);

	if (typeMap == NULL || typeMap->to_text == NULL)
	{
		PGDUCK_SERVER_ERROR("could not find type mapping for DuckDB type: %d", duckType);
		return DUCKDB_TYPE_CONVERSION_ERROR;
	}

	char		buffer[STACK_ALLOCATED_OUTPUT_BUFFER_SIZE];
	TextOutputBuffer toTextBuffer = {
		.buffer = buffer,
		.needsFree = false
	};

	/* main conversion happens, toTextBuffer->buffer is filled */
	DuckDBStatus status = typeMap->to_text(vector, logicalType, row, &toTextBuffer);

	if (status != DUCKDB_SUCCESS)
		return status;

	if (responseFormat->isTransmit)
	{
		append_escaped_csv(output, toTextBuffer.buffer);
	}
	else
	{
		pq_sendcountedtext(output, toTextBuffer.buffer, strlen(toTextBuffer.buffer),
						   false);
	}

	if (toTextBuffer.needsFree)
		pg_free(toTextBuffer.buffer);

	return DUCKDB_SUCCESS;
}


/*
 * Main entry point for running a query on duckdb on a given DuckDBSession.
 * On success, it returns the results back to the pg client session.
 * On failure, it sets the errorMessage, and returns the appropriate DuckDBStatus.
 */
DuckDBStatus
duckdb_session_run_command(DuckDBSession * duckSession, const char *queryString,
						   ResponseFormat * responseFormat, char **errorMessage)
{
	duckdb_result duckResult;
	duckdb_state duckState = duckdb_query(duckSession->connection, queryString, &duckResult);
	DuckDBStatus status = DUCKDB_INVALID;

	if (duckState == DuckDBError)
	{
		duckdb_error_type duckdbErrorType = duckdb_result_error_type(&duckResult);
		const char *duckdbError = duckdb_result_error(&duckResult);

		if (is_set_command(queryString))
		{
			/*
			 * we ignore invalid SET commands to appease various postgres
			 * clients
			 */
			PGDUCK_SERVER_WARN("ignoring failure in %s: %s", queryString, duckdbError);

			status = return_command_completion_pgsession(duckSession, duckResult);

			duckdb_destroy_result(&duckResult);

			return status;
		}

		if (IS_FATAL_DUCKDB_ERROR(duckdbErrorType))
		{
			/*
			 * Shutdown the server -- will be restarted by systemd.  If we
			 * have a FATAL error returned by DuckDB, this is permanently in a
			 * state which we cannot recover from without restarting the whole
			 * server.
			 */

			PGDUCK_SERVER_ERROR("unrecoverable failure from duckdb; terminating: %s", duckdbError);
			status = DUCKDB_FATAL_ERROR;
		}
		else
		{
			PGDUCK_SERVER_WARN("query on duckdb failed: %s", duckdbError);
			status = DUCKDB_QUERY_ERROR;
		}

		if (errorMessage != NULL)
			*errorMessage = pstrdup(duckdbError);

		duckdb_destroy_result(&duckResult);

		return status;
	}

	switch (duckdb_result_return_type(duckResult))
	{
		case DUCKDB_RESULT_TYPE_QUERY_RESULT:
			status = return_query_result_to_pgsession(duckSession, duckResult,
													  responseFormat,
													  errorMessage);
			break;
		case DUCKDB_RESULT_TYPE_NOTHING:
		case DUCKDB_RESULT_TYPE_CHANGED_ROWS:
			status = return_command_completion_pgsession(duckSession, duckResult);
			break;
		default:
			if (errorMessage != NULL)
				*errorMessage = pstrdup("unknown return type");
			status = DUCKDB_QUERY_ERROR;
			break;
	}

	duckdb_destroy_result(&duckResult);

	return status;
}


/*
 * duckdb_session_prepare prepares the prepared statement of the DuckDB session
 *
 * In case of error, DUCKDB_QUERY_ERROR is returned and the error message is set.
 */
DuckDBStatus
duckdb_session_prepare(DuckDBSession * duckSession,
					   const char *queryString,
					   char **errorMessage)
{
	duckdb_state duckDBStatus = duckdb_prepare(duckSession->connection,
											   queryString,
											   &duckSession->duckPreparedStatement);

	if (duckDBStatus != DuckDBSuccess)
	{
		const char *duckdbError =
			duckdb_prepare_error(duckSession->duckPreparedStatement);

		/*
		 * The duckdb_prepare_error message survives this functions, but for
		 * consistency with duckdb_session_run_command we copy it anyway.
		 */
		if (errorMessage != NULL)
			*errorMessage = pstrdup(duckdbError);

		return DUCKDB_QUERY_ERROR;
	}

	return DUCKDB_SUCCESS;
}


/*
 * duckdb_session_destroy_prepare destroys the prepared statement of
 * duckSession.
 */
void
duckdb_session_destroy_prepare(DuckDBSession * duckSession)
{
	duckdb_destroy_prepare(&duckSession->duckPreparedStatement);
}


/*
 * duckdb_session_prepared_nparams returns the number of parameters of
 * the current prepared statement.
 */
int
duckdb_session_prepared_nparams(DuckDBSession * duckSession)
{
	return duckdb_nparams(duckSession->duckPreparedStatement);
}


/*
 * duckdb_session_bind_varchar sets the value of a parameter in the current
 * prepared statement.
 *
 * If binding fails, an error is returned.
 */
DuckDBStatus
duckdb_session_bind_varchar(DuckDBSession * duckSession,
							int paramNumber, const char *value,
							char **errorMessage)
{
	duckdb_state bindResult;

	if (value == NULL)
	{
		bindResult = duckdb_bind_null(duckSession->duckPreparedStatement, paramNumber);
	}
	else
	{
		bindResult = duckdb_bind_varchar(duckSession->duckPreparedStatement,
										 paramNumber, value);
	}

	if (bindResult != DuckDBSuccess)
	{
		const char *duckdbError =
			duckdb_prepare_error(duckSession->duckPreparedStatement);

		if (duckdbError == NULL)
			duckdbError = "failed to bind parameter";

		/*
		 * The duckdb_prepare_error message survives this functions, but for
		 * consistency with duckdb_session_run_command we copy it anyway.
		 */
		if (errorMessage != NULL)
			*errorMessage = pstrdup(duckdbError);

		PGDUCK_SERVER_WARN("could not bind prepared statement: %s", duckdbError);

		return DUCKDB_QUERY_ERROR;
	}

	return DUCKDB_SUCCESS;
}


/*
* duckdb_session_execute_prepared executes a prepared statement on the DuckDB session.
* On success, it returns the results back to the pg client session.
* On failure, returns the appropriate DuckDBStatus.
*/
DuckDBStatus
duckdb_session_execute_prepared(DuckDBSession * duckSession,
								ResponseFormat * responseFormat,
								char **errorMessage)
{
	duckdb_result duckResult;
	duckdb_state duckDBPreparedResult =
		duckdb_execute_prepared_streaming(duckSession->duckPreparedStatement, &duckResult);

	if (duckDBPreparedResult != DuckDBSuccess)
	{
		DuckDBStatus status;
		duckdb_error_type duckdbErrorType = duckdb_result_error_type(&duckResult);
		const char *duckdbError = duckdb_result_error(&duckResult);

		if (IS_FATAL_DUCKDB_ERROR(duckdbErrorType))
		{
			/*
			 * Shutdown the server -- will be restarted by systemd.  If we
			 * have a FATAL error returned by DuckDB, this is permanently in a
			 * state which we cannot recover from without restarting the whole
			 * server.
			 */

			PGDUCK_SERVER_ERROR("unrecoverable failure from duckdb; terminating: %s", duckdbError);
			status = DUCKDB_FATAL_ERROR;
		}
		else
		{
			PGDUCK_SERVER_WARN("could not execute prepared statement: %s", duckdbError);
			status = DUCKDB_QUERY_ERROR;
		}

		if (errorMessage != NULL)
			*errorMessage = pstrdup(duckdbError);


		duckdb_destroy_result(&duckResult);
		return status;
	}

	DuckDBStatus duckReturnResult =
		return_query_result_to_pgsession(duckSession, duckResult, responseFormat,
										 errorMessage);

	duckdb_destroy_result(&duckResult);

	return duckReturnResult;
}

/*
 * return_query_result_to_pgsession is the main entry point for returning query
 * results, like SELECT.
 */
static DuckDBStatus
return_query_result_to_pgsession(DuckDBSession * duckSession, duckdb_result duckResult,
								 ResponseFormat * responseFormat, char **errorMessage)
{
	DuckDBQueryResult duckdb_query_result;

	duckdb_query_result_init(&duckdb_query_result, &duckResult);


	DuckDBStatus sendMetadataResult =
		duckdb_query_result_send_column_metadata(&duckdb_query_result,
												 duckSession->clientSession,
												 responseFormat);

	if (sendMetadataResult != DUCKDB_SUCCESS)
	{
		duckdb_query_result_destroy(&duckdb_query_result);

		/* error message is already logged */
		return sendMetadataResult;
	}

	idx_t		rowsReturned = 0;

	/*
	 * DuckDB stores query results in a columnar format, where each column of
	 * data is organized into 'chunks'. Each chunk contains a fixed number of
	 * rows (2048) allowing efficient data processing in a column-wise manner.
	 *
	 * This design enhances performance for analytical queries as operations
	 * can be vectorized and executed on entire columns simultaneously.
	 *
	 * The `process_and_send_data_chunks` function iterates over each chunk in
	 * the DuckDB result set. For each chunk, it retrieves the rows and sends
	 * them to the PostgreSQL client session. This process involves converting
	 * the DuckDB internal data representation (columnar, chunk-based) into a
	 * format suitable for PostgreSQL, which typically handles data in a
	 * row-wise manner.
	 *
	 * Within each chunk, the data is accessed by row indices. The function
	 * `process_data_chunk` handles the conversion of each row within a chunk
	 * into the PostgreSQL wire format. It accounts for NULL values and type
	 * conversions as necessary. This approach ensures that the columnar data
	 * in DuckDB chunks is accurately translated into the row-oriented format
	 * expected by PostgreSQL clients, maintaining data integrity and format
	 * compatibility.
	 *
	 * We think that there are some more efficient ways of approaching the
	 * problem, see Old repo issues number 42
	 */
	DuckDBStatus processChunkStatus =
		process_and_send_data_chunks(&duckdb_query_result,
									 duckSession->clientSession,
									 responseFormat,
									 &rowsReturned,
									 errorMessage);

	if (processChunkStatus != DUCKDB_SUCCESS)
	{
		duckdb_query_result_destroy(&duckdb_query_result);

		/* error message is already logged */
		return processChunkStatus;
	}

	if (responseFormat->isTransmit)
	{
		/* send CopyDone response */
		if (!IsOK(pgsession_putemptymessage(duckSession->clientSession, 'c')))
		{
			PGDUCK_SERVER_ERROR("could not send CopyDone");

			return DUCKDB_PG_COMMUNICATION_ERROR;
		}
	}

	char		completionTag[COMPLETION_TAG_BUFSIZE];

	append_completion_tag(completionTag, duckResult, rowsReturned);

	if (!IsOK(pgsession_put_message(duckSession->clientSession, 'C', completionTag,
									strlen(completionTag) + 1)))
	{
		duckdb_query_result_destroy(&duckdb_query_result);

		PGDUCK_SERVER_ERROR("could not send command completion to the client");

		/* pq_flush failed */
		return DUCKDB_PG_COMMUNICATION_ERROR;
	}

	duckdb_query_result_destroy(&duckdb_query_result);

	return DUCKDB_SUCCESS;
}

/*
 * ReturnQueryCompletionPGSession is the main entry point for command completion
 * for SET/BEGIN/COMMIT etc.
 */
static DuckDBStatus
return_command_completion_pgsession(DuckDBSession * duckSession, duckdb_result duckResult)
{
	char		completionTag[COMPLETION_TAG_BUFSIZE];

	append_completion_tag(completionTag, duckResult, 0);

	if (!IsOK(pgsession_put_message(duckSession->clientSession, 'C', completionTag,
									strlen(completionTag) + 1)))
	{
		PGDUCK_SERVER_ERROR("could not send command completion to the client");

		/* pq_flush failed */
		return DUCKDB_PG_COMMUNICATION_ERROR;
	}

	return DUCKDB_SUCCESS;
}

/*
 * Initializes a StringInfo buffer and allocates memory for an array of DuckDBResultColumn.
 */
static void
duckdb_query_result_init(DuckDBQueryResult * duckdb_query_result, duckdb_result * duckResult)
{
	duckdb_query_result->columnCount = duckdb_column_count(duckResult);
	duckdb_query_result->chunkCount = duckdb_result_chunk_count(*duckResult);
	duckdb_query_result->duckResult = duckResult;

	initStringInfo(&duckdb_query_result->buf);
	enlargeStringInfo(&duckdb_query_result->buf,
					  (NAMEDATALEN * MAX_CONVERSION_GROWTH
					   + sizeof(Oid)
					   + sizeof(AttrNumber)
					   + sizeof(Oid)
					   + sizeof(int16)
					   + sizeof(int32)
					   + sizeof(int16)) * duckdb_query_result->columnCount);
	duckdb_query_result->resultColumns =
		palloc0(sizeof(DuckDBResultColumn) * duckdb_query_result->columnCount);
}

/*
 * Sends the metadata for each column in the result set to the client session.
 */
static DuckDBStatus
duckdb_query_result_send_column_metadata(DuckDBQueryResult * duckdb_query_result,
										 PGSession * clientSession,
										 ResponseFormat * responseFormat)
{
	StringInfoData *buf = &duckdb_query_result->buf;
	duckdb_result *duckResult = duckdb_query_result->duckResult;
	idx_t		columnCount = duckdb_query_result->columnCount;
	DuckDBResultColumn *resultColumns = duckdb_query_result->resultColumns;

	if (responseFormat->isTransmit)
	{
		/* send CopyOutResponse */
		pq_beginmessage_reuse(buf, 'H');
		pq_sendbyte(buf, PG_WIRE_TEXT_FORMAT);
	}
	else
	{
		/* send RowDescription */
		pq_beginmessage_reuse(buf, 'T');
	}

	pq_sendint16(buf, columnCount);

	for (idx_t columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		const char *columnName = duckdb_column_name(duckResult, columnIndex);
		duckdb_type duckType = duckdb_column_type(duckResult, columnIndex);
		DuckDBResultColumn *resultColumn = &resultColumns[columnIndex];

		Oid			originalTableId = 0;
		AttrNumber	originalColumnNumber = 0;
		DuckDBTypeInfo *typeInfo = find_duck_type_info(duckType);

		if (typeInfo == NULL)
		{
			PGDUCK_SERVER_ERROR("could not convert DuckDB type to text: %d", duckType);
			return DUCKDB_TYPE_CONVERSION_ERROR;
		}

		if (!responseFormat->isTransmit)
		{
			pq_writestring(buf, columnName);
			pq_writeint32(buf, originalTableId);
			pq_writeint16(buf, originalColumnNumber);

			/*
			 * We always send columnTypeId=InvalidOid, columnLength=-1,
			 * columnTypeMod=-1 see TypeInfo struct comment for the reasoning.
			 */
			pq_writeint32(buf, InvalidOid);
			pq_writeint16(buf, -1);
			pq_writeint32(buf, -1);
		}

		pq_writeint16(buf, PG_WIRE_TEXT_FORMAT);

		resultColumn->duckType = duckType;
	}

	if (!IsOK(pq_endmessage_reuse(clientSession, buf)))
	{
		PGDUCK_SERVER_ERROR("could not send column metadata to the client");

		/* pq_flush failed */
		return DUCKDB_PG_COMMUNICATION_ERROR;
	}

	return DUCKDB_SUCCESS;
}


/*
 * Processes each data chunk in the DuckDB result and sends it to the client session.
 */
static DuckDBStatus
process_and_send_data_chunks(DuckDBQueryResult * duckdb_query_result,
							 PGSession * clientSession,
							 ResponseFormat * responseFormat,
							 idx_t * rowsReturned,
							 char **errorMessage)
{
	StringInfoData *buf = &duckdb_query_result->buf;
	duckdb_result *duckResult = duckdb_query_result->duckResult;
	idx_t		columnCount = duckdb_query_result->columnCount;
	DuckDBResultColumn *resultColumns = duckdb_query_result->resultColumns;
	DuckDBStatus status;

	while (true)
	{
		duckdb_data_chunk chunk = duckdb_fetch_chunk(*duckResult);

		if (chunk == NULL)
		{

			/* an error may have been triggered during execution */
			const char *duckdbError = duckdb_result_error(duckResult);
			duckdb_error_type duckdbErrorType = duckdb_result_error_type(duckResult);

			if (duckdbError != NULL)
			{
				if (IS_FATAL_DUCKDB_ERROR(duckdbErrorType))
				{
					/*
					 * Shutdown the server -- will be restarted by systemd. If
					 * we have a FATAL error returned by DuckDB, this is
					 * permanently in a state which we cannot recover from
					 * without restarting the whole server.
					 */

					PGDUCK_SERVER_ERROR("unrecoverable failure from duckdb; terminating: %s", duckdbError);
					status = DUCKDB_FATAL_ERROR;
				}
				else
				{
					PGDUCK_SERVER_WARN("query on duckdb failed during execution: %s", duckdbError);
					status = DUCKDB_QUERY_ERROR;
				}

				*errorMessage = pstrdup(duckdbError);
				return status;
			}
			else
			{
				/* no more data remaining */
				break;
			}
		}

		create_result_column_state(resultColumns, chunk, columnCount);

		status =
			process_data_chunk(chunk, buf, clientSession, resultColumns, columnCount,
							   responseFormat);

		*rowsReturned += duckdb_data_chunk_get_size(chunk);

		destroy_result_column_state(resultColumns, columnCount);
		duckdb_destroy_data_chunk(&chunk);

		if (status != DUCKDB_SUCCESS)
			return status;
	}

	return DUCKDB_SUCCESS;
}


/*
 * Processes a single data chunk and sends the row data to the client session.
 *
 * It supports both regular query results through DataRow messages or batches
 * of query results in CSV format through CopyData messages.
 */
static DuckDBStatus
process_data_chunk(duckdb_data_chunk chunk, StringInfoData *buf, PGSession * clientSession,
				   DuckDBResultColumn * resultColumns, idx_t columnCount,
				   ResponseFormat * responseFormat)
{
	idx_t		chunkSize = duckdb_data_chunk_get_size(chunk);
	bool		isTransmit = responseFormat->isTransmit;

	if (isTransmit)
	{
		/*
		 * Start CopyData ('d') for all rows in the chunk, or when
		 * TRANSMIT_MESSAGE_SIZE_THRESHOLD is reached.
		 */
		pq_beginmessage_reuse(buf, 'd');
	}

	for (idx_t rowInChunk = 0; rowInChunk < chunkSize; rowInChunk++)
	{
		if (!isTransmit)
		{
			/* start DataRow ('D') for a single tuple */
			pq_beginmessage_reuse(buf, 'D');
			pq_sendint16(buf, columnCount);
		}
		else if (buf->len >= TRANSMIT_MESSAGE_SIZE_THRESHOLD)
		{
			/* send the CopyData message */
			if (!IsOK(pq_endmessage_reuse(clientSession, buf)))
			{
				PGDUCK_SERVER_ERROR("could not send CopyData to the client");

				/* pq_flush failed */
				return DUCKDB_PG_COMMUNICATION_ERROR;
			}

			/* start CopyData ('d') for remaining rows in the chunk */
			pq_beginmessage_reuse(buf, 'd');
		}

		for (idx_t columnIndex = 0; columnIndex < columnCount; columnIndex++)
		{
			DuckDBResultColumn *resultColumn = &resultColumns[columnIndex];
			duckdb_type duckType = resultColumn->duckType;
			duckdb_logical_type logicalType = resultColumn->logicalType;
			duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, columnIndex);
			uint64_t   *validity = duckdb_vector_get_validity(vector);

			if (isTransmit && columnIndex > 0)
			{
				appendStringInfoCharMacro(buf, TRANSMIT_COLUMN_SEPARATOR_CHAR);
			}

			/*
			 * In DuckDB terminology, validity is used to indicate whether a
			 * row inside a chunk is not NULL. So, if a row is NULL, we should
			 * send -1 to the client session.
			 */
			if (validity != NULL && !duckdb_validity_row_is_valid(validity, rowInChunk))
			{
				if (!isTransmit)
				{
					/* -1 indicates a NULL column value in PG protocol */
					pq_sendint32(buf, -1);
				}
				else
				{
					appendStringInfoString(buf, TRANSMIT_NULL_STRING);
				}

				continue;
			}

			if (!IsOK(duckdb_vector_to_pg_wire(vector, duckType, logicalType, rowInChunk, responseFormat, buf)))
			{
				PGDUCK_SERVER_ERROR("could not convert column data type to PG wire format: %d", duckType);

				return DUCKDB_TYPE_CONVERSION_ERROR;
			}
		}

		/* send the DataRow message */
		if (!isTransmit && !IsOK(pq_endmessage_reuse(clientSession, buf)))
		{
			PGDUCK_SERVER_ERROR("could not send data row to the client");

			/* pq_flush failed */
			return DUCKDB_PG_COMMUNICATION_ERROR;
		}
		else if (isTransmit)
		{
			/* CSV end-of-line */
			appendStringInfoCharMacro(buf, TRANSMIT_END_OF_LINE_CHAR);
		}
	}

	/* send the last CopyData message for this chunk */
	if (isTransmit && !IsOK(pq_endmessage_reuse(clientSession, buf)))
	{
		PGDUCK_SERVER_ERROR("could not send CopyData to the client");

		/* pq_flush failed */
		return DUCKDB_PG_COMMUNICATION_ERROR;
	}

	return DUCKDB_SUCCESS;
}


/*
 * create_result_column_state sets the logical types for each of the vectors
 * in the ResultColumn
 */
static void
create_result_column_state(DuckDBResultColumn * resultColumns, duckdb_data_chunk chunk,
						   idx_t columnCount)
{
	for (idx_t columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		DuckDBResultColumn *resultColumn = &resultColumns[columnIndex];
		duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, columnIndex);

		resultColumn->logicalType = duckdb_vector_get_column_type(vector);
	}
}


/*
 * destroy_result_column_state destroys the logical type of a result column.
 */
static void
destroy_result_column_state(DuckDBResultColumn * resultColumns, idx_t columnCount)
{
	for (idx_t columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		DuckDBResultColumn *resultColumn = &resultColumns[columnIndex];

		duckdb_destroy_logical_type(&resultColumn->logicalType);
	}
}


/*
 * Cleans up the resources used by the DuckDB query.
 *
 * This function destroys the DuckDB result, frees the memory allocated for the result columns,
 * and frees the memory allocated for the string buffer.
 */
static void
duckdb_query_result_destroy(DuckDBQueryResult * duckdb_query_result)
{
	pfree(duckdb_query_result->buf.data);
	pfree(duckdb_query_result->resultColumns);
}


/*
 * Naively checks if the given command is a "SET" command. We should
 * perhaps use a proper parser to do this.
 */
static bool
is_set_command(const char *command)
{
	/* Skip leading white spaces */
	while (isspace((unsigned char) *command))
	{
		command++;
	}

	/* Check if the command starts with 'SET', case-insensitive */
	if (strcasestr(command, "SET") != command)
	{
		return false;
	}

	/* Advance the pointer beyond 'SET' */
	command += 3;

	/* Ensure at least one space after 'SET' */
	if (!isspace((unsigned char) *command))
	{
		return false;
	}

	/* Skip spaces after 'SET' */
	while (isspace((unsigned char) *command))
	{
		command++;
	}

	/* Ensure there's a GUC name following 'SET' */
	if (*command == '\0' || isspace((unsigned char) *command))
	{
		return false;
	}

	/* Iterate through GUC name until a space or TO/= is encountered */
	while (*command != '\0' && !isspace((unsigned char) *command) && strncmp(command, "TO", 2) != 0 && strncmp(command, "=", 1) != 0)
	{
		command++;
	}

	return true;
}


/*
 * append_escaped_csv appends an escaped a value for use in a CSV file.
 *
 * If there are no characters that require escaping, the original value
 * is appended.
 *
 * Very loosely based on CopyAttributeOutCSV
 */
static void
append_escaped_csv(StringInfo buffer, char *value)
{
	bool		needsQuotes = false;

	if (*value == '\0')
	{
		/* empty string needs quotes */
		needsQuotes = true;
	}
	else if (strcmp(value, TRANSMIT_NULL_STRING) == 0 ||
			 strcmp(value, TRANSMIT_END_OF_FILE_STRING) == 0)
	{
		/* special control sequences need quotes */
		needsQuotes = true;
	}

	/*
	 * check for control characters, or multi-byte characters, that require
	 * quoting
	 */
	for (char *charPointer = value; *charPointer != '\0'; charPointer++)
	{
		char		currentChar = *charPointer;

		if (currentChar == TRANSMIT_QUOTE_CHAR ||
			currentChar == TRANSMIT_COLUMN_SEPARATOR_CHAR ||
			currentChar == '\n' || currentChar == '\r')
		{
			/* control character */
			needsQuotes = true;
			break;
		}
		else if (IS_HIGHBIT_SET(currentChar))
		{
			/* multi-byte character */
			needsQuotes = true;
			break;
		}
	}

	if (!needsQuotes)
	{
		/* can simply copy the value */
		appendStringInfoString(buffer, value);
		return;
	}

	/* need quotes */
	appendStringInfoCharMacro(buffer, TRANSMIT_QUOTE_CHAR);

	for (char *charPointer = value; *charPointer != '\0'; charPointer++)
	{
		char		currentChar = *charPointer;

		if (currentChar == TRANSMIT_QUOTE_CHAR ||
			currentChar == TRANSMIT_ESCAPE_CHAR)
		{
			appendStringInfoCharMacro(buffer, TRANSMIT_ESCAPE_CHAR);
			appendStringInfoCharMacro(buffer, currentChar);
		}
		else if (IS_HIGHBIT_SET(currentChar))
		{
			/*
			 * multi-byte character, DuckDB only supports UTF-8, so must be
			 * UTF-8
			 */
			int			charLength = pg_encoding_mblen(PG_UTF8, charPointer);

			appendBinaryStringInfo(buffer, charPointer, charLength);

			/* skip extra bytes */
			charPointer += charLength - 1;
		}
		else
		{
			appendStringInfoCharMacro(buffer, currentChar);

		}
	}

	appendStringInfoCharMacro(buffer, TRANSMIT_QUOTE_CHAR);
}


/*
 * append_completion_tag appends a command tag to a buffer.
 *
 * The buffer is assumed to be at least COMPLETION_TAG_BUFSIZE in size.
 */
static void
append_completion_tag(char *buffer, duckdb_result duckResult, idx_t rowsReturned)
{
	duckdb_statement_type statementType = duckdb_result_statement_type(duckResult);
	const char *prefix = command_tag(statementType);

	switch (statementType)
	{
		case DUCKDB_STATEMENT_TYPE_SELECT:
			snprintf(buffer, COMPLETION_TAG_BUFSIZE, "%s %" PRIu64, prefix, rowsReturned);
			break;

		case DUCKDB_STATEMENT_TYPE_INSERT:
		case DUCKDB_STATEMENT_TYPE_UPDATE:
		case DUCKDB_STATEMENT_TYPE_DELETE:
		case DUCKDB_STATEMENT_TYPE_COPY:
			snprintf(buffer, COMPLETION_TAG_BUFSIZE, "%s %" PRIu64, prefix, duckdb_rows_changed(&duckResult));
			break;

		default:
			strlcpy(buffer, prefix, COMPLETION_TAG_BUFSIZE);
			break;
	}
}


/*
 * command_tag returns the command tag for a given statement type.
 */
static const char *
command_tag(duckdb_statement_type statementType)
{
	switch (statementType)
	{
		case DUCKDB_STATEMENT_TYPE_SELECT:
			return "SELECT";
		case DUCKDB_STATEMENT_TYPE_INSERT:
			return "INSERT 0";
		case DUCKDB_STATEMENT_TYPE_UPDATE:
			return "UPDATE";
		case DUCKDB_STATEMENT_TYPE_DELETE:
			/* also used for TRUNCATE */
			return "DELETE";
		case DUCKDB_STATEMENT_TYPE_COPY:
			return "COPY";

		case DUCKDB_STATEMENT_TYPE_EXPLAIN:
			return "EXPLAIN";
		case DUCKDB_STATEMENT_TYPE_PREPARE:
			return "PREPARE";
		case DUCKDB_STATEMENT_TYPE_CREATE:
			return "CREATE";
		case DUCKDB_STATEMENT_TYPE_EXECUTE:
			return "EXECUTE";
		case DUCKDB_STATEMENT_TYPE_ALTER:
			return "ALTER";
		case DUCKDB_STATEMENT_TYPE_ANALYZE:
			return "ANALYZE";
		case DUCKDB_STATEMENT_TYPE_VARIABLE_SET:
			/* for non-existent settings we use our own logic and return DUCK */
			return "SET";
		case DUCKDB_STATEMENT_TYPE_CREATE_FUNC:
			return "CREATE FUNCTION";
		case DUCKDB_STATEMENT_TYPE_DROP:
			return "DROP";
		case DUCKDB_STATEMENT_TYPE_EXPORT:
			return "EXPORT";
		case DUCKDB_STATEMENT_TYPE_PRAGMA:
			return "PRAGMA";
		case DUCKDB_STATEMENT_TYPE_VACUUM:
			return "VACUUM";
		case DUCKDB_STATEMENT_TYPE_CALL:
			return "CALL";
		case DUCKDB_STATEMENT_TYPE_SET:
			return "SET";
		case DUCKDB_STATEMENT_TYPE_LOAD:
			return "LOAD";
		case DUCKDB_STATEMENT_TYPE_ATTACH:
			return "ATTACH";
		case DUCKDB_STATEMENT_TYPE_DETACH:
			return "DETACH";

		default:
		case DUCKDB_STATEMENT_TYPE_RELATION:
		case DUCKDB_STATEMENT_TYPE_EXTENSION:
		case DUCKDB_STATEMENT_TYPE_MULTI:
		case DUCKDB_STATEMENT_TYPE_TRANSACTION:
		case DUCKDB_STATEMENT_TYPE_LOGICAL_PLAN:
		case DUCKDB_STATEMENT_TYPE_INVALID:
			return "DUCK";
	}
}
