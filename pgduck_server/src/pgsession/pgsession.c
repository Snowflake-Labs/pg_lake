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
 * The functions in this file implement the Postgres server wire protocol.
 * The protocol implemented here follows ProcessStartupPacket() in
 * postmaster.c.
 *
 * For ease of read and documentation purposes, it is split into smaller
 * functions such as pgsession_send_auth_ok().
 *
 * The low-level functions communication functions, such as pgsession_get_bytes(),
 * follows Postgres pq_getbytes() and these are described in pgsession_io.c.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#include "c.h"
#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <signal.h>

#include <arpa/inet.h>
#include <lib/stringinfo.h>
#include <sys/socket.h>
#include <common/ip.h>
#include <common/fe_memutils.h>
#include <port/pg_bswap.h>

#include "duckdb/duckdb.h"
#include "pgserver/client_threadpool.h"
#include "pgsession/pgsession.h"
#include "pgsession/pgsession_io.h"
#include "pgsession/pqformat.h"
#include "pgsession/recv_sink.h"
#include "utils/pgduck_log_utils.h"
#include "utils/pg_log_utils.h"

/*
 * When queries are prefixed with transmit, we simulate what PostgreSQL does
 * in case of COPY (<query>) TO STDOUT WITH (format 'csv', null '\N');
 *
 * This allows us to pass rows to the client in batches, which significantly
 * reduces overhead compared to reading results in single-row mode in libpq.
 *
 * We cannot do this via DuckDB directly, since DuckDB does not know about our
 * socket (or PG protocol) and COPY .. TO STDOUT will simply write to /dev/stdout.
 */
#define TRANSMIT_PREFIX "transmit "
#define TRANSMIT_PREFIX_LENGTH (strlen(TRANSMIT_PREFIX))

/*
 * RECEIVE-prefix detection (mirror of TRANSMIT, but for the input side).
 *
 * RECEIVE queries stage CopyData bytes streamed from the client into a
 * server-local sink. Once the client sends CopyDone, the actual query
 * (with SINK_PLACEHOLDER replaced by the sink path) runs against DuckDB.
 *
 * Form expected from clients:
 *   "RECEIVE INSERT INTO foo SELECT * FROM read_csv('@@PG_LAKE_RECV@@', ...)"
 *
 * The placeholder is replaced exactly once; the substituted string is
 * what gets passed to duckdb_query().
 */
#define RECEIVE_PREFIX "receive "
#define RECEIVE_PREFIX_LENGTH (strlen(RECEIVE_PREFIX))

#define SINK_PLACEHOLDER "'@@PG_LAKE_RECV@@'"
#define SINK_PLACEHOLDER_LENGTH (strlen(SINK_PLACEHOLDER))

/*
 * Convenience macro for pgsession_handle_connection to terminate
 * the connection in case of error, except regular query errors.
 */
#define check(fn_call, pgSession, errorMessage) \
    do { \
		int _status = fn_call; \
        if (_status != OK && _status != QUERY_ERROR) { \
            PGDUCK_SERVER_ERROR( "%s for connection %d", (errorMessage), (pgSession).pgClient->clientSocket); \
            goto finally; \
        } \
    } while (0)

#define IS_REPORTABLE_DUCKDB_ERROR(status) \
	((status) == DUCKDB_QUERY_ERROR ||			 \
	 (status) == DUCKDB_TYPE_CONVERSION_ERROR || \
	 (status) == DUCKDB_FATAL_ERROR || \
	 (status) == DUCKDB_OUT_OF_MEMORY_ERROR)

#define IS_FATAL_DUCKDB_STATUS(status) \
	((status) == DUCKDB_FATAL_ERROR || \
	 (oom_is_fatal && (status) == DUCKDB_OUT_OF_MEMORY_ERROR))

/* The main functions that implement PG protocol */
static int	pgsession_send_auth_ok(PGSession * pgSession);
#if PG_VERSION_NUM >= 180000
static int	pgsession_send_cancellation_key(PGSession * pgSession, int cancellationProcId, uint8 *cancellationToken,
											size_t cancellationTokenSize);
#else
static int	pgsession_send_cancellation_key(PGSession * pgSession, int cancellationProcId, int32 cancellationToken);
#endif
static int	pgsession_send_server_version(PGSession * pgSession, const char *serverVersion);
static int	pgsession_send_client_encoding(PGSession * pgSession, const char *clientEncoding);
static int	pgsession_send_extra_float_digits(PGSession * pgSession, const char *extra_float_digits);
static int	pgsession_send_ready_for_query(PGSession * pgSession);
static int	pgsession_init(PGSession * pgSession, PGClient * pgClient);
static int	pgsession_destroy(PGSession * pgSession);
static void pgsession_prepared_statement_deallocate(PGSession * pgSession);
static void pgsession_log_client_info(PGClient * pgClient);
static int	handle_pgsession_error_message(DuckDBStatus status, PGSession * pgSession,
										   char *errorMessage);

static int	process_query_message(PGSession * pgSession, StringInfo inputMessage);
static int	process_parse_message(PGSession * pgSession, StringInfo inputMessage);
static int	process_bind_message(PGSession * pgSession, StringInfo inputMessage);
static int	process_execute_message(PGSession * pgSession, StringInfo inputMessage);

static int	process_copy_data(PGSession * pgSession, StringInfo inputMessage);
static int	process_copy_done(PGSession * pgSession, StringInfo inputMessage);
static int	process_copy_fail(PGSession * pgSession, StringInfo inputMessage);
static int	pgsession_send_copy_in_response(PGSession * pgSession);
static char *substitute_sink_placeholder(const char *queryString,
										 const char *sinkPath,
										 char **errorMessageOut);
static void recv_session_reset(PGSession * pgSession);

static bool is_transmit_query(const char *queryString);
static bool is_receive_query(const char *queryString);

/* global flag on whether to exit on OOM */
int			oom_is_fatal = true;

/*
 * Per-client entrance point for the pgsession logic.
 *
 * After setting up the communication with the client, this function
 * gets into an infinite loop, waiting for new commands.
 */
void *
pgsession_handle_connection(void *input)
{
	PGClient   *pgClient = (PGClient *) input;

	PGSession	pgSession;

	StringInfoData inputMessage;

	initStringInfo(&inputMessage);

	/*
	 * Similar to Postgres, ignore PIPE errors as the client might have exited
	 * before we send anything.
	 */
	signal(SIGPIPE, SIG_IGN);

	/* log clients as Postgres does */
	pgsession_log_client_info(pgClient);

	/* follow through the protocol setup */
	check(pgsession_init(&pgSession, pgClient), pgSession, "failed to initialize connection");

	/*
	 * Tell the thread pool about our DuckDB connection, such that it can do
	 * duckdb_interrupt() to cancel a query on a thread.
	 */
	pgclient_threadpool_set_duckdb_conn(pgClient->threadIndex, pgSession.duckSession.connection);

	check(pgsession_read_startup_packet(&pgSession), pgSession, "failed to read startup packet");

	/*
	 * In the PG Protocol, cancellation is facilitated when another client
	 * connects and provides a cancellation token. This token signifies the
	 * request to cancel an ongoing client.
	 *
	 * If this session is one of those cancellation sessions, we should not
	 * proceed with the normal protocol.
	 */
	if (pgSession.isCancelSession)
		goto finally;

	check(pgsession_send_auth_ok(&pgSession), pgSession, "failed to send authentication info");
#if PG_VERSION_NUM >= 180000
	check(pgsession_send_cancellation_key(&pgSession, pgClient->cancellationProcId, pgClient->cancellationToken, pgClient->cancellationTokenSize), pgSession, "failed to send cancellation key");
#else
	check(pgsession_send_cancellation_key(&pgSession, pgClient->cancellationProcId, pgClient->cancellationToken), pgSession, "failed to send cancellation key");
#endif
	check(pgsession_send_server_version(&pgSession, DUCKPG_SERVER_VERSION), pgSession, "failed to send server version");

	/* todo: should be configurable via CLI/configuration file */
	check(pgsession_send_client_encoding(&pgSession, "UTF8"), pgSession, "failed to send server version");
	check(pgsession_send_extra_float_digits(&pgSession, "1"), pgSession, "failed to send extra_float_digits");

	bool		sendReadyForQuery = true;

	while (1)
	{
		if (sendReadyForQuery)
		{
			check(pgsession_send_ready_for_query(&pgSession), pgSession,
				  "failed to send ready for query");
			check(pgsession_flush(&pgSession), pgSession, "failed to flush pgSession");

			sendReadyForQuery = false;
		}

		int			messageType = pgsession_read_command(&pgSession, &inputMessage);

		switch (messageType)
		{
			case EOF:
				{
					/* error reading command */
					PGDUCK_SERVER_DEBUG("connection %d lost",
										pgSession.pgClient->clientSocket);
					goto finally;
				}

			case 'X':
				{
					/* termination */
					PGDUCK_SERVER_DEBUG("connection %d closed",
										pgSession.pgClient->clientSocket);
					goto finally;
				}

			case 'Q':
				{
					/* simple query */
					check(process_query_message(&pgSession, &inputMessage), pgSession, "failed to process query message");

					/*
					 * If process_query_message entered COPY-IN-active state
					 * (RECEIVE prefix), defer ReadyForQuery until CopyDone /
					 * CopyFail completes the deferred query.
					 */
					if (pgSession.activeRecvSink == NULL)
						sendReadyForQuery = true;
					break;
				}

			case 'P':
				{
					/* parse message */
					check(process_parse_message(&pgSession, &inputMessage), pgSession, "failed to process parse message");
					break;
				}


			case 'B':
				{
					/* bind message */
					check(process_bind_message(&pgSession, &inputMessage), pgSession, "failed to process bind message");
					break;
				}

			case 'E':
				{
					/* execute message */
					check(process_execute_message(&pgSession, &inputMessage), pgSession, "failed to process execute message");
					break;
				}

			case 'C':
				{
					/* close */
					char	   *errorMessage = "close message not yet supported";

					/*
					 * In the protocol, Parse ('P') message comes before Close
					 * ('C'). Given that we do not support, 'P', there is
					 * currently no code-path to trigger this, unless a
					 * malformed client directly sends 'B' before 'C'.
					 */
					check(pgsession_send_postgres_error(&pgSession, ERROR, errorMessage), pgSession, errorMessage);

					break;
				}

			case 'D':
				{
					/*
					 * Postgres protocol is flexible and allows the client to
					 * defer sending response to a Describe message until we
					 * send the execute message. Normally, we should be
					 * sending the response to the Describe message here.
					 * However, we are deferring it to the Execute message for
					 * simplicity.
					 *
					 * The response to the Describe message is a row
					 * description message, which is sent
					 * duckdb_query_result_send_column_metadata() in
					 * process_execute_message().
					 */
					char		describeType = pq_getmsgbyte(&inputMessage);

					(void) describeType;

					const char *preparedStmtName = pq_getmsgstring(&inputMessage);

					if (strlen(preparedStmtName) != 0)
					{
						PGDUCK_SERVER_ERROR("pgduck_server currently does not support named prepared statements: %s",
											preparedStmtName);
						goto finally;
					}

					check(pq_getmsgend(&inputMessage), pgSession, "failed to end input message");
					break;
				}

			case 'H':
				{
					/* flush */
					check(pq_getmsgend(&inputMessage), pgSession, "failed to get flush message");
					check(pgsession_flush(&pgSession), pgSession, "failed to flush pgSession");

					break;
				}

			case 'S':
				{
					/* sync */
					check(pgsession_flush(&pgSession), pgSession, "failed to flush pgSession");
					check(pq_getmsgend(&inputMessage), pgSession, "failed to end input message");

					sendReadyForQuery = true;
					break;
				}

			case 'd':
				{
					/* CopyData: only valid while a RECEIVE is in flight */
					if (pgSession.activeRecvSink == NULL)
					{
						char	   *errorMessage = "unexpected CopyData message; no RECEIVE in progress";

						check(pgsession_send_postgres_error(&pgSession, ERROR, errorMessage), pgSession, errorMessage);
						break;
					}
					check(process_copy_data(&pgSession, &inputMessage), pgSession, "failed to process CopyData message");
					break;
				}

			case 'c':
				{
					/* CopyDone: finalize the sink and run the deferred query */
					if (pgSession.activeRecvSink == NULL)
					{
						char	   *errorMessage = "unexpected CopyDone message; no RECEIVE in progress";

						check(pgsession_send_postgres_error(&pgSession, ERROR, errorMessage), pgSession, errorMessage);
						sendReadyForQuery = true;
						break;
					}
					check(process_copy_done(&pgSession, &inputMessage), pgSession, "failed to process CopyDone message");
					sendReadyForQuery = true;
					break;
				}

			case 'f':
				{
					/* CopyFail: client aborted the upload */
					if (pgSession.activeRecvSink == NULL)
					{
						char	   *errorMessage = "unexpected CopyFail message; no RECEIVE in progress";

						check(pgsession_send_postgres_error(&pgSession, ERROR, errorMessage), pgSession, errorMessage);
						sendReadyForQuery = true;
						break;
					}
					check(process_copy_fail(&pgSession, &inputMessage), pgSession, "failed to process CopyFail message");
					sendReadyForQuery = true;
					break;
				}

			case 'F':
				{
					/* fastpath function call */
					char	   *errorMessage = "fastpath message not yet supported";

					check(pgsession_send_postgres_error(&pgSession, ERROR, errorMessage), pgSession, errorMessage);

					break;
				}

			default:
				{
					PGDUCK_SERVER_ERROR("ignoring unknown message type %c on connection %d", messageType, pgClient->clientSocket);
					break;
				}
		}
	}


finally:
	pgsession_prepared_statement_deallocate(&pgSession);
	pgsession_destroy(&pgSession);
	pg_free(inputMessage.data);

	return NULL;
}


/*
 * process_query_message handles simple query protocol messages,
 * sent via PQsendQuery (e.g. psql).
 *
 * In case of a network failure, it returns COMM_ERROR and the connection
 * should shut down.
 *
 * In case of a query error, it sends an error to the client and returns
 * QUERY_ERROR.
 *
 * Otherwise, it returns OK.
 */
static int
process_query_message(PGSession * pgSession, StringInfo inputMessage)
{
	const char *queryString = pq_getmsgstring(inputMessage);

	if (queryString == NULL)
	{
		PGDUCK_SERVER_ERROR("could not read query string for connection %d",
							pgSession->pgClient->clientSocket);
		return COMM_ERROR;
	}

	PGDUCK_SERVER_DEBUG("connection %d sent query: %s",
						pgSession->pgClient->clientSocket, queryString);

	ResponseFormat responseFormat = {
		.isTransmit = false
	};

	if (is_receive_query(queryString))
	{
		ReceiveSink *sink;
		char	   *substituted;
		char	   *substituteError = NULL;

		/* Strip RECEIVE prefix. */
		queryString += RECEIVE_PREFIX_LENGTH;

		/*
		 * RECEIVE may be combined with TRANSMIT (e.g. INSERT ... RETURNING) —
		 * honor both prefixes.
		 */
		if (is_transmit_query(queryString))
		{
			responseFormat.isTransmit = true;
			queryString += TRANSMIT_PREFIX_LENGTH;
		}

		sink = recv_sink_create();
		if (sink == NULL)
		{
			char	   *err = "failed to create RECEIVE sink";

			return pgsession_send_postgres_error(pgSession, ERROR, err);
		}

		substituted = substitute_sink_placeholder(queryString, recv_sink_path(sink), &substituteError);
		if (substituted == NULL)
		{
			recv_sink_destroy(sink);
			return pgsession_send_postgres_error(pgSession, ERROR, substituteError);
		}

		pgSession->activeRecvSink = sink;
		pgSession->deferredQueryString = substituted;
		pgSession->deferredResponseFormat = responseFormat;

		if (!IsOK(pgsession_send_copy_in_response(pgSession)))
		{
			recv_session_reset(pgSession);
			return COMM_ERROR;
		}

		/*
		 * pgsession_send_copy_in_response only queues 5 bytes into the send
		 * buffer; pq_endmessage / pgsession_put_bytes don't flush unless the
		 * buffer fills. The outer pgsession_handle_connection loop only
		 * flushes inside the `if (sendReadyForQuery)` branch, which is
		 * skipped for RECEIVE (we leave activeRecvSink set so the loop stays
		 * in COPY-IN-active state). Without this explicit flush the
		 * CopyInResponse never reaches the client and the client's
		 * PQgetResult blocks forever waiting for it — exactly what hung the
		 * streaming-writes smoke at run #2v through #2y.
		 */
		if (!IsOK(pgsession_flush(pgSession)))
		{
			recv_session_reset(pgSession);
			return COMM_ERROR;
		}

		/* Validate we read all the bytes from the original Q message. */
		if (!IsOK(pq_getmsgend(inputMessage)))
		{
			recv_session_reset(pgSession);
			return COMM_ERROR;
		}

		/* Stay in COPY-IN-active state; CopyDone runs the deferred query. */
		return OK;
	}

	responseFormat.isTransmit = is_transmit_query(queryString);
	if (responseFormat.isTransmit)
	{
		/* Skip the prefix by directly adding its length to the pointer */
		queryString += TRANSMIT_PREFIX_LENGTH;
	}

	char	   *errorMessage = NULL;
	DuckDBStatus status = duckdb_session_run_command(&pgSession->duckSession, queryString,
													 &responseFormat, &errorMessage);

	if (status == DUCKDB_SUCCESS)
	{
		/*
		 * We have completed query in duckdb_session_run_command() for success
		 * cases, nothing to do left, wait for the next query.
		 */
	}
	else if (IS_REPORTABLE_DUCKDB_ERROR(status))
	{
		/* we have to raise errors to the client */
		int			sentErrorMsg = handle_pgsession_error_message(status, pgSession, errorMessage);

		/* free error message allocated by duckdb_session_run_command */
		pfree(errorMessage);

		/* we report the error above, but need to terminate afterwards */
		if (IS_FATAL_DUCKDB_STATUS(status))
			exit(EXIT_FAILURE);

		return sentErrorMsg;
	}
	else
	{
		/* error has been logged already, now disconnect */
		return COMM_ERROR;
	}

	/* validate we read all the bytes */
	if (!IsOK(pq_getmsgend(inputMessage)))
		return COMM_ERROR;

	return OK;
}


/*
 * process_parse_message processes the Parse message from the client.
 * It prepares the statement on DuckDB. It also reads the parameter types
 * and stores them in the pgSession.
 *
 * In case of a network failure, it returns COMM_ERROR and the connection
 * should shut down.
 *
 * In case of a query error, it sends an error to the client and returns
 * QUERY_ERROR.
 *
 * Otherwise, it returns OK.
*/
static int
process_parse_message(PGSession * pgSession, StringInfo inputMessage)
{
	/*
	 * A single session only supports a single unnamed prepared statement. We
	 * are not storing the prepared statement in a hash table, as we are not
	 * supporting named prepared statements. So, here we are deallocating the
	 * previous prepared statement, if any.
	 *
	 * This is based on Postgres documentation: "An unnamed prepared statement
	 * lasts only until the next Parse statement specifying the unnamed
	 * statement as destination is issued."
	 */
	if (pgSession->pgSessionPreparedStmt.state != PREPARED_STATEMENT_INVALID)
		pgsession_prepared_statement_deallocate(pgSession);

	/* we only support unnamed prepared statements */
	const char *preparedStmtName = pq_getmsgstring(inputMessage);

	if (preparedStmtName == NULL)
	{
		PGDUCK_SERVER_ERROR("could not read prepared statement name for connection %d",
							pgSession->pgClient->clientSocket);
		return COMM_ERROR;
	}

	if (strlen(preparedStmtName) != 0)
	{
		char	   *errorMessage = "named prepared statements not supported in pgduck_server";

		if (!IsOK(pgsession_send_postgres_error(pgSession, ERROR, errorMessage)))
			return COMM_ERROR;

		return QUERY_ERROR;
	}

	/*
	 * Get the full query (may include transmit), points directly into the
	 * inputMessage.data array.
	 */
	const char *fullQueryString = pq_getmsgstring(inputMessage);

	if (fullQueryString == NULL)
	{
		PGDUCK_SERVER_ERROR("could not read query string in parse message for connection %d",
							pgSession->pgClient->clientSocket);
		return COMM_ERROR;
	}

	const char *queryString = fullQueryString;

	bool		isTransmit = is_transmit_query(queryString);

	if (isTransmit)
	{
		/* Skip the prefix by directly adding its length to the pointer */
		queryString = fullQueryString + TRANSMIT_PREFIX_LENGTH;
	}

	/* we need the query string to survive for a bit longer */
	char	   *queryStringCopy = pstrdup(queryString);

	char	   *errorMessage = NULL;
	DuckDBStatus status = duckdb_session_prepare(&pgSession->duckSession, queryStringCopy,
												 &errorMessage);

	/*
	 * make sure we destroy the prepared statement, even in case of failure
	 * below
	 */
	pgSession->pgSessionPreparedStmt.state = PREPARED_STATEMENT_ALLOCATED;
	pgSession->pgSessionPreparedStmt.queryString = queryStringCopy;

	/* remember whether the query was prefixed with transmit */
	pgSession->pgSessionPreparedStmt.responseFormat.isTransmit = isTransmit;

	bool		readFailed = false;

	pgSession->pgSessionPreparedStmt.nParams =
		pq_getmsgint(inputMessage, 2, &readFailed);

	/* error already logged */
	if (readFailed)
		return COMM_ERROR;

	for (int i = 0; i < pgSession->pgSessionPreparedStmt.nParams; i++)
	{
		int32		paramType = pq_getmsgint(inputMessage, 4, &readFailed);

		(void) paramType;

		/* error already logged */
		if (readFailed)
			return COMM_ERROR;

		/* DuckDB doesn't need paramType, so we just read and skip it */
	}

	char		parseComplete = '1';

	if (!IsOK(pgsession_putemptymessage(pgSession, parseComplete)))
		return COMM_ERROR;

	/* validate we read all the bytes */
	if (!IsOK(pq_getmsgend(inputMessage)))
		return COMM_ERROR;

	PGDUCK_SERVER_DEBUG("connection %d sent prepared statement: %s",
						pgSession->pgClient->clientSocket,
						fullQueryString);

	/*
	 * Check parse errors at the end after logging the statement.
	 */
	if (IS_REPORTABLE_DUCKDB_ERROR(status))
	{
		/* we have to raise errors to the client */
		int			sentErrorMsg = handle_pgsession_error_message(status, pgSession, errorMessage);

		PGDUCK_SERVER_WARN("query from client %d failed during parse: %s",
						   pgSession->pgClient->clientSocket,
						   errorMessage);

		/* free error message allocated by duckdb_session_prepare */
		pfree(errorMessage);

		return sentErrorMsg;
	}
	else if (status != DUCKDB_SUCCESS)
	{
		/* note: duckdb_session_prepare does not currently return other codes */
		return COMM_ERROR;
	}

	/* it is now ok to do bind */
	pgSession->pgSessionPreparedStmt.state = PREPARED_STATEMENT_PARSED;

	return OK;
}


/*
* process_bind_message processes the Bind message from the client. It binds the
* parameters to the prepared statement on DuckDB.
* We always pass the parameters as text for now for simplicity.
* The functions also reads the parameter format codes and stores them
* in the pgSession.
 *
 * In case of a network failure, it returns COMM_ERROR and the connection
 * should shut down.
 *
 * In case of a query error, it sends an error to the client and returns
 * QUERY_ERROR.
 *
 * Otherwise, it returns OK.
*/
static int
process_bind_message(PGSession * pgSession, StringInfo inputMessage)
{
	if (pgSession->pgSessionPreparedStmt.state != PREPARED_STATEMENT_PARSED &&
		pgSession->pgSessionPreparedStmt.state != PREPARED_STATEMENT_BOUND)
	{
		/* parse failed */
		return QUERY_ERROR;
	}

	const char *portalName = pq_getmsgstring(inputMessage);

	if (portalName == NULL)
		return COMM_ERROR;

	if (strlen(portalName) != 0)
	{
		char	   *errorMessage = "named prepared statements not supported in pgduck_server";

		if (!IsOK(pgsession_send_postgres_error(pgSession, ERROR, errorMessage)))
			return COMM_ERROR;

		return QUERY_ERROR;
	}
	const char *preparedStmtName = pq_getmsgstring(inputMessage);

	if (preparedStmtName == NULL)
		return COMM_ERROR;

	if (strlen(preparedStmtName) != 0)
	{
		char	   *errorMessage = "named prepared statements not supported in pgduck_server";

		if (!IsOK(pgsession_send_postgres_error(pgSession, ERROR, errorMessage)))
			return COMM_ERROR;

		return QUERY_ERROR;
	}

	bool		readFailed = false;
	int16		parameterFormatCodeCount = pq_getmsgint(inputMessage, 2, &readFailed);

	if (readFailed)
		return COMM_ERROR;

	/*
	 * DuckDB doesn't need parameterFormatCodes, so we just read and skip it
	 */
	for (int paramIndex = 0; paramIndex < parameterFormatCodeCount; paramIndex++)
	{
		int16		paramFormatCode = pq_getmsgint(inputMessage, 2, &readFailed);

		(void) paramFormatCode;

		/* error already logged */
		if (readFailed)
			return COMM_ERROR;
	}

	int16		nParams = pq_getmsgint(inputMessage, 2, &readFailed);

	if (readFailed)
		return COMM_ERROR;

	for (int16 paramIndex = 0; paramIndex < nParams; paramIndex++)
	{
		int32		paramLen = pq_getmsgint(inputMessage, 4, &readFailed);

		if (readFailed)
			return COMM_ERROR;

		DuckDBStatus bindResult;
		char	   *errorMessage = NULL;

		if (paramLen != -1)
		{
			const char *paramValue = pq_getmsgbytes(inputMessage, paramLen);

			if (paramValue == NULL)
			{
				PGDUCK_SERVER_ERROR("could not read parameter value");
				return COMM_ERROR;
			}

			/*
			 * Bind the parameter to the prepared statement. We are using
			 * duckdb_create_varchar() for now, as we are always passing the
			 * parameter as text. This helps us to avoid the specialized type
			 * bindings for now (e.g., duckdb_bind_int32()).
			 */
			bindResult = duckdb_session_bind_varchar(&pgSession->duckSession,
													 paramIndex + 1,
													 paramValue,
													 &errorMessage);
		}
		else
		{
			bindResult = duckdb_session_bind_varchar(&pgSession->duckSession,
													 paramIndex + 1,
													 NULL,
													 &errorMessage);
		}

		if (IS_REPORTABLE_DUCKDB_ERROR(bindResult))
		{
			int			sentResult = handle_pgsession_error_message(bindResult, pgSession, errorMessage);

			PGDUCK_SERVER_WARN("query from client %d failed during bind: %s",
							   pgSession->pgClient->clientSocket,
							   errorMessage);

			/* free error message allocated by duckdb_session_bind_varchar */
			pfree(errorMessage);

			return sentResult;
		}
	}

	/*
	 * We could have done this check a bit earlier, but this way we get to
	 * verify error handling in bind_value.
	 *
	 * Cases with 0 parameters provided will end up here.
	 */
	if (nParams != duckdb_session_prepared_nparams(&pgSession->duckSession))
	{
		char	   *errorMessage = "incorrect number of parameters";

		return pgsession_send_postgres_error(pgSession, ERROR, errorMessage);
	}


	int16		resultFormatCodeCount = pq_getmsgint(inputMessage, 2, &readFailed);

	if (readFailed)
		return COMM_ERROR;

	for (int16 i = 0; i < resultFormatCodeCount; i++)
	{
		int16		resultFormatCode = pq_getmsgint(inputMessage, 2, &readFailed);

		(void) resultFormatCode;

		/* error already logged */
		if (readFailed)
			return COMM_ERROR;
	}

	/* validate we read all the bytes */
	if (!IsOK(pq_getmsgend(inputMessage)))
		return COMM_ERROR;

	pgSession->pgSessionPreparedStmt.state = PREPARED_STATEMENT_BOUND;

	return OK;
}


/*
 * process_execute_message processes the Execute message from the client.
 * It executes the prepared statement on DuckDB and sends the result to
 * the client.
 *
 * In case of a network failure, it returns COMM_ERROR and the connection
 * should shut down.
 *
 * In case of a query error, it sends an error to the client and returns
 * QUERY_ERROR.
 *
 * Otherwise, it returns OK.
 */
static int
process_execute_message(PGSession * pgSession, StringInfo inputMessage)
{
	if (pgSession->pgSessionPreparedStmt.state != PREPARED_STATEMENT_BOUND)
	{
		/* parse or bind failed */
		return QUERY_ERROR;
	}

	const char *portalName = pq_getmsgstring(inputMessage);

	if (portalName == NULL)
		return COMM_ERROR;

	if (strlen(portalName) != 0)
	{
		char	   *errorMessage = "named prepared statements not supported in pgduck_server";

		if (!IsOK(pgsession_send_postgres_error(pgSession, ERROR, errorMessage)))
			return COMM_ERROR;

		return QUERY_ERROR;
	}

	bool		readFailed = false;
	int32		executeExpectedRowCount = pq_getmsgint(inputMessage, 4, &readFailed);

	if (readFailed)
	{
		PGDUCK_SERVER_ERROR("could not read expected row count");
		return COMM_ERROR;
	}

	if (executeExpectedRowCount != 0)
	{
		PGDUCK_SERVER_ERROR("pgduck_server currently only supports fetching all rows: %d",
							executeExpectedRowCount);
		return COMM_ERROR;
	}

	ResponseFormat *responseFormat = &pgSession->pgSessionPreparedStmt.responseFormat;

	char	   *errorMessage = NULL;
	DuckDBStatus status = duckdb_session_execute_prepared(&pgSession->duckSession,
														  responseFormat,
														  &errorMessage);

	if (status == DUCKDB_SUCCESS)
	{
		/* all good, continue below */
	}
	else if (IS_REPORTABLE_DUCKDB_ERROR(status))
	{
		/* we have to raise errors to the client */
		int			sentErrorMsg = handle_pgsession_error_message(status, pgSession, errorMessage);

		/* free error message allocated by duckdb_session_execute_prepared */
		pfree(errorMessage);

		return sentErrorMsg;
	}
	else
	{
		/* error has been logged already, now disconnect */
		return COMM_ERROR;
	}

	/* validate we read all the bytes */
	if (!IsOK(pq_getmsgend(inputMessage)))
		return COMM_ERROR;

	return OK;
}


/*
 * Helper function to handle different DuckDB statuses.
 */
static int
handle_pgsession_error_message(DuckDBStatus status, PGSession * pgSession, char *errorMessage)
{
	int			errorRes;

	switch (status)
	{
		case DUCKDB_QUERY_ERROR:
			errorRes = pgsession_send_postgres_error(pgSession, ERROR, errorMessage);
			break;
		case DUCKDB_INITIALIZATION_ERROR:
			errorRes = pgsession_send_postgres_error(pgSession, ERROR, "Initialization Error");
			break;
		case DUCKDB_SESSION_INITIALIZATION_ERROR:
			errorRes = pgsession_send_postgres_error(pgSession, ERROR, "Session Initialization Error");
			break;
		case DUCKDB_TYPE_CONVERSION_ERROR:
			errorRes = pgsession_send_postgres_error(pgSession, ERROR, "Unsupported type");
			break;
		case DUCKDB_OUT_OF_MEMORY_ERROR:
			errorRes = pgsession_send_postgres_error(pgSession, ERROR, "Out of Memory");
			break;
		default:
			errorRes = pgsession_send_postgres_error(pgSession, ERROR, "Unknown Error");
			break;
	}

	return errorRes;
}


/*
 * pgsession_send_ready_for_query sends a message to the client signaling that
 * the server is ready for a query.
 */
static int
pgsession_send_ready_for_query(PGSession * pgSession)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'Z');

	/* idle transaction */
	pq_sendbyte(&buf, 'I');
	if (!IsOK(pq_endmessage(pgSession, &buf)))
	{
		return EOF;
	}

	return OK;
}


/*
 * pgsession_send_auth_ok sends a message to the client signaling that
 * authentication is done.
 */
static int
pgsession_send_auth_ok(PGSession * pgSession)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'R');
	pq_sendint32(&buf, (int32) AUTH_REQ_OK);
	if (!IsOK(pq_endmessage(pgSession, &buf)))
	{
		return EOF;
	}

	return OK;
}

/*
 * pgsession_send_cancellation_key sends a message to the client to inform
 * it of its cancellation key.
 */
#if PG_VERSION_NUM >= 180000
static int
pgsession_send_cancellation_key(PGSession * pgSession, int cancellationProcId, uint8 *cancelKey, size_t cancelKeyLength)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'K');
	pq_sendint32(&buf, (int32) cancellationProcId);
	pq_sendbytes(&buf, cancelKey, cancelKeyLength);

	if (!IsOK(pq_endmessage(pgSession, &buf)))
	{
		return EOF;
	}

	return OK;
}
#else
static int
pgsession_send_cancellation_key(PGSession * pgSession, int cancellationProcId, int32 cancellationToken)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'K');
	pq_sendint32(&buf, (int32) cancellationProcId);
	pq_sendint32(&buf, cancellationToken);

	if (!IsOK(pq_endmessage(pgSession, &buf)))
	{
		return EOF;
	}

	return OK;
}
#endif


/*
 * SendServerVersion sends the server version to the client
 */
static int
pgsession_send_server_version(PGSession * pgSession, const char *serverVersion)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'S');
	pq_sendstring(&buf, "server_version");
	pq_sendstring(&buf, serverVersion);

	if (!IsOK(pq_endmessage(pgSession, &buf)))
	{
		return EOF;
	}

	return OK;
}

/*
 * SendServerVersion sends the server version to the client
 */
static int
pgsession_send_client_encoding(PGSession * pgSession, const char *clientEncoding)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'S');
	pq_sendstring(&buf, "client_encoding");
	pq_sendstring(&buf, clientEncoding);

	if (!IsOK(pq_endmessage(pgSession, &buf)))
	{
		return EOF;
	}

	return OK;
}


/*
 * pgsession_send_extra_float_digits sends the extra_float_digits to the client
 */
static int
pgsession_send_extra_float_digits(PGSession * pgSession, const char *extra_float_digits)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'S');
	pq_sendstring(&buf, "extra_float_digits");
	pq_sendstring(&buf, extra_float_digits);

	if (!IsOK(pq_endmessage(pgSession, &buf)))
	{
		return EOF;
	}

	return OK;
}

/*
 * pgsession_get_client_info is used to log the newly connected
 * client.
 *
 * Extracted from BackendInitialize() from postmaster.c
 */
static void
pgsession_log_client_info(PGClient * pgClient)
{
	int			retVal = 0;
	char		clientHost[NI_MAXHOST];
	char		clientPort[NI_MAXSERV];
	socklen_t	clientAddressLen = sizeof(pgClient->clientAddress);

	/*
	 * Get the remote host name and port for logging and status display.
	 */
	clientHost[0] = '\0';
	clientPort[0] = '\0';
	if ((retVal = pg_getnameinfo_all(&pgClient->clientAddress, clientAddressLen,
									 clientHost, sizeof(clientHost),
									 clientPort, sizeof(clientPort),
									 NI_NUMERICHOST)) != 0)
	{
		PGDUCK_SERVER_WARN("could not obtain client hostname %s", gai_strerror(retVal));
	}


	if (clientPort[0])
	{
		PGDUCK_SERVER_DEBUG("connection %d received: host=%s port=%s",
							pgClient->clientSocket, clientHost, clientPort);
	}
	else
	{
		PGDUCK_SERVER_DEBUG("connection %d received: host=%s",
							pgClient->clientSocket, clientHost);
	}

}


/*
 * pgsession_init initializes the pgSession state.
 */
static int
pgsession_init(PGSession * pgSession, PGClient * pgClient)
{
	memset(pgSession, '\0', sizeof(PGSession));
	pgSession->pgClient = pgClient;
	pgSession->pqSendBufferSize = PQ_SEND_BUFFER_SIZE;
	pgSession->pqSendBuffer = pg_malloc0(pgSession->pqSendBufferSize);
	pgSession->pqSendPointer = 0;
	pgSession->pqSendStart = 0;
	pgSession->pqRecvPointer = 0;
	pgSession->pqRecvLength = 0;
	pgSession->lastReportedSendErrno = 0;
	pgSession->clientConnectionLost = false;

	if (duckdb_session_init(&pgSession->duckSession, pgSession) != DUCKDB_SUCCESS)
	{
		return INIT_ERROR;
	}

	return OK;
}


/*
 * pgsession_destroy frees any memory associated with the pgSession.
 */
static int
pgsession_destroy(PGSession * pgSession)
{
	/* clean up any in-flight RECEIVE state */
	recv_session_reset(pgSession);

	pg_free(pgSession->pqSendBuffer);
	duckdb_session_destroy(&pgSession->duckSession);
	return OK;
}

/*
* pgsession_prepared_statement_deallocate deallocates the prepared statement
* in the pgSession. Given we only support unnamed prepared statements, we
* are not storing the prepared statement in a hash table. So, here we are
* deallocating the previous prepared statement, if any.
*/
static void
pgsession_prepared_statement_deallocate(PGSession * pgSession)
{
	if (pgSession->pgSessionPreparedStmt.state != PREPARED_STATEMENT_INVALID)
	{
		duckdb_session_destroy_prepare(&pgSession->duckSession);
		pg_free(pgSession->pgSessionPreparedStmt.queryString);
		pgSession->pgSessionPreparedStmt.state = PREPARED_STATEMENT_INVALID;
	}
}


/*
 * is_transmit_query returns whether the given query string starts with
 * transmit.
 *
 * Transmit queries use the COPY protocol to transmit their results to
 * the client.
 */
static bool
is_transmit_query(const char *queryString)
{
	return strncasecmp(queryString, TRANSMIT_PREFIX, TRANSMIT_PREFIX_LENGTH) == 0;
}


/*
 * is_receive_query returns whether the given query string starts with
 * "receive ".
 *
 * RECEIVE queries land CopyData bytes streamed from the client into a
 * server-local sink, and run the substituted query (with the sink path
 * baked in) once the client sends CopyDone.
 */
static bool
is_receive_query(const char *queryString)
{
	return strncasecmp(queryString, RECEIVE_PREFIX, RECEIVE_PREFIX_LENGTH) == 0;
}


/*
 * Substitute exactly one occurrence of SINK_PLACEHOLDER in queryString
 * with a single-quoted SQL literal containing sinkPath. Returns a
 * malloc'd string (caller frees via pg_free) or NULL on error.
 *
 * sinkPath is generated by recv_sink and never contains a single quote;
 * we still defensively reject the case in case future code changes that.
 */
static char *
substitute_sink_placeholder(const char *queryString, const char *sinkPath, char **errorMessageOut)
{
	const char *first;
	const char *second;
	size_t		pathLen;
	size_t		prefixLen;
	size_t		suffixLen;
	size_t		resultLen;
	char	   *result;
	char	   *p;

	*errorMessageOut = NULL;

	if (strchr(sinkPath, '\'') != NULL)
	{
		*errorMessageOut = "RECEIVE sink path must not contain single quote";
		return NULL;
	}

	first = strstr(queryString, SINK_PLACEHOLDER);
	if (first == NULL)
	{
		*errorMessageOut = "RECEIVE query must contain placeholder '@@PG_LAKE_RECV@@' exactly once";
		return NULL;
	}
	second = strstr(first + SINK_PLACEHOLDER_LENGTH, SINK_PLACEHOLDER);
	if (second != NULL)
	{
		*errorMessageOut = "RECEIVE query must contain placeholder '@@PG_LAKE_RECV@@' exactly once";
		return NULL;
	}

	prefixLen = (size_t) (first - queryString);
	suffixLen = strlen(first + SINK_PLACEHOLDER_LENGTH);
	pathLen = strlen(sinkPath);
	/* prefix + ' + path + ' + suffix + NUL */
	resultLen = prefixLen + 1 + pathLen + 1 + suffixLen + 1;
	result = pg_malloc(resultLen);
	p = result;

	memcpy(p, queryString, prefixLen);
	p += prefixLen;
	*p++ = '\'';
	memcpy(p, sinkPath, pathLen);
	p += pathLen;
	*p++ = '\'';
	memcpy(p, first + SINK_PLACEHOLDER_LENGTH, suffixLen);
	p += suffixLen;
	*p = '\0';

	return result;
}


/*
 * Send a CopyInResponse ('G') message: text format, zero columns. DuckDB
 * consumes opaque bytes via read_csv() once we land them in the sink.
 */
static int
pgsession_send_copy_in_response(PGSession * pgSession)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'G');
	pq_sendbyte(&buf, 0);		/* PG_WIRE_TEXT_FORMAT */
	pq_sendint16(&buf, 0);		/* zero columns */
	return pq_endmessage(pgSession, &buf);
}


/*
 * Tear down per-RECEIVE state. Idempotent.
 */
static void
recv_session_reset(PGSession * pgSession)
{
	if (pgSession->activeRecvSink != NULL)
	{
		recv_sink_destroy(pgSession->activeRecvSink);
		pgSession->activeRecvSink = NULL;
	}
	if (pgSession->deferredQueryString != NULL)
	{
		pg_free(pgSession->deferredQueryString);
		pgSession->deferredQueryString = NULL;
	}
}


/*
 * Append the CopyData payload to the active sink.
 */
static int
process_copy_data(PGSession * pgSession, StringInfo inputMessage)
{
	int			payloadLen;
	const char *payload;

	payloadLen = inputMessage->len - inputMessage->cursor;
	if (payloadLen < 0)
		return COMM_ERROR;

	payload = pq_getmsgbytes(inputMessage, payloadLen);
	if (payload == NULL)
		return COMM_ERROR;

	if (recv_sink_write(pgSession->activeRecvSink, payload, (size_t) payloadLen) != 0)
	{
		char	   *errorMessage = "failed to write RECEIVE bytes to local sink";

		recv_session_reset(pgSession);
		return pgsession_send_postgres_error(pgSession, ERROR, errorMessage);
	}

	return OK;
}


/*
 * Finalize the sink and run the deferred query against DuckDB.
 *
 * Mirrors the success/error handling at the bottom of process_query_message
 * so that fatal DuckDB errors still terminate the server process and
 * reportable errors flow back to the client as 'E' messages.
 */
static int
process_copy_done(PGSession * pgSession, StringInfo inputMessage)
{
	int			status;
	DuckDBStatus duckStatus;
	char	   *errorMessage = NULL;
	ResponseFormat responseFormat;
	char	   *queryString;

	if (!IsOK(pq_getmsgend(inputMessage)))
	{
		recv_session_reset(pgSession);
		return COMM_ERROR;
	}

	if (recv_sink_finalize(pgSession->activeRecvSink) != 0)
	{
		char	   *err = "failed to finalize RECEIVE sink";

		recv_session_reset(pgSession);
		return pgsession_send_postgres_error(pgSession, ERROR, err);
	}

	queryString = pgSession->deferredQueryString;
	responseFormat = pgSession->deferredResponseFormat;

	PGDUCK_SERVER_DEBUG("connection %d running deferred RECEIVE query: %s",
						pgSession->pgClient->clientSocket, queryString);

	duckStatus = duckdb_session_run_command(&pgSession->duckSession, queryString,
											&responseFormat, &errorMessage);

	/* Tear down RECEIVE state regardless of outcome. */
	recv_session_reset(pgSession);

	if (duckStatus == DUCKDB_SUCCESS)
		return OK;

	if (IS_REPORTABLE_DUCKDB_ERROR(duckStatus))
	{
		status = handle_pgsession_error_message(duckStatus, pgSession, errorMessage);
		pfree(errorMessage);
		if (IS_FATAL_DUCKDB_STATUS(duckStatus))
			exit(EXIT_FAILURE);
		return status;
	}

	/* error has been logged already; disconnect */
	return COMM_ERROR;
}


/*
 * Discard the sink and surface the client's failure reason as an error.
 */
static int
process_copy_fail(PGSession * pgSession, StringInfo inputMessage)
{
	const char *clientReason;
	char	   *errMsg;
	size_t		len;
	int			status;

	clientReason = pq_getmsgstring(inputMessage);
	if (clientReason == NULL)
		clientReason = "(no reason given)";

	if (!IsOK(pq_getmsgend(inputMessage)))
	{
		recv_session_reset(pgSession);
		return COMM_ERROR;
	}

	len = strlen("RECEIVE aborted by client: ") + strlen(clientReason) + 1;
	errMsg = pg_malloc(len);
	snprintf(errMsg, len, "RECEIVE aborted by client: %s", clientReason);

	recv_session_reset(pgSession);

	status = pgsession_send_postgres_error(pgSession, ERROR, errMsg);
	pg_free(errMsg);
	return status;
}
