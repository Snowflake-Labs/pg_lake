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
 * Functions for mimicking a Postgres client.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef PGDUCK_PG_SESSION_H
#define PGDUCK_PG_SESSION_H

#include "c.h"
#include <sys/socket.h>

#include "duckdb/duckdb.h"

#define DUCKPG_SERVER_VERSION "16.4.DuckPG"

/*
 * QUERY_ERROR is used when the query the client sent errors in DuckDB, but we can
 * happily continue.
 */
#define QUERY_ERROR (1)

/*
 * OK means everything is going according to plan.
 */
#define OK (0)
#define IsOK(C) ((C) == OK)

/*
 * COMM_ERROR means we failed to read or write the expected number of bytes on a
 * connection and it should die. We reuse the value of EOF (-1) which is used in
 * pqformat code that we copied from postgres.
 */
#define COMM_ERROR (-1)

/*
 * INIT_ERROR means we could not initialize all of the required data structures.
 */
#define INIT_ERROR (-2)

/* use same values as src/backend/libpq/pqcomm.c */
#define PQ_SEND_BUFFER_SIZE 8192
#define PQ_RECV_BUFFER_SIZE 8192

#define MaxAllocSize       ((Size) 0x3fffffff)	/* 1 gigabyte - 1 */
#define PQ_LARGE_MESSAGE_LIMIT  (MaxAllocSize - 1)
#define PQ_SMALL_MESSAGE_LIMIT	10000

typedef struct PGClient
{
	int			clientSocket;
	struct sockaddr_storage clientAddress;
	int			cancellationProcId;
#if PG_VERSION_NUM >= 180000
	uint8	   *cancellationToken;
	size_t		cancellationTokenSize;
#else
	int32		cancellationToken;
#endif
	int			threadIndex;
}			PGClient;

typedef uint32 ProtocolVersion;

/*
 * PreparedStatementState indicates whether a prepared statement
 * successfully passed the parse and bind stages.
 */
typedef enum PreparedStatementState
{
	PREPARED_STATEMENT_INVALID,

	/* prepared statement should be destroyed */
	PREPARED_STATEMENT_ALLOCATED,

	/* prepared statement successfully parsed */
	PREPARED_STATEMENT_PARSED,

	/* prepared statement successfully bound */
	PREPARED_STATEMENT_BOUND
}			PreparedStatementState;

/*
 * ResponseFormat describes the expected format of the response.
 *
 * If add parameters to the response format (e.g. binary vs. text), this
 * would be the place to put it.
 */
typedef struct ResponseFormat
{
	/*
	 * isTransmit indicates whether the COPY protocol should be used to return
	 * data to clients.
	 */
	bool		isTransmit;
}			ResponseFormat;


/*
 * PG extended protocol also sends paramTypes, paramFormatCodes and
 * resultFormatCodes. However, DuckDB is smart to find the parameter
 * bindings from text format. And, none of its APIs support
 * resultFormatCodes etc. That's why we are not storing them here.
 */
typedef struct PgSessionPreparedStatement
{
	char	   *queryString;
	int16		nParams;

	/*
	 * The response format of the prepared statement is decided at parse time
	 * (specifically, whether or not the query was prefixed with transmit) and
	 * needs to be kept for as long as this statement can be executed.
	 */
	ResponseFormat responseFormat;

	/* current state of the prepared statement */
	PreparedStatementState state;

}			PgSessionPreparedStatement;



/*
 * PGSession struct is heavily inspired by the local variables
 * in src/backend/libpq/pqcomm.c.
 *
 * This data structure is used for the low-level details of communication
 * between frontend and backend.  They just shove data across the communication
 * channel, and are ignorant of the semantics of the data.
 *
 * To emit an outgoing message, use the routines in pqformat.c to construct
 * the message in a buffer and then emit it in one call to pgsession_put_message.
 * There are no functions to send raw bytes or partial messages; this
 * ensures that the channel will not be clogged by an incomplete message if
 * execution is aborted through the message.
 */
typedef struct PGSession
{
	PGClient   *pgClient;
	DuckDBSession duckSession;
	bool		isCancelSession;

	/* currently only unnamed prepared statements are supported */
	PgSessionPreparedStatement pgSessionPreparedStmt;

	ProtocolVersion frontendProtocol;

	char	   *pqSendBuffer;
	int			pqSendBufferSize;	/* Size send buffer */
	int			pqSendPointer;	/* Next index to store a byte in PqSendBuffer */
	int			pqSendStart;	/* Next index to send a byte in PqSendBuffer */

	char		pqRecvBuffer[PQ_RECV_BUFFER_SIZE];
	int			pqRecvPointer;	/* Next index to read a byte from PqRecvBuffer */
	int			pqRecvLength;	/* End of data available in PqRecvBuffer */

	bool		clientConnectionLost;	/* true once the connection is lost */

	int			lastReportedSendErrno;	/* needed to make pgsession_flush
										 * thread-safe */
}			PGSession;

/* per-client entrance point for the pgsession logic */
extern void *pgsession_handle_connection(void *input);

extern int	oom_is_fatal;

#endif							/* // PGDUCK_PG_SESSION_H */
