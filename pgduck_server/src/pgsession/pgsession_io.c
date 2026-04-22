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
 * The functions in this file implement the low-level communication
 * functions Postgres server wire protocol.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#include "c.h"
#include "postgres_fe.h"

#include <stdio.h>
#include <stdlib.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <pthread.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include <common/ip.h>
#include <port/pg_bswap.h>
#include <lib/stringinfo.h>
#include <common/fe_memutils.h>

#include "pgserver/client_threadpool.h"
#include "pgsession/pgsession.h"
#include "pgsession/pgsession_io.h"
#include "pgsession/pqformat.h"
#include "utils/pgduck_log_utils.h"
#include "utils/pg_log_utils.h"

static int	process_cancel_request(char *startupPacketBuf, int startupPacketLen);


/*
 * First reads the socket for the message type, then fills in the
 * inputMessage.
 *
 * Derived from SocketBackend() at postgres.c
 */
int
pgsession_read_command(PGSession *pgSession, StringInfo inputMessage)
{
	int			maxMessageLength;

	/*
	 * Get message type code from the frontend.
	 */
	int			messageType = pgsession_get_byte(pgSession);

	if (messageType < 0)
	{
		return EOF;
	}

	/*
	 * Validate message type code before trying to read body; if we have lost
	 * sync, better to say "command unknown" than to run out of memory because
	 * we used garbage as a length word.  We can also select a type-dependent
	 * limit on what a sane length word could be.  (The limit could be chosen
	 * more granularly, but it's not clear it's worth fussing over.)
	 */
	switch (messageType)
	{

		case 'Q':				/* simple query */
			maxMessageLength = PQ_LARGE_MESSAGE_LIMIT;
			break;

		case 'X':
			maxMessageLength = PQ_SMALL_MESSAGE_LIMIT;
			break;

		case 'B':				/* bind */
		case 'P':				/* parse */
		case 'E':				/* execute */
		case 'D':				/* describe */
		case 'S':				/* sync */
		case 'C':				/* close */
		case 'c':				/* copy done  */
		case 'd':				/* copy data */
		case 'f':				/* copy fail */
			maxMessageLength = PQ_LARGE_MESSAGE_LIMIT;
			break;

		default:

			/*
			 * Otherwise we got either unsupported message or garbage from the
			 * frontend.  For now, we treat this as fatal because we have
			 * probably not added support for the message, or in an unlikely
			 * scenario lost message boundary sync, and there's no good way to
			 * recover.
			 */
			PGDUCK_SERVER_ERROR("invalid frontend message type %d", messageType);
			return EOF;
	}

	/*
	 * In protocol version 3, all frontend messages have a length word next
	 * after the type code; we can read the message contents independently of
	 * the type.
	 */
	if (!IsOK(pgsession_get_message(pgSession, inputMessage, maxMessageLength)))
	{
		return EOF;				/* suitable message already logged */
	}

	return messageType;
}

/*
 * pgsession_get_message reads a message from the receive buffer.
 *
 * Original description:
 *		pq_getmessage	- get a message with length word from connection
 *
 *		The return value is placed in an expansible StringInfo, which has
 *		already been initialized by the caller.
 *		Only the message body is placed in the StringInfo; the length word
 *		is removed.  Also, s->cursor is initialized to zero for convenience
 *		in scanning the message contents.
 *
 *		maxlen is the upper limit on the length of the
 *		message we are willing to accept.  We abort the connection (by
 *		returning EOF) if client tries to send more than that.
 *
 *		returns 0 if OK, EOF if trouble
 */
int
pgsession_get_message(PGSession *pgSession, StringInfo message, int maxLength)
{
	int32		messageLength;

	resetStringInfo(message);

	/* Read message length word */
	if (pgsession_get_bytes(pgSession, (char *) &messageLength, 4) < 0)
	{
		PGDUCK_SERVER_ERROR("unexpected EOF within message length word");
		return EOF;
	}

	messageLength = pg_ntoh32(messageLength);

	if (messageLength < 4 || messageLength > maxLength)
	{
		PGDUCK_SERVER_ERROR("invalid message length");
		return EOF;
	}

	messageLength -= 4;			/* discount length itself */

	if (messageLength > 0)
	{
		/*
		 * Allocate space for message. An OOM here will cause the whole
		 * process to exit.
		 */
		enlargeStringInfo(message, messageLength);

		/* And grab the message */
		if (!IsOK(pgsession_get_bytes(pgSession, message->data, messageLength)))
		{
			PGDUCK_SERVER_ERROR("incomplete message from client");
			return EOF;
		}
		message->len = messageLength;
		/* Place a trailing null per StringInfo convention */
		message->data[messageLength] = '\0';
	}

	return OK;
}


/*
 * pgsession_read_startup_packet reads the start-up packet sent by the client.
 *
 * The logic is extracted from ProcessStartupPacket() in postgres.
 */
int
pgsession_read_startup_packet(PGSession *pgSession)
{
	int			startupPacketLen = 0;
	char	   *startupPacketBuf = NULL;
	unsigned int proto = 0;

	/*
	 * Grab the first byte of the length word separately, so that we can tell
	 * whether we have no data at all or an incomplete packet.  (This might
	 * sound inefficient, but it's not really, because of buffering in
	 * pqcomm.c.)
	 */
	if (pgsession_get_bytes(pgSession, (char *) &startupPacketLen, 1) == EOF)
	{
		/*
		 * If we get no data at all, don't clutter the log with a complaint;
		 * such cases often occur for legitimate reasons.  An example is that
		 * we might be here after responding to NEGOTIATE_SSL_CODE, and if the
		 * client didn't like our response, it'll probably just drop the
		 * connection.  Service-monitoring software also often just opens and
		 * closes a connection without sending anything.  (So do port
		 * scanners, which may be less benign, but it's not really our job to
		 * notice those.)
		 */
		return EOF;
	}

	if (pgsession_get_bytes(pgSession, ((char *) &startupPacketLen) + 1, 3) == EOF)
	{
		/* Got a partial length word, so bleat about that */
		PGDUCK_SERVER_ERROR("incomplete startup packet");
		return EOF;
	}

	startupPacketLen = pg_ntoh32(startupPacketLen);
	startupPacketLen -= 4;

	if (startupPacketLen < (int32) sizeof(ProtocolVersion) ||
		startupPacketLen > MAX_STARTUP_PACKET_LENGTH)
	{
		PGDUCK_SERVER_ERROR("invalid length of startup packet");
		return EOF;
	}

	/*
	 * Allocate space to hold the startup packet, plus one extra byte that's
	 * initialized to be zero.  This ensures we will have null termination of
	 * all strings inside the packet.
	 */
	startupPacketBuf = pg_malloc0(startupPacketLen + 1);
	startupPacketBuf[startupPacketLen] = '\0';

	if (pgsession_get_bytes(pgSession, startupPacketBuf, startupPacketLen) == EOF)
	{
		PGDUCK_SERVER_ERROR("incomplete startup packet");
		pg_free(startupPacketBuf);
		return EOF;
	}

	proto = pg_ntoh32(*((ProtocolVersion *) startupPacketBuf));

	if (proto == CANCEL_REQUEST_CODE)
	{
		pgSession->isCancelSession = true;

		if (process_cancel_request(startupPacketBuf, startupPacketLen) == EOF)
		{
			pg_free(startupPacketBuf);
			return EOF;
		}
	}
	else if (proto == NEGOTIATE_SSL_CODE || proto == NEGOTIATE_GSS_CODE)
	{
		/*
		 * We currently only support unix-domain sockets, and SSL is not
		 * supported over unix domain sockets.
		 *
		 * For pg-hackers discussion on this topic:
		 * https://www.postgresql.org/message-id/flat/49CA2524.5010809%40gmx.net
		 */
		PGDUCK_SERVER_ERROR("server does not support SSL or GSSAPI: %u", proto);

		pg_free(startupPacketBuf);
		return EOF;
	}
	else if (proto != PG_PROTOCOL(3, 0))
	{
		PGDUCK_SERVER_ERROR("unexpected protocol message: %u", proto);
		pg_free(startupPacketBuf);
		return EOF;
	}

	pg_free(startupPacketBuf);
	return OK;
}


/*
 * process_cancel_request function processes a cancel request packet received from a client.
 * It extracts the backend PID and cancel authentication code from the cancel request packet
 * and cancels the thread that is running the query. This thread sends the cancel request
 * to the other thread that is running the query, then exits.
 */
static int
process_cancel_request(char *startupPacketBuf, int startupPacketLen)
{
	/* check if the length of the startup packet is correct */
#if PG_VERSION_NUM >= 180000
	if (startupPacketLen < offsetof(CancelRequestPacket, cancelAuthCode))
	{
		PGDUCK_SERVER_ERROR("wrong length of cancel request packet");
		return EOF;
	}

	int			cancellationTokenSize = startupPacketLen - offsetof(CancelRequestPacket, cancelAuthCode);

	if (cancellationTokenSize == 0 || cancellationTokenSize > 256)
	{
		PGDUCK_SERVER_ERROR("wrong length of cancel request packet");
		return EOF;
	}
#else
	if (startupPacketLen != sizeof(CancelRequestPacket))
	{
		PGDUCK_SERVER_ERROR("wrong length of cancel request packet");
		return EOF;
	}
#endif

	/*
	 * extract the backend PID and cancel authentication code from the cancel
	 * request packet
	 */
	CancelRequestPacket *cancel_request = (CancelRequestPacket *) startupPacketBuf;
	int			cancellationProcId = (int) pg_ntoh32(cancel_request->backendPID);

#if PG_VERSION_NUM >= 180000
	uint8	   *cancellationToken = cancel_request->cancelAuthCode;

	PGDUCK_SERVER_DEBUG("cancel request for client with PID %d and auth code %s",
						cancellationProcId, cancellationToken);

	/* cancel the thread that is running the query */
	pgclient_threadpool_cancel_thread(cancellationProcId, cancellationToken,
									  cancellationTokenSize);
#else
	int32		cancellationToken = pg_ntoh32(cancel_request->cancelAuthCode);

	PGDUCK_SERVER_DEBUG("cancel request for client with PID %d and auth code %d",
						cancellationProcId, cancellationToken);

	/* cancel the thread that is running the query */
	pgclient_threadpool_cancel_thread(cancellationProcId, cancellationToken);
#endif

	return OK;
}


/* --------------------------------
 *		pq_sendstring	- append a null-terminated text string (with conversion)
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 * --------------------------------
 */
void
pq_sendstring(StringInfo buf, const char *str)
{
	int			strLen = strlen(str);
	char	   *strEncoded;

	strEncoded = pgduck_client_to_server(str, strLen);
	if (strEncoded != str)		/* actual conversion has been done? */
	{
		strLen = strlen(strEncoded);
		appendBinaryStringInfoNT(buf, strEncoded, strLen + 1);
		pg_free(strEncoded);
	}
	else
		appendBinaryStringInfoNT(buf, str, strLen + 1);
}

/*
 * pgsession_get_bytes - get a known number of bytes from connection
 *
 * returns 0 if OK, EOF if trouble.
 *
 * This can be considered as as wrapper around pqRecv* variables. We
 * are allowed to read until PQ_RECV_BUFFER_SIZE is filled, however
 * only return the first "len" of bytes. So, in some calls to this function
 * we'll read more bytes from the wire than "len", in some other calls we'll
 * not even read from the wire, but return directly from pgSession->pqRecvBuffer.
 *
 * Derived from pq_getbytes in postgres
 */
int
pgsession_get_bytes(PGSession *pgSession, char *buf, size_t len)
{
	size_t		amount;

	while (len > 0)
	{
		while (pgSession->pqRecvPointer >= pgSession->pqRecvLength)
		{
			if (pgsession_receive(pgSession))	/* If nothing in buffer, then
												 * recv some */
			{
				return EOF;		/* Failed to recv data */
			}
		}
		amount = pgSession->pqRecvLength - pgSession->pqRecvPointer;
		if (amount > len)
		{
			amount = len;
		}
		memcpy(buf, pgSession->pqRecvBuffer + pgSession->pqRecvPointer, amount);
		pgSession->pqRecvPointer += amount;
		buf += amount;
		len -= amount;
	}
	return OK;
}


/*
 * pgsession_receive receives bytes from the client.
 *
 * Derived from pq_recvbuf in postgres.
 */
int
pgsession_receive(PGSession *pgSession)
{
	if (pgSession->pqRecvPointer > 0)
	{
		if (pgSession->pqRecvLength > pgSession->pqRecvPointer)
		{
			/* still some unread data, left-justify it in the buffer */
			memmove(pgSession->pqRecvBuffer, pgSession->pqRecvBuffer + pgSession->pqRecvPointer,
					pgSession->pqRecvLength - pgSession->pqRecvPointer);
			pgSession->pqRecvLength -= pgSession->pqRecvPointer;
			pgSession->pqRecvPointer = 0;
		}
		else
			pgSession->pqRecvLength = pgSession->pqRecvPointer = 0;
	}

	/* Can fill buffer from pgSession->pqRecvLength and upwards */
	for (;;)
	{
		int			bytesReceived = read(pgSession->pgClient->clientSocket,
										 pgSession->pqRecvBuffer + pgSession->pqRecvLength,
										 PQ_RECV_BUFFER_SIZE - pgSession->pqRecvLength);

		if (bytesReceived < 0)
		{
			if (errno == EINTR)
			{
				continue;		/* Ok if interrupted */
			}

			PGDUCK_SERVER_ERROR("could not receive data from client");
			return EOF;
		}
		if (bytesReceived == 0)
		{
			/*
			 * EOF detected.  We used to write a log message here, but it's
			 * better to expect the ultimate caller to do that.
			 */
			return EOF;
		}

		/* r contains number of bytes read, so just incr length */
		pgSession->pqRecvLength += bytesReceived;
		return OK;
	}
}

/*
 * pgsession_put_message appends a message to the send buffer of the pgSession
 * and flushes the buffer if it is full.
 *
 * Original description:
 *      socket_putmessage - send a normal message (suppressed in COPY OUT mode)
 *
 *      msgtype is a message type code to place before the message body.
 *
 *      len is the length of the message body data at *s.  A message length
 *      word (equal to len+4 because it counts itself too) is inserted by this
 *      routine.
 *
 *      We suppress messages generated while pqcomm.c is busy.  This
 *      avoids any possibility of messages being inserted within other
 *      messages.  The only known trouble case arises if SIGQUIT occurs
 *      during a pqcomm.c routine --- quickdie() will try to send a warning
 *      message, and the most reasonable approach seems to be to drop it.
 *
 *      returns 0 if OK, EOF if trouble
 */
int
pgsession_put_message(PGSession *pgSession, char messageType, char *buf, size_t bufferLength)
{
	uint32		n32;

	if (!IsOK(pgsession_put_bytes(pgSession, &messageType, 1)))
	{
		return EOF;
	}

	n32 = pg_hton32((uint32) (bufferLength + 4));
	if (!IsOK(pgsession_put_bytes(pgSession, (char *) &n32, 4)))
	{
		return EOF;
	}

	if (!IsOK(pgsession_put_bytes(pgSession, buf, bufferLength)))
	{
		return EOF;
	}

	return OK;
}


/*
* pgsession_putemptymessage appends an empty message to the send
* buffer of the pgSession.
*/
int
pgsession_putemptymessage(PGSession *pgSession, char msgtype)
{
	return pgsession_put_message(pgSession, msgtype, NULL, 0);
}

/*
 * pgsession_put_bytes appends bytes to the send buffer of the pgSession
 * and flushes the buffer if it is full.
 */
int
pgsession_put_bytes(PGSession *pgSession, char *buf, size_t bufferLength)
{
	size_t		amount;

	while (bufferLength > 0)
	{
		/* If buffer is full, then flush it out */
		if (pgSession->pqSendPointer >= pgSession->pqSendBufferSize)
		{
			if (pgsession_flush(pgSession))
			{
				return EOF;
			}
		}
		amount = pgSession->pqSendBufferSize - pgSession->pqSendPointer;
		if (amount > bufferLength)
		{
			amount = bufferLength;
		}
		memcpy(pgSession->pqSendBuffer + pgSession->pqSendPointer, buf, amount);
		pgSession->pqSendPointer += amount;
		buf += amount;
		bufferLength -= amount;
	}
	return OK;
}


/*
 * pgsession_flush writes the send buffer to the client.
 *
 * Original description:
 *      internal_flush - flush pending output
 *
 * Returns 0 if OK (meaning everything was sent, or operation would block
 * and the socket is in non-blocking mode), or EOF if trouble.
 */
int
pgsession_flush(PGSession *pgSession)
{
	char	   *bufPtr = pgSession->pqSendBuffer + pgSession->pqSendStart;
	char	   *bufEnd = pgSession->pqSendBuffer + pgSession->pqSendPointer;

	while (bufPtr < bufEnd)
	{
		int			writtenBytes = write(pgSession->pgClient->clientSocket, bufPtr, bufEnd - bufPtr);

		if (writtenBytes <= 0)
		{
			if (errno == EINTR)
			{
				continue;		/* Ok if we were interrupted */
			}

			/*
			 * Ok if no data writable without blocking, and the socket is in
			 * non-blocking mode.
			 */
			if (errno == EAGAIN ||
				errno == EWOULDBLOCK)
			{
				/*
				 * We do not expect to get here, but just in case we add the
				 * message for visibility.
				 */
				PGDUCK_SERVER_WARN("Socket write temporarily blocked: non-blocking "
								   "socket is not ready for writing. Need to retry.");
				return OK;
			}

			if (errno != pgSession->lastReportedSendErrno)
			{
				pgSession->lastReportedSendErrno = errno;
				PGDUCK_SERVER_ERROR("could not send data to client errno: %u", errno);
				return EOF;
			}

			/*
			 * We drop the buffered data anyway so that processing can
			 * continue, even though we'll probably quit soon. We also set a
			 * flag that'll cause the next CHECK_FOR_INTERRUPTS to terminate
			 * the connection.
			 */
			pgSession->pqSendStart = pgSession->pqSendPointer = 0;
			pgSession->clientConnectionLost = true;

			return EOF;
		}

		pgSession->lastReportedSendErrno = 0;	/* reset after any successful
												 * send */
		bufPtr += writtenBytes;
		pgSession->pqSendStart += writtenBytes;
	}

	pgSession->pqSendStart = pgSession->pqSendPointer = 0;
	return OK;

}

/*
 * pgsession_get_byte gets a single byte from the receive buffer.
 *
 * Original description:
 *		pq_getbyte	- get a single byte from connection, or return EOF
 */
int
pgsession_get_byte(PGSession *pgSession)
{
	while (pgSession->pqRecvPointer >= pgSession->pqRecvLength)
	{
		/* If nothing in buffer, then recv some */
		if (pgsession_receive(pgSession) < 0)
		{
			return EOF;			/* Failed to recv data */
		}
	}
	return (unsigned char) pgSession->pqRecvBuffer[pgSession->pqRecvPointer++];
}


/*
 * pgsession_send_postgres_error sends an error to the client.
 *
 * On success, returns OK, else EOF.
 * Derived from send_message_to_frontend
 */
int
pgsession_send_postgres_error(PGSession *pgSession, int errSev, char *errorMessage)
{
	StringInfoData msgBuffer;

	bool		codeFound = false;
	const char *errSevStr = GetErrorCodeStr(errSev, &codeFound);

	if (!codeFound)
		return EOF;

	pq_beginmessage(&msgBuffer, 'E');

	pq_sendbyte(&msgBuffer, PG_DIAG_SEVERITY);
	pq_sendstring(&msgBuffer, _(errSevStr));
	pq_sendbyte(&msgBuffer, PG_DIAG_SEVERITY_NONLOCALIZED);
	pq_sendstring(&msgBuffer, errSevStr);

	pq_sendbyte(&msgBuffer, PG_DIAG_SQLSTATE);

	/* 0A000    E    ERRCODE_FEATURE_NOT_SUPPORTED feature_not_supported */
	pq_sendstring(&msgBuffer, "0A000");

	/* M field is required per protocol, so always send something */
	pq_sendbyte(&msgBuffer, PG_DIAG_MESSAGE_PRIMARY);
	if (errorMessage)
		pq_sendstring(&msgBuffer, errorMessage);
	else
		pq_sendstring(&msgBuffer, _("missing error text"));

	pq_sendbyte(&msgBuffer, '\0');	/* terminator */

	if (!IsOK(pq_endmessage(pgSession, &msgBuffer)))
		return EOF;

	/*
	 * This flush is normally not necessary, since we will flush out waiting
	 * data when control returns to the main loop. But it seems best to leave
	 * it here, so that the client has some clue what happened if the backend
	 * dies before getting back to the main loop ... error/notice messages
	 * should not be a performance-critical path anyway, so an extra flush
	 * won't hurt much ...
	 */
	if (!IsOK(pgsession_flush(pgSession)))
		return EOF;

	return OK;
}


/*
 * pgduck_client_to_server is a simplified version of pg_server_to_client
 * in postgres.
 *
 * TODO: This is overly simplified version of pg_server_to_client, we might
 * have to expand this to handle encoding better.
 */
char *
pgduck_client_to_server(const char *s, int len)
{
	return unconstify(char *, s);
}
