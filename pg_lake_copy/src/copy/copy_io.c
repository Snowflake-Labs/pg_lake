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
 * Functions for COPY input/output.
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 */
#include "postgres.h"
#include "miscadmin.h"

#include "pg_lake/copy/copy_io.h"
#include "lib/stringinfo.h"
#include "libpq/pqformat.h"
#include "storage/fd.h"
#include "utils/memutils.h"

#include "libpq/libpq.h"

#define MAX_READ_SIZE (65536)

/*
 * CopyFromStdinState is a simplified version of CopyFromState
 * in PostgreSQL to use in ReceiveDataFromClient with names
 * preserved.
 */
typedef struct CopyFromStdinState
{
	/* buffer in which we store incoming bytes */
	StringInfo	fe_msgbuf;

	/* whether we reached the end-of-file */
	bool		raw_reached_eof;
} CopyFromStdinState;

static void SendCopyInResponseToClient(int16 columnCount, bool isBinary);
static int	ReceiveDataFromClient(CopyFromStdinState *cstate, char *databuf);
static void SendCopyBegin(int columnCount, bool isBinary);
static void SendCopyEnd(void);
static void SendCopyData(char *sendBuffer, int sendBufferLength);


/*
 * CopyInputToFile copies data from the socket to the given file.
 * We request the client send a specific column count.
 */
void
CopyInputToFile(char *filePath, int columnCount, bool isBinary)
{
	StringInfoData copyData;

	initStringInfo(&copyData);

	CopyFromStdinState cstate = {
		.fe_msgbuf = makeStringInfo(),
		.raw_reached_eof = false
	};

	/* open the destination file for writing */
	FILE	   *file = AllocateFile(filePath, PG_BINARY_W);

	if (file == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create file \"%s\": %m",
							   filePath)));
	}

	/* tell the client we are ready for data */
	SendCopyInResponseToClient(columnCount, isBinary);

	/* allocate on the heap since it's quite big */
	char	   *receiveBuffer = palloc(MAX_READ_SIZE);

	while (!cstate.raw_reached_eof)
	{
		/* copy some bytes from the client into fe_msgbuf */
		unsigned long bytesRead = ReceiveDataFromClient(&cstate, receiveBuffer);

		if (bytesRead > 0)
		{
			/* copy bytes from fe_msgbuf to the destination file */
			if (fwrite(receiveBuffer, 1, bytesRead, file) != bytesRead)
			{
				ereport(ERROR, (errcode_for_file_access(),
								errmsg("could not write to file \"%s\": %m",
									   filePath)));
			}
		}
		else if (bytesRead == 0)
		{
			break;
		}
	}

	pfree(receiveBuffer);
	pfree(copyData.data);

	if (FreeFile(file))
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not write to file \"%s\": %m",
							   filePath)));
	}
}


/*
 * SendCopyInResponseToClient sends the CopyInResponse message that the client
 * expects after a COPY .. FROM STDIN.
 *
 * This code is adapted from ReceiveCopyBegin in PostgreSQL.
 */
static void
SendCopyInResponseToClient(int16 columnCount, bool isBinary)
{
	StringInfoData buf;
	int			copyFormat = isBinary ? 1 : 0;

	pq_beginmessage(&buf, 'G');
	pq_sendbyte(&buf, copyFormat);
	pq_sendint16(&buf, columnCount);
	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		/* use the same format for all columns */
		pq_sendint16(&buf, copyFormat);
	}
	pq_endmessage(&buf);
	pq_flush();
}


/*
 * ReceiveDataFromClient reads COPY data from the client.
 *
 * We attempt to read at least minread, and at most maxread, bytes from
 * the source.  The actual number of bytes read is returned; if this is
 * less than minread, EOF was detected.
 *
 * Note: when copying from the frontend, we expect a proper EOF mark per
 * protocol; if the frontend simply drops the connection, we raise error.
 * It seems unwise to allow the COPY IN to complete normally in that case.
 *
 * NB: no data conversion is applied here.
 *
 * This code is copied ad verbatim from CopyGetData in PostgreSQL.
 */
static int
ReceiveDataFromClient(CopyFromStdinState *cstate, char *databuf)
{
	int			minread = 1;
	int			maxread = MAX_READ_SIZE;

	int			bytesread = 0;

	while (maxread > 0 && bytesread < minread && !cstate->raw_reached_eof)
	{
		int			avail;

		while (cstate->fe_msgbuf->cursor >= cstate->fe_msgbuf->len)
		{
			/* Try to receive another message */
			int			mtype;
			int			maxmsglen;

	readmessage:
			HOLD_CANCEL_INTERRUPTS();
			pq_startmsgread();
			mtype = pq_getbyte();
			if (mtype == EOF)
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unexpected EOF on client connection with an open transaction")));
			/* Validate message type and set packet size limit */
			switch (mtype)
			{
				case 'd':		/* CopyData */
					maxmsglen = PQ_LARGE_MESSAGE_LIMIT;
					break;
				case 'c':		/* CopyDone */
				case 'f':		/* CopyFail */
				case 'H':		/* Flush */
				case 'S':		/* Sync */
					maxmsglen = PQ_SMALL_MESSAGE_LIMIT;
					break;
				default:
					ereport(ERROR,
							(errcode(ERRCODE_PROTOCOL_VIOLATION),
							 errmsg("unexpected message type 0x%02X during COPY from stdin",
									mtype)));
					maxmsglen = 0;	/* keep compiler quiet */
					break;
			}
			/* Now collect the message body */
			if (pq_getmessage(cstate->fe_msgbuf, maxmsglen))
				ereport(ERROR,
						(errcode(ERRCODE_CONNECTION_FAILURE),
						 errmsg("unexpected EOF on client connection with an open transaction")));
			RESUME_CANCEL_INTERRUPTS();
			/* ... and process it */
			switch (mtype)
			{
				case 'd':		/* CopyData */
					break;
				case 'c':		/* CopyDone */
					/* COPY IN correctly terminated by frontend */
					cstate->raw_reached_eof = true;
					return bytesread;
				case 'f':		/* CopyFail */
					ereport(ERROR,
							(errcode(ERRCODE_QUERY_CANCELED),
							 errmsg("COPY from stdin failed: %s",
									pq_getmsgstring(cstate->fe_msgbuf))));
					break;
				case 'H':		/* Flush */
				case 'S':		/* Sync */

					/*
					 * Ignore Flush/Sync for the convenience of client
					 * libraries (such as libpq) that may send those without
					 * noticing that the command they just sent was COPY.
					 */
					goto readmessage;
				default:
					Assert(false);	/* NOT REACHED */
			}
		}
		avail = cstate->fe_msgbuf->len - cstate->fe_msgbuf->cursor;
		if (avail > maxread)
			avail = maxread;
		pq_copymsgbytes(cstate->fe_msgbuf, databuf, avail);
		databuf = (void *) ((char *) databuf + avail);
		maxread -= avail;
		bytesread += avail;
	}

	return bytesread;
}


/*
 * CopyFileToOutput copies the raw contents of a file to the
 * client as part of a COPY .. TO STDOUT.
 */
void
CopyFileToOutput(char *filePath, int columnCount, bool isBinary)
{
	FILE	   *file = AllocateFile(filePath, PG_BINARY_R);

	if (file == NULL)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("could not read \"%s\"", filePath)));
	}

	SendCopyBegin(columnCount, isBinary);

	/* allocate on the heap since it's quite big */
	char	   *sendBuffer = palloc(MAX_READ_SIZE);

	while (true)
	{
		int			bytesRead = fread(sendBuffer, 1, MAX_READ_SIZE, file);

		if (ferror(file))
		{
			ereport(ERROR, (errcode_for_file_access(),
							errmsg("could not read from COPY file: %m")));
		}

		if (bytesRead == 0)
		{
			break;
		}

		SendCopyData(sendBuffer, bytesRead);
	}

	SendCopyEnd();

	pfree(sendBuffer);
	FreeFile(file);
}


/*
 * SendCopyBegin sends the CopyOutResponse message to start a
 * COPY .. TO STDOUT.
 *
 * This code is adapted from SendCopyBegin in PostgreSQL.
 */
static void
SendCopyBegin(int columnCount, bool isBinary)
{
	StringInfoData buf;
	int			copyFormat = isBinary ? 1 : 0;

	pq_beginmessage(&buf, 'H');
	pq_sendbyte(&buf, copyFormat);	/* overall format */
	pq_sendint16(&buf, columnCount);
	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		/* use the same format for all columns */
		pq_sendint16(&buf, copyFormat);
	}
	pq_endmessage(&buf);
}


/*
 * SendCopyEnd sends the CopyDone message to end a
 * COPY .. TO STDOUT.
 */
static void
SendCopyEnd(void)
{
	pq_putemptymessage('c');
}


/*
 * SendCopyData sends a CopyData message containing the given buffer.
 */
static void
SendCopyData(char *sendBuffer, int sendBufferLength)
{
	StringInfoData buf;

	pq_beginmessage(&buf, 'd');
	pq_sendbytes(&buf, sendBuffer, sendBufferLength);
	pq_endmessage(&buf);
}
