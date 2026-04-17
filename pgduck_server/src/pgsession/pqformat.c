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

/*-------------------------------------------------------------------------
 *
 * pqformat.c
 *		Routines for formatting and parsing frontend/backend messages
 *
 * Outgoing messages are built up in a StringInfo buffer (which is expansible)
 * and then sent in a single call to pq_putmessage.  This module provides data
 * formatting/conversion routines that are needed to produce valid messages.
 * Note in particular the distinction between "raw data" and "text"; raw data
 * is message protocol characters and binary values that are not subject to
 * character set conversion, while text is converted by character encoding
 * rules.
 *
 * Incoming messages are similarly read into a StringInfo buffer, via
 * pq_getmessage, and then parsed and converted from that using the routines
 * in this module.
 *
 * These same routines support reading and writing of external binary formats
 * (typsend/typreceive routines).  The conversion routines for individual
 * data types are exactly the same, only initialization and completion
 * are different.
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *	src/backend/libpq/pqformat.c
 *
 *-------------------------------------------------------------------------
 */
/*
 * INTERFACE ROUTINES
 * Message assembly and output:
 *		pq_beginmessage - initialize StringInfo buffer
 *		pq_sendbyte		- append a raw byte to a StringInfo buffer
 *		pq_sendint		- append a binary integer to a StringInfo buffer
 *		pq_sendint64	- append a binary 8-byte int to a StringInfo buffer
 *		pq_sendfloat4	- append a float4 to a StringInfo buffer
 *		pq_sendfloat8	- append a float8 to a StringInfo buffer
 *		pq_send_ascii_string - append a null-terminated text string (without conversion)
 *		pq_endmessage	- send the completed message to the frontend
 * Note: it is also possible to append data to the StringInfo buffer using
 * the regular StringInfo routines, but this is discouraged since required
 * character set conversion may not occur.
 *
 * typsend support (construct a bytea value containing external binary data):
 *		pq_begintypsend - initialize StringInfo buffer
 *		pq_endtypsend	- return the completed string as a "bytea*"
 *
 * Special-case message output:
 *		pq_puttextmessage - generate a character set-converted message in one step
 *		pq_putemptymessage - convenience routine for message with empty body
 *
 * Message parsing after input:
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 *		pq_getmsgint	- get a binary integer from a message buffer
 *		pq_getmsgint64	- get a binary 8-byte int from a message buffer
 *		pq_getmsgfloat4 - get a float4 from a message buffer
 *		pq_getmsgfloat8 - get a float8 from a message buffer
 *		pq_getmsgbytes	- get raw data from a message buffer
 *		pq_copymsgbytes - copy raw data from a message buffer
 *		pq_getmsgtext	- get a counted text string (with conversion)
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *		pq_getmsgrawstring - get a null-terminated text string - NO conversion
 *		pq_getmsgend	- verify message fully consumed
 */
#include <stdio.h>
#include <sys/param.h>

#include "postgres_fe.h"

#include "port/pg_bswap.h"
#include "varatt.h"
#include "lib/stringinfo.h"

#include "pgsession/pgsession.h"
#include "pgsession/pgsession_io.h"
#include "pgsession/pqformat.h"
#include "utils/pgduck_log_utils.h"
#include "utils/pg_log_utils.h"


/* --------------------------------
 *		pq_beginmessage		- initialize for sending a message
 * --------------------------------
 */
void
pq_beginmessage(StringInfo buf, char msgType)
{
	initStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the pq_sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgType;
}

/* --------------------------------

 *		pq_beginmessage_reuse - initialize for sending a message, reuse buffer
 *
 * This requires the buffer to be allocated in a sufficiently long-lived
 * memory context.
 * --------------------------------
 */
void
pq_beginmessage_reuse(StringInfo buf, char msgtype)
{
	resetStringInfo(buf);

	/*
	 * We stash the message type into the buffer's cursor field, expecting
	 * that the pq_sendXXX routines won't touch it.  We could alternatively
	 * make it the first byte of the buffer contents, but this seems easier.
	 */
	buf->cursor = msgtype;
}


/* --------------------------------
 *		pq_endmessage	- send the completed message to the frontend
 *
 * The data buffer is pfree()d, but if the StringInfo was allocated with
 * makeStringInfo then the caller must still pfree it.
 * --------------------------------
 */
int
pq_endmessage(PGSession *session, StringInfo buf)
{
	/* msgtype was saved in cursor field */
	int			ret = pgsession_put_message(session, buf->cursor, buf->data, buf->len);

	/* no need to complain about any failure, since pqcomm.c already did */
	pg_free(buf->data);
	buf->data = NULL;

	return ret;
}

/* --------------------------------
 *		pq_endmessage_reuse	- send the completed message to the frontend
 *
 * The data buffer is *not* freed, allowing to reuse the buffer with
 * pq_beginmessage_reuse.
 --------------------------------
 */

int
pq_endmessage_reuse(PGSession *session, StringInfo buf)
{
	/* msgtype was saved in cursor field */
	return pgsession_put_message(session, buf->cursor, buf->data, buf->len);
}



/* --------------------------------
 *		pq_getmsgstring - get a null-terminated text string (with conversion)
 *
 *		May return a pointer directly into the message buffer, or a pointer
 *		to a palloc'd conversion result.
 * --------------------------------
 */
const char *
pq_getmsgstring(StringInfo msg)
{
	char	   *str;
	int			strLen;

	str = &msg->data[msg->cursor];

	/*
	 * It's safe to use strlen() here because a StringInfo is guaranteed to
	 * have a trailing null byte.  But check we found a null inside the
	 * message.
	 */
	strLen = strlen(str);
	if (msg->cursor + strLen >= msg->len)
	{
		PGDUCK_SERVER_ERROR("insufficient data left in message");
		return NULL;
	}
	msg->cursor += strLen + 1;

	return pgduck_client_to_server(str, strLen);
}

/* --------------------------------
 *		pq_getmsgint	- get a binary integer from a message buffer
 *
 *		Values are treated as unsigned.
 * --------------------------------
 */
unsigned int
pq_getmsgint(StringInfo msg, int b, bool *readFailed)
{
	unsigned int result;
	unsigned char n8;
	uint16		n16;
	uint32		n32;

	*readFailed = false;

	switch (b)
	{
		case 1:
			pq_copymsgbytes(msg, (char *) &n8, 1, readFailed);
			result = n8;
			break;
		case 2:
			pq_copymsgbytes(msg, (char *) &n16, 2, readFailed);
			result = pg_ntoh16(n16);
			break;
		case 4:
			pq_copymsgbytes(msg, (char *) &n32, 4, readFailed);
			result = pg_ntoh32(n32);
			break;
		default:
			PGDUCK_SERVER_ERROR("unsupported integer size %d", b);
			*readFailed = true;

			result = 0;
			break;
	}
	return result;
}


/* --------------------------------
 *		pq_copymsgbytes - copy raw data from a message buffer
 *
 *		Same as above, except data is copied to caller's buffer.
 * --------------------------------
 */
void
pq_copymsgbytes(StringInfo msg, char *buf, int datalen, bool *readFailed)
{
	if (datalen < 0 || datalen > (msg->len - msg->cursor))
	{
		PGDUCK_SERVER_ERROR("insufficient data left in message");
		*readFailed = true;
		return;
	}

	*readFailed = false;
	memcpy(buf, &msg->data[msg->cursor], datalen);
	msg->cursor += datalen;
}


/* --------------------------------
 *		pq_getmsgend	- verify message fully consumed
 * --------------------------------
 */
int
pq_getmsgend(StringInfo msg)
{
	if (msg->cursor != msg->len)
	{
		PGDUCK_SERVER_ERROR("invalid message format");
		return EOF;
	}

	return 0;
}


/* --------------------------------
 *		pq_sendcountedtext - append a counted text string (with character set conversion)
 *
 * The data sent to the frontend by this routine is a 4-byte count field
 * followed by the string.  The count includes itself or not, as per the
 * countincludesself flag (pre-3.0 protocol requires it to include itself).
 * The passed text string need not be null-terminated, and the data sent
 * to the frontend isn't either.
 * --------------------------------
 */
void
pq_sendcountedtext(StringInfo buf, const char *str, int slen,
				   bool countincludesself)
{
	int			extra = countincludesself ? 4 : 0;
	char	   *p;

	p = pgduck_client_to_server(str, slen);
	if (p != str)				/* actual conversion has been done? */
	{
		slen = strlen(p);
		pq_sendint32(buf, slen + extra);
		appendBinaryStringInfoNT(buf, p, slen);
		free(p);
	}
	else
	{
		pq_sendint32(buf, slen + extra);
		appendBinaryStringInfoNT(buf, str, slen);
	}
}


/* --------------------------------
 *		pq_getmsgbytes	- get raw data from a message buffer
 *
 *		Returns a pointer directly into the message buffer; note this
 *		may not have any particular alignment.
 * --------------------------------
 */
const char *
pq_getmsgbytes(StringInfo msg, int datalen)
{
	const char *result;

	if (datalen < 0 || datalen > (msg->len - msg->cursor))
		PGDUCK_SERVER_ERROR("insufficient data left in message");

	result = &msg->data[msg->cursor];
	msg->cursor += datalen;
	return result;
}


/* --------------------------------
 *		pq_getmsgbyte	- get a raw byte from a message buffer
 * --------------------------------
 */
int
pq_getmsgbyte(StringInfo msg)
{
	if (msg->cursor >= msg->len)
		PGDUCK_SERVER_ERROR("insufficient data left in message");

	return (unsigned char) msg->data[msg->cursor++];
}
