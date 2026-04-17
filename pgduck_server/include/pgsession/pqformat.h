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
 * pqformat.h
 *		Definitions for formatting and parsing frontend/backend messages
 *
 * Follows a similar structure with src/include/libpq/pqformat.h
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 *-------------------------------------------------------------------------
 */
#ifndef PQFORMAT_H
#define PQFORMAT_H

#include "c.h"

#include "lib/stringinfo.h"
#include "mb/pg_wchar.h"
#include "port/pg_bswap.h"

#include "pgsession/pgsession.h"
#include "pgsession/pgsession_io.h"
#include "pgsession/pqformat.h"


/*
 * Append a [u]int8 to a StringInfo buffer, which already has enough space
 * preallocated.
 *
 * The use of pg_restrict allows the compiler to optimize the code based on
 * the assumption that buf, buf->len, buf->data and *buf->data don't
 * overlap. Without the annotation buf->len etc cannot be kept in a register
 * over subsequent pq_writeintN calls.
 *
 * The use of StringInfoData * rather than StringInfo is due to MSVC being
 * overly picky and demanding a * before a restrict.
 *
 * Copied verbatim from src/include/libpq/pqformat.h
 * except for Assert.
 */
static inline void
pq_writeint8(StringInfoData *pg_restrict buf, uint8 value)
{
	uint8		ni = value;

	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint8));
	buf->len += sizeof(uint8);
}

/*
 * Append a [u]int16 to a StringInfo buffer, which already has enough space
 * preallocated.
 *
 * Copied verbatim from src/include/libpq/pqformat.h
 * except for Assert.
 */
static inline void
pq_writeint16(StringInfoData *pg_restrict buf, uint16 value)
{
	uint16		ni = pg_hton16(value);

	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint16));
	buf->len += sizeof(uint16);
}

/*
 * Append a [u]int32 to a StringInfo buffer, which already has enough space
 * preallocated.
 *
 * Copied verbatim from src/include/libpq/pqformat.h except for Assert.
 */
static inline void
pq_writeint32(StringInfoData *pg_restrict buf, uint32 value)
{
	uint32		ni = pg_hton32(value);

	memcpy((char *pg_restrict) (buf->data + buf->len), &ni, sizeof(uint32));
	buf->len += sizeof(uint32);
}


/*
 * Append a binary [u]int8 to a StringInfo buffer
 *
 * Copied verbatim from src/include/libpq/pqformat.h.
 */
static inline void
pq_sendint8(StringInfo buf, uint8 value)
{
	enlargeStringInfo(buf, sizeof(uint8));
	pq_writeint8(buf, value);
}

/*
 * Append a binary [u]int16 to a StringInfo buffer.
 *
 * Copied verbatim from src/include/libpq/pqformat.h.
 */
static inline void
pq_sendint16(StringInfo buf, uint16 value)
{
	enlargeStringInfo(buf, sizeof(uint16));
	pq_writeint16(buf, value);
}

/*
 * Append a binary [u]int32 to a StringInfo buffer.
 *
 * Copied verbatim from src/include/libpq/pqformat.h.
 */
static inline void
pq_sendint32(StringInfo buf, uint32 value)
{
	enlargeStringInfo(buf, sizeof(uint32));
	pq_writeint32(buf, value);
}


/*
 * Append a binary byte to a StringInfo buffer.
 *
 * Copied verbatim from src/include/libpq/pqformat.h.
 */
static inline void
pq_sendbyte(StringInfo buf, uint8 byte)
{
	pq_sendint8(buf, byte);
}


/*
 * Append raw data to a StringInfo buffer.
 *
 * Copied verbatim from src/include/libpq/pqformat.h.
 */
static inline void
pq_sendbytes(StringInfo buf, const void *data, int datalen)
{
	/* use variant that maintains a trailing null-byte, out of caution */
	appendBinaryStringInfo(buf, data, datalen);
}


/*
 * Append a null-terminated text string (with conversion) to a buffer with
 * preallocated space.
 *
 * NB: The pre-allocated space needs to be sufficient for the string after
 * converting to client encoding.
 *
 * NB: passed text string must be null-terminated, and so is the data
 * sent to the frontend.
 */
static inline void
pq_writestring(StringInfoData *pg_restrict buf, const char *pg_restrict str)
{
	int			slen = strlen(str);
	char	   *p;

	p = pgduck_client_to_server(str, slen);
	if (p != str)				/* actual conversion has been done? */
		slen = strlen(p);

	memcpy(((char *pg_restrict) buf->data + buf->len), p, slen + 1);
	buf->len += slen + 1;

	if (p != str)
		free(p);
}


extern void pq_beginmessage(StringInfo buf, char msgtype);
extern void pq_beginmessage_reuse(StringInfo buf, char msgtype);
extern int	pq_endmessage(PGSession *session, StringInfo buf);
extern int	pq_endmessage_reuse(PGSession *session, StringInfo buf);
extern const char *pq_getmsgstring(StringInfo msg);
extern unsigned int pq_getmsgint(StringInfo msg, int b, bool *readFailed);
extern void pq_copymsgbytes(StringInfo msg, char *buf, int datalen, bool *readFailed);
extern int	pq_getmsgend(StringInfo msg);
extern void pq_sendcountedtext(StringInfo buf, const char *str, int slen,
							   bool countincludesself);
extern const char *pq_getmsgbytes(StringInfo msg, int datalen);
extern int	pq_getmsgbyte(StringInfo msg);
#endif							/* PQFORMAT_H */
