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
* url_encode.c
* URL-encodes a given string
*/

#include <ctype.h>
#include <string.h>
#include <postgres.h>
#include <fmgr.h>
#include "lib/stringinfo.h"

#include "pg_lake/util/url_encode.h"

#define URI_RESERVED " !~*'();/?:@&=+$,#"

static char get_low_nibble(char byte);
static char get_high_nibble(char byte);
static char to_hex_char(char value);


/*
 * Function to URL-encode a given string, escaping non-unreserved characters
* It returns a newly palloced string.
*/
char *
URLEncodePath(const char *input)
{
	const char *input_ptr = input;
	char	   *encoded_str = palloc0(strlen(input) * 3 + 1);	/* Allocate enough space
																 * for the worst-case
																 * scenario */
	char	   *encoded_ptr = encoded_str;

	/* characters considered unreserved in the URI specification */

	while (*input_ptr)
	{
		/*
		 * If the character is alphanumeric or an unreserved special
		 * character, keep it as is.
		 */
		if (isalnum(*input_ptr) || strchr(URI_RESERVED, *input_ptr) == NULL)
		{
			*encoded_ptr++ = *input_ptr;
		}
		else
		{
			/*
			 * For all other characters, encode them as '%HH' where HH is the
			 * hex representation.
			 */
			*encoded_ptr++ = '%';
			/* first hex digit(high nibble) */
			*encoded_ptr++ = to_hex_char(get_high_nibble(*input_ptr));
			/* second hex digit(low nibble) */
			*encoded_ptr++ = to_hex_char(get_low_nibble(*input_ptr));
		}

		input_ptr++;
	}
	*encoded_ptr = '\0';

	return encoded_str;
}


/*
 * URLEncodePrefix URL-encodes a REST catalog prefix, keeping '/' as a
 * path separator. Iceberg REST prefixes may span several path segments
 * (e.g. Unity Catalog's "catalogs/<name>"), and the reference client
 * joins the prefix into the path as-is. Each segment is encoded with
 * URLEncodePath, so other reserved characters are still escaped.
 * It returns a newly palloced string.
 */
char *
URLEncodePrefix(const char *input)
{
	StringInfoData encoded;
	const char *segmentStart = input;

	initStringInfo(&encoded);

	for (;;)
	{
		const char *slash = strchr(segmentStart, '/');
		size_t		segmentLen = slash ? (size_t) (slash - segmentStart) : strlen(segmentStart);
		char	   *segment = pnstrdup(segmentStart, segmentLen);

		appendStringInfoString(&encoded, URLEncodePath(segment));

		if (slash == NULL)
			break;

		appendStringInfoChar(&encoded, '/');
		segmentStart = slash + 1;
	}

	return encoded.data;
}


/*
* Helper function to convert a half-byte (nibble) to
* its hexadecimal character representation.
*/
static char
to_hex_char(char value)
{
	static char hex_chars[] = "0123456789abcdef";

	return hex_chars[value & 15];
}

/*
 * Helper function to extract the high nibble
 * (first 4 bits) of a byte.
 */
static char
get_high_nibble(char byte)
{
	return (byte >> 4) & 15;
}

/* Helper function to extract the low nibble (last 4 bits) of a byte */
static char
get_low_nibble(char byte)
{
	return byte & 15;
}
