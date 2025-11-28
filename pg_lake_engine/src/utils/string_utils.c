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
#include "fmgr.h"

#include "access/htup_details.h"
#include "catalog/pg_collation_d.h"
#include "pg_lake/util/string_utils.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/uuid.h"

/*
* PgLakeReplaceText is a wrapper around SQL function replace_text.
*/
char *
PgLakeReplaceText(const char *source, const char *fromSubstitute,
				  const char *toSubstitute)
{
	/* Check for null input */
	if (source == NULL)
		return NULL;

	/* Call the replace function using DirectFunctionCall3 */
	text	   *resultText =
		DatumGetTextP(DirectFunctionCall3Coll(replace_text, DEFAULT_COLLATION_OID,
											  CStringGetTextDatum(source),
											  CStringGetTextDatum(fromSubstitute),
											  CStringGetTextDatum(toSubstitute)));

	/* result is palloced by text_to_cstring */
	char	   *result = text_to_cstring(resultText);

	return result;
}


pg_uuid_t *
GeneratePGUUID(void)
{
	pg_uuid_t  *uuid = palloc(UUID_LEN);

	if (!pg_strong_random(uuid, UUID_LEN))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate random values")));
	}

	/*
	 * Set magic numbers for a "version 4" (pseudorandom) UUID, see
	 * http://tools.ietf.org/html/rfc4122#section-4.4
	 */
	uuid->data[6] = (uuid->data[6] & 0x0f) | 0x40;	/* time_hi_and_version */
	uuid->data[8] = (uuid->data[8] & 0x3f) | 0x80;	/* clock_seq_hi_and_reserved */

	return uuid;
}


/*
 * GenerateUUID generates a random UUID.
 */
char *
GenerateUUID(void)
{
	pg_uuid_t  *uuid = GeneratePGUUID();
	static const char hex_chars[] = "0123456789abcdef";
	StringInfoData buf;
	int			charIndex;

	initStringInfo(&buf);

	for (charIndex = 0; charIndex < UUID_LEN; charIndex++)
	{
		int			hi;
		int			lo;

		/*
		 * We print uuid values as a string of 8, 4, 4, 4, and then 12
		 * hexadecimal characters, with each group is separated by a hyphen
		 * ("-"). Therefore, add the hyphens at the appropriate places here.
		 */
		if (charIndex == 4 || charIndex == 6 || charIndex == 8 || charIndex == 10)
			appendStringInfoChar(&buf, '-');

		hi = uuid->data[charIndex] >> 4;
		lo = uuid->data[charIndex] & 0x0F;

		appendStringInfoChar(&buf, hex_chars[hi]);
		appendStringInfoChar(&buf, hex_chars[lo]);
	}

	return buf.data;
}


/*
 * StripFromChar truncates a string to the first occurrence of a character or
 * returns the truncated string, or the original string if the character does
 * not appear.
 */
char *
StripFromChar(char *input, char ch)
{
	char	   *charPointer = strchr(input, '?');

	if (charPointer == NULL)
		return input;

	size_t		truncatedLength = (size_t) (charPointer - input);
	char	   *truncatedString = palloc(truncatedLength + 1);

	/* copy the first truncatedLength bytes and adds a 0 byte */
	strlcpy(truncatedString, input, truncatedLength + 1);

	return truncatedString;
}


/*
 * ToLowerCase converts a string to lower case.
 */
char *
ToLowerCase(char *input)
{
	if (input == NULL)
		return NULL;

	char	   *result = palloc(strlen(input) + 1);
	char	   *p = result;

	while (*input)
	{
		*p++ = tolower((unsigned char) *input);
		input++;
	}

	*p = '\0';

	return result;
}


/*
 * Returns output similar to postgres_fdw's deparseStringLiteral, so always in
 * the `E'blah'` format.  This is compatible back to 8.1, so long past the
 * point that we care.  This is more appropriate than quote_literal_cstr when
 * dealing with remote servers which may have other
 * standard_conforming_strings settings, etc.
 */
char *
EscapedStringLiteral(const char *val)
{
	StringInfoData buf;

	initStringInfo(&buf);

	const char *valptr;

	/*
	 * Rather than making assumptions about the remote server's value of
	 * standard_conforming_strings, always use E'foo' syntax if there are any
	 * backslashes.  This will fail on remote servers before 8.1, but those
	 * are long out of support.
	 */
	if (strchr(val, '\\') != NULL)
		appendStringInfoCharMacro(&buf, ESCAPE_STRING_SYNTAX);
	appendStringInfoChar(&buf, '\'');
	for (valptr = val; *valptr; valptr++)
	{
		char		ch = *valptr;

		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoCharMacro(&buf, ch);
		appendStringInfoCharMacro(&buf, ch);
	}
	appendStringInfoCharMacro(&buf, '\'');
	return buf.data;
}


/*
 * ReverseStringSearch is like strstr, but in reverse.
 */
char *
ReverseStringSearch(const char *haystack, const char *needle)
{
	char	   *result = NULL;
	const char *remainder = haystack;

	while ((remainder = strstr(remainder, needle)) != NULL)
	{
		/* found a substring */
		result = (char *) remainder;

		/* keep looking in case there is another occurrence */
		remainder++;
	}

	return result;
}


/*
 * AdjustAnyCharTypmod adjusts the typmod of a character type to the new length.
 * It returns the new typmod for VARCHAR(newLength) or BPCHAR(newLength).
 *
 * Taken from PostgreSQL's varchar.c. (anychar_typmodin)
 */
int32_t
AdjustAnyCharTypmod(int32_t typmod, int32_t newLength)
{
	return VARHDRSZ + newLength;
}


/*
 * GetAnyCharLengthFrom returns the length of a character type from its typmod.
 */
int32_t
GetAnyCharLengthFrom(int32_t typmod)
{
	if (typmod < VARHDRSZ)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid typmod for anychar type: %d", typmod)));
	}

	return typmod - VARHDRSZ;
}


/*
* PgStrcasecmpNullable compares two strings for equality, treating NULLs as equal.
*/
bool
PgStrcasecmpNullable(const char *a, const char *b)
{
	if (a == NULL && b == NULL)
		return true;

	if (a == NULL || b == NULL)
		return false;

	return pg_strcasecmp(a, b) == 0;
}
