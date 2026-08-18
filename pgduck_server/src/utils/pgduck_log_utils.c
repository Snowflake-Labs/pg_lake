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
 * Utility functions for string logs.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#include "c.h"

#include <ctype.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>

#include "utils/pgduck_log_utils.h"
#include "utils/pg_log_utils.h"

/* Structure to hold error code and corresponding string */
typedef struct ErrorCode
{
	int			code;
	const char *description;
}			ErrorCode;


/* Define the lookup table for error codes, derived from elog.h */
static const ErrorCode errorCodes[] = {
	{DEBUG5, "DEBUG5"},
	{DEBUG4, "DEBUG4"},
	{DEBUG3, "DEBUG3"},
	{DEBUG2, "DEBUG2"},
	{DEBUG1, "DEBUG1"},
	{LOG, "LOG"},
	{INFO, "INFO"},
	{NOTICE, "NOTICE"},
	{WARNING_CLIENT_ONLY, "WARNING_CLIENT_ONLY"},
	{ERROR, "ERROR"},
	{FATAL, "FATAL"},
	{PANIC, "PANIC"}
};


/*
 * StatementDefinesSecret reports whether a statement is the kind that
 * carries credentials in its argument list.
 *
 * Only the text before the argument list is examined, so a query that
 * merely mentions the word further along is not mistaken for one.
 */
static bool
StatementDefinesSecret(const char *queryString)
{
	const char *statement = queryString;

	while (isspace((unsigned char) *statement))
		statement++;

	if (pg_strncasecmp(statement, "CREATE", 6) != 0 &&
		pg_strncasecmp(statement, "ALTER", 5) != 0)
		return false;

	const char *arguments = strchr(statement, '(');
	size_t		headLength = arguments != NULL ?
		(size_t) (arguments - statement) : strlen(statement);

	for (size_t i = 0; i + 6 <= headLength; i++)
	{
		if (pg_strncasecmp(statement + i, "SECRET", 6) == 0)
			return true;
	}

	return false;
}


/*
 * QueryStringForLog returns the form of a statement that may be written
 * to the log.
 *
 * A failing statement is logged with its text at WARNING, and a session
 * may log every statement at DEBUG.  CREATE SECRET states an access key
 * and an STS session token in plain text, so for those the argument list
 * is dropped: the verb and the secret's name are what make the line
 * worth having, and neither is sensitive.
 *
 * Returns queryString itself when there is nothing to redact, so the
 * common path neither copies nor allocates.  Otherwise the redacted text
 * is written to buf, which the caller owns.
 */
const char *
QueryStringForLog(const char *queryString, char *buf, size_t bufSize)
{
	if (queryString == NULL || !StatementDefinesSecret(queryString))
		return queryString;

	const char *arguments = strchr(queryString, '(');

	if (arguments == NULL)
		return queryString;

	snprintf(buf, bufSize, "%.*s(<redacted>)",
			 (int) (arguments - queryString), queryString);

	return buf;
}


/*
 * Function to return the error string for a given code.
 */
const char *
GetErrorCodeStr(int code, bool *found)
{
	*found = false;
	for (size_t i = 0; i < sizeof(errorCodes) / sizeof(errorCodes[0]); i++)
	{
		if (errorCodes[i].code == code)
		{
			*found = true;
			return errorCodes[i].description;
		}
	}

	return "Unknown Error Code";
}
