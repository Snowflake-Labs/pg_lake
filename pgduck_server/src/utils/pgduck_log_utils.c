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

#include <stddef.h>

#include "utils/pgduck_log_utils.h"
#include "utils/pg_log_utils.h"

/* Structure to hold error code and corresponding string */
typedef struct ErrorCode
{
	int			code;
	const char *description;
} ErrorCode;


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
