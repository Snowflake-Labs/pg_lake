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
 * http_client.h
 * Simple HTTP GET/POST wrapper for PostgreSQL extensions
 */
#pragma once

#include "postgres.h"
#include "nodes/pg_list.h"

typedef enum
{
	HTTP_GET,
	HTTP_HEAD,
	HTTP_POST,
	HTTP_PUT,
	HTTP_DELETE
}			HttpMethod;

typedef struct
{
	long		status;			/* e.g., 200, 404        */
	char	   *body;			/* full response body    */
	size_t		bodyLength;		/* length of response body */
	char	   *headers;		/* raw response headers  */
	size_t		headersLength;	/* length of response headers */
	const char *errorMsg;		/* error message */
}			HttpResult;

extern bool HttpClientTraceTraffic;

/* plain C API (no PostgreSQL types) */
extern PGDLLEXPORT HttpResult HttpWithRetry(HttpMethod method, const char *url, const char *body, List *headers, int retryCount);
