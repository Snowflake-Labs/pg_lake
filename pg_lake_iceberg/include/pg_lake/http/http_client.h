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
#include "utils/guc.h"

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

/*
 * Client-certificate material for outbound HTTPS.  Empty means "not
 * configured", in which case libcurl keeps its defaults and no client
 * certificate is presented.  Catalogs that sit behind an mTLS edge need
 * these even when the request itself is authorized by a bearer token,
 * because the certificate governs admission rather than identity.
 *
 * The settings describe the deployment, not any one catalog, so a request
 * states whether it is addressed to that edge (see HttpTlsClientAuth) rather
 * than every request carrying them.
 */
extern char *HttpClientTlsCaFile;
extern char *HttpClientTlsCertFile;
extern char *HttpClientTlsKeyFile;

/*
 * Whether a request may present the deployment's client certificate.
 *
 * Only requests addressed to the edge that issued it may, and the certificate
 * authority above travels with it: it identifies that edge and replaces the
 * default bundle outright, so a request carrying it cannot verify a catalog
 * with an ordinary publicly-signed certificate.  Sending them separately gets
 * this wrong in both directions, which is why one value governs both.
 *
 * The default is to send neither, so a catalog reached some new way does not
 * silently inherit them.
 */
typedef enum HttpTlsClientAuth
{
	HTTP_TLS_NO_CLIENT_CERT = 0,
	HTTP_TLS_DEPLOYMENT_CLIENT_CERT
}			HttpTlsClientAuth;

/*
 * The three settings above are one credential and are only usable together: a
 * certificate cannot be offered without its key, and offering it while
 * verifying the peer against the public bundle would hand the deployment's
 * identity to any publicly-signed host a catalog happens to name.  So a
 * request presents all three or none, and a partial configuration is a
 * mistake to report rather than a state to work around.
 */
typedef enum HttpClientTlsMaterial
{
	HTTP_TLS_MATERIAL_ABSENT = 0,
	HTTP_TLS_MATERIAL_COMPLETE,
	HTTP_TLS_MATERIAL_PARTIAL
}			HttpClientTlsMaterial;

extern PGDLLEXPORT HttpClientTlsMaterial GetHttpClientTlsMaterial(void);
extern bool CheckHttpClientTlsFile(char **newval, void **extra, GucSource source);

#define HTTP_STATUS_UNAUTHORIZED		401
#define HTTP_STATUS_TOKEN_EXPIRED		419
#define HTTP_STATUS_TOO_MANY_REQUESTS	429
#define HTTP_STATUS_SERVICE_UNAVAILABLE	503

/* Callback function to determine if a request should be retried */
typedef bool (*HttpRetryFn) (long status, int maxRetry, int retryNo);

/* plain C API (no PostgreSQL types) */
extern PGDLLEXPORT HttpResult HttpGet(const char *url, List *headers);
extern PGDLLEXPORT HttpResult HttpHead(const char *url, List *headers);
extern PGDLLEXPORT HttpResult HttpPost(const char *url, const char *body, List *headers);
extern PGDLLEXPORT HttpResult HttpDelete(const char *url, List *headers);
extern PGDLLEXPORT HttpResult HttpPut(const char *url, const char *body, List *headers);
extern PGDLLEXPORT HttpResult SendHttpRequest(HttpMethod method, const char *url, const char *body, List *headers,
											  HttpTlsClientAuth clientAuth);
extern PGDLLEXPORT HttpResult SendHttpRequestWithRetry(HttpMethod method, const char *url, const char *body,
													   List *headers, HttpRetryFn retryFn, int maxRetry);
extern PGDLLEXPORT int LinearBackoffSleepMs(int baseMs, int retryNo);
