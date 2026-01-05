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
 * http_client.c  – minimal HTTP client for PostgreSQL using libcurl
 *
 * Features
 *   • Never throws error (caller calls it at post-commit hook)
 *   • Only allows http / https protocols
 *   • 1s connect timeout, 5s total timeout (change the constants below if needed)
 *   • Follows up to 5 redirects
 *   • Cancels cleanly on any failure, including Ctrl-C or statement_timeout fires
 */
#include "postgres.h"
#include "miscadmin.h"
#include "storage/ipc.h"
#include "utils/memutils.h"
#include "pg_lake/http/http_client.h"

#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <curl/curl.h>

/* 20 seconds */
#define CONNECT_TIMEOUT_MS  20000

/* 180 seconds */
#define TOTAL_TIMEOUT_MS   180000


static HttpResult SendHttpRequest(HttpMethod method, const char *url, const char *body, List *headers);
static HttpResult HttpCommonNoThrows(HttpMethod method, const char *url, const char *postData,
									 const List *headers);
static bool CheckMinCurlVersion(const curl_version_info_data * versionInfo);
static size_t CurlResponseBodyWriteCallback(void *ptr, size_t size, size_t nmemb, void *userdata);
static size_t CurlResponseHeaderWriteCallback(void *ptr, size_t size, size_t nmemb, void *userdata);
static void GrowResponseData(char **data, size_t *dataLength, void *newData, size_t newDataBytes);
#if CURL_AT_LEAST_VERSION(7, 32, 0)
static int	CurlProgressCallback(void *clientp, curl_off_t dltotal, curl_off_t dlnow,
								 curl_off_t ultotal, curl_off_t ulnow);
#endif
static void CurlLogError(CURLcode curlCode, const char *error_buffer);
static CURLcode CurlGloballyInitIfNotInitialized(void);
static CURLcode CurlSetErrorBuffer(CURL * curl, char **errorBuffer);
static CURLcode CurlSetOptions(CURL * curl, const char *url, HttpMethod method,
							   const char *postData, HttpResult * httpResult);
static CURLcode CurlSetHeaders(CURL * curl, const List *headers, struct curl_slist **headerList);
static void CurlGlobalCleanup(int code, Datum arg);
static void CurlCleanup(CURL * curl, struct curl_slist *headerList);
static HttpResult CurlReturnError(CURL * curl, struct curl_slist *headerList,
								  CURLcode curlCode, const char *errorMsg);
static const char *HttpRequestMethodToString(HttpMethod method);
static char *RedactSensitiveJson(char *s);

#define CURL_SETOPT(curl, opt, value) do { \
	curlCode = curl_easy_setopt((curl), (opt), (value)); \
	if ( curlCode != CURLE_OK ) \
	{ \
		return curlCode; \
	} \
	} while (0);

static bool curlInitialized = false;


bool		HttpClientTraceTraffic = false;


/*
 * CurlGloballyInitIfNotInitialized globally initiates curl state if not initialized.
 */
static CURLcode
CurlGloballyInitIfNotInitialized(void)
{
	if (curlInitialized)
		return CURLE_OK;

	ereport(DEBUG4, (errmsg("initializing libcurl globally")));

	/* ensure cleanup at exit */
	on_proc_exit(CurlGlobalCleanup, 0);

	/* one time initialization for libcurl */
	CURLcode	curlCode = curl_global_init(CURL_GLOBAL_ALL);

	if (curlCode != CURLE_OK)
		return curlCode;

	curlInitialized = true;

	return CURLE_OK;
}


/*
 * CurlSetErrorBuffer sets up the error buffer for a given CURL handle.
 */
static CURLcode
CurlSetErrorBuffer(CURL * curl, char **errorBuffer)
{
	ereport(DEBUG4, (errmsg("setting libcurl error buffer")));

	CURLcode	curlCode = CURLE_OK;

	*errorBuffer = palloc_extended(CURL_ERROR_SIZE, MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);

	if (*errorBuffer == NULL)
		return CURLE_OUT_OF_MEMORY;

	/* Set up the error buffer */
	CURL_SETOPT(curl, CURLOPT_ERRORBUFFER, *errorBuffer);

	return curlCode;
}


/*
 * CurlSetOptions sets the options for a given CURL handle.
 */
static CURLcode
CurlSetOptions(CURL * curl, const char *url, HttpMethod method,
			   const char *postData, HttpResult * res)
{
	ereport(DEBUG4, (errmsg("setting libcurl options")));

	CURLcode	curlCode = CURLE_OK;

	/* Basic safety-related options */
	CURL_SETOPT(curl, CURLOPT_URL, url);
#if CURL_AT_LEAST_VERSION(7, 85, 0)
	CURL_SETOPT(curl, CURLOPT_PROTOCOLS_STR, "http,https");
	CURL_SETOPT(curl, CURLOPT_REDIR_PROTOCOLS_STR, "http,https");
#else
	CURL_SETOPT(curl, CURLOPT_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
	CURL_SETOPT(curl, CURLOPT_REDIR_PROTOCOLS, CURLPROTO_HTTP | CURLPROTO_HTTPS);
#endif
	CURL_SETOPT(curl, CURLOPT_FOLLOWLOCATION, 1L);
	CURL_SETOPT(curl, CURLOPT_CONNECTTIMEOUT_MS, CONNECT_TIMEOUT_MS);
	CURL_SETOPT(curl, CURLOPT_TIMEOUT_MS, TOTAL_TIMEOUT_MS);

	/* Connect the progress callback for interrupt support */
#if CURL_AT_LEAST_VERSION(7, 32, 0)
	CURL_SETOPT(curl, CURLOPT_XFERINFOFUNCTION, CurlProgressCallback);
	CURL_SETOPT(curl, CURLOPT_NOPROGRESS, 0L);
#endif

	/* response body buffer */
	CURL_SETOPT(curl, CURLOPT_WRITEFUNCTION, CurlResponseBodyWriteCallback);
	CURL_SETOPT(curl, CURLOPT_WRITEDATA, res);

	/* response header buffer */
	CURL_SETOPT(curl, CURLOPT_HEADERFUNCTION, CurlResponseHeaderWriteCallback);
	CURL_SETOPT(curl, CURLOPT_HEADERDATA, res);

	/* Method-specific setup */
	switch (method)
	{
		case HTTP_GET:
			CURL_SETOPT(curl, CURLOPT_CUSTOMREQUEST, "GET");
			break;
		case HTTP_POST:
			CURL_SETOPT(curl, CURLOPT_POST, 1L);
			CURL_SETOPT(curl, CURLOPT_POSTFIELDS, postData);
			break;
		case HTTP_PUT:
			CURL_SETOPT(curl, CURLOPT_CUSTOMREQUEST, "PUT");
			CURL_SETOPT(curl, CURLOPT_POSTFIELDS, postData);
			break;
		case HTTP_DELETE:
			CURL_SETOPT(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
			break;
		case HTTP_HEAD:
			CURL_SETOPT(curl, CURLOPT_NOBODY, 1L);
			break;
		default:				/* GET needs nothing extra */
			break;
	}

	return curlCode;
}


/*
 * CurlSetHeaders sets the headers for a given CURL handle.
 */
static CURLcode
CurlSetHeaders(CURL * curl, const List *headers, struct curl_slist **headerList)
{
	ereport(DEBUG4, (errmsg("setting libcurl headers")));

	ListCell   *headerCell;

	foreach(headerCell, headers)
	{
		const char *header = (const char *) lfirst(headerCell);

		*headerList = curl_slist_append(*headerList, header);

		if (*headerList == NULL)
			return CURLE_OUT_OF_MEMORY;
	}

	CURLcode	curlCode = CURLE_OK;

	if (*headerList)
		CURL_SETOPT(curl, CURLOPT_HTTPHEADER, *headerList);

	return curlCode;
}


/*
 * CurlGlobalCleanup globally cleans up curl state.
 */
static void
CurlGlobalCleanup(int code, Datum arg)
{
	if (curlInitialized)
		curl_global_cleanup();
}


/*
 * CurlCleanup cleans up given curl handle and headers.
 */
static void
CurlCleanup(CURL * curl, struct curl_slist *headerList)
{
	ereport(DEBUG4, (errmsg("cleaning up libcurl")));

	if (headerList)
		curl_slist_free_all(headerList);

	if (curl)
		curl_easy_cleanup(curl);
}


/*
 * CurlReturnError handles errors from libcurl and finally returns an HttpResult
 * with the error message.
 */
static HttpResult
CurlReturnError(CURL * curl, struct curl_slist *headerList,
				CURLcode curlCode, const char *errorMsg)
{
	CurlLogError(curlCode, errorMsg);

	CurlCleanup(curl, headerList);

	HttpResult	errorRes = {0};

	errorRes.errorMsg = errorMsg;

	return errorRes;
}


/*
 * SendHttpRequestWithRetry sends an HTTP request with the given method, url, body, headers,
 * and retry callback.
 */
HttpResult
SendHttpRequestWithRetry(HttpMethod method, const char *url, const char *body,
						 List *headers, HttpRetryFn retryFn, int maxRetry)
{
	Assert(maxRetry > 0);

	HttpResult	result;

	for (int retryNo = 1; retryNo <= maxRetry; retryNo++)
	{
		result = SendHttpRequest(method, url, body, headers);

		if (retryFn != NULL && retryFn(result.status, maxRetry, retryNo))
			continue;
		else
			break;
	}

	return result;
}


static HttpResult
SendHttpRequest(HttpMethod method, const char *url, const char *body, List *headers)
{
	HttpResult	result;

	if (method == HTTP_GET)
	{
		Assert(body == NULL);
		result = HttpGet(url, headers);
	}
	else if (method == HTTP_HEAD)
	{
		Assert(body == NULL);
		result = HttpHead(url, headers);
	}
	else if (method == HTTP_POST)
	{
		result = HttpPost(url, body, headers);
	}
	else if (method == HTTP_PUT)
	{
		result = HttpPut(url, body, headers);
	}
	else if (method == HTTP_DELETE)
	{
		Assert(body == NULL);
		result = HttpDelete(url, headers);
	}
	else
	{
		pg_unreachable();
	}

	return result;
}


/*
 * LinearBackoffSleepMs returns sleep duration in milliseconds
 * for the current retry no using linear backoff with jitter.
 */
int
LinearBackoffSleepMs(int baseMs, int retryNo)
{
	const int	maxMs = 10000;	/* cap at 10 s */

	int			sleepMs = baseMs * retryNo;

	if (sleepMs > maxMs)
		sleepMs = maxMs;

	/* add some jitter up to baseMs */
	sleepMs += (rand() % baseMs);

	return sleepMs;
}


/*
 * HttpGet performs a simple HTTP GET request.
 * Returns an HttpResult with status, body, and headers.
 */
HttpResult
HttpGet(const char *url, List *headers)
{
	return HttpCommonNoThrows(HTTP_GET, url, NULL, headers);
}

HttpResult
HttpHead(const char *url, List *headers)
{
	/* HEAD never carries a body */
	return HttpCommonNoThrows(HTTP_HEAD, url, NULL, headers);
}

/*
* HttpPost performs a simple HTTP POST request.
 * Returns an HttpResult with status, body, and headers.
 */
HttpResult
HttpPost(const char *url, const char *body, List *headers)
{
	return HttpCommonNoThrows(HTTP_POST, url, body, headers);
}


HttpResult
HttpPut(const char *url, const char *body, List *headers)
{
	return HttpCommonNoThrows(HTTP_PUT, url, body, headers);
}


HttpResult
HttpDelete(const char *url, List *headers)
{
	return HttpCommonNoThrows(HTTP_DELETE, url, NULL, headers);
}


/* CurlResponseBodyWriteCallback grows response body from libcurl buffer. */
static size_t
CurlResponseBodyWriteCallback(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	size_t		bytes = size * nmemb;

	if (bytes == 0 || userdata == NULL)
		return bytes;			/* nothing to do */

	HttpResult *res = (HttpResult *) userdata;

	GrowResponseData(&res->body, &res->bodyLength, ptr, bytes);

	/* success: tell curl we consumed all */
	return bytes;
}


/* CurlResponseHeaderWriteCallback grows response headers from libcurl buffer. */
static size_t
CurlResponseHeaderWriteCallback(void *ptr, size_t size, size_t nmemb, void *userdata)
{
	size_t		bytes = size * nmemb;

	if (bytes == 0 || userdata == NULL)
		return bytes;			/* nothing to do */

	HttpResult *res = (HttpResult *) userdata;

	GrowResponseData(&res->headers, &res->headersLength, ptr, bytes);

	/* success: tell curl we consumed all */
	return bytes;
}


/*
 * GrowResponseData grows the response data buffers.
 */
static void
GrowResponseData(char **data, size_t *dataLength, void *newData, size_t newDataBytes)
{
	/* ensure null terminated */
	if (*data == NULL)
		*data = palloc_extended(newDataBytes + 1, MCXT_ALLOC_NO_OOM | MCXT_ALLOC_ZERO);
	else
	{
		*data = repalloc_extended(*data, *dataLength + newDataBytes + 1, MCXT_ALLOC_NO_OOM);
		memset(*data + *dataLength, 0, newDataBytes + 1);	/* zero out new space */
	}

	if (*data == NULL)
	{
		/*
		 * signals an error to curl when return value is different than the
		 * amount passed to callback
		 */
		*dataLength = 0;
		return;
	}

	memcpy(*data + *dataLength, newData, newDataBytes);

	*dataLength += newDataBytes;
}

/*
* To support request interruption, we have libcurl run the progress meter
* callback frequently, and here we watch to see if PgSQL has flipped
* the global QueryCancelPending || ProcDiePending flags.
* Curl should then return CURLE_ABORTED_BY_CALLBACK
* to the curl_easy_perform() call.
*/
#if CURL_AT_LEAST_VERSION(7, 32, 0)
static int
CurlProgressCallback(void *clientp, curl_off_t dltotal, curl_off_t dlnow, curl_off_t ultotal, curl_off_t ulnow)
{
	/* Check the PgSQL global flags */
	return QueryCancelPending || ProcDiePending;
}
#endif

/*
 * CurlLogError logs the error string from libcurl.
 * If the error buffer is not empty, it logs that message too.
 */
static void
CurlLogError(CURLcode curlCode, const char *error_buffer)
{
	ereport(WARNING, (errmsg("%s", curl_easy_strerror(curlCode))));

	if (strlen(error_buffer) > 0)
		ereport(WARNING, (errmsg("%s", error_buffer)));
}


/*
 * CheckMinCurlVersion checks if curl version >= the minimum required version.
 */
static bool
CheckMinCurlVersion(const curl_version_info_data * versionInfo)
{
	elog(DEBUG4, "curl version %s", versionInfo->version);
	elog(DEBUG4, "curl version number 0x%x", versionInfo->version_num);
	elog(DEBUG4, "ssl version %s", versionInfo->ssl_version);

	const unsigned int CURL_MIN_VERSION = CURL_VERSION_BITS(7, 20, 0);

	return versionInfo->version_num >= CURL_MIN_VERSION;
}


/*
 * HttpCommonNoThrows is the common implementation for http requests.
 * It initializes libcurl, sets options, performs the request, and returns
 * an HttpResult.
 *
 * method is the HTTP method to use (GET, POST, etc.).
 * url is the URL to request.
 * postData is the data to send in a POST or PUT request, or NULL.
 * headers is a List of additional headers to include in the request.
 *
 * This function should not throw errors because we plan to use it in
 * post-commit hook.
 */
static HttpResult
HttpCommonNoThrows(HttpMethod method, const char *url, const char *postData, const List *headers)
{
	CURL	   *curl = NULL;
	struct curl_slist *curlHeaders = NULL;
	CURLcode	curlCode = CURLE_OK;

	if (HttpClientTraceTraffic && message_level_is_interesting(INFO))
	{
		StringInfo	postDataInfo = NULL;

		if (postData)
		{
			postDataInfo = makeStringInfo();
			appendStringInfo(postDataInfo, ", body: {%s}", postData);
		}

		ereport(INFO, (errmsg("making %s request to URL %s%s",
							  HttpRequestMethodToString(method), url,
							  postDataInfo ? RedactSensitiveJson(postDataInfo->data) : "")));
	}

	if (!CheckMinCurlVersion(curl_version_info(CURLVERSION_NOW)))
		return CurlReturnError(curl, curlHeaders, CURLE_FAILED_INIT, "pg_lake_iceberg requires Curl version 7.20.0 or higher");

	curlCode = CurlGloballyInitIfNotInitialized();

	if (curlCode != CURLE_OK)
		return CurlReturnError(curl, curlHeaders, curlCode, "failed to globally initialize libcurl");

	ereport(DEBUG4, (errmsg("initializing libcurl handle")));

	curl = curl_easy_init();

	if (!curl)
		return CurlReturnError(curl, curlHeaders, CURLE_FAILED_INIT, "failed to initialize libcurl");

	/* Set up the error buffer */
	char	   *curlErrorBuffer = NULL;

	curlCode = CurlSetErrorBuffer(curl, &curlErrorBuffer);

	if (curlCode != CURLE_OK)
		return CurlReturnError(curl, curlHeaders, curlCode, "failed to set libcurl error buffer");

	Assert(curlErrorBuffer != NULL);

	/* set curl options */
	HttpResult	res = {0};

	curlCode = CurlSetOptions(curl, url, method, postData, &res);

	if (curlCode != CURLE_OK)
		return CurlReturnError(curl, curlHeaders, curlCode, curlErrorBuffer);


	/* set curl headers */
	curlCode = CurlSetHeaders(curl, headers, &curlHeaders);

	if (curlCode != CURLE_OK)
		return CurlReturnError(curl, curlHeaders, curlCode, curlErrorBuffer);

	/* perform curl request */
	ereport(DEBUG4, (errmsg("performing libcurl request")));

	curlCode = curl_easy_perform(curl);

	if (curlCode != CURLE_OK)
		return CurlReturnError(curl, curlHeaders, curlCode, curlErrorBuffer);

	/* fetch curl response code */
	ereport(DEBUG4, (errmsg("fetching libcurl response status code")));

	curlCode = curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &res.status);

	if (curlCode != CURLE_OK)
		return CurlReturnError(curl, curlHeaders, curlCode, curlErrorBuffer);

	/* curl cleanup */
	CurlCleanup(curl, curlHeaders);

	ereport(DEBUG4, (errmsg("libcurl request completed successfully")));

	if (HttpClientTraceTraffic && message_level_is_interesting(INFO))
	{
		ereport(INFO, (errmsg("received response with status code %ld, body: %s",
							  res.status, res.body ? RedactSensitiveJson(res.body) : "<empty>")));
	}

	return res;
}


static const char *
HttpRequestMethodToString(HttpMethod method)
{
	switch (method)
	{
		case HTTP_GET:
			return "GET";
		case HTTP_HEAD:
			return "HEAD";
		case HTTP_POST:
			return "POST";
		case HTTP_PUT:
			return "PUT";
		case HTTP_DELETE:
			return "DELETE";
		default:
			return "UNKNOWN";
	}
}


/*
 * RedactSensitiveJson
 *   In-place redaction of token-looking values in JSON-ish text.
 */
static char *
RedactSensitiveJson(char *input)
{
	if (input == NULL)
		return "NULL";

	/*
	 * Never touch the original input string, make a copy to redact.
	 */
	char	   *copyOfinput = pstrdup(input);

	const char *keys[] = {
		"\"access_token\"",
		"\"refresh_token\"",
		"\"id_token\"",
		"\"session_token\"",
		"\"token\"",
		"\"client_secret\"",
		"\"authorization\"",
		"\"Authorization\"",
		"\"s3.access-key-id\"",
		"\"s3.secret-access-key\"",
		"\"s3.session-token\"",
		"\"storage-credentials\""
	};
	const int	keyCount = sizeof(keys) / sizeof(keys[0]);

	for (int i = 0; i < keyCount; i++)
	{
		const char *key = keys[i];
		char	   *p = copyOfinput;

		while ((p = strstr(p, key)) != NULL)
		{
			/* Move to the colon after the key */
			char	   *colon = strchr(p + strlen(key), ':');

			if (colon == NULL)
			{
				/* No colon? then this isn't a key-value pair, skip */
				p += strlen(key);
				continue;
			}

			char	   *v = colon + 1;	/* start of value (maybe spaces /
										 * quote) */

			/* Skip whitespace */
			while (*v && isspace((unsigned char) *v))
				v++;

			int			quoted = 0;

			if (!v)
				return copyOfinput;

			if (*v == '"')
			{
				quoted = 1;
				v++;			/* move to first character of value */
			}

			char	   *q = v;

			if (quoted)
			{
				/* Redact until the closing quote */
				while (*q && *q != '"')
				{
					*q = '*';
					q++;
				}
			}
			else
			{
				/* Redact until comma, closing brace, or whitespace */
				while (*q &&
					   *q != ',' &&
					   *q != '}' &&
					   !isspace((unsigned char) *q))
				{
					*q = '*';
					q++;
				}
			}

			/* Continue search after the value we just redacted */
			p = q;
		}
	}

	return copyOfinput;
}
