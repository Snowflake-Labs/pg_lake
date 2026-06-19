/*
 * Copyright 2026 Snowflake Inc.
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
 * REST catalog HTTP transport.
 *
 * SendRequestToRestCatalog wraps SendHttpRequest with REST-catalog-
 * specific retry classification: 429 (short backoff), 503 (long
 * backoff), and 419 (force-refresh the OAuth token via the auth layer
 * and patch the Authorization header before retrying).
 *
 * ReportHTTPError translates a non-200 HttpResult into a Postgres
 * ereport, parsing the standard REST-catalog error envelope:
 *
 *     { "error": { "message": ..., "type": ..., "code": ... } }
 *
 * JsonbGetStringByPath is the JSONB navigator used here, in the auth
 * layer, and in the REST API ops to extract leaf string values from
 * REST response bodies.
 */

#include "postgres.h"

#include "fmgr.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"

#include "pg_extension_base/base_workers.h"
#include "pg_lake/http/http_client.h"
#include "pg_lake/rest_catalog/rest_catalog.h"


/*
 * Retry actions returned by ClassifyRestCatalogRequestRetry.
 */
typedef enum RestCatalogRequestRetryAction
{
	REST_CATALOG_RETRY_STOP,
	REST_CATALOG_RETRY_BACKOFF_SHORT,	/* 429 Too Many Requests */
	REST_CATALOG_RETRY_BACKOFF_LONG,	/* 503 Service Unavailable */
	REST_CATALOG_RETRY_REFRESH_AUTH /* 419 Token Expired */
}			RestCatalogRequestRetryAction;


/*
 * UpdateAuthorizationHeader finds the "Authorization: Bearer ..." entry in the
 * header list and replaces it with a new one carrying the given token.  If no
 * matching header is found the function is a no-op (defensive).
 */
static void
UpdateAuthorizationHeader(List *headers, const char *token)
{
	const char *prefix = "Authorization: Bearer ";
	ListCell   *lc;

	foreach(lc, headers)
	{
		char	   *header = (char *) lfirst(lc);

		if (strncmp(header, prefix, strlen(prefix)) == 0)
		{
			lfirst(lc) = psprintf("Authorization: Bearer %s", token);
			return;
		}
	}
}


/*
 * ClassifyRestCatalogRequestRetry decides whether to retry and, if so, what
 * kind of action the caller should take.
 */
static RestCatalogRequestRetryAction
ClassifyRestCatalogRequestRetry(long status, int maxRetry, int retryNo)
{
	if (retryNo > maxRetry)
		return REST_CATALOG_RETRY_STOP;

	/* too many requests, wait some time */
	if (status == HTTP_STATUS_TOO_MANY_REQUESTS)
		return REST_CATALOG_RETRY_BACKOFF_SHORT;

	/* server unavailable, let's wait a bit more */
	if (status == HTTP_STATUS_SERVICE_UNAVAILABLE)
		return REST_CATALOG_RETRY_BACKOFF_LONG;

	/* token expired, retry after refreshing token */
	if (status == HTTP_STATUS_TOKEN_EXPIRED)
		return REST_CATALOG_RETRY_REFRESH_AUTH;

	return REST_CATALOG_RETRY_STOP;
}


/*
 * SendRequestToRestCatalog sends an HTTP request to the rest catalog
 * with retry logic for retriable errors, attempting up to
 * MAX_HTTP_RETRY_FOR_REST_CATALOG times.
 *
 * LightSleep reacts to signals, and can easily throw an error (e.g.,
 * cancel backend). This function can be called at post-commit hook,
 * so normally we wouldn't want any errors to happen, but then
 * Postgres already prevents post-commit backends to receive signals.
 *
 * When opts is non-NULL the retry callback can force-refresh the
 * access token and patch the Authorization header on a 419 response.
 * Pass opts = NULL for the token-fetch request itself to avoid recursion.
 */
HttpResult
SendRequestToRestCatalog(RestCatalogOptions * opts, HttpMethod method, const char *url,
						 const char *body, List *headers)
{
	const int	MAX_HTTP_RETRY_FOR_REST_CATALOG = 3;

	HttpResult	result;

	for (int retryNo = 1; retryNo <= MAX_HTTP_RETRY_FOR_REST_CATALOG; retryNo++)
	{
		result = SendHttpRequest(method, url, body, headers);

		switch (ClassifyRestCatalogRequestRetry(result.status, MAX_HTTP_RETRY_FOR_REST_CATALOG, retryNo))
		{
			case REST_CATALOG_RETRY_BACKOFF_SHORT:
				LightSleep(LinearBackoffSleepMs(500, retryNo));
				continue;

			case REST_CATALOG_RETRY_BACKOFF_LONG:
				LightSleep(LinearBackoffSleepMs(5000, retryNo));
				continue;

			case REST_CATALOG_RETRY_REFRESH_AUTH:
				{
					/*
					 * Force-refresh the cached token and update the
					 * Authorization header so the retried request carries the
					 * new token.
					 */
					bool		forceRefreshToken = true;
					char	   *freshToken = GetRestCatalogAccessToken(opts, forceRefreshToken);

					UpdateAuthorizationHeader(headers, freshToken);
					continue;
				}

			case REST_CATALOG_RETRY_STOP:
				return result;
		}
	}

	return result;
}


/*
* Reports an HTTP error by raising an appropriate error message.
* The error format of rest catalog is follows:
* {
*  "error": {
*    "message": "Malformed request",
*    "type": "BadRequestException",
*    "code": 400
*  }
*/
void
ReportHTTPError(HttpResult httpResult, int level)
{
	/*
	 * This is a curl error, so we don't have a proper HttpResult, don't even
	 * try to parse the response.
	 */
	if (httpResult.status == 0)
	{
		ereport(level,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("HTTP request failed %s", httpResult.errorMsg ? httpResult.errorMsg : "unknown error")));

		return;
	}

	const char *message = httpResult.body ? JsonbGetStringByPath(httpResult.body, 2, "error", "message") : NULL;
	const char *type = httpResult.body ? JsonbGetStringByPath(httpResult.body, 2, "error", "type") : NULL;

	ereport(level,
			(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
			 errmsg("HTTP request failed (HTTP %ld)", httpResult.status),
			 message ? errdetail_internal("%s", message) : 0,
			 type ? errhint("The rest catalog returned error type: %s", type) : 0));
}


/*
 * Get a string value at the given JSON path: key1 -> key2 -> ... -> keyN
 * - jsonb_text: input JSON text (e.g., from an HTTP response)
 * - nkeys: number of keys in the path
 * - ...: const char* keys, in order
 *
 * On success: returns palloc'd C-string in the current memory context.
 * On failure: ERROR (missing key, non-object mid-level, non-string leaf).
 */
char *
JsonbGetStringByPath(const char *jsonb_text, int nkeys,...)
{
	if (nkeys <= 0)
		ereport(ERROR, (errmsg("invalid jsonb path: number of keys must be > 0")));

	Datum		jsonbDatum = DirectFunctionCall1(jsonb_in, CStringGetDatum(jsonb_text));
	Jsonb	   *jb = DatumGetJsonbP(jsonbDatum);

	JsonbContainer *container = &jb->root;

	va_list		variableArgList;

	va_start(variableArgList, nkeys);

	for (int argIndex = 0; argIndex < nkeys; argIndex++)
	{
		const char *key = va_arg(variableArgList, const char *);
		JsonbValue	keyVal;
		JsonbValue *val;

		if (!JsonContainerIsObject(container))
			ereport(ERROR, (errmsg("json path step %d: not an object", argIndex + 1)));

		keyVal.type = jbvString;
		keyVal.val.string.val = (char *) key;
		keyVal.val.string.len = strlen(key);

		val = findJsonbValueFromContainer(container, JB_FOBJECT, &keyVal);
		if (val == NULL)
			return NULL;

		if (argIndex < nkeys - 1)
		{
			if (val->type != jbvBinary || !JsonContainerIsObject(val->val.binary.data))
				ereport(ERROR, (errmsg("json path step %d: key \"%s\" is not an object", argIndex + 1, key)));

			container = val->val.binary.data;	/* descend */
		}
		else
		{
			if (!(val->type == jbvString || val->type == jbvNumeric))
				ereport(ERROR, (errmsg("leaf \"%s\" is not a string or numeric", key)));

			va_end(variableArgList);

			if (val->type == jbvString)
				return pnstrdup(val->val.string.val, val->val.string.len);
			else
			{
				bool		haveError = false;

				int			valInt = numeric_int4_opt_error(val->val.numeric,
															&haveError);

				if (haveError)
				{
					ereport(ERROR, (errmsg("integer out of range")));
				}

				return psprintf("%d", valInt);
			}
		}
	}

	va_end(variableArgList);
	ereport(ERROR, (errmsg("unexpected json path handling error")));
}
