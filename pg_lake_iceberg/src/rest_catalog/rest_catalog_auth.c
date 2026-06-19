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
 * REST catalog authentication: per-catalog OAuth token cache, the
 * actual OAuth grant fetch, and the auth-header builders consumed
 * by HTTP transport (rest_catalog_http.c) and the REST API ops
 * (rest_catalog_ops.c).
 *
 * The token cache is keyed by the iceberg_catalog server OID and
 * invalidated wholesale on any pg_foreign_server change via a
 * FOREIGNSERVEROID syscache callback, so stale credentials are
 * never reused across ALTER SERVER.
 */

#include "postgres.h"

#include "common/base64.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "pg_lake/http/http_client.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/url_encode.h"


/*
 * Per-rest-catalog token cache, keyed by the iceberg_catalog server OID.
 * Should always be accessed via GetRestCatalogAccessToken().
 */
typedef struct RestCatalogTokenCacheEntry
{
	Oid			serverOid;		/* hash key */
	char	   *accessToken;
	TimestampTz accessTokenExpiry;
}			RestCatalogTokenCacheEntry;

static HTAB *RestCatalogTokenCache = NULL;
static MemoryContext RestTokenCacheCtx = NULL;

/*
 * TokenCacheCallbackRegistered is separate from RestCatalogTokenCache because
 * the callback must be registered exactly once per backend lifetime
 * (CacheRegisterSyscacheCallback appends to a fixed-size array), while
 * RestCatalogTokenCache is reset to NULL on every invalidation.
 */
static bool TokenCacheCallbackRegistered = false;


static void FetchRestCatalogAccessToken(RestCatalogOptions * opts, char **accessToken, int *expiresIn);
static char *EncodeBasicAuth(const char *clientId, const char *clientSecret);


/*
 * Syscache invalidation callback for pg_foreign_server changes.
 * Any ALTER/DROP SERVER blows away the entire token cache so stale
 * credentials are never reused.  The cache is rebuilt lazily on the
 * next token lookup.
 *
 * We ignore hashvalue and reset the whole cache rather than selectively
 * invalidating a single server's entry.  With a handful of servers and
 * infrequent ALTER SERVER, the cost of a few extra OAuth round-trips is
 * negligible compared to the complexity of tracking per-entry hash
 * values for targeted invalidation.
 */
static void
InvalidateRestTokenCache(Datum arg, int cacheid, uint32 hashvalue)
{
	if (RestCatalogTokenCache == NULL)
		return;

	MemoryContextReset(RestTokenCacheCtx);
	RestCatalogTokenCache = NULL;
}


/*
 * Initialize the per-catalog token cache hash table if needed.
 */
static void
InitTokenCacheIfNeeded(void)
{
	if (!TokenCacheCallbackRegistered)
	{
		CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
									  InvalidateRestTokenCache,
									  (Datum) 0);
		TokenCacheCallbackRegistered = true;
	}

	if (RestCatalogTokenCache != NULL)
		return;

	if (RestTokenCacheCtx == NULL)
		RestTokenCacheCtx = AllocSetContextCreate(CacheMemoryContext,
												  "RestTokenCacheCtx",
												  ALLOCSET_DEFAULT_SIZES);

	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(Oid);
	ctl.entrysize = sizeof(RestCatalogTokenCacheEntry);
	ctl.hcxt = RestTokenCacheCtx;

	RestCatalogTokenCache = hash_create("REST Catalog Token Cache",
										8, &ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}


/*
 * Gets an access token from rest catalog. Caches the token per catalog
 * (keyed by iceberg_catalog server OID) until it is about to expire.
 */
char *
GetRestCatalogAccessToken(RestCatalogOptions * opts, bool forceRefreshToken)
{
	if (opts == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("REST catalog options must not be NULL when fetching access token")));

	/*
	 * Every resolved RestCatalogOptions originates from
	 * BuildRestCatalogOptionsFromServer, which always sets serverOid. A
	 * missing OID would silently funnel every catalog into the same cache
	 * slot, so trap it loudly here.
	 */
	Assert(OidIsValid(opts->serverOid));

	InitTokenCacheIfNeeded();

	bool		found = false;
	RestCatalogTokenCacheEntry *entry =
		hash_search(RestCatalogTokenCache, &opts->serverOid, HASH_ENTER, &found);

	if (!found)
	{
		entry->accessToken = NULL;
		entry->accessTokenExpiry = 0;
	}

	/*
	 * Calling initial time or token will expire in 1 minute, fetch a new
	 * token.
	 */
	TimestampTz now = GetCurrentTimestamp();
	const int	MINUTE_IN_MSECS = 60 * 1000;

	if (forceRefreshToken || entry->accessTokenExpiry == 0 ||
		!TimestampDifferenceExceeds(now, entry->accessTokenExpiry, MINUTE_IN_MSECS))
	{
		if (entry->accessToken)
		{
			pfree(entry->accessToken);
			entry->accessToken = NULL;
			entry->accessTokenExpiry = 0;
		}

		char	   *accessToken = NULL;
		int			expiresIn = 0;

		FetchRestCatalogAccessToken(opts, &accessToken, &expiresIn);

		entry->accessToken = MemoryContextStrdup(RestTokenCacheCtx, accessToken);
		entry->accessTokenExpiry = now + (int64_t) expiresIn * 1000000; /* expiresIn is in
																		 * seconds */
	}

	Assert(entry->accessToken != NULL);

	return entry->accessToken;
}


/*
* Fetches an access token from rest catalog using the given options.
*/
static void
FetchRestCatalogAccessToken(RestCatalogOptions * opts, char **accessToken, int *expiresIn)
{
	Assert(opts->host != NULL && opts->host[0] != '\0');
	if (!opts->clientSecret || !*opts->clientSecret)
		ereport(ERROR,
				(errmsg("REST catalog client_secret is not configured"),
				 errhint("Set the \"client_secret\" option on the server "
						 "or the pg_lake_iceberg.rest_catalog_client_secret GUC.")));

	char	   *accessTokenUrl = opts->oauthHostPath;

	/*
	 * if oauthHostPath is not set, use Polaris' default oauth token endpoint
	 */
	if (!accessTokenUrl || *accessTokenUrl == '\0')
		accessTokenUrl = psprintf(REST_CATALOG_AUTH_TOKEN_PATH, opts->host);

	/* Form-encoded body */
	StringInfoData body;

	initStringInfo(&body);
	appendStringInfo(&body, "grant_type=client_credentials&scope=%s",
					 URLEncodePath(opts->scope));

	/* Headers */
	List	   *headers = NIL;

	if (opts->authType == REST_CATALOG_AUTH_TYPE_HORIZON)
	{
		/* Put secret in body (ignore client ID) */
		appendStringInfo(&body, "&client_secret=%s", URLEncodePath(opts->clientSecret));
	}
	else
	{
		if (!opts->clientId || !*opts->clientId)
			ereport(ERROR,
					(errmsg("REST catalog client_id is not configured"),
					 errhint("Set the \"client_id\" option on the server "
							 "or the pg_lake_iceberg.rest_catalog_client_id GUC.")));

		/* Build Authorization: Basic <base64(clientId:clientSecret)> */
		char	   *encodedAuth = EncodeBasicAuth(opts->clientId, opts->clientSecret);
		char	   *authHeader = psprintf("Authorization: Basic %s", encodedAuth);

		headers = lappend(headers, authHeader);
	}

	headers = lappend(headers, "Content-Type: application/x-www-form-urlencoded");

	/*
	 * Pass NULL opts so SendRequestToRestCatalog skips the 419 token-refresh
	 * retry branch.  Otherwise a 419 here would call
	 * GetRestCatalogAccessToken -> FetchRestCatalogAccessToken ->
	 * SendRequestToRestCatalog in an infinite loop.
	 */
	HttpResult	httpResponse = SendRequestToRestCatalog(NULL, HTTP_POST, accessTokenUrl,
														body.data, headers);

	if (httpResponse.status != 200)
		ereport(ERROR,
				(errmsg("Rest Catalog OAuth token request failed (HTTP %ld)", httpResponse.status),
				 httpResponse.body ? errdetail_internal("%s", httpResponse.body) : 0));

	if (!httpResponse.body || !*httpResponse.body)
		ereport(ERROR, (errmsg("Rest Catalog OAuth token response body is empty")));

	*accessToken = JsonbGetStringByPath(httpResponse.body, 1, "access_token");

	if (*accessToken == NULL)
		ereport(ERROR, (errmsg("key \"access_token\" missing in json response")));

	char	   *expiresInStr = JsonbGetStringByPath(httpResponse.body, 1, "expires_in");

	if (expiresInStr == NULL)
		ereport(ERROR, (errmsg("key \"expires_in\" missing in json response")));

	*expiresIn = pg_strtoint32(expiresInStr);
}


/*
* Encodes the client ID and secret into a Base64-encoded string
* suitable for use in the Authorization header.
*/
static char *
EncodeBasicAuth(const char *clientId, const char *clientSecret)
{
	StringInfoData src;

	initStringInfo(&src);
	appendStringInfo(&src, "%s:%s", clientId, clientSecret);

	/* dst length per RFC: 4 * ceil(n/3) + 1 for '\0' */
	int			srcLen = (int) strlen(src.data);
	int			dstLen = 4 * ((srcLen + 2) / 3) + 1;

	char	   *dst = (char *) palloc(dstLen);
#if PG_VERSION_NUM >= 180000
	int			out = pg_b64_encode((uint8 *) src.data, srcLen, dst, dstLen);
#else
	int			out = pg_b64_encode(src.data, srcLen, dst, dstLen);
#endif

	if (out < 0)
		ereport(ERROR, (errmsg("failed to base64-encode client credentials")));

	dst[out] = '\0';
	return dst;
}


/*
* Creates the headers for a POST request with authentication.
*/
List *
PostHeadersWithAuth(RestCatalogOptions * opts)
{
	bool		forceRefreshToken = false;

	return list_make3(psprintf("Authorization: Bearer %s", GetRestCatalogAccessToken(opts, forceRefreshToken)),
					  pstrdup("Accept: application/json"),
					  pstrdup("Content-Type: application/json"));
}


/*
* Creates the headers for a DELETE request with authentication.
*/
List *
DeleteHeadersWithAuth(RestCatalogOptions * opts)
{
	bool		forceRefreshToken = false;

	return list_make1(psprintf("Authorization: Bearer %s", GetRestCatalogAccessToken(opts, forceRefreshToken)));
}


/*
* Creates the headers for a GET request with authentication.
*/
List *
GetHeadersWithAuth(RestCatalogOptions * opts)
{
	bool		forceRefreshToken = false;

	return list_make2(psprintf("Authorization: Bearer %s", GetRestCatalogAccessToken(opts, forceRefreshToken)),
					  pstrdup("Accept: application/json"));
}
