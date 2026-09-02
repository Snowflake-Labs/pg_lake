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
 * The token cache is keyed by (serverOid, userMappingOid) and
 * invalidated wholesale on any pg_foreign_server or pg_user_mapping
 * change, so stale credentials are never reused across
 * ALTER SERVER, ALTER USER MAPPING, or DROP USER MAPPING.
 */

#include "postgres.h"

#include "common/base64.h"
#include "fmgr.h"
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
 * Per-rest-catalog token cache.  Keyed by (serverOid, userMappingOid):
 *   - serverOid identifies which iceberg_catalog server the token
 *     belongs to, so an ALTER SERVER on one server never reuses
 *     another's credentials.
 *   - userMappingOid scopes tokens to the contributing pg_user_mapping
 *     row, so different SET ROLEs in the same backend each get the
 *     credentials of their own user mapping (or PUBLIC).
 *     userMappingOid is InvalidOid when no user mapping is involved
 *     (built-in pg_lake_rest_catalog, or a user-created server falling
 *     back to GUCs).
 *
 * Should always be accessed via GetRestCatalogAuthorization().
 */
typedef struct RestCatalogTokenCacheKey
{
	Oid			serverOid;
	Oid			userMappingOid;
}			RestCatalogTokenCacheKey;

typedef struct RestCatalogTokenCacheEntry
{
	RestCatalogTokenCacheKey key;	/* hash key */
	char	   *authorization;	/* full header value, scheme included */
	TimestampTz authorizationExpiry;
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


/*
 * The rendezvous slot a provider registers in, resolved once per backend.
 * RestCatalogAuthProviderLastSeen is what was in it when the credential cache
 * was last known to agree with it.
 */
static void **RestCatalogAuthProviderSlot = NULL;
static PgLakeRestCatalogAuthProvider RestCatalogAuthProviderLastSeen = NULL;


static PgLakeRestCatalogAuthProvider GetRestCatalogAuthProvider(void);
static void FetchRestCatalogAuthorization(RestCatalogOptions * opts,
										  PgLakeRestCatalogAuthProvider provider,
										  bool forceRefresh,
										  char **authorization, int *expiresIn);
static void FetchOAuth2AccessToken(RestCatalogOptions * opts, char **accessToken, int *expiresIn);
static char *EncodeBasicAuth(const char *clientId, const char *clientSecret);


/*
 * Syscache invalidation callback for pg_foreign_server and
 * pg_user_mapping changes.  Any ALTER/DROP on either object blows away
 * the entire token cache so stale credentials are never reused.  The
 * cache is rebuilt lazily on the next token lookup.
 *
 * We ignore hashvalue and reset the whole cache rather than selectively
 * invalidating a single server / user-mapping entry.  With a handful of
 * servers and infrequent ALTER, the cost of a few extra OAuth round
 * trips is negligible compared to the complexity of tracking per-entry
 * hash values for targeted invalidation.
 */
static void
InvalidateRestTokenCache(Datum arg, int cacheid, uint32 hashvalue)
{
	if (RestCatalogTokenCache != NULL)
	{
		MemoryContextReset(RestTokenCacheCtx);
		RestCatalogTokenCache = NULL;
	}

	InvalidateVendedCredentialsCache();
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
		CacheRegisterSyscacheCallback(USERMAPPINGOID,
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
	ctl.keysize = sizeof(RestCatalogTokenCacheKey);
	ctl.entrysize = sizeof(RestCatalogTokenCacheEntry);
	ctl.hcxt = RestTokenCacheCtx;

	RestCatalogTokenCache = hash_create("REST Catalog Token Cache",
										8, &ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}


/*
 * Gets the Authorization header value for a rest catalog, including its
 * scheme.  Caches it per (server, user-mapping) pair so that different
 * SET ROLEs in the same backend each see the credentials of their own
 * user mapping (or PUBLIC), while still letting the built-in
 * pg_lake_rest_catalog share a single (server, InvalidOid) slot across
 * all sessions and roles.
 */
char *
GetRestCatalogAuthorization(RestCatalogOptions * opts, bool forceRefreshToken)
{
	if (opts == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("REST catalog options must not be NULL when fetching access token")));

	/*
	 * Every resolved RestCatalogOptions originates from
	 * BuildRestCatalogOptionsFromServer, which always sets serverOid. A
	 * missing OID would silently funnel every catalog into the same cache
	 * slot, so trap it loudly here.  userMappingOid is allowed to be
	 * InvalidOid: that simply means "no user mapping contributed
	 * credentials".
	 */
	Assert(OidIsValid(opts->serverOid));

	/*
	 * Ahead of the cache, since a provider that changed since the last
	 * request drops what it minted, and this is where that is noticed.
	 */
	PgLakeRestCatalogAuthProvider provider = GetRestCatalogAuthProvider();

	InitTokenCacheIfNeeded();

	RestCatalogTokenCacheKey key;

	memset(&key, 0, sizeof(key));	/* zero out any compiler padding so
									 * HASH_BLOBS keys compare cleanly */
	key.serverOid = opts->serverOid;
	key.userMappingOid = opts->userMappingOid;

	bool		found = false;
	RestCatalogTokenCacheEntry *entry =
		hash_search(RestCatalogTokenCache, &key, HASH_ENTER, &found);

	if (!found)
	{
		entry->authorization = NULL;
		entry->authorizationExpiry = 0;
	}

	/*
	 * Calling initial time or credential will expire in 1 minute, fetch a new
	 * one.  A provider reporting expiresIn = 0 lands its expiry at "now",
	 * which fails this check on every subsequent call, so an uncacheable
	 * credential is re-fetched per request without a special case here.
	 */
	TimestampTz now = GetCurrentTimestamp();
	const int	MINUTE_IN_MSECS = 60 * 1000;

	if (forceRefreshToken || entry->authorizationExpiry == 0 ||
		!TimestampDifferenceExceeds(now, entry->authorizationExpiry, MINUTE_IN_MSECS))
	{
		if (entry->authorization)
		{
			pfree(entry->authorization);
			entry->authorization = NULL;
			entry->authorizationExpiry = 0;
		}

		char	   *authorization = NULL;
		int			expiresIn = 0;

		FetchRestCatalogAuthorization(opts, provider, forceRefreshToken,
									  &authorization, &expiresIn);

		entry->authorization = MemoryContextStrdup(RestTokenCacheCtx, authorization);
		entry->authorizationExpiry = now + (int64_t) expiresIn * 1000000;	/* expiresIn is in
																			 * seconds */
	}

	Assert(entry->authorization != NULL);

	return entry->authorization;
}


/*
 * GetRestCatalogAuthProvider reads whatever provider is registered, NULL when
 * none is.
 *
 * A provider that changed since the last look takes its credentials with it:
 * they are cached per catalog rather than per provider, so leaving them behind
 * would keep sending a credential minted by a provider no longer in charge.
 */
static PgLakeRestCatalogAuthProvider
GetRestCatalogAuthProvider(void)
{
	if (RestCatalogAuthProviderSlot == NULL)
		RestCatalogAuthProviderSlot =
			find_rendezvous_variable(PG_LAKE_REST_CATALOG_AUTH_PROVIDER);

	void	   *registered = *RestCatalogAuthProviderSlot;
	PgLakeRestCatalogAuthProvider provider =
		(PgLakeRestCatalogAuthProvider) registered;

	if (provider != RestCatalogAuthProviderLastSeen)
	{
		RestCatalogAuthProviderLastSeen = provider;
		InvalidateRestTokenCache((Datum) 0, 0, 0);
	}

	return provider;
}


/*
 * RestCatalogAuthProviderIsRegistered reports whether some extension has
 * offered to supply credentials, which decides whether a catalog with no
 * stored secret is a misconfiguration or a catalog authenticated some other
 * way.
 */
bool
RestCatalogAuthProviderIsRegistered(void)
{
	return GetRestCatalogAuthProvider() != NULL;
}


/*
 * Produces the Authorization header value for a catalog, either from a
 * configured provider or from pg_lake's own OAuth2 client-credentials
 * grant.
 *
 * The provider is offered only the built-in catalog.  It mints a
 * deployment-wide credential from the machine's own identity rather than from
 * anything the caller supplied, so it is subject to the same rule as the
 * credential GUCs: it may not be spent on an endpoint a server owner chose.
 * Any role with USAGE on the FDW can CREATE SERVER and name its
 * rest_endpoint, so consulting the provider there would hand that role a
 * token minted for the deployment.  User-created servers authenticate with
 * their own user mapping credentials instead.
 */
static void
FetchRestCatalogAuthorization(RestCatalogOptions * opts,
							  PgLakeRestCatalogAuthProvider provider,
							  bool forceRefresh,
							  char **authorization, int *expiresIn)
{
	if (opts->isBuiltin && provider != NULL)
	{
		RestCatalogAuthRequest request = {
			.version = REST_CATALOG_AUTH_REQUEST_VERSION,
			.catalogBaseUri = opts->baseUri,
			.oauthEndpoint = opts->oauthHostPath,
			.catalogName = opts->catalogName,
			.scope = opts->scope,
			.authType = opts->authType,
			.forceRefresh = forceRefresh,
		};
		RestCatalogAuthMaterial material = {0};

		if (provider(&request, &material))
		{
			if (material.authorization == NULL || *material.authorization == '\0')
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("REST catalog credential provider returned an empty authorization")));

			if (material.expiresIn < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("REST catalog credential provider returned a negative lifetime")));

			*authorization = material.authorization;
			*expiresIn = material.expiresIn;
			return;
		}
	}

	char	   *accessToken = NULL;

	FetchOAuth2AccessToken(opts, &accessToken, expiresIn);

	*authorization = psprintf("Bearer %s", accessToken);
}


/*
* Fetches an access token from rest catalog using the given options.
*/
static void
FetchOAuth2AccessToken(RestCatalogOptions * opts, char **accessToken, int *expiresIn)
{
	Assert(opts->baseUri != NULL && opts->baseUri[0] != '\0');

	/*
	 * Defense in depth: ValidateRestCatalogOptions already rejected resolved
	 * options without credentials at resolution time.  These checks are kept
	 * so that any future code path that builds RestCatalogOptions outside
	 * ResolveRestCatalogOptions still gets an actionable error before we POST
	 * empty credentials to the OAuth endpoint.
	 */
	if (!opts->clientSecret || !*opts->clientSecret)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("REST catalog client_secret is not configured"),
				 errhint("Set client_secret via a USER MAPPING or the "
						 "pg_lake_iceberg.rest_catalog_client_secret GUC.")));

	/*
	 * opts->oauthHostPath (set via the oauth_endpoint server option) is
	 * treated as a fully-qualified URL and used verbatim -- it is NOT passed
	 * through ResolveRestCatalogBaseUri.  This is intentional: the OAuth
	 * token endpoint is often on a different host from the catalog (e.g. a
	 * separate IdP), so the mount-path normalization that applies to
	 * rest_endpoint is not appropriate here.
	 *
	 * When oauthHostPath is absent the token URL is derived from the
	 * already-normalized opts->baseUri via REST_CATALOG_AUTH_TOKEN_PATH.
	 */
	char	   *accessTokenUrl = opts->oauthHostPath;

	if (!accessTokenUrl || *accessTokenUrl == '\0')
		accessTokenUrl = psprintf(REST_CATALOG_AUTH_TOKEN_PATH, opts->baseUri);

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
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("REST catalog client_id is not configured"),
					 errhint("Set client_id via a USER MAPPING or the "
							 "pg_lake_iceberg.rest_catalog_client_id GUC.")));

		/* Build Authorization: Basic <base64(clientId:clientSecret)> */
		char	   *encodedAuth = EncodeBasicAuth(opts->clientId, opts->clientSecret);
		char	   *authHeader = psprintf("Authorization: Basic %s", encodedAuth);

		headers = lappend(headers, authHeader);
	}

	headers = lappend(headers, "Content-Type: application/x-www-form-urlencoded");

	/*
	 * This request is the refresh, so it is sent as one that cannot be
	 * retried by refreshing: otherwise a 419 here would call
	 * GetRestCatalogAuthorization -> FetchOAuth2AccessToken ->
	 * SendRestCatalogRequest in an infinite loop.
	 */
	HttpResult	httpResponse = SendCredentialRequestToRestCatalog(opts, accessTokenUrl,
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

	return list_make3(psprintf("Authorization: %s", GetRestCatalogAuthorization(opts, forceRefreshToken)),
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

	return list_make1(psprintf("Authorization: %s", GetRestCatalogAuthorization(opts, forceRefreshToken)));
}


/*
* Creates the headers for a GET request with authentication.
*/
List *
GetHeadersWithAuth(RestCatalogOptions * opts)
{
	bool		forceRefreshToken = false;

	return list_make2(psprintf("Authorization: %s", GetRestCatalogAuthorization(opts, forceRefreshToken)),
					  pstrdup("Accept: application/json"));
}
