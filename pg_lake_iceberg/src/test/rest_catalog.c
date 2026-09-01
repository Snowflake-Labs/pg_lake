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
#include "miscadmin.h"

#include "fmgr.h"
#include "funcapi.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/memutils.h"

#include "pg_lake/rest_catalog/rest_catalog.h"

PG_FUNCTION_INFO_V1(register_namespace_to_rest_catalog);
PG_FUNCTION_INFO_V1(register_namespace_to_named_catalog);
PG_FUNCTION_INFO_V1(get_rest_metadata_location);
PG_FUNCTION_INFO_V1(get_rest_vended_credentials);
PG_FUNCTION_INFO_V1(resolve_rest_catalog_base_uri);
PG_FUNCTION_INFO_V1(set_test_rest_catalog_auth_response);
PG_FUNCTION_INFO_V1(test_rest_catalog_auth_provider_calls);
PG_FUNCTION_INFO_V1(test_rest_catalog_auth_provider_endpoints);

/*
* register_namespace_to_rest_catalog is a test function that registers
* a namespace to the rest catalog.
*/
Datum
register_namespace_to_rest_catalog(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(1));

	RestCatalogOptions *opts = ResolveRestCatalogOptions(REST_CATALOG_NAME);

	RegisterNamespaceToRestCatalog(opts, catalogName, namespaceName);
	PG_RETURN_VOID();
}


/*
 * register_namespace_to_named_catalog is the same as above for a catalog the
 * caller names, which is how a test reaches a user-created server rather than
 * the built-in one.
 */
Datum
register_namespace_to_named_catalog(PG_FUNCTION_ARGS)
{
	char	   *catalog = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(2));

	RestCatalogOptions *opts = ResolveRestCatalogOptions(catalog);

	RegisterNamespaceToRestCatalog(opts, catalogName, namespaceName);
	PG_RETURN_VOID();
}


/*
 * get_rest_metadata_location is a test function that calls
 * LoadRestCatalogMetadataLocation and returns the metadata location.
 * This exercises the full LoadTableFromRestCatalog path including
 * vended credential extraction and caching.
 */
Datum
get_rest_metadata_location(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char	   *tableName = text_to_cstring(PG_GETARG_TEXT_P(2));

	RestCatalogOptions *opts = ResolveRestCatalogOptions(REST_CATALOG_NAME);

	char	   *metadataLocation =
		LoadRestCatalogMetadataLocation(opts, catalogName, namespaceName,
										tableName);

	PG_RETURN_TEXT_P(cstring_to_text(metadataLocation));
}


/*
 * get_rest_vended_credentials is a test function that loads a table from
 * the REST catalog and returns the extracted vended credentials as a
 * pipe-delimited summary:
 *
 *     "<access-key-id>|<scope>|<yes|no session token>|<expiry|noexpiry>|
 *      <region>|<endpoint>|<url-style>|<use-ssl>"
 *
 * A catalog may vend more than one credential, in which case the
 * summaries are joined with ';' in the order the catalog returned them.
 *
 * Returns NULL when the loadTable response carried no vended
 * credentials.  This exercises ExtractVendedCredentials, including
 * storage-credentials parsing, scope resolution/clamping, expiry parsing,
 * and the catalog-provided S3 connection settings.
 */
Datum
get_rest_vended_credentials(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char	   *tableName = text_to_cstring(PG_GETARG_TEXT_P(2));

	RestCatalogOptions *opts = ResolveRestCatalogOptions(REST_CATALOG_NAME);

	RestCatalogLoadTableResult result =
		LoadTableFromRestCatalog(opts, catalogName, namespaceName, tableName);

	if (result.vendedCredentials == NIL)
		PG_RETURN_NULL();

	StringInfoData buf;
	ListCell   *credsCell = NULL;

	initStringInfo(&buf);

	foreach(credsCell, result.vendedCredentials)
	{
		VendedCredentials *creds = lfirst(credsCell);

		appendStringInfo(&buf, "%s%s|%s|%s|%s|%s|%s|%s|%s",
						 buf.len > 0 ? ";" : "",
						 creds->accessKeyId ? creds->accessKeyId : "",
						 creds->scope ? creds->scope : "",
						 creds->sessionToken ? "yes" : "no",
						 creds->expiresAt > 0 ? "expiry" : "noexpiry",
						 creds->region ? creds->region : "",
						 creds->endpoint ? creds->endpoint : "",
						 creds->urlStyle ? creds->urlStyle : "",
						 creds->useSsl ? creds->useSsl : "");
	}

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}


/*
 * resolve_rest_catalog_base_uri is a test function that exposes
 * ResolveRestCatalogBaseUri so its endpoint-normalization edge cases
 * (trailing slash, explicit mount path) can be asserted from pytest
 * without a live catalog server.
 */
Datum
resolve_rest_catalog_base_uri(PG_FUNCTION_ARGS)
{
	char	   *endpoint = text_to_cstring(PG_GETARG_TEXT_P(0));

	PG_RETURN_TEXT_P(cstring_to_text(ResolveRestCatalogBaseUri(endpoint)));
}


/*
 * Stand-in for the credential provider an external extension would supply,
 * letting tests exercise the provider path without one.  Tests point
 * pg_lake_iceberg.rest_catalog_auth_provider at
 * "pg_lake_iceberg:test_rest_catalog_auth_provider" and set the canned
 * response with set_test_rest_catalog_auth_response.
 *
 * The canned response lives in TopMemoryContext because the provider is
 * consulted long after that function's own call context is gone.
 */
static char *TestAuthProviderAuthorization = NULL;
static int	TestAuthProviderExpiresIn = 0;
static bool TestAuthProviderClaimsCatalog = true;
static int	TestAuthProviderCallCount = 0;

/*
 * What the provider was last handed, so tests can assert on how pg_lake
 * addresses a catalog rather than only on what it does with the answer.
 */
static char *TestAuthProviderBaseUri = NULL;
static char *TestAuthProviderOauthEndpoint = NULL;

/*
 * Exported so load_external_function can find it: this library is built with
 * hidden visibility, exactly as a real provider extension would be.
 */
extern PGDLLEXPORT bool test_rest_catalog_auth_provider(const RestCatalogAuthRequest * request,
														RestCatalogAuthMaterial * material);

bool
test_rest_catalog_auth_provider(const RestCatalogAuthRequest * request,
								RestCatalogAuthMaterial * material)
{
	TestAuthProviderCallCount++;

	/*
	 * A provider that cannot read the struct it was handed must decline
	 * rather than guess at the layout.
	 */
	if (request->version != REST_CATALOG_AUTH_REQUEST_VERSION)
		return false;

	if (TestAuthProviderBaseUri != NULL)
		pfree(TestAuthProviderBaseUri);
	if (TestAuthProviderOauthEndpoint != NULL)
		pfree(TestAuthProviderOauthEndpoint);

	TestAuthProviderBaseUri = request->catalogBaseUri
		? MemoryContextStrdup(TopMemoryContext, request->catalogBaseUri)
		: NULL;
	TestAuthProviderOauthEndpoint = request->oauthEndpoint
		? MemoryContextStrdup(TopMemoryContext, request->oauthEndpoint)
		: NULL;

	/*
	 * Declining sends pg_lake back to its built-in OAuth2 grant.  An unprimed
	 * stub declines too, rather than handing back a NULL authorization.
	 */
	if (!TestAuthProviderClaimsCatalog || TestAuthProviderAuthorization == NULL)
		return false;

	material->authorization = pstrdup(TestAuthProviderAuthorization);
	material->expiresIn = TestAuthProviderExpiresIn;

	return true;
}


/*
 * set_test_rest_catalog_auth_response(authorization, expires_in, claims)
 * primes the stub provider above.  Pass claims = false to check that a
 * declining provider falls back to the built-in flow, and expires_in = 0
 * to check that an uncacheable credential is re-fetched per request.
 */
Datum
set_test_rest_catalog_auth_response(PG_FUNCTION_ARGS)
{
	char	   *authorization = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (TestAuthProviderAuthorization != NULL)
		pfree(TestAuthProviderAuthorization);

	TestAuthProviderAuthorization = MemoryContextStrdup(TopMemoryContext, authorization);
	TestAuthProviderExpiresIn = PG_GETARG_INT32(1);
	TestAuthProviderClaimsCatalog = PG_GETARG_BOOL(2);
	TestAuthProviderCallCount = 0;

	PG_RETURN_VOID();
}


/*
 * test_rest_catalog_auth_provider_calls returns how many times the stub
 * provider has been consulted since it was last primed.
 */
Datum
test_rest_catalog_auth_provider_calls(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(TestAuthProviderCallCount);
}


/*
 * test_rest_catalog_auth_provider_endpoints reports how the catalog was
 * addressed on the last call, as "<base uri>|<oauth endpoint>", with an unset
 * oauth endpoint rendered as the empty string.
 */
Datum
test_rest_catalog_auth_provider_endpoints(PG_FUNCTION_ARGS)
{
	if (TestAuthProviderCallCount == 0)
		PG_RETURN_NULL();

	PG_RETURN_TEXT_P(cstring_to_text(psprintf("%s|%s",
											  TestAuthProviderBaseUri ? TestAuthProviderBaseUri : "",
											  TestAuthProviderOauthEndpoint ? TestAuthProviderOauthEndpoint : "")));
}
