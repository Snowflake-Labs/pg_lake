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
PG_FUNCTION_INFO_V1(get_rest_metadata_location);
PG_FUNCTION_INFO_V1(get_rest_vended_credentials);
PG_FUNCTION_INFO_V1(resolve_rest_catalog_base_uri);
PG_FUNCTION_INFO_V1(install_test_rest_catalog_auth_hook);
PG_FUNCTION_INFO_V1(remove_test_rest_catalog_auth_hook);
PG_FUNCTION_INFO_V1(test_rest_catalog_auth_hook_calls);

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
 * Stand-in for the credential provider an external extension would
 * register, letting tests drive PgLakeRestCatalogAuthHook without one.
 *
 * The canned response lives in TopMemoryContext because the hook is
 * consulted long after install_test_rest_catalog_auth_hook's own call
 * context is gone.
 */
static char *TestAuthHookAuthorization = NULL;
static int	TestAuthHookExpiresIn = 0;
static bool TestAuthHookClaimsCatalog = true;
static int	TestAuthHookCallCount = 0;


static bool
TestRestCatalogAuthHook(RestCatalogOptions * opts, bool forceRefresh,
						RestCatalogAuthMaterial * material)
{
	TestAuthHookCallCount++;

	/* declining sends pg_lake back to its built-in OAuth2 grant */
	if (!TestAuthHookClaimsCatalog)
		return false;

	material->authorization = pstrdup(TestAuthHookAuthorization);
	material->expiresIn = TestAuthHookExpiresIn;

	return true;
}


/*
 * install_test_rest_catalog_auth_hook(authorization, expires_in, claims)
 * registers the stub provider above.  Pass claims = false to check that a
 * declining provider falls back to the built-in flow, and expires_in = 0
 * to check that an uncacheable credential is re-fetched per request.
 */
Datum
install_test_rest_catalog_auth_hook(PG_FUNCTION_ARGS)
{
	char	   *authorization = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (TestAuthHookAuthorization != NULL)
		pfree(TestAuthHookAuthorization);

	TestAuthHookAuthorization = MemoryContextStrdup(TopMemoryContext, authorization);
	TestAuthHookExpiresIn = PG_GETARG_INT32(1);
	TestAuthHookClaimsCatalog = PG_GETARG_BOOL(2);
	TestAuthHookCallCount = 0;

	PgLakeRestCatalogAuthHook = TestRestCatalogAuthHook;

	PG_RETURN_VOID();
}


/*
 * test_rest_catalog_auth_hook_calls returns how many times the stub
 * provider has been consulted since it was installed.
 */
Datum
test_rest_catalog_auth_hook_calls(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT32(TestAuthHookCallCount);
}


/*
 * remove_test_rest_catalog_auth_hook unregisters the stub provider.
 */
Datum
remove_test_rest_catalog_auth_hook(PG_FUNCTION_ARGS)
{
	PgLakeRestCatalogAuthHook = NULL;

	PG_RETURN_VOID();
}
