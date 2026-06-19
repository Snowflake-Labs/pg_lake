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
 * REST catalog option resolution.
 *
 * Turns a user-visible catalog identifier into a fully-populated
 * RestCatalogOptions by layering GUC defaults, the foreign server's
 * options, and (for user-created servers) the current user's
 * pg_user_mapping options.  Every REST catalog operation in the
 * module -- auth, HTTP transport, table/namespace ops -- starts from
 * the RestCatalogOptions produced here.
 */

#include "postgres.h"

#include "foreign/foreign.h"
#include "utils/memutils.h"

#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/catalog_type.h"


/* determined by GUC */
char	   *RestCatalogHost = "http://localhost:8181";
char	   *RestCatalogOauthHostPath = "";
char	   *RestCatalogClientId = NULL;
char	   *RestCatalogClientSecret = NULL;
char	   *RestCatalogScope = "PRINCIPAL_ROLE:ALL";
int			RestCatalogAuthType = REST_CATALOG_AUTH_TYPE_OAUTH2;
bool		RestCatalogEnableVendedCredentials = true;


/*
 * ApplyGUCDefaults populates opts with the current GUC values.
 * All string fields are pstrdup'd so the struct is self-contained.
 *
 * isBuiltin gates the credential GUCs (rest_catalog_client_id /
 * rest_catalog_client_secret): they seed opts only when the resolver
 * is building options for the built-in pg_lake_rest_catalog.
 * User-created servers receive every other GUC default but must supply
 * credentials through pg_user_mapping -- see
 * BuildRestCatalogOptionsFromServer for the security rationale.
 */
static void
ApplyGUCDefaults(RestCatalogOptions * opts, bool isBuiltin)
{
	char	   *defaultLocationPrefix = GetIcebergDefaultLocationPrefix();

	opts->host = RestCatalogHost ? pstrdup(RestCatalogHost) : NULL;
	opts->oauthHostPath = RestCatalogOauthHostPath ? pstrdup(RestCatalogOauthHostPath) : NULL;

	if (isBuiltin)
	{
		opts->clientId = RestCatalogClientId ? pstrdup(RestCatalogClientId) : NULL;
		opts->clientSecret = RestCatalogClientSecret ? pstrdup(RestCatalogClientSecret) : NULL;
	}

	opts->scope = RestCatalogScope ? pstrdup(RestCatalogScope) : NULL;
	opts->authType = RestCatalogAuthType;
	opts->enableVendedCredentials = RestCatalogEnableVendedCredentials;
	opts->locationPrefix = defaultLocationPrefix ? pstrdup(defaultLocationPrefix) : NULL;
}


/*
 * ValidateRestCatalogOptions checks that the resolved options carry
 * the minimum fields needed to talk to a REST catalog: the endpoint
 * and the credentials required by the configured auth flow.  Running
 * this at resolution time -- after GUCs and (for user servers) the
 * user mapping have been folded in -- means an incompletely-configured
 * catalog fails up front on first DML, instead of silently issuing an
 * unauthenticated request to the OAuth endpoint.
 *
 * Credential requirements are auth-type specific:
 *   OAuth2:  client_id AND client_secret (Basic auth header).
 *   Horizon: client_secret only (carried in the form body;
 *            client_id is intentionally ignored).
 *
 * The hint differs by server kind because the credential surfaces
 * differ: GUCs feed only the built-in catalog, user mappings feed
 * only user-created servers.  See BuildRestCatalogOptionsFromServer
 * for the resolution rules.
 *
 * FetchRestCatalogAccessToken still re-checks the fields it actually
 * dereferences.  Those late checks are defense in depth in case any
 * future code path constructs RestCatalogOptions without going
 * through this resolver.
 */
static void
ValidateRestCatalogOptions(const RestCatalogOptions * opts,
						   const char *catalog,
						   bool isBuiltin)
{
	if (opts->host == NULL || opts->host[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("\"rest_endpoint\" is not configured for REST catalog \"%s\"",
						catalog),
				 errhint("Set the pg_lake_iceberg.rest_catalog_host GUC or "
						 "the \"rest_endpoint\" option on the server.")));

	bool		missingSecret = (opts->clientSecret == NULL || opts->clientSecret[0] == '\0');
	bool		missingId = (opts->authType != REST_CATALOG_AUTH_TYPE_HORIZON) &&
		(opts->clientId == NULL || opts->clientId[0] == '\0');

	if (missingSecret || missingId)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("no credentials found for REST catalog \"%s\"",
						catalog),
				 errhint("%s", isBuiltin
						 ? "Set the pg_lake_iceberg.rest_catalog_client_id and "
						 "pg_lake_iceberg.rest_catalog_client_secret GUCs."
						 : "Create a USER MAPPING on this server with "
						 "client_id and client_secret options.  "
						 "GUC credentials are restricted to the built-in "
						 "\"rest\" catalog and do not apply to "
						 "user-created servers.")));
}


/*
 * Build RestCatalogOptions for an iceberg_catalog server.
 *
 * Resolution differs by server kind because the credential trust
 * boundary differs.
 *
 * Built-in pg_lake_rest_catalog
 *   1. GUC defaults                       (all fields, including creds)
 *   2. Server options                     (no-op; ALTER SERVER is blocked)
 *
 * User-created server
 *   1. GUC defaults                       (non-credential fields only;
 *                                          rest_catalog_client_id /
 *                                          rest_catalog_client_secret
 *                                          are NOT inherited -- see below)
 *   2. Server options                     (anything CATALOG_OPT_CTX_SERVER)
 *   3. pg_user_mapping options            (credentials + per-user scope)
 *
 * Why credentials are gated to the built-in catalog: any role with
 * USAGE on the iceberg_catalog FDW (lake_write, via the 3.4 grant)
 * can CREATE SERVER and choose rest_endpoint / oauth_endpoint.  If we
 * let those user servers inherit the system-wide credential GUCs, the
 * next CREATE TABLE ... USING iceberg WITH (catalog='evil') would
 * POST the production client_id/secret to whatever endpoint the
 * server's owner picked -- a non-superuser credential exfiltration
 * path.  So GUC credentials are intentionally restricted to the
 * single, extension-owned built-in server, and user-created servers
 * must provide their own credentials through pg_user_mapping.
 *
 * postgres / object_store catalogs never reach this function; their
 * resolution stays in their own modules.
 *
 * `userVisibleCatalog` is the short identifier the user typed
 * (e.g. "rest" or a user server name); it is what we store in
 * opts->catalog so that error messages and the cross-catalog DML check
 * stay in user-facing terms.  The long built-in server name never
 * leaks past this function.
 */
static RestCatalogOptions *
BuildRestCatalogOptionsFromServer(const char *serverName,
								  const char *userVisibleCatalog)
{
	ForeignServer *server = GetForeignServerByName(serverName, false);
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);
	bool		isBuiltin = IsBuiltinCatalogServerName(serverName);

	Assert(strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0);

	RestCatalogOptions *opts = palloc0(sizeof(RestCatalogOptions));

	opts->serverOid = server->serverid;
	opts->userMappingOid = InvalidOid;
	opts->catalog = pstrdup(userVisibleCatalog);
	ApplyGUCDefaults(opts, isBuiltin);
	ApplyServerOptionOverrides(opts, server);

	if (!isBuiltin)
		ApplyUserMappingOverrides(opts, server);

	ValidateRestCatalogOptions(opts, userVisibleCatalog, isBuiltin);
	return opts;
}


/*
 * ResolveRestCatalogOptions builds RestCatalogOptions for the catalog
 * identifier the user typed.  The short reserved names ('postgres',
 * 'object_store', 'rest') are mapped to their pre-created built-in
 * server names; all other inputs are looked up verbatim.
 */
RestCatalogOptions *
ResolveRestCatalogOptions(const char *catalog)
{
	const char *serverName = ResolveCatalogServerName(catalog);

	return BuildRestCatalogOptionsFromServer(serverName, catalog);
}


/*
 * GetRestCatalogOptionsForRelation returns the REST catalog options for
 * the given relation.  The catalog option value is used as the server
 * name (or built-in 'rest' literal).
 */
RestCatalogOptions *
GetRestCatalogOptionsForRelation(Oid relationId)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	char	   *catalog = GetStringOption(foreignTable->options, "catalog", false);

	if (catalog == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("catalog option is not set for relation %u", relationId)));

	return ResolveRestCatalogOptions(catalog);
}


/*
 * CopyRestCatalogOptions deep-copies a RestCatalogOptions into the given
 * memory context.  All string fields are duplicated so the result is
 * self-contained and independent of the source's lifetime.
 */
RestCatalogOptions *
CopyRestCatalogOptions(MemoryContext dst, const RestCatalogOptions * src)
{
	MemoryContext oldctx = MemoryContextSwitchTo(dst);
	RestCatalogOptions *copy = palloc0(sizeof(RestCatalogOptions));

	copy->serverOid = src->serverOid;
	copy->userMappingOid = src->userMappingOid;
	copy->catalog = src->catalog ? pstrdup(src->catalog) : NULL;
	copy->host = src->host ? pstrdup(src->host) : NULL;
	copy->oauthHostPath = src->oauthHostPath ? pstrdup(src->oauthHostPath) : NULL;
	copy->clientId = src->clientId ? pstrdup(src->clientId) : NULL;
	copy->clientSecret = src->clientSecret ? pstrdup(src->clientSecret) : NULL;
	copy->scope = src->scope ? pstrdup(src->scope) : NULL;
	copy->locationPrefix = src->locationPrefix ? pstrdup(src->locationPrefix) : NULL;
	copy->catalogName = src->catalogName ? pstrdup(src->catalogName) : NULL;
	copy->authType = src->authType;
	copy->enableVendedCredentials = src->enableVendedCredentials;

	MemoryContextSwitchTo(oldctx);
	return copy;
}
