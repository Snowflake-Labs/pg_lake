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
 * RestCatalogOptions by layering GUC defaults under the foreign
 * server's options.  Every REST catalog operation in the module --
 * auth, HTTP transport, table/namespace ops -- starts from the
 * RestCatalogOptions produced here.
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
 */
static void
ApplyGUCDefaults(RestCatalogOptions * opts)
{
	char	   *defaultLocationPrefix = GetIcebergDefaultLocationPrefix();

	opts->host = RestCatalogHost ? pstrdup(RestCatalogHost) : NULL;
	opts->oauthHostPath = RestCatalogOauthHostPath ? pstrdup(RestCatalogOauthHostPath) : NULL;
	opts->clientId = RestCatalogClientId ? pstrdup(RestCatalogClientId) : NULL;
	opts->clientSecret = RestCatalogClientSecret ? pstrdup(RestCatalogClientSecret) : NULL;
	opts->scope = RestCatalogScope ? pstrdup(RestCatalogScope) : NULL;
	opts->authType = RestCatalogAuthType;
	opts->enableVendedCredentials = RestCatalogEnableVendedCredentials;
	opts->locationPrefix = defaultLocationPrefix ? pstrdup(defaultLocationPrefix) : NULL;
}


/*
 * ValidateRestCatalogOptions checks that the resolved options have
 * the minimum required fields (e.g. rest_endpoint).
 */
static void
ValidateRestCatalogOptions(const RestCatalogOptions * opts, const char *catalog)
{
	if (opts->host == NULL || opts->host[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("\"rest_endpoint\" is not configured for REST catalog \"%s\"",
						catalog),
				 errhint("Set the pg_lake_iceberg.rest_catalog_host GUC or "
						 "the \"rest_endpoint\" option on the server.")));
}


/*
 * Build RestCatalogOptions for an iceberg_catalog server.
 *
 * The built-in pg_lake_rest_catalog server and any user-created
 * iceberg_catalog REST server go through the same path: GUC defaults
 * first, then server-level options applied on top.  ALTER SERVER OPTIONS
 * is blocked on the built-in server, so in practice its option set is
 * always empty and the GUC defaults survive untouched -- which is
 * exactly the historical "GUCs-only built-in REST" behavior, now
 * reached through a single code path.
 *
 * `userVisibleCatalog` is the short identifier the user typed
 * (e.g. "rest" or a user server name); it is what we store in
 * opts->catalog so that error messages, the cross-catalog DML check,
 * and the token cache key all stay in user-facing terms.  The long
 * built-in server name never leaks past this function.
 */
static RestCatalogOptions *
BuildRestCatalogOptionsFromServer(const char *serverName,
								  const char *userVisibleCatalog)
{
	ForeignServer *server = GetForeignServerByName(serverName, false);
	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	Assert(strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0);

	RestCatalogOptions *opts = palloc0(sizeof(RestCatalogOptions));

	opts->serverOid = server->serverid;
	opts->catalog = pstrdup(userVisibleCatalog);
	ApplyGUCDefaults(opts);
	ApplyServerOptionOverrides(opts, server);
	ValidateRestCatalogOptions(opts, userVisibleCatalog);
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
