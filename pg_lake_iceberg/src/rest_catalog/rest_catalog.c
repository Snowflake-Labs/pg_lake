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

#include <inttypes.h>

#include "postgres.h"
#include "miscadmin.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "common/base64.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "foreign/foreign.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/conffiles.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "nodes/parsenodes.h"

#include "pg_extension_base/base_workers.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/http/http_client.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/util/temporal_utils.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/url_encode.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/string_utils.h"


/* determined by GUC */
char	   *RestCatalogHost = "http://localhost:8181";
char	   *RestCatalogOauthHostPath = "";
char	   *RestCatalogClientId = NULL;
char	   *RestCatalogClientSecret = NULL;
char	   *RestCatalogScope = "PRINCIPAL_ROLE:ALL";
int			RestCatalogAuthType = REST_CATALOG_AUTH_TYPE_OAUTH2;
bool		RestCatalogEnableVendedCredentials = true;

/*
 * Per-catalog token cache. Keyed by catalog.
 */
#define TOKEN_CACHE_KEY_LEN NAMEDATALEN

typedef struct RestCatalogTokenCacheEntry
{
	char		key[TOKEN_CACHE_KEY_LEN];
	char	   *accessToken;
	TimestampTz accessTokenExpiry;
}			RestCatalogTokenCacheEntry;

static HTAB *RestCatalogTokenCache = NULL;
static MemoryContext RestTokenCacheCtx = NULL;

static char *GetRestCatalogAccessToken(RestCatalogOptions * opts, bool forceRefreshToken);
static void FetchRestCatalogAccessToken(RestCatalogOptions * opts, char **accessToken, int *expiresIn);
static void CreateNamespaceOnRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName);
static char *EncodeBasicAuth(const char *clientId, const char *clientSecret);
static char *JsonbGetStringByPath(const char *jsonb_text, int nkeys,...);
static List *GetHeadersWithAuth(RestCatalogOptions * opts);
static char *AppendIcebergPartitionSpecForRestCatalog(List *partitionSpecs);

PG_FUNCTION_INFO_V1(iceberg_catalog_validator);

/*
 * Valid options for iceberg_catalog servers and their user mappings
 */

static const char *iceberg_catalog_server_options[] = {
	"rest_endpoint",
	"scope",
	"rest_auth_type",
	"oauth_endpoint",
	"enable_vended_credentials",
	"location_prefix",
	"catalog_name",
	NULL
};

static const char *iceberg_catalog_user_mapping_options[] = {
	"client_id",
	"client_secret",
	"scope",
	NULL
};



static bool
is_valid_option_in_list(const char *keyword, const char *const *options)
{
	for (int i = 0; options[i] != NULL; i++)
	{
		if (pg_strcasecmp(keyword, options[i]) == 0)
			return true;
	}
	return false;
}


/*
 * iceberg_catalog_validator validates options for the iceberg_catalog FDW.
 *
 * Server options: rest_endpoint, scope, rest_auth_type, oauth_endpoint,
 *   enable_vended_credentials, location_prefix, catalog_name.
 * User mapping options: client_id, client_secret, scope.
 *
 * scope is accepted in both places; user mapping scope takes priority.
 */
Datum
iceberg_catalog_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	if (catalog == UserMappingRelationId)
	{
		foreach(cell, options_list)
		{
			DefElem    *def = (DefElem *) lfirst(cell);

			if (!is_valid_option_in_list(def->defname, iceberg_catalog_user_mapping_options))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
						 errmsg("invalid option \"%s\" for iceberg_catalog user mapping",
								def->defname),
						 errhint("Valid options are: client_id, client_secret, scope.")));
		}
		PG_RETURN_VOID();
	}

	/*
	 * Reject options in any context other than SERVER or USER MAPPING.
	 * PostgreSQL also calls the validator for the FDW itself and for foreign
	 * tables; allow empty option lists so those succeed.
	 */
	if (catalog != ForeignServerRelationId)
	{
		if (list_length(options_list) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("iceberg_catalog options are only valid for SERVER or USER MAPPING objects")));
		PG_RETURN_VOID();
	}

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option_in_list(def->defname, iceberg_catalog_server_options))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\" for iceberg_catalog server", def->defname),
					 errhint("Valid options are: rest_endpoint, rest_auth_type, "
							 "oauth_endpoint, scope, enable_vended_credentials, "
							 "location_prefix, catalog_name.")));
		}

		if (pg_strcasecmp(def->defname, "rest_auth_type") == 0)
		{
			char	   *authType = defGetString(def);

			if (pg_strcasecmp(authType, "oauth2") != 0 &&
				pg_strcasecmp(authType, "default") != 0 &&
				pg_strcasecmp(authType, "horizon") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid rest_auth_type option: \"%s\"", authType),
						 errhint("Valid values are \"oauth2\" and \"horizon\".")));
		}
		else if (pg_strcasecmp(def->defname, "enable_vended_credentials") == 0)
		{
			(void) defGetBoolean(def);
		}
	}

	PG_RETURN_VOID();
}


/*
 * IsIcebergCatalogServer returns true if the named server exists and
 * uses the iceberg_catalog FDW.
 */
static bool
IsIcebergCatalogServer(const char *serverName)
{
	ForeignServer *server = GetForeignServerByName(serverName, true);

	if (server == NULL)
		return false;

	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	return strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0;
}


/*
 * ScrubUserMappingSecrets overwrites secret option values in the query
 * string in-place with asterisks.
 *
 * In-place mutation is essential: pg_stat_statements captures the queryString
 * pointer before calling prev_ProcessUtility, then stores it after the call
 * returns. A copy with pstrdup would be invisible to pg_stat_statements
 * because its local pointer still references the original memory. By
 * overwriting the original buffer, every holder of that pointer — including
 * pg_stat_statements — sees the scrubbed version.
 *
 * The actual DDL execution is unaffected because CREATE/ALTER USER MAPPING
 * reads option values from the parse tree (DefElem nodes), not from
 * queryString.
 */
static void
ScrubUserMappingSecrets(const char *queryString, List *options)
{
	const char *secret_options[] = {"client_id", "client_secret", NULL};
	ListCell   *lc;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);

		if (!is_valid_option_in_list(def->defname, secret_options))
			continue;

		if (def->location < 0)
			continue;

		char	   *p = (char *) queryString + def->location;

		/* skip past the key name */
		while (*p && !isspace((unsigned char) *p) && *p != '\'')
			p++;

		/* skip whitespace between key and opening quote */
		while (*p && *p != '\'')
			p++;

		if (*p != '\'')
			continue;

		p++;					/* skip opening quote */

		/* overwrite value characters with '*', handling '' escapes */
		while (*p && *p != '\'')
			*p++ = '*';
		while (*(p + 1) == '\'')
		{
			*p++ = '*';			/* first quote of '' pair */
			*p++ = '*';			/* second quote of '' pair */
			while (*p && *p != '\'')
				*p++ = '*';
		}
	}
}


/*
 * ScrubIcebergUserMappingHandler is a ProcessUtility handler registered via
 * pg_lake_engine's RegisterUtilityStatementHandler.  When it detects a
 * CREATE/ALTER USER MAPPING targeting an iceberg_catalog server, it scrubs
 * secret values in the queryString in-place and returns false so normal
 * processing continues.
 */
bool
ScrubIcebergUserMappingHandler(ProcessUtilityParams *processUtilityParams,
							   void *arg)
{
	Node	   *parsetree = processUtilityParams->plannedStmt->utilityStmt;
	const char *serverName = NULL;
	List	   *options = NIL;

	if (IsA(parsetree, CreateUserMappingStmt))
	{
		CreateUserMappingStmt *stmt = (CreateUserMappingStmt *) parsetree;

		serverName = stmt->servername;
		options = stmt->options;
	}
	else if (IsA(parsetree, AlterUserMappingStmt))
	{
		AlterUserMappingStmt *stmt = (AlterUserMappingStmt *) parsetree;

		serverName = stmt->servername;
		options = stmt->options;
	}
	else
		return false;

	if (serverName == NULL || options == NIL)
		return false;

	if (!IsIcebergCatalogServer(serverName))
		return false;

	ScrubUserMappingSecrets(processUtilityParams->queryString, options);

	return false;
}


/*
 * BlockDDLOnExtensionCatalogs prevents misuse of the reserved catalog
 * names 'postgres', 'object_store', and 'rest'.  These are built-in
 * identifiers (not actual foreign servers), so we block:
 *
 *  - CREATE SERVER with a reserved name.
 *  - CREATE SERVER with TYPE 'postgres' or 'object_store'.
 *  - RENAME TO a reserved name.
 *
 * ALTER/DROP/OWNER on these names will fail naturally because no
 * server object exists.
 */
bool
BlockDDLOnExtensionCatalogs(ProcessUtilityParams * processUtilityParams,
							void *arg)
{
	Node	   *parsetree = processUtilityParams->plannedStmt->utilityStmt;

	if (creating_extension)
		return false;

	if (IsA(parsetree, CreateForeignServerStmt))
	{
		CreateForeignServerStmt *stmt = (CreateForeignServerStmt *) parsetree;

		if (stmt->fdwname == NULL ||
			strcmp(stmt->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0)
			return false;

		if (IsCatalogOwnedByExtension(stmt->servername))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("server name \"%s\" is reserved for the extension-owned catalog",
							stmt->servername),
					 errhint("Choose a different server name.")));

		if (stmt->servertype != NULL &&
			(pg_strcasecmp(stmt->servertype, POSTGRES_CATALOG_NAME) == 0 ||
			 pg_strcasecmp(stmt->servertype, OBJECT_STORE_CATALOG_NAME) == 0))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot create iceberg_catalog server with TYPE '%s'",
							stmt->servertype),
					 errhint("Use the built-in \"%s\" or \"%s\" catalogs, "
							 "or create a server of type 'rest'.",
							 POSTGRES_CATALOG_NAME, OBJECT_STORE_CATALOG_NAME)));
	}
	else if (IsA(parsetree, RenameStmt))
	{
		RenameStmt *stmt = (RenameStmt *) parsetree;

		if (stmt->renameType != OBJECT_FOREIGN_SERVER)
			return false;

		if (IsCatalogOwnedByExtension(stmt->newname))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("server name \"%s\" is reserved for the extension-owned catalog",
							stmt->newname),
					 errhint("Choose a different server name.")));
	}

	return false;
}


#define CATALOGS_CONF_FILENAME "catalogs.conf"

/*
 * LookupUserMappingOptions returns the user mapping options for the current
 * user on the given server, or NULL if no mapping exists. Checks for a
 * user-specific mapping first, then falls back to PUBLIC.
 */
static List *
LookupUserMappingOptions(Oid serverid)
{
	HeapTuple	tp;
	Datum		datum;
	bool		isnull;
	List	   *options;

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(GetUserId()),
						 ObjectIdGetDatum(serverid));

	if (!HeapTupleIsValid(tp))
	{
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverid));
	}

	if (!HeapTupleIsValid(tp))
		return NIL;

	datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
							Anum_pg_user_mapping_umoptions, &isnull);
	options = isnull ? NIL : untransformRelOptions(datum);
	ReleaseSysCache(tp);

	return options;
}


/*
 * ReadCatalogsConfCredentials reads $PGDATA/catalogs.conf and extracts
 * credentials for the given server name. The file uses PostgreSQL's
 * standard key = value format with dotted keys:
 *
 *   horizon.client_id = 'platform_id'
 *   horizon.client_secret = 'platform_secret'
 *   horizon.scope = 'PRINCIPAL_ROLE:ALL'
 *
 * Re-reads the file every time for simplicity -- these requests are
 * infrequent (one per REST catalog operation, not per row).
 *
 * Returns true if any credential was found for serverName.
 */
static bool
ReadCatalogsConfCredentials(const char *serverName,
							char **clientId, char **clientSecret,
							char **scope)
{
	char		path[MAXPGPATH];
	FILE	   *fp;
	ConfigVariable *head = NULL;
	ConfigVariable *tail = NULL;
	ConfigVariable *item;
	bool		found = false;
	int			serverNameLen = strlen(serverName);

	snprintf(path, MAXPGPATH, "%s/%s", DataDir, CATALOGS_CONF_FILENAME);

	fp = AllocateFile(path, "r");
	if (fp == NULL)
	{
		if (errno == ENOENT)
			return false;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not open catalog config file \"%s\": %m", path)));
		return false;
	}

	ParseConfigFp(fp, path, CONF_FILE_START_DEPTH, WARNING, &head, &tail);
	FreeFile(fp);

	for (item = head; item != NULL; item = item->next)
	{
		const char *key;

		if (item->errmsg != NULL)
			continue;

		/*
		 * Match entries of the form "servername.key = value". The dot
		 * separates the server name from the property name.
		 */
		if (strncmp(item->name, serverName, serverNameLen) != 0 ||
			item->name[serverNameLen] != '.')
			continue;

		key = item->name + serverNameLen + 1;

		if (strcmp(key, "client_id") == 0)
		{
			*clientId = pstrdup(item->value);
			found = true;
		}
		else if (strcmp(key, "client_secret") == 0)
		{
			*clientSecret = pstrdup(item->value);
			found = true;
		}
		else if (strcmp(key, "scope") == 0)
		{
			*scope = pstrdup(item->value);
			found = true;
		}
	}

	FreeConfigVariables(head);
	return found;
}


/*
 * RequireRestTypeForIcebergCatalogServer ensures that CREATE SERVER
 * commands using the iceberg_catalog FDW specify TYPE 'rest'.
 */
bool
RequireRestTypeForIcebergCatalogServer(ProcessUtilityParams * processUtilityParams,
									   void *arg)
{
	Node	   *parsetree = processUtilityParams->plannedStmt->utilityStmt;

	if (creating_extension)
		return false;

	if (!IsA(parsetree, CreateForeignServerStmt))
		return false;

	CreateForeignServerStmt *stmt = (CreateForeignServerStmt *) parsetree;

	if (stmt->fdwname == NULL ||
		strcmp(stmt->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0)
		return false;

	if (stmt->servertype == NULL ||
		pg_strcasecmp(stmt->servertype, REST_CATALOG_NAME) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("iceberg_catalog server requires TYPE 'rest'")));

	return false;
}


/*
 * GetRestCatalogOptionsFromCatalog returns a RestCatalogOptions struct.
 * For the built-in 'rest' catalog name the GUCs are used directly.
 * For user-created servers, the GUCs serve as defaults,
 * overridden by any option set on the server.
 *
 * Credential resolution order:
 * 1. pg_user_mapping for the current user + user-created server
 * 2. $PGDATA/catalogs.conf (re-read each time; infrequent requests)
 * 3. GUC variables (backward compatibility)
 * 4. Error if no credentials found
 */
RestCatalogOptions *
GetRestCatalogOptionsFromCatalog(const char *catalog)
{
	RestCatalogOptions *opts = palloc0(sizeof(RestCatalogOptions));

	/*
	 * Normalize built-in catalog name to the canonical constant so that
	 * case variations (e.g. 'REST', 'rEst') compare equal with strcmp.
	 * User-created server names are case-sensitive and stored as-is.
	 */
	if (pg_strcasecmp(catalog, REST_CATALOG_NAME) == 0)
		opts->catalog = pstrdup(REST_CATALOG_NAME);
	else
		opts->catalog = pstrdup(catalog);

	/* GUC values serve as defaults */
	opts->host = RestCatalogHost;
	opts->oauthHostPath = RestCatalogOauthHostPath;
	opts->clientId = RestCatalogClientId;
	opts->clientSecret = RestCatalogClientSecret;
	opts->scope = RestCatalogScope;
	opts->authType = RestCatalogAuthType;
	opts->enableVendedCredentials = RestCatalogEnableVendedCredentials;
	opts->locationPrefix = GetIcebergDefaultLocationPrefix();

	/*
	 * The built-in 'rest' name uses GUCs exclusively. For user-created
	 * servers, look up server options and override the GUC defaults.
	 */
	if (pg_strcasecmp(catalog, REST_CATALOG_NAME) != 0)
	{
		ForeignServer *server = GetForeignServerByName(catalog, false);
		ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

		Assert(strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0);

		ListCell   *lc;

		foreach(lc, server->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (pg_strcasecmp(def->defname, "rest_endpoint") == 0)
				opts->host = defGetString(def);
			else if (pg_strcasecmp(def->defname, "scope") == 0)
				opts->scope = defGetString(def);
			else if (pg_strcasecmp(def->defname, "rest_auth_type") == 0)
			{
				char	   *authType = defGetString(def);

				opts->authType = (pg_strcasecmp(authType, "horizon") == 0)
					? REST_CATALOG_AUTH_TYPE_HORIZON
					: REST_CATALOG_AUTH_TYPE_OAUTH2;
			}
			else if (pg_strcasecmp(def->defname, "oauth_endpoint") == 0)
				opts->oauthHostPath = defGetString(def);
			else if (pg_strcasecmp(def->defname, "enable_vended_credentials") == 0)
				opts->enableVendedCredentials = defGetBoolean(def);
			else if (pg_strcasecmp(def->defname, "catalog_name") == 0)
				opts->catalogName = defGetString(def);
			else if (pg_strcasecmp(def->defname, "location_prefix") == 0)
			{
				bool		inPlace = false;

				opts->locationPrefix = StripTrailingSlash(defGetString(def), inPlace);
			}
		}

		if (opts->host == NULL || opts->host[0] == '\0')
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("\"rest_endpoint\" is not configured for REST catalog \"%s\"",
							catalog),
					 errhint("Set the pg_lake_iceberg.rest_catalog_host GUC or "
							 "the \"rest_endpoint\" option on the server.")));

		/*
		 * Phase 2: Resolve credentials and scope.
		 *
		 * Priority: user mapping > catalogs.conf > GUC fallback (set above).
		 */
		List	   *umOptions = LookupUserMappingOptions(server->serverid);

		foreach(lc, umOptions)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (strcmp(def->defname, "client_id") == 0)
				opts->clientId = pstrdup(defGetString(def));
			else if (strcmp(def->defname, "client_secret") == 0)
				opts->clientSecret = pstrdup(defGetString(def));
			else if (strcmp(def->defname, "scope") == 0)
				opts->scope = pstrdup(defGetString(def));
		}

	}




	/* catalogs.conf overrides GUCs but not user mapping */
	char	   *confClientId = NULL;
	char	   *confClientSecret = NULL;
	char	   *confScope = NULL;

	if (ReadCatalogsConfCredentials(catalog,
									&confClientId, &confClientSecret,
									&confScope))
	{
		if (opts->clientId == NULL && confClientId != NULL)
			opts->clientId = confClientId;
		if (opts->clientSecret == NULL && confClientSecret != NULL)
			opts->clientSecret = confClientSecret;
		if (opts->scope == NULL && confScope != NULL)
			opts->scope = confScope;
	}

	if (opts->clientId == NULL || opts->clientSecret == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("no credentials found for iceberg_catalog \"%s\"",
						catalog)));

	return opts;
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

	return GetRestCatalogOptionsFromCatalog(catalog);
}


/*
* StartStageRestCatalogIcebergTableCreate stages the creation of an iceberg table
* in the rest catalog. On any failure, an error is raised. If the table exists,
* an error is raised as well.
*
* As per REST catalog spec, we need to provide an empty schema when creating
* a table. The schema will be updated when we make this table visible/committed.
* The main reason for staging early is to be able to get the vended credentials
* for writable tables.
*/
void
StartStageRestCatalogIcebergTableCreate(Oid relationId)
{
	const char *relationName = GetRestCatalogTableName(relationId);

	StringInfo	body = makeStringInfo();

	appendStringInfoChar(body, '{');	/* start body */
	appendJsonString(body, "name", relationName);

	appendStringInfoString(body, ", ");
	appendJsonKey(body, "schema");

	appendStringInfoChar(body, '{');	/* start schema object */

	appendJsonString(body, "type", "struct");
	appendStringInfoString(body, ", ");
	appendJsonKey(body, "fields");
	appendStringInfoString(body, "[]"); /* empty fields array, we don't know
										 * the schema yet */

	appendStringInfoChar(body, '}');	/* close schema object */
	appendStringInfoString(body, ", ");

	appendJsonString(body, "stage-create", "true");

	appendStringInfoChar(body, '}');	/* close body */

	const char *catalogName = GetRestCatalogName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	char	   *postUrl =
		psprintf(REST_CATALOG_TABLES, opts->host,
				 URLEncodePath(catalogName), URLEncodePath(namespaceName));
	List	   *headers = PostHeadersWithAuth(opts);

	if (opts->enableVendedCredentials)
	{
		char	   *vendedCreds = pstrdup("X-Iceberg-Access-Delegation: vended-credentials");

		headers = lappend(headers, vendedCreds);
	}

	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_POST, postUrl, body->data,
													  headers, opts);

	if (httpResult.status != 200)
	{
		ReportHTTPError(httpResult, ERROR);
	}
}


/*
* FinishStageRestCatalogIcebergTableCreateRestRequest creates the REST catalog
* request to finalize the staging of an iceberg table creation in the rest
* catalog.
*/
char *
FinishStageRestCatalogIcebergTableCreateRestRequest(Oid relationId, DataFileSchema * dataFileSchema, List *partitionSpecs)
{
	StringInfo	body = makeStringInfo();

	appendStringInfoChar(body, '{');

	appendJsonKey(body, "requirements");
	appendStringInfoChar(body, '[');	/* start requirements array */
	appendStringInfoChar(body, '{');	/* start requirements element */

	appendJsonString(body, "type", "assert-create");

	appendStringInfoChar(body, '}');	/* close requirements element */
	appendStringInfoChar(body, ']');	/* close requirements array */

	appendStringInfoChar(body, ',');

	appendJsonKey(body, "updates");
	appendStringInfoChar(body, '[');	/* start updates array */
	appendStringInfoChar(body, '{');	/* start updates element */

	appendJsonString(body, "action", "add-schema");

	appendStringInfoChar(body, ',');

	int			lastColumnId = 0;
	IcebergTableSchema *newSchema =
		RebuildIcebergSchemaFromDataFileSchema(relationId, dataFileSchema, &lastColumnId);
	int			schemaCount = 1;

	AppendIcebergTableSchemaForRestCatalog(body, newSchema, schemaCount);
	appendStringInfoChar(body, '}');	/* close updates element */

	appendStringInfoChar(body, ',');
	appendStringInfoChar(body, '{');	/* start add-sort-order */
	appendJsonString(body, "action", "add-sort-order");
	appendStringInfoString(body, ", ");
	appendJsonKey(body, "sort-order");
	appendStringInfoChar(body, '{');	/* start sort-order object */
	appendJsonInt32(body, "order-id", 0);
	appendStringInfoString(body, ", ");
	appendJsonKey(body, "fields");
	appendStringInfoString(body, "[]"); /* empty fields array */
	appendStringInfoChar(body, '}');	/* finish sort-order object */
	appendStringInfoChar(body, '}');	/* finish add-sort-order */
	appendStringInfoChar(body, ',');
	appendStringInfoChar(body, '{');	/* start add-sort-order */
	appendJsonString(body, "action", "set-default-sort-order");
	appendStringInfoString(body, ", ");
	appendJsonInt32(body, "sort-order-id", 0);
	appendStringInfoChar(body, '}');	/* finish add-sort-order */

	appendStringInfoString(body, ", ");
	appendStringInfoChar(body, '{');	/* start set-location */
	appendJsonString(body, "action", "set-location");
	appendStringInfoChar(body, ',');

	/* construct location */
	StringInfo	location = makeStringInfo();
	const char *catalogName = GetRestCatalogName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);
	const char *relationName = GetRestCatalogTableName(relationId);
	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	appendStringInfo(location, "%s/%s/%s/%s/%d", opts->locationPrefix, catalogName, namespaceName, relationName, relationId);
	appendJsonString(body, "location", location->data);
	appendStringInfoChar(body, '}');	/* end set-location */

	/* add partition spec */
	appendStringInfoChar(body, ',');

	ListCell   *partitionSpecCell = NULL;

	foreach(partitionSpecCell, partitionSpecs)
	{
		IcebergPartitionSpec *spec = (IcebergPartitionSpec *) lfirst(partitionSpecCell);

		appendStringInfoChar(body, '{');	/* start add-partition-spec */
		appendJsonString(body, "action", "add-spec");
		appendStringInfoString(body, ", ");

		appendStringInfoString(body, AppendIcebergPartitionSpecForRestCatalog(list_make1(spec)));

		appendStringInfoChar(body, '}');	/* finish add-partition-spec */
		appendStringInfoString(body, ", ");
	}

	if (list_length(partitionSpecs) == 0)
		appendStringInfoChar(body, ',');

	appendStringInfoChar(body, '{');	/* start set-default-spec */
	appendJsonString(body, "action", "set-default-spec");
	appendStringInfoString(body, ", ");
	appendJsonInt32(body, "spec-id", -1);	/* -1 means latest */
	appendStringInfoChar(body, '}');	/* finish set-default-spec */
	appendStringInfoChar(body, ']');	/* end updates array */
	appendStringInfoChar(body, '}');

	return body->data;
}


/*
* Register a namespace in the Rest Catalog.
* If the catalog exists, and the allowedLocations is different,
* an error is raised. This  is used to ensure that the same
* namespace is not registered multiple times as we define
* allowed locations as part of the namespace.
*/
void
RegisterNamespaceToRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName)
{
	/*
	 * First, we need to check if the namespace already exists in Rest Catalog
	 * via a GET request.
	 */
	char	   *getUrl =
		psprintf(REST_CATALOG_NAMESPACE_NAME,
				 opts->host, URLEncodePath(catalogName),
				 URLEncodePath(namespaceName));
	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_GET, getUrl, NULL,
													  GetHeadersWithAuth(opts),
													  opts);

	switch (httpResult.status)
	{
			/* namespace not found */
		case 404:
			{
				/*
				 * For debugging purposes
				 */
				ReportHTTPError(httpResult, DEBUG2);

				/*
				 * Does not exists, we'll create it.
				 */
				CreateNamespaceOnRestCatalog(opts, catalogName, namespaceName);
				break;
			}

			/* namespace already exists */
		case 200:
			{
				/*
				 * Verify allowed location matches, otherwise raise an error.
				 * We raise error because we use the default location as the
				 * place where tables are stored. So, we cannot afford to have
				 * different locations for the same namespace.
				 */
				char	   *serverAllowedLocation =
					JsonbGetStringByPath(httpResult.body, 2, "properties", "location");

				if (serverAllowedLocation)
				{
					const char *defaultAllowedLocation =
						psprintf("%s/%s/%s", opts->locationPrefix, catalogName, namespaceName);


					/*
					 * Compare by ignoring the trailing `/` char that the
					 * server might have for internal iceberg tables. For
					 * external ones, we don't have any control over.
					 */
					if ((strlen(serverAllowedLocation) - strlen(defaultAllowedLocation) > 1 ||
						 strncmp(serverAllowedLocation, defaultAllowedLocation, strlen(defaultAllowedLocation)) != 0))
					{
						ereport(DEBUG1,
								(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
								 errmsg("namespace \"%s\" is already registered with a different location than the default expected location based on default location prefix",
										namespaceName),
								 errdetail_internal("Expected location: %s, but got: %s",
													defaultAllowedLocation, serverAllowedLocation)));
					}
				}

				break;
			}

		default:
			{
				/*
				 * Report the error to the user. Expected errors: 400 - Bad
				 * Request 401 - Unauthorized 403 - Forbidden 419 -
				 * Credentials timed out 503 - Slowdown 5XX - Internal Server
				 * Error
				 */
				ReportHTTPError(httpResult, ERROR);

				break;
			}

	}
}


/*
* ErrorIfRestNamespaceDoesNotExist checks if the namespace exists in the Rest Catalog.
* If it does not exist, an error is raised. This is used to ensure that the
* namespace exists when creating a table in the given namespace.
*/
void
ErrorIfRestNamespaceDoesNotExist(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName)
{
	/*
	 * First, we need to check if the namespace already exists in Rest Catalog
	 * via a GET request.
	 */
	char	   *getUrl =
		psprintf(REST_CATALOG_NAMESPACE_NAME,
				 opts->host, URLEncodePath(catalogName),
				 URLEncodePath(namespaceName));
	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_GET, getUrl, NULL,
													  GetHeadersWithAuth(opts),
													  opts);

	/* namespace not found */
	if (httpResult.status == 404)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("namespace \"%s\" does not exist in the rest catalog while creating on catalog \"%s\"",
						namespaceName, catalogName)));
	}
	else if (httpResult.status != 200)
	{
		/*
		 * Report the error to the user. Expected errors: 400 - Bad Request
		 * 401 - Unauthorized 403 - Forbidden 419 - Credentials timed out 503
		 * - Slowdown 5XX - Internal Server Error
		 */
		ReportHTTPError(httpResult, ERROR);
	}
}


/*
* Gets the metadata location for a relation from the external rest catalog.
*/
char *
GetMetadataLocationForRestCatalogForIcebergTable(Oid relationId)
{
	const char *restCatalogName = GetRestCatalogName(relationId);
	const char *relationName = GetRestCatalogTableName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	return GetMetadataLocationFromRestCatalog(opts, restCatalogName, namespaceName, relationName);
}


/*
* Gets the metadata location for a relation from the external catalog.
*/
char *
GetMetadataLocationFromRestCatalog(RestCatalogOptions * opts, const char *restCatalogName, const char *namespaceName, const char *relationName)
{
	char	   *getUrl =
		psprintf(REST_CATALOG_TABLE,
				 opts->host, URLEncodePath(restCatalogName), URLEncodePath(namespaceName), URLEncodePath(relationName));

	List	   *headers = GetHeadersWithAuth(opts);
	HttpResult	hr = SendRequestToRestCatalog(HTTP_GET, getUrl, NULL, headers,
											  opts);

	if (hr.status != 200)
	{
		ReportHTTPError(hr, ERROR);
	}

	char	   *metadataLocation = JsonbGetStringByPath(hr.body, 1, "metadata-location");

	if (metadataLocation == NULL)
		ereport(ERROR, (errmsg("key \"metadata-location\" missing in json response")));

	return metadataLocation;
}


/*
* CreateNamespaceOnRestCatalog creates a namespace on the rest catalog. On any failure,
* an error is raised.
*/
static void
CreateNamespaceOnRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName)
{
	/* POST create */
	StringInfoData body;

	initStringInfo(&body);
	appendStringInfoChar(&body, '{');	/* start body */
	appendJsonKey(&body, "namespace");

	appendStringInfoChar(&body, '[');	/* start namespace array */
	appendJsonValue(&body, namespaceName);
	appendStringInfoChar(&body, ']');	/* close namespace array */

	appendStringInfoChar(&body, ',');	/* close namespace array */

	/* set properties location */
	appendJsonKey(&body, "properties");

	appendStringInfoChar(&body, '{');	/* start properties object */
	appendStringInfoChar(&body, '}');	/* close properties object */

	appendStringInfoChar(&body, '}');	/* close body */

	char	   *postUrl =
		psprintf(REST_CATALOG_NAMESPACE, opts->host,
				 URLEncodePath(catalogName));

	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_POST, postUrl, body.data,
													  PostHeadersWithAuth(opts),
													  opts);

	if (httpResult.status != 200)
	{
		ReportHTTPError(httpResult, ERROR);
	}
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
static List *
GetHeadersWithAuth(RestCatalogOptions * opts)
{
	bool		forceRefreshToken = false;

	return list_make2(psprintf("Authorization: Bearer %s", GetRestCatalogAccessToken(opts, forceRefreshToken)),
					  pstrdup("Accept: application/json"));
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
 * Build a cache key for the per-catalog token cache.
 */
static void
BuildTokenCacheKey(char *key, const RestCatalogOptions * opts)
{
	Assert(opts->catalog != NULL);
	strlcpy(key, opts->catalog, TOKEN_CACHE_KEY_LEN);
}


/*
 * Initialize the per-catalog token cache hash table if needed.
 */
static void
InitTokenCacheIfNeeded(void)
{
	if (RestCatalogTokenCache != NULL)
		return;

	RestTokenCacheCtx = AllocSetContextCreate(TopMemoryContext,
											  "RestTokenCacheCtx",
											  ALLOCSET_DEFAULT_SIZES);

	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = TOKEN_CACHE_KEY_LEN;
	ctl.entrysize = sizeof(RestCatalogTokenCacheEntry);
	ctl.hcxt = RestTokenCacheCtx;

	RestCatalogTokenCache = hash_create("REST Catalog Token Cache",
										8, &ctl,
										HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}


/*
 * Gets an access token from rest catalog. Caches the token per catalog
 * (keyed by catalog) until it is about to expire.
 */
static char *
GetRestCatalogAccessToken(RestCatalogOptions * opts, bool forceRefreshToken)
{
	InitTokenCacheIfNeeded();

	char		cacheKey[TOKEN_CACHE_KEY_LEN];

	BuildTokenCacheKey(cacheKey, opts);

	bool		found = false;
	RestCatalogTokenCacheEntry *entry =
		hash_search(RestCatalogTokenCache, cacheKey, HASH_ENTER, &found);

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
			pfree(entry->accessToken);
		entry->accessToken = NULL;
		entry->accessTokenExpiry = 0;

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
	if (!opts->host || !*opts->host)
		ereport(ERROR,
				(errmsg("REST catalog host is not configured"),
				 errhint("Set the \"rest_endpoint\" option on the server "
						 "or the pg_lake_iceberg.rest_catalog_host GUC.")));
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

	/* POST — pass NULL opts to skip 419 token refresh (avoids recursion) */
	HttpResult	httpResponse = SendRequestToRestCatalog(HTTP_POST, accessTokenUrl,
														body.data, headers,
														NULL);

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
 * Get a string value at the given JSON path: key1 -> key2 -> ... -> keyN
 * - jsonb_text: input JSON text (e.g., from an HTTP response)
 * - nkeys: number of keys in the path
 * - ...: const char* keys, in order
 *
 * On success: returns palloc'd C-string in the current memory context.
 * On failure: ERROR (missing key, non-object mid-level, non-string leaf).
 */
static char *
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
* Readable rest catalog tables always use the catalog_table_name option
* as the table name in the external catalog. Writable rest catalog tables
* use the Postgres table name as the catalog table name.
*/
char *
GetRestCatalogTableName(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	if (catalogType == REST_CATALOG_READ_ONLY)
	{
		ForeignTable *foreignTable = GetForeignTable(relationId);
		List	   *options = foreignTable->options;

		char	   *catalogTableName = GetStringOption(options, "catalog_table_name", false);

		/* user provided the custom catalog table name */
		if (!catalogTableName)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("catalog_table_name option is required for rest catalog iceberg tables")));

		return catalogTableName;
	}
	else
	{
		/* for writable rest catalog tables, we use the Postgres table name */
		return get_rel_name(relationId);
	}
}


/*
* Readable rest catalog tables always use the catalog_namespace option
* as the namespace in the external catalog. Writable rest catalog tables
* use the Postgres schema name as the namespace.
*/
char *
GetRestCatalogNamespace(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	if (catalogType == REST_CATALOG_READ_ONLY)
	{

		ForeignTable *foreignTable = GetForeignTable(relationId);
		List	   *options = foreignTable->options;

		char	   *catalogNamespace = GetStringOption(options, "catalog_namespace", false);

		/* user provided the custom catalog namespace */
		if (!catalogNamespace)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("catalog_namespace option is required for rest catalog iceberg tables")));

		return catalogNamespace;
	}
	else
	{
		/* for writable rest catalog tables, we use the Postgres schema name */
		return get_namespace_name(get_rel_namespace(relationId));
	}
}


/*
 * Returns the catalog name to use for REST API calls.
 *
 * Precedence: table option catalog_name > server option catalog_name
 *             > current database name.
 *
 * Read-only tables must have catalog_name set (on the table or server).
 */
char *
GetRestCatalogName(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	ForeignTable *foreignTable = GetForeignTable(relationId);
	char	   *catalogName = GetStringOption(foreignTable->options, "catalog_name", false);

	if (catalogName != NULL)
		return catalogName;

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	if (opts->catalogName != NULL)
		return opts->catalogName;

	if (catalogType == REST_CATALOG_READ_ONLY)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("catalog_name is required for read-only REST catalog tables"),
				 errhint("Set catalog_name on the table or the server.")));

	return get_database_name(MyDatabaseId);
}


/*
* Appends the given IcebergPartitionSpec list as JSON to the given StringInfo, specifically
* for use in Rest Catalog requests.
*/
static char *
AppendIcebergPartitionSpecForRestCatalog(List *partitionSpecs)
{
	StringInfo	command = makeStringInfo();

	ListCell   *partitionSpecCell = NULL;

	foreach(partitionSpecCell, partitionSpecs)
	{
		IcebergPartitionSpec *spec = (IcebergPartitionSpec *) lfirst(partitionSpecCell);

		appendJsonKey(command, "spec");
		appendStringInfoString(command, "{");

		/* append spec-id */
		appendJsonInt32(command, "spec-id", spec->spec_id);

		/* Append fields */
		appendStringInfoString(command, ", \"fields\":");
		AppendIcebergPartitionSpecFields(command, spec->fields, spec->fields_length);

		appendStringInfoString(command, "}");
	}
	return command->data;
}


/*
* GetAddSnapshotCatalogRequest creates a RestCatalogRequest to add a snapshot
* to the rest catalog for the given new snapshot.
*/
RestCatalogRequest *
GetAddSnapshotCatalogRequest(IcebergSnapshot * newSnapshot, Oid relationId)
{
	StringInfo	body = makeStringInfo();

	appendStringInfoString(body,
						   "{\"action\":\"add-snapshot\",\"snapshot\":{");

	appendStringInfo(body, "\"snapshot-id\":%" PRId64, newSnapshot->snapshot_id);
	if (newSnapshot->parent_snapshot_id > 0)
		appendStringInfo(body, ",\"parent-snapshot-id\":%" PRId64, newSnapshot->parent_snapshot_id);

	appendStringInfo(body, ",\"sequence-number\":%" PRId64, newSnapshot->sequence_number);
	appendStringInfo(body, ",\"timestamp-ms\":%ld", (long) (PostgresTimestampToIcebergTimestampMs()));	/* coarse ms */
	appendStringInfo(body, ",\"manifest-list\":\"%s\"", newSnapshot->manifest_list);
	appendStringInfoString(body, ",\"summary\":{\"operation\": \"append\"}");
	appendStringInfo(body, ",\"schema-id\":%d", newSnapshot->schema_id);
	appendStringInfoString(body, "}}, ");	/* end add-snapshot */

	appendStringInfo(body, "{\"action\":\"set-snapshot-ref\", \"type\":\"branch\", \"ref-name\":\"main\", \"snapshot-id\":" INT64_FORMAT "}", newSnapshot->snapshot_id);

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_ADD_SNAPSHOT;
	request->body = body->data;

	return request;
}


/*
 * GetAddSchemaCatalogRequest creates a RestCatalogRequest that adds a schema
 * to the table and sets it as the current schema (schema-id = -1 means
 * "the last added schema" per the REST spec).
 */
RestCatalogRequest *
GetAddSchemaCatalogRequest(Oid relationId, DataFileSchema * dataFileSchema)
{
	StringInfo	body = makeStringInfo();

	/* add-schema */
	appendStringInfoString(body, "{\"action\":\"add-schema\",");

	int			lastColumnId = 0;
	IcebergTableSchema *newSchema =
		RebuildIcebergSchemaFromDataFileSchema(relationId, dataFileSchema, &lastColumnId);

	int			schemaCount = 1;

	AppendIcebergTableSchemaForRestCatalog(body, newSchema, schemaCount);

	/* set-current-schema to the one we just added */
	appendStringInfoString(body, "}, {\"action\":\"set-current-schema\",\"schema-id\":-1}");

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_ADD_SCHEMA;
	request->body = body->data;

	return request;
}

/*
 * GetSetCurrentSchemaCatalogRequest creates a RestCatalogRequest that sets
 * the current schema to the given schema ID.
 */
RestCatalogRequest *
GetSetCurrentSchemaCatalogRequest(Oid relationId, int32_t schemaId)
{
	StringInfo	body = makeStringInfo();

	/* set-current-schema to the given schema ID */
	appendStringInfo(body, "{\"action\":\"set-current-schema\",\"schema-id\":%d}", schemaId);

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_SET_CURRENT_SCHEMA;
	request->body = body->data;

	return request;
}


/*
 * GetAddPartitionCatalogRequest creates a RestCatalogRequest that adds a
 * partition spec and sets it as the default (spec-id = -1 means "last added").
 */
RestCatalogRequest *
GetAddPartitionCatalogRequest(Oid relationId, List *partitionSpecs)
{
	StringInfo	body = makeStringInfo();

	/* add-spec */
	appendStringInfoString(body, "{\"action\":\"add-spec\",");

	char	   *bodyPart = AppendIcebergPartitionSpecForRestCatalog(partitionSpecs);

	appendStringInfoString(body, bodyPart);
	appendStringInfoChar(body, '}');

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_ADD_PARTITION;
	request->body = body->data;

	return request;
}


/*
 * GetAddPartitionCatalogRequest creates a RestCatalogRequest that adds a
 * partition spec and sets it as the default (spec-id = -1 means "last added").
 */
RestCatalogRequest *
GetSetPartitionDefaultIdCatalogRequest(Oid relationId, int specId)
{
	StringInfo	body = makeStringInfo();

	/* set-default-spec to the one we just added */
	appendStringInfo(body, "{\"action\":\"set-default-spec\",\"spec-id\":%d}", specId);

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_SET_DEFAULT_PARTITION_ID;
	request->body = body->data;

	return request;
}


/*
 * GetRemoveSnapshotCatalogRequest creates a RestCatalogRequest that removes
 * a list of snapshots from the REST catalog.
 */
RestCatalogRequest *
GetRemoveSnapshotCatalogRequest(List *removedSnapshotIds, Oid relationId)
{
	StringInfo	body = makeStringInfo();
	bool		first = true;

	appendStringInfoString(body,
						   "{\"action\":\"remove-snapshots\",\"snapshot-ids\":[");
	ListCell   *lc;

	foreach(lc, removedSnapshotIds)
	{
		int64_t		snapshotId = *((int64_t *) lfirst(lc));

		if (!first)
			appendStringInfoChar(body, ',');

		appendStringInfo(body, "%" PRId64, snapshotId);

		first = false;
	}

	appendStringInfoString(body, "]}");

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_REMOVE_SNAPSHOT;
	request->body = body->data;

	return request;
}


/*
 * SendRequestToRestCatalog sends an HTTP request to the rest catalog
 * with retry logic for retriable errors, attempting up to MAX_HTTP_RETRY_FOR_REST_CATALOG
 * times.  When opts is non-NULL the retry callback can force-refresh the
 * access token and patch the Authorization header on a 419 response.
 * Pass opts = NULL for the token-fetch request itself to avoid recursion.
 */
HttpResult
SendRequestToRestCatalog(HttpMethod method, const char *url, const char *body,
						 List *headers, RestCatalogOptions * opts)
{
	const int	MAX_HTTP_RETRY_FOR_REST_CATALOG = 3;

	return SendHttpRequestWithRetry(method, url, body, headers,
									ShouldRetryRequestToRestCatalog,
									MAX_HTTP_RETRY_FOR_REST_CATALOG,
									opts);
}


/*
 * ShouldRetryRequestToRestCatalog checks if the given HTTP result status is retriable.
 * If it is retriable, it performs necessary actions (like sleeping or refreshing token)
 * and returns true. Otherwise, it returns false.
 */
bool
ShouldRetryRequestToRestCatalog(long status, int maxRetry, int retryNo,
								void *context, List *headers)
{
	if (retryNo > maxRetry)
		return false;

	const int	TOO_MANY_REQUEST_STATUS = 429;
	const int	SERVER_UNAVAILABLE_STATUS = 503;
	const int	TOKEN_EXPIRED_STATUS = 419;

	/* too many request, wait some time */
	if (status == TOO_MANY_REQUEST_STATUS)
	{
		int			baseMs = 500;

		/*
		 * LightSleep reacts to signals, and can easily throw an error (e.g.,
		 * cancel backend). This function can be called at post-commit hook,
		 * so normally we wouldn't want any errors to happen, but then
		 * Postgres already prevents post-commit backends to receive signals.
		 */
		LightSleep(LinearBackoffSleepMs(baseMs, retryNo));
		return true;
	}

	/* server unavailable, lets wait a bit more */
	else if (status == SERVER_UNAVAILABLE_STATUS)
	{
		int			baseMs = 5000;

		LightSleep(LinearBackoffSleepMs(baseMs, retryNo));
		return true;
	}

	/* token expired, retry after refreshing token */
	else if (status == TOKEN_EXPIRED_STATUS)
	{
		RestCatalogOptions *opts = (RestCatalogOptions *) context;

		if (opts == NULL)
			return false;

		/*
		 * We normally refresh the token only when it is about to expire
		 * (forceRefreshToken = false), just 1 minute before the expiration
		 * for each request. Retry logic makes it safer by ensuring we get a
		 * fresh token for unforeseen circumstances.
		 */
		bool		forceRefreshToken = true;
		char	   *newToken = GetRestCatalogAccessToken(opts, forceRefreshToken);

		linitial(headers) = psprintf("Authorization: Bearer %s", newToken);
		return true;
	}

	/* successful or other error, no retry */
	return false;
}
