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

#include "access/genam.h"
#include "access/reloptions.h"
#include "access/table.h"
#include "catalog/pg_class.h"
#include "catalog/pg_depend.h"
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
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/inval.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "pg_extension_base/base_workers.h"
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
 * Path to the optional credentials file consulted between server
 * options and pg_user_mapping during REST catalog resolution.  Defined
 * via the pg_lake_iceberg.catalogs_conf_credentials_path GUC in init.c.  Relative
 * paths are resolved against DataDir.
 */
char	   *CatalogsConfCredentialsPath = NULL;

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
 *     back to catalogs.conf / GUCs).
 *
 * Should always be accessed via GetRestCatalogAccessToken().
 */
typedef struct RestCatalogTokenCacheKey
{
	Oid			serverOid;
	Oid			userMappingOid;
}			RestCatalogTokenCacheKey;

typedef struct RestCatalogTokenCacheEntry
{
	RestCatalogTokenCacheKey key;	/* hash key */
	char	   *accessToken;
	TimestampTz accessTokenExpiry;
}			RestCatalogTokenCacheEntry;

static HTAB *RestCatalogTokenCache = NULL;
static MemoryContext RestTokenCacheCtx = NULL;

/* end of per-catalog token cache variables */

static char *GetRestCatalogAccessToken(RestCatalogOptions * opts, bool forceRefreshToken);
static void FetchRestCatalogAccessToken(RestCatalogOptions * opts, char **accessToken, int *expiresIn);
static void CreateNamespaceOnRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName);
static char *EncodeBasicAuth(const char *clientId, const char *clientSecret);
static char *JsonbGetStringByPath(const char *jsonb_text, int nkeys,...);
static List *GetHeadersWithAuth(RestCatalogOptions * opts);
static char *AppendIcebergPartitionSpecForRestCatalog(List *partitionSpecs);
static void UpdateAuthorizationHeader(List *headers, const char *token);

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

PG_FUNCTION_INFO_V1(iceberg_catalog_validator);


/*
 * Descriptor for a single iceberg_catalog option.  This is the single
 * source of truth: validation, the user-facing hint, and the
 * option-to-struct applier all derive from this table.  An option may
 * be valid on a SERVER, on a USER MAPPING, or both (see contexts).
 */
typedef enum IcebergCatalogOptionType
{
	CATALOG_OPT_STRING,
	CATALOG_OPT_BOOL,
	CATALOG_OPT_AUTH_TYPE,
	CATALOG_OPT_LOCATION_PREFIX
}			IcebergCatalogOptionType;

/* Validation flags checked at CREATE/ALTER time. */
#define CATALOG_OPT_NONEMPTY    0x01	/* reject empty string */
#define CATALOG_OPT_HAS_SCHEME  0x02	/* must contain "://" */

/*
 * Where an option may legally appear.  An option can carry any bitwise
 * combination -- `scope`, for instance, is accepted in both contexts
 * with USER MAPPING winning by virtue of being applied last during
 * resolution.
 */
#define CATALOG_OPT_CTX_SERVER       0x01
#define CATALOG_OPT_CTX_USER_MAPPING 0x02

typedef struct IcebergCatalogOptionDesc
{
	const char *name;
	IcebergCatalogOptionType type;
	size_t		offset;			/* offsetof into RestCatalogOptions */
	int			flags;			/* CATALOG_OPT_NONEMPTY |
								 * CATALOG_OPT_HAS_SCHEME */
	int			contexts;		/* CATALOG_OPT_CTX_SERVER |
								 * CATALOG_OPT_CTX_USER_MAPPING */
}			IcebergCatalogOptionDesc;

static const IcebergCatalogOptionDesc iceberg_catalog_option_descs[] = {
	{"rest_endpoint", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, host),
		CATALOG_OPT_NONEMPTY | CATALOG_OPT_HAS_SCHEME,
	CATALOG_OPT_CTX_SERVER},
	{"rest_auth_type", CATALOG_OPT_AUTH_TYPE, offsetof(RestCatalogOptions, authType),
		0,
	CATALOG_OPT_CTX_SERVER},
	{"oauth_endpoint", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, oauthHostPath),
		CATALOG_OPT_NONEMPTY | CATALOG_OPT_HAS_SCHEME,
	CATALOG_OPT_CTX_SERVER},
	{"scope", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, scope),
		CATALOG_OPT_NONEMPTY,
	CATALOG_OPT_CTX_SERVER | CATALOG_OPT_CTX_USER_MAPPING},
	{"enable_vended_credentials", CATALOG_OPT_BOOL, offsetof(RestCatalogOptions, enableVendedCredentials),
		0,
	CATALOG_OPT_CTX_SERVER},
	{"location_prefix", CATALOG_OPT_LOCATION_PREFIX, offsetof(RestCatalogOptions, locationPrefix),
		CATALOG_OPT_NONEMPTY | CATALOG_OPT_HAS_SCHEME,
	CATALOG_OPT_CTX_SERVER},
	{"catalog_name", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, catalogName),
		CATALOG_OPT_NONEMPTY,
	CATALOG_OPT_CTX_SERVER},
	{"client_id", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, clientId),
		CATALOG_OPT_NONEMPTY,
	CATALOG_OPT_CTX_USER_MAPPING},
	{"client_secret", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, clientSecret),
		CATALOG_OPT_NONEMPTY,
	CATALOG_OPT_CTX_USER_MAPPING},
};

#define NUM_CATALOG_OPTIONS lengthof(iceberg_catalog_option_descs)


/*
 * Look up a descriptor by option name, or return NULL if not found.
 */
static const IcebergCatalogOptionDesc *
FindCatalogOptionDesc(const char *name)
{
	for (int i = 0; i < NUM_CATALOG_OPTIONS; i++)
	{
		if (pg_strcasecmp(name, iceberg_catalog_option_descs[i].name) == 0)
			return &iceberg_catalog_option_descs[i];
	}
	return NULL;
}


/*
 * Build the "Valid options are: ?" hint string for the given context
 * (either CATALOG_OPT_CTX_SERVER or CATALOG_OPT_CTX_USER_MAPPING).
 */
static const char *
GetValidCatalogOptionsHint(int contextBit)
{
	StringInfoData buf;
	bool		first = true;

	Assert(contextBit == CATALOG_OPT_CTX_SERVER ||
		   contextBit == CATALOG_OPT_CTX_USER_MAPPING);

	initStringInfo(&buf);
	appendStringInfoString(&buf, "Valid options are: ");

	for (int i = 0; i < NUM_CATALOG_OPTIONS; i++)
	{
		if (!(iceberg_catalog_option_descs[i].contexts & contextBit))
			continue;

		if (!first)
			appendStringInfoString(&buf, ", ");

		appendStringInfoString(&buf, iceberg_catalog_option_descs[i].name);
		first = false;
	}

	appendStringInfoChar(&buf, '.');

	return buf.data;
}


/*
 * Validate a single option value.  Called from iceberg_catalog_validator
 * after the name has already been accepted.  Type-specific checks run
 * first, then flag-based checks (non-empty, scheme present).
 */
static void
ValidateCatalogOptionValue(const IcebergCatalogOptionDesc * desc, DefElem *def)
{
	switch (desc->type)
	{
		case CATALOG_OPT_AUTH_TYPE:
			{
				char	   *authType = defGetString(def);

				if (pg_strcasecmp(authType, "default") != 0 &&
					pg_strcasecmp(authType, "oauth2") != 0 &&
					pg_strcasecmp(authType, "horizon") != 0)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("invalid rest_auth_type option: \"%s\"", authType),
							 errhint("Valid values are \"default\", \"oauth2\", and \"horizon\".")));
				return;
			}
		case CATALOG_OPT_BOOL:
			(void) defGetBoolean(def);
			return;
		default:
			break;
	}

	if (desc->flags == 0)
		return;

	char	   *value = defGetString(def);

	if ((desc->flags & CATALOG_OPT_NONEMPTY) && value[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for \"%s\": must not be empty",
						desc->name)));

	if ((desc->flags & CATALOG_OPT_HAS_SCHEME) && strstr(value, "://") == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid value for \"%s\": \"%s\"",
						desc->name, value),
				 errhint("Include a URI scheme (e.g. \"https://...\").")));
}


/*
 * Apply a single server option onto the RestCatalogOptions struct.
 * Called from ApplyServerOptionOverrides for each DefElem on the server.
 */
static void
ApplyCatalogOptionValue(RestCatalogOptions * opts,
						const IcebergCatalogOptionDesc * desc, DefElem *def)
{
	switch (desc->type)
	{
		case CATALOG_OPT_STRING:
			*(char **) ((char *) opts + desc->offset) = pstrdup(defGetString(def));
			break;
		case CATALOG_OPT_BOOL:
			*(bool *) ((char *) opts + desc->offset) = defGetBoolean(def);
			break;
		case CATALOG_OPT_AUTH_TYPE:
			{
				char	   *authType = defGetString(def);

				*(int *) ((char *) opts + desc->offset) =
					(pg_strcasecmp(authType, "horizon") == 0)
					? REST_CATALOG_AUTH_TYPE_HORIZON
					: REST_CATALOG_AUTH_TYPE_OAUTH2;
				break;
			}
		case CATALOG_OPT_LOCATION_PREFIX:
			{
				bool		inPlace = false;

				*(char **) ((char *) opts + desc->offset) =
					pstrdup(StripTrailingSlash(defGetString(def), inPlace));
				break;
			}
	}
}


/*
 * iceberg_catalog_validator validates options for the iceberg_catalog FDW.
 *
 * Options are accepted on two catalog objects:
 *
 *   - ForeignServerRelationId:  CREATE SERVER / ALTER SERVER OPTIONS.
 *     Anything non-credential: rest_endpoint, rest_auth_type,
 *     oauth_endpoint, scope, enable_vended_credentials, location_prefix,
 *     catalog_name.
 *
 *   - UserMappingRelationId:    CREATE USER MAPPING / ALTER USER MAPPING
 *     OPTIONS.  Per-user credentials: client_id, client_secret, and
 *     optionally scope (overrides whatever is set on the server).
 *
 * client_id and client_secret are credentials and therefore belong on
 * a user mapping; they may also be supplied by $PGDATA/catalogs.conf
 * or GUCs.  Each option's descriptor carries a CATALOG_OPT_CTX_*
 * bitmask saying where it is legal.
 *
 * PostgreSQL also calls the validator for CREATE FOREIGN DATA WRAPPER
 * itself (with ForeignDataWrapperRelationId); allow empty option lists
 * there so extension creation succeeds.
 */
Datum
iceberg_catalog_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalogRelId = PG_GETARG_OID(1);
	ListCell   *cell;
	int			contextBit;
	const char *contextLabel;

	if (catalogRelId == ForeignServerRelationId)
	{
		contextBit = CATALOG_OPT_CTX_SERVER;
		contextLabel = "server";
	}
	else if (catalogRelId == UserMappingRelationId)
	{
		contextBit = CATALOG_OPT_CTX_USER_MAPPING;
		contextLabel = "user mapping";
	}
	else
	{
		if (list_length(options_list) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("iceberg_catalog options are only valid for "
							"SERVER and USER MAPPING objects")));
		PG_RETURN_VOID();
	}

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		const		IcebergCatalogOptionDesc *desc = FindCatalogOptionDesc(def->defname);

		if (desc == NULL || !(desc->contexts & contextBit))
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\" for iceberg_catalog %s",
							def->defname, contextLabel),
					 errhint("%s", GetValidCatalogOptionsHint(contextBit))));

		ValidateCatalogOptionValue(desc, def);
	}

	PG_RETURN_VOID();
}


/*
 * ServerHasDependentRestIcebergTable returns true if the given server
 * has at least one dependent REST-backed iceberg table (read-only or
 * writable) recorded in pg_depend.  Used to block ALTER SERVER changes
 * that would silently break existing tables, since both writable and
 * read-only REST tables make REST API calls at runtime against the
 * server's rest_endpoint.
 */
static bool
ServerHasDependentRestIcebergTable(Oid serverOid)
{
	Relation	depRel;
	ScanKeyData key[2];
	SysScanDesc scan;
	HeapTuple	tup;
	bool		found = false;

	depRel = table_open(DependRelationId, AccessShareLock);

	ScanKeyInit(&key[0],
				Anum_pg_depend_refclassid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(ForeignServerRelationId));
	ScanKeyInit(&key[1],
				Anum_pg_depend_refobjid,
				BTEqualStrategyNumber, F_OIDEQ,
				ObjectIdGetDatum(serverOid));

	scan = systable_beginscan(depRel, DependReferenceIndexId, true,
							  NULL, 2, key);

	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		Form_pg_depend depForm = (Form_pg_depend) GETSTRUCT(tup);

		if (depForm->classid != RelationRelationId)
			continue;

		IcebergCatalogType type = GetIcebergCatalogType(depForm->objid);

		if (type == REST_CATALOG_READ_WRITE || type == REST_CATALOG_READ_ONLY)
		{
			found = true;
			break;
		}
	}

	systable_endscan(scan);
	table_close(depRel, AccessShareLock);

	return found;
}


/*
 * RejectIfBuiltinCatalogServerName errors out if `name` is one of the
 * three internal pg_lake_iceberg catalog server names.  Shared by the
 * CREATE SERVER and ALTER SERVER ... RENAME TO branches, where the
 * concern is "users must not be able to create or end up with a server
 * carrying one of these reserved names".
 */
static void
RejectIfBuiltinCatalogServerName(const char *name)
{
	if (!IsBuiltinCatalogServerName(name))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("server name \"%s\" is reserved for an internal "
					"pg_lake_iceberg catalog server", name),
			 errhint("Choose a different server name.")));
}


/*
 * RejectIfModifyingBuiltinCatalogServer errors out if `name` refers to
 * one of the three built-in pg_lake_iceberg catalog servers.  The
 * `operation` verb fills in the user-facing error ("cannot %s the
 * built-in...") and should read naturally in that template (e.g.
 * "alter", "change owner of").  Shared by the ALTER SERVER OPTIONS
 * and ALTER SERVER ... OWNER TO branches.
 */
static void
RejectIfModifyingBuiltinCatalogServer(const char *name, const char *operation)
{
	if (!IsBuiltinCatalogServerName(name))
		return;

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("cannot %s the built-in pg_lake_iceberg "
					"catalog server \"%s\"", operation, name),
			 errhint("Configure built-in catalogs via "
					 "pg_lake_iceberg GUCs, or create a "
					 "user-defined iceberg_catalog server.")));
}


/*
 * ValidateIcebergCatalogServerDDL validates DDL on iceberg_catalog
 * servers and on user mappings that target them, the iceberg_catalog FDW itself,
 * and extension-membership changes that affect any of these objects.
 *
 * Two layers of protection for server DDL:
 *
 * 1. Short reserved names ('postgres', 'object_store', 'rest') -- these
 *    are the user-facing catalog= values.  We block CREATE SERVER and
 *    RENAME TO these names so users can't shadow the built-in catalogs.
 *
 * 2. Built-in long server names ('pg_lake_postgres_catalog', etc.) --
 *    these are the pre-created anchors.  Outside of CREATE/ALTER
 *    EXTENSION we block CREATE/ALTER/RENAME/OWNER on them entirely so
 *    they remain pure structural anchors with all configuration in
 *    GUCs.  DROP is blocked by core via extension membership.
 *
 * Additionally:
 *  - CREATE SERVER must specify TYPE 'rest'; TYPE 'postgres' and
 *    'object_store' are reserved for the built-in servers.
 *  - Renaming any iceberg_catalog server is blocked (dependent tables
 *    record the server name as a string option in ftoptions).
 *  - For user-created REST servers, ALTER SERVER OPTIONS may not change
 *    rest_endpoint while dependent iceberg tables exist.
 *  - CREATE/ALTER USER MAPPING is rejected against the three built-in
 *    catalog servers.  The built-in pg_lake_rest_catalog reads
 *    credentials from GUCs only (and ignores both pg_user_mapping and
 *    $PGDATA/catalogs.conf), while the postgres / object_store built-ins
 *    have no notion of OAuth credentials at all.  User mappings on
 *    user-created REST servers are unaffected.
 *  - ALTER EXTENSION ... DROP SERVER/FOREIGN DATA WRAPPER is rejected
 *    for iceberg_catalog objects, preventing detachment of the
 *    DEPENDENCY_EXTENSION edge that shields them from standalone DROP.
 *  - ALTER FOREIGN DATA WRAPPER iceberg_catalog is rejected, preventing
 *    replacement of the validator or addition of a handler.
 *
 * The last two are defense-in-depth against superuser misuse: both
 * require superuser privilege, but a superuser detaching the
 * extension edge or swapping the validator would silently weaken the
 * protection model.
 */
bool
ValidateIcebergCatalogServerDDL(ProcessUtilityParams * processUtilityParams,
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

		RejectIfBuiltinCatalogServerName(stmt->servername);

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

		if (stmt->servertype == NULL ||
			pg_strcasecmp(stmt->servertype, REST_CATALOG_NAME) != 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("iceberg_catalog server requires TYPE 'rest'")));
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

		RejectIfBuiltinCatalogServerName(stmt->newname);

		/*
		 * Renaming an iceberg_catalog server is blocked because dependent
		 * iceberg tables store the server name as a string option
		 * (catalog='<name>') in pg_foreign_table.ftoptions.  A rename would
		 * silently break those references.
		 */
		ForeignServer *server = GetForeignServerByName(strVal(stmt->object),
													   true);

		if (server != NULL)
		{
			ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

			if (strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot rename iceberg_catalog server \"%s\"",
								strVal(stmt->object)),
						 errhint("Drop and recreate the server with the new name.")));
		}
	}
	else if (IsA(parsetree, AlterOwnerStmt))
	{
		AlterOwnerStmt *stmt = (AlterOwnerStmt *) parsetree;

		if (stmt->objectType != OBJECT_FOREIGN_SERVER)
			return false;

		const char *serverName = strVal(stmt->object);

		RejectIfModifyingBuiltinCatalogServer(serverName, "change owner of");
	}
	else if (IsA(parsetree, AlterForeignServerStmt))
	{
		AlterForeignServerStmt *stmt = (AlterForeignServerStmt *) parsetree;

		ForeignServer *server = GetForeignServerByName(stmt->servername, true);

		if (server == NULL)
			return false;

		ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

		if (strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0)
			return false;

		/*
		 * The three built-in servers are immutable structural anchors.
		 * Configuration for the built-in catalogs lives in GUCs; reject any
		 * option, version, or other ALTER change so we never end up with
		 * hidden server-side state that pg_dump cannot round-trip cleanly for
		 * an extension-owned object.
		 */
		RejectIfModifyingBuiltinCatalogServer(stmt->servername, "alter");

		/*
		 * Changing rest_endpoint on a user-created server with dependent
		 * iceberg tables (writable or read-only) would silently point them at
		 * a different REST catalog, breaking the metadata chain.
		 */
		ListCell   *lc;

		foreach(lc, stmt->options)
		{
			DefElem    *def = (DefElem *) lfirst(lc);

			if (pg_strcasecmp(def->defname, "rest_endpoint") == 0 &&
				ServerHasDependentRestIcebergTable(server->serverid))
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change \"rest_endpoint\" on server \"%s\" "
								"because it has dependent iceberg tables",
								stmt->servername),
						 errhint("Drop the dependent tables first, or create a "
								 "new server with the desired endpoint.")));
			}
		}
	}
	else if (IsA(parsetree, CreateUserMappingStmt))
	{
		CreateUserMappingStmt *stmt = (CreateUserMappingStmt *) parsetree;

		/*
		 * Name-only check: the three built-in long names are reserved for
		 * iceberg_catalog servers and can never be backed by any other FDW
		 * (the extension creates them and the CREATE SERVER guard above
		 * blocks anyone from grabbing the name).  No GetForeignServerByName
		 * lookup needed here.
		 */
		RejectIfModifyingBuiltinCatalogServer(stmt->servername,
											  "create user mapping for");
	}
	else if (IsA(parsetree, AlterUserMappingStmt))
	{
		AlterUserMappingStmt *stmt = (AlterUserMappingStmt *) parsetree;

		RejectIfModifyingBuiltinCatalogServer(stmt->servername,
											  "alter user mapping for");
	}
	else if (IsA(parsetree, AlterExtensionContentsStmt))
	{
		AlterExtensionContentsStmt *stmt =
			(AlterExtensionContentsStmt *) parsetree;

		/*
		 * Block ALTER EXTENSION pg_lake_iceberg DROP SERVER / DROP FOREIGN
		 * DATA WRAPPER for iceberg_catalog objects.  Without this, a
		 * superuser could detach the DEPENDENCY_EXTENSION edge and then DROP
		 * the object freely, breaking every table that depends on it.
		 *
		 * Only the DROP direction (action < 0) is dangerous; ADD is benign
		 * (it re-attaches membership, which is the state we want).
		 */
		if (stmt->action < 0)
		{
			if (stmt->objtype == OBJECT_FOREIGN_SERVER)
			{
				const char *serverName = strVal(stmt->object);

				if (IsBuiltinCatalogServerName(serverName))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot remove the built-in catalog "
									"server \"%s\" from its extension",
									serverName),
							 errhint("The built-in catalog servers are "
									 "structural anchors; detaching them "
									 "from the extension would allow DROP "
									 "and break every dependent table.")));
			}
			else if (stmt->objtype == OBJECT_FDW)
			{
				const char *fdwName = strVal(stmt->object);

				if (strcmp(fdwName, ICEBERG_CATALOG_FDW_NAME) == 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot remove the \"%s\" foreign data "
									"wrapper from its extension",
									ICEBERG_CATALOG_FDW_NAME),
							 errhint("The iceberg_catalog FDW is required by "
									 "all catalog servers; detaching it would "
									 "allow DROP and break the entire catalog "
									 "infrastructure.")));
			}
		}
	}
	else if (IsA(parsetree, AlterFdwStmt))
	{
		AlterFdwStmt *stmt = (AlterFdwStmt *) parsetree;

		/*
		 * Block ALTER FOREIGN DATA WRAPPER iceberg_catalog entirely.
		 * Replacing the validator would silently disable option checking on
		 * all future CREATE/ALTER SERVER and USER MAPPING statements; adding
		 * a handler would change the FDW semantics.  Both require superuser,
		 * but the downstream damage is hard to notice and impossible to undo
		 * without recreating every dependent server.
		 */
		if (strcmp(stmt->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot alter the \"%s\" foreign data wrapper",
							ICEBERG_CATALOG_FDW_NAME),
					 errhint("The iceberg_catalog FDW is managed by the "
							 "pg_lake_iceberg extension and must not be "
							 "modified directly.")));
	}

	return false;
}


/*
 * IsRedactableUserMappingSecret returns true for option names whose
 * values are secrets that must never appear in queryString-derived
 * surfaces (pg_stat_statements, log_min_duration_statement, ereport
 * error contexts).  Kept in lockstep with the descriptor table above:
 * any option carrying credential material should be added here as
 * well.
 */
static bool
IsRedactableUserMappingSecret(const char *name)
{
	return pg_strcasecmp(name, "client_id") == 0 ||
		pg_strcasecmp(name, "client_secret") == 0;
}


/*
 * IsIcebergCatalogServerByName returns true when `serverName` refers
 * to an iceberg_catalog server.  Both shapes count:
 *
 *   1. Any of the three built-in long names
 *      (pg_lake_postgres_catalog, ...).  These are reserved and only
 *      ever back the extension's catalog servers, so we can recognise
 *      them by name alone without a syscache lookup -- and we must,
 *      so that we still redact the queryString for the failing
 *      CREATE/ALTER USER MAPPING that ValidateIcebergCatalogServerDDL
 *      is about to reject.
 *   2. A user-created server whose FDW is iceberg_catalog.
 *
 * Never raises: a missing server returns false.  In that case the
 * surrounding CREATE/ALTER USER MAPPING will itself fail in core, and
 * we cannot know whether the user intended an iceberg_catalog target,
 * so we leave the queryString untouched.
 */
static bool
IsIcebergCatalogServerByName(const char *serverName)
{
	ForeignServer *server;
	ForeignDataWrapper *fdw;

	if (serverName == NULL)
		return false;

	if (IsBuiltinCatalogServerName(serverName))
		return true;

	server = GetForeignServerByName(serverName, /* missing_ok */ true);
	if (server == NULL)
		return false;

	fdw = GetForeignDataWrapper(server->fdwid);
	return strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0;
}


/*
 * RedactUserMappingSecrets overwrites secret option values in the
 * supplied queryString in place, replacing each character between the
 * opening and closing quote with '*'.  Callers must have already
 * confirmed that the target server is iceberg_catalog.
 *
 * In-place mutation is essential: pg_stat_statements and ereport
 * error contexts both retain the original queryString pointer.  A
 * copy via pstrdup would be invisible to them.  By overwriting the
 * backing buffer, every holder of the pointer sees the redacted
 * version.  DDL execution is unaffected because CREATE/ALTER USER
 * MAPPING reads option values from DefElem->arg, not from queryString.
 *
 * Each DefElem carries a parse `location` pointing at the start of
 * its option name in queryString (see gram.y: generic_option_elem
 * uses makeDefElem(name, value, @1)).  From there we walk forward to
 * the opening quote of the Sconst literal and then overwrite up to
 * (but not including) the matching closing quote.  Three string
 * shapes are recognised:
 *
 *   'foo''bar'   -- plain Sconst with '' as the only escape
 *   E'foo\nbar'  -- extended-quote string with backslash escapes
 *   U&'foo\0021' -- unicode string with backslash unicode escapes
 *
 * For E'' / U&'' strings we additionally consume `\X` as a two-byte
 * escape so that `\'` inside the literal does not look like a closing
 * quote and leak the suffix.  String continuation across whitespace +
 * newline (the SQL standard `'foo'\n'bar'` form) is rare in DDL
 * OPTIONS and is not handled; only the first segment is redacted in
 * that case.
 */
static void
RedactUserMappingSecrets(const char *queryString, List *options)
{
	ListCell   *lc;
	char	   *queryEnd;

	if (queryString == NULL || options == NIL)
		return;

	queryEnd = (char *) queryString + strlen(queryString);

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		char	   *cursor;
		bool		hasBackslashEscapes = false;
		bool		hasValidLocation;
		bool		isEscapePrefix;
		bool		isUnicodePrefix;

		if (def->arg == NULL)
			continue;			/* DROP options have no value to scrub */

		if (!IsRedactableUserMappingSecret(def->defname))
			continue;

		hasValidLocation = (def->location >= 0 &&
							(size_t) def->location < (size_t) (queryEnd - queryString));
		if (!hasValidLocation)
			continue;

		cursor = (char *) queryString + def->location;

		/*
		 * Scan past the option name to the opening quote of its Sconst value.
		 * Along the way, detect an E'' or U&'' prefix that enables backslash
		 * escapes inside the literal.
		 */
		while (cursor < queryEnd && *cursor != '\'')
		{
			isEscapePrefix = ((*cursor == 'E' || *cursor == 'e') &&
							  cursor + 1 < queryEnd && cursor[1] == '\'');
			isUnicodePrefix = ((*cursor == 'U' || *cursor == 'u') &&
							   cursor + 2 < queryEnd &&
							   cursor[1] == '&' && cursor[2] == '\'');

			if (isEscapePrefix)
			{
				hasBackslashEscapes = true;
				cursor++;		/* advance past 'E', loop exits on '\'' */
				break;
			}
			if (isUnicodePrefix)
			{
				hasBackslashEscapes = true;
				cursor += 2;	/* advance past 'U&', loop exits on '\'' */
				break;
			}
			cursor++;
		}

		if (cursor >= queryEnd || *cursor != '\'')
			continue;			/* no opening quote found */

		cursor++;				/* skip the opening quote itself */

		/*
		 * Overwrite every byte of the literal body with '*', respecting the
		 * quoting rules so we correctly find the real closing quote.
		 */
		while (cursor < queryEnd)
		{
			bool		isDoubledQuote = (*cursor == '\'' &&
										  cursor + 1 < queryEnd &&
										  cursor[1] == '\'');
			bool		isClosingQuote = (*cursor == '\'' && !isDoubledQuote);
			bool		isBackslashEscape = (hasBackslashEscapes &&
											 *cursor == '\\' &&
											 cursor + 1 < queryEnd);

			if (isClosingQuote)
				break;

			if (isDoubledQuote || isBackslashEscape)
			{
				*cursor++ = '*';
				*cursor++ = '*';
				continue;
			}

			*cursor++ = '*';
		}
	}
}


/*
 * RedactRestCatalogUserMappingSecrets is a ProcessUtility handler
 * that detects CREATE/ALTER USER MAPPING targeting an iceberg_catalog
 * server and scrubs secret option values (client_id, client_secret)
 * out of the queryString in place.  Returns false so that downstream
 * handlers (notably ValidateIcebergCatalogServerDDL) and the rest of
 * ProcessUtility continue to run unaffected.
 *
 * Registered AFTER ValidateIcebergCatalogServerDDL on purpose:
 * RegisterUtilityStatementHandler prepends to the handler list, so
 * the last registration is the head and runs first.  Redaction must
 * happen before the validator's ereport(ERROR), so that the failing
 * "cannot create user mapping for the built-in ..." path never
 * surfaces an unredacted queryString in its error context.
 */
bool
RedactRestCatalogUserMappingSecrets(ProcessUtilityParams * processUtilityParams,
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

	if (!IsIcebergCatalogServerByName(serverName))
		return false;

	RedactUserMappingSecrets(processUtilityParams->queryString, options);

	return false;
}


/*
 * LookupUserMappingOptions returns the option list for the user mapping
 * that applies to the current user on the given server, or NIL if none
 * exists.  Resolution mirrors core's GetUserMapping: a mapping for the
 * current user wins, otherwise PUBLIC (umuser = InvalidOid) is the
 * fallback.
 *
 * Unlike GetUserMapping, this never raises on a miss -- the caller is
 * expected to fall through to lower-priority credential sources
 * (catalogs.conf, GUCs).  The matched mapping's OID is returned via
 * *umidOut for use as part of the token cache key; *umidOut is left
 * InvalidOid when nothing matched.
 */
static List *
LookupUserMappingOptions(Oid serverOid, Oid *umidOut)
{
	HeapTuple	tp;
	Datum		datum;
	bool		isnull;
	List	   *options = NIL;

	*umidOut = InvalidOid;

	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
						 ObjectIdGetDatum(GetUserId()),
						 ObjectIdGetDatum(serverOid));
	if (!HeapTupleIsValid(tp))
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverOid));

	if (!HeapTupleIsValid(tp))
		return NIL;

	*umidOut = ((Form_pg_user_mapping) GETSTRUCT(tp))->oid;

	datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
							Anum_pg_user_mapping_umoptions, &isnull);
	if (!isnull)
		options = untransformRelOptions(datum);

	ReleaseSysCache(tp);
	return options;
}


/*
 * ReadCatalogsConfCredentials reads $PGDATA/catalogs.conf (or whatever
 * path the pg_lake_iceberg.catalogs_conf_credentials_path GUC points at) and
 * extracts credentials for the given server name.
 *
 * The file uses PostgreSQL's standard "key = value" config grammar
 * (ParseConfigFp), with dotted keys scoping each setting to a server:
 *
 *   horizon.client_id     = 'platform_id'
 *   horizon.client_secret = 'platform_secret'
 *
 * The file is re-read on every call -- catalog operations are
 * infrequent, and the simpler "no caching" path avoids needing
 * per-file mtime tracking or SIGHUP wiring.  *clientId, *clientSecret
 * are written only when a matching key is found; existing values are
 * left untouched.
 *
 * A missing file is treated as "no credentials, no error".  A
 * permission or I/O error is logged at WARNING and treated likewise so
 * one bad config file does not break the whole catalog path.
 *
 * Returns true iff at least one credential was extracted.
 */
static bool
ReadCatalogsConfCredentials(const char *serverName,
							char **clientId, char **clientSecret)
{
	char		path[MAXPGPATH];
	FILE	   *fp;
	ConfigVariable *head = NULL;
	ConfigVariable *tail = NULL;
	ConfigVariable *item;
	bool		found = false;
	size_t		serverNameLen;

	if (CatalogsConfCredentialsPath == NULL || CatalogsConfCredentialsPath[0] == '\0')
		return false;

	if (is_absolute_path(CatalogsConfCredentialsPath))
		strlcpy(path, CatalogsConfCredentialsPath, MAXPGPATH);
	else if (DataDir != NULL)
		snprintf(path, MAXPGPATH, "%s/%s", DataDir, CatalogsConfCredentialsPath);
	else
		strlcpy(path, CatalogsConfCredentialsPath, MAXPGPATH);

	fp = AllocateFile(path, "r");
	if (fp == NULL)
	{
		if (errno == ENOENT)
			return false;
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not open catalog config file \"%s\": %m",
						path)));
		return false;
	}

	(void) ParseConfigFp(fp, path, CONF_FILE_START_DEPTH, WARNING,
						 &head, &tail);
	FreeFile(fp);

	serverNameLen = strlen(serverName);

	for (item = head; item != NULL; item = item->next)
	{
		const char *key;

		if (item->errmsg != NULL)
			continue;

		/*
		 * Match entries of the form "<servername>.<key> = <value>".  The dot
		 * separates the server scope from the property name; require at least
		 * one character after the dot.
		 */
		if (strncmp(item->name, serverName, serverNameLen) != 0 ||
			item->name[serverNameLen] != '.' ||
			item->name[serverNameLen + 1] == '\0')
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
		/* unknown keys are silently ignored */
	}

	FreeConfigVariables(head);
	return found;
}


/*
 * ApplyCatalogsConfCredentials applies credentials read from
 * catalogs.conf onto opts.  Only the credentials actually present in
 * the file are set; unset entries leave whatever lower-priority layer
 * (GUCs for client_id/client_secret) populated.
 */
static void
ApplyCatalogsConfCredentials(RestCatalogOptions * opts, const char *serverName)
{
	char	   *clientId = NULL;
	char	   *clientSecret = NULL;

	if (!ReadCatalogsConfCredentials(serverName,
									 &clientId, &clientSecret))
		return;

	if (clientId != NULL)
		opts->clientId = clientId;
	if (clientSecret != NULL)
		opts->clientSecret = clientSecret;
}


/*
 * ApplyUserMappingOverrides overlays credentials from pg_user_mapping
 * onto opts and records the matched mapping's OID in opts->userMappingOid.  Has
 * no effect if no user mapping applies; the caller may then fall back
 * to catalogs.conf-derived or GUC-derived values.
 *
 * Only options carrying CATALOG_OPT_CTX_USER_MAPPING are applied here.
 * The validator already rejects anything else at DDL time, so the
 * defensive contexts check is just belt-and-suspenders against options
 * that might have slipped in before this code path existed.
 */
static void
ApplyUserMappingOverrides(RestCatalogOptions * opts, ForeignServer *server)
{
	List	   *options;
	ListCell   *lc;
	Oid			userMappingOid;

	options = LookupUserMappingOptions(server->serverid, &userMappingOid);
	if (options == NIL)
		return;

	opts->userMappingOid = userMappingOid;

	foreach(lc, options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const		IcebergCatalogOptionDesc *desc = FindCatalogOptionDesc(def->defname);

		if (desc == NULL || !(desc->contexts & CATALOG_OPT_CTX_USER_MAPPING))
			continue;

		ApplyCatalogOptionValue(opts, desc, def);
	}
}


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
 * ApplyServerOptionOverrides overrides the GUC-derived defaults in opts
 * with any options explicitly set on the foreign server.
 */
static void
ApplyServerOptionOverrides(RestCatalogOptions * opts, ForeignServer *server)
{
	ListCell   *lc;

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const		IcebergCatalogOptionDesc *desc = FindCatalogOptionDesc(def->defname);

		if (desc != NULL)
			ApplyCatalogOptionValue(opts, desc, def);
	}
}


/*
 * ValidateRestCatalogOptions checks that the resolved options carry
 * the minimum fields needed to talk to a REST catalog: the endpoint
 * and the credentials required by the configured auth flow.  Running
 * this at resolution time -- after GUCs, catalogs.conf, and the user
 * mapping have all been folded in -- means an incompletely-configured
 * catalog fails up front on first DML, instead of silently issuing an
 * unauthenticated request to the OAuth endpoint.
 *
 * Credential requirements are auth-type specific:
 *   OAuth2:  client_id AND client_secret (Basic auth header).
 *   Horizon: client_secret only (carried in the form body;
 *            client_id is intentionally ignored).
 *
 * FetchRestCatalogAccessToken still re-checks the fields it actually
 * dereferences.  Those late checks are defense in depth in case any
 * future code path constructs RestCatalogOptions without going
 * through this resolver.
 */
static void
ValidateRestCatalogOptions(const RestCatalogOptions * opts, const char *catalog)
{
	bool		missingSecret;
	bool		missingId;

	if (opts->host == NULL || opts->host[0] == '\0')
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("\"rest_endpoint\" is not configured for REST catalog \"%s\"",
						catalog),
				 errhint("Set the pg_lake_iceberg.rest_catalog_host GUC or "
						 "the \"rest_endpoint\" option on the server.")));

	missingSecret = (opts->clientSecret == NULL || opts->clientSecret[0] == '\0');
	missingId = (opts->authType != REST_CATALOG_AUTH_TYPE_HORIZON) &&
		(opts->clientId == NULL || opts->clientId[0] == '\0');

	if (missingSecret || missingId)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				 errmsg("no credentials found for REST catalog \"%s\"",
						catalog),
				 errhint("Provide client_id and client_secret via a "
						 "USER MAPPING, $PGDATA/catalogs.conf, or the "
						 "pg_lake_iceberg.rest_catalog_client_id / "
						 "pg_lake_iceberg.rest_catalog_client_secret GUCs.")));
}


/*
 * Build RestCatalogOptions for an iceberg_catalog server.
 *
 * Resolution proceeds from lowest to highest priority:
 *
 *   1. GUC defaults                       (always)
 *   2. Server options                     (always; no-op for built-ins
 *                                          since ALTER SERVER is blocked)
 *   3. $PGDATA/catalogs.conf credentials  (user-created servers only)
 *   4. pg_user_mapping options            (user-created servers only)
 *
 * The built-in pg_lake_rest_catalog deliberately stops after step 2.
 * Its credentials live exclusively in pg_lake_iceberg.rest_catalog_*
 * GUCs so the built-in catalog stays a single, global, instance-wide
 * configuration with no hidden per-user view.  postgres / object_store
 * never reach this function; their resolution stays in their own
 * modules.
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

	Assert(strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) == 0);

	RestCatalogOptions *opts = palloc0(sizeof(RestCatalogOptions));

	opts->serverOid = server->serverid;
	opts->userMappingOid = InvalidOid;
	opts->catalog = pstrdup(userVisibleCatalog);
	ApplyGUCDefaults(opts);
	ApplyServerOptionOverrides(opts, server);

	if (!IsBuiltinCatalogServerName(serverName))
	{
		ApplyCatalogsConfCredentials(opts, serverName);
		ApplyUserMappingOverrides(opts, server);
	}

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

	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_POST, postUrl, body->data,
													  headers);

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
	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL,
													  GetHeadersWithAuth(opts));

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
	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL,
													  GetHeadersWithAuth(opts));

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
	HttpResult	hr = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL, headers);

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

	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_POST, postUrl, body.data,
													  PostHeadersWithAuth(opts));

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
	if (RestCatalogTokenCache == NULL)
		return;

	MemoryContextReset(RestTokenCacheCtx);
	RestCatalogTokenCache = NULL;
}


/*
 * Initialize the per-catalog token cache hash table if needed.
 *
 * TokenCacheCallbackRegistered is separate from RestCatalogTokenCache because
 * the callback must be registered exactly once per backend lifetime
 * (CacheRegisterSyscacheCallback appends to a fixed-size array), while
 * RestCatalogTokenCache is reset to NULL on every invalidation.
 */
static bool TokenCacheCallbackRegistered = false;

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
 * Gets an access token from rest catalog.  Caches the token per
 * (server, user-mapping) pair so that different SET ROLEs in the same
 * backend each see the credentials of their own user mapping (or
 * PUBLIC), while still letting the built-in pg_lake_rest_catalog share
 * a single (server, InvalidOid) slot across all sessions and roles.
 */
static char *
GetRestCatalogAccessToken(RestCatalogOptions * opts, bool forceRefreshToken)
{
	RestCatalogTokenCacheKey key;
	RestCatalogTokenCacheEntry *entry;
	bool		found = false;

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

	InitTokenCacheIfNeeded();

	memset(&key, 0, sizeof(key));	/* zero out any compiler padding so
									 * HASH_BLOBS keys compare cleanly */
	key.serverOid = opts->serverOid;
	key.userMappingOid = opts->userMappingOid;

	entry = hash_search(RestCatalogTokenCache, &key, HASH_ENTER, &found);

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
				 errhint("Set client_secret via a USER MAPPING, "
						 "$PGDATA/catalogs.conf, or the "
						 "pg_lake_iceberg.rest_catalog_client_secret GUC.")));

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
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					 errmsg("REST catalog client_id is not configured"),
					 errhint("Set client_id via a USER MAPPING, "
							 "$PGDATA/catalogs.conf, or the "
							 "pg_lake_iceberg.rest_catalog_client_id GUC.")));

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
 * Writable tables always use the current database name so that a
 * subsequent ALTER SERVER ? ADD/SET catalog_name cannot silently
 * re-route an existing table to a different REST namespace.
 *
 * Read-only tables always have catalog_name baked into their table
 * options at CREATE TABLE time (inherited from the server option or
 * defaulted to the database name).
 */
char *
GetRestCatalogName(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	if (catalogType == REST_CATALOG_READ_WRITE)
		return get_database_name(MyDatabaseId);

	ForeignTable *foreignTable = GetForeignTable(relationId);
	char	   *catalogName = GetStringOption(foreignTable->options, "catalog_name", false);

	if (catalogName != NULL)
		return catalogName;

	elog(ERROR, "catalog_name missing on read-only REST catalog table %u", relationId);
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
	appendStringInfoString(body, ",\"manifest-list\":");
	appendStringInfoString(body, EscapeJson(newSnapshot->manifest_list));
	appendStringInfoString(body, ",\"summary\":{\"operation\": \"append\"}");
	appendStringInfo(body, ",\"schema-id\":%d", newSnapshot->schema_id);
	appendStringInfoString(body, "}}, ");	/* end add-snapshot */

	appendStringInfo(body, "{\"action\":\"set-snapshot-ref\", \"type\":\"branch\", \"ref-name\":\"main\", \"snapshot-id\":%" PRId64 "}", newSnapshot->snapshot_id);

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
