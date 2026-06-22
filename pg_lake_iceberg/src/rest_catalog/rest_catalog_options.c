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
 * iceberg_catalog FDW option schema and validator.
 *
 * Single source of truth for the option set that CREATE/ALTER SERVER
 * and CREATE/ALTER USER MAPPING accept on iceberg_catalog servers:
 * validation, the user-facing "Valid options are: ..." hint, and the
 * option-to-struct applier all derive from iceberg_catalog_option_descs[].
 *
 * Each descriptor carries a CATALOG_OPT_CTX_* bitmask saying which
 * catalog object the option is legal on (SERVER, USER MAPPING, or
 * both).  The resolver in rest_catalog.c walks the same table via
 * ApplyServerOptionOverrides and ApplyUserMappingOverrides to layer
 * server options and user-mapping options on top of the GUC defaults
 * during RestCatalogOptions construction.
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "nodes/parsenodes.h"
#include "utils/syscache.h"

#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/string_utils.h"


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
 * a user mapping; they may also be supplied via GUCs.  Each option's
 * descriptor carries a CATALOG_OPT_CTX_* bitmask saying where it is
 * legal.
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
 * ApplyServerOptionOverrides overrides the GUC-derived defaults in opts
 * with any options explicitly set on the foreign server.  Only options
 * carrying CATALOG_OPT_CTX_SERVER are applied here; the validator
 * already rejects anything else at DDL time.
 */
void
ApplyServerOptionOverrides(RestCatalogOptions * opts, ForeignServer *server)
{
	ListCell   *lc;

	foreach(lc, server->options)
	{
		DefElem    *def = (DefElem *) lfirst(lc);
		const		IcebergCatalogOptionDesc *desc = FindCatalogOptionDesc(def->defname);

		if (desc == NULL || !(desc->contexts & CATALOG_OPT_CTX_SERVER))
			continue;

		ApplyCatalogOptionValue(opts, desc, def);
	}
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
 * (GUCs).  The matched mapping's OID is returned via *umidOut for use
 * as part of the token cache key; *umidOut is left InvalidOid when
 * nothing matched.
 */
static List *
LookupUserMappingOptions(Oid serverOid, Oid *umidOut)
{
	*umidOut = InvalidOid;

	HeapTuple	tp = SearchSysCache2(USERMAPPINGUSERSERVER,
									 ObjectIdGetDatum(GetUserId()),
									 ObjectIdGetDatum(serverOid));

	if (!HeapTupleIsValid(tp))
		tp = SearchSysCache2(USERMAPPINGUSERSERVER,
							 ObjectIdGetDatum(InvalidOid),
							 ObjectIdGetDatum(serverOid));

	if (!HeapTupleIsValid(tp))
		return NIL;

	*umidOut = ((Form_pg_user_mapping) GETSTRUCT(tp))->oid;

	bool		isnull;
	Datum		datum = SysCacheGetAttr(USERMAPPINGUSERSERVER, tp,
										Anum_pg_user_mapping_umoptions, &isnull);
	List	   *options = NIL;

	if (!isnull)
		options = untransformRelOptions(datum);

	ReleaseSysCache(tp);
	return options;
}


/*
 * LookupUserMappingOptionsByOid returns the option list for the user
 * mapping with the given OID and reports its server in *serverOidOut.
 * Returns NIL with *serverOidOut = InvalidOid if the user mapping
 * cannot be found.  When the mapping exists, *serverOidOut is its
 * server's OID and the return is its option list (NIL if empty).
 *
 * By-OID counterpart to LookupUserMappingOptions (which resolves via
 * GetUserId() with a PUBLIC fallback), used by the OAT_DROP capture
 * path that targets a known mapping directly.
 */
List *
LookupUserMappingOptionsByOid(Oid umOid, Oid *serverOidOut)
{
	*serverOidOut = InvalidOid;

	HeapTuple	tp = SearchSysCache1(USERMAPPINGOID, ObjectIdGetDatum(umOid));

	if (!HeapTupleIsValid(tp))
		return NIL;

	*serverOidOut = ((Form_pg_user_mapping) GETSTRUCT(tp))->umserver;

	bool		isnull;
	Datum		datum = SysCacheGetAttr(USERMAPPINGOID, tp,
										Anum_pg_user_mapping_umoptions, &isnull);
	List	   *options = NIL;

	if (!isnull)
		options = untransformRelOptions(datum);

	ReleaseSysCache(tp);
	return options;
}


/*
 * ApplyUserMappingOptionsList overlays an already-fetched user-mapping
 * option list onto opts and records umOid in opts->userMappingOid.
 * Shared by ApplyUserMappingOverrides (per-current-user) and the
 * by-OID path used by BuildRestCatalogOptionsFromUserMapping.
 *
 * Only options carrying CATALOG_OPT_CTX_USER_MAPPING are applied; the
 * contexts check is defensive (the validator already rejects other
 * options at DDL time).
 */
void
ApplyUserMappingOptionsList(RestCatalogOptions * opts, List *options, Oid umOid)
{
	opts->userMappingOid = umOid;

	ListCell   *lc;

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
 * ApplyUserMappingOverrides overlays credentials from pg_user_mapping
 * onto opts and records the matched mapping's OID in
 * opts->userMappingOid.  Has no effect if no user mapping applies; the
 * caller may then fall back to GUC-derived values.
 */
void
ApplyUserMappingOverrides(RestCatalogOptions * opts, ForeignServer *server)
{
	Oid			userMappingOid;
	List	   *options = LookupUserMappingOptions(server->serverid,
												   &userMappingOid);

	if (options == NIL)
		return;

	ApplyUserMappingOptionsList(opts, options, userMappingOid);
}
