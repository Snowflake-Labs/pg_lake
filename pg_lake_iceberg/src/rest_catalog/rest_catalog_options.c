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
 * accepts on iceberg_catalog servers: validation, the user-facing
 * "Valid options are: ..." hint, and the option-to-struct applier
 * all derive from iceberg_catalog_option_descs[].
 *
 * The resolver in rest_catalog.c walks the same descriptor table via
 * ApplyServerOptionOverrides to layer server options on top of the
 * GUC defaults during RestCatalogOptions construction.
 */

#include "postgres.h"

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "commands/defrem.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"

#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/string_utils.h"


PG_FUNCTION_INFO_V1(iceberg_catalog_validator);


/*
 * Descriptor for a single iceberg_catalog server option.  This is the
 * single source of truth: validation, the user-facing hint, and the
 * option-to-struct applier all derive from this table.
 */
typedef enum IcebergCatalogOptionType
{
	CATALOG_OPT_STRING,
	CATALOG_OPT_BOOL,
	CATALOG_OPT_AUTH_TYPE,
	CATALOG_OPT_LOCATION_PREFIX
}			IcebergCatalogOptionType;

/* Validation flags checked at CREATE/ALTER SERVER time. */
#define CATALOG_OPT_NONEMPTY    0x01	/* reject empty string */
#define CATALOG_OPT_HAS_SCHEME  0x02	/* must contain "://" */

typedef struct IcebergCatalogOptionDesc
{
	const char *name;
	IcebergCatalogOptionType type;
	size_t		offset;			/* offsetof into RestCatalogOptions */
	int			flags;			/* CATALOG_OPT_NONEMPTY |
								 * CATALOG_OPT_HAS_SCHEME */
}			IcebergCatalogOptionDesc;

static const IcebergCatalogOptionDesc iceberg_catalog_option_descs[] = {
	{"rest_endpoint", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, host),
	CATALOG_OPT_NONEMPTY | CATALOG_OPT_HAS_SCHEME},
	{"rest_auth_type", CATALOG_OPT_AUTH_TYPE, offsetof(RestCatalogOptions, authType), 0},
	{"oauth_endpoint", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, oauthHostPath),
	CATALOG_OPT_NONEMPTY | CATALOG_OPT_HAS_SCHEME},
	{"scope", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, scope),
	CATALOG_OPT_NONEMPTY},
	{"enable_vended_credentials", CATALOG_OPT_BOOL, offsetof(RestCatalogOptions, enableVendedCredentials), 0},
	{"location_prefix", CATALOG_OPT_LOCATION_PREFIX, offsetof(RestCatalogOptions, locationPrefix),
	CATALOG_OPT_NONEMPTY | CATALOG_OPT_HAS_SCHEME},
	{"catalog_name", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, catalogName),
	CATALOG_OPT_NONEMPTY},
	{"client_id", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, clientId),
	CATALOG_OPT_NONEMPTY},
	{"client_secret", CATALOG_OPT_STRING, offsetof(RestCatalogOptions, clientSecret),
	CATALOG_OPT_NONEMPTY},
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
 * Build the "Valid options are: ?" hint string.
 */
static const char *
GetValidCatalogOptionsHint(void)
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfoString(&buf, "Valid options are: ");
	for (int i = 0; i < NUM_CATALOG_OPTIONS; i++)
	{
		if (i > 0)
			appendStringInfoString(&buf, ", ");
		appendStringInfoString(&buf, iceberg_catalog_option_descs[i].name);
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
 * Only server-level options are supported.
 */
Datum
iceberg_catalog_validator(PG_FUNCTION_ARGS)
{
	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalogRelId = PG_GETARG_OID(1);
	ListCell   *cell;

	/*
	 * PostgreSQL calls the validator for CREATE FOREIGN DATA WRAPPER itself
	 * (with ForeignDataWrapperRelationId), not just for CREATE SERVER.  Allow
	 * empty option lists for non-server contexts so extension creation
	 * succeeds, but still reject if someone passes options where they don't
	 * belong.
	 */
	if (catalogRelId != ForeignServerRelationId)
	{
		if (list_length(options_list) > 0)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("iceberg_catalog options are only valid for SERVER objects")));
		PG_RETURN_VOID();
	}

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);
		const		IcebergCatalogOptionDesc *desc = FindCatalogOptionDesc(def->defname);

		if (desc == NULL)
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\" for iceberg_catalog server",
							def->defname),
					 errhint("%s", GetValidCatalogOptionsHint())));

		ValidateCatalogOptionValue(desc, def);
	}

	PG_RETURN_VOID();
}


/*
 * ApplyServerOptionOverrides overrides the GUC-derived defaults in opts
 * with any options explicitly set on the foreign server.
 */
void
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
