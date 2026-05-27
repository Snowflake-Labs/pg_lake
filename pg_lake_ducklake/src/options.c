/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * FDW option validator for pg_lake_ducklake foreign tables.
 *
 * Originally lived alongside the iceberg/lake validators in
 * pg_lake_table/src/fdw/option.c. Moved here so DuckLake-specific
 * option handling stays self-contained inside pg_lake_ducklake.so.
 * The SQL definition in pg_lake_ducklake--3.4.sql now points at
 * '$libdir/pg_lake_ducklake' instead of pg_lake_table.
 */
#include "postgres.h"

#include <inttypes.h>

#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "utils/builtins.h"
#include "utils/varlena.h"
#include "fmgr.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/permissions/roles.h"

typedef struct PgLakeDucklakeOption
{
	const char *keyword;
	Oid			optcontext;
}			PgLakeDucklakeOption;

static PgLakeDucklakeOption * pg_lake_ducklake_options;

static void
InitPgLakeDucklakeOptions(void)
{
	PgLakeDucklakeOption *popt;

	static const PgLakeDucklakeOption fdw_options[] = {
		{"location", ForeignTableRelationId},
		{"autovacuum_enabled", ForeignTableRelationId},
		{"partition_by", ForeignTableRelationId},
		{"writable", ForeignTableRelationId},
		{"format", ForeignTableRelationId},

		{NULL, InvalidOid}
	};

	if (pg_lake_ducklake_options)
		return;

	pg_lake_ducklake_options = (PgLakeDucklakeOption *) malloc(sizeof(fdw_options));
	if (pg_lake_ducklake_options == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	popt = pg_lake_ducklake_options;

	memcpy(popt, fdw_options, sizeof(fdw_options));
}


static bool
is_valid_option_pg_lake_ducklake(const char *keyword, Oid context)
{
	PgLakeDucklakeOption *opt;

	Assert(pg_lake_ducklake_options);

	for (opt = pg_lake_ducklake_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, keyword) == 0)
			return true;
	}

	return false;
}


PG_FUNCTION_INFO_V1(pg_lake_ducklake_validator);

Datum
pg_lake_ducklake_validator(PG_FUNCTION_ARGS)
{
	CheckURLWriteAccess();

	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);

	bool		locationProvided = false;

	InitPgLakeDucklakeOptions();

	ListCell   *cell;

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option_pg_lake_ducklake(def->defname, catalog))
		{
			PgLakeDucklakeOption *opt;
			const char *closest_match;
			ClosestMatchState match_state;
			bool		has_valid_options = false;

			initClosestMatch(&match_state, def->defname, 4);
			for (opt = pg_lake_ducklake_options; opt->keyword; opt++)
			{
				if (catalog == opt->optcontext)
				{
					has_valid_options = true;
					updateClosestMatch(&match_state, opt->keyword);
				}
			}

			closest_match = getClosestMatch(&match_state);
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					 errmsg("invalid option \"%s\"", def->defname),
					 has_valid_options ? closest_match ?
					 errhint("Perhaps you meant the option \"%s\".",
							 closest_match) : 0 :
					 errhint("There are no valid options in this context.")));
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "location") == 0)
		{
			char	   *location = defGetString(def);

			if (!IsSupportedURL(location))
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("pg_lake_ducklake: only s3:// and gs:// "
									   "URLs are currently supported for the \"location\" "
									   "option.")));

			char	   *charPointer = strchr(location, '?');

			if (charPointer != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("s3 configuration parameters are not allowed in the \"location\" "
								"option for pg_lake_ducklake tables")));

			locationProvided = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "autovacuum_enabled") == 0)
		{
			(void) defGetBoolean(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "partition_by") == 0)
		{
			/* validated downstream */
		}
	}

	if (!locationProvided && catalog == ForeignTableRelationId)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("\"location\" option is required for pg_lake_ducklake tables")));

	PG_RETURN_VOID();
}
