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

/*-------------------------------------------------------------------------
 *
 * option.c
 *		  FDW and GUC option handling for pg_lake_table
 *
 * Portions Copyright (c) 2012-2023, PostgreSQL Global Development Group
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <inttypes.h>

#include "access/reloptions.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "commands/defrem.h"
#include "commands/extension.h"
#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/util/string_utils.h"
#include "libpq/libpq-be.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/varlena.h"

#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/util/rel_utils.h"


/*
 * Describes the valid options for objects that this wrapper uses.
 */
typedef struct PgLakeOption
{
	const char *keyword;
	Oid			optcontext;		/* OID of catalog in which option may appear */
}			PgLakeOption;


/*
 * Valid options for lake_table.
 * Allocated and filled in InitPgLakeOptions.
 */
static PgLakeOption * pg_lake_options;
static PgLakeOption * pg_lake_iceberg_options;

/*
 * Helper functions
 */
static void InitPgLakeOptions(void);
static void InitPgLakeIcebergOptions(void);
static bool is_valid_option_pg_lake(const char *keyword, Oid context);
static bool is_valid_option_pg_lake_iceberg(const char *keyword, Oid context);

#include "miscadmin.h"

/*
 * Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * USER MAPPING or FOREIGN TABLE that uses lake_table.
 *
 * Raise an ERROR if the option or its value is considered invalid.
 */
PG_FUNCTION_INFO_V1(pg_lake_table_validator);
PG_FUNCTION_INFO_V1(pg_lake_iceberg_validator);

Datum
pg_lake_table_validator(PG_FUNCTION_ARGS)
{
	/*
	 * Check that the user has the necessary permissions. All pg_lake tables
	 * are readable. Below, we also check for write permissions.
	 */
	CheckURLReadAccess();

	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);
	ListCell   *cell;

	char	   *path = NULL;

	bool		foundServerPath = false;
	bool		foundLocation = false;
	bool		isWritable = false;
	bool		foundFilename = false;
	bool		foundMaximumObjectSize = false;

	bool		csvOptionProvided = false;
	char	   *csvDelim = NULL;
	char	   *csvQuote = NULL;
	char	   *csvEscape = NULL;
	char	   *csvNull = NULL;

	bool		foundZipPath = false;
	bool		foundLayer = false;

	char	   *logFormat = NULL;

	CopyDataFormat copyDataFormat = DATA_FORMAT_INVALID;
	CopyDataCompression copyDataCompression = DATA_COMPRESSION_INVALID;

	/* Build our options lists if we didn't yet. */
	InitPgLakeOptions();

	/*
	 * Check that only options supported by pg_lake_tables, and allowed for
	 * the current object type, are given.
	 */
	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option_pg_lake(def->defname, catalog))
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with a valid option that looks similar, if there is one.
			 */
			PgLakeOption *opt;
			const char *closest_match;
			ClosestMatchState match_state;
			bool		has_valid_options = false;

			initClosestMatch(&match_state, def->defname, 4);
			for (opt = pg_lake_options; opt->keyword; opt++)
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

		/*
		 * Validate option value, when we can do so without any context.
		 */
		if (strcmp(def->defname, "fdw_startup_cost") == 0 ||
			strcmp(def->defname, "fdw_tuple_cost") == 0)
		{
			/*
			 * These must have a floating point value greater than or equal to
			 * zero.
			 */
			char	   *value;
			double		real_val;
			bool		is_parsed;

			value = defGetString(def);
			is_parsed = parse_real(value, &real_val, 0, NULL);

			if (!is_parsed)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid value for floating point option \"%s\": %s",
								def->defname, value)));

			if (real_val < 0)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"%s\" must be a floating point value greater than or equal to zero",
								def->defname)));
		}
		else if (strcmp(def->defname, "writable") == 0)
		{
			isWritable = defGetBoolean(def);
			if (isWritable)
			{
				CheckURLWriteAccess();
			}
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "path") == 0)
		{
			path = defGetString(def);

			if (!IsSupportedURL(path))
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("pg_lake_table: only s3:// and gs:// "
									   "URLs are currently supported for the \"path\" "
									   "option.")));

			foundServerPath = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "location") == 0)
		{
			char	   *value = defGetString(def);

			if (!IsSupportedURL(value))
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("pg_lake_table: only s3:// and gs:// "
									   "URLs are currently supported for the \"location\" "
									   "option.")));

			foundLocation = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "format") == 0)
		{
			char	   *format = defGetString(def);

			copyDataFormat = NameToCopyDataFormat(format);
			if (copyDataFormat == DATA_FORMAT_INVALID)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("pg_lake_table: only csv, json, gdal, and parquet "
									   "formats are currently supported for the \"format\" option.")));
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "header") == 0)
		{
			/* only accept boolean */
			(void) defGetBoolean(def);
			csvOptionProvided = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "delimiter") == 0)
		{
			/* only accept single char */
			csvDelim = defGetString(def);

			if (strlen(csvDelim) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("delimiter must be a single one-byte character")));

			/* Disallow end-of-line characters */
			if (strchr(csvDelim, '\r') != NULL ||
				strchr(csvDelim, '\n') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("delimiter cannot be newline or carriage return")));

			csvOptionProvided = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "quote") == 0)
		{
			/* only accept single char */
			csvQuote = defGetString(def);

			if (strlen(csvQuote) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("quote must be a single one-byte character")));

			csvOptionProvided = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "escape") == 0)
		{
			/* only accept single char */
			csvEscape = defGetString(def);

			if (strlen(csvEscape) != 1)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("escape must be a single one-byte character")));

			csvOptionProvided = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "null") == 0)
		{
			csvNull = defGetString(def);

			/* Disallow end-of-line characters */
			if (strchr(csvNull, '\r') != NULL ||
				strchr(csvNull, '\n') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("null cannot be newline or carriage return")));


			csvOptionProvided = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "zip_path") == 0)
		{
			foundZipPath = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "layer") == 0)
		{
			foundLayer = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "log_format") == 0)
		{
			logFormat = defGetString(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "filename") == 0)
		{
			foundFilename = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "maximum_object_size") == 0)
		{
			if (IsA(def->arg, Integer))
			{
				if (intVal(def->arg) < 0)
					ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
									errmsg("option value \"maximum_object_size\" must be non-negative")));
			}
			else
				ereport(ERROR, (errcode(ERRCODE_WRONG_OBJECT_TYPE),
								errmsg("option \"maximum_object_size\" must have integer value")));

			foundMaximumObjectSize = true;
		}
	}

	if (catalog != ForeignTableRelationId)
	{
		/* we don't really deal in server options */
		PG_RETURN_VOID();
	}

	if (isWritable)
	{
		if (foundServerPath)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("\"path\" option is not supported for writable pg_lake tables")));
		if (!foundLocation)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("\"location\" option is required for writable pg_lake tables")));
		if (foundFilename)
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("\"filename\" option is not allowed for writable pg_lake tables")));

		if (copyDataFormat == DATA_FORMAT_INVALID)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("\"format\" option is required for writable "
								   "pg_lake tables"),
							errdetail("Supported formats are csv, json, gdal, and parquet.")));
		}
		else if (copyDataFormat != DATA_FORMAT_PARQUET &&
				 copyDataFormat != DATA_FORMAT_CSV &&
				 copyDataFormat != DATA_FORMAT_JSON)
		{
			ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
							errmsg("%s format is not supported for writable "
								   "pg_lake tables",
								   CopyDataFormatToName(copyDataFormat)),
							errdetail("Supported formats are csv, json, and parquet.")));
		}
	}
	else if (!foundServerPath)
	{
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("\"path\" option is required for regular pg_lake tables")));
	}

	if (copyDataFormat == DATA_FORMAT_INVALID)
	{
		copyDataFormat = URLToCopyDataFormat(path);

		if (copyDataFormat == DATA_FORMAT_INVALID)
		{
			ereport(ERROR,
					(errcode(ERRCODE_SYNTAX_ERROR),
					 errmsg("\"format\" option is required for pg_lake "
							"tables when not using a common extension"),
					 errdetail("Supported formats are csv, json, gdal, and parquet.")));
		}
	}

	/* additional validation on format and compression */
	FindDataFormatAndCompression(PG_LAKE_TABLE_TYPE, path, options_list,
								 &copyDataFormat, &copyDataCompression);

	if (copyDataFormat != DATA_FORMAT_CSV && csvOptionProvided)
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("\"header\", \"delimiter\", \"quote\", \"escape\" and \"null\" options "
						"are only supported for csv format tables")));

	if (copyDataFormat == DATA_FORMAT_CSV)
	{
		/* Don't allow the delimiter to appear in the null string. */
		if (csvNull && csvDelim && (strchr(csvNull, csvDelim[0]) != NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("CSV delimiter character must not appear in the NULL specification")));

		/* Don't allow the CSV quote char to appear in the null string. */
		if (csvNull && csvQuote && (strchr(csvNull, csvQuote[0]) != NULL))
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("CSV quote character must not appear in the NULL specification")));

		if (csvDelim && csvQuote && csvDelim[0] == csvQuote[0])
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("CSV delimiter and quote must be different")));

	}

	if (foundZipPath &&
		(copyDataFormat != DATA_FORMAT_GDAL || copyDataCompression != DATA_COMPRESSION_ZIP))
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("\"zip_path\" option is only supported for GDAL "
							   "format with zip compression")));
	}

	if (foundLayer && copyDataFormat != DATA_FORMAT_GDAL)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("\"layer\" option is only supported for GDAL "
							   "format")));
	}


	if (foundMaximumObjectSize && copyDataFormat != DATA_FORMAT_JSON)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("\"maximum_object_size\" option is only supported for JSON "
							   "format")));
	}

	if (logFormat != NULL && copyDataFormat != DATA_FORMAT_LOG)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("\"log_format\" option is only supported for log "
							   "format")));
	}
	else if (logFormat == NULL && copyDataFormat == DATA_FORMAT_LOG)
	{
		ereport(ERROR, (errcode(ERRCODE_SYNTAX_ERROR),
						errmsg("\"log_format\" option is required for log "
							   "format")));
	}

	PG_RETURN_VOID();
}


/*
 * Initialize option lists.
 */
static void
InitPgLakeOptions(void)
{
	PgLakeOption *popt;

	/* FDW-specific FDW options */
	static const PgLakeOption fdw_options[] = {
		{"path", ForeignTableRelationId},

		/* parquet, csv or json */
		{"format", ForeignTableRelationId},

		/* none, zstd, snappy, gzip */
		{"compression", ForeignTableRelationId},

		/* csv options */
		{"header", ForeignTableRelationId},
		{"delimiter", ForeignTableRelationId},
		{"quote", ForeignTableRelationId},
		{"escape", ForeignTableRelationId},
		{"null", ForeignTableRelationId},

		/* whether the table is writable */
		{"writable", ForeignTableRelationId},
		{"location", ForeignTableRelationId},

		/* whether to include the filename */
		{"filename", ForeignTableRelationId},

		/* GDAL options */
		{"zip_path", ForeignTableRelationId},
		{"layer", ForeignTableRelationId},

		/* JSON options */
		{"maximum_object_size", ForeignTableRelationId},

		/* log options */
		{"log_format", ForeignTableRelationId},

		{NULL, InvalidOid}
	};

	/* Prevent redundant initialization. */
	if (pg_lake_options)
		return;

	/*
	 * Construct an array which consists of all valid options for lake_table.
	 *
	 * We use plain malloc here to allocate pg_lake_options because it lives
	 * as long as the backend process does.
	 */
	pg_lake_options = (PgLakeOption *) malloc(sizeof(fdw_options));
	if (pg_lake_options == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	popt = pg_lake_options;

	/* Append FDW-specific options and dummy terminator. */
	memcpy(popt, fdw_options, sizeof(fdw_options));
}

/*
 * Initialize option lists.
 */
static void
InitPgLakeIcebergOptions(void)
{
	PgLakeOption *popt;

	/* FDW-specific FDW options */
	static const PgLakeOption fdw_options[] = {
		{"location", ForeignTableRelationId},

		{"autovacuum_enabled", ForeignTableRelationId},
		{"column_stats_mode", ForeignTableRelationId},
		{"row_ids", ForeignTableRelationId},
		{"partition_by", ForeignTableRelationId},
		{"catalog", ForeignTableRelationId},
		{"read_only", ForeignTableRelationId},

		{"catalog_name", ForeignTableRelationId},
		{"catalog_table_name", ForeignTableRelationId},
		{"catalog_namespace", ForeignTableRelationId},

		{NULL, InvalidOid}
	};

	/* Prevent redundant initialization. */
	if (pg_lake_iceberg_options)
		return;

	/*
	 * Construct an array which consists of all valid options for
	 * pg_lake_iceberg.
	 *
	 * We use plain malloc here to allocate pg_lake_iceberg because it lives
	 * as long as the backend process does.
	 */
	pg_lake_iceberg_options = (PgLakeOption *) malloc(sizeof(fdw_options));
	if (pg_lake_iceberg_options == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OUT_OF_MEMORY),
				 errmsg("out of memory")));

	popt = pg_lake_iceberg_options;

	/* Append FDW-specific options and dummy terminator. */
	memcpy(popt, fdw_options, sizeof(fdw_options));
}


/*
 * Check whether the given option is one of the valid pg_lake options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option_pg_lake(const char *keyword, Oid context)
{
	PgLakeOption *opt;

	Assert(pg_lake_options);	/* must be initialized already */

	for (opt = pg_lake_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, keyword) == 0)
			return true;
	}

	return false;
}

/*
 * Check whether the given option is one of the valid pg_lake_iceberg options.
 * context is the Oid of the catalog holding the object the option is for.
 */
static bool
is_valid_option_pg_lake_iceberg(const char *keyword, Oid context)
{
	PgLakeOption *opt;

	Assert(pg_lake_iceberg_options);	/* must be initialized already */

	for (opt = pg_lake_iceberg_options; opt->keyword; opt++)
	{
		if (context == opt->optcontext && strcmp(opt->keyword, keyword) == 0)
			return true;
	}

	return false;
}


Datum
pg_lake_iceberg_validator(PG_FUNCTION_ARGS)
{
	/*
	 * Check that the user has the necessary permissions. All iceberg tables
	 * are writable.
	 */
	CheckURLWriteAccess();

	List	   *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid			catalog = PG_GETARG_OID(1);

	bool		locationProvided = false;

	/* if not provided, assume postgres catalog */
	IcebergCatalogType icebergCatalogType = POSTGRES_CATALOG;
	bool		readOnlyExternalCatalogTable = false;
	char	   *catalogName = NULL;
	char	   *catalogTableName = NULL;
	char	   *catalogNamespace = NULL;

	/* Build our options lists if we didn't yet. */
	InitPgLakeIcebergOptions();

	/*
	 * Check that only options supported by pg_lake_iceberg, and allowed for
	 * the current object type, are given.
	 */
	ListCell   *cell;

	foreach(cell, options_list)
	{
		DefElem    *def = (DefElem *) lfirst(cell);

		if (!is_valid_option_pg_lake_iceberg(def->defname, catalog))
		{
			/*
			 * Unknown option specified, complain about it. Provide a hint
			 * with a valid option that looks similar, if there is one.
			 */
			PgLakeOption *opt;
			const char *closest_match;
			ClosestMatchState match_state;
			bool		has_valid_options = false;

			initClosestMatch(&match_state, def->defname, 4);
			for (opt = pg_lake_iceberg_options; opt->keyword; opt++)
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

			/*
			 * the placeholder will be replaced at post hook by default
			 * location
			 */
			if (strcmp(location, DEFAULT_ICEBERG_LOCATION_PLACEHOLDER) == 0)
			{
				locationProvided = true;
				continue;
			}

			if (!IsSupportedURL(location))
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("pg_lake_iceberg: only s3:// and gs:// "
									   "URLs are currently supported for the \"location\" "
									   "option.")));

			char	   *charPointer = strchr(location, '?');

			if (charPointer != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_SYNTAX_ERROR),
						 errmsg("s3 configuration parameters are not allowed in the \"location\" "
								"option for pg_lake_iceberg tables")));

			locationProvided = true;
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "autovacuum_enabled") == 0)
		{
			/* only accept boolean */
			(void) defGetBoolean(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "catalog") == 0)
		{
			char	   *icebergCatalogName = defGetString(def);

			/*
			 * We only accept "rest" and "postgres" for now. If not provided,
			 * assume "postgres" by default. Don't allow anything.
			 */
			if (strcmp(icebergCatalogName, "rest") == 0)
			{
				/*
				 * at this point, we cannot tell whether it's read only or
				 * read write. We'll determine that later based on the
				 * read_only option.
				 */
				icebergCatalogType = REST_CATALOG_READ_ONLY;
			}
			else if (strcmp(icebergCatalogName, "object_store") == 0)
			{

				/*
				 * at this point, we cannot tell whether it's read only or
				 * read write. We'll determine that later based on the
				 * read_only option.
				 */
				icebergCatalogType = OBJECT_STORE_READ_ONLY;
			}
			else if (strcmp(icebergCatalogName, "postgres") == 0)
				icebergCatalogType = POSTGRES_CATALOG;
			else
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid catalog option: %s", icebergCatalogName),
						 errdetail("Only \"rest\" and \"postgres\" are supported for now.")));
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "read_only") == 0)
		{
			/* only accept boolean */
			readOnlyExternalCatalogTable = defGetBoolean(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "catalog_table_name") == 0)
		{
			catalogTableName = defGetString(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "catalog_namespace") == 0)
		{
			catalogNamespace = defGetString(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "catalog_name") == 0)
		{
			catalogName = defGetString(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "row_ids") == 0)
		{
			/* only accept boolean */
			(void) defGetBoolean(def);
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "partition_by") == 0)
		{
			/* only accept text */
			(void) defGetString(def);

			/*
			 * we only make sure partitionBy is a string, but we'll verify
			 * whether the syntax and semantics of the partitionBy is correct
			 * in ParseAnalyzeIcebergTablePartitionBy when the table is
			 * actually created.
			 */
		}
		else if (catalog == ForeignTableRelationId && strcmp(def->defname, "column_stats_mode") == 0)
		{
			const char *columnStatsMode = ToLowerCase(defGetString(def));

			if (columnStatsMode &&
				strncasecmp(columnStatsMode, "truncate(", strlen("truncate(")) == 0 &&
				columnStatsMode[strlen(columnStatsMode) - 1] == ')')
			{
				const char *truncateLenStrStart = strchr(columnStatsMode, '(');
				const char *truncateLenStrEnd = columnStatsMode + strlen(columnStatsMode) - 1;

				char	   *columnStatsTruncateLenStr = palloc0(truncateLenStrEnd - truncateLenStrStart);

				strncpy(columnStatsTruncateLenStr, truncateLenStrStart + 1, truncateLenStrEnd - truncateLenStrStart - 1);

				char	   *parseEnd = NULL;
				int64_t		columnStatsTruncateLen = strtoi64(columnStatsTruncateLenStr, &parseEnd, 10);

				if (errno == EINVAL || *parseEnd != '\0')
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("column_stats_mode truncate length must be a valid integer")));

				if (errno == ERANGE)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("column_stats_mode truncate length must be at most %" PRId64, INT64_MAX))
						);

				if (columnStatsTruncateLen < 1)
					ereport(ERROR,
							(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							 errmsg("column_stats_mode truncate length must be greater than 0")));
			}
			else if (columnStatsMode &&
					 strcmp(columnStatsMode, "full") != 0 &&
					 strcmp(columnStatsMode, "none") != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("invalid column_stats_mode option: %s", columnStatsMode)));
			}
		}
	}

	/* first, adjust readable vs writable for external catalog tables */
	if (catalog == ForeignTableRelationId && icebergCatalogType == REST_CATALOG_READ_ONLY &&
		!readOnlyExternalCatalogTable)
		icebergCatalogType = REST_CATALOG_READ_WRITE;
	if (catalog == ForeignTableRelationId && icebergCatalogType == OBJECT_STORE_READ_ONLY &&
		!readOnlyExternalCatalogTable)
		icebergCatalogType = OBJECT_STORE_READ_WRITE;

	if (catalog == ForeignTableRelationId && locationProvided == false &&
		(icebergCatalogType == POSTGRES_CATALOG || icebergCatalogType == OBJECT_STORE_READ_WRITE))
		ereport(ERROR,
				(errcode(ERRCODE_SYNTAX_ERROR),
				 errmsg("\"location\" option is required for pg_lake_iceberg tables"),
				 errhint("You can set the GUC pg_lake_iceberg.default_location_prefix "
						 "to globally specify the location prefix.")));

	if (catalog == ForeignTableRelationId && readOnlyExternalCatalogTable &&
		!(icebergCatalogType == REST_CATALOG_READ_ONLY || icebergCatalogType == OBJECT_STORE_READ_ONLY))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"read_only\" option is only valid for catalog=\"rest\"")));

	if (catalog == ForeignTableRelationId)
	{
		if (icebergCatalogType == REST_CATALOG_READ_ONLY || icebergCatalogType == OBJECT_STORE_READ_ONLY)
		{
			/*
			 * catalog_namespace, catalog_table_name and catalog_name is
			 * required for catalog=rest
			 */
			if (!catalogName)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"catalog_name\" option is required for catalog=\"rest\"")));

			if (!catalogNamespace && icebergCatalogType != OBJECT_STORE_READ_ONLY)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"catalog_namespace\" option is required for catalog=\"rest\"")));

			if (!catalogTableName)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"catalog_table_name\" option is required for catalog=\"rest\"")));
		}
		else
		{
			/*
			 * For other catalog types these options are not valid.
			 */
			if (catalogNamespace)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"catalog_namespace\" option is only valid for writable catalog=\"rest\"")));

			if (catalogTableName)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"catalog_table_name\" option is only valid for writable catalog=\"rest\"")));

			if (catalogName)
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("\"catalog_name\" option is only valid for writable catalog=\"rest\"")));

		}
	}
	PG_RETURN_VOID();
}
