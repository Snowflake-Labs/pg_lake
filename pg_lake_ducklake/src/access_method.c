/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Placeholder access-method handler for the "pg_lake_ducklake" / "ducklake"
 * table access methods. The handler must exist as a SQL-callable C function
 * because CREATE ACCESS METHOD requires it, but the code path that would
 * use a real table_am_handler is never taken — DuckLake tables are foreign
 * tables managed by the pg_lake_table FDW. Calling this would mean
 * something tried to use the AM as a heap-style storage engine, which we
 * explicitly reject.
 */
#include "postgres.h"
#include "fmgr.h"

#include "access/tableam.h"

PG_FUNCTION_INFO_V1(pg_lake_ducklake_am_handler);

Datum
pg_lake_ducklake_am_handler(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("pg_lake_ducklake access method is a placeholder "
					"and should not be used")));
}
