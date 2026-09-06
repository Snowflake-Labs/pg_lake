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

#ifndef PG_LAKE_COPY_H
#define PG_LAKE_COPY_H

#include "pg_lake/copy/copy_format.h"
#include "nodes/parsenodes.h"
#include "nodes/plannodes.h"
#include "parser/parse_node.h"
#include "pg_lake/ddl/utility_hook.h"

/* settings */
extern bool EnablePgLakeCopy;
extern bool EnablePgLakeCopyJson;

/*
 * JsonCopyMode controls who handles a JSON COPY. It is a hidden, test-only knob
 * (see pg_lake_copy.json_copy_mode); normal users never set it and rely on the
 * "auto" default. We follow the same rule as CSV: Postgres gets precedence
 * whenever it natively supports the case, and pg_lake covers the rest.
 *
 *   JSON_COPY_MODE_AUTO     - surgical default: defer to Postgres only for the
 *                             cases it supports (uncompressed COPY TO a local
 *                             file/STDOUT); pg_lake handles remote targets,
 *                             COPY FROM json, and compression.
 *   JSON_COPY_MODE_POSTGRES - always defer to Postgres (used by the upstream
 *                             PostgreSQL regression suite so its expected
 *                             output/error wording is reproduced verbatim).
 *   JSON_COPY_MODE_PGLAKE   - always handle in pg_lake (used by pg_lake's own
 *                             pytest suite to exercise the DuckDB JSON path).
 */
typedef enum JsonCopyModeType
{
	JSON_COPY_MODE_AUTO,
	JSON_COPY_MODE_POSTGRES,
	JSON_COPY_MODE_PGLAKE,
}			JsonCopyModeType;

/* GUC storage for an enum is an int (holds a JsonCopyModeType value) */
extern int	JsonCopyMode;

/* allowed values for the pg_lake_copy.json_copy_mode enum GUC (defined in copy.c) */
struct config_enum_entry;
extern const struct config_enum_entry json_copy_mode_options[];

bool		PgLakeCopyHandler(ProcessUtilityParams * params, void *arg);
void		ProcessPgLakeCopy(ParseState *pstate, PlannedStmt *plannedStmt,
							  const char *queryString, uint64 *rowsProcessed);

/* support additional extension-provided checks for copy from */
typedef void (*PgLakeCopyValidityCheckHookType) (Oid relation);
extern PGDLLEXPORT PgLakeCopyValidityCheckHookType PgLakeCopyValidityCheckHook;

#endif
