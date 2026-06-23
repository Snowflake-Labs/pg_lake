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
 * pg_lake_copy extension entry-point.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pg_lake/copy/create_table.h"
#include "pg_lake/copy/copy.h"
#include "utils/guc.h"

#define GUC_STANDARD 0

PG_MODULE_MAGIC;

/* function declarations */
void		_PG_init(void);


/*
 * _PG_init is the entry-point for pg_lake_copy which is called on postmaster
 * start-up when pg_lake_copy is in shared_preload_libraries.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR, (errmsg("pg_lake_copy can only be loaded via shared_preload_libraries"),
						errhint("Add pg_lake_copy to shared_preload_libraries configuration "
								"variable in postgresql.conf")));
	}

	DefineCustomBoolVariable(
							 "pg_lake_copy.enable",
							 gettext_noop("Enables pg_lake_copy enhancements"),
							 NULL,
							 &EnablePgLakeCopy,
							 true,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_copy.enable_json",
							 gettext_noop("Enables pg_lake_copy JSON support"),
							 NULL,
							 &EnablePgLakeCopyJson,
							 true,
							 PGC_SUSET,
							 GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE,
							 NULL, NULL, NULL);

	DefineCustomBoolVariable(
							 "pg_lake_copy.include_generated_columns",
							 gettext_noop("Include generated columns in COPY TO output"),
							 gettext_noop("When off, generated columns are excluded from COPY TO output so the result can be reloaded with COPY FROM, matching core PostgreSQL behavior"),
							 &PgLakeCopyIncludeGeneratedColumns,
							 true,
							 PGC_USERSET,
							 0,
							 NULL, NULL, NULL);

	RegisterUtilityStatementHandler(CreateTableFromFileHandler, NULL);
	RegisterUtilityStatementHandler(PgLakeCopyHandler, NULL);
}
