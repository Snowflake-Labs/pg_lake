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
 * Test extension that exposes hooks/helpers used to exercise narrow
 * temp-object-creation sites under SPI_START_EXTENSION_OWNER.
 *
 * 1. Registers a PgLakeAddDataFileHook returning true.  pg_lake_table
 *    calls this hook on each newly-added data file; when the hook is set
 *    and returns true, pg_lake_table records the file ID in a
 *    per-transaction temp table.  That site exercises the
 *    SPI_START_EXTENSION_OWNER_ALLOWING_TEMP_OBJECTS variant in
 *    CreateTxDataFileIdsTempTableIfNotExists.
 *
 * 2. Exposes run_iceberg_dml_under_extension_owner(query text), a SQL
 *    helper that runs an UPDATE/DELETE on an Iceberg foreign table from
 *    inside SPI_START_EXTENSION_OWNER.  pg_lake_table's BeginForeignModify
 *    creates a per-statement temp tracking table for both UPDATE and
 *    DELETE; the temp create must succeed under
 *    SECURITY_RESTRICTED_OPERATION.  Without ALLOW_TEMP_OBJECTS_BEGIN/END
 *    in CreateUpdateTrackingTable this fails with "cannot create temporary
 *    table within security-restricted operation".
 *
 * Used by tests/pytests/test_data_file_hook_temp_table.py.
 */
#include "postgres.h"
#include "fmgr.h"

#include "pg_extension_base/extension_ids.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "utils/builtins.h"

PG_MODULE_MAGIC;

extern PGDLLIMPORT CachedExtensionIds * PgLakeTable;

void		_PG_init(void);

static bool AlwaysAddDataFile(void);


static bool
AlwaysAddDataFile(void)
{
	return true;
}


void
_PG_init(void)
{
	PgLakeAddDataFileHook = AlwaysAddDataFile;
}


PG_FUNCTION_INFO_V1(run_iceberg_dml_under_extension_owner);

/*
 * run_iceberg_dml_under_extension_owner(query text) RETURNS void
 *
 * Runs an arbitrary SQL string from inside SPI_START_EXTENSION_OWNER, i.e.
 * with SECURITY_RESTRICTED_OPERATION set and search_path pinned to
 * pg_catalog,pg_temp.  Tests pass an UPDATE/DELETE on an Iceberg foreign
 * table to exercise CreateUpdateTrackingTable's narrow ALLOW_TEMP_OBJECTS
 * scope.
 *
 * Test-only.  Not safe to expose to untrusted callers since it executes
 * caller-supplied SQL as the extension owner.
 */
Datum
run_iceberg_dml_under_extension_owner(PG_FUNCTION_ARGS)
{
	char	   *query = text_to_cstring(PG_GETARG_TEXT_PP(0));

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	int			spiStatus = SPI_execute(query, false /* readOnly */ , 0);

	if (spiStatus < 0)
		ereport(ERROR, (errmsg("SPI_execute returned %d", spiStatus)));

	SPI_END();

	PG_RETURN_VOID();
}
