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
 * Test extension that registers a PgLakeAddDataFileHook returning true.
 *
 * pg_lake_table calls this hook on each newly-added data file; when the
 * hook is set and returns true, pg_lake_table records the file ID in a
 * per-transaction temp table.  That temp-table creation path is the only
 * SPI_START_EXTENSION_OWNER caller that needs to allow CREATE TEMP, so
 * having this extension loaded during DML on an Iceberg table guarantees
 * we exercise SPI_START_EXTENSION_OWNER_ALLOW_TEMP end-to-end.
 *
 * Used by tests/pytests/test_data_file_hook_temp_table.py.
 */
#include "postgres.h"
#include "fmgr.h"

#include "pg_lake/fdw/data_files_catalog.h"

PG_MODULE_MAGIC;

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
