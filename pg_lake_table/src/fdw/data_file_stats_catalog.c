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

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/data_file_stats_catalog.h"
#include "pg_extension_base/spi_helpers.h"

#include "catalog/namespace.h"
#include "utils/lsyscache.h"


/*
 * DataFileColumnStatsCatalogExists checks if the
 * lake_table.data_file_column_stats catalog exists.
 */
bool
DataFileColumnStatsCatalogExists(void)
{
	bool		missingOk = true;

	Oid			namespaceId = get_namespace_oid(PG_LAKE_TABLE_SCHEMA, missingOk);

	if (namespaceId == InvalidOid)
		return false;

	return get_relname_relid(PG_LAKE_TABLE_DATA_FILE_COLUMN_STATS_TABLE_NAME, namespaceId) != InvalidOid;
}
