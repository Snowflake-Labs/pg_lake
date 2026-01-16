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
 * Functions for getting the size of a table.
 */
#include "postgres.h"
#include "fmgr.h"

#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/util/rel_utils.h"
#include "utils/builtins.h"


PG_FUNCTION_INFO_V1(pg_lake_table_size);


/*
 * pg_lake_table_size gets the size of a table, which can be an
 * Iceberg table.
 */
Datum
pg_lake_table_size(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);

	if (IsPgLakeIcebergForeignTableById(relationId))
	{
		int64		tableSize = GetTableSizeFromCatalog(relationId);

		PG_RETURN_INT64(tableSize);
	}
	else
	{
		return pg_table_size(fcinfo);
	}
}
