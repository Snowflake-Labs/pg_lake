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

#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/catalog/row_id_mappings.h"
#include "pg_lake/query/execute.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/plan_cache.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/util/string_utils.h"
#include "executor/spi.h"
#include "utils/snapmgr.h"


/*
 * Insert a row mapping entry into the appropriate data file catalog.
 *
 * Note that rowIdEnd is exclusive
 */
void
InsertSingleRowMapping(Oid relationId, int64 fileNumber,
					   int64 rowIdStart, int64 rowIdEnd, int64 rowNumberStart)
{
	/* conflicting mappings may have been removed by current statement */
	PushActiveSnapshot(GetLatestSnapshot());

	/* insert with an inclusive range */
	char	   *query =
		"insert into " ROW_ID_MAPPINGS_QUALIFIED_TABLE_NAME
		" (table_name, file_id, row_id_range, file_row_number) "
		"values"
		" ($1, $2, pg_catalog.int8range($3,$4,'[)'), $5)";

	Assert(OidIsValid(relationId));
	Assert(fileNumber > 0);
	Assert(rowIdEnd >= rowIdStart);
	Assert(rowIdStart >= 0);
	Assert(rowIdEnd >= 0);
	Assert(rowNumberStart >= 0);

	DECLARE_SPI_ARGS(5);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT8OID, fileNumber, false);
	SPI_ARG_VALUE(3, INT8OID, rowIdStart, false);
	SPI_ARG_VALUE(4, INT8OID, rowIdEnd, false);
	SPI_ARG_VALUE(5, INT8OID, rowNumberStart, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	SPIPlanPtr	plan = GetCachedQueryPlan(query, spiArgCount, spiArgTypes);

	if (plan == NULL)
		elog(ERROR, "SPI_prepare returned %s while inserting metadata",
			 SPI_result_code_string(SPI_result));

	bool		readOnly = false;

	SPI_execute_plan(plan, spiArgValues, spiArgNulls, readOnly, 0);

	SPI_END();

	PopActiveSnapshot();
}


/*
 * DeleteRowMappingsForTable deletes all the row mappings for a given table.
 */
void
DeleteRowMappingsForTable(Oid relationId)
{
	/* insert with an inclusive range */
	char	   *query =
		"delete from " ROW_ID_MAPPINGS_QUALIFIED_TABLE_NAME
		"where table_name operator(pg_catalog.=) $1";

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE(query, readOnly);

	SPI_END();
}
