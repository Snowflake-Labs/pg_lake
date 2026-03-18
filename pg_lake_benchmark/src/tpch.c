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
#include "utils/builtins.h"

#include "pg_lake/benchmark.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/util/string_utils.h"

/* make sure the order matches TpchTableNames */
#define LINEITEM_TABLE_IDX 0
#define CUSTOMER_TABLE_IDX 1
#define NATION_TABLE_IDX 2
#define ORDERS_TABLE_IDX 3
#define PART_TABLE_IDX 4
#define PARTSUPP_TABLE_IDX 5
#define REGION_TABLE_IDX 6
#define SUPPLIER_TABLE_IDX 7

static const char *TpchTableNames[] = {
	"lineitem",
	"customer",
	"nation",
	"orders",
	"part",
	"partsupp",
	"region",
	"supplier"
};

static const int TpchTableNamesLength = sizeof(TpchTableNames) / sizeof(TpchTableNames[0]);

static const int TpchQueryCount = 22;

static void TpchGen(float4 scaleFactor, int iterationCount, char *location,
					BenchmarkTableType tableType, char **partitionBys);


PG_FUNCTION_INFO_V1(pg_lake_tpch_gen);
PG_FUNCTION_INFO_V1(pg_lake_tpch_gen_partitioned);
PG_FUNCTION_INFO_V1(pg_lake_tpch_queries);

Datum
pg_lake_tpch_gen(PG_FUNCTION_ARGS)
{
	char	   *location = NULL;

	if (!PG_ARGISNULL(0))
	{
		location = text_to_cstring(PG_GETARG_TEXT_P(0));
	}

	if (!IsSupportedURL(location))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("only s3://, gs://, az://, azure://, and abfss:// URLs are "
							   "currently supported")));

	Oid			tableTypeId = (BenchmarkTableType) PG_GETARG_OID(1);
	BenchmarkTableType tableType = GetBenchTableType(tableTypeId);

	float4		scaleFactor = PG_GETARG_FLOAT4(2);

	int			iterationCount = PG_GETARG_INT32(3);

	/* remove trailing slash if any */
	int			locationLen = strlen(location);

	if (locationLen > 1 && location[locationLen - 1] == '/')
		location[locationLen - 1] = '\0';

	TpchGen(scaleFactor, iterationCount, location, tableType, NULL);

	PG_RETURN_VOID();
}


Datum
pg_lake_tpch_gen_partitioned(PG_FUNCTION_ARGS)
{
	char	   *location = NULL;

	if (!PG_ARGISNULL(0))
	{
		location = text_to_cstring(PG_GETARG_TEXT_P(0));
	}

	if (!IsSupportedURL(location))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("only s3://, gs://, az://, azure://, and abfss:// URLs are "
							   "currently supported")));

	Oid			tableTypeId = (BenchmarkTableType) PG_GETARG_OID(1);
	BenchmarkTableType tableType = GetBenchTableType(tableTypeId);

	float4		scaleFactor = PG_GETARG_FLOAT4(2);

	int			iterationCount = PG_GETARG_INT32(3);

	/* create a text array for partition transforms for each table */
	char	  **partitionBys = palloc0(sizeof(char *) * TpchTableNamesLength);

	/* lineitem_partition_by */
	if (!PG_ARGISNULL(4))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(4));

		partitionBys[LINEITEM_TABLE_IDX] = partitionBy;
	}

	/* customer_partition_by */
	if (!PG_ARGISNULL(5))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(5));

		partitionBys[CUSTOMER_TABLE_IDX] = partitionBy;
	}

	/* nation_partition_by */
	if (!PG_ARGISNULL(6))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(6));

		partitionBys[NATION_TABLE_IDX] = partitionBy;
	}

	/* orders_partition_by */
	if (!PG_ARGISNULL(7))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(7));

		partitionBys[ORDERS_TABLE_IDX] = partitionBy;
	}

	/* part_partition_by */
	if (!PG_ARGISNULL(8))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(8));

		partitionBys[PART_TABLE_IDX] = partitionBy;
	}

	/* partsupp_partition_by */
	if (!PG_ARGISNULL(9))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(9));

		partitionBys[PARTSUPP_TABLE_IDX] = partitionBy;
	}

	/* region_partition_by */
	if (!PG_ARGISNULL(10))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(10));

		partitionBys[REGION_TABLE_IDX] = partitionBy;
	}

	/* supplier_partition_by */
	if (!PG_ARGISNULL(11))
	{
		char	   *partitionBy = text_to_cstring(PG_GETARG_TEXT_P(11));

		partitionBys[SUPPLIER_TABLE_IDX] = partitionBy;
	}

	/* remove trailing slash if any */
	int			locationLen = strlen(location);

	if (locationLen > 1 && location[locationLen - 1] == '/')
		location[locationLen - 1] = '\0';

	TpchGen(scaleFactor, iterationCount, location, tableType, partitionBys);

	PG_RETURN_VOID();
}


/*
* Generate TPC-H tables and copy them to the remote location.
* Then create pg lake tables from the remote location.
* Finally, drop the duckdb tables.
*/
static void
TpchGen(float4 scaleFactor, int iterationCount, char *location,
		BenchmarkTableType tableType, char **partitionBys)
{

	PgDuckInstallBenchExtension(BENCHMARK_TPCH);

	/* export generated tables to s3 */
	PgDuckDropBenchTables(TpchTableNames, TpchTableNamesLength);
	PgDuckGenerateBenchTables(BENCHMARK_TPCH, scaleFactor, iterationCount);
	PgDuckCopyBenchTablesToRemoteParquet(TpchTableNames, TpchTableNamesLength, location);

	/* create pg lake tables from s3 */
	PgLakeDropBenchTables(TpchTableNames, TpchTableNamesLength, location);
	PgLakeCreateBenchTables(TpchTableNames, partitionBys, TpchTableNamesLength, tableType, location);

	/* do not leave duckdb tables around */
	PgDuckDropBenchTables(TpchTableNames, TpchTableNamesLength);
}


Datum
pg_lake_tpch_queries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	Datum		values[2];
	bool		nulls[2];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	BenchQuery *queries = palloc0(sizeof(BenchQuery) * TpchQueryCount);

	PgDuckGetQueries(BENCHMARK_TPCH, queries, TpchQueryCount);

	for (int i = 0; i < TpchQueryCount; i++)
	{
		BenchQuery *query = &queries[i];

		values[0] = Int32GetDatum(query->query_nr);
		values[1] = CStringGetTextDatum(query->query);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}
