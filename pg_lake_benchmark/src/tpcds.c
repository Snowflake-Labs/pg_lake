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


static const char *TpcdsTableNames[] = {
	"call_center",
	"catalog_page",
	"catalog_returns",
	"catalog_sales",
	"customer",
	"customer_address",
	"customer_demographics",
	"date_dim",
	"household_demographics",
	"income_band",
	"inventory",
	"item",
	"promotion",
	"reason",
	"ship_mode",
	"store",
	"store_returns",
	"store_sales",
	"time_dim",
	"warehouse",
	"web_page",
	"web_returns",
	"web_sales",
	"web_site"
};

static const int TpcdsTableNamesLength = sizeof(TpcdsTableNames) / sizeof(TpcdsTableNames[0]);

static const int TpcdsQueryCount = 99;


PG_FUNCTION_INFO_V1(pg_lake_tpcds_gen);
PG_FUNCTION_INFO_V1(pg_lake_tpcds_queries);

Datum
pg_lake_tpcds_gen(PG_FUNCTION_ARGS)
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

	/* invalid for tpcds */
	int			iterationCount = -1;

	/* remove trailing slash if any */
	int			locationLen = strlen(location);

	if (locationLen > 1 && location[locationLen - 1] == '/')
		location[locationLen - 1] = '\0';

	PgDuckInstallBenchExtension(BENCHMARK_TPCDS);

	/* export generated tables to s3 */
	PgDuckDropBenchTables(TpcdsTableNames, TpcdsTableNamesLength);
	PgDuckGenerateBenchTables(BENCHMARK_TPCDS, scaleFactor, iterationCount);
	PgDuckCopyBenchTablesToRemoteParquet(TpcdsTableNames, TpcdsTableNamesLength, location);

	/* create pg lake tables from s3 */
	PgLakeDropBenchTables(TpcdsTableNames, TpcdsTableNamesLength, location);
	PgLakeCreateBenchTables(TpcdsTableNames, NULL, TpcdsTableNamesLength, tableType, location);

	/* do not leave duckdb tables around */
	PgDuckDropBenchTables(TpcdsTableNames, TpcdsTableNamesLength);

	PG_RETURN_VOID();
}


Datum
pg_lake_tpcds_queries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	Datum		values[2];
	bool		nulls[2];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	BenchQuery *queries = palloc0(sizeof(BenchQuery) * TpcdsQueryCount);

	PgDuckGetQueries(BENCHMARK_TPCDS, queries, TpcdsQueryCount);

	for (int i = 0; i < TpcdsQueryCount; i++)
	{
		BenchQuery *query = &queries[i];

		values[0] = Int32GetDatum(query->query_nr);
		values[1] = CStringGetTextDatum(query->query);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}
