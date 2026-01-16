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
#include "utils/builtins.h"

#include "pg_lake/benchmark.h"
#include "pg_lake/pgduck/client.h"
#include "pg_extension_base/spi_helpers.h"


/*
 * PgDuckInstallBenchExtension installs the benchmark extension via pgduck server.
 */
void
PgDuckInstallBenchExtension(BenchmarkType benchType)
{
	char	   *query;

	if (benchType == BENCHMARK_TPCH)
	{
		query = "INSTALL tpch; LOAD tpch;";
	}
	else if (benchType == BENCHMARK_TPCDS)
	{
		query = "INSTALL tpcds; LOAD tpcds;";
	}
	else
	{
		ereport(ERROR, (errmsg("invalid benchmark type")));
	}

	ExecuteCommandInPGDuck(query);
}


/*
 * PgDuckDropBenchTables drops the benchmark tables via pgduck server.
 */
void
PgDuckDropBenchTables(const char *tableNames[], int length)
{
	for (int i = 0; i < length; i++)
	{
		const char *tableName = tableNames[i];

		char	   *query = psprintf("DROP TABLE IF EXISTS %s;", tableName);

		ExecuteCommandInPGDuck(query);
	}
}


/*
 * PgDuckGenerateBenchTables generates benchmark data via pgduck server with the
 * specified scale factor and iteration count by using duckdb tpch or tpcds extension.
 */
void
PgDuckGenerateBenchTables(BenchmarkType benchType, float4 scaleFactor, int iterationCount)
{
	if (benchType == BENCHMARK_TPCH)
	{
		for (int i = 0; i < iterationCount; i++)
		{
			char	   *query = psprintf("CALL dbgen(sf = %f, children = %d, step = %d)",
										 scaleFactor, iterationCount, i);

			ExecuteCommandInPGDuck(query);
		}
	}
	else if (benchType == BENCHMARK_TPCDS)
	{
		char	   *query = psprintf("CALL dsdgen(sf = %f)", scaleFactor);

		ExecuteCommandInPGDuck(query);
	}
	else
	{
		ereport(ERROR, (errmsg("invalid benchmark type")));
	}
}


/*
 * PgDuckCopyBenchTablesToRemoteParquet copies the benchmark tables to the specified S3 location
 * in Parquet format via pgduck server.
 */
void
PgDuckCopyBenchTablesToRemoteParquet(const char *tableNames[], int length, char *location)
{
	for (int i = 0; i < length; i++)
	{
		const char *tableName = tableNames[i];

		char	   *query = psprintf("COPY %s TO '%s/%s.parquet' (FORMAT 'parquet')",
									 tableName, location, tableName);

		ExecuteCommandInPGDuck(query);
	}
}


/*
 * PgDuckGetQueries returns the benchmark queries via pgduck server.
 */
void
PgDuckGetQueries(BenchmarkType benchType, BenchQuery * queries, int queryCount)
{
	char	   *query;

	if (benchType == BENCHMARK_TPCH)
	{
		query = "SELECT * FROM tpch_queries() ORDER BY query_nr;";
	}
	else if (benchType == BENCHMARK_TPCDS)
	{
		query = "SELECT * FROM tpcds_queries() ORDER BY query_nr;";
	}
	else
	{
		ereport(ERROR, (errmsg("invalid benchmark type")));
	}

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query);

	/* throw error if anything failed  */
	CheckPGDuckResult(pgDuckConn, result);

	Assert(PQntuples(result) == queryCount);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		for (int rowIndex = 0; rowIndex < queryCount; rowIndex++)
		{
			BenchQuery *query = &queries[rowIndex];

			query->query_nr = atoi(PQgetvalue(result, rowIndex, 0));
			query->query = pstrdup(PQgetvalue(result, rowIndex, 1));
		}

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleasePGDuckConnection(pgDuckConn);
}


/*
 * PgLakeDropBenchTables drops the bench tables in PgLake and
 * makes sure the corresponding parquet files are removed from the cache.
 */
void
PgLakeDropBenchTables(const char *tableNames[], int length, char *location)
{
	StringInfo	query = makeStringInfo();

	for (int i = 0; i < length; i++)
	{
		const char *tableName = tableNames[i];

		appendStringInfo(query, "DROP TABLE IF EXISTS %s;", tableName);

		char	   *path = psprintf("%s/%s.parquet", location, tableName);

		appendStringInfo(query, "SELECT  lake_file_cache.remove('%s');", path);
	}

	SPI_START();

	bool		readOnly = false;

	SPI_execute(query->data, readOnly, 0);

	SPI_END();
}


/*
 * PgLakeCreateBenchTables creates the bench tables in PgLake
 * from the specified location.
 */
void
PgLakeCreateBenchTables(const char *tableNames[], char **partitionBys, int length,
						BenchmarkTableType tableType, char *location)
{
	StringInfo	query = makeStringInfo();

	for (int i = 0; i < length; i++)
	{
		const char *tableName = tableNames[i];
		const char *partitionBy = (partitionBys != NULL && partitionBys[i] != NULL) ? partitionBys[i] : NULL;

		char	   *path = psprintf("%s/%s.parquet", location, tableName);

		if (tableType == BENCHMARK_ICEBERG_TABLE)
		{
			appendStringInfo(query,
							 "CREATE TABLE %s() USING iceberg WITH (load_from = '%s' %s);",
							 tableName, path,
							 partitionBy ? psprintf(", partition_by='%s'", partitionBy) : "");
		}
		else if (tableType == BENCHMARK_LAKE_TABLE)
		{
			appendStringInfo(query,
							 "CREATE FOREIGN TABLE %s() SERVER pg_lake "
							 "OPTIONS (path '%s', format 'parquet');",
							 tableName, path);
		}
		else if (tableType == BENCHMARK_HEAP_TABLE)
		{
			appendStringInfo(query,
							 "CREATE TABLE %s() WITH (load_from = '%s');", tableName, path);
		}
		else
		{
			ereport(ERROR, (errmsg("invalid table type")));
		}
	}

	SPI_START();

	bool		readOnly = false;

	SPI_execute(query->data, readOnly, 0);

	SPI_END();
}


/*
 * GetBenchTableType returns the BenchmarkTableType for the given table type id.
 */
BenchmarkTableType
GetBenchTableType(Oid tableTypeId)
{
	char	   *tableTypeLabel = DatumGetCString(DirectFunctionCall1(enum_out, ObjectIdGetDatum(tableTypeId)));

	if (strcmp(tableTypeLabel, "pg_lake_iceberg") == 0)
		return BENCHMARK_ICEBERG_TABLE;
	else if (strcmp(tableTypeLabel, "pg_lake") == 0)
		return BENCHMARK_LAKE_TABLE;
	else if (strcmp(tableTypeLabel, "heap") == 0)
		return BENCHMARK_HEAP_TABLE;
	else
		ereport(ERROR, (errmsg("invalid table type")));
}
