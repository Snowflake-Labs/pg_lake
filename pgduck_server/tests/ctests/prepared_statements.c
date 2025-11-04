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

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/wait.h>
#include <pthread.h>
#include <signal.h>

#include "libpq-fe.h"
#include "postgres_fe.h"
#include "catalog/pg_type_d.h"
#include "libpq-fe.h"
#include "utils/elog.h"

const char *conninfo = "host=/tmp port=7778";
pid_t		pgduck_pid;


/* test binding all PG types, following order in GetDuckDBTypeForPGType */
static int	testParamUnnamedPreparedStatement_boolean();
static int	testParamUnnamedPreparedStatement_bit();
static int	testParamUnnamedPreparedStatement_bytea();
static int	testParamUnnamedPreparedStatement_date();
static int	testParamUnnamedPreparedStatement_float4();
static int	testParamUnnamedPreparedStatement_float8();
static int	testParamUnnamedPreparedStatement_int2();
static int	testParamUnnamedPreparedStatement_int4();
static int	testParamUnnamedPreparedStatement_int8();
static int	testParamUnnamedPreparedStatement_interval();
static int	testParamUnnamedPreparedStatement_numeric();
static int	testParamUnnamedPreparedStatement_oid();
static int	testParamUnnamedPreparedStatement_timestamp();
static int	testParamUnnamedPreparedStatement_timestamptz();
static int	testParamUnnamedPreparedStatement_time();
static int	testParamUnnamedPreparedStatement_uuid();
static int	testParamUnnamedPreparedStatement_varchar();

/* test binding all duckdb types that do not have 1-1 mapping on PG */
static int	testParamUnnamedPreparedStatement_uhugeint();
static int	testParamUnnamedPreparedStatement_ubigint();
static int	testParamUnnamedPreparedStatement_usmallint();
static int	testParamUnnamedPreparedStatement_utinyint();
static int	testParamUnnamedPreparedStatement_tinyint();
static int	testParamUnnamedPreparedStatement_uinteger();
static int	testParamUnnamedPreparedStatement_timestamp_s();
static int	testParamUnnamedPreparedStatement_timestamp_ms();
static int	testParamUnnamedPreparedStatement_timestamp_ns();
static int	testParamUnnamedPreparedStatement_time_tz();
static int	testParamUnnamedPreparedStatement_timestamp_tz();

/* more complex prepared statement tests */
static int	testExecutePreparedStatementMultipleTimesAndCheck();
static int	testPreparedStatementWithTenColumns();
static int	testUpdateDelete();
static int	testFloorFunctionWithPreparedStatement();
static int	testPQSendParams();
static int	executeCopyAndVerifyWithPQsendQueryParams();
static int	testNamedPreparedStatement();
static int	testWithLargeDataSize();
static int	testParseError();
static int	testBindError();
static int	testExecuteError();
static int	testExecuteSuccessError();
static int	testTransmitPrepared();

/* helper functions */
static void prepareOrExit(PGconn *conn, const char *query, int nParams, const Oid *paramTypes);
static void executeQueryOrExit(PGconn *conn, const char *query);
static void cleanUpPgDuckServer();
static void addCurrentDirectoryToPath();

/*  Macro to run a test function and check its result */
#define RUN_TEST(testFunc) if (testFunc() != EXIT_SUCCESS) {cleanUpPgDuckServer(); exit(EXIT_FAILURE); }

/*  Macros for repetition */
#define SETUP_CONNECTION(conn) \
    PGconn *conn = PQconnectdb(conninfo); \
    if (PQstatus(conn) != CONNECTION_OK) { \
        PQfinish(conn); \
    	cleanUpPgDuckServer(); \
        return EXIT_FAILURE; \
    }
#define TEARDOWN_CONNECTION(conn) \
    PQfinish(conn);

#define EXECUTE_PREPARED_STATEMENT(conn, nParams, paramValues, paramLengths, paramFormats, res) \
    res = PQexecPrepared(conn, "", nParams, paramValues, paramLengths, paramFormats, 0); \
    if (PQresultStatus(res) != PGRES_TUPLES_OK && PQresultStatus(res) != PGRES_COMMAND_OK) { \
        PQclear(res); \
        PQfinish(conn); \
        cleanUpPgDuckServer(); \
        return EXIT_FAILURE; \
    }

#define CHECK_QUERY_RESULT(res, expectedValue) \
    if (strcmp(expectedValue, PQgetvalue(res, 0, 0)) != 0) { \
        PQclear(res); \
        PQfinish(conn); \
    	cleanUpPgDuckServer(); \
        return EXIT_FAILURE; \
    }

#define EXECUTE_QUERY_AND_CHECK_RESULT(conn, query, expectedValue) \
    do { \
        PGresult *res = PQexec(conn, query); \
        if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1) { \
            PQclear(res); \
            PQfinish(conn); \
	    	cleanUpPgDuckServer(); \
            return EXIT_FAILURE; \
        } \
        CHECK_QUERY_RESULT(res, expectedValue) \
        PQclear(res); \
    } while (0);

#define CLEAR_RESULT(res) \
    PQclear(res);

#define EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedValue) \
    do { \
        PGresult *res; \
        EXECUTE_PREPARED_STATEMENT(conn, nParams, paramValues, paramLengths, paramFormats, res) \
        CHECK_QUERY_RESULT(res, expectedValue) \
        CLEAR_RESULT(res); \
    } while (0);

#define EXECUTE_PREPARED_AND_CHECK_LAST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedValue) \
    do { \
        PGresult *res; \
        EXECUTE_PREPARED_STATEMENT(conn, nParams, paramValues, paramLengths, paramFormats, res) \
        int lastRow = PQntuples(res) - 1; /* Index of the last row */ \
        if (lastRow < 0) { \
            PQclear(res); \
            PQfinish(conn); \
            cleanUpPgDuckServer(); \
            return EXIT_FAILURE; \
        } \
        const char* actualValue = PQgetvalue(res, lastRow, 0); \
        if (strcmp(expectedValue, actualValue) != 0) { \
            PQclear(res); \
            PQfinish(conn); \
            cleanUpPgDuckServer(); \
            return EXIT_FAILURE; \
        } \
        CLEAR_RESULT(res); \
    } while (0);

int
main()
{

	/*
	 * We first fork the process and let the child process start
	 * pgduck_server. The main process continues and executes the tests.
	 */
	pgduck_pid = fork();

	if (pgduck_pid == -1)
		return EXIT_FAILURE;
	else if (pgduck_pid == 0)
	{
		/* Child process: Start pgduck_server */
		if (chdir("../../") == -1)
		{
			perror("chdir");
			exit(EXIT_FAILURE);
		}

		/* the first parameter to execlp is the name of the program */
		addCurrentDirectoryToPath();
		execlp("pgduck_server", "pgduck_server", "--duckdb_database_file_path", "/tmp/duckdb_c_test", "--port", "7778", (char *) NULL);

		/* If execlp returns, it failed */
		perror("execlp");
		exit(EXIT_FAILURE);
	}
	else
	{
		/* Parent process: Continue to run tests */
		sleep(1);

		/*
		 * In this set of tests, we test the data type bindings works
		 * expected. We first test binding all PG types, following order in
		 * GetDuckDBTypeForPGType. Then remaining types that exists on duckdb
		 * but does not have 1-1 mapping on PG.
		 */
		RUN_TEST(testParamUnnamedPreparedStatement_boolean);
		RUN_TEST(testParamUnnamedPreparedStatement_bit);
		RUN_TEST(testParamUnnamedPreparedStatement_bytea);
		RUN_TEST(testParamUnnamedPreparedStatement_date);
		RUN_TEST(testParamUnnamedPreparedStatement_float4);
		RUN_TEST(testParamUnnamedPreparedStatement_float8);
		RUN_TEST(testParamUnnamedPreparedStatement_int2);
		RUN_TEST(testParamUnnamedPreparedStatement_int4);
		RUN_TEST(testParamUnnamedPreparedStatement_int8);
		RUN_TEST(testParamUnnamedPreparedStatement_interval);
		RUN_TEST(testParamUnnamedPreparedStatement_numeric);
		RUN_TEST(testParamUnnamedPreparedStatement_oid);
		RUN_TEST(testParamUnnamedPreparedStatement_timestamp);
		RUN_TEST(testParamUnnamedPreparedStatement_timestamptz);
		RUN_TEST(testParamUnnamedPreparedStatement_time);
		RUN_TEST(testParamUnnamedPreparedStatement_uuid);
		RUN_TEST(testParamUnnamedPreparedStatement_varchar);
		RUN_TEST(testParamUnnamedPreparedStatement_uhugeint);
		RUN_TEST(testParamUnnamedPreparedStatement_ubigint);
		RUN_TEST(testParamUnnamedPreparedStatement_usmallint);
		RUN_TEST(testParamUnnamedPreparedStatement_utinyint);
		RUN_TEST(testParamUnnamedPreparedStatement_tinyint);
		RUN_TEST(testParamUnnamedPreparedStatement_uinteger);
		RUN_TEST(testParamUnnamedPreparedStatement_timestamp_s);
		RUN_TEST(testParamUnnamedPreparedStatement_timestamp_ms);
		RUN_TEST(testParamUnnamedPreparedStatement_timestamp_ns);
		RUN_TEST(testParamUnnamedPreparedStatement_timestamp_tz);
		RUN_TEST(testParamUnnamedPreparedStatement_time_tz);

		/* more complex prepared statement scenarios */
		RUN_TEST(testExecutePreparedStatementMultipleTimesAndCheck);
		RUN_TEST(testPreparedStatementWithTenColumns);
		RUN_TEST(testUpdateDelete);
		RUN_TEST(testFloorFunctionWithPreparedStatement);
		RUN_TEST(testPQSendParams);
		RUN_TEST(executeCopyAndVerifyWithPQsendQueryParams);
		RUN_TEST(testNamedPreparedStatement);
		RUN_TEST(testWithLargeDataSize);
		RUN_TEST(testParseError);
		RUN_TEST(testBindError);
		RUN_TEST(testExecuteError);
		RUN_TEST(testExecuteSuccessError);
		RUN_TEST(testTransmitPrepared);

		/* tests are done, lets terminate pgduck_server and clean-up */
		cleanUpPgDuckServer();

	}

	return EXIT_SUCCESS;
}


static int
testTransmitPrepared()
{
	SETUP_CONNECTION(conn);

	int			nParams = 1;
	Oid			paramTypes[1] = {INT4OID};

	/* prepare a transmit statement */
	prepareOrExit(conn, "transmit select '$' from generate_series(1,$1::int)", nParams, paramTypes);

	/* execute the prepared statement */
	const char *paramValues[] = {"1000"};
	PGresult   *res = PQexecPrepared(conn, "", nParams, paramValues, NULL, NULL, 0);

	if (PQresultStatus(res) != PGRES_COPY_OUT)
		return EXIT_FAILURE;

	PQclear(res);

	/* count whether we get $1000 via transmit */
	int			bytesReceived = 0;
	int			dollarCount = 0;

	while (bytesReceived >= 0)
	{
		char	   *copyBuffer = NULL;

		bytesReceived = PQgetCopyData(conn, &copyBuffer, 0);

		for (int byteIndex = 0; byteIndex < bytesReceived; byteIndex++)
		{
			if (copyBuffer[byteIndex] == '$')
				dollarCount++;
		}
	}

	if (dollarCount != 1000)
		return EXIT_FAILURE;

	/* per the libpq docs, we should do a final result check */
	PGresult   *copyRes = PQgetResult(conn);

	if (PQresultStatus(copyRes) != PGRES_COMMAND_OK)
		return EXIT_FAILURE;

	PQclear(res);

	/* we should be able to use the same connection again */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT 1", "1");

	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;

}


static int
testExecuteSuccessError()
{
	SETUP_CONNECTION(conn);

	int			nParams = 1;
	Oid			paramTypes[1] = {TEXTOID};

	/* prepare a statement that can fail at execution time */
	prepareOrExit(conn, "SELECT $1::int / 10", nParams, paramTypes);

	/* execute the prepared statement with a valid value */
	const char *paramValues1[] = {"50"};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues1, NULL, NULL, "5");

	/* now induce a failure, abc cannot be cast to int */
	const char *paramValues2[] = {"abc"};
	PGresult   *res = PQexecPrepared(conn, "", nParams, paramValues2, NULL, NULL, 0);

	/* we expect a query failure (fatal refers to the query, not session) */
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		return EXIT_FAILURE;

	const char *errorMsg = PQerrorMessage(conn);

	if (strstr(errorMsg, "Conversion Error: Could not convert string 'abc' to INT32") == NULL)
		return EXIT_FAILURE;

	PQclear(res);

	/* make sure we can still use the prepared statement after failure */
	const char *paramValues3[] = {"100"};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues3, NULL, NULL, "10");

	/* we should be able to use the same connection again */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT 1", "1");

	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}


static int
testExecuteError()
{
	SETUP_CONNECTION(conn);

	int			nParams = 1;
	Oid			paramTypes[1] = {TEXTOID};

	/* prepare a statement that can fail at execution time */
	prepareOrExit(conn, "SELECT $1::int", nParams, paramTypes);

	const char *paramValues[] = {"abc"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};

	PGresult   *res = PQexecPrepared(conn, "", nParams, paramValues, paramLengths, paramFormats, 0);

	/* we expect a query failure (fatal refers to the query, not session) */
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		return EXIT_FAILURE;

	const char *errorMsg = PQerrorMessage(conn);

	if (strstr(errorMsg, "Conversion Error: Could not convert string 'abc' to INT32") == NULL)
		return EXIT_FAILURE;

	PQclear(res);

	/*
	 * prepare a statement which expects a number and pass in text
	 *
	 * while this gives a "Binder Error", it happens at execution time.
	 */
	nParams = 1;
	prepareOrExit(conn, "SELECT 3 + $1", nParams, paramTypes);

	/* now pass in text parameter */
	const char *textValue = "abc";

	res = PQexecPrepared(conn, "", nParams, &textValue, paramLengths, paramFormats, 0);

	/* we expect a query failure (fatal refers to the query, not session) */
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		return EXIT_FAILURE;

	errorMsg = PQerrorMessage(conn);

	if (strstr(errorMsg, "Binder Error: ") == NULL)
		return EXIT_FAILURE;

	PQclear(res);


	/* we should be able to use the same connection again */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT 1", "1");

	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}


static int
testBindError()
{
	SETUP_CONNECTION(conn);

	int			nParams = 1;
	Oid			paramTypes[1] = {INT4OID};

	/* prepare a statement with one parameter */
	const char *query = "SELECT $1 + generate_series FROM generate_series(1,10)";

	prepareOrExit(conn, query, nParams, paramTypes);

	const char *paramValues[] = {"4", "5"};
	const int	paramLengths[] = {0, 0};
	const int	paramFormats[] = {0, 0};

	/* now pass in 2 parameters */
	PGresult   *res = PQexecPrepared(conn, "", 2, paramValues, paramLengths, paramFormats, 0);

	/* we expect a query failure (fatal refers to the query, not session) */
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		return EXIT_FAILURE;

	const char *errorMsg = PQerrorMessage(conn);

	if (strstr(errorMsg, "Can not bind to parameter") == NULL)
		return EXIT_FAILURE;

	PQclear(res);

	/* prepare another statement with one parameter */
	query = "SELECT generate_series FROM generate_series(1,10) WHERE generate_series = $1";
	prepareOrExit(conn, query, nParams, paramTypes);

	/* now pass in 0 parameters */
	res = PQexecPrepared(conn, "", 0, NULL, NULL, NULL, 0);

	/* we expect a query failure (fatal refers to the query, not session) */
	if (PQresultStatus(res) != PGRES_FATAL_ERROR)
		return EXIT_FAILURE;

	errorMsg = PQerrorMessage(conn);

	if (strstr(errorMsg, "incorrect number of parameters") == NULL)
		return EXIT_FAILURE;

	PQclear(res);

	/* we should be able to use the same connection again */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT 1", "1");

	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}


static int
testParseError()
{
	SETUP_CONNECTION(conn);

	/* Attempt to prepare an invalid prepared statement */
	const char *query = "BLAH BLAH BLAH $1::text AS message";
	Oid			paramTypes[1] = {TEXTOID};

	PGresult   *prepareRes = PQprepare(conn, "", query, 1, paramTypes);

	const char *errorMsg = PQerrorMessage(conn);

	if (strstr(errorMsg, "syntax error at or near \"BLAH\"") == NULL)
		return EXIT_FAILURE;

	PQclear(prepareRes);

	/* Attempt to prepare a statement with a non-existent table */
	query = "SELECT * FROM notexists WHERE id = $1";
	prepareRes = PQprepare(conn, "", query, 1, paramTypes);

	errorMsg = PQerrorMessage(conn);

	if (strstr(errorMsg, "Catalog Error: Table with name notexists does not exist!") == NULL)
		return EXIT_FAILURE;

	PQclear(prepareRes);

	/* we should be able to use the same connection again */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT 1", "1");

	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}

static int
testWithLargeDataSize()
{
	SETUP_CONNECTION(conn);

	/*
	 * Step 1: Create a table and ingest 2048 * 5 rows, whereas 2048 is the
	 * default chunk size
	 */
	executeQueryOrExit(conn, "CREATE TABLE test_large_table(a INT)");
	executeQueryOrExit(conn, "INSERT INTO test_large_table SELECT generate_series FROM generate_series(0, 10240);");

	/* Step 2: Prepare the SELECT statement */
	Oid			paramTypes[1] = {INT4OID};

	prepareOrExit(conn, "SELECT * FROM test_large_table WHERE a > $1::int ORDER BY a ASC", 1, paramTypes);

	/*
	 * Step 3: Execute the prepared statement with different filters and check
	 * results
	 */
	const char *paramValues1[1] = {"0"};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, 1, paramValues1, NULL, NULL, "1");
	EXECUTE_PREPARED_AND_CHECK_LAST_TUPLE(conn, 1, paramValues1, NULL, NULL, "10240");

	const char *paramValues2[1] = {"10"};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, 1, paramValues2, NULL, NULL, "11");
	EXECUTE_PREPARED_AND_CHECK_LAST_TUPLE(conn, 1, paramValues2, NULL, NULL, "10240");

	const char *paramValues3[1] = {"50"};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, 1, paramValues3, NULL, NULL, "51");
	EXECUTE_PREPARED_AND_CHECK_LAST_TUPLE(conn, 1, paramValues3, NULL, NULL, "10240");

	const char *paramValues4[1] = {"90"};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, 1, paramValues4, NULL, NULL, "91");
	EXECUTE_PREPARED_AND_CHECK_LAST_TUPLE(conn, 1, paramValues4, NULL, NULL, "10240");

	const char *paramValues5[1] = {"99"};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, 1, paramValues5, NULL, NULL, "100");
	EXECUTE_PREPARED_AND_CHECK_LAST_TUPLE(conn, 1, paramValues5, NULL, NULL, "10240");


	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}

static int
testNamedPreparedStatement()
{
	SETUP_CONNECTION(conn);


	/* Attempt to prepare a named prepared statement */
	const char *stmtName = "my_named_stmt";
	const char *query = "SELECT $1::text AS message";
	Oid			paramTypes[1] = {TEXTOID};

	PGresult   *prepareRes = PQprepare(conn, stmtName, query, 1, paramTypes);

	/*
	 * Check if the error message indicates named prepared statements are not
	 * supported
	 */
	const char *errorMsg = PQerrorMessage(conn);

	perror(errorMsg);
	if (strstr(errorMsg, "named prepared statements not supported in pgduck_server") == NULL)
		return EXIT_FAILURE;

	PQclear(prepareRes);

	/* we should be able to use the same connection again */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT 1", "1");

	PQfinish(conn);
	return EXIT_SUCCESS;
}


static int
executeCopyAndVerifyWithPQsendQueryParams()
{
	SETUP_CONNECTION(conn);

	/* Execute COPY command to export data using PQsendQueryParams */
	const char *copyQuery = "COPY (SELECT s FROM generate_series(0, 100) s) TO '/tmp/out.parquet' WITH (FORMAT 'parquet');";

	if (!PQsendQueryParams(conn, copyQuery, 0, NULL, NULL, NULL, NULL, 0))
	{
		TEARDOWN_CONNECTION(conn);
		return EXIT_FAILURE;
	}

	/* Wait for and collect the COPY command result */
	PGresult   *copyRes = PQgetResult(conn);

	if (PQresultStatus(copyRes) != PGRES_TUPLES_OK)
	{

		PQclear(copyRes);
		TEARDOWN_CONNECTION(conn);
		return EXIT_FAILURE;
	}
	PQclear(copyRes);

	/* Execute verification query to count rows in the exported Parquet file */
	const char *verifyQuery = "SELECT count(*) FROM read_parquet('/tmp/out.parquet');";

	EXECUTE_QUERY_AND_CHECK_RESULT(conn, verifyQuery, "101");

	return EXIT_SUCCESS;
}



static int
testPQSendParams()
{
	SETUP_CONNECTION(conn);

	/* Define the query and parameters */
	const char *query = "SELECT $1::text as message WHERE $1 = $2;";
	const char *paramValues[2] = {"Hello, World!", "Hello, World!"};
	int			nParams = 2;
	const int	paramLengths[2] = {0, 0};	/* lengths array, not needed for
											 * text parameters */
	const int	paramFormats[2] = {0, 0};	/* text format for both parameters */

	/* Execute the query with parameters */
	int			sendStatus = PQsendQueryParams(conn, query, nParams, NULL, paramValues, paramLengths, paramFormats, 0);

	if (sendStatus == 0)
	{
		TEARDOWN_CONNECTION(conn);
		return EXIT_FAILURE;
	}

	/* Collect and check the result */
	PGresult   *res = PQgetResult(conn);

	if (PQresultStatus(res) != PGRES_TUPLES_OK || PQntuples(res) != 1)
	{
		PQclear(res);
		TEARDOWN_CONNECTION(conn);
		return EXIT_FAILURE;
	}

	const char *expectedResult = "Hello, World!";

	CHECK_QUERY_RESULT(res, expectedResult);

	PQclear(res);
	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}


static int
testExecutePreparedStatementMultipleTimesAndCheck()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE test_exec_check(value VARCHAR(255))");
	executeQueryOrExit(conn, "INSERT INTO test_exec_check SELECT generate_series FROM generate_series(1, 11);");

	Oid			paramTypes[1] = {TEXTOID};

	prepareOrExit(conn, "SELECT value || ' checked' FROM test_exec_check WHERE value = $1;", 1, paramTypes);

	const char *values[] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"};
	const char *expectedValues[] = {
		"1 checked", "2 checked", "3 checked", "4 checked", "5 checked",
		"6 checked", "7 checked", "8 checked", "9 checked", "10 checked"
	};

	/* execute the same statement 10 times */
	for (int i = 0; i < 10; i++)
	{
		const char *paramValues[1] = {values[i]};
		const int	paramLengths[] = {0};
		const int	paramFormats[] = {0};

		PGresult   *res;

		EXECUTE_PREPARED_STATEMENT(conn, 1, paramValues, paramLengths, paramFormats, res);

		/* Check the query result */
		CHECK_QUERY_RESULT(res, expectedValues[i]);

		/* Clear the result for the next iteration */
		CLEAR_RESULT(res);
	}

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testPreparedStatementWithTenColumns()
{
	SETUP_CONNECTION(conn);
	executeQueryOrExit(conn, "CREATE TABLE test_multi_column(col_int INT, "
					   "col_text VARCHAR(255), col_bool BOOLEAN,"
					   "col_float FLOAT, col_date DATE,"
					   "col_int2 INT, col_text2 VARCHAR(255),"
					   "col_bool2 BOOLEAN, col_float2 FLOAT,"
					   "col_date2 DATE);");


	Oid			paramTypes[10] = {INT4OID, TEXTOID, BOOLOID, FLOAT4OID, DATEOID, INT4OID, TEXTOID, BOOLOID, FLOAT4OID, DATEOID};

	prepareOrExit(conn,
				  "INSERT INTO test_multi_column(col_int, col_text, col_bool, col_float, col_date, col_int2, col_text2, col_bool2, col_float2, col_date2) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10);",
				  10, paramTypes);

	const char *paramValues1[10] = {"1", "First row", "true", "1.5", "2023-01-01", "2", "Second text", "false", "2.5", "2023-01-02"};
	const int	paramLengths1[10] = {0};
	const int	paramFormats1[10] = {0};

	const char *paramValues2[10] = {"3", "Third row", "true", "3.5", "2023-01-03", "4", "Fourth text", "false", "4.5", "2023-01-04"};
	const int	paramLengths2[10] = {0};
	const int	paramFormats2[10] = {0};

	const char *paramValues3[10] = {NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL};
	const int	paramLengths3[10] = {0};
	const int	paramFormats3[10] = {0};

	/* Execute the prepared statement twice with different parameters */
	PGresult   *res;

	EXECUTE_PREPARED_STATEMENT(conn, 10, paramValues1, paramLengths1, paramFormats1, res);
	CLEAR_RESULT(res);

	EXECUTE_PREPARED_STATEMENT(conn, 10, paramValues2, paramLengths2, paramFormats2, res);
	CLEAR_RESULT(res);

	EXECUTE_PREPARED_STATEMENT(conn, 10, paramValues3, paramLengths3, paramFormats3, res);
	CLEAR_RESULT(res);

	/*
	 * Prepare the SELECT statement with no parameters, as we're just
	 * retrieving all rows
	 */
	prepareOrExit(conn, "SELECT col_int, col_text, col_bool, col_float, col_date, col_int2, col_text2, col_bool2, col_float2, col_date2 FROM test_multi_column ORDER BY col_int NULLS LAST", 0, NULL);

	/* Execute the prepared statement with no parameters */
	PGresult   *selectRes;

	EXECUTE_PREPARED_STATEMENT(conn, 0, NULL, NULL, NULL, selectRes);
	int			rowIndex = 0;

	if (strcmp(PQgetvalue(selectRes, rowIndex, 0), "1") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 1), "First row") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 2), "t") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 3), "1.5") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 4), "2023-01-01") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 5), "2") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 6), "Second text") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 7), "f") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 8), "2.5") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 9), "2023-01-02") != 0)
	{
		PQclear(selectRes);
		TEARDOWN_CONNECTION(conn);
		return EXIT_FAILURE;
	}

	rowIndex = 1;
	if (strcmp(PQgetvalue(selectRes, rowIndex, 0), "3") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 1), "Third row") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 2), "t") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 3), "3.5") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 4), "2023-01-03") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 5), "4") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 6), "Fourth text") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 7), "f") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 8), "4.5") != 0 ||
		strcmp(PQgetvalue(selectRes, rowIndex, 9), "2023-01-04") != 0)
	{
		PQclear(selectRes);
		TEARDOWN_CONNECTION(conn);
		return EXIT_FAILURE;
	}

	rowIndex = 2;
	if (PQgetisnull(selectRes, rowIndex, 0) == 0 ||
		PQgetisnull(selectRes, rowIndex, 1) == 0 ||
		PQgetisnull(selectRes, rowIndex, 2) == 0 ||
		PQgetisnull(selectRes, rowIndex, 3) == 0 ||
		PQgetisnull(selectRes, rowIndex, 4) == 0 ||
		PQgetisnull(selectRes, rowIndex, 5) == 0 ||
		PQgetisnull(selectRes, rowIndex, 6) == 0 ||
		PQgetisnull(selectRes, rowIndex, 7) == 0 ||
		PQgetisnull(selectRes, rowIndex, 8) == 0 ||
		PQgetisnull(selectRes, rowIndex, 9) == 0)
	{
		PQclear(selectRes);
		TEARDOWN_CONNECTION(conn);
		return EXIT_FAILURE;
	}

	PQclear(selectRes);
	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testUpdateDelete()
{
	SETUP_CONNECTION(conn);
	PGresult   *res;

	/** Step 1: Create a table */
	executeQueryOrExit(conn, "CREATE TABLE test_timestamp(id INT PRIMARY KEY, ts TIMESTAMP)");
	Oid			paramTypes[1] = {TIMESTAMPOID};

	/** Step 2: Prepare and Execute INSERT */
	prepareOrExit(conn, "INSERT INTO test_timestamp(id, ts) VALUES (1, $1)", 1, paramTypes);
	const char *insertParamValues[1] = {"2023-10-05 15:00:00"};

	EXECUTE_PREPARED_STATEMENT(conn, 1, insertParamValues, NULL, NULL, res);
	CLEAR_RESULT(res);

	/** Verify INSERT */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT COUNT(*) FROM test_timestamp WHERE id = 1", "1")

	/** Step 3: Prepare and Execute UPDATE */
		prepareOrExit(conn, "UPDATE test_timestamp SET ts = $1 WHERE id = 1", 1, paramTypes);
	const char *updateParamValues[1] = {"2023-10-06 16:00:00"};

	EXECUTE_PREPARED_STATEMENT(conn, 1, updateParamValues, NULL, NULL, res);
	CLEAR_RESULT(res);

	/** Verify UPDATE */
	Oid			paramTypesInt[1] = {INT4OID};

	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT ts FROM test_timestamp WHERE id = 1", "2023-10-06 16:00:00")

	/** Step 4: Prepare and Execute DELETE */
		prepareOrExit(conn, "DELETE FROM test_timestamp WHERE id = $1", 1, paramTypesInt);
	const char *deleteParamValues[1] = {"1"};

	EXECUTE_PREPARED_STATEMENT(conn, 1, deleteParamValues, NULL, NULL, res)
		CLEAR_RESULT(res);

	/** Verify DELETE */
	EXECUTE_QUERY_AND_CHECK_RESULT(conn, "SELECT COUNT(*) FROM test_timestamp", "0");

	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}

static int
testFloorFunctionWithPreparedStatement()
{
	SETUP_CONNECTION(conn);

	/** Prepare the statement to call the floor function */
	Oid			paramTypes[1] = {FLOAT8OID};

	prepareOrExit(conn, "SELECT floor($1::DOUBLE)", 1, paramTypes);

	/** Define the parameter for the floor function and execute with check */
	const char *paramValues[1] = {"50.4"};
	const int	paramLengths[1] = {0};
	const int	paramFormats[1] = {0};

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, 1, paramValues, paramLengths, paramFormats, "50");

	TEARDOWN_CONNECTION(conn);
	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_boolean()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_boolean(a BOOLEAN)");
	executeQueryOrExit(conn, "INSERT INTO t_boolean VALUES (TRUE)");

	Oid			paramTypes[1] = {BOOLOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_boolean WHERE a = $1::bool;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"TRUE"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_bit()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_bit(a VARCHAR)");
	executeQueryOrExit(conn, "INSERT INTO t_bit VALUES ('1')");

	Oid			paramTypes[1] = {BITOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_bit WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"1"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_bytea()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_bytea(a BLOB)");
	executeQueryOrExit(conn, "INSERT INTO t_bytea VALUES ('\\xDEADBEEF')");

	Oid			paramTypes[1] = {BYTEAOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_bytea WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"\\xDEADBEEF"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_date()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_date(a DATE)");
	executeQueryOrExit(conn, "INSERT INTO t_date VALUES ('2022-01-01')");

	Oid			paramTypes[1] = {DATEOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_date WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"2022-01-01"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_float4()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_float4(a FLOAT)");
	executeQueryOrExit(conn, "INSERT INTO t_float4 VALUES (123.456)");

	Oid			paramTypes[1] = {FLOAT4OID};

	prepareOrExit(conn, "SELECT count(*) FROM t_float4 WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"123.456"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_float8()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_float8(a DOUBLE)");
	executeQueryOrExit(conn, "INSERT INTO t_float8 VALUES (12345678.12345678)");

	Oid			paramTypes[1] = {FLOAT8OID};

	prepareOrExit(conn, "SELECT count(*) FROM t_float8 WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"12345678.12345678"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}



static int
testParamUnnamedPreparedStatement_int2()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_int2(a SMALLINT)");
	executeQueryOrExit(conn, "INSERT INTO t_int2 VALUES (32767)");

	Oid			paramTypes[1] = {INT2OID};

	prepareOrExit(conn, "SELECT count(*) FROM t_int2 WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"32767"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_int4()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_int(a int)");
	executeQueryOrExit(conn, "INSERT INTO t_int VALUES (50)");

	Oid			paramTypes[1] = {INT4OID};

	prepareOrExit(conn, "SELECT count(*) FROM t_int WHERE a = $1;", 1, paramTypes);

	/* Parameter value to be passed to the prepared statement */
	int			nParams = 1;
	const char *paramValues[1] = {"50"};
	const int	paramLengths[] = {sizeof(paramValues[0])};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;

}

static int
testParamUnnamedPreparedStatement_int8()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_int8(a BIGINT)");
	executeQueryOrExit(conn, "INSERT INTO t_int8 VALUES (9223372036854775807)");

	Oid			paramTypes[1] = {INT8OID};

	prepareOrExit(conn, "SELECT count(*) FROM t_int8 WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"9223372036854775807"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_interval()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_interval(a INTERVAL)");
	executeQueryOrExit(conn, "INSERT INTO t_interval VALUES ('1 year 2 months 3 days 04:05:06')");

	Oid			paramTypes[1] = {INTERVALOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_interval WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"1 year 2 months 3 days 04:05:06"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_numeric()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_numeric(a DECIMAL)");
	executeQueryOrExit(conn, "INSERT INTO t_numeric VALUES (123456789.123456789)");

	Oid			paramTypes[1] = {NUMERICOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_numeric WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"123456789.123456789"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_oid()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_oid(a INTEGER)");
	executeQueryOrExit(conn, "INSERT INTO t_oid VALUES (10000)");

	Oid			paramTypes[1] = {OIDOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_oid WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"10000"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_timestamp()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_timestamp(a TIMESTAMP)");
	executeQueryOrExit(conn, "INSERT INTO t_timestamp VALUES ('2022-01-01 12:00:00')");

	Oid			paramTypes[1] = {TIMESTAMPOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_timestamp WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"2022-01-01 12:00:00"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_timestamptz()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_timestamptz(a TIMESTAMP)");
	executeQueryOrExit(conn, "INSERT INTO t_timestamptz VALUES ('2022-01-01 12:00:00+00')");

	Oid			paramTypes[1] = {TIMESTAMPTZOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_timestamptz WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"2022-01-01 12:00:00+00"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_time()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_time(a TIME)");
	executeQueryOrExit(conn, "INSERT INTO t_time VALUES ('12:00:00')");

	Oid			paramTypes[1] = {TIMEOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_time WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"12:00:00"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_uuid()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_uuid(a UUID)");
	executeQueryOrExit(conn, "INSERT INTO t_uuid VALUES ('123e4567-e89b-12d3-a456-426614174000')");

	Oid			paramTypes[1] = {UUIDOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_uuid WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"123e4567-e89b-12d3-a456-426614174000"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_varchar()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_varchar(a VARCHAR)");
	executeQueryOrExit(conn, "INSERT INTO t_varchar VALUES ('test varchar')");

	Oid			paramTypes[1] = {VARCHAROID};

	prepareOrExit(conn, "SELECT count(*) FROM t_varchar WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"test varchar"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}




static int
testParamUnnamedPreparedStatement_tinyint()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_tinyint(a TINYINT)");
	executeQueryOrExit(conn, "INSERT INTO t_tinyint VALUES (127)");

	Oid			paramTypes_tinyint[1] = {INT2OID};

	/* Using SMALLINT as closest equivalent */
	prepareOrExit(conn, "SELECT count(*) FROM t_tinyint WHERE a = $1;", 1, paramTypes_tinyint);

	/*
	 * Assuming a way to execute a statement that doesn't expect a direct
	 * PostgreSQL type equivalent
	 */
	int			nParams = 1;
	const char *paramValues[1] = {"127"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_utinyint()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_utinyint(a UTINYINT)");
	executeQueryOrExit(conn, "INSERT INTO t_utinyint VALUES (255)");

	Oid			paramTypes_utinyint[1] = {INT2OID};

	/*
	 * Using SMALLINT as closest equivalent, acknowledging the lack of
	 * unsigned types in PostgreSQL
	 */
	prepareOrExit(conn, "SELECT count(*) FROM t_utinyint WHERE a = $1;", 1, paramTypes_utinyint);


	int			nParams = 1;
	const char *paramValues[1] = {"255"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_usmallint()
{
	SETUP_CONNECTION(conn);
	executeQueryOrExit(conn, "CREATE TABLE t_usmallint(a USMALLINT)");
	executeQueryOrExit(conn, "INSERT INTO t_usmallint VALUES (65535)");

	Oid			paramTypes_usmallint[1] = {INT4OID};

	/* Using INTEGER as closest equivalent */
	prepareOrExit(conn, "SELECT count(*) FROM t_usmallint WHERE a = $1;", 1, paramTypes_usmallint);

	int			nParams = 1;
	const char *paramValues[1] = {"65535"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_uinteger()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_uinteger(a UINTEGER)");
	executeQueryOrExit(conn, "INSERT INTO t_uinteger VALUES (4294967295)");

	Oid			paramTypes_uinteger[1] = {INT4OID};

	prepareOrExit(conn, "SELECT count(*) FROM t_uinteger WHERE a = $1;", 1, paramTypes_uinteger);

	int			nParams = 1;
	const char *paramValues[1] = {"4294967295"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_ubigint()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_ubigint(a UBIGINT)");
	executeQueryOrExit(conn, "INSERT INTO t_ubigint VALUES (18446744073709551615)");

	Oid			paramTypes_ubigint[1] = {INT8OID};

	prepareOrExit(conn, "SELECT count(*) FROM t_ubigint WHERE a = $1;", 1, paramTypes_ubigint);

	int			nParams = 1;
	const char *paramValues[1] = {"18446744073709551615"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_timestamp_s()
{
	SETUP_CONNECTION(conn);
	executeQueryOrExit(conn, "CREATE TABLE t_timestamp_s(a TIMESTAMP_S)");
	executeQueryOrExit(conn, "INSERT INTO t_timestamp_s VALUES ('2022-01-01 12:00:00')");

	Oid			paramTypes_timestamp_s[1] = {TIMESTAMPOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_timestamp_s WHERE a = $1;", 1, paramTypes_timestamp_s);

	int			nParams = 1;
	const char *paramValues[1] = {"2022-01-01 12:00:00"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_timestamp_ms()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_timestamp_ms(a TIMESTAMP_MS)");
	executeQueryOrExit(conn, "INSERT INTO t_timestamp_ms VALUES ('2022-01-01 12:00:00.123')");

	Oid			paramTypes_timestamp_ms[1] = {TIMESTAMPOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_timestamp_ms WHERE a = $1;", 1, paramTypes_timestamp_ms);

	int			nParams = 1;
	const char *paramValues[1] = {"2022-01-01 12:00:00.123"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}



static int
testParamUnnamedPreparedStatement_timestamp_ns()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_timestamp_ns(a TIMESTAMP_NS)");
	executeQueryOrExit(conn, "INSERT INTO t_timestamp_ns VALUES ('2022-01-01 12:00:00.123456789')");

	Oid			paramTypes_timestamp_ns[1] = {TIMESTAMPOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_timestamp_ns WHERE a = $1;", 1, paramTypes_timestamp_ns);

	int			nParams = 1;
	const char *paramValues[1] = {"2022-01-01 12:00:00.123456789"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_time_tz()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_time_tz(a TIMETZ)");
	executeQueryOrExit(conn, "INSERT INTO t_time_tz VALUES ('12:00:00+01')");

	Oid			paramTypes_time_tz[1] = {TIMETZOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_time_tz WHERE a = $1;", 1, paramTypes_time_tz);


	int			nParams = 1;
	const char *paramValues[1] = {"12:00:00+01"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}

static int
testParamUnnamedPreparedStatement_timestamp_tz()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_timestamp_tz(a TIMESTAMPTZ)");
	executeQueryOrExit(conn, "INSERT INTO t_timestamp_tz VALUES ('2022-01-01 12:00:00+01')");

	Oid			paramTypes_timestamp_tz[1] = {TIMESTAMPTZOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_timestamp_tz WHERE a = $1;", 1, paramTypes_timestamp_tz);

	int			nParams = 1;
	const char *paramValues[1] = {"2022-01-01 12:00:00+01"};
	const int	paramLengths[] = {0};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static int
testParamUnnamedPreparedStatement_uhugeint()
{
	SETUP_CONNECTION(conn);

	executeQueryOrExit(conn, "CREATE TABLE t_uhugeint2(a uhugeint)");
	executeQueryOrExit(conn, "INSERT INTO t_uhugeint2 VALUES (34028236692093846346337460743176821145)");

	Oid			paramTypes[1] = {NUMERICOID};

	prepareOrExit(conn, "SELECT count(*) FROM t_uhugeint2 WHERE a = $1;", 1, paramTypes);

	int			nParams = 1;
	const char *paramValues[1] = {"34028236692093846346337460743176821145"};
	const int	paramLengths[] = {sizeof(paramValues[0])};
	const int	paramFormats[] = {0};
	const char *expectedResult = "1";

	EXECUTE_PREPARED_AND_CHECK_FIRST_TUPLE(conn, nParams, paramValues, paramLengths, paramFormats, expectedResult);

	TEARDOWN_CONNECTION(conn);

	return EXIT_SUCCESS;
}


static void
executeQueryOrExit(PGconn *conn, const char *query)
{
	PGresult   *res = PQexec(conn, query);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		PQfinish(conn);
		exit(EXIT_FAILURE);
	}
	PQclear(res);
}

/* always prepare unnamed */
static void
prepareOrExit(PGconn *conn, const char *query, int nParams, const Oid *paramTypes)
{
	PGresult   *res = PQprepare(conn, "", query, nParams, paramTypes);

	if (PQresultStatus(res) != PGRES_COMMAND_OK)
	{
		PQclear(res);
		PQfinish(conn);
		exit(EXIT_FAILURE);
	}
	PQclear(res);
}


static void
cleanUpPgDuckServer()
{
	kill(pgduck_pid, SIGTERM);
	waitpid(pgduck_pid, NULL, 0);
	remove("/tmp/duckdb_c_test");
	remove("/tmp/duckdb_c_test.wal");
}

static void
addCurrentDirectoryToPath()
{
	/* get the current working directory */
	char	   *cwd = getcwd(NULL, 0);

	/* If the current working directory is obtained successfully */
	if (cwd != NULL)
	{
		/* Get the current PATH environment variable */
		char	   *path = getenv("PATH");

		/* If PATH is not found or empty, set it to the current directory */
		if (path == NULL || path[0] == '\0')
			setenv("PATH", cwd, 1);
		else
		{
			/* Calculate the length of the concatenated string */
			/* +2 for ':' and '\0' */
			size_t		newPathLen = strlen(cwd) + strlen(path) + 2;

			/* Allocate memory for the new PATH */
			char	   *newPath = (char *) malloc(newPathLen);

			if (newPath != NULL)
			{
				/* Copy the current directory to the new PATH */
				strcpy(newPath, cwd);

				/* Append ':' character */
				newPath[strlen(cwd)] = ':';

				/* Copy the existing PATH to the new PATH after ':' */
				strcpy(newPath + strlen(cwd) + 1, path);

				/* Set the new PATH environment variable */
				setenv("PATH", newPath, 1);

				/* Free the memory allocated for the new PATH */
				free(newPath);
			}
		}

		free(cwd);
	}
}
