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
 * sql_api.c
 *
 * Statement execution over the Snowflake SQL API v2.
 *
 * A statement is one POST to /api/v2/statements. Snowflake answers with the
 * result set inline when it finishes quickly, and with 202 plus a statement
 * handle when it does not, in which case the handle is polled until the result
 * is ready. Large result sets are split into partitions and only the first one
 * comes with the answer, so the rest are fetched as the cursor reaches them.
 *
 * Every statement that has not finished remotely is registered here, and the
 * transaction callback cancels whatever is left when the transaction ends. That
 * matters more than it would for a local scan: an abandoned statement keeps a
 * warehouse busy and the customer pays for it.
 */

#include "postgres.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"

#include "pg_lake/http/http_client.h"
#include "pg_lake/json/json_utils.h"

#include "pg_lake_snowflake/auth.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"
#include "pg_lake_snowflake/sql_api.h"

/* how often a running statement is polled, and how long a poll waits */
#define SNOWFLAKE_POLL_BASE_MS 200

/* how many times a request is retried when the service asks us to back off */
#define SNOWFLAKE_MAX_RETRIES 5

/* the statement is still running and has to be polled for */
#define HTTP_STATUS_STATEMENT_RUNNING 202

/* statements that may still be running remotely, in TopMemoryContext */
static List *SnowflakeRunningStatements = NIL;
static bool SnowflakeXactCallbackRegistered = false;

/* whether this transaction has already been told that its writes will stand */
static bool SnowflakeWarnedAboutTransactionBlock = false;

static void SnowflakeXactCallback(XactEvent event, void *arg);
static void RegisterRunningStatement(SnowflakeStatement * statement);
static void ForgetRunningStatement(SnowflakeStatement * statement);
static void CancelStatement(SnowflakeStatement * statement);

static char *BuildStatementRequestBody(SnowflakeConnection * connection, const char *sql);
static Jsonb *SendApiRequest(SnowflakeConnection * connection, HttpMethod method,
							 const char *url, const char *body, const char *sql,
							 MemoryContext resultContext, long *statusCode);
static void PollUntilComplete(SnowflakeStatement * statement, Jsonb **response);
static void ReadResultSetMetadata(SnowflakeStatement * statement, JsonbContainer *response);
static void ReadPartitionData(SnowflakeStatement * statement, Jsonb *response);
static void FetchPartition(SnowflakeStatement * statement, int partitionIndex);
static void ReportApiError(SnowflakeConnection * connection, HttpResult * result,
						   const char *sql);
static bool ShouldRetryStatus(long statusCode);

static SnowflakeConnection * CopyConnection(SnowflakeConnection * connection);
static JsonbValue *FindJsonField(JsonbContainer *container, const char *fieldName);
static char *JsonFieldAsCString(JsonbContainer *container, const char *fieldName);
static int64 JsonFieldAsInt64(JsonbContainer *container, const char *fieldName,
							  int64 defaultValue);
static bool JsonFieldAsBool(JsonbContainer *container, const char *fieldName,
							bool defaultValue);
static char *JsonbValueAsCString(JsonbValue *value);
static char *GenerateRequestId(void);


/*
 * SnowflakeExecute submits a statement and returns a cursor over its result.
 */
SnowflakeStatement *
SnowflakeExecute(SnowflakeConnection * connection, const char *sql)
{
	MemoryContext statementContext = AllocSetContextCreate(TopMemoryContext,
														   "pg_lake_snowflake statement",
														   ALLOCSET_DEFAULT_SIZES);
	MemoryContext previousContext = MemoryContextSwitchTo(statementContext);
	SnowflakeStatement *statement = palloc0(sizeof(SnowflakeStatement));

	statement->connection = CopyConnection(connection);
	statement->sql = pstrdup(sql);
	statement->statementContext = statementContext;
	statement->partitionContext = AllocSetContextCreate(statementContext,
														"pg_lake_snowflake partition",
														ALLOCSET_DEFAULT_SIZES);
	statement->currentPartition = -1;
	statement->currentRowInPartition = -1;

	MemoryContextSwitchTo(previousContext);

	if (SnowflakeLogRemoteSql)
		ereport(LOG, (errmsg("pg_lake_snowflake: %s", sql)));

	/*
	 * The request id makes a retry of the submission idempotent: Snowflake
	 * recognises the repeat and returns the same statement rather than
	 * running it twice.
	 */
	char	   *url = psprintf("%s/api/v2/statements?requestId=%s",
							   connection->accountUrl, GenerateRequestId());
	char	   *body = BuildStatementRequestBody(connection, sql);
	long		statusCode = 0;

	/*
	 * The statement is registered before it is submitted, and errors below
	 * are deliberately not caught here: the transaction callback cancels and
	 * releases whatever is left over, which keeps cancellation out of an
	 * error path that is about to re-throw.
	 */
	RegisterRunningStatement(statement);

	Jsonb	   *response = SendApiRequest(connection, HTTP_POST, url, body, sql,
										  statement->partitionContext, &statusCode);
	char	   *handle = JsonFieldAsCString(&response->root, "statementHandle");

	if (handle != NULL)
		statement->handle = MemoryContextStrdup(statement->statementContext, handle);

	if (statusCode == HTTP_STATUS_STATEMENT_RUNNING)
		PollUntilComplete(statement, &response);

	statement->completed = true;

	ReadResultSetMetadata(statement, &response->root);
	ReadPartitionData(statement, response);
	statement->currentPartition = 0;

	return statement;
}


/*
 * GenerateRequestId returns a random UUID for the requestId of a submission.
 */
static char *
GenerateRequestId(void)
{
	uint8		randomBytes[16];

	if (!pg_strong_random(randomBytes, sizeof(randomBytes)))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not generate a request id")));
	}

	/* version 4, variant 1, as a UUID of this shape is expected */
	randomBytes[6] = (randomBytes[6] & 0x0f) | 0x40;
	randomBytes[8] = (randomBytes[8] & 0x3f) | 0x80;

	StringInfo	requestId = makeStringInfo();

	for (int byteIndex = 0; byteIndex < 16; byteIndex++)
	{
		if (byteIndex == 4 || byteIndex == 6 || byteIndex == 8 || byteIndex == 10)
			appendStringInfoChar(requestId, '-');

		appendStringInfo(requestId, "%02x", randomBytes[byteIndex]);
	}

	return requestId->data;
}


/*
 * BuildStatementRequestBody builds the JSON body of a statement submission.
 */
static char *
BuildStatementRequestBody(SnowflakeConnection * connection, const char *sql)
{
	StringInfo	body = makeStringInfo();

	appendStringInfoChar(body, '{');
	appendJsonString(body, "statement", sql);
	appendStringInfoChar(body, ',');
	appendJsonInt32(body, "timeout", connection->statementTimeoutSeconds);

	if (connection->database != NULL)
	{
		appendStringInfoChar(body, ',');
		appendJsonString(body, "database", connection->database);
	}

	if (connection->schemaName != NULL)
	{
		appendStringInfoChar(body, ',');
		appendJsonString(body, "schema", connection->schemaName);
	}

	if (connection->warehouse != NULL)
	{
		appendStringInfoChar(body, ',');
		appendJsonString(body, "warehouse", connection->warehouse);
	}

	if (connection->role != NULL)
	{
		appendStringInfoChar(body, ',');
		appendJsonString(body, "role", connection->role);
	}

	/*
	 * BINARY_OUTPUT_FORMAT decides how a binary column is encoded in the
	 * result, and the account default is not ours to assume, so it is pinned
	 * to the encoding the value conversion expects.
	 */
	appendStringInfoString(body, ",\"parameters\":{");
	appendJsonString(body, "BINARY_OUTPUT_FORMAT", "HEX");
	appendStringInfoChar(body, ',');
	appendJsonString(body, "QUERY_TAG", "pg_lake_snowflake");
	appendStringInfoChar(body, '}');

	appendStringInfoChar(body, '}');

	return body->data;
}


/*
 * SendApiRequest performs one SQL API request and returns its parsed response.
 * The response is parsed in resultContext, which is where the rows of a result
 * partition end up.
 */
static Jsonb *
SendApiRequest(SnowflakeConnection * connection, HttpMethod method, const char *url,
			   const char *body, const char *sql, MemoryContext resultContext,
			   long *statusCode)
{
	MemoryContext requestContext = AllocSetContextCreate(CurrentMemoryContext,
														 "pg_lake_snowflake request",
														 ALLOCSET_DEFAULT_SIZES);

	/* volatile because it is set inside PG_TRY and read after it */
	Jsonb	   *volatile response = NULL;

	PG_TRY();
	{
		HttpResult	result = {0};
		bool		succeeded = false;

		for (int attempt = 1; attempt <= SNOWFLAKE_MAX_RETRIES; attempt++)
		{
			MemoryContext previousContext = MemoryContextSwitchTo(requestContext);
			List	   *headers = SnowflakeRequestHeaders(connection, body != NULL);

			CHECK_FOR_INTERRUPTS();

			switch (method)
			{
				case HTTP_POST:
					result = HttpPost(url, body, headers);
					break;
				case HTTP_GET:
					result = HttpGet(url, headers);
					break;
				default:
					elog(ERROR, "unsupported HTTP method for the Snowflake SQL API");
			}

			MemoryContextSwitchTo(previousContext);

			if (result.status == HTTP_STATUS_UNAUTHORIZED && attempt == 1)
			{
				/*
				 * A rejected token may be one we minted and still consider
				 * valid, so drop it and try once with a fresh one.
				 */
				SnowflakeForgetCachedToken(connection);
				continue;
			}

			if (ShouldRetryStatus(result.status) && attempt < SNOWFLAKE_MAX_RETRIES)
			{
				SnowflakeSleepMs(LinearBackoffSleepMs(500, attempt));
				continue;
			}

			succeeded = true;
			break;
		}

		if (!succeeded || result.body == NULL || result.body[0] != '{')
			ReportApiError(connection, &result, sql);

		if (result.status != 200 && result.status != HTTP_STATUS_STATEMENT_RUNNING)
			ReportApiError(connection, &result, sql);

		*statusCode = result.status;

		MemoryContext previousContext = MemoryContextSwitchTo(resultContext);

		response = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
													  CStringGetDatum(result.body)));

		MemoryContextSwitchTo(previousContext);
	}
	PG_FINALLY();
	{
		MemoryContextDelete(requestContext);
	}
	PG_END_TRY();

	return response;
}


/*
 * ShouldRetryStatus returns whether a status means "come back later" rather
 * than "this request was wrong".
 */
static bool
ShouldRetryStatus(long statusCode)
{
	return statusCode == HTTP_STATUS_TOO_MANY_REQUESTS ||
		statusCode == HTTP_STATUS_SERVICE_UNAVAILABLE ||
		statusCode == 502 ||
		statusCode == 504;
}


/*
 * PollUntilComplete polls a statement that Snowflake did not finish within the
 * submission request, and replaces the response with the completed one.
 */
static void
PollUntilComplete(SnowflakeStatement * statement, Jsonb **response)
{
	char	   *statusUrl = JsonFieldAsCString(&(*response)->root, "statementStatusUrl");

	if (statusUrl == NULL)
	{
		statusUrl = psprintf("/api/v2/statements/%s", statement->handle);
	}

	char	   *pollUrl = psprintf("%s%s", statement->connection->accountUrl, statusUrl);
	long		statusCode = HTTP_STATUS_STATEMENT_RUNNING;

	for (int attempt = 1; statusCode == HTTP_STATUS_STATEMENT_RUNNING; attempt++)
	{
		SnowflakeSleepMs(LinearBackoffSleepMs(SNOWFLAKE_POLL_BASE_MS, attempt));

		MemoryContextReset(statement->partitionContext);

		*response = SendApiRequest(statement->connection, HTTP_GET, pollUrl, NULL,
								   statement->sql, statement->partitionContext,
								   &statusCode);
	}
}


/*
 * ReadResultSetMetadata reads the column list and the partitioning of the
 * result set out of a statement response.
 */
static void
ReadResultSetMetadata(SnowflakeStatement * statement, JsonbContainer *response)
{
	JsonbValue *statistics = FindJsonField(response, "stats");

	if (statistics != NULL && statistics->type == jbvBinary)
	{
		JsonbContainer *statisticsContainer = statistics->val.binary.data;

		statement->affectedRowCount =
			JsonFieldAsInt64(statisticsContainer, "numRowsInserted", 0) +
			JsonFieldAsInt64(statisticsContainer, "numRowsUpdated", 0) +
			JsonFieldAsInt64(statisticsContainer, "numRowsDeleted", 0);
	}

	JsonbValue *metadata = FindJsonField(response, "resultSetMetaData");

	if (metadata == NULL || metadata->type != jbvBinary)
	{
		/*
		 * Statements that produce no result set at all do not describe one.
		 * There is nothing to iterate over, which the cursor reports as an
		 * empty result.
		 */
		statement->columnCount = 0;
		statement->partitionCount = 0;
		return;
	}

	JsonbContainer *metadataContainer = metadata->val.binary.data;

	statement->totalRowCount = JsonFieldAsInt64(metadataContainer, "numRows", 0);

	JsonbValue *partitionInfo = FindJsonField(metadataContainer, "partitionInfo");

	if (partitionInfo != NULL && partitionInfo->type == jbvBinary)
		statement->partitionCount = JsonContainerSize(partitionInfo->val.binary.data);
	else
		statement->partitionCount = 1;

	JsonbValue *rowType = FindJsonField(metadataContainer, "rowType");

	if (rowType == NULL || rowType->type != jbvBinary)
	{
		statement->columnCount = 0;
		return;
	}

	JsonbContainer *rowTypeContainer = rowType->val.binary.data;
	int			columnCount = JsonContainerSize(rowTypeContainer);
	MemoryContext previousContext = MemoryContextSwitchTo(statement->statementContext);

	statement->columnCount = columnCount;
	statement->columns = palloc0(sizeof(SnowflakeResultColumn) * Max(columnCount, 1));

	for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
	{
		JsonbValue *columnValue = getIthJsonbValueFromContainer(rowTypeContainer,
																columnIndex);

		if (columnValue == NULL || columnValue->type != jbvBinary)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE_DESCRIPTORS),
					 errmsg("unexpected column description in the Snowflake response")));
		}

		JsonbContainer *columnContainer = columnValue->val.binary.data;
		SnowflakeResultColumn *column = &statement->columns[columnIndex];

		column->name = JsonFieldAsCString(columnContainer, "name");
		column->typeName = JsonFieldAsCString(columnContainer, "type");
		column->typeCode = SnowflakeTypeCodeFromName(column->typeName);
		column->precision = (int32) JsonFieldAsInt64(columnContainer, "precision", -1);
		column->scale = (int32) JsonFieldAsInt64(columnContainer, "scale", -1);
		column->length = JsonFieldAsInt64(columnContainer, "length", -1);
		column->nullable = JsonFieldAsBool(columnContainer, "nullable", true);
	}

	MemoryContextSwitchTo(previousContext);
}


/*
 * ReadPartitionData points the cursor at the rows in a response.
 */
static void
ReadPartitionData(SnowflakeStatement * statement, Jsonb *response)
{
	MemoryContext previousContext = MemoryContextSwitchTo(statement->partitionContext);

	statement->partitionData = FindJsonField(&response->root, "data");

	MemoryContextSwitchTo(previousContext);

	if (statement->partitionData == NULL || statement->partitionData->type != jbvBinary)
	{
		statement->partitionData = NULL;
		statement->rowCountInPartition = 0;
	}
	else
	{
		statement->rowCountInPartition =
			JsonContainerSize(statement->partitionData->val.binary.data);
	}

	statement->currentRowInPartition = -1;
}


/*
 * FetchPartition fetches one partition of an already completed result set.
 */
static void
FetchPartition(SnowflakeStatement * statement, int partitionIndex)
{
	char	   *url = psprintf("%s/api/v2/statements/%s?partition=%d",
							   statement->connection->accountUrl,
							   statement->handle,
							   partitionIndex);
	long		statusCode = 0;

	MemoryContextReset(statement->partitionContext);
	statement->partitionData = NULL;

	Jsonb	   *response = SendApiRequest(statement->connection, HTTP_GET, url, NULL,
										  statement->sql, statement->partitionContext,
										  &statusCode);

	ReadPartitionData(statement, response);
	statement->currentPartition = partitionIndex;
}


/*
 * SnowflakeStatementNextRow advances the cursor to the next row, fetching the
 * next partition when the current one runs out. It returns false at the end of
 * the result set.
 */
bool
SnowflakeStatementNextRow(SnowflakeStatement * statement)
{
	statement->currentRowInPartition++;

	while (statement->currentRowInPartition >= statement->rowCountInPartition)
	{
		if (statement->currentPartition + 1 >= statement->partitionCount)
			return false;

		FetchPartition(statement, statement->currentPartition + 1);
		statement->currentRowInPartition = 0;
	}

	return true;
}


/*
 * SnowflakeStatementGetValue returns the value of one column of the current
 * row, or NULL for a SQL NULL. The result is allocated in the current memory
 * context.
 */
char *
SnowflakeStatementGetValue(SnowflakeStatement * statement, int columnIndex)
{
	if (statement->partitionData == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_HANDLE),
				 errmsg("no Snowflake result row is available")));
	}

	JsonbValue *rowValue =
		getIthJsonbValueFromContainer(statement->partitionData->val.binary.data,
									  statement->currentRowInPartition);

	if (rowValue == NULL || rowValue->type != jbvBinary)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INCONSISTENT_DESCRIPTOR_INFORMATION),
				 errmsg("unexpected row in the Snowflake response")));
	}

	JsonbValue *cellValue = getIthJsonbValueFromContainer(rowValue->val.binary.data,
														  columnIndex);

	if (cellValue == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_COLUMN_NUMBER),
				 errmsg("Snowflake returned a row with fewer columns than expected"),
				 errdetail("Column %d was requested.", columnIndex + 1)));
	}

	return JsonbValueAsCString(cellValue);
}


/*
 * SnowflakeStatementClose releases a statement, cancelling it first when it may
 * still be running remotely.
 */
void
SnowflakeStatementClose(SnowflakeStatement * statement)
{
	if (statement == NULL)
		return;

	/*
	 * A statement can only be cancelled once Snowflake has told us its
	 * handle, which for a statement that is still inside its submission
	 * request it has not. Snowflake holds a submission open for up to about
	 * 45 seconds, so that window is exactly the one where a statement is
	 * short enough for the request timeout to end it anyway.
	 */
	if (!statement->completed && statement->handle != NULL)
		CancelStatement(statement);

	ForgetRunningStatement(statement);

	MemoryContext statementContext = statement->statementContext;

	if (statementContext != NULL)
		MemoryContextDelete(statementContext);
}


/*
 * SnowflakeExecuteScalar runs a statement and returns the first column of the
 * first row, or NULL when the statement produced no rows.
 */
char *
SnowflakeExecuteScalar(SnowflakeConnection * connection, const char *sql)
{
	SnowflakeStatement *statement = SnowflakeExecute(connection, sql);
	char	   *value = NULL;

	PG_TRY();
	{
		if (statement->columnCount > 0 && SnowflakeStatementNextRow(statement))
			value = SnowflakeStatementGetValue(statement, 0);
	}
	PG_FINALLY();
	{
		SnowflakeStatementClose(statement);
	}
	PG_END_TRY();

	return value;
}


/*
 * CancelStatement asks Snowflake to stop working on a statement. Failures are
 * reported as warnings: the caller is on its way out, and an abandoned
 * statement is not worth failing a rollback over.
 */
static void
CancelStatement(SnowflakeStatement * statement)
{
	char	   *url = psprintf("%s/api/v2/statements/%s/cancel",
							   statement->connection->accountUrl,
							   statement->handle);

	/* one attempt is enough, whether or not it works */
	statement->completed = true;

	/* a backend that is going away should not be held up by a courtesy */
	if (ProcDiePending)
		return;

	/*
	 * The usual reason to cancel is that the query was cancelled, and the
	 * HTTP client abandons a transfer while a cancellation is pending, so the
	 * request that carries the cancellation out would be the first thing it
	 * aborted.
	 */
	bool		savedQueryCancelPending = QueryCancelPending;

	QueryCancelPending = false;

	PG_TRY();
	{
		List	   *headers = SnowflakeRequestHeaders(statement->connection, true);
		HttpResult	result = HttpPost(url, "{}", headers);

		if (result.status != 200 && result.status != 404 && result.status != 422)
		{
			ereport(WARNING,
					(errmsg("could not cancel Snowflake statement %s",
							statement->handle),
					 errdetail("The account answered HTTP %ld.", result.status)));
		}
	}
	PG_CATCH();
	{
		/*
		 * This runs while a transaction is ending, where the error that got
		 * us here has already been reported, so the failure is downgraded
		 * rather than allowed to replace it.
		 */
		FlushErrorState();
		ereport(WARNING,
				(errmsg("could not cancel Snowflake statement %s",
						statement->handle)));
	}
	PG_END_TRY();

	QueryCancelPending = savedQueryCancelPending;
}


/*
 * RegisterRunningStatement remembers a statement so that the transaction
 * callback can cancel it if the scan never finishes.
 */
static void
RegisterRunningStatement(SnowflakeStatement * statement)
{
	if (!SnowflakeXactCallbackRegistered)
	{
		RegisterXactCallback(SnowflakeXactCallback, NULL);
		SnowflakeXactCallbackRegistered = true;
	}

	MemoryContext previousContext = MemoryContextSwitchTo(TopMemoryContext);

	SnowflakeRunningStatements = lappend(SnowflakeRunningStatements, statement);

	MemoryContextSwitchTo(previousContext);
}


/*
 * ForgetRunningStatement removes a statement from the registry.
 */
static void
ForgetRunningStatement(SnowflakeStatement * statement)
{
	SnowflakeRunningStatements = list_delete_ptr(SnowflakeRunningStatements, statement);
}


/*
 * SnowflakeXactCallback cancels and releases every statement that is still
 * around at the end of a transaction.
 */
static void
SnowflakeXactCallback(XactEvent event, void *arg)
{
	if (event != XACT_EVENT_COMMIT && event != XACT_EVENT_ABORT &&
		event != XACT_EVENT_PREPARE)
		return;

	while (SnowflakeRunningStatements != NIL)
	{
		SnowflakeStatement *statement = linitial(SnowflakeRunningStatements);

		/* SnowflakeStatementClose removes the statement from the list */
		SnowflakeStatementClose(statement);
	}

	SnowflakeWarnedAboutTransactionBlock = false;
}


/*
 * SnowflakeWarnIfWriteIsNotTransactional warns that a write inside a transaction
 * block is not part of that transaction.
 *
 * The SQL API has no session that spans requests, so each statement commits on
 * its own and a ROLLBACK afterwards leaves it in place. Outside a transaction
 * block there is nothing to mislead anyone, so the warning is only for the case
 * where the surrounding block suggests otherwise, and only once for it.
 */
void
SnowflakeWarnIfWriteIsNotTransactional(void)
{
	if (!SnowflakeWarnOnWriteInTransactionBlock)
		return;

	if (SnowflakeWarnedAboutTransactionBlock || !IsTransactionBlock())
		return;

	SnowflakeWarnedAboutTransactionBlock = true;

	ereport(WARNING,
			(errmsg("a write to a Snowflake table is not part of this transaction"),
			 errdetail("Snowflake commits each statement on its own, so ROLLBACK "
					   "will not undo it."),
			 errhint("Set pg_lake_snowflake.warn_on_write_in_transaction_block to "
					 "off to silence this.")));
}


/*
 * ReportApiError turns a failed request into a Postgres error, carrying over
 * whatever the account said about it.
 */
static void
ReportApiError(SnowflakeConnection * connection, HttpResult * result, const char *sql)
{
	if (result->body == NULL || result->body[0] != '{')
	{
		const char *transportError = result->errorMsg != NULL ? result->errorMsg :
			"no response body";

		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("could not reach the Snowflake SQL API of server \"%s\"",
						connection->serverName),
				 errdetail("HTTP status %ld: %s", result->status, transportError),
				 errcontext("remote SQL: %s", sql)));
	}

	Jsonb	   *errorJson = NULL;

	PG_TRY();
	{
		errorJson = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
													   CStringGetDatum(result->body)));
	}
	PG_CATCH();
	{
		FlushErrorState();
		errorJson = NULL;
	}
	PG_END_TRY();

	char	   *message = NULL;
	char	   *code = NULL;
	char	   *sqlState = NULL;

	if (errorJson != NULL)
	{
		message = JsonFieldAsCString(&errorJson->root, "message");
		code = JsonFieldAsCString(&errorJson->root, "code");
		sqlState = JsonFieldAsCString(&errorJson->root, "sqlState");
	}

	if (message == NULL)
		message = pnstrdup(result->body, Min(result->bodyLength, 512));

	if (result->status == HTTP_STATUS_UNAUTHORIZED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("Snowflake rejected the credentials of server \"%s\": %s",
						connection->serverName, message),
				 errhint("Check the user mapping. A programmatic access token "
						 "expires, and key-pair authentication needs the public "
						 "key registered on the Snowflake user.")));
	}

	ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg("Snowflake statement failed: %s", message),
			 errdetail("HTTP status %ld%s%s%s%s.",
					   result->status,
					   code != NULL ? ", Snowflake code " : "",
					   code != NULL ? code : "",
					   sqlState != NULL ? ", SQL state " : "",
					   sqlState != NULL ? sqlState : ""),
			 errcontext("remote SQL: %s", sql)));
}


/*
 * CopyConnection copies a connection into the current memory context, so that a
 * statement keeps working after the executor state it was created for is gone.
 */
static SnowflakeConnection *
CopyConnection(SnowflakeConnection * connection)
{
	SnowflakeConnection *copy = palloc0(sizeof(SnowflakeConnection));

	*copy = *connection;

	copy->serverName = pstrdup(connection->serverName);
	copy->accountUrl = pstrdup(connection->accountUrl);
	copy->account = connection->account ? pstrdup(connection->account) : NULL;
	copy->database = connection->database ? pstrdup(connection->database) : NULL;
	copy->schemaName = connection->schemaName ? pstrdup(connection->schemaName) : NULL;
	copy->warehouse = connection->warehouse ? pstrdup(connection->warehouse) : NULL;
	copy->role = connection->role ? pstrdup(connection->role) : NULL;
	copy->userName = connection->userName ? pstrdup(connection->userName) : NULL;
	copy->token = connection->token ? pstrdup(connection->token) : NULL;
	copy->privateKeyPem = connection->privateKeyPem ?
		pstrdup(connection->privateKeyPem) : NULL;
	copy->privateKeyPassphrase = connection->privateKeyPassphrase ?
		pstrdup(connection->privateKeyPassphrase) : NULL;

	return copy;
}


/*
 * FindJsonField returns a field of a JSON object, or NULL when it is absent.
 */
static JsonbValue *
FindJsonField(JsonbContainer *container, const char *fieldName)
{
	JsonbValue	fieldKey;

	fieldKey.type = jbvString;
	fieldKey.val.string.val = (char *) fieldName;
	fieldKey.val.string.len = strlen(fieldName);

	return findJsonbValueFromContainer(container, JB_FOBJECT, &fieldKey);
}


/*
 * JsonFieldAsCString returns a field as text, or NULL when it is absent or null.
 */
static char *
JsonFieldAsCString(JsonbContainer *container, const char *fieldName)
{
	JsonbValue *value = FindJsonField(container, fieldName);

	if (value == NULL || value->type == jbvNull)
		return NULL;

	return JsonbValueAsCString(value);
}


/*
 * JsonFieldAsInt64 returns a numeric field, or the default when it is absent.
 */
static int64
JsonFieldAsInt64(JsonbContainer *container, const char *fieldName, int64 defaultValue)
{
	JsonbValue *value = FindJsonField(container, fieldName);

	if (value == NULL || value->type == jbvNull)
		return defaultValue;

	if (value->type == jbvNumeric)
	{
		Datum		numericDatum = NumericGetDatum(value->val.numeric);

		return DatumGetInt64(DirectFunctionCall1(numeric_int8, numericDatum));
	}

	if (value->type == jbvString)
		return strtoll(JsonbValueAsCString(value), NULL, 10);

	return defaultValue;
}


/*
 * JsonFieldAsBool returns a boolean field, or the default when it is absent.
 */
static bool
JsonFieldAsBool(JsonbContainer *container, const char *fieldName, bool defaultValue)
{
	JsonbValue *value = FindJsonField(container, fieldName);

	if (value == NULL || value->type == jbvNull)
		return defaultValue;

	if (value->type == jbvBool)
		return value->val.boolean;

	if (value->type == jbvString)
	{
		bool		result = defaultValue;

		(void) parse_bool(JsonbValueAsCString(value), &result);

		return result;
	}

	return defaultValue;
}


/*
 * JsonbValueAsCString renders a scalar JSON value as text. Result values arrive
 * as JSON strings, but metadata mixes strings, numbers and booleans.
 */
static char *
JsonbValueAsCString(JsonbValue *value)
{
	switch (value->type)
	{
		case jbvNull:
			return NULL;
		case jbvString:
			return pnstrdup(value->val.string.val, value->val.string.len);
		case jbvNumeric:
			return DatumGetCString(DirectFunctionCall1(numeric_out,
													   NumericGetDatum(value->val.numeric)));
		case jbvBool:
			return pstrdup(value->val.boolean ? "true" : "false");
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					 errmsg("unexpected composite value in a Snowflake result row")));
			return NULL;		/* keep the compiler quiet */
	}
}
