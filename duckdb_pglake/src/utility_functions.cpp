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

#include "duckdb.hpp"

#include "pg_lake/query_listener.hpp"
#include "pg_lake/utility_functions.hpp"
#include "duckdb/main/connection_manager.hpp"

#include <chrono>
#include <thread>

namespace duckdb {

/*
 * StatActivityFunctionData defines the custom state for pg_lake_stat_activity
 */
struct StatActivityFunctionData : public TableFunctionData
{
	/* Function state */
	vector<shared_ptr<ClientContext>> connections;
	int offset;
	bool finished = false;
};


/*
 * StatActivityBind implements the bind phase for pg_lake_stat_activity.
 */
static unique_ptr<FunctionData> StatActivityBind(ClientContext &context,
											     TableFunctionBindInput &input,
											     vector<LogicalType> &return_types,
											     vector<string> &names) {

	/* Get the arguments */
	auto functionData = make_uniq<StatActivityFunctionData>();

	/* Set the return type */
	return_types.emplace_back(LogicalType::BIGINT);
	return_types.emplace_back(LogicalType::VARCHAR);
	return_types.emplace_back(LogicalType::TIMESTAMP);
	names.emplace_back("connection_id");
	names.emplace_back("query");
	names.emplace_back("query_start");

	return std::move(functionData);
}


/*
 * StatActivityExecute implements the execution for pg_lake_stat_activity.
 */
static void StatActivityExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &functionData = (StatActivityFunctionData &)*data_p.bind_data;
	if (functionData.finished)
		return;

	/* Do the work */
	if (functionData.offset == 0)
	{
		ConnectionManager &connectionManager = context.db->GetConnectionManager();
		functionData.connections = connectionManager.GetConnectionList();
	}

	/* Set return values */
	idx_t rowsInChunk = 0;
	while (functionData.offset< functionData.connections.size() && rowsInChunk < STANDARD_VECTOR_SIZE) {
		shared_ptr<ClientContext> connection = functionData.connections[functionData.offset];

		if (connection && !connection->ExecutionIsFinished())
		{
			shared_ptr<PgLakeQueryListener> queryListener =
				connection->registered_state->Get<PgLakeQueryListener>("pg_lake_query_listener");

			if (queryListener && queryListener->isQueryActive)
			{
				output.SetValue(0, rowsInChunk, Value::BIGINT(queryListener->connectionId));
				output.SetValue(1, rowsInChunk, Value(queryListener->queryString));
				output.SetValue(2, rowsInChunk, Value::TIMESTAMP(queryListener->queryStart));

				rowsInChunk++;
			}
		}

		functionData.offset++;
	}

	output.SetCardinality(rowsInChunk);

	if (functionData.offset == functionData.connections.size())
		functionData.finished = true;
}


/*
 * QueryProgressFunctionData defines the custom state for pg_lake_query_progress.
 */
struct QueryProgressFunctionData : public TableFunctionData
{
	int64_t targetConnectionId;
	bool finished = false;
};


/*
 * QueryProgressBind implements the bind phase for pg_lake_query_progress.
 *
 * Takes a connection identifier and returns at most one row, with the
 * cumulative progress that DuckDB's executor publishes on
 * ClientContext::query_progress for the matching session. This is the
 * same value DuckDB's progress bar would display, exposed so that a
 * monitor connection can read it for any session by id.
 *
 * What the percentage measures.  The aggregator walks the executor's
 * pipelines and reads progress from each scan operator that registered
 * a table_scan_progress callback (postgres_scan, parquet_scan, csv read,
 * range, and so on).  It does not look at sinks (COPY TO, parquet
 * writer, S3 multipart upload finalize) or post-scan operators
 * (sort, join, aggregate beyond their input scans).  The number is
 * therefore "how much of the source has been read", not "how much of
 * the wall-clock work is left".  In particular for COPY (SELECT FROM
 * postgres_scan(...)) TO ... the bar can sit pinned near 100% while
 * the destination side is still flushing.
 *
 * The fields are populated by the executor only for sessions where
 * enable_progress_bar is on.  pgduck enables it by default in
 * duckdb_pglake_init_connection, but the executor still waits
 * client_config.wait_time milliseconds (default 2000) before turning
 * the aggregator on, so very short queries return -1 / 0 / 0 even on
 * a session with the bar enabled.  If a session has disabled the bar
 * (`SET enable_progress_bar = false`) the row carries percentage = -1
 * with rows_processed and total_rows_to_process both 0.  We surface
 * those raw values without filtering.  If no session has the
 * requested connection_id, or the matching session has no active
 * query, the result is empty.
 */
static unique_ptr<FunctionData> QueryProgressBind(ClientContext &context,
												  TableFunctionBindInput &input,
												  vector<LogicalType> &return_types,
												  vector<string> &names) {
	auto functionData = make_uniq<QueryProgressFunctionData>();
	functionData->targetConnectionId = input.inputs[0].GetValue<int64_t>();

	return_types.emplace_back(LogicalType::DOUBLE);
	return_types.emplace_back(LogicalType::BIGINT);
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("percentage");
	names.emplace_back("rows_processed");
	names.emplace_back("total_rows_to_process");

	return std::move(functionData);
}


/*
 * QueryProgressExec implements the execution for pg_lake_query_progress.
 *
 * GetQueryProgress returns a copy of the QueryProgress struct, whose
 * fields are atomic, so the call is safe to make from a thread other than
 * the one running the query. The same pattern is used by DuckDB's own C
 * API entry point duckdb_query_progress.
 */
static void QueryProgressExec(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &functionData = (QueryProgressFunctionData &)*data_p.bind_data;
	if (functionData.finished)
		return;
	functionData.finished = true;

	ConnectionManager &connectionManager = context.db->GetConnectionManager();
	vector<shared_ptr<ClientContext>> connections = connectionManager.GetConnectionList();

	for (auto &connection : connections)
	{
		if (!connection || connection->ExecutionIsFinished())
			continue;

		shared_ptr<PgLakeQueryListener> queryListener =
			connection->registered_state->Get<PgLakeQueryListener>("pg_lake_query_listener");

		if (!queryListener || !queryListener->isQueryActive)
			continue;

		if (queryListener->connectionId != functionData.targetConnectionId)
			continue;

		QueryProgress progress = connection->GetQueryProgress();

		output.SetValue(0, 0, Value::DOUBLE(progress.GetPercentage()));
		output.SetValue(1, 0, Value::BIGINT(progress.GetRowsProcesseed()));
		output.SetValue(2, 0, Value::BIGINT(progress.GetTotalRowsToProcess()));
		output.SetCardinality(1);
		return;
	}

	output.SetCardinality(0);
}


/*
 * Implementation of the pg_lake_connection_id scalar function.
 *
 * Returns the connection identifier assigned by pgduck_server to the
 * current session. This is the same value libpq's PQbackendPID returns
 * to a client connected to pgduck, and the same value reported in the
 * connection_id column of pg_lake_stat_activity. Pass it to
 * pg_lake_query_progress(connection_id) to read the executor's progress
 * for this session from another connection.
 */
static void
ConnectionIdScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	ClientContext &context = state.GetContext();
	shared_ptr<PgLakeQueryListener> queryListener =
		context.registered_state->Get<PgLakeQueryListener>("pg_lake_query_listener");

	int64_t connectionId = queryListener ? queryListener->connectionId : 0;

	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	ConstantVector::GetData<int64_t>(result)[0] = connectionId;
}


/*
 * Implementation of the pg_lake_sleep scalar function.
 */
static void
SleepScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	ClientContext &context = state.GetContext();
	auto &sleepVector = args.data[0];

	UnaryExecutor::Execute<double, bool>(
		sleepVector, result, args.size(),
		[&](double totalSleepSeconds) {
			int64_t elapsed = 0;
			int64_t totalSleepMillis = (int64_t) (totalSleepSeconds * 1000.);

			const int64_t SLICE_MS = 10;
			auto start = std::chrono::steady_clock::now();

			/* do small sleeps and check for interrupts */
			while (elapsed < totalSleepMillis) {
				if (context.interrupted)
					throw InterruptException();

				const auto remaining = totalSleepMillis - elapsed;
				const auto singleSleepMillis = remaining < SLICE_MS ? remaining : SLICE_MS;
				std::this_thread::sleep_for(std::chrono::milliseconds(singleSleepMillis));

				elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
			}

			return true;
		}
	);
}


/*
 * RegisterFunctions registers the SQL utility functions.
 */
void
PgLakeUtilityFunctions::RegisterFunctions(ExtensionLoader &loader)
{
	/* pg_lake_stat_activity function definition */
	{
		TableFunctionSet pg_lake_stat_activity("pg_lake_stat_activity");

		/* pg_lake_stat_activity() */
		pg_lake_stat_activity.AddFunction(
			TableFunction({},
						  StatActivityExec, StatActivityBind));

	    loader.RegisterFunction(pg_lake_stat_activity);
	}

	/* pg_lake_sleep function definition */
	{
		ScalarFunction pg_lake_sleep=
			ScalarFunction("pg_lake_sleep",
						   {LogicalType::DOUBLE},
						   LogicalType::BOOLEAN,
						   SleepScalarFun);

		loader.RegisterFunction(pg_lake_sleep);
	}

	/* pg_lake_query_progress function definition */
	{
		TableFunctionSet pg_lake_query_progress("pg_lake_query_progress");

		pg_lake_query_progress.AddFunction(
			TableFunction({LogicalType::BIGINT},
						  QueryProgressExec, QueryProgressBind));

		loader.RegisterFunction(pg_lake_query_progress);
	}

	/* pg_lake_connection_id function definition */
	{
		ScalarFunction pg_lake_connection_id=
			ScalarFunction("pg_lake_connection_id",
						   {},
						   LogicalType::BIGINT,
						   ConnectionIdScalarFun);

		loader.RegisterFunction(pg_lake_connection_id);
	}
}

}
