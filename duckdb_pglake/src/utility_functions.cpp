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
}

}
