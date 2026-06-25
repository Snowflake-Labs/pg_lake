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

#pragma once

#include <mutex>

namespace duckdb {

/*
 * PgLakeQueryListener tracks the active query for one DuckDB ClientContext
 * so that pg_lake_stat_activity (and similar inspection functions) can
 * report it from a different connection.
 *
 * QueryBegin / QueryEnd run on the session's own thread.  GetActiveQuery
 * runs on whatever thread the caller is on -- typically a different
 * session iterating ConnectionManager::GetConnectionList().  The query
 * string and start timestamp are mutated on every QueryBegin / QueryEnd,
 * so a concurrent direct read is a data race; std::string in particular
 * is undefined behaviour to read while another thread writes to it.
 *
 * The mutex protects the (is_active, active_query, active_start) triple.
 * connectionId is set once during duckdb_pglake_init_connection and
 * never modified afterwards, so it is safe to read directly.
 */
struct PgLakeQueryListener : public ClientContextState {
	int64_t connectionId = 0;

	void QueryBegin(ClientContext &context) override {
		std::lock_guard<std::mutex> lock(active_mutex);
		is_active = true;
		active_query = context.GetCurrentQuery();
		active_start = Timestamp::GetCurrentTimestamp();
	}

	void QueryEnd(ClientContext &context) override {
		std::lock_guard<std::mutex> lock(active_mutex);
		is_active = false;
		active_query.clear();
	}

	//! Snapshot the active query state under the listener's mutex.
	//! Returns true and fills out_query / out_start when a query is
	//! currently running; returns false otherwise.  Safe to call from
	//! any thread.
	bool GetActiveQuery(string &out_query, timestamp_t &out_start) const {
		std::lock_guard<std::mutex> lock(active_mutex);
		if (!is_active) {
			return false;
		}
		out_query = active_query;
		out_start = active_start;
		return true;
	}

	//! Cheap probe of the active flag without copying the query string.
	//! Held briefly under the same mutex as GetActiveQuery.
	bool IsQueryActive() const {
		std::lock_guard<std::mutex> lock(active_mutex);
		return is_active;
	}

private:
	mutable std::mutex active_mutex;
	bool is_active = false;
	string active_query;
	timestamp_t active_start = timestamp_t(0);
};


} // namespace duckdb
