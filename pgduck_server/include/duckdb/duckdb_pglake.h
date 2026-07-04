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

#ifndef PGDUCK_PGCOMPAT_H
#define PGDUCK_PGCOMPAT_H

#include <duckdb.h>

/*
 * Declarations of the functions that are used to convert
 * DuckDB data types to their string representations. These functions are defined
 * in the duckdb_pg_lake shared library. The duckdb_pg_lake shared library is
 * loaded by the duckdb_pg_lake extension, which is a DuckDB extension.
 * We have access to the C functions it exposes. Here, we declare the C functions
 * such that we can access them from the pgduck_server.
 */
const char *duckdb_pglake_decimal_to_string(duckdb_decimal input);
const char *duckdb_pglake_uuid_to_string(duckdb_hugeint input);
const char *duckdb_pglake_hugeint_to_string(duckdb_hugeint input);
const char *duckdb_pglake_uhugeint_to_string(duckdb_uhugeint input);
const char *duckdb_pglake_timestamp_to_string(duckdb_timestamp input);
const char *duckdb_pglake_timestamp_ns_to_string(duckdb_timestamp input);
const char *duckdb_pglake_timestamp_ms_to_string(duckdb_timestamp input);
const char *duckdb_pglake_timestamp_sec_to_string(duckdb_timestamp input);
const char *duckdb_pglake_timestamp_tz_to_string(duckdb_timestamp input);
const char *duckdb_pglake_interval_to_string(duckdb_interval input);
const char *duckdb_pglake_time_to_string(duckdb_time input);
const char *duckdb_pglake_time_tz_to_string(duckdb_time_tz input);
const char *duckdb_pglake_date_to_string(duckdb_date input);
const char *duckdb_pglake_geometry_to_string(duckdb_database database, duckdb_string_t input);
bool		duckdb_pglake_is_geometry_type(duckdb_logical_type type);
int			duckdb_pglake_geometry_get_srid(duckdb_logical_type type);

/*
 * Other utility functions in duckdb_pglake.
 */
void		duckdb_pglake_set_output_verbose(bool verbose);
void		duckdb_pglake_init_connection(duckdb_connection connection, int64_t connectionId);

#endif
