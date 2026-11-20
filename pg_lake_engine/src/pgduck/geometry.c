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

#include "pg_lake/pgduck/keywords.h"
#include "pg_lake/pgduck/geometry.h"
#include "utils/builtins.h"

/*
 * Query to decode parquet metadata to identify geometry columns.
 */
char *
GetGeomColumnsMetadataQuery(const char *url)
{
	StringInfoData command;

	initStringInfo(&command);

	/*
	 * Returned metadata object resembles:
	 *
	 * { "version": "1.0.0", "primary_column": "geometry", "columns": {
	 * "geometry": { "encoding": "WKB", "geometry_types": [ "Point" ], "bbox":
	 * [ -179.9984671, -85, -0.0833333, 44.9999981 ] } } }
	 */


	/*
	 * This hideous CTE is required to extract the column names and encodings
	 * from the internalized JSON structure; perhaps we want to add a
	 * `"version"` key check here to avoid doing anything nonsensical if
	 * something changes.
	 *
	 * "columns_lookup" is a singleton JSON object, so the join in the CTE
	 * does not need to be constrained.
	 */

	appendStringInfo(&command,
					 "WITH columns_lookup AS ("
					 "    SELECT decode(value)->'columns' AS coljson"
					 "    FROM parquet_kv_metadata(%s)"
					 "    WHERE decode(key) = 'geo'"
					 "    LIMIT 1"
					 "),"
					 "column_names AS ("
					 "    SELECT UNNEST(json_keys(coljson)) AS colname FROM columns_lookup"
					 ")"
					 "SELECT"
					 "    colname, "
					 "    coljson->colname->>'encoding' "
					 "FROM columns_lookup, column_names",
					 duckdb_quote_literal(url)
		);

	return command.data;
}
