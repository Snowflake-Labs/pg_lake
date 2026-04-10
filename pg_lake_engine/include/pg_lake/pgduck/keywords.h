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

/*
 * IsDuckDBReservedWord — returns true for any keyword that is not
 * UNRESERVED_KEYWORD in DuckDB (i.e., RESERVED, COL_NAME, or
 * TYPE_FUNC_NAME).  Used for struct field-access quoting.
 */
PGDLLEXPORT bool IsDuckDBReservedWord(char *candidateWord);

/*
 * duckdb_quote_identifier — like quote_identifier() but also quotes
 * identifiers that are RESERVED_KEYWORD in DuckDB but not in PostgreSQL
 * (e.g. LAMBDA, PIVOT, QUALIFY, SUMMARIZE, DESCRIBE, SHOW, UNPIVOT).
 *
 * Use this for all identifiers (column names, field names, relation names)
 * that will appear in SQL sent to pgduck_server.
 */
PGDLLEXPORT const char *duckdb_quote_identifier(const char *ident);

/*
 * RequoteDuckDBReservedInSQL — post-process a SQL string from pg_get_querydef
 * and wrap any unquoted identifier that is RESERVED_KEYWORD in DuckDB but
 * not reserved in PostgreSQL (e.g. PIVOT, QUALIFY, LAMBDA, SHOW, SUMMARIZE).
 *
 * Use this on every SQL string produced by pg_get_querydef() before sending
 * it to pgduck_server.
 */
PGDLLEXPORT char *RequoteDuckDBReservedInSQL(const char *sql);
