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

#ifndef PGDUCK_TYPE_CONVERSION_H
#define PGDUCK_TYPE_CONVERSION_H

#include <duckdb.h>
#include "catalog/pg_type_d.h"
#include "duckdb/duckdb.h"

/*
 * We should have a relatively large buffer allocated for hugint
 */
#define	STACK_ALLOCATED_OUTPUT_BUFFER_SIZE 42

/*
 * The buffer can be allocated on the stack or on the heap, depending on the
 * size of the data. If the data is small enough, it can be allocated on the
 * stack, otherwise, it will be allocated on the heap. The flag needsFree is
 * used to indicate if the buffer needs to be freed (e.g., allocated on the
 * heap).
 *
 * In general, we use the stack-allocated buffer to avoid the overhead of
 * memory allocation and deallocation.
 *
 * Also, all the numeric types in DuckDB are converted to text using the pre-
 * allocated buffer, which is STACK_ALLOCATED_OUTPUT_BUFFER_SIZE(42) bytes.
 */
typedef struct TextOutputBuffer
{
	char	   *buffer;
	bool		needsFree;
}			TextOutputBuffer;

typedef struct DuckDBTypeInfo
{
	duckdb_type duckType;

	/*
	 * Function pointer for conversion, it is like OUT function in Postgres
	 * for DuckDB types.
	 */
				DuckDBStatus(*to_text) (duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer * buffer);
} DuckDBTypeInfo;

extern DuckDBTypeInfo *find_duck_type_info(duckdb_type duckType);

#endif
