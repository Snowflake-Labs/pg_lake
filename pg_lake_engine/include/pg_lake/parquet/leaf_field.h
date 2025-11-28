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

#include "postgres.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/type.h"

/*
 * LeafField represents a leaf field in the data file schema.
 * Leaf fields are the leaf nodes of the schema. They are the
 * scalar fields in the schema.
 */
typedef struct LeafField
{
	int			fieldId;
	Field	   *field;

	/*
	 * useful when converting stats values from text to Postgres datums
	 */
	PGType		pgType;

	/*
	 * useful when fetching stats from the parquet file via duckdb
	 */
	const char *duckTypeName;

	/*
	 * The level of the field in the schema. The level is 1 for top-level
	 * fields.
	 */
	int			level;
}			LeafField;

extern PGDLLEXPORT int LeafFieldCompare(const ListCell *a, const ListCell *b);
extern PGDLLEXPORT bool SchemaFieldsEquivalent(DataFileSchemaField * fieldA, DataFileSchemaField * fieldB);
#if PG_VERSION_NUM < 170000
extern PGDLLEXPORT int pg_cmp_s32(int32 a, int32 b);
#endif
