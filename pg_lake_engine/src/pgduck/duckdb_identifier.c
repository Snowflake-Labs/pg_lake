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
 * DuckDB folds identifiers. When top-level Iceberg siblings differ only by
 * ASCII case, DuckDB sees c<field id> for every sibling at that level.
 * Iceberg metadata keeps the real names and field ids.
 */
#include "postgres.h"

#include "mb/pg_wchar.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/duckdb_identifier.h"


bool
PgLakeAsciiNamesEqual(const char *left, const char *right)
{
	if (left == NULL || right == NULL)
		return left == right;

	while (*left != '\0' && *right != '\0')
	{
		if (pg_ascii_tolower((unsigned char) *left) !=
			pg_ascii_tolower((unsigned char) *right))
			return false;

		left++;
		right++;
	}

	return *left == *right;
}


bool
DataFileSchemaHasCaseCollision(const DataFileSchema * schema)
{
	if (schema == NULL)
		return false;

	for (size_t leftIndex = 0; leftIndex < schema->nfields; leftIndex++)
	{
		const char *left = schema->fields[leftIndex].name;

		if (left == NULL || left[0] == '\0')
			continue;

		for (size_t rightIndex = leftIndex + 1; rightIndex < schema->nfields; rightIndex++)
		{
			const char *right = schema->fields[rightIndex].name;

			if (right == NULL || right[0] == '\0' || strcmp(left, right) == 0)
				continue;

			if (PgLakeAsciiNamesEqual(left, right))
				return true;
		}
	}

	return false;
}


char *
DuckDBNameForField(const DataFileSchema * schema, const char *name, int fieldId)
{
	if (name == NULL)
		return pstrdup("");

	if (!DataFileSchemaHasCaseCollision(schema))
		return pstrdup(name);

	return psprintf("c%d", fieldId);
}


char *
DuckDBNameForColumnName(const DataFileSchema * schema, const char *name)
{
	if (name == NULL)
		return pstrdup("");

	if (schema == NULL)
		return pstrdup(name);

	for (size_t fieldIndex = 0; fieldIndex < schema->nfields; fieldIndex++)
	{
		const		DataFileSchemaField *field = &schema->fields[fieldIndex];

		if (field->name != NULL && strcmp(field->name, name) == 0)
			return DuckDBNameForField(schema, name, field->id);
	}

	return pstrdup(name);
}
