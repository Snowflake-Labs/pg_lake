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

/* DuckDB ignores case, so case-colliding top-level Iceberg columns use c<field-id> there. */
#include "postgres.h"

#include "mb/pg_wchar.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/duckdb_identifier.h"


static uint32
AsciiFoldedHash(const char *name)
{
	uint32		hash = 2166136261u;

	for (; *name != '\0'; name++)
		hash = (hash ^ (unsigned char) pg_ascii_tolower((unsigned char) *name)) * 16777619u;

	return hash;
}


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


/* Find two names that differ only by ASCII case, in one hashed pass. */
bool
NamesHaveCaseCollision(const char **names, int count)
{
	int			slotCount = count * 2 + 1;
	int		   *slots = NULL;
	bool		collision = false;

	if (count < 2)
		return false;

	slots = palloc(slotCount * sizeof(int));
	memset(slots, -1, slotCount * sizeof(int));

	for (int i = 0; i < count && !collision; i++)
	{
		const char *name = names[i];
		uint32		slot = 0;

		if (name == NULL || name[0] == '\0')
			continue;

		slot = AsciiFoldedHash(name) % slotCount;

		for (; slots[slot] >= 0; slot = (slot + 1) % slotCount)
		{
			const char *other = names[slots[slot]];

			if (strcmp(other, name) != 0 && PgLakeAsciiNamesEqual(other, name))
				collision = true;
		}

		slots[slot] = i;
	}

	pfree(slots);

	return collision;
}


bool
DataFileSchemaHasCaseCollision(const DataFileSchema * schema)
{
	const char **names = NULL;
	bool		collision = false;

	if (schema == NULL || schema->nfields < 2)
		return false;

	names = palloc(schema->nfields * sizeof(char *));

	for (size_t fieldIndex = 0; fieldIndex < schema->nfields; fieldIndex++)
		names[fieldIndex] = schema->fields[fieldIndex].name;

	collision = NamesHaveCaseCollision(names, (int) schema->nfields);
	pfree(names);

	return collision;
}


char *
DuckDBNameForFieldId(const char *name, int fieldId, bool hasCollision)
{
	if (name == NULL)
		return pstrdup("");

	if (!hasCollision)
		return pstrdup(name);

	return psprintf("c%d", fieldId);
}


char *
DuckDBNameForField(const DataFileSchema * schema, const char *name, int fieldId)
{
	return DuckDBNameForFieldId(name, fieldId, DataFileSchemaHasCaseCollision(schema));
}


char *
DuckDBNameForColumn(const DataFileSchema * schema, const char *name, bool hasCollision)
{
	if (name == NULL)
		return pstrdup("");

	if (schema == NULL || !hasCollision)
		return pstrdup(name);

	for (size_t fieldIndex = 0; fieldIndex < schema->nfields; fieldIndex++)
	{
		const		DataFileSchemaField *field = &schema->fields[fieldIndex];

		if (field->name != NULL && strcmp(field->name, name) == 0)
			return DuckDBNameForFieldId(name, field->id, true);
	}

	return pstrdup(name);
}


char *
DuckDBNameForColumnName(const DataFileSchema * schema, const char *name)
{
	return DuckDBNameForColumn(schema, name, DataFileSchemaHasCaseCollision(schema));
}


static bool
FieldTreeHasCaseCollision(const Field * field)
{
	if (field == NULL)
		return false;

	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			return false;

		case FIELD_TYPE_LIST:
			return FieldTreeHasCaseCollision(field->field.list.element);

		case FIELD_TYPE_MAP:
			return FieldTreeHasCaseCollision(field->field.map.key) ||
				FieldTreeHasCaseCollision(field->field.map.value);

		case FIELD_TYPE_STRUCT:
			if (DataFileSchemaHasCaseCollision(&field->field.structType))
				return true;

			for (size_t fieldIndex = 0; fieldIndex < field->field.structType.nfields; fieldIndex++)
			{
				if (FieldTreeHasCaseCollision(field->field.structType.fields[fieldIndex].type))
					return true;
			}

			return false;

		default:
			return false;
	}
}


/* DuckDB still rejects a nested struct whose fields differ only by case. */
void
ErrorOnNestedCaseCollision(const DataFileSchema * schema)
{
	if (schema == NULL)
		return;

	for (size_t fieldIndex = 0; fieldIndex < schema->nfields; fieldIndex++)
	{
		if (!FieldTreeHasCaseCollision(schema->fields[fieldIndex].type))
			continue;

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("nested fields that differ only by letter case are not supported")));
	}
}
