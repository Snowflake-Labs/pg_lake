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

#include "common/fe_memutils.h"
#include "pg_lake/json/json_reader.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/numeric.h"


bool
JsonExtractInt32Field(JsonbContainer *json, char *fieldName, FieldRequired required, int32_t *intPointer)
{
	JsonbValue *fieldValueJson;

	*intPointer = 0;

	fieldValueJson = getKeyJsonValueFromContainer(json,
												  fieldName,
												  strlen(fieldName),
												  NULL);
	if (fieldValueJson != NULL && fieldValueJson->type == jbvNumeric)
	{
		bool		haveError = false;

		*intPointer = numeric_int4_opt_error(fieldValueJson->val.numeric,
											 &haveError);
		if (haveError)
		{
			ereport(ERROR, (errmsg("integer out of range")));
		}

		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	return false;
}


bool
JsonExtractInt64Field(JsonbContainer *json, char *fieldName, FieldRequired required, int64_t *longPointer)
{
	JsonbValue *fieldValueJson;

	*longPointer = 0L;

	fieldValueJson = getKeyJsonValueFromContainer(json,
												  fieldName,
												  strlen(fieldName),
												  NULL);
	if (fieldValueJson != NULL && fieldValueJson->type == jbvNumeric)
	{
		Datum		longDatum = DirectFunctionCall1(numeric_int8,
													NumericGetDatum(fieldValueJson->val.numeric));

		*longPointer = DatumGetInt64(longDatum);
		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	return false;
}


bool
JsonExtractBoolField(JsonbContainer *json, char *fieldName, FieldRequired required, bool *boolPointer)
{
	JsonbValue *fieldValueJson;

	*boolPointer = false;

	fieldValueJson = getKeyJsonValueFromContainer(json,
												  fieldName,
												  strlen(fieldName),
												  NULL);
	if (fieldValueJson != NULL && fieldValueJson->type == jbvBool)
	{
		*boolPointer = fieldValueJson->val.boolean;
		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	return false;
}


bool
JsonExtractStringField(JsonbContainer *json, char *fieldName, FieldRequired required, const char **stringPointer,
					   size_t *lengthPointer)
{
	JsonbValue *fieldValueJson;

	*stringPointer = NULL;
	*lengthPointer = 0L;

	fieldValueJson = getKeyJsonValueFromContainer(json,
												  fieldName,
												  strlen(fieldName),
												  NULL);
	if (fieldValueJson != NULL && fieldValueJson->type == jbvString)
	{
		int			stringLength = fieldValueJson->val.string.len;

		*stringPointer = (const char *) pnstrdup(fieldValueJson->val.string.val, stringLength);
		*lengthPointer = stringLength;
		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	return false;
}


/*
 * JsonExtractFieldAsJsonString extracts a field from a JSON object and returns it as a JSON string.
 */
bool
JsonExtractFieldAsJsonString(JsonbContainer *jsonbContainer, const char *fieldName, FieldRequired required,
							 const char **stringPointer, size_t *lengthPointer)
{
	JsonbValue *fieldValueJson = getKeyJsonValueFromContainer(jsonbContainer, fieldName, strlen(fieldName), NULL);

	if (fieldValueJson == NULL && required == FIELD_OPTIONAL)
	{
		*lengthPointer = 0;
		return false;
	}
	else if (fieldValueJson == NULL && required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	Jsonb	   *fieldJsonb = JsonbValueToJsonb(fieldValueJson);
	JsonbContainer *fieldJsonContainer = &fieldJsonb->root;

	*stringPointer = JsonbToCString(NULL, fieldJsonContainer, VARSIZE(fieldJsonb));
	*lengthPointer = strlen(*stringPointer);

	return true;
}


bool
JsonExtractObjectArrayField(JsonbContainer *json, char *fieldName, FieldRequired required,
							JsonParseFunction parseFn,
							size_t entrySize, void **arrayPointer, size_t *lengthPointer)
{
	JsonbValue *fieldValueJson;

	*arrayPointer = NULL;
	*lengthPointer = 0L;

	fieldValueJson = getKeyJsonValueFromContainer(json,
												  fieldName,
												  strlen(fieldName),
												  NULL);

	if (fieldValueJson != NULL && fieldValueJson->type == jbvArray)
	{
		int			itemCount = fieldValueJson->val.array.nElems;
		int			itemIndex = 0;

		/* allocate N entries */
		char	   *entries = palloc0(entrySize * itemCount);

		*arrayPointer = entries;
		*lengthPointer = itemCount;
		for (itemIndex = 0; itemIndex < itemCount; itemIndex++)
		{
			JsonbValue *item = &fieldValueJson->val.array.elems[itemIndex];

			/* call parse for each array item and allocated entry */
			parseFn(item->val.binary.data, (void *) entries);

			entries += entrySize;
		}

		return true;
	}
	else if (fieldValueJson != NULL && fieldValueJson->type == jbvBinary)
	{
		JsonbIterator *iterator = JsonbIteratorInit(fieldValueJson->val.binary.data);
		int			itemCount = JsonContainerSize(fieldValueJson->val.binary.data);

		/* no items, return before allocating anything */
		if (itemCount == 0)
			return true;

		/* allocate N entries */
		char	   *entries = palloc0(entrySize * itemCount);

		*arrayPointer = entries;
		*lengthPointer = itemCount;

		for (;;)
		{
			JsonbValue	content;
			JsonbIteratorToken token = JsonbIteratorNext(&iterator, &content, true);

			if (token == WJB_DONE)
			{
				break;
			}

			if (token == WJB_ELEM)
			{

				/* call parse for each array item and allocated entry */
				parseFn(content.val.binary.data, (void *) entries);

				entries += entrySize;
			}
		}

		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	return true;
}


bool
JsonExtractInt32ArrayField(JsonbContainer *json, char *fieldName, FieldRequired required,
						   JsonParseNumericFunction parseFn,
						   size_t entrySize, void **arrayPointer, size_t *lengthPointer)
{
	JsonbValue *fieldValueJson;
	bool		haveError = false;

	*arrayPointer = NULL;
	*lengthPointer = 0L;

	fieldValueJson = getKeyJsonValueFromContainer(json,
												  fieldName,
												  strlen(fieldName),
												  NULL);
	if (fieldValueJson != NULL && fieldValueJson->type == jbvArray)
	{
		int			itemCount = fieldValueJson->val.array.nElems;
		int			itemIndex = 0;

		/* allocate N entries */
		char	   *entries = palloc0(entrySize * itemCount);

		*arrayPointer = entries;
		*lengthPointer = itemCount;
		for (itemIndex = 0; itemIndex < itemCount; itemIndex++)
		{
			int			endVal = numeric_int4_opt_error(fieldValueJson->val.numeric,
														&haveError);

			if (haveError)
			{
				ereport(ERROR, (errmsg("integer out of range")));
			}

			/* call parse for each array item and allocated entry */
			parseFn(endVal, (void *) entries);

			entries += entrySize;
		}

		return true;
	}
	else if (fieldValueJson != NULL && fieldValueJson->type == jbvBinary)
	{
		JsonbIterator *iterator = JsonbIteratorInit(fieldValueJson->val.binary.data);
		int			itemCount = JsonContainerSize(fieldValueJson->val.binary.data);

		/* no items, return before allocating anything */
		if (itemCount == 0)
			return true;

		/* allocate N entries */
		char	   *entries = palloc0(entrySize * itemCount);

		*arrayPointer = entries;
		*lengthPointer = itemCount;

		for (;;)
		{
			JsonbValue	content;
			JsonbIteratorToken token = JsonbIteratorNext(&iterator, &content, true);

			if (token == WJB_DONE)
			{
				break;
			}

			if (token == WJB_ELEM)
			{
				if (content.type != jbvNumeric)
				{
					ereport(ERROR, (errmsg("expected numeric in json, got %d", content.type)));
				}

				int			endVal = numeric_int4_opt_error(content.val.numeric,
															&haveError);

				if (haveError)
				{
					ereport(ERROR, (errmsg("integer out of range")));
				}

				/* call parse for each array item and allocated entry */
				parseFn(endVal, (void *) entries);
				entries += entrySize;
			}
		}

		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	return true;
}


bool
JsonExtractMapField(JsonbContainer *json, char *fieldName, FieldRequired required,
					JsonParseMapEntryFunction parseFn,
					size_t entrySize, void **arrayPointer, size_t *lengthPointer)
{
	JsonbValue *fieldValueJson;

	*arrayPointer = NULL;
	*lengthPointer = 0L;

	fieldValueJson = getKeyJsonValueFromContainer(json,
												  fieldName,
												  strlen(fieldName),
												  NULL);
	if (fieldValueJson != NULL && fieldValueJson->type == jbvObject)
	{
		ereport(ERROR, (errmsg("jbvObject handling not yet implemented")));
	}
	else if (fieldValueJson != NULL && fieldValueJson->type == jbvBinary)
	{
		char	   *key = NULL;

		JsonbIterator *iterator = JsonbIteratorInit(fieldValueJson->val.binary.data);
		int			itemCount = JsonContainerSize(fieldValueJson->val.binary.data);

		/* no items, return before allocating anything */
		if (itemCount == 0)
			return true;

		/* allocate N entries */
		char	   *entries = palloc0(entrySize * itemCount);

		*arrayPointer = entries;
		*lengthPointer = itemCount;

		for (;;)
		{
			JsonbValue	content;
			JsonbIteratorToken token = JsonbIteratorNext(&iterator, &content, true);

			if (token == WJB_DONE)
			{
				break;
			}

			if (token == WJB_KEY)
			{
				/* store the last key */
				key = pnstrdup(content.val.string.val, content.val.string.len);
			}
			else if (token == WJB_VALUE)
			{
				/*
				 * pass the last seen key (allocated) and value (not
				 * allocated)
				 */
				parseFn(key, &content, (void *) entries);

				entries += entrySize;
			}
		}

		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}

	return true;
}
