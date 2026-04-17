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

#include "utils/jsonb.h"

typedef void (*JsonParseFunction) (JsonbContainer *json, void *entry);
typedef void (*JsonParseNumericFunction) (int, void *entry);
typedef void (*JsonParseMapEntryFunction) (const char *key, JsonbValue *value, void *entry);


typedef enum FieldRequired
{
	FIELD_REQUIRED,
	FIELD_OPTIONAL
} FieldRequired;

extern PGDLLEXPORT bool JsonExtractInt32Field(JsonbContainer *json, char *fieldName, FieldRequired required, int32_t *intPointer);
extern PGDLLEXPORT bool JsonExtractInt64Field(JsonbContainer *json, char *fieldName, FieldRequired required, int64_t *longPointer);
extern PGDLLEXPORT bool JsonExtractBoolField(JsonbContainer *json, char *fieldName, FieldRequired required, bool *boolPointer);
extern PGDLLEXPORT bool JsonExtractStringField(JsonbContainer *json, char *fieldName, FieldRequired required,
											   const char **stringPointer, size_t *lengthPointer);
extern PGDLLEXPORT bool JsonExtractFieldAsJsonString(JsonbContainer *jsonbContainer, const char *fieldName, FieldRequired required,
													 const char **stringPointer, size_t *lengthPointer);
extern PGDLLEXPORT bool JsonExtractObjectArrayField(JsonbContainer *json, char *fieldName, FieldRequired required,
													JsonParseFunction parseFn,
													size_t entrySize, void **arrayPointer, size_t *lengthPointer);
extern PGDLLEXPORT bool JsonExtractMapField(JsonbContainer *json, char *fieldName, FieldRequired required,
											JsonParseMapEntryFunction parseFn,
											size_t entrySize, void **arrayPointer, size_t *lengthPointer);
extern PGDLLEXPORT bool JsonExtractInt32ArrayField(JsonbContainer *json, char *fieldName, FieldRequired required,
												   JsonParseNumericFunction parseFn,
												   size_t entrySize, void **arrayPointer, size_t *lengthPointer);
