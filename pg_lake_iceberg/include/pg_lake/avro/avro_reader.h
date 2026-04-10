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

#include "avro.h"
#include "utils/palloc.h"


typedef enum AvroFieldRequired
{
	AVRO_FIELD_REQUIRED,
	AVRO_FIELD_OPTIONAL
}			AvroFieldRequired;

typedef void (*AvroParseFunction) (avro_value_t * record, void *entry, void *context);
typedef void (*AvroParseMapEntryFunction) (const char *key, avro_value_t * record, void *entry, void *context);

typedef struct AvroReader
{
	MemoryContext memoryContext;

	/* file we are reading from */
	FILE	   *avroFile;

	/* reader for Avro records */
	avro_file_reader_t dataReader;
	bool		initializedDataReader;

	/* schema of the Avro records */
	avro_schema_t dataSchema;
	const char *jsonSchema;

	/* interface for the record */
	avro_value_iface_t *dataInterface;
}			AvroReader;


AvroReader *AvroReaderCreate(const char *filePath);
bool		AvroReaderReadRecord(AvroReader * reader, AvroParseFunction parseFn, void *entry, void *context);
void		AvroReaderClose(AvroReader * reader);

bool		AvroFieldExists(avro_value_t * record, char *fieldName);
void		AvroGetBoolField(avro_value_t * record, char *fieldName, AvroFieldRequired required, bool *boolPointer);
void		AvroGetInt32Field(avro_value_t * record, char *fieldName, AvroFieldRequired required, int32_t *valPointer);
void		AvroGetNullableInt32Field(avro_value_t * record, char *fieldName, int32_t *intPointer, bool *isSet);
void		AvroGetInt64Field(avro_value_t * record, char *fieldName, AvroFieldRequired required, int64_t *valPointer);
void		AvroGetNullableInt64Field(avro_value_t * record, char *fieldName, int64_t *longPointer, bool *isSet);
void		AvroGetStringField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
							   const char **stringPointer, size_t *lengthPointer);
void		AvroGetNullableStringField(avro_value_t * record, char *fieldName, const char **stringPointer, size_t *lengthPointer, bool *isSet);
void		AvroGetBinaryField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
							   const void **bytesPointer, size_t *lengthPointer);
void		AvroGetNullableBinaryField(avro_value_t * record, char *fieldName, const void **bytesPointer, size_t *lengthPointer, bool *isSet);
void		AvroGetRecordField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
							   AvroParseFunction parseFn, void *entry, void *context);
void		AvroGetObjectArrayField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
									AvroParseFunction parseFn, size_t entrySize,
									void **arrayPointer, size_t *lengthPointer, void *context);
void		AvroGetInt32ArrayField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
								   int32_t **arrayPointer, size_t *lengthPointer);
void		AvroGetInt64ArrayField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
								   int64_t **arrayPointer, size_t *lengthPointer);
void		AvroGetMapField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
							AvroParseMapEntryFunction parseFn, size_t entrySize,
							void **arrayPointer, size_t *lengthPointer, void *context);
size_t		AvroGetTotalRecordFields(avro_value_t * record);
void		AvroExtractNullableFieldFromRecordByIndex(avro_value_t * record, int index,
													  void **value, size_t *valueLength,
													  char **fieldName);
