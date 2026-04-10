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

#define DEFAULT_AVRO_WRITER_BLOCK_SIZE 64

/* pg_lake_iceberg.default_avro_writer_block_size_kb */
extern int	DefaultAvroWriterBlockSize;

typedef void (*AvroSerializeFunction) (void *entry, avro_value_t * record);

typedef struct AvroWriter
{
	MemoryContext memoryContext;

	/* file we are writing to */
	FILE	   *avroFile;

	/* writer for Avro records */
	avro_file_writer_t dataWriter;
	bool		initializedDataWriter;

	/* schema of the Avro records */
	avro_schema_t dataSchema;

	/* interface for the record */
	avro_value_iface_t *dataInterface;

	/* top-level record container */
	avro_value_t record;
}			AvroWriter;


AvroWriter *AvroWriterCreateWithJsonSchema(const char *filePath,
										   const char *schemaJson);
void		AvroWriterWriteRecord(AvroWriter * writer, AvroSerializeFunction serializeFn, void *entry);
void		AvroWriterClose(AvroWriter * writer);

void		AvroSetNull(avro_value_t * record, char *fieldName);
void		AvroSetBoolField(avro_value_t * record, char *fieldName, bool value);
void		AvroSetInt32Field(avro_value_t * record, char *fieldName, int32_t value);
void		AvroSetNullableInt32Field(avro_value_t * record, char *fieldName, int32_t value, bool isSet);
void		AvroSetNullableBoolField(avro_value_t * record, char *fieldName, bool value, bool isSet);
void		AvroSetNullableFloatField(avro_value_t * record, char *fieldName, float4 value, bool isSet);
void		AvroSetNullableDoubleField(avro_value_t * record, char *fieldName, float8 value, bool isSet);
void		AvroSetInt64Field(avro_value_t * record, char *fieldName, int64_t value);
void		AvroSetNullableInt64Field(avro_value_t * record, char *fieldName, int64_t value, bool isSet);
void		AvroSetRecordField(avro_value_t * record, char *fieldName, AvroSerializeFunction serializeFn, void *entry);
void		AvroSetStringField(avro_value_t * record, char *fieldName, const char *value);
void		AvroSetNullableStringField(avro_value_t * record, char *fieldName, const char *value, bool isSet);
void		AvroSetBinaryField(avro_value_t * record, char *fieldName, const void *bytes, size_t length);
void		AvroSetNullableBinaryField(avro_value_t * record, char *fieldName, const void *bytes, size_t length, bool isSet);
void		AvroSetRecordArrayField(avro_value_t * record, char *fieldName, AvroSerializeFunction serializeFn, size_t entrySize, void *entryArray, size_t entryCount);
void		AvroSetInt32ArrayField(avro_value_t * record, char *fieldName, int32_t *values, size_t valueCount);
void		AvroSetInt64ArrayField(avro_value_t * record, char *fieldName, int64_t *values, size_t valueCount);
