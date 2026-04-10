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

#include "avro.h"

#include "postgres.h"

#include "pg_lake/avro/avro_writer.h"

#include "storage/fd.h"
#include "utils/memutils.h"
#include "utils/palloc.h"

/*
 * ExtraMetadata which is used by AvroWriter.
 */
typedef struct ExtraMetadata
{
	const char *schema_json;
	size_t		schema_json_length;
}			ExtraMetadata;

static AvroWriter * AvroWriterCreate(const char *filePath, avro_schema_t schema, avro_value_t * meta);
static void AvroFileWriterClose(AvroWriter * writer);
static void AvroPrepareSetNullable(avro_value_t * record, char *fieldName, avro_value_t * innerValue, bool isSet);
static void WriteExtraMetadataToAvro(avro_value_t * record, ExtraMetadata * metadata);
static avro_value_t * CreateAvroExtraMetadata(const char *schemaJson);
static avro_schema_t GetSchemaFromJson(const char *schemaJson);


#define KB_BYTES 1024

/*
 * configurable avro block size. Can be set by superuser when the writer
 * fails to write complex records. (e.g. large iceberg schema)
 */
int			DefaultAvroWriterBlockSize = DEFAULT_AVRO_WRITER_BLOCK_SIZE;


/*
 * AvroWriterCreate creates an Avro writer for the given file of given JSON schema
 * and metadata.
 */
AvroWriter *
AvroWriterCreateWithJsonSchema(const char *filePath, const char *schemaJson)
{
	avro_schema_t schema = GetSchemaFromJson(schemaJson);

	/* "avro.schema" */
	avro_value_t *meta = CreateAvroExtraMetadata(schemaJson);

	return AvroWriterCreate(filePath, schema, meta);
}


/*
 * AvroWriterCreate creates an Avro writer for the given file.
 */
static AvroWriter *
AvroWriterCreate(const char *filePath, avro_schema_t schema, avro_value_t * meta)
{
	MemoryContext writerMemoryContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AvroWriter",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(writerMemoryContext);

	AvroWriter *writer = palloc0(sizeof(AvroWriter));

	writer->memoryContext = writerMemoryContext;

	writer->dataSchema = schema;
	writer->dataInterface = avro_generic_class_from_schema(writer->dataSchema);
	avro_generic_value_new(writer->dataInterface, &writer->record);

	writer->avroFile = AllocateFile(filePath, PG_BINARY_W);
	if (writer->avroFile == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open file \"%s\": %m",
							   filePath)));
	}

	/* we will close via FreeFile */
	int			shouldClose = 0;

	size_t		blockSize = KB_BYTES * (size_t) DefaultAvroWriterBlockSize;

	/* uses modified libavro with metadata */
	if (avro_file_writer_create_with_codec_metadata_fp(writer->avroFile,
													   filePath, shouldClose,
													   writer->dataSchema,
													   &writer->dataWriter,
													   "deflate", blockSize,
													   meta) != 0)
	{
		ereport(ERROR, (errmsg("unable to open avro file: %s", avro_strerror())));
	}

	writer->initializedDataWriter = true;

	MemoryContextCallback *cb = MemoryContextAllocZero(CurrentMemoryContext,
													   sizeof(MemoryContextCallback));

	cb->func = (MemoryContextCallbackFunction) AvroFileWriterClose;
	cb->arg = writer;
	MemoryContextRegisterResetCallback(CurrentMemoryContext, cb);

	MemoryContextSwitchTo(oldContext);

	return writer;
}


/*
 * GetSchemaFromJson returns an Avro schema from the given JSON.
 */
static avro_schema_t
GetSchemaFromJson(const char *schemaJson)
{
	size_t		schemaLen = strlen(schemaJson);

	avro_schema_t schema = {0};

	if (avro_schema_from_json_length(schemaJson, schemaLen, &schema) != 0)
	{
		ereport(ERROR, (errmsg("unable to parse schema: %s", avro_strerror())));
	}

	return schema;
}


/*
 * AvroFileWriterClose is called via a MemoryContextCallback to make sure we
 * close the Avro file writer. Even though we use a custom allocator, it
 * does not propagate to some the libraries that libavro calls (e.g. libz).
 */
static void
AvroFileWriterClose(AvroWriter * writer)
{
	if (writer->initializedDataWriter)
	{
		avro_file_writer_flush(writer->dataWriter);
		avro_file_writer_close(writer->dataWriter);
	}

	writer->initializedDataWriter = false;
}


static void
WriteExtraMetadataToAvro(avro_value_t * record, ExtraMetadata * metadata)
{
	const char *metadataSchemaJson = "{\
										\"type\": \"record\", \
										\"name\": \"metadata\", \
										\"fields\": [ \
										  { \
										    \"type\": \"string\", \
										    \"name\": \"avroschema\" \
										  }, \
										  { \
										    \"type\": \"int\", \
										    \"name\": \"length\" \
										  } \
									    ] \
									 }";
	avro_schema_t schema = {0};

	if (avro_schema_from_json_length(metadataSchemaJson, strlen(metadataSchemaJson), &schema) != 0)
	{
		ereport(ERROR, (errmsg("unable to parse schema: %s", avro_strerror())));
	}

	avro_value_iface_t *iface = avro_generic_class_from_schema(schema);

	avro_generic_value_new(iface, record);

	AvroSetStringField(record, "avroschema", metadata->schema_json);
	AvroSetInt32Field(record, "length", metadata->schema_json_length);
}


/*
 * CreateAvroExtraMetadata creates an Avro record for extra metadata.
 * It currently only contains the JSON "avro.schema", which is used by the
 * Avro writer to properly specify field-id and default for fields.
 * If not set, the writer is not able to write field ids and defaults.
 */
static avro_value_t *
CreateAvroExtraMetadata(const char *schemaJson)
{
	size_t		schemaJsonLength = strlen(schemaJson);

	ExtraMetadata metadata;

	metadata.schema_json = schemaJson;
	metadata.schema_json_length = schemaJsonLength;

	avro_value_t *meta = palloc0(sizeof(avro_value_t));

	WriteExtraMetadataToAvro(meta, &metadata);

	return meta;
}


/*
 * AvroWriterWriteRecord writes a single record to the Avro file.
 */
void
AvroWriterWriteRecord(AvroWriter * writer, AvroSerializeFunction serializeFn, void *entry)
{
	avro_value_reset(&writer->record);
	serializeFn(entry, &writer->record);

	if (avro_file_writer_append_value(writer->dataWriter, &writer->record) != 0)
	{
		const char *errorString = avro_strerror();

		/* give a hint to the user if the block size was small for the write */
		if (strstr(errorString, "Value too large for file block size") != NULL)
		{
			ereport(ERROR, (errmsg("unable to write to avro file: %s", errorString),
							errdetail("Writing manifests with large Iceberg schema "
									  "requires a higher avro writer block size."),
							errhint("SET pg_lake_iceberg.default_avro_writer_block_size_kb TO a larger value.")));
		}
		else
		{
			ereport(ERROR, (errmsg("unable to write to avro file: %s", errorString)));
		}
	}
}


/*
 * AvroWriterClose releases all Avro-related resources.
 */
void
AvroWriterClose(AvroWriter * writer)
{
	AvroFileWriterClose(writer);
	FreeFile(writer->avroFile);
	MemoryContextDelete(writer->memoryContext);
}

/*
 * AvroSetNull sets a nullable field (union of [null,?]) to null.
 */
void
AvroSetNull(avro_value_t * record, char *fieldName)
{
	avro_value_t innerValue;
	bool		isSet = false;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);
}


void
AvroSetBoolField(avro_value_t * record, char *fieldName, bool value)
{
	avro_value_t avroValue;

	if (avro_value_get_by_name(record, fieldName, &avroValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	if (avro_value_set_boolean(&avroValue, value) != 0)
	{
		ereport(ERROR, (errmsg("could not set %s", fieldName)));
	}
}


void
AvroSetInt32Field(avro_value_t * record, char *fieldName, int32_t value)
{
	avro_value_t avroValue;

	if (avro_value_get_by_name(record, fieldName, &avroValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	if (avro_value_set_int(&avroValue, value) != 0)
	{
		ereport(ERROR, (errmsg("could not set %s", fieldName)));
	}
}


/*
 * AvroSetNullableInt32Field sets a union of types [null,int].
 */
void
AvroSetNullableInt32Field(avro_value_t * record, char *fieldName, int32_t value, bool isSet)
{
	avro_value_t innerValue;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);

	if (isSet)
	{
		if (avro_value_set_int(&innerValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s inner value", fieldName)));
		}
	}
}


/*
 * AvroSetBoolField sets a nullable field (union of [null,bool]) to a boolean value.
 */
void
AvroSetNullableBoolField(avro_value_t * record, char *fieldName, bool value, bool isSet)
{
	avro_value_t innerValue;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);

	if (isSet)
	{
		if (avro_value_set_boolean(&innerValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s inner value", fieldName)));
		}
	}
}


/*
  * AvroSetFloatField sets a nullable field (union of [null,float]) to a float value.
  */
void
AvroSetNullableFloatField(avro_value_t * record, char *fieldName, float4 value, bool isSet)
{
	avro_value_t innerValue;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);

	if (isSet)
	{
		if (avro_value_set_float(&innerValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s inner value", fieldName)));
		}
	}
}


 /*
  * AvroSetNullableDoubleField sets a nullable field (union of [null,double])
  * to a double value.
  */
void
AvroSetNullableDoubleField(avro_value_t * record, char *fieldName, float8 value, bool isSet)
{
	avro_value_t innerValue;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);

	if (isSet)
	{
		if (avro_value_set_double(&innerValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s inner value", fieldName)));
		}
	}
}


void
AvroSetInt64Field(avro_value_t * record, char *fieldName, int64_t value)
{
	avro_value_t avroValue;

	if (avro_value_get_by_name(record, fieldName, &avroValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	if (avro_value_set_long(&avroValue, value) != 0)
	{
		ereport(ERROR, (errmsg("could not set %s", fieldName)));
	}
}


/*
 * AvroSetNullableInt64Field sets a union of types [null,long].
 */
void
AvroSetNullableInt64Field(avro_value_t * record, char *fieldName, int64_t value, bool isSet)
{
	avro_value_t innerValue;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);

	if (isSet)
	{
		if (avro_value_set_long(&innerValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s inner value", fieldName)));
		}
	}
}


void
AvroSetRecordField(avro_value_t * record, char *fieldName, AvroSerializeFunction serializeFn, void *entry)
{
	avro_value_t avroValue;

	if (avro_value_get_by_name(record, fieldName, &avroValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	serializeFn(entry, &avroValue);
}


void
AvroSetStringField(avro_value_t * record, char *fieldName, const char *value)
{
	avro_value_t avroValue;

	if (avro_value_get_by_name(record, fieldName, &avroValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	if (avro_value_set_string(&avroValue, value) != 0)
	{
		ereport(ERROR, (errmsg("could not set %s", fieldName)));
	}
}


/*
 * AvroSetNullableStringField sets a union of types [null,string].
 */
void
AvroSetNullableStringField(avro_value_t * record, char *fieldName, const char *value, bool isSet)
{
	avro_value_t innerValue;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);

	if (isSet)
	{
		if (avro_value_set_string(&innerValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s inner value", fieldName)));
		}
	}
}


void
AvroSetBinaryField(avro_value_t * record, char *fieldName, const void *bytes, size_t length)
{
	avro_value_t avroValue;

	if (avro_value_get_by_name(record, fieldName, &avroValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	if (avro_value_set_bytes(&avroValue, (void *) bytes, length) != 0)
	{
		ereport(ERROR, (errmsg("could not set %s", fieldName)));
	}
}


/*
 * AvroSetNullableBinaryField sets a union of types [null,bytes].
 */
void
AvroSetNullableBinaryField(avro_value_t * record, char *fieldName, const void *bytes, size_t length, bool isSet)
{
	avro_value_t innerValue;

	AvroPrepareSetNullable(record, fieldName, &innerValue, isSet);

	if (isSet)
	{
		if (avro_value_set_bytes(&innerValue, (void *) bytes, length) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s inner value", fieldName)));
		}
	}
}


/*
 * AvroPrepareSetNullable sets a record field to a [null,?] union and sets the inner
 * value to the right branch of the union. When isNUll is true, it also sets the
 * inner value to null.
 */
static void
AvroPrepareSetNullable(avro_value_t * record, char *fieldName, avro_value_t * innerValue, bool isSet)
{
	avro_value_t unionValue;
	int			discriminant = isSet ? 1 : 0;

	if (avro_value_get_by_name(record, fieldName, &unionValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	if (avro_value_get_type(&unionValue) != AVRO_UNION)
	{
		ereport(ERROR, (errmsg("%s is not a nullable field", fieldName)));
	}

	if (avro_value_set_branch(&unionValue, discriminant, innerValue) != 0)
	{
		ereport(ERROR, (errmsg("could not set %s union", fieldName)));
	}

	if (!isSet)
	{
		if (avro_value_set_null(innerValue) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s to null", fieldName)));
		}
	}
}


void
AvroSetRecordArrayField(avro_value_t * record, char *fieldName, AvroSerializeFunction serializeFn, size_t entrySize, void *entryArray, size_t entryCount)
{
	avro_value_t arrayValue;
	bool		isSet = entryCount > 0;


	/* behave like a Nullable function when empty */
	AvroPrepareSetNullable(record, fieldName, &arrayValue, isSet);

	if (!isSet)
	{
		/* already set to NULL */
		return;
	}

	char	   *entries = (char *) entryArray;

	for (int entryIndex = 0; entryIndex < entryCount; entryIndex++)
	{
		avro_value_t avroValue;

		if (avro_value_append(&arrayValue, &avroValue, NULL) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s", fieldName)));
		}

		serializeFn((void *) entries, &avroValue);
		entries += entrySize;
	}
}


void
AvroSetInt32ArrayField(avro_value_t * record, char *fieldName, int32_t *values, size_t valueCount)
{
	avro_value_t arrayValue;
	bool		isSet = valueCount > 0;


	/* behave like a Nullable function when empty */
	AvroPrepareSetNullable(record, fieldName, &arrayValue, isSet);

	if (!isSet)
	{
		/* already set to NULL */
		return;
	}

	for (int valueIndex = 0; valueIndex < valueCount; valueIndex++)
	{
		int32_t		value = values[valueIndex];

		avro_value_t avroValue;

		if (avro_value_append(&arrayValue, &avroValue, NULL) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s", fieldName)));
		}

		if (avro_value_set_int(&avroValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s", fieldName)));
		}
	}
}


void
AvroSetInt64ArrayField(avro_value_t * record, char *fieldName, int64_t *values, size_t valueCount)
{
	avro_value_t arrayValue;
	bool		isSet = valueCount > 0;


	/* behave like a Nullable function when empty */
	AvroPrepareSetNullable(record, fieldName, &arrayValue, isSet);

	if (!isSet)
	{
		/* already set to NULL */
		return;
	}

	for (int valueIndex = 0; valueIndex < valueCount; valueIndex++)
	{
		int64_t		value = values[valueIndex];

		avro_value_t avroValue;

		if (avro_value_append(&arrayValue, &avroValue, NULL) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s", fieldName)));
		}

		if (avro_value_set_long(&avroValue, value) != 0)
		{
			ereport(ERROR, (errmsg("could not set %s", fieldName)));
		}
	}
}
