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

#include "pg_lake/avro/avro_reader.h"
#include "storage/fd.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/palloc.h"


static void AvroFileReaderClose(AvroReader * reader);
static bool GetFieldValue(avro_value_t * record, char *fieldName, AvroFieldRequired required,
						  avro_value_t * fieldValue);
static bool GetFieldArrayValue(avro_value_t * record, char *fieldName, AvroFieldRequired required,
							   avro_value_t * fieldValue, size_t *itemCount);
static bool AvroPrepareGetNullable(avro_value_t * record, char *fieldName,
								   avro_value_t * innerValue);
static avro_type_t AvroGetUnionNotNullType(avro_value_t * unionValue);


/*
 * AvroReaderCreate creates an Avro reader for the given file by using writer's schema.
 */
AvroReader *
AvroReaderCreate(const char *filePath)
{
	MemoryContext readerMemoryContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "AvroReader",
							  ALLOCSET_DEFAULT_SIZES);
	MemoryContext oldContext = MemoryContextSwitchTo(readerMemoryContext);

	AvroReader *reader = palloc0(sizeof(AvroReader));

	reader->memoryContext = readerMemoryContext;

	reader->avroFile = AllocateFile(filePath, PG_BINARY_R);
	if (reader->avroFile == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not open file \"%s\": %m",
							   filePath)));
	}

	if (avro_file_reader_fp(reader->avroFile, filePath, 0, &reader->dataReader) != 0)
	{
		ereport(ERROR, (errmsg("unable to open avro file: %s", avro_strerror())));
	}

	reader->initializedDataReader = true;

	MemoryContextCallback *cb = MemoryContextAllocZero(CurrentMemoryContext,
													   sizeof(MemoryContextCallback));

	cb->func = (MemoryContextCallbackFunction) AvroFileReaderClose;
	cb->arg = reader;
	MemoryContextRegisterResetCallback(CurrentMemoryContext, cb);

	if (avro_file_reader_json_schema(filePath, &reader->jsonSchema) != 0)
	{
		ereport(ERROR, (errmsg("unable to get json schema: %s", avro_strerror())));
	}

	if (avro_schema_from_json(reader->jsonSchema, 0, &reader->dataSchema, NULL) != 0)
	{
		ereport(ERROR, (errmsg("unable to parse json schema: %s", avro_strerror())));
	}

	reader->dataInterface = avro_generic_class_from_schema(reader->dataSchema);

	MemoryContextSwitchTo(oldContext);

	return reader;
}


/*
 * AvroFileReaderClose is called via a MemoryContextCallback to make sure we
 * close the Avro file reader. Even though we use a custom allocator, it
 * does not propagate to some the libraries that libavro calls (e.g. libz).
 */
static void
AvroFileReaderClose(AvroReader * reader)
{
	if (reader->initializedDataReader)
		avro_file_reader_close(reader->dataReader);

	reader->initializedDataReader = false;
}


/*
 * AvroReaderReadRecord reads a single record from the Avro file and passes it
 * to the parse function.
 */
bool
AvroReaderReadRecord(AvroReader * reader, AvroParseFunction parseFn, void *entry, void *context)
{
	/*
	 * always start with a new top level record, otherwise we will get
	 * duplicate values for some of the fields of different records
	 */
	avro_value_t record;

	avro_generic_value_new(reader->dataInterface, &record);

	int			rc = avro_file_reader_read_value(reader->dataReader, &record);

	if (rc == EOF)
	{
		return false;
	}

	if (rc != 0)
	{
		ereport(ERROR, (errmsg("unable to read avro record: %s", avro_strerror())));
	}

	parseFn(&record, entry, context);

	return true;
}


/*
 * AvroReaderClose releases all Avro-related resources.
 */
void
AvroReaderClose(AvroReader * reader)
{
	AvroFileReaderClose(reader);
	FreeFile(reader->avroFile);
	MemoryContextDelete(reader->memoryContext);
}

void
AvroGetBoolField(avro_value_t * record, char *fieldName, AvroFieldRequired required, bool *boolPointer)
{
	avro_value_t fieldValue;

	if (!GetFieldValue(record, fieldName, required, &fieldValue))
	{
		*boolPointer = false;
		return;
	}

	if (avro_value_get_boolean(&fieldValue, (int *) boolPointer) != 0)
	{
		ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
	}
}


void
AvroGetInt32Field(avro_value_t * record, char *fieldName, AvroFieldRequired required, int32_t *intPointer)
{
	avro_value_t fieldValue;

	if (!GetFieldValue(record, fieldName, required, &fieldValue))
	{
		*intPointer = 0L;
		return;
	}

	if (avro_value_get_int(&fieldValue, intPointer) != 0)
	{
		ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
	}
}


void
AvroGetNullableInt32Field(avro_value_t * record, char *fieldName, int32_t *intPointer, bool *isSet)
{
	avro_value_t fieldValue;

	if ((*isSet = AvroPrepareGetNullable(record, fieldName, &fieldValue)))
	{
		if (avro_value_get_int(&fieldValue, intPointer) != 0)
		{
			ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
		}
	}
}


void
AvroGetInt64Field(avro_value_t * record, char *fieldName, AvroFieldRequired required, int64_t *longPointer)
{
	avro_value_t fieldValue;

	if (!GetFieldValue(record, fieldName, required, &fieldValue))
	{
		*longPointer = 0L;
		return;
	}

	if (avro_value_get_long(&fieldValue, longPointer) != 0)
	{
		ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
	}
}


void
AvroGetNullableInt64Field(avro_value_t * record, char *fieldName, int64_t *longPointer, bool *isSet)
{
	avro_value_t fieldValue;

	if ((*isSet = AvroPrepareGetNullable(record, fieldName, &fieldValue)))
	{
		if (avro_value_get_long(&fieldValue, longPointer) != 0)
		{
			ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
		}
	}
}


void
AvroGetStringField(avro_value_t * record, char *fieldName, AvroFieldRequired required, const char **stringPointer,
				   size_t *lengthPointer)
{
	avro_value_t fieldValue;

	if (!GetFieldValue(record, fieldName, required, &fieldValue))
	{
		*stringPointer = NULL;
		*lengthPointer = 0L;
		return;
	}

	if (avro_value_get_string(&fieldValue, stringPointer, lengthPointer) != 0)
	{
		ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
	}
}


void
AvroGetNullableStringField(avro_value_t * record, char *fieldName,
						   const char **stringPointer, size_t *lengthPointer,
						   bool *isSet)
{
	avro_value_t fieldValue;

	if ((*isSet = AvroPrepareGetNullable(record, fieldName, &fieldValue)))
	{
		if (avro_value_get_string(&fieldValue, stringPointer, lengthPointer) != 0)
		{
			ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
		}
	}
}


void
AvroGetBinaryField(avro_value_t * record, char *fieldName, AvroFieldRequired required, const void **bytesPointer,
				   size_t *lengthPointer)
{
	avro_value_t fieldValue;

	if (!GetFieldValue(record, fieldName, required, &fieldValue))
	{
		*bytesPointer = NULL;
		*lengthPointer = 0L;
		return;
	}

	if (avro_value_get_bytes(&fieldValue, bytesPointer, lengthPointer) != 0)
	{
		ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
	}
}


void
AvroGetNullableBinaryField(avro_value_t * record, char *fieldName,
						   const void **bytesPointer, size_t *lengthPointer,
						   bool *isSet)
{
	avro_value_t fieldValue;

	if ((*isSet = AvroPrepareGetNullable(record, fieldName, &fieldValue)))
	{
		if (avro_value_get_bytes(&fieldValue, bytesPointer, lengthPointer) != 0)
		{
			ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
		}
	}
}


void
AvroGetRecordField(avro_value_t * record, char *fieldName, AvroFieldRequired required, AvroParseFunction parseFn,
				   void *entry, void *context)
{
	avro_value_t fieldValue;

	GetFieldValue(record, fieldName, required, &fieldValue);
	parseFn(&fieldValue, entry, context);
}


void
AvroGetObjectArrayField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
						AvroParseFunction parseFn, size_t entrySize,
						void **arrayPointer, size_t *lengthPointer, void *context)
{
	avro_value_t fieldValue;
	size_t		itemCount;

	if (!GetFieldArrayValue(record, fieldName, required, &fieldValue, &itemCount))
	{
		*arrayPointer = NULL;
		*lengthPointer = 0L;
		return;
	}

	/* allocate N entries */
	char	   *entries = palloc0(entrySize * itemCount);

	*arrayPointer = entries;
	*lengthPointer = itemCount;

	for (int itemIndex = 0; itemIndex < itemCount; itemIndex++)
	{
		avro_value_t child;

		if (avro_value_get_by_index(&fieldValue, itemIndex, &child, NULL) != 0)
		{
			ereport(ERROR, (errmsg("could not get array element %d", itemIndex)));
		}

		parseFn(&child, (void *) entries, context);

		entries += entrySize;
	}
}


void
AvroGetInt32ArrayField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
					   int32_t **arrayPointer, size_t *lengthPointer)
{
	avro_value_t fieldValue;
	size_t		itemCount;
	int			itemIndex;

	if (!GetFieldArrayValue(record, fieldName, required, &fieldValue, &itemCount))
	{
		*arrayPointer = NULL;
		*lengthPointer = 0L;
		return;
	}

	/* allocate N entries */
	int32_t    *entries = palloc0(sizeof(int32_t) * itemCount);

	*arrayPointer = entries;
	*lengthPointer = itemCount;

	for (itemIndex = 0; itemIndex < itemCount; itemIndex++)
	{
		avro_value_t child;

		if (avro_value_get_by_index(&fieldValue, itemIndex, &child, NULL) != 0)
		{
			ereport(ERROR, (errmsg("could not get array element %d", itemIndex)));
		}

		if (avro_value_get_int(&child, &entries[itemIndex]) != 0)
		{
			ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
		}

		entries++;
	}
}


void
AvroGetInt64ArrayField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
					   int64_t **arrayPointer, size_t *lengthPointer)
{
	avro_value_t fieldValue;
	size_t		itemCount;
	int			itemIndex;

	if (!GetFieldArrayValue(record, fieldName, required, &fieldValue, &itemCount))
	{
		*arrayPointer = NULL;
		*lengthPointer = 0L;
		return;
	}

	/* allocate N entries */
	int64_t    *entries = palloc0(sizeof(int64_t) * itemCount);

	*arrayPointer = entries;
	*lengthPointer = itemCount;

	for (itemIndex = 0; itemIndex < itemCount; itemIndex++)
	{
		avro_value_t child;

		if (avro_value_get_by_index(&fieldValue, itemIndex, &child, NULL) != 0)
		{
			ereport(ERROR, (errmsg("could not get array element %d", itemIndex)));
		}

		if (avro_value_get_long(&child, &entries[itemIndex]) != 0)
		{
			ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
		}
	}
}



void
AvroGetMapField(avro_value_t * record, char *fieldName, AvroFieldRequired required,
				AvroParseMapEntryFunction parseFn, size_t entrySize,
				void **arrayPointer, size_t *lengthPointer, void *context)
{
	avro_value_t fieldValue;
	size_t		itemCount;
	int			itemIndex;
	char	   *entries;

	if (!GetFieldValue(record, fieldName, required, &fieldValue))
	{
		*arrayPointer = NULL;
		*lengthPointer = 0L;
		return;
	}

	if (avro_value_get_size(&fieldValue, &itemCount) != 0)
	{
		ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
	}

	/* allocate N entries */
	entries = palloc0(entrySize * itemCount);

	*arrayPointer = entries;
	*lengthPointer = itemCount;

	for (itemIndex = 0; itemIndex < itemCount; itemIndex++)
	{
		avro_value_t child;
		const char *key = NULL;

		if (avro_value_get_by_index(&fieldValue, 0, &child, &key) != 0)
		{
			ereport(ERROR, (errmsg("could not get map element %d", itemIndex)));
		}

		parseFn(key, &child, (void *) entries, context);

		entries += entrySize;
	}
}

/*
 * AvroGetTotalRecordFields returns the total number of fields in a record.
 */
size_t
AvroGetTotalRecordFields(avro_value_t * record)
{
	Assert(avro_typeof(avro_value_get_schema(record)) == AVRO_RECORD);

	size_t		fieldCount = 0;

	if (avro_value_get_size(record, &fieldCount) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not get fields count")));
	}

	return fieldCount;
}


/*
 * AvroExtractNullableFieldFromRecordByIndex extracts the field from
 * nullable field of a record by index. It also sets information
 * about the field.
 */
void
AvroExtractNullableFieldFromRecordByIndex(avro_value_t * record, int index,
										  void **value, size_t *valueLength,
										  char **fieldName)
{
	Assert(avro_typeof(avro_value_get_schema(record)) == AVRO_RECORD);

	avro_value_t unionField;

	const char *name = NULL;

	if (avro_value_get_by_index(record, index, &unionField, &name) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not get nullable field at index %d", index)));
	}

	*fieldName = pstrdup(name);

	avro_type_t fieldType = AvroGetUnionNotNullType(&unionField);

	/* extract union */
	avro_value_t fieldValue;

	if (avro_value_get_current_branch(&unionField, &fieldValue) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not get nullable field")));
	}

	if (avro_value_get_type(&fieldValue) == AVRO_NULL)
	{
		*value = NULL;
		*valueLength = 0;
		return;
	}

	int			rc = 0;

	if (fieldType == AVRO_INT32)
	{
		*value = palloc0(sizeof(int32_t));
		*valueLength = sizeof(int32_t);
		rc = avro_value_get_int(&fieldValue, (int32_t *) *value);
	}
	else if (fieldType == AVRO_INT64)
	{
		*value = palloc0(sizeof(int64_t));
		*valueLength = sizeof(int64_t);
		rc = avro_value_get_long(&fieldValue, (int64_t *) *value);
	}
	else if (fieldType == AVRO_STRING)
	{
		rc = avro_value_get_string(&fieldValue, (const char **) value, valueLength);
	}
	else if (fieldType == AVRO_BYTES)
	{
		rc = avro_value_get_bytes(&fieldValue, (const void **) value, valueLength);
	}
	else if (fieldType == AVRO_FLOAT)
	{
		*value = palloc0(sizeof(float));
		*valueLength = sizeof(float);
		rc = avro_value_get_float(&fieldValue, (float *) *value);
	}
	else if (fieldType == AVRO_DOUBLE)
	{
		*value = palloc0(sizeof(double));
		*valueLength = sizeof(double);
		rc = avro_value_get_double(&fieldValue, (double *) *value);
	}
	else if (fieldType == AVRO_BOOLEAN)
	{
		/* avro_value_get_boolean requires int */
		*value = palloc0(sizeof(int));
		*valueLength = sizeof(int);
		rc = avro_value_get_boolean(&fieldValue, (int *) *value);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Not supported field type during extraction")));
	}

	if (rc != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to extract nullable field value %s", avro_strerror())));
	}
}

/*
 * AvroFieldExists checks if a field exists in a record.
 */
bool
AvroFieldExists(avro_value_t * record, char *fieldName)
{
	avro_value_t fieldValue;

	return GetFieldValue(record, fieldName, AVRO_FIELD_OPTIONAL, &fieldValue);
}


/*
 * GetFieldValue gets a value from a record by name and unpacks unions.
 * If the value is NULL, GetFieldValue returns false.
 */
static bool
GetFieldValue(avro_value_t * record, char *fieldName, AvroFieldRequired required, avro_value_t * fieldValue)
{
	avro_type_t actualType;

	if (avro_value_get_by_name(record, fieldName, fieldValue, NULL) != 0)
	{
		if (required == AVRO_FIELD_REQUIRED)
		{
			ereport(ERROR, (errmsg("missing %s field", fieldName)));
		}

		return false;
	}

	actualType = avro_value_get_type(fieldValue);
	if (actualType == AVRO_UNION)
	{
		/* TODO: replace will nullable logic */
		avro_value_t branch;

		if (avro_value_get_current_branch(fieldValue, &branch) != 0)
		{
			ereport(ERROR, (errmsg("could not read union %s: %s", fieldName, avro_strerror())));
		}

		*fieldValue = branch;
	}

	actualType = avro_value_get_type(fieldValue);
	if (actualType == AVRO_NULL)
	{
		return false;
	}

	return true;
}


/*
 * GetFieldArrayValue is a wrapper around GetFieldValue for arrays that
 * also gets the size (if not NULL).
 */
static bool
GetFieldArrayValue(avro_value_t * record, char *fieldName, AvroFieldRequired required,
				   avro_value_t * fieldValue, size_t *itemCount)
{

	if (!GetFieldValue(record, fieldName, required, fieldValue))
	{
		return false;
	}

	if (avro_value_get_size(fieldValue, itemCount) != 0)
	{
		ereport(ERROR, (errmsg("%s is not of expected type", fieldName)));
	}

	return true;
}


/*
 * AvroPrepareGetNullable is a helper function for getting the value
 * from a [null,?] union.
 */
static bool
AvroPrepareGetNullable(avro_value_t * record, char *fieldName, avro_value_t * innerValue)
{
	avro_value_t unionValue;

	if (avro_value_get_by_name(record, fieldName, &unionValue, NULL) != 0)
	{
		ereport(ERROR, (errmsg("%s not found in schema", fieldName)));
	}

	if (avro_value_get_type(&unionValue) != AVRO_UNION)
	{
		ereport(ERROR, (errmsg("%s is not a nullable field", fieldName)));
	}

	if (avro_value_get_current_branch(&unionValue, innerValue) != 0)
	{
		ereport(ERROR, (errmsg("could not read union %s: %s", fieldName, avro_strerror())));
	}

	return avro_value_get_type(innerValue) != AVRO_NULL;
}


/*
 * AvroGetUnionNotNullType returns the type of the non-null branch of a union.
 */
static avro_type_t
AvroGetUnionNotNullType(avro_value_t * unionValue)
{
	avro_schema_t schema = avro_value_get_schema(unionValue);

	if (avro_typeof(schema) != AVRO_UNION)
	{
		ereport(ERROR, (errmsg("expected union type")));
	}

	avro_schema_t firstSchema = avro_schema_union_branch(schema, 0);
	avro_schema_t otherSchema = avro_schema_union_branch(schema, 1);

	if (is_avro_null(firstSchema))
	{
		return avro_typeof(otherSchema);
	}
	else
	{
		return avro_typeof(firstSchema);
	}
}
