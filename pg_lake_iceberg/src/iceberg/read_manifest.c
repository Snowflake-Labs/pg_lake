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
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/wait_event.h"

#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/avro/avro_reader.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_lake/util/plan_cache.h"
#include "pg_extension_base/spi_helpers.h"


/* 63 is the identifier length limit in Postgres. */
#define	PARTITION_FIELD_NAME_MAX_LENGTH 100

/*
 * PartitionFieldIdMapEntry represents a mapping between a partition field name
 * and its field-id.
 */
typedef struct PartitionFieldIdMapEntry
{
	/*
	 * field ids are not available when parsing partition fields but field
	 * names are available. We built a map by using json avro.schema to build
	 * the field name => field id mapping, which is looked up when parsing
	 * partition fields.
	 */
	char		fieldName[PARTITION_FIELD_NAME_MAX_LENGTH];
	int32_t		fieldId;
	IcebergScalarAvroType fieldType;
}			PartitionFieldIdMapEntry;


/*
 * ManifestReaderContext is used to pass the partition field map to the
 * manifest reader.
 */
typedef struct ManifestReaderContext
{
	/* partition field name => field id mapping */
	HTAB	   *partitionFieldMap;
}			ManifestReaderContext;


static void ReadFieldSummaryFromAvro(avro_value_t * record, FieldSummary * summary, void *context);
static void ReadColumnStatFromAvro(avro_value_t * record, ColumnStat * stat, void *context);
static void ReadColumnBoundFromAvro(avro_value_t * record, ColumnBound * bound, void *context);
static void ReadDataFileFromAvro(avro_value_t * record, DataFile * dataFile, ManifestReaderContext * context);
static void ReadPartitionFromAvro(avro_value_t * record, Partition * partition, ManifestReaderContext * context);
static void ReadIcebergManifestFromAvro(avro_value_t * record, IcebergManifest * manifest, void *context);
static void ReadIcebergManifestEntryFromAvro(avro_value_t * record, IcebergManifestEntry * entry,
											 ManifestReaderContext * context);
static HTAB *CreateManifestPartitionFieldMap(AvroReader * manifestReader);
static IcebergScalarAvroType IcebergAvroTypeFromString(const char *physicalTypeName, const char *logicalTypeName);


/*
 * ReadIcebergManifests reads manifests from manifest list path.
 */
List *
ReadIcebergManifests(const char *manifestListPath)
{
	size_t		contentLength = 0;
	char	   *manifestListBlob = GetBlobFromURI(manifestListPath, &contentLength);

	/*
	 * TODO: replace below hack via avro memory api? tempfile will be closed
	 * at the end of the transaction.
	 */
	File		manifestListLocalFile = OpenTemporaryFile(false);
	const char *manifestListLocalPath = FilePathName(manifestListLocalFile);

	int			writtenLength = FileWrite(manifestListLocalFile, manifestListBlob, contentLength, 0, WAIT_EVENT_COPY_FILE_WRITE);

	if (writtenLength != contentLength)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to write manifest list to temporary file")));
	}

	List	   *manifests = NIL;
	IcebergManifest *manifest = palloc0(sizeof(IcebergManifest));
	AvroReader *manifestListReader = AvroReaderCreate(manifestListLocalPath);

	void	   *context = NULL;

	while (AvroReaderReadRecord(manifestListReader,
								(AvroParseFunction) ReadIcebergManifestFromAvro,
								manifest, context))
	{
		manifests = lappend(manifests, manifest);
		manifest = palloc0(sizeof(IcebergManifest));
	}

	AvroReaderClose(manifestListReader);
	FileClose(manifestListLocalFile);

	return manifests;
}

/*
 * ReadManifestEntries reads an Iceberg manifest file and
 * returns a list of manifest entries.
 */
List *
ReadManifestEntries(const char *manifestPath)
{
	size_t		contentLength = 0;
	char	   *manifestBlob = GetBlobFromURI(manifestPath, &contentLength);

	/*
	 * TODO: replace below hack via avro memory api? tempfile will be closed
	 * at the end of the transaction.
	 */
	File		manifestLocalFile = OpenTemporaryFile(false);
	const char *manifestLocalPath = FilePathName(manifestLocalFile);

	int			writtenLength = FileWrite(manifestLocalFile, manifestBlob, contentLength, 0, WAIT_EVENT_COPY_FILE_WRITE);

	if (writtenLength != contentLength)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Failed to write manifest list to temporary file")));
	}

	List	   *manifestEntries = NIL;
	IcebergManifestEntry *manifestEntry = palloc0(sizeof(IcebergManifestEntry));
	AvroReader *manifestReader = AvroReaderCreate(manifestLocalPath);

	ManifestReaderContext *context = palloc0(sizeof(ManifestReaderContext));

	context->partitionFieldMap = CreateManifestPartitionFieldMap(manifestReader);

	while (AvroReaderReadRecord(manifestReader,
								(AvroParseFunction) ReadIcebergManifestEntryFromAvro,
								manifestEntry, context))
	{
		manifestEntries = lappend(manifestEntries, manifestEntry);
		manifestEntry = palloc0(sizeof(IcebergManifestEntry));
	}

	AvroReaderClose(manifestReader);
	FileClose(manifestLocalFile);

	return manifestEntries;
}

static void
ReadIcebergManifestFromAvro(avro_value_t * record, IcebergManifest * manifest, void *context)
{
	memset(manifest, '\0', sizeof(IcebergManifest));
	AvroGetStringField(record, "manifest_path", AVRO_FIELD_REQUIRED, &manifest->manifest_path, &manifest->manifest_path_length);
	AvroGetInt64Field(record, "manifest_length", AVRO_FIELD_REQUIRED, &manifest->manifest_length);
	AvroGetInt32Field(record, "partition_spec_id", AVRO_FIELD_REQUIRED, &manifest->partition_spec_id);
	AvroGetInt32Field(record, "content", AVRO_FIELD_REQUIRED, (int32_t *) &manifest->content);
	AvroGetInt64Field(record, "sequence_number", AVRO_FIELD_REQUIRED, &manifest->sequence_number);
	AvroGetInt64Field(record, "min_sequence_number", AVRO_FIELD_REQUIRED, &manifest->min_sequence_number);
	AvroGetInt64Field(record, "added_snapshot_id", AVRO_FIELD_REQUIRED, &manifest->added_snapshot_id);

	/* Reference implementation uses data_files, spec says files */
	if (AvroFieldExists(record, "added_data_files_count"))
	{
		AvroGetInt32Field(record, "added_data_files_count", AVRO_FIELD_REQUIRED, &manifest->added_files_count);
	}
	else
	{
		AvroGetInt32Field(record, "added_files_count", AVRO_FIELD_REQUIRED, &manifest->added_files_count);
	}

	/* Reference implementation uses data_files, spec says files */
	if (AvroFieldExists(record, "existing_data_files_count"))
	{
		AvroGetInt32Field(record, "existing_data_files_count", AVRO_FIELD_REQUIRED, &manifest->existing_files_count);
	}
	else
	{
		AvroGetInt32Field(record, "existing_files_count", AVRO_FIELD_REQUIRED, &manifest->existing_files_count);
	}

	/* Reference implementation uses data_files, spec says files */
	if (AvroFieldExists(record, "deleted_data_files_count"))
	{
		AvroGetInt32Field(record, "deleted_data_files_count", AVRO_FIELD_REQUIRED, &manifest->deleted_files_count);
	}
	else
	{
		AvroGetInt32Field(record, "deleted_files_count", AVRO_FIELD_REQUIRED, &manifest->deleted_files_count);
	}

	AvroGetInt64Field(record, "added_rows_count", AVRO_FIELD_REQUIRED, &manifest->added_rows_count);
	AvroGetInt64Field(record, "existing_rows_count", AVRO_FIELD_REQUIRED, &manifest->existing_rows_count);
	AvroGetInt64Field(record, "deleted_rows_count", AVRO_FIELD_REQUIRED, &manifest->deleted_rows_count);
	AvroGetObjectArrayField(record, "partitions", AVRO_FIELD_OPTIONAL,
							(AvroParseFunction) ReadFieldSummaryFromAvro,
							sizeof(FieldSummary),
							(void **) &manifest->partitions,
							&manifest->partitions_length, context);

	AvroGetBinaryField(record, "key_metadata", AVRO_FIELD_OPTIONAL, (const
																	 void **) &manifest->key_metadata, &manifest->key_metadata_length);
}


static void
ReadIcebergManifestEntryFromAvro(avro_value_t * record, IcebergManifestEntry * entry, ManifestReaderContext * context)
{
	memset(entry, '\0', sizeof(IcebergManifestEntry));
	AvroGetInt32Field(record, "status", AVRO_FIELD_REQUIRED, (int32_t *) &entry->status);
	AvroGetNullableInt64Field(record, "snapshot_id", &entry->snapshot_id, &entry->has_snapshot_id);
	AvroGetNullableInt64Field(record, "sequence_number", &entry->sequence_number, &entry->has_sequence_number);
	AvroGetNullableInt64Field(record, "file_sequence_number", &entry->file_sequence_number, &entry->has_file_sequence_number);
	AvroGetRecordField(record, "data_file", AVRO_FIELD_REQUIRED, (AvroParseFunction) ReadDataFileFromAvro, &entry->data_file, context);
}


static void
ReadDataFileFromAvro(avro_value_t * record, DataFile * dataFile, ManifestReaderContext * context)
{
	memset(dataFile, '\0', sizeof(DataFile));
	AvroGetInt32Field(record, "content", AVRO_FIELD_REQUIRED, (int32_t *) &dataFile->content);
	AvroGetStringField(record, "file_path", AVRO_FIELD_REQUIRED, &dataFile->file_path, &dataFile->file_path_length);
	AvroGetStringField(record, "file_format", AVRO_FIELD_REQUIRED, &dataFile->file_format, &dataFile->file_format_length);
	AvroGetRecordField(record, "partition", AVRO_FIELD_REQUIRED, (AvroParseFunction) ReadPartitionFromAvro, &dataFile->partition, context);
	AvroGetInt64Field(record, "record_count", AVRO_FIELD_REQUIRED, &dataFile->record_count);
	AvroGetInt64Field(record, "file_size_in_bytes", AVRO_FIELD_REQUIRED, &dataFile->file_size_in_bytes);
	AvroGetObjectArrayField(record, "column_sizes", AVRO_FIELD_OPTIONAL,
							(AvroParseFunction) ReadColumnStatFromAvro,
							sizeof(ColumnStat),
							(void **) &dataFile->column_sizes,
							&dataFile->column_sizes_length, context);
	AvroGetObjectArrayField(record, "value_counts", AVRO_FIELD_OPTIONAL,
							(AvroParseFunction) ReadColumnStatFromAvro,
							sizeof(ColumnStat),
							(void **) &dataFile->value_counts,
							&dataFile->value_counts_length, context);
	AvroGetObjectArrayField(record, "null_value_counts", AVRO_FIELD_OPTIONAL,
							(AvroParseFunction) ReadColumnStatFromAvro,
							sizeof(ColumnStat),
							(void **) &dataFile->null_value_counts,
							&dataFile->null_value_counts_length, context);
	AvroGetObjectArrayField(record, "nan_value_counts", AVRO_FIELD_OPTIONAL,
							(AvroParseFunction) ReadColumnStatFromAvro,
							sizeof(ColumnStat),
							(void **) &dataFile->nan_value_counts,
							&dataFile->nan_value_counts_length, context);
	AvroGetObjectArrayField(record, "lower_bounds", AVRO_FIELD_OPTIONAL,
							(AvroParseFunction) ReadColumnBoundFromAvro,
							sizeof(ColumnBound),
							(void **) &dataFile->lower_bounds,
							&dataFile->lower_bounds_length, context);
	AvroGetObjectArrayField(record, "upper_bounds", AVRO_FIELD_OPTIONAL,
							(AvroParseFunction) ReadColumnBoundFromAvro,
							sizeof(ColumnBound),
							(void **) &dataFile->upper_bounds,
							&dataFile->upper_bounds_length, context);
	AvroGetBinaryField(record, "key_metadata", AVRO_FIELD_OPTIONAL,
					   (const void **) &dataFile->key_metadata, &dataFile->key_metadata_length);
	AvroGetInt64ArrayField(record, "split_offsets", AVRO_FIELD_OPTIONAL,
						   &dataFile->split_offsets, &dataFile->split_offsets_length);
	AvroGetInt32ArrayField(record, "equality_ids", AVRO_FIELD_OPTIONAL,
						   &dataFile->equality_ids, &dataFile->equality_ids_length);
	AvroGetNullableInt32Field(record, "sort_order_id",
							  &dataFile->sort_order_id, &dataFile->has_sort_order_id);
}


/*
 * ReadPartitionFromAvro reads the partition from the avro record.
 */
static void
ReadPartitionFromAvro(avro_value_t * partitionRecord, Partition * partition, ManifestReaderContext * context)
{
	memset(partition, '\0', sizeof(Partition));

	size_t		partitionFieldsCount = AvroGetTotalRecordFields(partitionRecord);

	if (partitionFieldsCount == 0)
	{
		return;
	}

	PartitionField *partitionFields = palloc0(sizeof(PartitionField) * partitionFieldsCount);

	for (size_t partitionFieldIndex = 0; partitionFieldIndex < partitionFieldsCount; partitionFieldIndex++)
	{
		PartitionField *partitionField = &partitionFields[partitionFieldIndex];

		/* each partition field should be union type e.g. [null, int] */
		AvroExtractNullableFieldFromRecordByIndex(partitionRecord,
												  (int) partitionFieldIndex,
												  &partitionField->value,
												  &partitionField->value_length,
												  &partitionField->field_name);

		/* find partition field id for given partition field name */
		bool		found = false;

		PartitionFieldIdMapEntry *entry = hash_search(context->partitionFieldMap,
													  partitionField->field_name, HASH_FIND, &found);

		if (!found)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Partition field name %s not found in the partition field map", partitionField->field_name)));
		}
		else
		{
			partitionField->field_id = entry->fieldId;
			partitionField->value_type = entry->fieldType;
		}
	}

	partition->fields = partitionFields;
	partition->fields_length = partitionFieldsCount;
}


/*
 * IcebergAvroTypeFromString gets avro type from given physical and logical type names.
 */
static IcebergScalarAvroType
IcebergAvroTypeFromString(const char *physicalTypeName, const char *logicalTypeName)
{
	IcebergScalarAvroType type = {0};

	if (strcmp(physicalTypeName, "int") == 0)
	{
		type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT32;

		if (logicalTypeName == NULL)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
		}
		else if (strcmp(logicalTypeName, "date") == 0)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_DATE;
		}
	}
	else if (strcmp(physicalTypeName, "long") == 0)
	{
		type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT64;

		if (logicalTypeName == NULL)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
		}
		else if (strcmp(logicalTypeName, "timestamp") == 0)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_TIMESTAMP;
		}
		else if (strcmp(logicalTypeName, "time") == 0)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_TIME;
		}
	}
	else if (strcmp(physicalTypeName, "string") == 0)
	{
		type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_STRING;
		type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
	}
	else if (strcmp(physicalTypeName, "bytes") == 0)
	{
		type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_BINARY;

		if (logicalTypeName == NULL)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
		}
		else if (strcmp(logicalTypeName, "decimal") == 0)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_DECIMAL;
		}
		else if (strcmp(logicalTypeName, "uuid") == 0)
		{
			type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_UUID;
		}
	}
	else if (strcmp(physicalTypeName, "float") == 0)
	{
		type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT;
		type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
	}
	else if (strcmp(physicalTypeName, "double") == 0)
	{
		type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE;
		type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
	}
	else if (strcmp(physicalTypeName, "boolean") == 0)
	{
		type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_BOOL;
		type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
	}

	return type;
}

/*
 * CreateManifestPartitionFieldMap creates a partition field id mapping for current manifest reader.
 */
static HTAB *
CreateManifestPartitionFieldMap(AvroReader * manifestReader)
{
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));

	hashCtl.keysize = PARTITION_FIELD_NAME_MAX_LENGTH;
	hashCtl.entrysize = sizeof(PartitionFieldIdMapEntry);
	hashCtl.hcxt = CurrentMemoryContext;

	HTAB	   *partitionFieldMap = hash_create("Partition Field Map", 32, &hashCtl, HASH_STRINGS | HASH_ELEM | HASH_CONTEXT);

	/* populate the map from manifest json schema */
	const char *partitionFieldIdQuery = psprintf(
	/* 1) Provide the JSON as a JSONB value */
												 "WITH j AS ( "
												 " SELECT $1::jsonb AS doc "
												 "), "

	/* 2) Unnest top-level fields */
												 "top_fields AS ( "
												 " SELECT jsonb_array_elements(doc->'fields') AS field_obj "
												 " FROM j "
												 "), "

	/* 3) Locate the object where "name" = "data_file" */
												 "data_file_obj AS ( "
												 " SELECT (field_obj->'type'->'fields') AS data_file_fields "
												 " FROM top_fields WHERE field_obj->>'name' = 'data_file' "
												 "), "

	/* 4) Unnest the array of fields within data_file */
												 "data_file_fields AS ( "
												 " SELECT jsonb_array_elements(data_file_fields) AS df_field "
												 " FROM data_file_obj"
												 "), "

	/* 5) Find the object where "name" = "partition" */
												 "partition_obj AS ( "
												 " SELECT (df_field->'type'->'fields') AS partition_fields "
												 " FROM data_file_fields WHERE df_field->>'name' = 'partition'"
												 "), "

	/* 6) Finally unnest the partition's fields array */
												 "partition_fields_unrolled AS ( "
												 " SELECT jsonb_array_elements(partition_fields) AS part_field "
												 " FROM partition_obj"
												 ") "

	/*
	 * 7) Extract "name", "field-id", "type" (physical and logical if any)
	 * from each partition field.
	 */
												 "SELECT part_field->>'name'                   AS partition_field_name,"
												 "       part_field->>'field-id'               AS partition_field_id,"
												 "       CASE "
	/* "type" could be JSON string like ["null", "int"] */
												 "	      WHEN jsonb_typeof(part_field->'type'->1) = 'string' THEN part_field->'type'->>1  "

	/*
	 * "type" could be JSON object like ["null", {"type": "int",
	 * "logicalType": "date"}]
	 */
												 "	      ELSE part_field->'type'->1->>'type' "
												 "       END                                   AS partition_field_physical_type,"
												 "       part_field->'type'->1->>'logicalType' AS partition_field_logical_type "
												 "FROM partition_fields_unrolled;");

	MemoryContext currentContext = CurrentMemoryContext;

	SPI_START();

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, TEXTOID, manifestReader->jsonSchema, false);

	Snapshot	snapshot = NULL;

	if (ActiveSnapshotSet())
	{
		snapshot = GetActiveSnapshot();
	}

	if (!snapshot)
	{
		bool		readOnly = true;

		SPI_EXECUTE(partitionFieldIdQuery, readOnly);
	}
	else
	{
		SPIPlanPtr	qplan = GetCachedQueryPlan(partitionFieldIdQuery, spiArgCount, spiArgTypes);

		if (qplan == NULL)
			elog(ERROR, "SPI_prepare returned %s while fetching partition field id",
				 SPI_result_code_string(SPI_result));

		bool		readOnly = true;
		bool		fireTriggers = false;
		int			spi_result =
			SPI_execute_snapshot(qplan,
								 spiArgValues, spiArgNulls,
								 snapshot,
								 InvalidSnapshot,
								 readOnly, fireTriggers, 0);

		/* Check result */
		if (spi_result != SPI_OK_SELECT)
			elog(ERROR, "SPI_execute_snapshot returned %s", SPI_result_code_string(spi_result));

	}

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(currentContext);

		bool		isNull = false;

		Datum		fieldNameDatum = GET_SPI_DATUM(rowIndex, 1, &isNull);
		const char *fieldName = NULL;

		if (!isNull)
		{
			fieldName = TextDatumGetCString(fieldNameDatum);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Partition field name is null")));
		}

		Datum		fieldIdDatum = GET_SPI_DATUM(rowIndex, 2, &isNull);
		const char *fieldId = NULL;

		if (!isNull)
		{
			fieldId = TextDatumGetCString(fieldIdDatum);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Partition field id is null")));
		}

		Datum		physicalTypeNameDatum = GET_SPI_DATUM(rowIndex, 3, &isNull);
		const char *physicalTypeName = NULL;

		if (!isNull)
		{
			physicalTypeName = TextDatumGetCString(physicalTypeNameDatum);
		}
		else
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Partition field physical type is null")));
		}

		Datum		logicalTypeNameDatum = GET_SPI_DATUM(rowIndex, 4, &isNull);
		const char *logicalTypeName = NULL;

		if (!isNull)
		{
			logicalTypeName = TextDatumGetCString(logicalTypeNameDatum);
		}

		bool		fieldFound = false;

		PartitionFieldIdMapEntry *entry = hash_search(partitionFieldMap, fieldName, HASH_ENTER, &fieldFound);

		Assert(!fieldFound);

		entry->fieldId = atoi(fieldId);
		entry->fieldType = IcebergAvroTypeFromString(physicalTypeName, logicalTypeName);

		if (strlen(fieldName) > PARTITION_FIELD_NAME_MAX_LENGTH)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("Partition field name %s is too long with length %d",
							fieldName, PARTITION_FIELD_NAME_MAX_LENGTH)));
		}

		strcpy(entry->fieldName, fieldName);

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return partitionFieldMap;
}


static void
ReadFieldSummaryFromAvro(avro_value_t * record, FieldSummary * summary, void *context)
{
	memset(summary, '\0', sizeof(FieldSummary));
	AvroGetBoolField(record, "contains_null", AVRO_FIELD_REQUIRED, &summary->contains_null);
	AvroGetBoolField(record, "contains_nan", AVRO_FIELD_OPTIONAL, &summary->contains_nan);
	AvroGetBinaryField(record, "lower_bound", AVRO_FIELD_OPTIONAL, (const void **) &summary->lower_bound, &summary->lower_bound_length);
	AvroGetBinaryField(record, "upper_bound", AVRO_FIELD_OPTIONAL, (const void **) &summary->upper_bound, &summary->upper_bound_length);
}

static void
ReadColumnStatFromAvro(avro_value_t * record, ColumnStat * stat, void *context)
{
	memset(stat, '\0', sizeof(ColumnStat));
	AvroGetInt32Field(record, "key", AVRO_FIELD_REQUIRED, &stat->column_id);
	AvroGetInt64Field(record, "value", AVRO_FIELD_REQUIRED, &stat->value);
}


static void
ReadColumnBoundFromAvro(avro_value_t * record, ColumnBound * bound, void *context)
{
	memset(bound, '\0', sizeof(ColumnBound));
	AvroGetInt32Field(record, "key", AVRO_FIELD_REQUIRED, &bound->column_id);
	AvroGetBinaryField(record, "value", AVRO_FIELD_REQUIRED, (const void **) &bound->value, &bound->value_length);
}
