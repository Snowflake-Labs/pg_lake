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

#include "pg_lake/avro/avro_writer.h"
#include "pg_lake/iceberg/api/manifest_entry.h"
#include "pg_lake/iceberg/format_version.h"
#include "pg_lake/iceberg/manifest_list_schema_v2.h"
#include "pg_lake/iceberg/manifest_list_schema_v3.h"
#include "pg_lake/iceberg/manifest_schema_v2.h"
#include "pg_lake/iceberg/manifest_schema_v3.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/util/string_utils.h"

static void WriteColumnStatToAvro(ColumnStat * stat, avro_value_t * record);
static void WriteColumnBoundToAvro(ColumnBound * bound, avro_value_t * record);
static void WriteFieldSummaryToAvro(FieldSummary * summary, avro_value_t * record);
static void WriteDataFileToAvro(DataFile * dataFile, avro_value_t * record);
static void WritePartitionToAvro(Partition * partition, avro_value_t * record);
static void WriteIcebergManifestToAvro(IcebergManifest * manifest, avro_value_t * record);
static void WriteIcebergManifestEntryToAvro(IcebergManifestEntry * entry, avro_value_t * record);
static const char *GetIcebergManifestJsonSchema(List *manifestEntries, IcebergFormatVersion formatVersion);
static const char *AdjustPartitionsInManifestJsonSchema(char *manifestSchema, Partition * partition);


/*
 * Format-version of the manifest currently being serialised, threaded into
 * the Avro callback path so v3-only fields (row lineage 142, deletion-vector
 * 143/144/145, manifest-list 520) are only emitted when the active schema
 * actually contains them. The Avro library's serialise callback signature
 * does not carry a user context pointer, but every public writer entry
 * point in this file synchronously and non-recursively walks its record
 * list, so a single file-scope value is sufficient. The writer entry points
 * set this on entry and reset it on exit so a future caller that mixes v2
 * and v3 serialisations in the same backend lifetime cannot leak the wrong
 * version between calls.
 */
static IcebergFormatVersion CurrentManifestWriterFormatVersion =
ICEBERG_FORMAT_VERSION_V2;


/*
 * WriteIcebergManifest writes given manifest entries to the given manifest file.
 *
 * `formatVersion` selects the Avro schema. v2 uses `manifest_schema_v2_json`,
 * v3 uses `manifest_schema_v3_json` which adds row-lineage (field-id 142)
 * and deletion-vector columns (143/144/145) as optional fields. The
 * writer-level hard error against v3 lives in callers (CREATE TABLE,
 * INSERT/UPDATE/DELETE planning); this routine is also reachable from
 * test C UDFs that pin the v3 wire shape without going through the
 * full CREATE+INSERT pipeline.
 */
void
WriteIcebergManifest(const char *manifestPath, List *manifestEntries,
					 IcebergFormatVersion formatVersion)
{
	const char *manifestSchema = GetIcebergManifestJsonSchema(manifestEntries, formatVersion);

	AvroWriter *manifestWriter =
		AvroWriterCreateWithJsonSchema(manifestPath, manifestSchema);

	ListCell   *manifestEntryCell = NULL;

	IcebergFormatVersion savedVersion = CurrentManifestWriterFormatVersion;

	CurrentManifestWriterFormatVersion = formatVersion;

	PG_TRY();
	{
		foreach(manifestEntryCell, manifestEntries)
		{
			IcebergManifestEntry *manifestEntry = lfirst(manifestEntryCell);

			AvroWriterWriteRecord(manifestWriter,
								  (AvroSerializeFunction) WriteIcebergManifestEntryToAvro,
								  manifestEntry);

		}

		AvroWriterClose(manifestWriter);
	}
	PG_FINALLY();
	{
		CurrentManifestWriterFormatVersion = savedVersion;
	}
	PG_END_TRY();
}

/*
 * IcebergManifestSchemaJsonForVersion returns the static Avro JSON schema
 * for the given Iceberg format-version. Centralising the dispatch here
 * keeps GetIcebergManifestJsonSchema (which also has to splice in the
 * dynamic partition schema) free of per-version conditionals.
 */
const char *
IcebergManifestSchemaJsonForVersion(IcebergFormatVersion formatVersion)
{
	switch (formatVersion)
	{
		case ICEBERG_FORMAT_VERSION_V2:
			return manifest_schema_v2_json;
		case ICEBERG_FORMAT_VERSION_V3:
			return manifest_schema_v3_json;
	}

	/*
	 * Unreachable: IcebergFormatVersionFromInt is the only sanctioned way to
	 * obtain an IcebergFormatVersion and it rejects everything outside the
	 * enum. The default branch makes the compiler happy on platforms with
	 * stricter -Wreturn-type analyses.
	 */
	ereport(ERROR,
			(errmsg("no Iceberg manifest schema for format-version %d",
					(int) formatVersion)));
	pg_unreachable();
}


/*
 * IcebergManifestListSchemaJsonForVersion is the manifest-list counterpart
 * to IcebergManifestSchemaJsonForVersion.
 */
const char *
IcebergManifestListSchemaJsonForVersion(IcebergFormatVersion formatVersion)
{
	switch (formatVersion)
	{
		case ICEBERG_FORMAT_VERSION_V2:
			return manifest_list_schema_v2_json;
		case ICEBERG_FORMAT_VERSION_V3:
			return manifest_list_schema_v3_json;
	}

	ereport(ERROR,
			(errmsg("no Iceberg manifest list schema for format-version %d",
					(int) formatVersion)));
	pg_unreachable();
}


/*
 * GetIcebergManifestJsonSchema creates a json avro schema for the manifest file with
 * the given manifest entries. It adjusts the schema based on the partition fields
 * of the first manifest entry, if any.
 */
static const char *
GetIcebergManifestJsonSchema(List *manifestEntries, IcebergFormatVersion formatVersion)
{
	const char *baseSchema = IcebergManifestSchemaJsonForVersion(formatVersion);

	if (manifestEntries == NIL)
	{
		return baseSchema;
	}

	/*
	 * all manifest entries must have the same partition schema, so safe to
	 * use the first entry
	 */
	IcebergManifestEntry *firstManifestEntry = linitial(manifestEntries);

	size_t		totalPartitionFields = firstManifestEntry->data_file.partition.fields_length;

	if (totalPartitionFields == 0)
	{
		return baseSchema;
	}

	Partition  *partition = &firstManifestEntry->data_file.partition;

	/* do not modify original static schema */
	char	   *manifestSchema = pstrdup(baseSchema);

	return AdjustPartitionsInManifestJsonSchema(manifestSchema, partition);
}


/*
 * AdjustPartitionsInManifestJsonSchema adjusts the partition schema,
 * which can only be resolved at runtime. (partition fields are dynamic),
 * in the given manifest schema. Each manifest file might have different
 * partition fields.
 */
static const char *
AdjustPartitionsInManifestJsonSchema(char *manifestSchema, Partition * partition)
{
	StringInfo	partitionFields = makeStringInfo();

	for (int i = 0; i < partition->fields_length; i++)
	{
		PartitionField *partitionField = &partition->fields[i];

		const char *fieldName = partitionField->field_name;

		const char *fieldType = IcebergAvroTypeSchemaString(partitionField->value_type);

		int32_t		fieldId = partitionField->field_id;

		appendStringInfo(partitionFields, "{ \
			\"name\": \"%s\", \
			\"type\": [\"null\", %s], \
			\"field-id\": %d \
		  }", fieldName, fieldType, fieldId);

		if (i != partition->fields_length - 1)
		{
			appendStringInfo(partitionFields, ",");
		}
	}

	char	   *newPartitionSchema = psprintf("{ \
			\"name\": \"partition\", \
			\"type\": { \
			  \"type\": \"record\", \
			  \"name\": \"r102\", \
			  \"fields\": [%s] \
			}, \
			\"doc\": \"Partition data tuple, schema based on the partition spec\", \
			\"field-id\": 102 \
		  }", partitionFields->data);

	char	   *templatePartitionSchema = "{ \
            \"name\": \"partition\", \
            \"type\": { \
              \"type\": \"record\", \
              \"name\": \"r102\", \
              \"fields\": [] \
            }, \
            \"doc\": \"Partition data tuple, schema based on the partition spec\", \
            \"field-id\": 102 \
          }";

	return PgLakeReplaceText(manifestSchema, templatePartitionSchema, newPartitionSchema);
}


/*
 * WriteIcebergManifestList writes given manifests to the given manifest list
 * file.
 *
 * `formatVersion` selects the Avro schema. v2 uses
 * `manifest_list_schema_v2_json`; v3 uses `manifest_list_schema_v3_json`
 * which adds the optional `first_row_id` (field-id 520) row-lineage start.
 * The writer-level hard error against v3 lives in callers (CREATE TABLE,
 * DML planning); this routine is also reachable from test C UDFs.
 */
void
WriteIcebergManifestList(const char *manifestListPath, List *manifests,
						 IcebergFormatVersion formatVersion)
{
	const char *manifestListSchema = IcebergManifestListSchemaJsonForVersion(formatVersion);

	AvroWriter *manifestListWriter =
		AvroWriterCreateWithJsonSchema(manifestListPath, manifestListSchema);

	ListCell   *manifestCell = NULL;

	IcebergFormatVersion savedVersion = CurrentManifestWriterFormatVersion;

	CurrentManifestWriterFormatVersion = formatVersion;

	PG_TRY();
	{
		foreach(manifestCell, manifests)
		{
			IcebergManifest *manifest = lfirst(manifestCell);

			AvroWriterWriteRecord(manifestListWriter,
								  (AvroSerializeFunction) WriteIcebergManifestToAvro,
								  manifest);
		}

		AvroWriterClose(manifestListWriter);
	}
	PG_FINALLY();
	{
		CurrentManifestWriterFormatVersion = savedVersion;
	}
	PG_END_TRY();
}

static void
WriteIcebergManifestToAvro(IcebergManifest * manifest, avro_value_t * record)
{
	AvroSetStringField(record, "manifest_path", manifest->manifest_path);
	AvroSetInt64Field(record, "manifest_length", manifest->manifest_length);
	AvroSetInt32Field(record, "partition_spec_id", manifest->partition_spec_id);
	AvroSetInt32Field(record, "content", (int32_t) manifest->content);
	AvroSetInt64Field(record, "sequence_number", manifest->sequence_number);
	AvroSetInt64Field(record, "min_sequence_number", manifest->min_sequence_number);
	AvroSetInt64Field(record, "added_snapshot_id", manifest->added_snapshot_id);

	/* Reference implementation uses data_files, spec says files */
	AvroSetInt32Field(record, "added_files_count", manifest->added_files_count);
	AvroSetInt32Field(record, "existing_files_count", manifest->existing_files_count);
	AvroSetInt32Field(record, "deleted_files_count", manifest->deleted_files_count);
	AvroSetInt64Field(record, "added_rows_count", manifest->added_rows_count);
	AvroSetInt64Field(record, "existing_rows_count", manifest->existing_rows_count);
	AvroSetInt64Field(record, "deleted_rows_count", manifest->deleted_rows_count);
	AvroSetRecordArrayField(record, "partitions",
							(AvroSerializeFunction) WriteFieldSummaryToAvro,
							sizeof(FieldSummary),
							manifest->partitions, manifest->partitions_length);

	/* Reference implementation schema does not have key_metadata */
	AvroSetNullableBinaryField(record, "key_metadata",
							   manifest->key_metadata, manifest->key_metadata_length,
							   manifest->key_metadata != NULL);

	/*
	 * Iceberg v3 manifest list row-lineage start (field-id 520). Gated on the
	 * caller-pinned format version for the same reason as the data_file
	 * fields: v2 schema does not declare this field, so emitting it would
	 * fail inside libavro. The per-manifest has_first_row_id flag lets v3
	 * round-trips preserve "absent" semantics for manifests that did not
	 * contribute new rows.
	 */
	if (CurrentManifestWriterFormatVersion == ICEBERG_FORMAT_VERSION_V3)
	{
		AvroSetNullableInt64Field(record, "first_row_id",
								  manifest->first_row_id,
								  manifest->has_first_row_id);
	}
}


static void
WriteIcebergManifestEntryToAvro(IcebergManifestEntry * entry, avro_value_t * record)
{
	AvroSetInt32Field(record, "status", entry->status);
	AvroSetNullableInt64Field(record, "snapshot_id", entry->snapshot_id, entry->has_snapshot_id);
	AvroSetNullableInt64Field(record, "sequence_number", entry->sequence_number, entry->has_sequence_number);
	AvroSetNullableInt64Field(record, "file_sequence_number", entry->sequence_number, entry->has_file_sequence_number);
	AvroSetRecordField(record, "data_file", (AvroSerializeFunction) WriteDataFileToAvro, &entry->data_file);
}


static void
WriteDataFileToAvro(DataFile * dataFile, avro_value_t * record)
{
	AvroSetInt32Field(record, "content", dataFile->content);
	AvroSetStringField(record, "file_path", dataFile->file_path);
	AvroSetStringField(record, "file_format", dataFile->file_format);
	AvroSetRecordField(record, "partition", (AvroSerializeFunction) WritePartitionToAvro, &dataFile->partition);
	AvroSetInt64Field(record, "record_count", dataFile->record_count);
	AvroSetInt64Field(record, "file_size_in_bytes", dataFile->file_size_in_bytes);
	AvroSetRecordArrayField(record, "column_sizes",
							(AvroSerializeFunction) WriteColumnStatToAvro,
							sizeof(ColumnStat),
							dataFile->column_sizes, dataFile->column_sizes_length);
	AvroSetRecordArrayField(record, "value_counts",
							(AvroSerializeFunction) WriteColumnStatToAvro,
							sizeof(ColumnStat),
							dataFile->value_counts, dataFile->value_counts_length);
	AvroSetRecordArrayField(record, "null_value_counts",
							(AvroSerializeFunction) WriteColumnStatToAvro,
							sizeof(ColumnStat),
							dataFile->null_value_counts, dataFile->null_value_counts_length);
	AvroSetRecordArrayField(record, "nan_value_counts",
							(AvroSerializeFunction) WriteColumnStatToAvro,
							sizeof(ColumnStat),
							dataFile->nan_value_counts, dataFile->nan_value_counts_length);
	AvroSetRecordArrayField(record, "lower_bounds",
							(AvroSerializeFunction) WriteColumnBoundToAvro,
							sizeof(ColumnBound),
							(void *) dataFile->lower_bounds,
							dataFile->lower_bounds_length);
	AvroSetRecordArrayField(record, "upper_bounds",
							(AvroSerializeFunction) WriteColumnBoundToAvro,
							sizeof(ColumnBound),
							(void *) dataFile->upper_bounds,
							dataFile->upper_bounds_length);
	AvroSetNullableBinaryField(record, "key_metadata",
							   dataFile->key_metadata, dataFile->key_metadata_length,
							   dataFile->key_metadata != NULL);
	AvroSetInt64ArrayField(record, "split_offsets",
						   dataFile->split_offsets, dataFile->split_offsets_length);
	AvroSetInt32ArrayField(record, "equality_ids",
						   dataFile->equality_ids, dataFile->equality_ids_length);
	AvroSetNullableInt32Field(record, "sort_order_id",
							  dataFile->sort_order_id, dataFile->has_sort_order_id);

	/*
	 * Iceberg v3 manifest data_file fields (field-ids 142-145). Only the v3
	 * Avro schema declares these, so emitting them under the v2 schema would
	 * fail with "field not found in schema" inside libavro. Gate on the
	 * caller-pinned format version rather than on the DataFile flags so a
	 * v3-shaped struct accidentally fed to a v2 writer fails-closed (drops
	 * the lineage data) instead of corrupting the on-disk record.
	 *
	 * Within v3, we still honour the per-field has_* flags so reserialise
	 * round-trips of v3 manifests that don't yet carry row lineage produce
	 * identical output. Stage 12's allocator will be the first caller to set
	 * has_first_row_id on the way in; DV blob pointers (143/144/145) stay
	 * unset until pg_lake learns to emit deletion vectors.
	 */
	if (CurrentManifestWriterFormatVersion == ICEBERG_FORMAT_VERSION_V3)
	{
		AvroSetNullableInt64Field(record, "first_row_id",
								  dataFile->first_row_id,
								  dataFile->has_first_row_id);
		AvroSetNullableStringField(record, "referenced_data_file",
								   dataFile->referenced_data_file,
								   dataFile->referenced_data_file != NULL);
		AvroSetNullableInt64Field(record, "content_offset",
								  dataFile->content_offset,
								  dataFile->has_content_offset);
		AvroSetNullableInt64Field(record, "content_size_in_bytes",
								  dataFile->content_size_in_bytes,
								  dataFile->has_content_size_in_bytes);
	}
}

static void
WritePartitionToAvro(Partition * partition, avro_value_t * record)
{
	for (int i = 0; i < partition->fields_length; i++)
	{
		PartitionField *field = &partition->fields[i];

		bool		isSet = field->value != NULL;

		char	   *fieldName = field->field_name;

		if (field->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_INT32)
		{
			int32_t		value = (isSet) ? *((int32_t *) field->value) : 0;

			AvroSetNullableInt32Field(record, fieldName, value, isSet);
		}
		else if (field->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_INT64)
		{
			int64_t		value = (isSet) ? *((int64_t *) field->value) : 0;

			AvroSetNullableInt64Field(record, fieldName, value, isSet);
		}
		else if (field->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_STRING)
		{
			AvroSetNullableStringField(record, fieldName, (const char *) field->value, isSet);
		}
		else if (field->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_BINARY)
		{
			AvroSetNullableBinaryField(record, fieldName, (void *) field->value, field->value_length, isSet);
		}
		else if (field->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT)
		{
			float		value = (isSet) ? *((float *) field->value) : 0.0;

			AvroSetNullableFloatField(record, fieldName, value, isSet);
		}
		else if (field->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE)
		{
			double		value = (isSet) ? *((double *) field->value) : 0.0;

			AvroSetNullableDoubleField(record, fieldName, value, isSet);
		}
		else if (field->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_BOOL)
		{
			bool		value = (isSet) ? *((bool *) field->value) : false;

			AvroSetNullableBoolField(record, fieldName, value, isSet);
		}
		else
		{
			ereport(ERROR, (errmsg("Unexpected partition field value type")));
		}
	}
}

static void
WriteColumnStatToAvro(ColumnStat * stat, avro_value_t * record)
{
	AvroSetInt32Field(record, "key", stat->column_id);
	AvroSetInt64Field(record, "value", stat->value);

}


static void
WriteColumnBoundToAvro(ColumnBound * bound, avro_value_t * record)
{
	AvroSetInt32Field(record, "key", bound->column_id);
	AvroSetBinaryField(record, "value", (void *) bound->value, bound->value_length);
}

static void
WriteFieldSummaryToAvro(FieldSummary * summary, avro_value_t * record)
{
	/* fix: properly set summary->contains_null */
	AvroSetBoolField(record, "contains_null", summary->contains_null);
	/* fix: properly set summary->contains_nan */
	AvroSetNullableBoolField(record, "contains_nan", summary->contains_nan, true);
	AvroSetNullableBinaryField(record, "lower_bound", summary->lower_bound, summary->lower_bound_length, summary->lower_bound != NULL);
	AvroSetNullableBinaryField(record, "upper_bound", summary->upper_bound, summary->upper_bound_length, summary->upper_bound != NULL);
}
