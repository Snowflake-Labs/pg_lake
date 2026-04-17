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

#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/json/json_reader.h"
#include "pg_lake/util/s3_reader_utils.h"

#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"

static const char *SnapshotReferenceTypeName[] = {
	"tag", "branch", NULL
};


static void ReadIcebergTableMetadataFromJson(JsonbContainer *json, IcebergTableMetadata *metadata);
static void ReadIcebergTableSchema(JsonbContainer *json, IcebergTableSchema *schema);
static void ReadIcebergTableSchemaField(JsonbContainer *json, DataFileSchemaField *field);
static void ReadIcebergPartitionSpec(JsonbContainer *json, IcebergPartitionSpec *spec);
static void ReadIcebergPartitionSpecField(JsonbContainer *json, IcebergPartitionSpecField *field);
static void ReadIcebergSnapshot(JsonbContainer *json, IcebergSnapshot *snapshot);
static void ReadIcebergSnapshotLogEntry(JsonbContainer *json, IcebergSnapshotLogEntry *entry);
static void ReadIcebergPartitionStatistics(JsonbContainer *json, IcebergPartitionStatistics *entry);
static void ReadIcebergMetadataLogEntry(JsonbContainer *json, IcebergMetadataLogEntry *entry);
static void ReadIcebergSortOrder(JsonbContainer *json, IcebergSortOrder *order);
static void ReadIcebergSortOrderField(JsonbContainer *json, IcebergSortOrderField *field);
static void ReadSnapshotReference(const char *key, JsonbValue *jsonValue, SnapshotReference *ref);
static void ReadSnapshotReferenceType(JsonbContainer *refJson, SnapshotReferenceType *type);
static void ReadIcebergStatistics(JsonbContainer *json, IcebergStatistics *statistics);
static void ReadBlobMetadata(JsonbContainer *json, BlobMetadata *metadata);
static void ReadInt32(int input, int *value);
static void ReadProperty(const char *key, JsonbValue *jsonValue, Property *property);
static bool JsonExtractField(JsonbContainer *topLevelJsonContainer,
							 char *fieldName, FieldRequired required,
							 Field **field);


/*
 * ReadIcebergTableMetadata reads the Iceberg table metadata from the given URI.
 */
IcebergTableMetadata *
ReadIcebergTableMetadata(const char *tableMetadataPath)
{
	char	   *tableMetadataText = GetTextFromURI(tableMetadataPath);
	Datum		jsonbDatum = DirectFunctionCall1(jsonb_in, PointerGetDatum(tableMetadataText));
	Jsonb	   *jsonb = DatumGetJsonbP(jsonbDatum);

	IcebergTableMetadata *metadata = palloc0(sizeof(IcebergTableMetadata));

	ReadIcebergTableMetadataFromJson(&jsonb->root, metadata);

	return metadata;
}


/*
* Read the Iceberg table metadata from the given JSON object.
* This is the top-level function that Reads the entire Iceberg
* table metadata.
*/
static void
ReadIcebergTableMetadataFromJson(JsonbContainer *json, IcebergTableMetadata *metadata)
{
	memset(metadata, '\0', sizeof(IcebergTableMetadata));

	JsonExtractInt32Field(json, "format-version", FIELD_REQUIRED, &metadata->format_version);
	if (metadata->format_version != 2)
	{
		ereport(ERROR, (errmsg("unsupported iceberg format version %d",
							   metadata->format_version)));
	}

	JsonExtractStringField(json, "table-uuid", FIELD_REQUIRED, &metadata->table_uuid, &metadata->table_uuid_length);
	JsonExtractStringField(json, "location", FIELD_REQUIRED, &metadata->location, &metadata->location_length);
	JsonExtractInt64Field(json, "last-sequence-number", FIELD_REQUIRED, &metadata->last_sequence_number);
	JsonExtractInt64Field(json, "last-updated-ms", FIELD_REQUIRED, &metadata->last_updated_ms);
	JsonExtractInt32Field(json, "last-column-id", FIELD_REQUIRED, &metadata->last_column_id);
	JsonExtractInt32Field(json, "current-schema-id", FIELD_REQUIRED, &metadata->current_schema_id);
	JsonExtractObjectArrayField(json, "schemas", FIELD_REQUIRED, (JsonParseFunction) ReadIcebergTableSchema,
								sizeof(IcebergTableSchema),
								(void **) &metadata->schemas, &metadata->schemas_length);

	JsonExtractInt32Field(json, "default-spec-id", FIELD_REQUIRED, &metadata->default_spec_id);
	JsonExtractObjectArrayField(json, "partition-specs", FIELD_REQUIRED, (JsonParseFunction) ReadIcebergPartitionSpec,
								sizeof(IcebergPartitionSpec),
								(void **) &metadata->partition_specs, &metadata->partition_specs_length);
	JsonExtractInt32Field(json, "last-partition-id", FIELD_REQUIRED, &metadata->last_partition_id);

	JsonExtractMapField(json, "properties", FIELD_OPTIONAL,
						(JsonParseMapEntryFunction) ReadProperty,
						sizeof(Property),
						(void **) &metadata->properties, &metadata->properties_length);

	JsonExtractInt64Field(json, "current-snapshot-id", FIELD_OPTIONAL, &metadata->current_snapshot_id);

	JsonExtractObjectArrayField(json, "snapshots", FIELD_OPTIONAL,
								(JsonParseFunction) ReadIcebergSnapshot,
								sizeof(IcebergSnapshot),
								(void **) &metadata->snapshots,
								&metadata->snapshots_length);

	JsonExtractObjectArrayField(json, "snapshot-log", FIELD_OPTIONAL,
								(JsonParseFunction) ReadIcebergSnapshotLogEntry,
								sizeof(IcebergSnapshotLogEntry),
								(void **) &metadata->snapshot_log,
								&metadata->snapshot_log_length);

	JsonExtractObjectArrayField(json, "partition-statistics", FIELD_OPTIONAL,
								(JsonParseFunction) ReadIcebergPartitionStatistics,
								sizeof(IcebergPartitionStatistics),
								(void **) &metadata->partition_statistics,
								&metadata->partition_statistics_length);

	JsonExtractObjectArrayField(json, "metadata-log", FIELD_OPTIONAL,
								(JsonParseFunction) ReadIcebergMetadataLogEntry,
								sizeof(IcebergMetadataLogEntry),
								(void **) &metadata->metadata_log,
								&metadata->metadata_log_length);

	JsonExtractObjectArrayField(json, "sort-orders", FIELD_REQUIRED,
								(JsonParseFunction) ReadIcebergSortOrder,
								sizeof(IcebergSortOrder),
								(void **) &metadata->sort_orders,
								&metadata->sort_orders_length);

	JsonExtractInt32Field(json, "default-sort-order-id", FIELD_REQUIRED, &metadata->default_sort_order_id);

	JsonExtractMapField(json, "refs", FIELD_OPTIONAL,
						(JsonParseMapEntryFunction) ReadSnapshotReference,
						sizeof(SnapshotReference),
						(void **) &metadata->refs, &metadata->refs_length);

	JsonExtractObjectArrayField(json, "statistics", FIELD_OPTIONAL,
								(JsonParseFunction) ReadIcebergStatistics,
								sizeof(IcebergStatistics),
								(void **) &metadata->statistics,
								&metadata->statistics_length);
}


static void
ReadIcebergTableSchema(JsonbContainer *json, IcebergTableSchema *schema)
{
	memset(schema, '\0', sizeof(IcebergTableSchema));
	JsonExtractInt32Field(json, "schema-id", FIELD_REQUIRED, &schema->schema_id);
	JsonExtractStringField(json, "type", FIELD_OPTIONAL, &schema->type, &schema->type_length);
	JsonExtractObjectArrayField(json, "fields", FIELD_REQUIRED,
								(JsonParseFunction) ReadIcebergTableSchemaField,
								sizeof(DataFileSchemaField),
								(void **) &schema->fields, &schema->fields_length);

	/* specs says "required" but spark does not always generate */
	JsonExtractInt32ArrayField(json, "identifier-field-ids", FIELD_OPTIONAL,
							   (JsonParseNumericFunction) ReadInt32,
							   sizeof(int),
							   (void **) &schema->identifier_field_ids, &schema->identifier_field_ids_length);
}


static void
ReadIcebergTableSchemaField(JsonbContainer *json, DataFileSchemaField *field)
{
	memset(field, '\0', sizeof(DataFileSchemaField));

	JsonExtractInt32Field(json, "id", FIELD_REQUIRED, &field->id);

	size_t		nameLength = 0;

	JsonExtractStringField(json, "name", FIELD_REQUIRED, &field->name, &nameLength);

	JsonExtractField(json, "type", FIELD_OPTIONAL, &field->type);
	EnsureIcebergField(field->type);

	JsonExtractBoolField(json, "required", FIELD_REQUIRED, &field->required);

	size_t		docLength = 0;

	JsonExtractStringField(json, "doc", FIELD_OPTIONAL, &field->doc, &docLength);

	size_t		initialDefaultLength = 0;

	JsonExtractFieldAsJsonString(json, "initial-default", FIELD_OPTIONAL, &field->initialDefault,
								 &initialDefaultLength);

	size_t		writeDefaultLength = 0;

	JsonExtractFieldAsJsonString(json, "write-default", FIELD_OPTIONAL, &field->writeDefault,
								 &writeDefaultLength);
}


static void
ReadIcebergPartitionSpec(JsonbContainer *json, IcebergPartitionSpec *spec)
{
	memset(spec, '\0', sizeof(IcebergPartitionSpec));
	JsonExtractInt32Field(json, "spec-id", FIELD_REQUIRED, &spec->spec_id);
	JsonExtractObjectArrayField(json, "fields", FIELD_REQUIRED,
								(JsonParseFunction) ReadIcebergPartitionSpecField,
								sizeof(IcebergPartitionSpecField),
								(void **) &spec->fields, &spec->fields_length);
}


static void
ReadIcebergPartitionSpecField(JsonbContainer *json, IcebergPartitionSpecField *field)
{
	memset(field, '\0', sizeof(IcebergPartitionSpecField));
	JsonExtractInt32Field(json, "source-id", FIELD_REQUIRED, &field->source_id);
	JsonExtractInt32ArrayField(json, "source-ids", FIELD_OPTIONAL,
							   (JsonParseNumericFunction) ReadInt32,
							   sizeof(int),
							   (void **) &field->source_ids, &field->source_ids_length);
	JsonExtractInt32Field(json, "field-id", FIELD_REQUIRED, &field->field_id);
	JsonExtractStringField(json, "name", FIELD_REQUIRED, &field->name, &field->name_length);
	JsonExtractStringField(json, "transform", FIELD_REQUIRED, &field->transform, &field->transform_length);
}

static void
ReadSnapshotReference(const char *key, JsonbValue *jsonValue, SnapshotReference *ref)
{
	memset(ref, '\0', sizeof(SnapshotReference));

	if (jsonValue->type != jbvBinary)
	{
		ereport(ERROR, (errmsg("unexpected %s value for key %s",
							   JsonbTypeName(jsonValue), key)));
	}

	/* extract object from map */
	JsonbContainer *refJson = jsonValue->val.binary.data;

	/* JsonExtractMapField does palloc of the key */
	ref->key = key;
	ref->key_length = strlen(key);

	JsonExtractInt64Field(refJson, "snapshot-id", FIELD_REQUIRED, &ref->snapshot_id);
	ReadSnapshotReferenceType(refJson, &ref->type);

	ref->has_min_snapshots_to_keep =
		JsonExtractInt32Field(refJson, "min-snapshots-to-keep", FIELD_OPTIONAL, &ref->min_snapshots_to_keep);

	ref->has_max_snapshot_age_ms =
		JsonExtractInt64Field(refJson, "max-snapshot-age-ms", FIELD_OPTIONAL, &ref->max_snapshot_age_ms);

	ref->has_max_ref_age_ms =
		JsonExtractInt64Field(refJson, "max-ref-age-ms", FIELD_OPTIONAL, &ref->max_ref_age_ms);
}

static void
ReadSnapshotReferenceType(JsonbContainer *refJson, SnapshotReferenceType *type)
{
	const char *typeName = NULL;
	size_t		typeNameLength = 0;

	JsonExtractStringField(refJson, "type", FIELD_REQUIRED, &typeName, &typeNameLength);

	/* map type name to an enum */
	for (int typeIndex = 0; SnapshotReferenceTypeName[typeIndex] != NULL; typeIndex++)
	{
		if (strcmp(typeName, SnapshotReferenceTypeName[typeIndex]) == 0)
		{
			*type = (SnapshotReferenceType) typeIndex;
			return;
		}
	}

	*type = SNAPSHOT_REFERENCE_TYPE_INVALID;
}

static void
ReadProperty(const char *key, JsonbValue *jsonValue, Property *property)
{
	memset(property, '\0', sizeof(Property));

	if (jsonValue->type != jbvString)
	{
		ereport(ERROR, (errmsg("unexpected %s value for key %s",
							   JsonbTypeName(jsonValue), key)));
	}

	/* JsonExtractMapField does palloc */
	property->key = key;
	property->value = pnstrdup(jsonValue->val.string.val, jsonValue->val.string.len);
}


static void
ReadInt32(int input, int *value)
{
	*value = input;
}


static void
ReadIcebergSnapshot(JsonbContainer *json, IcebergSnapshot *snapshot)
{
	memset(snapshot, '\0', sizeof(IcebergSnapshot));
	JsonExtractInt64Field(json, "snapshot-id", FIELD_REQUIRED, &snapshot->snapshot_id);
	JsonExtractInt64Field(json, "parent-snapshot-id", FIELD_OPTIONAL, &snapshot->parent_snapshot_id);
	JsonExtractInt64Field(json, "sequence-number", FIELD_REQUIRED, &snapshot->sequence_number);
	JsonExtractInt64Field(json, "timestamp-ms", FIELD_REQUIRED, &snapshot->timestamp_ms);
	JsonExtractStringField(json, "manifest-list", FIELD_REQUIRED, &snapshot->manifest_list, &snapshot->manifest_list_length);
	/* required by spec, but not generated by reference implementation */
	JsonExtractMapField(json, "summary", FIELD_OPTIONAL,
						(JsonParseMapEntryFunction) ReadProperty,
						sizeof(Property),
						(void **) &snapshot->summary, &snapshot->summary_length);
	snapshot->schema_id_set = JsonExtractInt32Field(json, "schema-id", FIELD_OPTIONAL, &snapshot->schema_id);
}

static void
ReadIcebergSnapshotLogEntry(JsonbContainer *json, IcebergSnapshotLogEntry *entry)
{
	memset(entry, '\0', sizeof(IcebergSnapshotLogEntry));
	JsonExtractInt64Field(json, "timestamp-ms", FIELD_REQUIRED, &entry->timestamp_ms);
	JsonExtractInt64Field(json, "snapshot-id", FIELD_REQUIRED, &entry->snapshot_id);
}

static void
ReadIcebergPartitionStatistics(JsonbContainer *json, IcebergPartitionStatistics *entry)
{
	memset(entry, '\0', sizeof(IcebergPartitionStatistics));
	JsonExtractInt64Field(json, "snapshot-id", FIELD_REQUIRED, &entry->snapshot_id);
	JsonExtractInt64Field(json, "file-size-in-bytes", FIELD_REQUIRED, &entry->file_size_in_bytes);
	JsonExtractStringField(json, "statistics-path", FIELD_REQUIRED, &entry->statistics_path, &entry->statistics_path_length);
}

static void
ReadIcebergMetadataLogEntry(JsonbContainer *json, IcebergMetadataLogEntry *entry)
{
	memset(entry, '\0', sizeof(IcebergMetadataLogEntry));
	JsonExtractInt64Field(json, "timestamp-ms", FIELD_REQUIRED, &entry->timestamp_ms);
	JsonExtractStringField(json, "metadata-file", FIELD_REQUIRED, &entry->metadata_file, &entry->metadata_file_length);
}


static void
ReadIcebergSortOrder(JsonbContainer *json, IcebergSortOrder *order)
{
	memset(order, '\0', sizeof(IcebergSortOrder));
	JsonExtractInt32Field(json, "order-id", FIELD_REQUIRED, &order->order_id);
	JsonExtractObjectArrayField(json, "fields", FIELD_REQUIRED,
								(JsonParseFunction) ReadIcebergSortOrderField,
								sizeof(IcebergSortOrderField),
								(void **) &order->fields, &order->fields_length);
}


static void
ReadIcebergSortOrderField(JsonbContainer *json, IcebergSortOrderField *field)
{
	memset(field, '\0', sizeof(IcebergSortOrderField));
	JsonExtractStringField(json, "transform", FIELD_REQUIRED, &field->transform, &field->transform_length);
	JsonExtractInt32Field(json, "source-id", FIELD_REQUIRED, &field->source_id);
	JsonExtractStringField(json, "direction", FIELD_REQUIRED, &field->direction, &field->direction_length);
	JsonExtractStringField(json, "null-order", FIELD_REQUIRED, &field->null_order, &field->null_order_length);
}


static void
ReadBlobMetadata(JsonbContainer *json, BlobMetadata *metadata)
{
	memset(metadata, '\0', sizeof(BlobMetadata));
	JsonExtractStringField(json, "type", FIELD_REQUIRED, &metadata->type, &metadata->type_length);
	JsonExtractInt64Field(json, "snapshot-id", FIELD_REQUIRED, &metadata->snapshot_id);
	JsonExtractInt64Field(json, "sequence-number", FIELD_REQUIRED, &metadata->sequence_number);
	JsonExtractInt32ArrayField(json, "fields", FIELD_REQUIRED,
							   (JsonParseNumericFunction) ReadInt32,
							   sizeof(int),
							   (void **) &metadata->fields, &metadata->fields_length);
	JsonExtractMapField(json, "properties", FIELD_OPTIONAL,
						(JsonParseMapEntryFunction) ReadProperty,
						sizeof(Property),
						(void **) &metadata->properties, &metadata->properties_length);
}

static void
ReadIcebergStatistics(JsonbContainer *json, IcebergStatistics *statistics)
{
	memset(statistics, '\0', sizeof(IcebergStatistics));
	JsonExtractInt64Field(json, "snapshot-id", FIELD_REQUIRED, &statistics->snapshot_id);
	JsonExtractStringField(json, "statistics-path", FIELD_REQUIRED, &statistics->statistics_path, &statistics->statistics_path_length);
	JsonExtractInt64Field(json, "file-size-in-bytes", FIELD_REQUIRED, &statistics->file_size_in_bytes);
	JsonExtractInt64Field(json, "file-footer-size-in-bytes", FIELD_REQUIRED, &statistics->file_footer_size_in_bytes);
	JsonExtractStringField(json, "key-metadata", FIELD_OPTIONAL, &statistics->key_metadata, &statistics->key_metadata_length);
	JsonExtractObjectArrayField(json, "blob-metadata", FIELD_REQUIRED,
								(JsonParseFunction) ReadBlobMetadata,
								sizeof(BlobMetadata),
								(void **) &statistics->blobs, &statistics->blobs_length);
}


static void
ReadIcebergTypeFieldElement(JsonbContainer *json, DataFileSchemaField *fieldElement)
{
	memset(fieldElement, '\0', sizeof(DataFileSchemaField));
	JsonExtractInt32Field(json, "id", FIELD_REQUIRED, &fieldElement->id);
	size_t		nameLength = 0;

	JsonExtractStringField(json, "name", FIELD_REQUIRED, &fieldElement->name, &nameLength);
	JsonExtractBoolField(json, "required", FIELD_OPTIONAL, &fieldElement->required);
	size_t		docLength = 0;

	JsonExtractStringField(json, "doc", FIELD_OPTIONAL, &fieldElement->doc, &docLength);

	/* recurse into "type" field */
	JsonExtractField(json, "type", FIELD_REQUIRED, &fieldElement->type);
	EnsureIcebergField(fieldElement->type);
}


static bool
JsonExtractField(JsonbContainer *topLevelJsonContainer, char *fieldName, FieldRequired required, Field **field)
{
	JsonbValue *topLevelTypeFieldJson =
		getKeyJsonValueFromContainer(topLevelJsonContainer, fieldName, strlen(fieldName), NULL);
	Jsonb	   *topLevelJson =
		JsonbValueToJsonb(topLevelTypeFieldJson);

	/* each invocation of this function should fill one field */
	*field = palloc0(sizeof(Field));

	/*
	 * The type field can be a string or a json object. If it is a string, it
	 * is a scalar type, such as "type" : "int" in the json.
	 */
	if (topLevelTypeFieldJson != NULL && topLevelTypeFieldJson->type == jbvString)
	{
		(*field)->type = FIELD_TYPE_SCALAR;
		(*field)->field.scalar.typeName =
			pnstrdup(topLevelTypeFieldJson->val.string.val, topLevelTypeFieldJson->val.string.len);
		return true;
	}
	else if (topLevelTypeFieldJson != NULL && topLevelTypeFieldJson->type == jbvBinary)
	{
		/*
		 * Now, we are going to parse the non-scalar json object. The json
		 * object can be a map, list or struct. First, fetch the type field
		 * from the json object.
		 */
		JsonbValue *subLevelTypeFieldJson =
			getKeyJsonValueFromContainer(&topLevelJson->root,
										 "type", strlen("type"),
										 NULL);

		if (!(subLevelTypeFieldJson != NULL && subLevelTypeFieldJson->type == jbvString))
		{
			ereport(ERROR, (errmsg("missing or corrupted sub-field in Iceberg schema: type")));
		}

		char	   *subfieldType =
			pnstrdup(subLevelTypeFieldJson->val.string.val, subLevelTypeFieldJson->val.string.len);

		if (strcasecmp(subfieldType, "map") == 0)
		{
			(*field)->type = FIELD_TYPE_MAP;

			JsonExtractInt32Field(&topLevelJson->root, "key-id", FIELD_OPTIONAL, &(*field)->field.map.keyId);
			JsonExtractInt32Field(&topLevelJson->root, "value-id", FIELD_OPTIONAL, &(*field)->field.map.valueId);
			JsonExtractBoolField(&topLevelJson->root, "value-required", FIELD_OPTIONAL, &(*field)->field.map.valueRequired);

			/* recurse into "key" and "value" fields */
			JsonExtractField(&topLevelJson->root, "key",
							 FIELD_REQUIRED, &(*field)->field.map.key);
			JsonExtractField(&topLevelJson->root, "value",
							 FIELD_REQUIRED, &(*field)->field.map.value);
		}
		else if (strcasecmp(subfieldType, "struct") == 0)
		{
			(*field)->type = FIELD_TYPE_STRUCT;
			JsonExtractObjectArrayField(&topLevelJson->root, "fields",
										FIELD_REQUIRED,
										(JsonParseFunction) ReadIcebergTypeFieldElement,
										sizeof(DataFileSchemaField),
										(void **) &(*field)->field.structType.fields,
										&(*field)->field.structType.nfields);
		}
		else if (strcasecmp(subfieldType, "list") == 0)
		{
			(*field)->type = FIELD_TYPE_LIST;

			/* recurse into "element" field */
			JsonExtractField(&topLevelJson->root, "element", FIELD_REQUIRED, &(*field)->field.list.element);
			JsonExtractBoolField(&topLevelJson->root, "element-required", FIELD_OPTIONAL, &(*field)->field.list.elementRequired);
			JsonExtractInt32Field(&topLevelJson->root, "element-id", FIELD_OPTIONAL, &(*field)->field.list.elementId);

		}
		else
		{
			ereport(ERROR, (errmsg("unsupported Iceberg type: %s", subfieldType)));
		}

		return true;
	}
	else if (required == FIELD_REQUIRED)
	{
		ereport(ERROR, (errmsg("missing field in Iceberg table metadata: %s", fieldName)));
	}
	return false;
}
