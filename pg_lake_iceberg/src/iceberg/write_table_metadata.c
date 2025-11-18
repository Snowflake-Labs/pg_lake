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

#include "pg_lake/iceberg/api/table_metadata.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/json/json_reader.h"
#include "pg_lake/json/json_utils.h"

#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"

static void AppendProperties(StringInfo command, Property * properties, size_t properties_length);
static void AppendField(StringInfo command, Field * field);
static void AppendIcebergStructFields(StringInfo command, FieldStructElement * fields, size_t fields_length);
static void AppendIcebergTableSchemas(StringInfo command, IcebergTableSchema * schemas, size_t schemas_length);
static void AppendIcebergPartitionSpecs(StringInfo command, IcebergPartitionSpec * specs, size_t specs_length);
static void AppendIcebergSnapshots(StringInfo command, IcebergSnapshot * snapshots, size_t snapshots_length);
static void AppendIcebergSnapshotLogEntries(StringInfo command, IcebergSnapshotLogEntry * entries, size_t entries_length);
static void AppendcebergPartitionStatistics(StringInfo command, IcebergPartitionStatistics * entries, size_t entries_length);
static void AppendIcebergMetadataLogEntries(StringInfo command, IcebergMetadataLogEntry * entries, size_t entries_length);
static void AppendIcebergSortOrderFields(StringInfo command, IcebergSortOrderField * fields, size_t fields_length);
static void AppendIcebergSortOrders(StringInfo command, IcebergSortOrder * orders, size_t orders_length);
static void AppendSnapshotReferences(StringInfo command, SnapshotReference * refs, size_t refs_length);
static void AppendIcebergStatistics(StringInfo command, IcebergStatistics * statistics, size_t statistics_length);
static void AppendBlobMetadata(StringInfo command, BlobMetadata * metadata, size_t metadata_length);
static void AppendIntArray(StringInfo command, int *array, size_t array_length);

/*
* Write the IcebergTableMetadata struct to a JSON string.
* This is the top-level function that Writes the
* IcebergTableMetadata
*/
char *
WriteIcebergTableMetadataToJson(IcebergTableMetadata * metadata)
{
	StringInfo	command = makeStringInfo();

	appendStringInfoString(command, "{");

	/* Append format_version */
	appendJsonInt32(command, "format-version", metadata->format_version);
	appendStringInfoString(command, ", ");

	/* Append table_uuid */
	appendJsonString(command, "table-uuid", metadata->table_uuid);
	appendStringInfoString(command, ", ");

	/* Append location */
	appendJsonString(command, "location", metadata->location);
	appendStringInfoString(command, ", ");

	/* Append last_sequence_number */
	appendJsonInt64(command, "last-sequence-number", metadata->last_sequence_number);
	appendStringInfoString(command, ", ");

	/* Append last_updated_ms */
	appendJsonInt64(command, "last-updated-ms", metadata->last_updated_ms);
	appendStringInfoString(command, ", ");

	/* Append last_column_id */
	appendJsonInt32(command, "last-column-id", metadata->last_column_id);
	appendStringInfoString(command, ", ");

	/* Append current_schema_id */
	appendJsonInt32(command, "current-schema-id", metadata->current_schema_id);
	appendStringInfoString(command, ", ");

	/* Append schemas */
	appendStringInfoString(command, "\"schemas\":");
	AppendIcebergTableSchemas(command, metadata->schemas, metadata->schemas_length);
	appendStringInfoString(command, ", ");

	/* Append default_spec_id */
	appendJsonInt32(command, "default-spec-id", metadata->default_spec_id);
	appendStringInfoString(command, ", ");

	/* Append partition_specs */
	appendStringInfoString(command, "\"partition-specs\":");
	AppendIcebergPartitionSpecs(command, metadata->partition_specs, metadata->partition_specs_length);
	appendStringInfoString(command, ", ");

	/* Append last_partition_id */
	appendJsonInt32(command, "last-partition-id", metadata->last_partition_id);
	appendStringInfoString(command, ", ");

	/* Append properties */
	appendStringInfoString(command, "\"properties\":");
	AppendProperties(command, metadata->properties, metadata->properties_length);
	appendStringInfoString(command, ", ");

	/* Append current_snapshot_id */
	appendJsonInt64(command, "current-snapshot-id", metadata->current_snapshot_id);
	appendStringInfoString(command, ", ");

	/* Append snapshots */
	appendStringInfoString(command, "\"snapshots\":");
	AppendIcebergSnapshots(command, metadata->snapshots, metadata->snapshots_length);
	appendStringInfoString(command, ", ");

	/* Append snapshot_log */
	appendStringInfoString(command, "\"snapshot-log\":");
	AppendIcebergSnapshotLogEntries(command, metadata->snapshot_log, metadata->snapshot_log_length);

	/* Append partition_statistics */
	if (metadata->partition_statistics_length > 0)
	{
		appendStringInfoString(command, ", ");

		appendStringInfoString(command, "\"partition-statistics\":");
		AppendcebergPartitionStatistics(command, metadata->partition_statistics, metadata->partition_statistics_length);
	}

	appendStringInfoString(command, ", ");

	/* Append metadata_log */

	appendStringInfoString(command, "\"metadata-log\":");
	AppendIcebergMetadataLogEntries(command, metadata->metadata_log, metadata->metadata_log_length);
	appendStringInfoString(command, ", ");

	/* Append sort_orders */
	appendStringInfo(command, "\"sort-orders\":");
	AppendIcebergSortOrders(command, metadata->sort_orders, metadata->sort_orders_length);
	appendStringInfoString(command, ", ");

	/* Append default_sort_order_id */
	appendJsonInt32(command, "default-sort-order-id", metadata->default_sort_order_id);

	appendStringInfoString(command, ", ");

	/* Append refs */
	appendStringInfoString(command, "\"refs\":");
	AppendSnapshotReferences(command, metadata->refs, metadata->refs_length);

	appendStringInfoString(command, ", ");

	/* Append statistics */
	appendStringInfoString(command, "\"statistics\":");
	AppendIcebergStatistics(command, metadata->statistics, metadata->statistics_length);

	appendStringInfoString(command, "}");

	return command->data;
}

static void
AppendProperties(StringInfo command, Property * properties, size_t properties_length)
{
	appendStringInfoString(command, "{");

	for (size_t i = 0; i < properties_length; i++)
	{
		/* Append key */
		appendJsonString(command, properties[i].key, properties[i].value);

		if (i < properties_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "}");
}


static void
AppendIntArray(StringInfo command, int *array, size_t array_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < array_length; i++)
	{
		appendStringInfo(command, "%d", array[i]);

		if (i < array_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static void
AppendIcebergTableSchemas(StringInfo command, IcebergTableSchema * schemas, size_t schemas_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < schemas_length; i++)
	{
		appendStringInfoString(command, "{");

		/* append type */
		appendJsonString(command, "type", schemas[i].type);
		appendStringInfoString(command, ", ");

		/* Append schema_id */
		appendJsonInt32(command, "schema-id", schemas[i].schema_id);

		if (schemas[i].identifier_field_ids_length > 0)
		{
			appendStringInfoString(command, ", ");
			appendStringInfoString(command, "\"identifier-field-ids\":");
			AppendIntArray(command, schemas[i].identifier_field_ids,
						   schemas[i].identifier_field_ids_length);
		}

		/* Append fields */
		appendStringInfoString(command, ", ");

		appendStringInfoString(command, "\"fields\":");
		AppendIcebergStructFields(command, schemas[i].fields, schemas[i].fields_length);

		appendStringInfoString(command, "}");

		if (i < schemas_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}


/*
* Similar to AppendIcebergTableSchemas, but specifically for Rest Catalog stage
* API calls.
*/
void
AppendIcebergTableSchemaForRestCatalog(StringInfo command, IcebergTableSchema * schemas, size_t schemas_length)
{
	appendStringInfoString(command, "\"schema\":");

	for (size_t i = 0; i < schemas_length; i++)
	{
		appendStringInfoString(command, "{");

		/* append type */
		appendJsonString(command, "type", schemas[i].type);

		if (schemas[i].identifier_field_ids_length > 0)
		{
			appendStringInfoString(command, ", ");
			appendStringInfoString(command, "\"identifier-field-ids\":");
			AppendIntArray(command, schemas[i].identifier_field_ids,
						   schemas[i].identifier_field_ids_length);
		}

		/* Append fields */
		appendStringInfoString(command, ", ");

		appendStringInfoString(command, "\"fields\":");
		AppendIcebergStructFields(command, schemas[i].fields, schemas[i].fields_length);

		appendStringInfoString(command, "}");

		if (i < schemas_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}
}


static void
AppendIcebergPartitionSpecs(StringInfo command, IcebergPartitionSpec * specs, size_t specs_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < specs_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append spec_id */
		appendJsonInt32(command, "spec-id", specs[i].spec_id);
		appendStringInfoString(command, ", ");

		/* Append fields */
		appendStringInfoString(command, "\"fields\":");
		AppendIcebergPartitionSpecFields(command, specs[i].fields, specs[i].fields_length);

		appendStringInfoString(command, "}");

		if (i < specs_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static void
AppendIcebergSnapshots(StringInfo command, IcebergSnapshot * snapshots, size_t snapshots_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < snapshots_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append snapshot_id */
		appendJsonInt64(command, "snapshot-id", snapshots[i].snapshot_id);

		/* Append parent_snapshot_id */
		if (snapshots[i].parent_snapshot_id != 0)
		{
			appendStringInfoString(command, ", ");
			appendJsonInt64(command, "parent-snapshot-id", snapshots[i].parent_snapshot_id);
		}
		/* Append sequence_number */
		appendStringInfoString(command, ", ");
		appendJsonInt64(command, "sequence-number", snapshots[i].sequence_number);
		appendStringInfoString(command, ", ");

		/* Append timestamp_ms */
		appendJsonInt64(command, "timestamp-ms", snapshots[i].timestamp_ms);
		appendStringInfoString(command, ", ");

		/* Append manifest_list */
		appendJsonString(command, "manifest-list", snapshots[i].manifest_list);
		appendStringInfoString(command, ", ");

		appendStringInfoString(command, "\"summary\":");
		AppendProperties(command, snapshots[i].summary, snapshots[i].summary_length);

		/* Append schema_id */
		if (snapshots[i].schema_id_set)
		{
			appendStringInfoString(command, ", ");
			appendJsonInt32(command, "schema-id", snapshots[i].schema_id);
		}
		appendStringInfoString(command, "}");

		if (i < snapshots_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static void
AppendIcebergSnapshotLogEntries(StringInfo command, IcebergSnapshotLogEntry * entries, size_t entries_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < entries_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append timestamp_ms */
		appendJsonInt64(command, "timestamp-ms", entries[i].timestamp_ms);
		appendStringInfoString(command, ", ");

		/* Append snapshot_id */
		appendJsonInt64(command, "snapshot-id", entries[i].snapshot_id);

		appendStringInfoString(command, "}");

		if (i < entries_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}


static void
AppendcebergPartitionStatistics(StringInfo command, IcebergPartitionStatistics * entries, size_t entries_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < entries_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append timestamp_ms */
		appendJsonInt64(command, "file-size-in-bytes", entries[i].file_size_in_bytes);
		appendStringInfoString(command, ", ");

		/* Append snapshot_id */
		appendJsonInt64(command, "snapshot-id", entries[i].snapshot_id);

		appendStringInfoString(command, ", ");
		appendJsonString(command, "statistics-path", entries[i].statistics_path);
		appendStringInfoString(command, "}");

		if (i < entries_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static void
AppendIcebergMetadataLogEntries(StringInfo command, IcebergMetadataLogEntry * entries, size_t entries_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < entries_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append timestamp_ms */
		appendJsonInt64(command, "timestamp-ms", entries[i].timestamp_ms);
		appendStringInfoString(command, ", ");

		/* Append metadata_file */
		appendJsonString(command, "metadata-file", entries[i].metadata_file);

		appendStringInfoString(command, "}");

		if (i < entries_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static void
AppendIcebergSortOrders(StringInfo command, IcebergSortOrder * orders, size_t orders_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < orders_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append order_id */
		appendJsonInt32(command, "order-id", orders[i].order_id);
		appendStringInfoString(command, ", ");

		/* Append fields */
		appendStringInfoString(command, "\"fields\":");
		AppendIcebergSortOrderFields(command, orders[i].fields, orders[i].fields_length);

		appendStringInfoString(command, "}");

		if (i < orders_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static const char *
GetSnapshotReferenceType(SnapshotReferenceType type)
{
	switch (type)
	{
		case SNAPSHOT_REFERENCE_TYPE_TAG:
			return "tag";
		case SNAPSHOT_REFERENCE_TYPE_BRANCH:
			return "branch";
		case SNAPSHOT_REFERENCE_TYPE_INVALID:
		default:
			return "invalid";
	}
}

static void
AppendSnapshotReferences(StringInfo command, SnapshotReference * refs, size_t refs_length)
{
	appendStringInfoString(command, "{");

	for (size_t i = 0; i < refs_length; i++)
	{
		/* Append key */
		appendStringInfo(command, "\"%s\":", refs[i].key);
		appendStringInfoString(command, "{");

		/* Append snapshot_id */
		appendJsonInt64(command, "snapshot-id", refs[i].snapshot_id);
		appendStringInfoString(command, ", ");

		/* Append type */
		const char *type_str = GetSnapshotReferenceType(refs[i].type);

		appendStringInfo(command, "\"type\":\"%s\"", type_str);

		/* Append min_snapshots_to_keep if available */
		if (refs[i].has_min_snapshots_to_keep)
		{
			appendStringInfoString(command, ", ");
			appendJsonInt32(command, "min-snapshots-to-keep", refs[i].min_snapshots_to_keep);
		}

		/* Append max_snapshot_age_ms if available */
		if (refs[i].has_max_snapshot_age_ms)
		{
			appendStringInfoString(command, ", ");
			appendJsonInt64(command, "max-snapshot-age-ms", refs[i].max_snapshot_age_ms);
		}

		/* Append max_ref_age_ms if available */
		if (refs[i].has_max_ref_age_ms)
		{
			appendStringInfoString(command, ", ");
			appendJsonInt64(command, "max-ref-age-ms", refs[i].max_ref_age_ms);
		}


		appendStringInfoString(command, "}");

		if (i < refs_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "}");
}

static void
AppendIcebergStatistics(StringInfo command, IcebergStatistics * statistics, size_t statistics_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < statistics_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append snapshot_id */
		appendJsonInt64(command, "snapshot-id", statistics[i].snapshot_id);
		appendStringInfoString(command, ", ");

		/* Append statistics_path */
		appendJsonString(command, "statistics-path", statistics[i].statistics_path);
		appendStringInfoString(command, ", ");

		/* Append file_size_in_bytes */
		appendJsonInt64(command, "file-size-in-bytes", statistics[i].file_size_in_bytes);
		appendStringInfoString(command, ", ");

		/* Append file_footer_size_in_bytes */
		appendJsonInt64(command, "file-footer-size-in-bytes", statistics[i].file_footer_size_in_bytes);

		/* Append key_metadata */
		if (statistics[i].key_metadata != NULL)
		{
			appendStringInfoString(command, ", ");
			appendJsonString(command, "key-metadata", statistics[i].key_metadata);
		}

		appendStringInfoString(command, ", ");

		/* Append blobs using AppendBlobMetadata */
		appendStringInfoString(command, "\"blob-metadata\":");
		AppendBlobMetadata(command, statistics[i].blobs, statistics[i].blobs_length);

		appendStringInfoString(command, "}");

		if (i < statistics_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}


static void
AppendField(StringInfo command, Field * field)
{
	switch (field->type)
	{
		case FIELD_TYPE_SCALAR:
			appendStringInfo(command, "\"%s\"", field->field.scalar.typeName);
			break;
		case FIELD_TYPE_LIST:
			appendStringInfoString(command, "{");
			appendJsonString(command, "type", "list");
			appendStringInfoString(command, ", ");
			appendStringInfo(command, "\"element-id\":%d", field->field.list.elementId);
			appendStringInfoString(command, ", ");
			appendJsonKey(command, "element");
			AppendField(command, field->field.list.element);
			appendStringInfoString(command, ", ");
			appendJsonBool(command, "element-required", field->field.list.elementRequired);
			appendStringInfoString(command, "}");
			break;
		case FIELD_TYPE_MAP:
			appendStringInfoString(command, "{");
			appendJsonString(command, "type", "map");
			appendStringInfoString(command, ", ");
			appendStringInfo(command, "\"key-id\":%d", field->field.map.keyId);
			appendStringInfoString(command, ", ");
			appendJsonKey(command, "key");
			AppendField(command, field->field.map.key);
			appendStringInfoString(command, ", ");
			appendStringInfo(command, "\"value-id\":%d", field->field.map.valueId);
			appendStringInfoString(command, ", ");
			appendJsonKey(command, "value");
			AppendField(command, field->field.map.value);
			appendStringInfoString(command, ", ");
			appendJsonBool(command, "value-required", field->field.map.valueRequired);
			appendStringInfoString(command, "}");
			break;
		case FIELD_TYPE_STRUCT:
			appendStringInfoString(command, "{");
			appendJsonString(command, "type", "struct");
			appendStringInfoString(command, ", ");
			appendStringInfoString(command, "\"fields\":");
			AppendIcebergStructFields(command, field->field.structType.fields, field->field.structType.nfields);
			appendStringInfoString(command, "}");
			break;

		default:
			elog(ERROR, "Invalid field type: %d", field->type);
	}
}

static void
AppendIcebergStructFields(StringInfo command, FieldStructElement * fields, size_t fields_length)
{
	appendStringInfoString(command, "[");

	bool		addComma = false;

	for (size_t i = 0; i < fields_length; i++)
	{
		FieldStructElement *field = &fields[i];

		if (addComma)
		{
			appendStringInfoString(command, ", ");
		}

		appendStringInfoString(command, "{");

		/* Append id */
		appendJsonInt32(command, "id", field->id);
		appendStringInfoString(command, ", ");

		/* Append name */
		appendJsonString(command, "name", field->name);
		appendStringInfoString(command, ", ");
		if (field->type == NULL)
		{
			elog(ERROR, "field is NULL");
		}
		else if (field->type->type == FIELD_TYPE_SCALAR)
		{
			appendJsonKey(command, "type");
			AppendField(command, field->type);
		}
		else if (field->type->type == FIELD_TYPE_MAP ||
				 field->type->type == FIELD_TYPE_LIST ||
				 field->type->type == FIELD_TYPE_STRUCT)
		{
			appendJsonKey(command, "type");
			AppendField(command, field->type);
		}
		else
		{
			elog(ERROR, "Invalid type: %d", field->type->type);
		}

		appendStringInfoString(command, ", ");

		/* Append required */
		appendJsonBool(command, "required", field->required);

		/* Append doc */
		if (field->doc != NULL)
		{
			appendStringInfoString(command, ", ");
			appendJsonString(command, "doc", field->doc);
		}
		/* Append already escaped initial_default */
		if (fields[i].initialDefault != NULL)
		{
			appendStringInfoString(command, ", ");
			appendJsonStringWithEscapedValue(command, "initial-default", fields[i].initialDefault);
		}

		/* Append already escaped write_default */
		if (fields[i].writeDefault != NULL)
		{
			appendStringInfoString(command, ", ");
			appendJsonStringWithEscapedValue(command, "write-default", fields[i].writeDefault);
		}
		appendStringInfoString(command, "}");
		addComma = true;
	}

	appendStringInfoString(command, "]");
}


void
AppendIcebergPartitionSpecFields(StringInfo command, IcebergPartitionSpecField * fields, size_t fields_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < fields_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append source_id */
		appendJsonInt32(command, "source-id", fields[i].source_id);

		/* Append source_ids */
		if (fields[i].source_ids_length > 0)
		{
			appendStringInfoString(command, ", ");
			appendStringInfoString(command, "\"source-ids\":");
			AppendIntArray(command, fields[i].source_ids, fields[i].source_ids_length);
		}

		/* Append field_id */
		appendStringInfoString(command, ", ");
		appendJsonInt32(command, "field-id", fields[i].field_id);
		appendStringInfoString(command, ", ");

		/* Append name */
		appendJsonString(command, "name", fields[i].name);
		appendStringInfoString(command, ", ");

		/* Append transform */
		appendJsonString(command, "transform", fields[i].transform);

		appendStringInfoString(command, "}");

		if (i < fields_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static void
AppendIcebergSortOrderFields(StringInfo command, IcebergSortOrderField * fields, size_t fields_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < fields_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append transform */
		appendJsonString(command, "transform", fields[i].transform);
		appendStringInfoString(command, ", ");

		/* Append source_id */
		appendJsonInt32(command, "source-id", fields[i].source_id);
		appendStringInfoString(command, ", ");

		/* Append direction */
		appendJsonString(command, "direction", fields[i].direction);
		appendStringInfoString(command, ", ");

		/* Append null_order */
		appendJsonString(command, "null-order", fields[i].null_order);

		appendStringInfoString(command, "}");

		if (i < fields_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}

static void
AppendBlobMetadata(StringInfo command, BlobMetadata * metadata, size_t metadata_length)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < metadata_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append type */
		appendJsonString(command, "type", metadata[i].type);
		appendStringInfoString(command, ", ");

		/* Append snapshot_id */
		appendJsonInt64(command, "snapshot-id", metadata[i].snapshot_id);
		appendStringInfoString(command, ", ");

		/* Append sequence_number */
		appendJsonInt64(command, "sequence-number", metadata[i].sequence_number);
		appendStringInfoString(command, ", ");

		/* Append fields */
		appendStringInfoString(command, "\"fields\":");
		AppendIntArray(command, metadata[i].fields, metadata[i].fields_length);

		if (metadata[i].properties_length > 0)
		{
			appendStringInfoString(command, ", ");

			/* Append properties */
			appendStringInfoString(command, "\"properties\":");
			AppendProperties(command, metadata[i].properties, metadata[i].properties_length);
		}

		appendStringInfoString(command, "}");

		if (i < metadata_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}

	appendStringInfoString(command, "]");
}
