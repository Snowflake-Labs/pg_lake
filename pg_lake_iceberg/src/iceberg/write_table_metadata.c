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
#include "pg_lake/iceberg/format_version.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/json/json_reader.h"
#include "pg_lake/json/json_utils.h"

#include "utils/json.h"
#include "utils/jsonb.h"
#include "utils/builtins.h"

/*
 * Per-call writer context.
 *
 * Threaded explicitly through every internal Append* helper that has to
 * branch on a format-version-gated feature. Each version-aware field is
 * its own bool so the two known entry points -- the local metadata.json
 * writer and the REST add-schema request builder -- can disagree where
 * they need to without sharing file-scope state.
 *
 * The single asymmetry: ``allowColumnDefaults`` is *not* derived from
 * ``formatVersion`` for the REST add-schema path. The REST catalog (e.g.
 * Polaris) is its own Iceberg implementation: it owns its on-disk
 * metadata.json and is responsible for its own format-version compliance.
 * pg_lake therefore always sends the full schema (defaults and all) over
 * the REST wire and lets the catalog decide what to persist. Stripping
 * defaults on the REST wire makes consecutive ALTERs look idempotent to
 * the catalog -- the new metadata.json revision is collapsed away and
 * pg_lake's local ``metadata_location`` cache never advances, so the next
 * ALTER tries to enqueue the same already-deleted path in
 * ``deletion_queue`` a second time and trips the PK. See
 * test_set_default_on_one_column_drop_default_on_another for the
 * regression that drove this gate apart from the metadata.json gate.
 */
typedef struct IcebergMetadataWriteContext
{
	IcebergFormatVersion formatVersion;
	bool		allowColumnDefaults;
}			IcebergMetadataWriteContext;

static void AppendProperties(StringInfo command, Property * properties, size_t properties_length);
static void AppendField(StringInfo command, Field * field, const IcebergMetadataWriteContext * ctx);
static void AppendIcebergStructFields(StringInfo command, FieldStructElement * fields, size_t fields_length, const IcebergMetadataWriteContext * ctx);
static void AppendIcebergTableSchemas(StringInfo command, IcebergTableSchema * schemas, size_t schemas_length, const IcebergMetadataWriteContext * ctx);
static void AppendIcebergPartitionSpecs(StringInfo command, IcebergPartitionSpec * specs, size_t specs_length, const IcebergMetadataWriteContext * ctx);
static void AppendIcebergSnapshots(StringInfo command, IcebergSnapshot * snapshots, size_t snapshots_length);
static void AppendIcebergSnapshotLogEntries(StringInfo command, IcebergSnapshotLogEntry * entries, size_t entries_length);
static void AppendcebergPartitionStatistics(StringInfo command, IcebergPartitionStatistics * entries, size_t entries_length);
static void AppendIcebergMetadataLogEntries(StringInfo command, IcebergMetadataLogEntry * entries, size_t entries_length);
static void AppendIcebergSortOrderFields(StringInfo command, IcebergSortOrderField * fields, size_t fields_length, const IcebergMetadataWriteContext * ctx);
static void AppendIcebergSortOrders(StringInfo command, IcebergSortOrder * orders, size_t orders_length, const IcebergMetadataWriteContext * ctx);
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

	/*
	 * Per-call context for the metadata.json write path. ``allowColumn-
	 * Defaults`` is derived from the active format version: v3 emits column
	 * defaults, v2 must not (a v2 spec violation that some external readers
	 * reject). The companion writer-side gate at the ADD COLUMN site
	 * (register_field_ids.c::CreatePostgresColumnMappingsForColumnDefs)
	 * prevents users from *adding* a v2 column with a default in the first
	 * place; this gate is the belt-and-braces for legacy v2 tables that still
	 * have a writeDefault sitting in pg_lake's local catalog.
	 *
	 * For the REST add-schema path see AppendIcebergTableSchemaForRest-
	 * Catalog below, which builds its own context with ``allowColumn-
	 * Defaults = true`` regardless of the active version (the REST catalog
	 * owns its own format-version compliance and stripping on the wire causes
	 * a Polaris idempotency collapse -- see the typedef comment).
	 */
	IcebergMetadataWriteContext ctx = {
		.formatVersion = metadata->format_version,
		.allowColumnDefaults =
		IcebergFormatVersionSupportsColumnDefaults(metadata->format_version),
	};

	appendStringInfoString(command, "{");

	/* Append format_version */
	appendJsonInt32(command, "format-version", IcebergFormatVersionToInt(metadata->format_version));
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
	AppendIcebergTableSchemas(command, metadata->schemas, metadata->schemas_length, &ctx);
	appendStringInfoString(command, ", ");

	/* Append default_spec_id */
	appendJsonInt32(command, "default-spec-id", metadata->default_spec_id);
	appendStringInfoString(command, ", ");

	/* Append partition_specs */
	appendStringInfoString(command, "\"partition-specs\":");
	AppendIcebergPartitionSpecs(command, metadata->partition_specs, metadata->partition_specs_length, &ctx);
	appendStringInfoString(command, ", ");

	/* Append last_partition_id */
	appendJsonInt32(command, "last-partition-id", metadata->last_partition_id);
	appendStringInfoString(command, ", ");

	/* Append properties */
	appendStringInfoString(command, "\"properties\":");
	AppendProperties(command, metadata->properties, metadata->properties_length);
	appendStringInfoString(command, ", ");

	/*
	 * Append current_snapshot_id.
	 *
	 * Per Iceberg spec, ``current-snapshot-id`` is the id of the latest
	 * referenced snapshot, or a sentinel meaning "no snapshot exists yet".
	 * The shape of that sentinel changed between format versions:
	 *
	 * * v1 / v2: a JSON integer of -1. * v3:      JSON ``null`` (or the field
	 * omitted entirely; we always emit it for symmetry with every other
	 * Spark-reference output and so external readers do not have to fall back
	 * to ``find`` semantics).
	 *
	 * We use ``current_snapshot_id == -1`` as the in-memory sentinel for "no
	 * snapshot exists" in both versions (see InitializeIcebergTableMetadata
	 * in api/table_metadata.c). The version-aware predicate keeps the
	 * version-specific JSON shape isolated to this one branch -- callers
	 * never have to know whether the table is v2 or v3.
	 */
	if (metadata->current_snapshot_id == -1 &&
		IcebergFormatVersionSupportsNullCurrentSnapshotId(metadata->format_version))
	{
		appendJsonKey(command, "current-snapshot-id");
		appendStringInfoString(command, "null");
	}
	else
	{
		appendJsonInt64(command, "current-snapshot-id", metadata->current_snapshot_id);
	}
	appendStringInfoString(command, ", ");

	/* Append snapshots */
	appendStringInfoString(command, "\"snapshots\":");
	AppendIcebergSnapshots(command, metadata->snapshots, metadata->snapshots_length);
	appendStringInfoString(command, ", ");

	/* Append snapshot_log */
	appendStringInfoString(command, "\"snapshot-log\":");
	AppendIcebergSnapshotLogEntries(command, metadata->snapshot_log, metadata->snapshot_log_length);

	/*
	 * Append partition-statistics.
	 *
	 * We always emit this field, even when the array is empty, to match the
	 * convention adopted by the Iceberg reference Java writer (and therefore
	 * Spark) starting in iceberg-spark-runtime 1.10.x: empty optional arrays
	 * at the top level of the table metadata JSON are serialized as `[]`
	 * rather than being elided. Always emitting it keeps our round-trip tests
	 * (which compare pg_lake's output to Spark's) byte-stable against the new
	 * reference behaviour, and lets pg_lake be drop-in compatible with
	 * engines that key off the field's presence.
	 */
	appendStringInfoString(command, ", ");

	appendStringInfoString(command, "\"partition-statistics\":");
	AppendcebergPartitionStatistics(command,
									metadata->partition_statistics,
									metadata->partition_statistics_length);

	appendStringInfoString(command, ", ");

	/* Append metadata_log */

	appendStringInfoString(command, "\"metadata-log\":");
	AppendIcebergMetadataLogEntries(command, metadata->metadata_log, metadata->metadata_log_length);
	appendStringInfoString(command, ", ");

	/* Append sort_orders */
	appendStringInfo(command, "\"sort-orders\":");
	AppendIcebergSortOrders(command, metadata->sort_orders, metadata->sort_orders_length, &ctx);
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

	/*
	 * Iceberg v3 row lineage: emit ``next-row-id`` byte-faithfully relative
	 * to the input metadata. The struct flag ``has_next_row_id`` is set by
	 * the reader iff the field was present on disk and is set by the upcoming
	 * Stage 12 allocator whenever it owns the value; in all other cases we
	 * must not emit it (v2 metadata never has it, and v3 readers key off
	 * presence to know whether to walk per-snapshot row-ranges).
	 */
	if (metadata->has_next_row_id)
	{
		appendStringInfoString(command, ", ");
		appendJsonInt64(command, "next-row-id", metadata->next_row_id);
	}

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
AppendIcebergTableSchemas(StringInfo command, IcebergTableSchema * schemas, size_t schemas_length, const IcebergMetadataWriteContext * ctx)
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
		AppendIcebergStructFields(command, schemas[i].fields, schemas[i].fields_length, ctx);

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
AppendIcebergTableSchemaForRestCatalog(StringInfo command, IcebergTableSchema * schemas, size_t schemas_length, IcebergFormatVersion formatVersion)
{
	/*
	 * REST catalog (e.g. Polaris) is its own Iceberg implementation and is
	 * responsible for its own format-version compliance, so we send the full
	 * schema -- column defaults and all -- regardless of the active version.
	 * See the typedef comment on IcebergMetadataWriteContext and the public
	 * doc on AppendIcebergTableSchemaForRestCatalog in metadata_spec.h for
	 * the wire-level rationale, and the test_set_default_on_one_column_drop_
	 * default_on_another regression that this independence guards.
	 *
	 * The version still flows through ctx for the partition/sort-order
	 * ``source-ids`` plural gate, but those keys are not reachable from this
	 * helper -- it only writes schema fields. The version is harmless extra
	 * baggage here; threading it keeps every entry-point's ctx construction
	 * identical and resilient to a future v3-only schema-field gate.
	 */
	IcebergMetadataWriteContext ctx = {
		.formatVersion = formatVersion,
		.allowColumnDefaults = true,
	};

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
		AppendIcebergStructFields(command, schemas[i].fields, schemas[i].fields_length, &ctx);

		appendStringInfoString(command, "}");

		if (i < schemas_length - 1)
		{
			appendStringInfoString(command, ", ");
		}
	}
}


static void
AppendIcebergPartitionSpecs(StringInfo command, IcebergPartitionSpec * specs, size_t specs_length, const IcebergMetadataWriteContext * ctx)
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
		AppendIcebergPartitionSpecFields(command, specs[i].fields, specs[i].fields_length, ctx->formatVersion);

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

		/*
		 * Iceberg v3 row lineage emission. Both fields are optional per spec
		 * (a v3 snapshot that didn't append rows -- e.g. a future
		 * metadata-only commit -- may legitimately omit them) so we mirror
		 * the read side and only emit when the corresponding _set bit is on.
		 * This keeps reserialise round-trips byte-equal and avoids
		 * accidentally promoting a v2 snapshot to v3 wire shape if the struct
		 * is later reused across format versions.
		 */
		if (snapshots[i].first_row_id_set)
		{
			appendStringInfoString(command, ", ");
			appendJsonInt64(command, "first-row-id", snapshots[i].first_row_id);
		}
		if (snapshots[i].added_rows_set)
		{
			appendStringInfoString(command, ", ");
			appendJsonInt64(command, "added-rows", snapshots[i].added_rows);
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
AppendIcebergSortOrders(StringInfo command, IcebergSortOrder * orders, size_t orders_length, const IcebergMetadataWriteContext * ctx)
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
		AppendIcebergSortOrderFields(command, orders[i].fields, orders[i].fields_length, ctx);

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
AppendField(StringInfo command, Field * field, const IcebergMetadataWriteContext * ctx)
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
			AppendField(command, field->field.list.element, ctx);
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
			AppendField(command, field->field.map.key, ctx);
			appendStringInfoString(command, ", ");
			appendStringInfo(command, "\"value-id\":%d", field->field.map.valueId);
			appendStringInfoString(command, ", ");
			appendJsonKey(command, "value");
			AppendField(command, field->field.map.value, ctx);
			appendStringInfoString(command, ", ");
			appendJsonBool(command, "value-required", field->field.map.valueRequired);
			appendStringInfoString(command, "}");
			break;
		case FIELD_TYPE_STRUCT:
			appendStringInfoString(command, "{");
			appendJsonString(command, "type", "struct");
			appendStringInfoString(command, ", ");
			appendStringInfoString(command, "\"fields\":");
			AppendIcebergStructFields(command, field->field.structType.fields, field->field.structType.nfields, ctx);
			appendStringInfoString(command, "}");
			break;

		default:
			elog(ERROR, "Invalid field type: %d", field->type);
	}
}

static void
AppendIcebergStructFields(StringInfo command, FieldStructElement * fields, size_t fields_length, const IcebergMetadataWriteContext * ctx)
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
			AppendField(command, field->type, ctx);
		}
		else if (field->type->type == FIELD_TYPE_MAP ||
				 field->type->type == FIELD_TYPE_LIST ||
				 field->type->type == FIELD_TYPE_STRUCT)
		{
			appendJsonKey(command, "type");
			AppendField(command, field->type, ctx);
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

		/*
		 * Append already escaped initial_default / write_default.
		 *
		 * Column defaults are a v3-only spec feature. The per-context
		 * ``allowColumnDefaults`` flag gates emission: the metadata.json
		 * write path sets it from the active format version (strip on v2),
		 * the REST add-schema path leaves it ``true`` (the catalog is its own
		 * Iceberg implementation -- see typedef comment / Polaris-
		 * idempotency rationale on test_set_default_on_one_column_drop_
		 * default_on_another).
		 *
		 * The companion gate at the ADD COLUMN site
		 * (register_field_ids.c::CreatePostgresColumnMappingsForColumnDefs)
		 * prevents users from *adding* a v2 column with a default in the
		 * first place, so the v2 metadata.json strip should only be exercised
		 * for legacy fields carried in pg_lake's local catalog from before
		 * that gate landed.
		 */
		if (ctx->allowColumnDefaults)
		{
			if (fields[i].initialDefault != NULL)
			{
				appendStringInfoString(command, ", ");
				appendJsonStringWithEscapedValue(command, "initial-default", fields[i].initialDefault);
			}

			if (fields[i].writeDefault != NULL)
			{
				appendStringInfoString(command, ", ");
				appendJsonStringWithEscapedValue(command, "write-default", fields[i].writeDefault);
			}
		}
		appendStringInfoString(command, "}");
		addComma = true;
	}

	appendStringInfoString(command, "]");
}


void
AppendIcebergPartitionSpecFields(StringInfo command, IcebergPartitionSpecField * fields, size_t fields_length, IcebergFormatVersion formatVersion)
{
	appendStringInfoString(command, "[");

	for (size_t i = 0; i < fields_length; i++)
	{
		appendStringInfoString(command, "{");

		/* Append source_id */
		appendJsonInt32(command, "source-id", fields[i].source_id);

		/*
		 * Append source-ids (plural).
		 *
		 * source-ids was introduced in Iceberg v3 to carry multi-argument
		 * partition transforms (e.g. a future bucket(N, x, y)). v2
		 * metadata.json never carries it; the upstream pg_lake commit that
		 * introduced the field populated source_ids_length=1 for every v2
		 * spec field "to comply with the reference implementation", but that
		 * reading of the spec was too liberal -- the reference v2 writers
		 * don't emit source-ids at all.
		 *
		 * Stage 18 of the v3 rollout tightens the writer to gate the emission
		 * on the format-version we're serialising for, keeping v2 outputs
		 * byte-identical to Spark / PyIceberg.
		 */
		if (IcebergFormatVersionSupportsMultiArgTransforms(formatVersion) &&
			fields[i].source_ids_length > 0)
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
AppendIcebergSortOrderFields(StringInfo command, IcebergSortOrderField * fields, size_t fields_length, const IcebergMetadataWriteContext * ctx)
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
