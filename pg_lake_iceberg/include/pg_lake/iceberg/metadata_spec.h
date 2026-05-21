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

/*
* metadata.h - Iceberg metadata parsing and serialization
* In this file, we define the structures that represent the Iceberg metadata
* and the functions to parse and serialize the metadata.
*
* The metadata is stored in JSON format and is parsed into the structures
* defined in this file. The metadata is serialized back to JSON format
* when writing the metadata.
*
* The spec can be found https://iceberg.apache.org/spec/
*/

#pragma once

#include "postgres.h"
#include "utils/jsonb.h"

#include "pg_lake/iceberg/format_version.h"
#include "pg_lake/parquet/field.h"


/*
* Generic struct that multiple other structs rely on.
* It represents a key-value pair.
*/
typedef struct Property
{
	const char *key;
	size_t		key_length;
	const char *value;
	size_t		value_length;
}			Property;


/*
* Struct that represents the schema of the Iceberg table.
*/
typedef struct IcebergTableSchema
{
	int32_t		schema_id;

	const char *type;
	size_t		type_length;

	DataFileSchemaField *fields;
	size_t		fields_length;

	int		   *identifier_field_ids;
	size_t		identifier_field_ids_length;
}			IcebergTableSchema;


/*
* Struct that represents the transformation of the
* partition spec.
*/
typedef struct IcebergPartitionSpecField
{
	int32_t		source_id;
	int		   *source_ids;
	size_t		source_ids_length;

	int32_t		field_id;
	const char *name;
	size_t		name_length;
	const char *transform;
	size_t		transform_length;
}			IcebergPartitionSpecField;

/*
* Represents the partition specification of the Iceberg table.
*/
typedef struct IcebergPartitionSpec
{
	int32_t		spec_id;
	IcebergPartitionSpecField *fields;
	size_t		fields_length;
}			IcebergPartitionSpec;


/*
* Represents the snapshot of the Iceberg table.
*/
typedef struct IcebergSnapshot
{
	int64_t		snapshot_id;
	int64_t		parent_snapshot_id;
	int64_t		sequence_number;
	int64_t		timestamp_ms;
	const char *manifest_list;
	size_t		manifest_list_length;
	Property   *summary;
	size_t		summary_length;
	int32_t		schema_id;
	bool		schema_id_set;

	/*
	 * Row lineage (Iceberg v3 only).
	 *
	 * ``first_row_id`` is the inclusive starting row-id assigned to all
	 * *newly appended* rows in this snapshot; readers reconstruct each row's
	 * logical id from snapshot.first_row_id + data_file.first_row_id +
	 * row_position. ``added_rows`` is the count of newly-appended rows in
	 * this snapshot (i.e. the width of the [first_row_id, first_row_id +
	 * added_rows) range this snapshot claims from the table-level next-row-id
	 * counter).
	 *
	 * Both fields are optional in the v3 spec and omitted on v2; the
	 * accompanying _set flags distinguish "omitted" from "explicitly zero",
	 * matching the rest of this struct's convention.
	 *
	 * Allocator + emit wiring lands in Stage 12; this stage only parses and
	 * serialises the values as-is so v3 read fixtures round-trip cleanly.
	 */
	int64_t		first_row_id;
	bool		first_row_id_set;
	int64_t		added_rows;
	bool		added_rows_set;
}			IcebergSnapshot;

/*
* Represents the log entry of the Iceberg snapshot.
*/
typedef struct IcebergSnapshotLogEntry
{
	int64_t		timestamp_ms;
	int64_t		snapshot_id;
}			IcebergSnapshotLogEntry;

/*
* Represents the statistics of the Iceberg partition.
*/
typedef struct IcebergPartitionStatistics
{
	int64_t		snapshot_id;
	const char *statistics_path;
	size_t		statistics_path_length;
	int64_t		file_size_in_bytes;

}			IcebergPartitionStatistics;

/*
* Represents the log entry of the Iceberg metadata.
*/
typedef struct IcebergMetadataLogEntry
{
	int64_t		timestamp_ms;
	const char *metadata_file;
	size_t		metadata_file_length;
}			IcebergMetadataLogEntry;

/*
* Represents the sort order field of the Iceberg table.
*/
typedef struct IcebergSortOrderField
{
	const char *transform;
	size_t		transform_length;
	int32_t		source_id;
	const char *direction;
	size_t		direction_length;
	const char *null_order;
	size_t		null_order_length;
}			IcebergSortOrderField;

/*
* Represents the sort order of the Iceberg table.
*/
typedef struct IcebergSortOrder
{
	int32_t		order_id;
	IcebergSortOrderField *fields;
	size_t		fields_length;
}			IcebergSortOrder;

typedef enum SnapshotReferenceType
{
	SNAPSHOT_REFERENCE_TYPE_TAG,
	SNAPSHOT_REFERENCE_TYPE_BRANCH,
	SNAPSHOT_REFERENCE_TYPE_INVALID
}			SnapshotReferenceType;

/*
* Represents the reference of the Iceberg snapshot.
*/
typedef struct SnapshotReference
{
	const char *key;
	size_t		key_length;
	int64_t		snapshot_id;
	SnapshotReferenceType type;
	bool		has_min_snapshots_to_keep;
	int32_t		min_snapshots_to_keep;
	bool		has_max_snapshot_age_ms;
	int64_t		max_snapshot_age_ms;
	bool		has_max_ref_age_ms;
	int64_t		max_ref_age_ms;
}			SnapshotReference;

/*
* Represents the metadata of the blob-metadata field under
* statistics of Iceberg.
*/
typedef struct BlobMetadata
{
	const char *type;
	size_t		type_length;

	int64_t		snapshot_id;

	int64_t		sequence_number;

	int		   *fields;
	size_t		fields_length;

	Property   *properties;
	size_t		properties_length;

}			BlobMetadata;

/*
* Represents the statistics of the Iceberg table.
*/
typedef struct IcebergStatistics
{
	int64_t		snapshot_id;

	const char *statistics_path;
	size_t		statistics_path_length;

	int64_t		file_size_in_bytes;
	int64_t		file_footer_size_in_bytes;

	const char *key_metadata;
	size_t		key_metadata_length;

	BlobMetadata *blobs;
	size_t		blobs_length;
}			IcebergStatistics;


/*
* Represents the metadata of the Iceberg table. This is the
* main struct that holds all the metadata of the Iceberg table.
*/
typedef struct IcebergTableMetadata
{
	IcebergFormatVersion format_version;

	const char *table_uuid;
	size_t		table_uuid_length;

	const char *location;
	size_t		location_length;

	int64_t		last_sequence_number;
	int64_t		last_updated_ms;
	int32_t		last_column_id;

	int32_t		current_schema_id;
	IcebergTableSchema *schemas;
	size_t		schemas_length;

	int32_t		default_spec_id;
	IcebergPartitionSpec *partition_specs;
	size_t		partition_specs_length;

	int32_t		last_partition_id;

	Property   *properties;
	size_t		properties_length;

	IcebergPartitionStatistics *partition_statistics;
	size_t		partition_statistics_length;

	int64_t		current_snapshot_id;

	IcebergSnapshot *snapshots;
	size_t		snapshots_length;

	IcebergSnapshotLogEntry *snapshot_log;
	size_t		snapshot_log_length;

	IcebergMetadataLogEntry *metadata_log;
	size_t		metadata_log_length;

	IcebergSortOrder *sort_orders;
	size_t		sort_orders_length;

	int32_t		default_sort_order_id;
	SnapshotReference *refs;
	size_t		refs_length;

	IcebergStatistics *statistics;
	size_t		statistics_length;

	/*
	 * Row lineage (Iceberg v3 only, spec ``next-row-id``).
	 *
	 * ``next_row_id`` is the *exclusive* upper bound of all row-ids assigned
	 * so far across the table's history. The next snapshot's ``first_row_id``
	 * starts here. Tracked as an int64_t so a 1-trillion-row append still has
	 * head-room. ``has_next_row_id`` distinguishes "this v3 metadata has no
	 * snapshots yet (omit the field)" from "the value is 0 because no rows
	 * have been written yet" -- the former skips serialisation, the latter
	 * emits ``"next-row-id": 0``. For v2 metadata ``has_next_row_id`` is
	 * always false (the field is v3-only per spec).
	 *
	 * Allocator wiring lands in Stage 12 of the v3 rollout; this stage only
	 * parses and writes through the value byte-for-byte so the data model is
	 * v3-complete and v3 read fixtures round-trip with row lineage preserved.
	 */
	int64_t		next_row_id;
	bool		has_next_row_id;
}			IcebergTableMetadata;

extern PGDLLEXPORT IcebergTableMetadata * ReadIcebergTableMetadata(const char *tableMetadataPath);
extern PGDLLEXPORT char *WriteIcebergTableMetadataToJson(IcebergTableMetadata * metadata);
extern PGDLLEXPORT void AppendIcebergTableSchemaForRestCatalog(StringInfo command, IcebergTableSchema * schemas, size_t schemas_length);
extern PGDLLEXPORT void AppendIcebergPartitionSpecFields(StringInfo command, IcebergPartitionSpecField * fields, size_t fields_length);
