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
* manifest.h - Iceberg manifest and manifest list reading and writing
* In this file, we define the structures that represent the Iceberg manifest
* and manifest list and also the functions to read and write them.
*
* Below is common flow while reading the Iceberg manifest and manifest list:
* - Manifest list path is extracted from a snapshot in metadata,
* - Manifest list is read from the location and manifest file locations are extracted,
* - Each manifest file contains manifest entries. The manifest files are read and
*   the manifest entries are extracted,
* - The manifest entries includes the data file (usually in parquet format) information.
*
* The file also contains the functions to update and write the manifest and manifest list.
*
* The spec can be found https://iceberg.apache.org/spec/
*/

#pragma once

#include "avro.h"

#include "nodes/pg_list.h"


typedef enum IcebergManifestEntryStatus
{
	ICEBERG_MANIFEST_ENTRY_STATUS_EXISTING = 0,
	ICEBERG_MANIFEST_ENTRY_STATUS_ADDED = 1,
	ICEBERG_MANIFEST_ENTRY_STATUS_DELETED = 2
} IcebergManifestEntryStatus;

typedef enum IcebergManifestContentType
{
	ICEBERG_MANIFEST_FILE_CONTENT_DATA = 0,
	ICEBERG_MANIFEST_FILE_CONTENT_DELETES = 1
} IcebergManifestContentType;

typedef enum IcebergDataFileContentType
{
	ICEBERG_DATA_FILE_CONTENT_DATA = 0,
	ICEBERG_DATA_FILE_CONTENT_POSITION_DELETES = 1,
	ICEBERG_DATA_FILE_CONTENT_EQUALITY_DELETES = 2
} IcebergDataFileContentType;

typedef struct ColumnStat
{
	int32_t		column_id;
	int64_t		value;
} ColumnStat;

typedef struct ColumnBound
{
	int32_t		column_id;
	unsigned char *value;
	size_t		value_length;
} ColumnBound;

typedef struct FieldSummary
{
	bool		contains_null;
	bool		contains_nan;

	unsigned char *lower_bound;
	size_t		lower_bound_length;

	unsigned char *upper_bound;
	size_t		upper_bound_length;
} FieldSummary;

/*
* Important: Never change the order of the enum values.
* We store these values in the catalog and changing the order will break
* the compatibility with the existing data.
*/
typedef struct IcebergScalarAvroType
{
	enum
	{
		ICEBERG_AVRO_PHYSICAL_TYPE_INT32 = 0,
		ICEBERG_AVRO_PHYSICAL_TYPE_INT64 = 1,
		ICEBERG_AVRO_PHYSICAL_TYPE_STRING = 2,
		ICEBERG_AVRO_PHYSICAL_TYPE_BINARY = 3,
		ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT = 4,
		ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE = 5,
		ICEBERG_AVRO_PHYSICAL_TYPE_BOOL = 6,
	}			physical_type;

	enum
	{
		ICEBERG_AVRO_LOGICAL_TYPE_NONE = 0,
		ICEBERG_AVRO_LOGICAL_TYPE_DATE = 1,
		ICEBERG_AVRO_LOGICAL_TYPE_TIMESTAMP = 2,
		ICEBERG_AVRO_LOGICAL_TYPE_TIME = 3,
		ICEBERG_AVRO_LOGICAL_TYPE_DECIMAL = 4,
		ICEBERG_AVRO_LOGICAL_TYPE_UUID = 5,
	}			logical_type;

	/* logical type: decimal */
	int32_t		precision;
	int32_t		scale;
} IcebergScalarAvroType;

typedef struct PartitionField
{
	char	   *field_name;
	int32_t		field_id;
	IcebergScalarAvroType value_type;
	void	   *value;
	size_t		value_length;
} PartitionField;

typedef struct Partition
{
	PartitionField *fields;
	size_t		fields_length;
} Partition;

/*
 * IcebergManifest struct represents the Iceberg manifest file.
 * It should be read from a manifest file, location of which can be
 * obtained by a manifest list file.
 */
typedef struct IcebergManifest
{
	const char *manifest_path;
	size_t		manifest_path_length;

	int64_t		manifest_length;
	int32_t		partition_spec_id;
	IcebergManifestContentType content;
	int64_t		sequence_number;
	int64_t		min_sequence_number;
	int64_t		added_snapshot_id;
	int32_t		added_files_count;
	int32_t		existing_files_count;
	int32_t		deleted_files_count;
	int64_t		added_rows_count;
	int64_t		existing_rows_count;
	int64_t		deleted_rows_count;

	FieldSummary *partitions;
	size_t		partitions_length;

	char	   *key_metadata;
	size_t		key_metadata_length;
} IcebergManifest;

/*
 * DataFile struct represents the data file in the Iceberg manifest.
 * You can find the information about the data file in the manifest entry.
 */
typedef struct DataFile
{
	IcebergDataFileContentType content;

	const char *file_path;
	size_t		file_path_length;

	const char *file_format;
	size_t		file_format_length;

	Partition	partition;

	int64_t		record_count;
	int64_t		file_size_in_bytes;

	ColumnStat *column_sizes;
	size_t		column_sizes_length;

	ColumnStat *value_counts;
	size_t		value_counts_length;

	ColumnStat *null_value_counts;
	size_t		null_value_counts_length;

	ColumnStat *nan_value_counts;
	size_t		nan_value_counts_length;

	ColumnBound *lower_bounds;
	size_t		lower_bounds_length;

	ColumnBound *upper_bounds;
	size_t		upper_bounds_length;

	char	   *key_metadata;
	size_t		key_metadata_length;

	int64_t    *split_offsets;
	size_t		split_offsets_length;

	int		   *equality_ids;
	size_t		equality_ids_length;

	bool		has_sort_order_id;
	int32_t		sort_order_id;
} DataFile;

/*
 * IcebergManifestEntry struct represents the Iceberg manifest entry.
 * It should be read from a manifest file.
 */
typedef struct IcebergManifestEntry
{
	IcebergManifestEntryStatus status;

	bool		has_snapshot_id;
	int64_t		snapshot_id;

	bool		has_sequence_number;
	int64_t		sequence_number;

	bool		has_file_sequence_number;
	int64_t		file_sequence_number;

	DataFile	data_file;
} IcebergManifestEntry;

extern List *ReadIcebergManifests(const char *manifestListPath);
extern PGDLLEXPORT List *ReadManifestEntries(const char *manifestPath);

extern PGDLLEXPORT void WriteIcebergManifestList(const char *manifestListPath, List *manifests);
extern PGDLLEXPORT void WriteIcebergManifest(const char *manifestPath, List *manifestEntries);
