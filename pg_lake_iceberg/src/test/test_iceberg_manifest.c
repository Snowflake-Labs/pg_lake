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

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/iceberg/partitioning/partition.h"

#include "utils/builtins.h"

static const char *FetchCurrentSnapshotManifestListPathFromTableMetadataUri(const char *tableMetadataPath);
static List *FetchDataFilePathsFromTableMetadataUri(const char *tableMetadataPath, bool isDelete);
static List *FetchManifestPathsFromManifestListUri(const char *manifestListPath);
static const char *ExternalPartitionValueToString(PartitionField *field);

PG_FUNCTION_INFO_V1(reserialize_iceberg_manifest_list);
PG_FUNCTION_INFO_V1(reserialize_iceberg_manifest);
PG_FUNCTION_INFO_V1(manifest_list_path_from_table_metadata);
PG_FUNCTION_INFO_V1(manifest_paths_from_manifest_list);
PG_FUNCTION_INFO_V1(datafile_paths_from_table_metadata);
PG_FUNCTION_INFO_V1(current_manifests);
PG_FUNCTION_INFO_V1(current_manifest_entries);
PG_FUNCTION_INFO_V1(current_partition_fields);

/*
* reserialize_iceberg_manifest_list is a test function that reads the manifest
* list of an Iceberg table from input file and then reserializes it back to output file.
*/
Datum
reserialize_iceberg_manifest_list(PG_FUNCTION_ARGS)
{
	char	   *manifestListReadPath = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *manifestListWritePath = text_to_cstring(PG_GETARG_TEXT_P(1));

	List	   *manifests = ReadIcebergManifests(manifestListReadPath);

	WriteIcebergManifestList(manifestListWritePath, manifests);

	PG_RETURN_VOID();
}

/*
* reserialize_iceberg_manifest is a test function that reads the manifest
* of an Iceberg table from input file and then reserializes it back to output file.
*/
Datum
reserialize_iceberg_manifest(PG_FUNCTION_ARGS)
{
	char	   *manifestReadPath = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *manifestWritePath = text_to_cstring(PG_GETARG_TEXT_P(1));

	List	   *manifestEntries = ReadManifestEntries(manifestReadPath);

	WriteIcebergManifest(manifestWritePath, manifestEntries);

	PG_RETURN_VOID();
}

/*
 * manifest_list_path_from_table_metadata returns the manifest list uri from given the table metadata uri.
 */
Datum
manifest_list_path_from_table_metadata(PG_FUNCTION_ARGS)
{
	char	   *tableMetadataPath = text_to_cstring(PG_GETARG_TEXT_P(0));
	const char *manifestListPath = FetchCurrentSnapshotManifestListPathFromTableMetadataUri(tableMetadataPath);

	PG_RETURN_TEXT_P(cstring_to_text(manifestListPath));
}

/*
 * manifest_paths_from_manifest_list returns the manifest paths from given the manifest list uri.
 */
Datum
manifest_paths_from_manifest_list(PG_FUNCTION_ARGS)
{
	char	   *manifestListPath = text_to_cstring(PG_GETARG_TEXT_P(0));
	List	   *manifestPaths = FetchManifestPathsFromManifestListUri(manifestListPath);

	int			manifestPathDatumsLen = list_length(manifestPaths);

	ListCell   *manifestPathCell = NULL;
	Datum	   *manifestPathDatums = palloc0(sizeof(Datum) * manifestPathDatumsLen);
	int			datumIdx = 0;

	foreach(manifestPathCell, manifestPaths)
	{
		char	   *manifestPath = lfirst(manifestPathCell);

		manifestPathDatums[datumIdx] = CStringGetTextDatum(manifestPath);
		datumIdx += 1;
	}


	ArrayType  *manifestPathsArray = construct_array(manifestPathDatums,
													 manifestPathDatumsLen,
													 TEXTOID,
													 -1,
													 false,
													 TYPALIGN_INT);

	PG_RETURN_ARRAYTYPE_P(manifestPathsArray);
}

/*
 * datafile_paths_from_table_metadata returns the data file paths from given the table metadata uri.
 */
Datum
datafile_paths_from_table_metadata(PG_FUNCTION_ARGS)
{
	char	   *tableMetadataPath = text_to_cstring(PG_GETARG_TEXT_P(0));
	bool		isDelete = PG_GETARG_BOOL(1);

	List	   *dataFilePaths = FetchDataFilePathsFromTableMetadataUri(tableMetadataPath, isDelete);

	int			datumsLen = list_length(dataFilePaths);

	ListCell   *dataFilePathCell = NULL;
	Datum	   *datums = palloc0(sizeof(Datum) * datumsLen);
	int			datumIdx = 0;

	foreach(dataFilePathCell, dataFilePaths)
	{
		char	   *path = lfirst(dataFilePathCell);

		datums[datumIdx] = CStringGetTextDatum(path);
		datumIdx += 1;
	}

	ArrayType  *dataFilePathsArray = construct_array(datums,
													 datumsLen,
													 TEXTOID,
													 -1,
													 false,
													 TYPALIGN_INT);

	PG_RETURN_ARRAYTYPE_P(dataFilePathsArray);
}

/*
 * current_manifests returns all manifests from the current snapshot
 * given the table metadata uri.
 */
Datum
current_manifests(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	Datum		values[13];
	bool		nulls[13];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	char	   *tableMetadataPath = text_to_cstring(PG_GETARG_TEXT_P(0));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(tableMetadataPath);

	bool		missingOk = false;
	IcebergSnapshot *snapshot = GetCurrentSnapshot(metadata, missingOk);

	List	   *manifests = FetchManifestsFromSnapshot(snapshot, NULL);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		values[0] = CStringGetTextDatum(manifest->manifest_path);
		values[1] = Int64GetDatum(manifest->manifest_length);
		values[2] = Int32GetDatum(manifest->partition_spec_id);
		values[3] = CStringGetTextDatum(IcebergManifestContentTypeToName(manifest->content));
		values[4] = Int64GetDatum(manifest->sequence_number);
		values[5] = Int64GetDatum(manifest->min_sequence_number);
		values[6] = Int64GetDatum(manifest->added_snapshot_id);
		values[7] = Int32GetDatum(manifest->added_files_count);
		values[8] = Int32GetDatum(manifest->existing_files_count);
		values[9] = Int32GetDatum(manifest->deleted_files_count);
		values[10] = Int64GetDatum(manifest->added_rows_count);
		values[11] = Int64GetDatum(manifest->existing_rows_count);
		values[12] = Int64GetDatum(manifest->deleted_rows_count);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}


/*
 * current_manifest_entries returns all manifest entries from the current snapshot
 * given the table metadata uri.
 */
Datum
current_manifest_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	Datum		values[4];
	bool		nulls[4];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	char	   *tableMetadataPath = text_to_cstring(PG_GETARG_TEXT_P(0));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(tableMetadataPath);

	bool		missingOk = false;
	IcebergSnapshot *snapshot = GetCurrentSnapshot(metadata, missingOk);

	List	   *manifests = FetchManifestsFromSnapshot(snapshot, NULL);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		List	   *manifestEntries = FetchManifestEntriesFromManifest(manifest, NULL);

		ListCell   *manifestEntryCell = NULL;

		foreach(manifestEntryCell, manifestEntries)
		{
			IcebergManifestEntry *entry = lfirst(manifestEntryCell);

			values[0] = CStringGetTextDatum(IcebergManifestEntryStatusToName(entry->status));
			values[1] = Int64GetDatum(entry->snapshot_id);
			values[2] = Int64GetDatum(entry->sequence_number);
			values[3] = CStringGetTextDatum(entry->data_file.file_path);

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	PG_RETURN_VOID();
}


/*
 * current_partition_fields returns all partition fields for all data files
 * from the current snapshot given the table metadata uri.
 */
Datum
current_partition_fields(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	Datum		values[6];
	bool		nulls[6];

	char	   *tableMetadataPath = text_to_cstring(PG_GETARG_TEXT_P(0));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(tableMetadataPath);

	bool		missingOk = false;
	IcebergSnapshot *snapshot = GetCurrentSnapshot(metadata, missingOk);

	List	   *dataFiles = FetchDataFilesFromSnapshot(snapshot, NULL, NULL, NULL);

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);

		Partition  *partition = &dataFile->partition;

		for (int partitionFieldIndex = 0; partitionFieldIndex < partition->fields_length; partitionFieldIndex++)
		{
			PartitionField *partitionField = &partition->fields[partitionFieldIndex];

			IcebergScalarAvroType value_type = partitionField->value_type;

			const char *physicalTypeText = IcebergAvroPhysicalTypeName(value_type);

			const char *logicalTypeText = IcebergAvroLogicalTypeName(value_type);

			const char *valueText = ExternalPartitionValueToString(partitionField);

			memset(values, 0, sizeof(values));
			memset(nulls, 0, sizeof(nulls));

			values[0] = CStringGetTextDatum(dataFile->file_path);
			values[1] = Int32GetDatum(partitionField->field_id);
			values[2] = CStringGetTextDatum(partitionField->field_name);
			values[3] = CStringGetTextDatum(physicalTypeText);

			if (logicalTypeText)
			{
				values[4] = CStringGetTextDatum(logicalTypeText);
			}
			else
			{
				nulls[4] = true;
			}

			if (valueText)
			{
				values[5] = CStringGetTextDatum(valueText);
			}
			else
			{
				nulls[5] = true;
			}

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	PG_RETURN_VOID();
}

/*
 * ExternalPartitionValueToString returns the string representation of the partition field value
 * for external partitioning. Note SerializePartitionValueToPGText() is a lot more precise and
 * should be used for internal partitioning.
 */
static const char *
ExternalPartitionValueToString(PartitionField *field)
{
	if (field->value == NULL)
	{
		return NULL;
	}

	char	   *valueText = NULL;

	switch (field->value_type.physical_type)
	{
		case ICEBERG_AVRO_PHYSICAL_TYPE_INT32:
			valueText = psprintf("%" PRId32, *((int32_t *) field->value));
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_INT64:
			valueText = psprintf("%" PRId64, *((int64_t *) field->value));
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_STRING:
			valueText = psprintf("%s", (char *) field->value);
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_BINARY:
			valueText = palloc0(field->value_length * 2 + 1);
			hex_encode((const char *) field->value, field->value_length, valueText);
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT:
			valueText = psprintf("%f", *((float *) field->value));
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE:
			valueText = psprintf("%lf", *((double *) field->value));
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_BOOL:
			valueText = psprintf("%s", (*((bool *) field->value)) ? "true" : "false");
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected partition field value type")));
	}

	return valueText;
}


/*
 * FetchCurrentSnapshotManifestListPathFromTableMetadataUri returns current snapshot's
 * manifest list uri from given the table metadata uri.
 */
static const char *
FetchCurrentSnapshotManifestListPathFromTableMetadataUri(const char *tableMetadataPath)
{
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(tableMetadataPath);

	List	   *snapshots = FetchSnapshotsFromTableMetadata(metadata, IsCurrentSnapshot);

	if (snapshots == NULL || list_length(snapshots) == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("latest snapshot not found in table metadata")));
	}

	IcebergSnapshot *currentSnapshot = linitial(snapshots);

	return currentSnapshot->manifest_list;
}

/*
 * FetchDataFilePathsFromTableMetadataUri fetches data file paths, which are filtered by predicates,
 * from given table metadata uri.
 */
static List *
FetchDataFilePathsFromTableMetadataUri(const char *tableMetadataPath, bool isDelete)
{
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(tableMetadataPath);

	List	   *dataFilePaths = NIL;
	List	   *deleteFilePaths = NIL;

	FetchAllDataAndDeleteFilePathsFromCurrentSnapshot(metadata, &dataFilePaths, &deleteFilePaths);

	if (isDelete)
	{
		return deleteFilePaths;
	}
	else
	{
		return dataFilePaths;
	}
}

/*
 * FetchManifestPathsFromManifestListUri fetches manifest paths from manifest list path.
 */
static List *
FetchManifestPathsFromManifestListUri(const char *manifestListPath)
{
	List	   *manifestPaths = NIL;

	List	   *manifests = ReadIcebergManifests(manifestListPath);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		manifestPaths = lappend(manifestPaths, (char *) manifest->manifest_path);
	}

	return manifestPaths;
}
