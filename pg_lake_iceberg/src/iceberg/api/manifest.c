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

#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/compare_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/util/url_encode.h"

#include "utils/lsyscache.h"

static void SetManifestFileAndRowCounts(IcebergManifest * manifest, List *manifestEntries);
static void SetManifestPartitionSummary(IcebergManifest * manifest, List *manifestEntries, List *transforms);
static FieldSummary * GetPartitionFieldSummaryFromDataFiles(List *dataFiles, int32_t partitionFieldIndex,
															Field * resultField, PGType resultPgType);


/*
 * IsManifestOfFileContentAdd checks if the given manifest is of type
 * ICEBERG_MANIFEST_FILE_CONTENT_DATA.
 */
bool
IsManifestOfFileContentAdd(IcebergManifest * manifest)
{
	return manifest->content == ICEBERG_MANIFEST_FILE_CONTENT_DATA;
}

/*
 * IsManifestOfFileContentDeletes checks if the given manifest is of type
 * ICEBERG_MANIFEST_FILE_CONTENT_DELETES.
 */
bool
IsManifestOfFileContentDeletes(IcebergManifest * manifest)
{
	return manifest->content == ICEBERG_MANIFEST_FILE_CONTENT_DELETES;
}

/*
 * FetchManifestsFromSnapshot gets manifests, which are filtered by the callback,
 * from given snapshot.
 */
List *
FetchManifestsFromSnapshot(IcebergSnapshot * snapshot, ManifestPredicateFn manifestPredicateFn)
{
	if (snapshot == NULL)
	{
		return NIL;
	}

	List	   *manifests = ReadIcebergManifests(snapshot->manifest_list);

	List	   *filteredManifests = NIL;

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		if (manifestPredicateFn == NULL || manifestPredicateFn(manifest))
		{
			filteredManifests = lappend(filteredManifests, manifest);
		}
	}

	return filteredManifests;
}

/*
 * UploadIcebergManifestToURI writes the manifest to a file and uploads it to given uri.
 * It returns the size of the manifest file.
 */
int64_t
UploadIcebergManifestToURI(List *manifestEntries, char *manifestURI)
{
	char	   *localManifestPath = GenerateTempFileName(PG_LAKE_ICEBERG, true);

	WriteIcebergManifest(localManifestPath, manifestEntries);

	/*
	 * Manifest files will get auto-deleted on commit unless
	 * DeleteInProgressManifests is called. The reason is that manifest files
	 * generated at commit time might still get replaced via manifest merge
	 * before commit.
	 */
	bool		autoDeleteRecord = false;

	ScheduleFileCopyToS3WithCleanup(localManifestPath, manifestURI, autoDeleteRecord);

	int64		manifestSize = GetLocalFileSize(localManifestPath);

	return manifestSize;
}


/*
 * GenerateRemoteManifestPath generates a remote manifest path from given arguments
 */
char *
GenerateRemoteManifestPath(const char *location, const char *snapshotUUID, int manifestIndex, char *queryArguments)
{
	StringInfo	remoteFilePath = makeStringInfo();

	appendStringInfo(remoteFilePath, "%s/metadata/%s-m%d.avro%s",
					 location, snapshotUUID, manifestIndex, queryArguments);
	return remoteFilePath->data;
}

/*
* CreateNewIcebergManifest creates a new Iceberg manifest with the given parameters.
*/
IcebergManifest *
CreateNewIcebergManifest(IcebergSnapshot * snapshot,
						 int32_t partitionSpecId,
						 List *allTransforms,
						 int64 manifestFileSize,
						 IcebergManifestContentType contentType,
						 char *manifestPath,
						 List *manifestEntries)
{
	IcebergManifest *newManifest = palloc0(sizeof(IcebergManifest));

	newManifest->manifest_length = manifestFileSize;
	newManifest->content = contentType;

	newManifest->sequence_number = snapshot->sequence_number;
	newManifest->min_sequence_number = 0;

	newManifest->partition_spec_id = partitionSpecId;
	newManifest->added_snapshot_id = snapshot->snapshot_id;
	newManifest->manifest_path = manifestPath;

	/* finally set the file and row counts */
	SetManifestFileAndRowCounts(newManifest, manifestEntries);

	SetManifestPartitionSummary(newManifest, manifestEntries, allTransforms);

	return newManifest;
}

/*
 * SetManifestFileAndRowCounts calculates the number of added, existing, and deleted
 * files and rows in the manifest entry list and fills manifest's related fields.
 */
static void
SetManifestFileAndRowCounts(IcebergManifest * manifest, List *manifestEntries)
{
	ListCell   *manifestEntryCell = NULL;

	foreach(manifestEntryCell, manifestEntries)
	{
		IcebergManifestEntry *manifestEntry = lfirst(manifestEntryCell);

		if (manifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_ADDED)
		{
			manifest->added_files_count += 1;
			manifest->added_rows_count += manifestEntry->data_file.record_count;
		}
		else if (manifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_EXISTING)
		{
			manifest->existing_files_count += 1;
			manifest->existing_rows_count += manifestEntry->data_file.record_count;
		}
		else if (manifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_DELETED)
		{
			manifest->deleted_files_count += 1;
			manifest->deleted_rows_count += manifestEntry->data_file.record_count;
		}
		else
		{
			pg_unreachable();
		}
	}
}


/*
 * SetManifestPartitionSummary sets the partition summary for given manifest.
 * The partition summary is a list of FieldSummary, which contains the min
 * and max values for each partition field in all data files of the manifest.
 */
static void
SetManifestPartitionSummary(IcebergManifest * manifest, List *manifestEntries, List *transforms)
{
	List	   *dataFiles = NIL;

	ListCell   *manifestEntryCell = NULL;

	foreach(manifestEntryCell, manifestEntries)
	{
		IcebergManifestEntry *manifestEntry = lfirst(manifestEntryCell);

		dataFiles = lappend(dataFiles, &manifestEntry->data_file);
	}

	if (dataFiles == NIL)
	{
		manifest->partitions = NULL;
		manifest->partitions_length = 0;
		return;
	}

	DataFile   *firstDataFile = linitial(dataFiles);

	Partition  *firstPartition = &firstDataFile->partition;

	/*
	 * all data files of the same manifest has the same partition fields with
	 * possibly different values for each field.
	 */
	int			nPartitionFields = firstPartition->fields_length;

	if (firstPartition == NULL || nPartitionFields == 0)
	{
		manifest->partitions = NULL;
		manifest->partitions_length = 0;
		return;
	}

	if (transforms == NIL)
	{
		/* we expect transforms to be passed when we have partition set */
		ereport(ERROR, (errmsg("No partition transforms provided for manifest partition summary")));
	}

	/*
	 * The partition summary is a list of FieldSummary, which contains the min
	 * and max values for each partition field in all data files of the
	 * manifest.
	 */
	FieldSummary *partitionSummaries = palloc0(sizeof(FieldSummary) * nPartitionFields);

	for (int partitionFieldIdx = 0; partitionFieldIdx < nPartitionFields; partitionFieldIdx++)
	{
		int32_t		partitionFieldId = firstDataFile->partition.fields[partitionFieldIdx].field_id;

		bool		errorIfMissing = true;

		IcebergPartitionTransform *partitionTransform =
			FindPartitionTransformById(transforms, partitionFieldId, errorIfMissing);

		PGType		resultPgType = partitionTransform->resultPgType;
		Field	   *resultField = PostgresTypeToIcebergField(resultPgType, false, NULL);

		FieldSummary *partitionSummary =
			GetPartitionFieldSummaryFromDataFiles(dataFiles, partitionFieldIdx,
												  resultField, resultPgType);

		partitionSummaries[partitionFieldIdx] = *partitionSummary;
	}

	manifest->partitions = partitionSummaries;
	manifest->partitions_length = nPartitionFields;
}


/*
 * GetPartitionFieldSummaryFromDataFiles retrieves the partition field summary
 * from the data files. It calculates the min and max values for the partition
 * field across all data files.
 */
static FieldSummary *
GetPartitionFieldSummaryFromDataFiles(List *dataFiles, int32_t partitionFieldIndex,
									  Field * resultField, PGType resultPgType)
{
	FieldSummary *partitionSummary = palloc0(sizeof(FieldSummary));

	/*
	 * todo: we do not set contains_nan and contains_null yet but let them set
	 * true to not skip files by mistake
	 */
	partitionSummary->contains_nan = true;
	partitionSummary->contains_null = true;

	/* find min of all partition values for the fields in all data files */
	Datum		minLowerBoundDatum = {0};
	bool		minSet = false;

	/* find max of all partition values for the fields in all data files */
	Datum		maxUpperBoundDatum = {0};
	bool		maxSet = false;

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);

		Partition  *partition = &dataFile->partition;

		PartitionField *partitionField = &partition->fields[partitionFieldIndex];

		if (partitionField->value == NULL)
			continue;

		Datum		partitionFieldDatum = PGIcebergBinaryDeserialize(partitionField->value, partitionField->value_length,
																	 resultField, resultPgType);

		if (!minSet || IsDatumLessThan(partitionFieldDatum, minLowerBoundDatum, resultPgType))
		{
			minLowerBoundDatum = partitionFieldDatum;
			minSet = true;
		}

		if (!maxSet || IsDatumGreaterThan(partitionFieldDatum, maxUpperBoundDatum, resultPgType))
		{
			maxUpperBoundDatum = partitionFieldDatum;
			maxSet = true;
		}
	}

	Assert((!minSet && !maxSet) || (minSet && maxSet));

	if (minSet)
	{
		partitionSummary->lower_bound = PGIcebergBinarySerializeBoundValue(minLowerBoundDatum,
																		   resultField, resultPgType,
																		   &partitionSummary->lower_bound_length);
	}
	else
	{
		partitionSummary->lower_bound = NULL;
		partitionSummary->lower_bound_length = 0;
	}

	if (maxSet)
	{
		partitionSummary->upper_bound = PGIcebergBinarySerializeBoundValue(maxUpperBoundDatum,
																		   resultField, resultPgType,
																		   &partitionSummary->upper_bound_length);
	}
	else
	{
		partitionSummary->upper_bound = NULL;
		partitionSummary->upper_bound_length = 0;
	}

	return partitionSummary;
}
