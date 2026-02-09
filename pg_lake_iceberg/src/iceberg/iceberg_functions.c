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

#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/util/s3_reader_utils.h"

PG_FUNCTION_INFO_V1(iceberg_metadata);
PG_FUNCTION_INFO_V1(iceberg_files);
PG_FUNCTION_INFO_V1(iceberg_snapshots);

/*
 * iceberg_metadata returns the metadata for the iceberg table
 * in JSON format.
 */
Datum
iceberg_metadata(PG_FUNCTION_ARGS)
{
	char	   *metadataUri = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (!IsSupportedURL(metadataUri))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_iceberg: only s3://, gs://, az://, azure://, and abfss:// are supported")));
	}

	CheckURLReadAccess();

	char	   *tableMetadataText = GetTextFromURI(metadataUri);
	Datum		jsonbDatum = DirectFunctionCall1(jsonb_in, PointerGetDatum(tableMetadataText));

	return jsonbDatum;
}

/*
 * iceberg_files returns data files for the iceberg table.
 */
Datum
iceberg_files(PG_FUNCTION_ARGS)
{
	char	   *metadataUri = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (!IsSupportedURL(metadataUri))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_iceberg: only s3://, gs://, az://, azure://, and abfss:// are supported")));
	}

	CheckURLReadAccess();

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	Datum		values[7];
	bool		nulls[7];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataUri);

	IcebergSnapshot *snapshot = GetCurrentSnapshot(metadata, false);

	List	   *manifests = FetchManifestsFromSnapshot(snapshot, NULL);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = (IcebergManifest *) lfirst(manifestCell);

		List	   *manifestEntries = FetchManifestEntriesFromManifest(manifest, IsManifestEntryStatusScannable);

		ListCell   *manifestEntryCell = NULL;

		foreach(manifestEntryCell, manifestEntries)
		{
			IcebergManifestEntry *manifestEntry = (IcebergManifestEntry *) lfirst(manifestEntryCell);

			List	   *dataFiles = FetchDataFilesFromManifestEntry(manifestEntry, NULL);

			ListCell   *dataFileCell = NULL;

			foreach(dataFileCell, dataFiles)
			{
				DataFile   *dataFile = (DataFile *) lfirst(dataFileCell);

				/* todo: fill with actual spec_id when we support partitioning */
				int64_t		spec_id = 0;

				values[0] = CStringGetTextDatum(manifest->manifest_path);
				values[1] = CStringGetTextDatum(IcebergDataFileContentTypeToName(dataFile->content));
				values[2] = CStringGetTextDatum(dataFile->file_path);
				values[3] = CStringGetTextDatum(dataFile->file_format);
				values[4] = Int64GetDatum(spec_id);
				values[5] = Int64GetDatum(dataFile->record_count);
				values[6] = Int64GetDatum(dataFile->file_size_in_bytes);

				tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
			}
		}
	}

	PG_RETURN_VOID();
}

/*
 * iceberg_snapshots returns the snapshots for the iceberg table.
 */
Datum
iceberg_snapshots(PG_FUNCTION_ARGS)
{
	char	   *metadataUri = text_to_cstring(PG_GETARG_TEXT_P(0));

	if (!IsSupportedURL(metadataUri))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake_iceberg: only s3://, gs://, az://, azure://, and abfss:// are supported")));
	}

	CheckURLReadAccess();

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	Datum		values[4];
	bool		nulls[4];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataUri);

	List	   *snapshots = FetchSnapshotsFromTableMetadata(metadata, NULL);

	ListCell   *snapshotCell = NULL;

	foreach(snapshotCell, snapshots)
	{
		IcebergSnapshot *snapshot = (IcebergSnapshot *) lfirst(snapshotCell);

		Timestamp	postgresTimestamp =
			IcebergTimestampMsToPostgresTimestamp(snapshot->timestamp_ms);

		values[0] = Int64GetDatum(snapshot->sequence_number);
		values[1] = Int64GetDatum(snapshot->snapshot_id);
		values[2] = TimestampGetDatum(postgresTimestamp);
		values[3] = CStringGetTextDatum(snapshot->manifest_list);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}
