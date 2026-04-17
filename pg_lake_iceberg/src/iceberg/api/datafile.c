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
#include "pg_lake/iceberg/api/datafile.h"
#include "pg_lake/iceberg/api/snapshot.h"


/*
 * FetchDataFilesFromManifestEntry fetches data files, which are filtered by predicate,
 * from given manifest entry.
 */
List *
FetchDataFilesFromManifestEntry(IcebergManifestEntry *manifestEntry, DataFilePredicateFn dataFilePredicateFn)
{
	if (manifestEntry == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("manifest entry is NULL")));
	}

	List	   *dataFiles = NIL;

	if (dataFilePredicateFn == NULL || dataFilePredicateFn(&manifestEntry->data_file))
	{
		dataFiles = lappend(dataFiles, &manifestEntry->data_file);
	}

	return dataFiles;
}

/*
 * FetchDataFilesFromSnapshot fetches date files, which are filtered by predicates,
 * from given snapshot.
 */
List *
FetchDataFilesFromSnapshot(IcebergSnapshot *snapshot, ManifestPredicateFn manifestPredicateFn, ManifestEntryPredicateFn manifestEntryPredicateFn, DataFilePredicateFn dataFilePredicateFn)
{
	List	   *manifests = FetchManifestsFromSnapshot(snapshot, manifestPredicateFn);

	List	   *dataFiles = NIL;

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);
		bool		pathOnly = false;
		List	   *manifestDataFiles = FetchDataFilesFromManifest(manifest, pathOnly, manifestEntryPredicateFn, dataFilePredicateFn);

		dataFiles = list_concat(dataFiles, manifestDataFiles);
	}

	return dataFiles;
}

/*
 * FetchDataFilesFromSnapshot fetches date files, which are filtered by predicates,
 * from given snapshot.
 */
List *
FetchDataFilePathsFromSnapshot(IcebergSnapshot *snapshot, ManifestPredicateFn manifestPredicateFn, ManifestEntryPredicateFn manifestEntryPredicateFn, DataFilePredicateFn dataFilePredicateFn)
{
	List	   *manifests = FetchManifestsFromSnapshot(snapshot, manifestPredicateFn);

	List	   *dataFiles = NIL;

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);
		bool		pathOnly = true;
		List	   *manifestDataFiles = FetchDataFilesFromManifest(manifest, pathOnly, manifestEntryPredicateFn, dataFilePredicateFn);

		dataFiles = list_concat(dataFiles, manifestDataFiles);
	}

	return dataFiles;
}

/*
 * FetchAllDataAndDeleteFilesFromCurrentSnapshot fetches all data and delete files
 * from the current snapshot of given table metadata.
 */
void
FetchAllDataAndDeleteFilesFromCurrentSnapshot(IcebergTableMetadata *metadata, List **dataFiles, List **deleteFiles)
{
	if (metadata == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table metadata is NULL")));
	}

	IcebergSnapshot *snapshot = GetCurrentSnapshot(metadata, true);

	*dataFiles = FetchDataFilesFromSnapshot(snapshot, IsManifestOfFileContentAdd, IsManifestEntryStatusScannable, NULL);
	*deleteFiles = FetchDataFilesFromSnapshot(snapshot, IsManifestOfFileContentDeletes, IsManifestEntryStatusScannable, NULL);
}

/*
 * FetchAllDataAndDeleteFilePathsFromCurrentSnapshot fetches all data and delete
 * file paths from the current snapshot of given table metadata.
 */
void
FetchAllDataAndDeleteFilePathsFromCurrentSnapshot(IcebergTableMetadata *metadata, List **dataFilePaths, List **deleteFilePaths)
{
	List	   *dataFiles = NIL;
	List	   *deleteFiles = NIL;

	FetchAllDataAndDeleteFilesFromCurrentSnapshot(metadata, &dataFiles, &deleteFiles);

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);

		*dataFilePaths = lappend(*dataFilePaths, pstrdup(dataFile->file_path));
	}

	ListCell   *deleteFileCell = NULL;

	foreach(deleteFileCell, deleteFiles)
	{
		DataFile   *deleteFile = lfirst(deleteFileCell);

		*deleteFilePaths = lappend(*deleteFilePaths, pstrdup(deleteFile->file_path));
	}
}

/*
 * FetchDataFilesFromManifest fetches data files, which are filtered by predicates,
 * from given manifest.
 */
List *
FetchDataFilesFromManifest(IcebergManifest *manifest, bool pathOnly, ManifestEntryPredicateFn manifestEntryPredicateFn, DataFilePredicateFn dataFilePredicateFn)
{
	if (manifest == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("manifest is NULL")));
	}

	List	   *manifestEntries = FetchManifestEntriesFromManifest(manifest, manifestEntryPredicateFn);

	List	   *dataFiles = NIL;

	ListCell   *manifestEntryCell = NULL;

	foreach(manifestEntryCell, manifestEntries)
	{
		IcebergManifestEntry *manifestEntry = lfirst(manifestEntryCell);

		if (dataFilePredicateFn == NULL || dataFilePredicateFn(&manifestEntry->data_file))
		{
			if (pathOnly)
			{
				dataFiles = lappend(dataFiles, (char *) manifestEntry->data_file.file_path);
			}
			else
			{
				dataFiles = lappend(dataFiles, &manifestEntry->data_file);
			}
		}
	}

	return dataFiles;
}
