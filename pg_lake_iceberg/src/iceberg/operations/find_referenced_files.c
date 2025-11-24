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
* Iceberg is a dynamic system that is constantly adding/removing
* data files and metadata files.
*
* The logic in this file is to find any all files that are
* referenced in the latest metadata.json file.
*/
#include "postgres.h"
#include "miscadmin.h"
#include "fmgr.h"
#include "funcapi.h"

#include "nodes/pg_list.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "common/hashfn.h"
#include "utils/tuplestore.h"

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/operations/find_referenced_files.h"
#include "pg_lake/util/array_utils.h"
#include "pg_lake/util/s3_reader_utils.h"

typedef struct FileHashEntry
{
	char		path[MAX_S3_PATH_LENGTH];
}			FileHashEntry;

PG_FUNCTION_INFO_V1(find_all_referenced_files);
PG_FUNCTION_INFO_V1(find_unreferenced_files);

PG_FUNCTION_INFO_V1(find_all_referenced_files_via_snapshot_ids);
PG_FUNCTION_INFO_V1(find_unreferenced_files_via_snapshot_ids);


static void IcebergMetadataAddAllReferencedFiles(char *metadataPath, HTAB *fileHash);
static void IcebergSnapshotAddAllReferencedFiles(IcebergSnapshot * snapshot, HTAB *fileHash);
static List *FindUnreferencedFiles(List *prevMetadataList, char *currentMetadataPath);
static List *FindUnreferencedFilesAmongHTABs(HTAB *prevReferencedFileHash, HTAB *currentReferencedFileHash);
static IcebergSnapshot * GetIcebergSnapshotsViaSnapshotIdList(IcebergTableMetadata * metadata, List *snapshotIdList);

/*
* find_all_referenced_files reads the metadata file and returns a list of
* all files that are referenced in the metadata file.
*/
Datum
find_all_referenced_files(PG_FUNCTION_ARGS)
{
	char	   *metadataPath = text_to_cstring(PG_GETARG_TEXT_PP(0));

	HTAB	   *fileHash = CreateFilesHash();

	IcebergMetadataAddAllReferencedFiles(metadataPath, fileHash);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* convert the files in the hash to tuplestore */
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, fileHash);

	FileHashEntry *entry = NULL;

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		Datum		values[1];
		bool		nulls[1];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(entry->path);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}

/*
* find_all_referenced_files_via_snapshot_id reads the metadata file and returns a list of
* all files that are referenced in the metadata file.
*/
Datum
find_all_referenced_files_via_snapshot_ids(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	ArrayType  *snapshotIds = PG_GETARG_ARRAYTYPE_P(1);

	HTAB	   *fileHash = CreateFilesHash();

	char	   *currentMetadataPath = GetIcebergMetadataLocation(relationId, false);
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(currentMetadataPath);

	ListCell   *snapshotIdCell = NULL;
	List	   *snapshotIdList = Int64ArrayToList(snapshotIds);

	foreach(snapshotIdCell, snapshotIdList)
	{
		int64	   *snapshotId = (int64 *) lfirst(snapshotIdCell);
		IcebergSnapshot *snapshot = GetIcebergSnapshotViaId(metadata, *snapshotId);

		IcebergSnapshotAddAllReferencedFiles(snapshot, fileHash);
	}

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* convert the files in the hash to tuplestore */
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, fileHash);

	FileHashEntry *entry = NULL;

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		Datum		values[1];
		bool		nulls[1];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(entry->path);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}



/*
* find_unreferenced_files finds all the files that are not referenced in the
* current metadata file but are referenced in the previous metadata files.
* The function also returns these files as a set of rows.
*/
Datum
find_unreferenced_files(PG_FUNCTION_ARGS)
{
	ArrayType  *prevMetadataPaths = PG_GETARG_ARRAYTYPE_P(0);
	char	   *currentMetadataPath = text_to_cstring(PG_GETARG_TEXT_PP(1));

	List	   *prevMetadataList = StringArrayToList(prevMetadataPaths);

	List	   *unreferencedFiles = FindUnreferencedFiles(prevMetadataList, currentMetadataPath);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* convert the list of files to tuples in a tuple store */
	ListCell   *fileCell = NULL;

	foreach(fileCell, unreferencedFiles)
	{
		char	   *file = lfirst(fileCell);

		Datum		values[1];
		bool		nulls[1];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(file);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}

Datum
find_unreferenced_files_via_snapshot_ids(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	ArrayType  *prevSnapshotIds = PG_GETARG_ARRAYTYPE_P(1);
	ArrayType  *currentSnapshotIds = PG_GETARG_ARRAYTYPE_P(2);

	List	   *prevSnapshotIdList = Int64ArrayToList(prevSnapshotIds);
	List	   *currentSnapshotIdList = Int64ArrayToList(currentSnapshotIds);

	char	   *currentMetadataPath = GetIcebergMetadataLocation(relationId, false);
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(currentMetadataPath);

	IcebergSnapshot *prevSnapshots = GetIcebergSnapshotsViaSnapshotIdList(metadata, prevSnapshotIdList);
	IcebergSnapshot *currentSnapshots = GetIcebergSnapshotsViaSnapshotIdList(metadata, currentSnapshotIdList);

	List	   *unreferencedFiles = FindUnreferencedFilesForSnapshots(prevSnapshots, list_length(prevSnapshotIdList),
																	  currentSnapshots, list_length(currentSnapshotIdList));

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* convert the list of files to tuples in a tuple store */
	ListCell   *fileCell = NULL;

	foreach(fileCell, unreferencedFiles)
	{
		char	   *file = lfirst(fileCell);

		Datum		values[1];
		bool		nulls[1];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(file);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();

}

static IcebergSnapshot *
GetIcebergSnapshotsViaSnapshotIdList(IcebergTableMetadata * metadata, List *snapshotIdList)
{
	int			snapshotCount = list_length(snapshotIdList);
	IcebergSnapshot *snapshots = palloc0(sizeof(IcebergSnapshot) * snapshotCount);

	ListCell   *snapshotIdCell = NULL;
	int			snapshotIndex = 0;

	foreach(snapshotIdCell, snapshotIdList)
	{
		int64	   *snapshotId = (int64 *) lfirst(snapshotIdCell);
		IcebergSnapshot *snapshot = GetIcebergSnapshotViaId(metadata, *snapshotId);

		snapshots[snapshotIndex] = *snapshot;
		snapshotIndex++;
	}

	return snapshots;
}


/*
* FindUnreferencedFiles finds all the files that are not referenced in the
* current metadata file but are referenced in the previous metadata files.
*
* The algorithm is as follows:
* 1. Create a hash table for the files that are referenced in the previous
*    metadata files.
* 2. Create another hash table for the files that are referenced by the current
*    metadata path.
* 3. Iterate over the hash table of the previous metadata files and check
*    if the file is in the hash table of the current metadata files. If not,
*    add the file to the list of unreferenced files.
*/
static List *
FindUnreferencedFiles(List *prevMetadataList, char *currentMetadataPath)
{
	HTAB	   *prevReferencedFileHash = CreateFilesHash();
	ListCell   *metadataPathCell = NULL;

	foreach(metadataPathCell, prevMetadataList)
	{
		char	   *prevMetadataPath = lfirst(metadataPathCell);

		IcebergMetadataAddAllReferencedFiles(prevMetadataPath, prevReferencedFileHash);
	}

	HTAB	   *currentReferencedFileHash = CreateFilesHash();

	IcebergMetadataAddAllReferencedFiles(currentMetadataPath, currentReferencedFileHash);

	List	   *unreferencedFiles = FindUnreferencedFilesAmongHTABs(prevReferencedFileHash, currentReferencedFileHash);

	return unreferencedFiles;
}


/*
* Similar to FindUnreferencedFiles, but this function takes two lists of
* snapshot ids instead of metadata paths.
*/
List *
FindUnreferencedFilesForSnapshots(IcebergSnapshot * prevSnapshots, int prevSnapshotCount,
								  IcebergSnapshot * currentSnapshots, int currentSnapshotCount)
{
	HTAB	   *prevReferencedFileHash = CreateFilesHash();

	int			snapshotIndex = 0;

	for (snapshotIndex = 0; snapshotIndex < prevSnapshotCount; snapshotIndex++)
	{
		IcebergSnapshot *snapshot = &prevSnapshots[snapshotIndex];

		IcebergSnapshotAddAllReferencedFiles(snapshot, prevReferencedFileHash);
	}

	HTAB	   *currentReferencedFileHash = CreateFilesHash();

	for (snapshotIndex = 0; snapshotIndex < currentSnapshotCount; snapshotIndex++)
	{
		IcebergSnapshot *snapshot = &currentSnapshots[snapshotIndex];

		IcebergSnapshotAddAllReferencedFiles(snapshot, currentReferencedFileHash);
	}

	return FindUnreferencedFilesAmongHTABs(prevReferencedFileHash, currentReferencedFileHash);
}


/*
* FindUnreferencedFilesAmongHTABs finds all the files that are not referenced in the
* current hash table but are referenced in the previous hash table.
*/
static List *
FindUnreferencedFilesAmongHTABs(HTAB *prevReferencedFileHash, HTAB *currentReferencedFileHash)
{
	List	   *unreferencedFiles = NIL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, prevReferencedFileHash);
	FileHashEntry *entry = NULL;

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		bool		found = false;

		hash_search(currentReferencedFileHash, entry->path, HASH_FIND, &found);

		if (!found)
		{
			/*
			 * We had this file in the previous metadata paths but not in the
			 * current metadata paths. So, the path is not referenced anymore.
			 */
			unreferencedFiles = lappend(unreferencedFiles, entry->path);
		}
	}

	return unreferencedFiles;
}


/*
* IcebergFindAllReferencedFiles reads the metadata file and returns a list of
* all files that are referenced in the metadata file.
*/
List *
IcebergFindAllReferencedFiles(char *metadataPath)
{
	HTAB	   *fileHash = CreateFilesHash();

	IcebergMetadataAddAllReferencedFiles(metadataPath, fileHash);

	List	   *referencedFiles = NIL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, fileHash);
	FileHashEntry *entry = NULL;

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		referencedFiles = lappend(referencedFiles, pstrdup(entry->path));
	}

	return referencedFiles;
}


/*
* IcebergMetadataAddAllReferencedFiles reads the metadata file and
* returns a list of files that are referenced in the metadata file.
*/
static void
IcebergMetadataAddAllReferencedFiles(char *metadataPath, HTAB *fileHash)
{
	/* read the metadata file */
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataPath);
	bool		fileAlreadyExists PG_USED_FOR_ASSERTS_ONLY = false;

	/* add the metadata file itself */
	fileAlreadyExists = AppendFileToHash(metadataPath, fileHash);

	/* we should never add the same metadata.json */
	Assert(!fileAlreadyExists);

	/* add all the manifest_list files */
	int			snapshotIndex = 0;

	for (snapshotIndex = 0; snapshotIndex < metadata->snapshots_length; snapshotIndex++)
	{
		IcebergSnapshot *snapshot = &metadata->snapshots[snapshotIndex];

		IcebergSnapshotAddAllReferencedFiles(snapshot, fileHash);
	}
}


/*
* IcebergSnapshotAddAllReferencedFiles adds all the files that are referenced
* in the snapshot to the hash table.
*/
static void
IcebergSnapshotAddAllReferencedFiles(IcebergSnapshot * snapshot, HTAB *fileHash)
{
	bool		fileAlreadyExists = AppendFileToHash(snapshot->manifest_list, fileHash);

	if (fileAlreadyExists)
	{
		/*
		 * We already added the manifest_list file, and manifest_list is
		 * immutable. So we can skip the rest of the snapshot.
		 */
		return;
	}

	/* avoid keeping avro contents allocated */
	MemoryContext manifestDataFileFetchContext =
		AllocSetContextCreate(CurrentMemoryContext,
							  "FetchDataFilesFromManifest for IcebergSnapshotAddAllReferencedFiles",
							  ALLOCSET_DEFAULT_SIZES);

	/* add all the manifest files */
	List	   *manifests = FetchManifestsFromSnapshot(snapshot, NULL);
	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		fileAlreadyExists = AppendFileToHash(manifest->manifest_path, fileHash);
		if (fileAlreadyExists)
		{
			/*
			 * We already added the manifest_path file, and manifest_path is
			 * immutable. So we can skip the rest of the snapshot.
			 */
			continue;
		}

		MemoryContext oldContext = MemoryContextSwitchTo(manifestDataFileFetchContext);

		List	   *manifestDataFiles = FetchDataFilesFromManifest(manifest, NULL, IsManifestEntryStatusScannable, NULL);
		ListCell   *dataFileCell = NULL;

		foreach(dataFileCell, manifestDataFiles)
		{
			DataFile   *dataFile = lfirst(dataFileCell);

			AppendFileToHash(MemoryContextStrdup(oldContext, dataFile->file_path), fileHash);
		}

		MemoryContextSwitchTo(oldContext);
		MemoryContextReset(manifestDataFileFetchContext);
	}

	MemoryContextDelete(manifestDataFileFetchContext);
}

/*
* CreateFilesHash creates a hash table that is suitable for storing
* file paths.
*/
HTAB *
CreateFilesHash(void)
{
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = MAX_S3_PATH_LENGTH;
	hashCtl.entrysize = sizeof(FileHashEntry);
	hashCtl.hcxt = CurrentMemoryContext;

	HTAB	   *referencedFilesHash = hash_create("Referenced Files Hash",
												  1024,
												  &hashCtl,
												  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

	return referencedFilesHash;
}


/*
* AppendFileToHash appends the file to the hash table, and
* returns true if the file already exists in the hash table.
*/
bool
AppendFileToHash(const char *path, HTAB *referencedFilesHash)
{
	bool		found = false;

	hash_search(referencedFilesHash, path, HASH_ENTER, &found);

	return found;
}
