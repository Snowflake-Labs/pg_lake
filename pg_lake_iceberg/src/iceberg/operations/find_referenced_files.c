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
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/storage/storage_credentials.h"
#include "pg_lake/util/array_utils.h"
#include "pg_lake/util/injection_points.h"
#include "pg_lake/util/path_hash.h"
#include "pg_lake/util/s3_reader_utils.h"

PG_FUNCTION_INFO_V1(find_all_referenced_files);
PG_FUNCTION_INFO_V1(find_all_referenced_files_best_effort);
PG_FUNCTION_INFO_V1(find_unreferenced_files);

PG_FUNCTION_INFO_V1(find_all_referenced_files_via_snapshot_ids);
PG_FUNCTION_INFO_V1(find_unreferenced_files_via_snapshot_ids);


static Datum ReturnReferencedFilesOfMetadata(FunctionCallInfo fcinfo, char *metadataPath,
											 bool bestEffort);
static void IcebergMetadataAddAllReferencedFiles(char *metadataPath, HTAB *fileHash,
												 bool bestEffort);
static void IcebergSnapshotAddAllReferencedFiles(IcebergSnapshot * snapshot, HTAB *fileHash,
												 bool bestEffort);
static List *FindUnreferencedFiles(List *prevMetadataList, char *currentMetadataPath);
static IcebergSnapshot * GetIcebergSnapshotsViaSnapshotIdList(IcebergTableMetadata * metadata, List *snapshotIdList);

/*
* find_all_referenced_files reads the metadata file and returns a list of
* all files that are referenced in the metadata file.
*/
Datum
find_all_referenced_files(PG_FUNCTION_ARGS)
{
	char	   *metadataPath = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/*
	 * Test hook: the deferred-drop VACUUM path invokes this SQL entrypoint
	 * over SPI to resolve a queued metadata.json. A distinct name lets a test
	 * force resolution to fail (and assert the row is retried) without also
	 * arming the eager enumeration below.
	 */
	INJECTION_POINT_COMPAT("iceberg-find-referenced-files-udf");

	bool		bestEffort = false;

	return ReturnReferencedFilesOfMetadata(fcinfo, metadataPath, bestEffort);
}


/*
* find_all_referenced_files_best_effort is find_all_referenced_files for a
* metadata.json that can no longer be walked in full because something it
* references has gone missing out of band. The strict walk throws in that case,
* which reclaims nothing at all; this one reports what it can still reach and
* names what it had to skip.
*
* A pointer is skipped only when object storage says it is not there. Its parent
* was just read successfully, so the store is reachable and the credentials
* work, and that is what makes a negative existence probe mean "gone" rather
* than "cannot tell": RemoteFileExists answers false for any failure, a denied
* request included. The metadata.json itself is read strictly -- if that cannot
* be read there is nothing to be partial about.
*
* Files reachable only through a skipped pointer stay in object storage.
*/
Datum
find_all_referenced_files_best_effort(PG_FUNCTION_ARGS)
{
	char	   *metadataPath = text_to_cstring(PG_GETARG_TEXT_PP(0));

	/* an unreachable store fails either walk, so both share the point */
	INJECTION_POINT_COMPAT("iceberg-find-referenced-files-udf");

	bool		bestEffort = true;

	return ReturnReferencedFilesOfMetadata(fcinfo, metadataPath, bestEffort);
}


/*
* ReturnReferencedFilesOfMetadata walks the metadata file and returns the files
* it references as a set of rows.
*/
static Datum
ReturnReferencedFilesOfMetadata(FunctionCallInfo fcinfo, char *metadataPath,
								bool bestEffort)
{
	HTAB	   *fileHash = CreateFilesHash();

	IcebergMetadataAddAllReferencedFiles(metadataPath, fileHash, bestEffort);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* convert the files in the hash to tuplestore */
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, fileHash);

	PathHashEntry *entry = NULL;

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

	/*
	 * Reading the metadata and manifests below goes through object storage,
	 * so a REST-catalog table that is only reachable with vended credentials
	 * needs them resolved first.  No-op for every other kind of table.
	 */
	EnsureStorageCredentialsForRelation(relationId);

	HTAB	   *fileHash = CreateFilesHash();

	char	   *currentMetadataPath = GetIcebergMetadataLocation(relationId, false);
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(currentMetadataPath);

	ListCell   *snapshotIdCell = NULL;
	List	   *snapshotIdList = Int64ArrayToList(snapshotIds);

	foreach(snapshotIdCell, snapshotIdList)
	{
		int64	   *snapshotId = (int64 *) lfirst(snapshotIdCell);
		IcebergSnapshot *snapshot = GetIcebergSnapshotViaId(metadata, *snapshotId);

		bool		bestEffort = false;

		IcebergSnapshotAddAllReferencedFiles(snapshot, fileHash, bestEffort);
	}

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* convert the files in the hash to tuplestore */
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, fileHash);

	PathHashEntry *entry = NULL;

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

	/* see find_all_referenced_files_via_snapshot_ids */
	EnsureStorageCredentialsForRelation(relationId);

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
	bool		bestEffort = false;

	foreach(metadataPathCell, prevMetadataList)
	{
		char	   *prevMetadataPath = lfirst(metadataPathCell);

		IcebergMetadataAddAllReferencedFiles(prevMetadataPath, prevReferencedFileHash,
											 bestEffort);
	}

	HTAB	   *currentReferencedFileHash = CreateFilesHash();

	IcebergMetadataAddAllReferencedFiles(currentMetadataPath, currentReferencedFileHash,
										 bestEffort);

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
	bool		bestEffort = false;

	int			snapshotIndex = 0;

	for (snapshotIndex = 0; snapshotIndex < prevSnapshotCount; snapshotIndex++)
	{
		IcebergSnapshot *snapshot = &prevSnapshots[snapshotIndex];

		IcebergSnapshotAddAllReferencedFiles(snapshot, prevReferencedFileHash, bestEffort);
	}

	HTAB	   *currentReferencedFileHash = CreateFilesHash();

	for (snapshotIndex = 0; snapshotIndex < currentSnapshotCount; snapshotIndex++)
	{
		IcebergSnapshot *snapshot = &currentSnapshots[snapshotIndex];

		IcebergSnapshotAddAllReferencedFiles(snapshot, currentReferencedFileHash, bestEffort);
	}

	return FindUnreferencedFilesAmongHTABs(prevReferencedFileHash, currentReferencedFileHash);
}


/*
* FindUnreferencedFilesAmongHTABs finds all the files that are not referenced in the
* current hash table but are referenced in the previous hash table.
*/
List *
FindUnreferencedFilesAmongHTABs(HTAB *prevReferencedFileHash, HTAB *currentReferencedFileHash)
{
	List	   *unreferencedFiles = NIL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, prevReferencedFileHash);
	PathHashEntry *entry = NULL;

	while ((entry = hash_seq_search(&status)) != NULL)
	{
		bool		found = false;

		PathHashSearch(currentReferencedFileHash, entry->path, HASH_FIND, &found);

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
	/*
	 * Test hook: the eager, drop-time enumeration. A distinct name lets a
	 * test force it to fail (so the drop falls back to queuing the storage
	 * prefix, see test_injection_point_on_enumeration_path) independently of
	 * the deferred resolution path.
	 */
	INJECTION_POINT_COMPAT("iceberg-find-referenced-files");

	HTAB	   *fileHash = CreateFilesHash();
	bool		bestEffort = false;

	IcebergMetadataAddAllReferencedFiles(metadataPath, fileHash, bestEffort);

	List	   *referencedFiles = NIL;
	HASH_SEQ_STATUS status;

	hash_seq_init(&status, fileHash);
	PathHashEntry *entry = NULL;

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
IcebergMetadataAddAllReferencedFiles(char *metadataPath, HTAB *fileHash,
									 bool bestEffort)
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

		IcebergSnapshotAddAllReferencedFiles(snapshot, fileHash, bestEffort);
	}
}


/*
* IcebergSnapshotAddAllReferencedFiles adds all the files that are referenced
* in the snapshot to the hash table.
*/
static void
IcebergSnapshotAddAllReferencedFiles(IcebergSnapshot * snapshot, HTAB *fileHash,
									 bool bestEffort)
{
	if (bestEffort && !RemoteFileExists(snapshot->manifest_list))
	{
		ereport(WARNING,
				(errmsg("skipping manifest list %s, which is no longer in object storage",
						snapshot->manifest_list),
				 errdetail("Files that only this manifest list references cannot be "
						   "found and are left behind.")));
		return;
	}

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

		if (bestEffort && !RemoteFileExists(manifest->manifest_path))
		{
			ereport(WARNING,
					(errmsg("skipping manifest %s, which is no longer in object storage",
							manifest->manifest_path),
					 errdetail("The data files it references cannot be found and are "
							   "left behind.")));
			continue;
		}

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

		/*
		 * Add the paths from our own context: AppendFileToHash copies each
		 * path into the current context, and the context above is about to be
		 * reset.
		 */
		MemoryContextSwitchTo(oldContext);

		foreach(dataFileCell, manifestDataFiles)
		{
			DataFile   *dataFile = lfirst(dataFileCell);

			AppendFileToHash(dataFile->file_path, fileHash);
		}

		MemoryContextReset(manifestDataFileFetchContext);
	}

	MemoryContextDelete(manifestDataFileFetchContext);
}

/*
* CreateFilesHash creates a hash table that is suitable for storing
* file paths.
*
* The hash keys on the path pointer, and AppendFileToHash copies the path into
* the memory context that is current when it is called, so callers have to
* append from a context that outlives the hash (not from a scratch context
* they reset).
*/
HTAB *
CreateFilesHash(void)
{
	return CreatePathHash("Referenced Files Hash", sizeof(PathHashEntry),
						  1024, CurrentMemoryContext);
}


/*
* AppendFileToHash appends the file to the hash table, and
* returns true if the file already exists in the hash table.
*
* The hash keeps a copy of the path, allocated in the caller's memory context,
* so the caller has to append from a context that outlives the hash.
*/
bool
AppendFileToHash(const char *path, HTAB *referencedFilesHash)
{
	bool		found = false;

	PathHashSearch(referencedFilesHash, path, HASH_FIND, &found);

	if (found)
		return true;

	/*
	 * Only the pointer is stored, so the hash needs a copy of the path it can
	 * keep. Callers hand us paths decoded from a manifest, which they discard
	 * per manifest.
	 */
	PathHashSearch(referencedFilesHash, pstrdup(path), HASH_ENTER, NULL);

	return false;
}
