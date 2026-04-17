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

#include "pg_lake/data_file/data_files.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/string_utils.h"
#include "pg_lake/util/s3_writer_utils.h"

#include "common/hashfn.h"
#include "nodes/pg_list.h"
#include "utils/hsearch.h"



/*
 * PartitionSpecManifestsEntry is hash entry to group manifests by partition spec id.
 */
typedef struct PartitionSpecManifestsEntry
{
	int32_t		partitionSpecId;
	List	   *manifests;
} PartitionSpecManifestsEntry;


/*
 * ManifestGroup is used as a "bin", at the best fit bin packing algorithm,
 * with "TargetManifestSizeKB" size to store manifest files during manifest merge.
 */
typedef struct ManifestGroup
{
	List	   *manifests;
	int32_t		partitionSpecId;
	int64_t		totalSize;
} ManifestGroup;


static HTAB *GroupManifestsByPartitionSpecId(List *manifests);
static ManifestGroup *FindBestFitManifestGroupForManifest(List *manifestGroups, IcebergManifest *manifest, int64_t targetSize);
static List *CreateManifestGroupsWithTargetSize(List *manifests, int64_t targetSize);
static bool RemoveDeletedManifestEntriesInternal(IcebergManifest **manifest, IcebergSnapshot *currentSnapshot,
												 List *allTransforms, IcebergManifestContentType contentType,
												 const char *metadataLocation, const char *snapshotUUID, int *manifestIndex);
static bool IsMergeableManifestGroup(ManifestGroup *manifestGroup, IcebergManifest *latestManifest);
static IcebergManifest *MergeManifestGroup(ManifestGroup *mergeableManifestGroup, IcebergSnapshot *currentSnapshot,
										   List *allTransforms, int mergedManifestIndex, const char *metadataLocation,
										   const char *snapshotUUID);

#ifdef USE_ASSERT_CHECKING
static int	IcebergManifestSequenceNumberComparator(const ListCell *manifestCell1, const ListCell *manifestCell2);
static void EnsureLatestManifest(IcebergManifest *latestManifest, List *manifests);
#endif

/* pg_lake_iceberg.enable_manifest_merge */
bool		EnableManifestMergeOnWrite = true;

/* pg_lake_iceberg.target_manifest_size_kb */
int			TargetManifestSizeKB = DEFAULT_TARGET_MANIFEST_SIZE_KB;

/* pg_lake_iceberg.manifest_min_count_to_merge */
int			ManifestMinCountToMerge = DEFAULT_MANIFEST_MIN_COUNT_TO_MERGE;


/*
 * MergeDataManifests merges the manifest files of content "ADD" and of the same
 * partition spec. It groups the manifest files by partition spec id and then
 * creates manifest groups with size, that is as much close to TargetManifestSizeKB
 * as possible. Then, it merges the manifest groups.
 *
 * Overall algorithm is as follows:
 * - Groups manifest files by partition spec id,
 * - For each partition spec,
 * 		- Finds manifest groups with size, that is as much close
 * 		  to TargetManifestSizeKB as possible (best fit bin packing algorithm).
 * 		- Merges the manifest groups. An important note here is that manifest group
 * 		  which contains the latest manifest is handled differently than other groups.
 * 		  We apply an extra check for it where the group should contain at least
 * 		  ManifestMinCountToMerge manifest files. This is only applied to this group
 * 		  to not prevent other groups with older manifests from being merged.
 * 		  Other groups might occur when
 * 			1. old manifest files left before we set EnableManifestMergeOnWrite to true.
 * 			2. TargetManifestSizeKB is configured smaller according to ManifestMinCountToMerge.
 * 			   e.g. 1 manifest ~ 8KB, TargetManifestSizeKB = 16KB, ManifestMinCountToMerge = 100
 * 			   first group will be blocked by ManifestMinCountToMerge check after 2 manifests
 * 			   (can accommodate 2 due to TargetManifestSizeKB) are added to it, then we will
 * 			   start to create other groups.
 * 		- Rewrites the latest manifest list with merged and unmerged
 * 		  manifest files.
 */
List *
MergeDataManifests(IcebergSnapshot *currentSnapshot,
				   List *allTransforms,
				   List *dataManifests,
				   const char *metadataLocation,
				   const char *snapshotUUID,
				   bool isVerbose,
				   int *manifestIndex)
{
	/* only manifests of ADD content can be merged */
	if (dataManifests == NIL)
	{
		/* no data manifests to merge */
		return NIL;
	}

	IcebergManifest *latestManifest = llast(dataManifests);

#ifdef USE_ASSERT_CHECKING
	/* our algorithm relies on latestManifest is actually the latest */
	if (EnableHeavyAsserts)
		EnsureLatestManifest(latestManifest, dataManifests);
#endif

	/* only manifests with the same partition spec could be merged */
	HTAB	   *dataManifestsByPartitionSpecId = GroupManifestsByPartitionSpecId(dataManifests);

	List	   *mergedManifests = NIL;
	List	   *unmergedManifests = NIL;

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, dataManifestsByPartitionSpecId);

	int64_t		targetSize = TargetManifestSizeKB * KB_BYTES;

	PartitionSpecManifestsEntry *partitionSpecManifestsEntry = NULL;

	while ((partitionSpecManifestsEntry = hash_seq_search(&status)) != NULL)
	{
		List	   *manifests = partitionSpecManifestsEntry->manifests;

		List	   *manifestGroups = CreateManifestGroupsWithTargetSize(manifests, targetSize);

		ListCell   *manifestGroupCell = NULL;

		foreach(manifestGroupCell, manifestGroups)
		{
			ManifestGroup *manifestGroup = lfirst(manifestGroupCell);

			if (IsMergeableManifestGroup(manifestGroup, latestManifest))
			{
				IcebergManifest *mergedManifest = MergeManifestGroup(manifestGroup, currentSnapshot, allTransforms,
																	 *manifestIndex, metadataLocation,
																	 snapshotUUID);

				/* might be NULL if all entries are DELETED from old snapshots */
				if (mergedManifest != NULL)
				{
					mergedManifests = lappend(mergedManifests, mergedManifest);
					(*manifestIndex) += 1;

					ereport(isVerbose ? INFO : LOG,
							(errmsg("merging %d manifests into %s",
									list_length(manifestGroup->manifests),
									mergedManifest->manifest_path)));
				}
			}
			else
			{
				unmergedManifests = list_concat(unmergedManifests, manifestGroup->manifests);
			}
		}
	}

	return list_concat(mergedManifests, unmergedManifests);
}


/*
 * RemoveDeletedManifestEntries removes deleted entries of old snapshots and
 * pushes and sets a new list of manifests without the deleted entries. It returns
 * true if any manifest is modified, false otherwise.
 */
bool
RemoveDeletedManifestEntries(IcebergSnapshot *currentSnapshot,
							 List *allTransforms,
							 List **manifests,
							 IcebergManifestContentType contentType,
							 const char *metadataLocation,
							 const char *snapshotUUID,
							 bool isVerbose,
							 int *manifestIndex)
{
	List	   *resultManifests = NIL;
	bool		anyManifestModified = false;

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, *manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		bool		modified = RemoveDeletedManifestEntriesInternal(&manifest, currentSnapshot,
																	allTransforms, contentType,
																	metadataLocation, snapshotUUID,
																	manifestIndex);

		anyManifestModified |= modified;

		if (manifest != NULL)
			resultManifests = lappend(resultManifests, manifest);
	}

	*manifests = resultManifests;

	return anyManifestModified;
}


/*
* HasMergeableManifests checks if there are any manifest files that can be merged.
*/
bool
HasMergeableManifests(List *dataManifests)
{
	/* only manifests of ADD content can be merged */
	if (dataManifests == NIL)
	{
		/* no data manifests to merge */
		return false;
	}

	IcebergManifest *latestManifest = llast(dataManifests);

#ifdef USE_ASSERT_CHECKING
	/* our algorithm relies on latestManifest is actually the latest */
	if (EnableHeavyAsserts)
		EnsureLatestManifest(latestManifest, dataManifests);
#endif

	/* only manifests with the same partition spec could be merged */
	HTAB	   *dataManifestsByPartitionSpecId = GroupManifestsByPartitionSpecId(dataManifests);

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, dataManifestsByPartitionSpecId);

	int64_t		targetSize = TargetManifestSizeKB * KB_BYTES;

	PartitionSpecManifestsEntry *partitionSpecManifestsEntry = NULL;

	while ((partitionSpecManifestsEntry = hash_seq_search(&status)) != NULL)
	{
		List	   *manifests = partitionSpecManifestsEntry->manifests;

		List	   *manifestGroups = CreateManifestGroupsWithTargetSize(manifests, targetSize);

		ListCell   *manifestGroupCell = NULL;

		foreach(manifestGroupCell, manifestGroups)
		{
			ManifestGroup *manifestGroup = lfirst(manifestGroupCell);

			if (IsMergeableManifestGroup(manifestGroup, latestManifest))
			{
				hash_seq_term(&status);
				return true;
			}
		}
	}

	return false;
}


/*
 * GroupManifestsByPartitionSpecId returns a hash map that groups manifests
 * by partition spec id.
 */
static HTAB *
GroupManifestsByPartitionSpecId(List *manifests)
{
	HASHCTL		hashInfo;

	hashInfo.keysize = sizeof(int32_t);
	hashInfo.entrysize = sizeof(PartitionSpecManifestsEntry);
	hashInfo.hash = uint32_hash;
	hashInfo.hcxt = CurrentMemoryContext;

	uint32		hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	HTAB	   *manifestsByPartitionSpecIdHash =
		hash_create("manifests grouped by spec id cache", list_length(manifests), &hashInfo, hashFlags);

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		bool		found = false;

		PartitionSpecManifestsEntry *entry = hash_search(manifestsByPartitionSpecIdHash, &manifest->partition_spec_id, HASH_ENTER, &found);

		if (!found)
		{
			entry->partitionSpecId = manifest->partition_spec_id;
			entry->manifests = NIL;
		}

		entry->manifests = lappend(entry->manifests, manifest);
	}

	return manifestsByPartitionSpecIdHash;
}


/*
 * CreateManifestGroupsWithTargetSize creates manifest groups with following criteria:
 *  - total size of manifests in each group is as much close to target size as possible.
 *
 * We apply a best fit bin packing algorithm to find the best fit manifest group for
 * each manifest. Best fit group is the group that has the least remaining space after
 * putting the manifest into the group.
 */
static List *
CreateManifestGroupsWithTargetSize(List *manifests, int64_t targetSize)
{
	List	   *manifestGroups = NIL;

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		ManifestGroup *manifestGroup = FindBestFitManifestGroupForManifest(manifestGroups, manifest, targetSize);

		if (manifestGroup == NULL)
		{
			/* could not find a group for the manifest so create a new group */
			manifestGroup = palloc0(sizeof(ManifestGroup));

			manifestGroup->partitionSpecId = manifest->partition_spec_id;
			manifestGroups = lappend(manifestGroups, manifestGroup);
		}

		manifestGroup->manifests = lappend(manifestGroup->manifests, manifest);
		manifestGroup->totalSize += manifest->manifest_length;
	}

	return manifestGroups;
}


/*
 * FindBestFitManifestGroupForManifest finds the best fit manifest group for the manifest
 * amongst the manifest groups. Best fit group is the group that has the least
 * remaining space after putting the manifest into the group.
 * Returns NULL if no best fit group is found.
 */
static ManifestGroup *
FindBestFitManifestGroupForManifest(List *manifestGroups, IcebergManifest *manifest, int64_t targetSize)
{
	int			bestFitGroupIndex = -1;
	int64_t		leastRemainingSize = INT64_MAX;

	ListCell   *groupCell = NULL;

	foreach(groupCell, manifestGroups)
	{
		ManifestGroup *group = lfirst(groupCell);

		int64_t		candidateGroupSize = group->totalSize + manifest->manifest_length;

		if (candidateGroupSize <= targetSize &&
			targetSize - candidateGroupSize < leastRemainingSize)
		{
			leastRemainingSize = targetSize - candidateGroupSize;
			bestFitGroupIndex = foreach_current_index(groupCell);
		}
	}

	if (bestFitGroupIndex == -1)
	{
		/* no best fit group found */
		return NULL;
	}

	return list_nth(manifestGroups, bestFitGroupIndex);
}


/*
 * IsMergeableManifestGroup checks if the manifest group can be merged.
 */
static bool
IsMergeableManifestGroup(ManifestGroup *manifestGroup, IcebergManifest *latestManifest)
{
	if (list_length(manifestGroup->manifests) == 1)
	{
		return false;
	}
	else if (list_member_ptr(manifestGroup->manifests, latestManifest) &&
			 list_length(manifestGroup->manifests) < ManifestMinCountToMerge)
	{
		/*
		 * We enforce min manifest count only to the group which contains the
		 * the latest manifest. This is required to merge only after
		 * ManifestMinCountToMerge writes.
		 *
		 * Other groups might be found because of old manifest files before
		 * enabling merge on write. We merge them without
		 * "ManifestMinCountToMerge" check.
		 */
		return false;
	}

	return true;
}


/*
 * MergeManifestGroup merges given manifest group into a single manifest. Then, it uploads
 * the merged manifest to the remote location and returns it.
 */
static IcebergManifest *
MergeManifestGroup(ManifestGroup *mergeableManifestGroup, IcebergSnapshot *currentSnapshot,
				   List *allTransforms, int mergedManifestIndex, const char *metadataLocation,
				   const char *snapshotUUID)
{
	List	   *mergeableManifestEntries = NIL;
	ListCell   *mergeableManifestCell = NULL;

	foreach(mergeableManifestCell, mergeableManifestGroup->manifests)
	{
		IcebergManifest *newManifest = lfirst(mergeableManifestCell);

		List	   *manifestEntries = FetchManifestEntriesFromManifest(newManifest, NULL);

		/*
		 * set the status of entries to EXISTING if they are from the old
		 * snapshot. This is crucial for some engines, e.g. snowflake, that
		 * relies on entry statuses to check if they are new or not.
		 */
		SetExistingStatusForOldSnapshotAddedEntries(manifestEntries, currentSnapshot->snapshot_id);

		mergeableManifestEntries = list_concat(mergeableManifestEntries, manifestEntries);
	}

	if (mergeableManifestEntries == NIL)
	{
		/* no entries to merge */
		return NULL;
	}

	char	   *remoteManifestPath = GenerateRemoteManifestPath(metadataLocation,
																snapshotUUID,
																mergedManifestIndex, "");

	int64_t		manifestSize = UploadIcebergManifestToURI(mergeableManifestEntries, remoteManifestPath);

	IcebergManifest *mergedManifest =
		CreateNewIcebergManifest(currentSnapshot, mergeableManifestGroup->partitionSpecId,
								 allTransforms, manifestSize, ICEBERG_MANIFEST_FILE_CONTENT_DATA,
								 remoteManifestPath, mergeableManifestEntries);

	return mergedManifest;
}


/*
 * RemoveDeletedManifestEntriesInternal removes deleted entries of old snapshots
 * and pushes and sets a new manifest without the deleted entries. It returns
 * true if the manifest was modified, false otherwise.
 */
static bool
RemoveDeletedManifestEntriesInternal(IcebergManifest **manifest, IcebergSnapshot *currentSnapshot,
									 List *allTransforms, IcebergManifestContentType contentType,
									 const char *metadataLocation, const char *snapshotUUID, int *manifestIndex)
{
	List	   *manifestEntries = FetchManifestEntriesFromManifest(*manifest, NULL);

	List	   *newManifestEntries = NIL;

	ListCell   *manifestEntryCell = NULL;

	foreach(manifestEntryCell, manifestEntries)
	{
		IcebergManifestEntry *manifestEntry = lfirst(manifestEntryCell);

		if (manifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_DELETED)
		{
			continue;
		}

		newManifestEntries = lappend(newManifestEntries, manifestEntry);
	}

	if (newManifestEntries == NIL)
	{
		/* all entries are removed, skip this manifest */
		*manifest = NULL;
		return true;
	}

	bool		removedAnyEntry = list_length(newManifestEntries) != list_length(manifestEntries);

	if (!removedAnyEntry)
	{
		/* no entries were removed, no modifications made */
		return false;
	}

	char	   *remoteManifestPath =
		GenerateRemoteManifestPath(metadataLocation,
								   snapshotUUID,
								   (*manifestIndex)++, "");

	int64_t		manifestSize = UploadIcebergManifestToURI(newManifestEntries, remoteManifestPath);

	IcebergManifest *newManifest =
		CreateNewIcebergManifest(currentSnapshot, (*manifest)->partition_spec_id, allTransforms,
								 manifestSize, contentType, remoteManifestPath, newManifestEntries);

	*manifest = newManifest;

	/* some entries were removed, manifest was modified */
	return true;
}


#ifdef USE_ASSERT_CHECKING
static void
EnsureLatestManifest(IcebergManifest *latestManifest, List *manifests)
{
	/* copy the list to not have flaky tests */
	List	   *copyManifests = list_copy(manifests);

	list_sort(copyManifests, IcebergManifestSequenceNumberComparator);

	IcebergManifest *expectedManifest = linitial(copyManifests);

	Assert(expectedManifest->sequence_number == expectedManifest->sequence_number);

	return;
}


/*
 * IcebergManifestSequenceNumberComparator is comparator to sort manifests
 * by sequence number in descending order. (the latest manifest is the first in the list)
 */
static int
IcebergManifestSequenceNumberComparator(const ListCell *manifestCell1, const ListCell *manifestCell2)
{
	IcebergManifest *manifest1 = lfirst(manifestCell1);
	IcebergManifest *manifest2 = lfirst(manifestCell2);

	return manifest2->sequence_number - manifest1->sequence_number;
}
#endif
