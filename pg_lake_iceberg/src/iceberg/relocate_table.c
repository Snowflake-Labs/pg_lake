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
 * relocate_table.c - Move an iceberg table to a new storage location.
 *
 * Copies every referenced data file to the new prefix, rewrites all
 * manifests / manifest-lists / metadata with updated paths, writes the
 * new metadata tree to the destination, and updates the pg_lake catalog
 * pointer.
 *
 * The function operates on the full metadata tree (all snapshots), so
 * history is preserved. If only the current snapshot is desired, expire
 * old snapshots before calling this.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/operations/find_referenced_files.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/parallel_command.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/path_hash.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/util/string_utils.h"

#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pg_lake/util/temporal_utils.h"

PG_FUNCTION_INFO_V1(iceberg_relocate_table);


/*
 * Entry in the path-rewrite hash: maps an old path to its new path so
 * that shared data files (referenced by multiple manifests/snapshots)
 * are copied exactly once.
 */
typedef struct RelocatePathEntry
{
	char	   *path;			/* old path — hash key */
	char	   *newPath;
}			RelocatePathEntry;


/*
 * RewritePath replaces oldPrefix with newPrefix at the start of path.
 * Returns a freshly palloc'd string.  Errors if path does not start
 * with oldPrefix.
 */
static char *
RewritePath(const char *path, const char *oldPrefix, size_t oldPrefixLen,
			const char *newPrefix)
{
	if (strncmp(path, oldPrefix, oldPrefixLen) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("file path \"%s\" does not start with the table location \"%s\"",
						path, oldPrefix)));

	StringInfo	buf = makeStringInfo();

	appendStringInfoString(buf, newPrefix);
	appendStringInfoString(buf, path + oldPrefixLen);
	return buf->data;
}


/*
 * CopyRemoteFileCommand returns the DuckDB SQL to copy a remote file
 * to a new remote location via pg_lake_copy_file.
 */
static char *
CopyRemoteFileCommand(const char *srcUri, const char *dstUri)
{
	StringInfoData command;

	initStringInfo(&command);
	appendStringInfo(&command, "SELECT * FROM pg_lake_copy_file(%s,%s);",
					 quote_literal_cstr(srcUri), quote_literal_cstr(dstUri));
	return command.data;
}


/*
 * RelocateDataFiles walks every snapshot → manifest-list → manifest →
 * data-file, schedules a parallel copy for each unique data file, and
 * returns a hash mapping old path → new path for the rewrite pass.
 *
 * Manifest and manifest-list files are NOT copied here; they will be
 * regenerated with rewritten paths in the next pass.
 */
static HTAB *
RelocateDataFiles(IcebergTableMetadata *metadata,
				  const char *oldLocation, size_t oldLocationLen,
				  const char *newLocation)
{
	HTAB	   *rewriteHash = CreatePathHash("relocate rewrite",
											 sizeof(RelocatePathEntry),
											 1024, CurrentMemoryContext);

	List	   *copyCommands = NIL;
	int			filesCopied = 0;

	for (int si = 0; si < metadata->snapshots_length; si++)
	{
		IcebergSnapshot *snapshot = &metadata->snapshots[si];

		List	   *manifests = FetchManifestsFromSnapshot(snapshot, NULL);
		ListCell   *mc = NULL;

		foreach(mc, manifests)
		{
			IcebergManifest *manifest = lfirst(mc);

			MemoryContext perManifestCtx =
				AllocSetContextCreate(CurrentMemoryContext,
									  "relocate per-manifest",
									  ALLOCSET_DEFAULT_SIZES);
			MemoryContext callerCtx = MemoryContextSwitchTo(perManifestCtx);

			List	   *entries = ReadManifestEntries(manifest->manifest_path);
			ListCell   *ec = NULL;

			foreach(ec, entries)
			{
				IcebergManifestEntry *entry = lfirst(ec);
				const char *filePath = entry->data_file.file_path;

				/* dedup: skip if already scheduled */
				MemoryContextSwitchTo(callerCtx);

				bool		found = false;

				PathHashSearch(rewriteHash, filePath, HASH_FIND, &found);
				if (!found)
				{
					char	   *newPath = RewritePath(filePath, oldLocation,
													  oldLocationLen, newLocation);

					RelocatePathEntry *re =
						PathHashSearch(rewriteHash, pstrdup(filePath),
									   HASH_ENTER, NULL);

					re->newPath = newPath;

					copyCommands = lappend(copyCommands,
										   CopyRemoteFileCommand(filePath, newPath));
					filesCopied++;
				}

				MemoryContextSwitchTo(perManifestCtx);
			}

			MemoryContextSwitchTo(callerCtx);
			MemoryContextDelete(perManifestCtx);
		}
	}

	if (copyCommands != NIL)
	{
		ereport(NOTICE,
				(errmsg("relocating %d data file(s)", filesCopied)));

		ExecuteCommandsInParallelInPGDuck(copyCommands, DEFAULT_MAX_PARALLEL_FILE_UPLOADS);
	}

	return rewriteHash;
}


/*
 * LookupNewPath returns the rewritten path from the hash, or errors
 * if the old path was not found (which would be a bug).
 */
static const char *
LookupNewPath(HTAB *rewriteHash, const char *oldPath)
{
	bool		found = false;
	RelocatePathEntry *re =
		PathHashSearch(rewriteHash, oldPath, HASH_FIND, &found);

	if (!found)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("relocate: data file path not in rewrite hash: \"%s\"",
						oldPath)));
	return re->newPath;
}


/*
 * RewriteSnapshotManifests rewrites all manifests for every snapshot,
 * substituting data-file paths via the rewrite hash, and uploading the
 * new manifests + manifest-lists to the new location.  Updates each
 * snapshot's manifest_list in place.
 */
static void
RewriteSnapshotManifests(IcebergTableMetadata *metadata,
						 HTAB *rewriteHash,
						 const char *oldLocation, size_t oldLocationLen,
						 const char *newLocation)
{
	for (int si = 0; si < metadata->snapshots_length; si++)
	{
		IcebergSnapshot *snapshot = &metadata->snapshots[si];

		List	   *manifests = FetchManifestsFromSnapshot(snapshot, NULL);
		List	   *newManifestList = NIL;
		ListCell   *mc = NULL;
		int			manifestIndex = 0;

		char	   *snapshotUUID = GenerateUUID();

		foreach(mc, manifests)
		{
			IcebergManifest *manifest = lfirst(mc);

			MemoryContext perManifestCtx =
				AllocSetContextCreate(CurrentMemoryContext,
									  "relocate manifest rewrite",
									  ALLOCSET_DEFAULT_SIZES);
			MemoryContext callerCtx = MemoryContextSwitchTo(perManifestCtx);

			List	   *entries = ReadManifestEntries(manifest->manifest_path);
			ListCell   *ec = NULL;

			/* rewrite data_file.file_path in each entry */
			foreach(ec, entries)
			{
				IcebergManifestEntry *entry = lfirst(ec);
				const char *oldFilePath = entry->data_file.file_path;
				const char *newFilePath;

				MemoryContextSwitchTo(callerCtx);
				newFilePath = LookupNewPath(rewriteHash, oldFilePath);
				MemoryContextSwitchTo(perManifestCtx);

				entry->data_file.file_path = pstrdup(newFilePath);
				entry->data_file.file_path_length = strlen(newFilePath);
			}

			/* write new manifest to new location */
			MemoryContextSwitchTo(callerCtx);

			char	   *newManifestPath =
				GenerateRemoteManifestPath(newLocation, snapshotUUID,
										   manifestIndex++, "");

			int64_t		manifestSize =
				UploadIcebergManifestToURI(entries, newManifestPath);

			/* build a new manifest header pointing to the new path */
			IcebergManifest *newManifest = palloc0(sizeof(IcebergManifest));

			memcpy(newManifest, manifest, sizeof(IcebergManifest));
			newManifest->manifest_path = newManifestPath;
			newManifest->manifest_path_length = strlen(newManifestPath);
			newManifest->manifest_length = manifestSize;

			newManifestList = lappend(newManifestList, newManifest);

			MemoryContextDelete(perManifestCtx);
		}

		/* write new manifest list to new location */
		char	   *newManifestListPath =
			GenerateRemoteManifestListPath(snapshot->snapshot_id,
										   newLocation, snapshotUUID,
										   0, "");

		UploadIcebergManifestListToURI(newManifestList, newManifestListPath);

		/* update the snapshot in place */
		snapshot->manifest_list = newManifestListPath;
		snapshot->manifest_list_length = strlen(newManifestListPath);
	}
}


/*
 * RewriteMetadataPaths updates in-metadata paths that reference the
 * old location: metadata_log entries, partition_statistics, and
 * statistics paths.
 */
static void
RewriteMetadataPaths(IcebergTableMetadata *metadata,
					 const char *oldLocation, size_t oldLocationLen,
					 const char *newLocation)
{
	/* metadata_log: previous metadata.json paths */
	for (int i = 0; i < metadata->metadata_log_length; i++)
	{
		IcebergMetadataLogEntry *entry = &metadata->metadata_log[i];

		if (entry->metadata_file != NULL &&
			strncmp(entry->metadata_file, oldLocation, oldLocationLen) == 0)
		{
			entry->metadata_file = RewritePath(entry->metadata_file,
											   oldLocation, oldLocationLen,
											   newLocation);
		}
	}

	/* partition_statistics paths */
	for (int i = 0; i < metadata->partition_statistics_length; i++)
	{
		IcebergPartitionStatistics *ps = &metadata->partition_statistics[i];

		if (ps->statistics_path != NULL &&
			strncmp(ps->statistics_path, oldLocation, oldLocationLen) == 0)
		{
			ps->statistics_path = RewritePath(ps->statistics_path,
											  oldLocation, oldLocationLen,
											  newLocation);
		}
	}

	/* statistics paths */
	for (int i = 0; i < metadata->statistics_length; i++)
	{
		IcebergStatistics *st = &metadata->statistics[i];

		if (st->statistics_path != NULL &&
			strncmp(st->statistics_path, oldLocation, oldLocationLen) == 0)
		{
			st->statistics_path = RewritePath(st->statistics_path,
											  oldLocation, oldLocationLen,
											  newLocation);
		}
	}
}


/*
 * iceberg_relocate_table copies all data files, rewrites metadata, and
 * updates the catalog to point to the new location.
 *
 * SQL signature:
 *   lake_iceberg.relocate_table(table_name regclass, new_location text)
 *   RETURNS text  -- new metadata.json path
 */
Datum
iceberg_relocate_table(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	char	   *newLocation = text_to_cstring(PG_GETARG_TEXT_P(1));

	/* strip trailing slash for consistent prefix matching */
	bool		inPlace = false;

	newLocation = StripTrailingSlash(newLocation, inPlace);

	if (!IsSupportedURL(newLocation))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unsupported URL for new location: \"%s\"", newLocation)));

	CheckURLWriteAccess(newLocation);

	ErrorIfReadOnlyIcebergTable(relationId);

	/* lock the catalog row and read current metadata */
	char	   *oldMetadataLocation = GetIcebergMetadataLocation(relationId, true);
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(oldMetadataLocation);

	char	   *oldLocation = StripTrailingSlash(pstrdup(metadata->location), inPlace);
	size_t		oldLocationLen = strlen(oldLocation);

	if (strcmp(oldLocation, newLocation) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("new location is the same as the current location")));

	ereport(NOTICE,
			(errmsg("relocating table from \"%s\" to \"%s\"",
					oldLocation, newLocation)));

	/* 1. Copy all data files (deduplicated across snapshots) */
	HTAB	   *rewriteHash = RelocateDataFiles(metadata, oldLocation,
												oldLocationLen, newLocation);

	/* 2. Rewrite + upload manifests and manifest lists */
	RewriteSnapshotManifests(metadata, rewriteHash,
							 oldLocation, oldLocationLen, newLocation);

	/* 3. Update metadata.location */
	metadata->location = pstrdup(newLocation);
	metadata->location_length = strlen(newLocation);

	/* 4. Rewrite misc metadata paths (metadata_log, statistics, etc.) */
	RewriteMetadataPaths(metadata, oldLocation, oldLocationLen, newLocation);

	/* 5. Bump last_updated_ms */
	metadata->last_updated_ms = PostgresTimestampToIcebergTimestampMs();

	/* 6. Write new metadata.json */
	int			newVersion = 0;

	/* derive version from old metadata path if it follows the NNNnn pattern */
	const char *oldBasename = strrchr(oldMetadataLocation, '/');

	if (oldBasename != NULL)
	{
		oldBasename++;
		int			parsedVersion = atoi(oldBasename);

		if (parsedVersion > 0)
			newVersion = parsedVersion + 1;
	}

	char	   *newMetadataPath =
		GenerateRemoteMetadataFilePath(newVersion, newLocation, "");

	UploadTableMetadataToURI(metadata, newMetadataPath);

	/* 7. Flush all pending uploads (data files + metadata) */
	FinishAllUploads();

	/* 8. Update the catalog */
	UpdateInternalCatalogMetadataLocation(relationId, newMetadataPath,
										  oldMetadataLocation);

	ereport(NOTICE,
			(errmsg("relocation complete, new metadata: %s", newMetadataPath)));

	PG_RETURN_TEXT_P(cstring_to_text(newMetadataPath));
}
