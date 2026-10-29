/*
 * Copyright 2026 Snowflake Inc.
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
 * immutable_file_cache.c
 *
 * Table metadata, manifest lists and manifests are written once and never
 * rewritten at the same path: a change to a table writes new files and points
 * the catalog at them. Planning and scanning one table reads the same few of
 * them many times, a dozen reads of one metadata.json in a single INSERT ...
 * SELECT over a UNION, and the file cache in pgduck_server fills in the
 * background, so until it does each of those reads is another round trip to
 * object storage.
 *
 * So a backend keeps what it read, by path, and hands out copies. A file that
 * is not in object storage yet (an upload pending in this transaction) is read
 * from its local copy as before and not kept. The total kept is bounded; past
 * the bound everything is dropped and the backend starts over.
 */

#include "postgres.h"

#include "utils/hsearch.h"
#include "utils/memutils.h"

#include "pg_lake/iceberg/immutable_file_cache.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_lake/util/s3_writer_utils.h"


/* paths longer than this are read every time rather than kept */
#define IMMUTABLE_FILE_PATH_MAX 1024

/* what one backend keeps before starting over */
#define IMMUTABLE_FILE_CACHE_MAX_BYTES (64 * 1024 * 1024)

typedef struct ImmutableFile
{
	char		path[IMMUTABLE_FILE_PATH_MAX];
	char	   *content;
	size_t		length;
}			ImmutableFile;

static HTAB *ImmutableFiles = NULL;
static MemoryContext ImmutableFilesContext = NULL;
static size_t ImmutableFilesBytes = 0;

static char *ReadImmutableFile(const char *path, bool binary, size_t *length);
static void ResetImmutableFiles(void);


/*
 * ReadImmutableIcebergText returns a palloc'd copy of a write-once text file,
 * such as a table metadata file.
 */
char *
ReadImmutableIcebergText(const char *path)
{
	size_t		length = 0;

	return ReadImmutableFile(path, false, &length);
}


/*
 * ReadImmutableIcebergBlob returns a palloc'd copy of a write-once binary file,
 * such as a manifest list or a manifest, and its length.
 */
char *
ReadImmutableIcebergBlob(const char *path, size_t *length)
{
	return ReadImmutableFile(path, true, length);
}


static char *
ReadImmutableFile(const char *path, bool binary, size_t *length)
{
	bool		keepable = strlen(path) < IMMUTABLE_FILE_PATH_MAX &&
		GetPendingUploadLocalPath(path) == NULL;

	if (keepable && ImmutableFiles != NULL)
	{
		ImmutableFile *kept = hash_search(ImmutableFiles, path, HASH_FIND, NULL);

		if (kept != NULL)
		{
			/* the text copy keeps its terminator */
			char	   *copy = palloc(kept->length + 1);

			memcpy(copy, kept->content, kept->length + 1);
			*length = kept->length;
			return copy;
		}
	}

	char	   *content;

	if (binary)
		content = GetBlobFromURI(path, length);
	else
	{
		content = GetTextFromURI(path);
		*length = strlen(content);
	}

	if (!keepable)
		return content;

	if (ImmutableFilesBytes + *length > IMMUTABLE_FILE_CACHE_MAX_BYTES)
		ResetImmutableFiles();

	if (ImmutableFiles == NULL)
	{
		HASHCTL		info;

		ImmutableFilesContext =
			AllocSetContextCreate(TopMemoryContext,
								  "pg_lake immutable iceberg files",
								  ALLOCSET_DEFAULT_SIZES);

		memset(&info, 0, sizeof(info));
		info.keysize = IMMUTABLE_FILE_PATH_MAX;
		info.entrysize = sizeof(ImmutableFile);
		info.hcxt = ImmutableFilesContext;

		ImmutableFiles = hash_create("pg_lake immutable iceberg files", 256,
									 &info, HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}

	/* the copy first, so a failed allocation leaves no entry without one */
	char	   *keptContent = MemoryContextAlloc(ImmutableFilesContext, *length + 1);

	memcpy(keptContent, content, *length);
	keptContent[*length] = '\0';

	bool		found = false;
	ImmutableFile *kept = hash_search(ImmutableFiles, path, HASH_ENTER, &found);

	if (found)
		pfree(keptContent);
	else
	{
		kept->content = keptContent;
		kept->length = *length;
		ImmutableFilesBytes += *length;
	}

	return content;
}


/*
 * ResetImmutableFiles drops everything kept.
 */
static void
ResetImmutableFiles(void)
{
	if (ImmutableFilesContext != NULL)
		MemoryContextDelete(ImmutableFilesContext);

	ImmutableFilesContext = NULL;
	ImmutableFiles = NULL;
	ImmutableFilesBytes = 0;
}
