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

#include "common/hashfn.h"
#include "utils/memutils.h"

#include "pg_lake/transaction/data_file_path_set.h"

/*
 * The dynahash key is the pointer; PathHash and PathCompare dereference it so
 * that lookups match on path contents.
 */
typedef struct DataFilePathSetEntry
{
	const char *path;
	bool		flag;
}			DataFilePathSetEntry;

struct DataFilePathSet
{
	HTAB	   *entries;
};

static uint32 PathHash(const void *key, Size keysize);
static int	PathCompare(const void *key1, const void *key2, Size keysize);


/*
 * CreateDataFilePathSet creates an empty set in the current memory context.
 * initialSize is a hint; the set grows as needed.
 */
DataFilePathSet *
CreateDataFilePathSet(const char *name, long initialSize)
{
	DataFilePathSet *set = palloc0(sizeof(DataFilePathSet));

	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(char *);
	hashCtl.entrysize = sizeof(DataFilePathSetEntry);
	hashCtl.hash = PathHash;
	hashCtl.match = PathCompare;
	hashCtl.hcxt = CurrentMemoryContext;

	set->entries = hash_create(name, Max(initialSize, 32), &hashCtl,
							   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);

	return set;
}


/*
 * DestroyDataFilePathSet releases the set. The paths it points at belong to the
 * caller and are left alone.
 */
void
DestroyDataFilePathSet(DataFilePathSet * set)
{
	hash_destroy(set->entries);
	pfree(set);
}


/*
 * DataFilePathSetAdd adds path with its flag cleared, and returns false if the
 * path was already in the set.
 */
bool
DataFilePathSetAdd(DataFilePathSet * set, const char *path)
{
	bool		found = false;
	DataFilePathSetEntry *entry =
		hash_search(set->entries, &path, HASH_ENTER, &found);

	if (found)
		return false;

	entry->flag = false;

	return true;
}


/*
 * DataFilePathSetFlag returns a pointer to the flag of path's entry so the
 * caller can read or set it, or NULL when path is not in the set.
 */
bool *
DataFilePathSetFlag(DataFilePathSet * set, const char *path)
{
	DataFilePathSetEntry *entry =
		hash_search(set->entries, &path, HASH_FIND, NULL);

	return entry != NULL ? &entry->flag : NULL;
}


/*
 * PathHash hashes the string the key points to.
 */
static uint32
PathHash(const void *key, Size keysize)
{
	const char *path = *(const char *const *) key;

	return hash_bytes((const unsigned char *) path, strlen(path));
}


/*
 * PathCompare compares the strings the two keys point to.
 */
static int
PathCompare(const void *key1, const void *key2, Size keysize)
{
	return strcmp(*(const char *const *) key1, *(const char *const *) key2);
}
