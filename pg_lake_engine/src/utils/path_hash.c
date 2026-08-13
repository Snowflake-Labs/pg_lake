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

#include "postgres.h"

#include "common/hashfn.h"
#include "pg_lake/util/path_hash.h"

static uint32 PathHashKeyHash(const void *key, Size keysize);
static int	PathHashKeyCompare(const void *key1, const void *key2, Size keysize);


/*
 * CreatePathHash creates a hash table keyed on a char * path.
 *
 * entrySize is the size of the caller's entry struct, whose first member must
 * be the char * path. nelem and context have the same meaning as in
 * hash_create.
 *
 * The hash stores the pointer, not the string, so every path handed to
 * PathHashSearch with HASH_ENTER has to outlive the hash.
 */
HTAB *
CreatePathHash(const char *name, Size entrySize, long nelem, MemoryContext context)
{
	HASHCTL		hashCtl;

	Assert(entrySize >= sizeof(PathHashEntry));

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = sizeof(char *);
	hashCtl.entrysize = entrySize;
	hashCtl.hash = PathHashKeyHash;
	hashCtl.match = PathHashKeyCompare;
	hashCtl.hcxt = context;

	return hash_create(name, nelem, &hashCtl,
					   HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}


/*
 * PathHashSearch is hash_search for a hash created by CreatePathHash.
 *
 * dynahash copies keysize bytes from the key argument, which for a pointer key
 * means the pointer itself, so the key argument is the address of the pointer
 * rather than the pointer. Passing a path straight to hash_search would copy
 * the first 8 bytes of the string instead, which is why this wrapper exists.
 *
 * The hash and match functions dereference the key, so path cannot be NULL.
 * Every caller either takes the path from a catalog or manifest column that is
 * NOT NULL, or filters nulls out before getting here.
 */
void *
PathHashSearch(HTAB *pathHash, const char *path, HASHACTION action, bool *foundPtr)
{
	Assert(path != NULL);

	return hash_search(pathHash, &path, action, foundPtr);
}


/*
 * PathHashKeyHash hashes the string behind a pointer key.
 *
 * Unlike dynahash's string_hash it hashes the whole string, since there is no
 * fixed key size to truncate to.
 */
static uint32
PathHashKeyHash(const void *key, Size keysize PG_USED_FOR_ASSERTS_ONLY)
{
	Assert(keysize == sizeof(char *));

	const char *path = *(const char *const *) key;

	return hash_bytes((const unsigned char *) path, strlen(path));
}


/*
 * PathHashKeyCompare compares the strings behind two pointer keys.
 */
static int
PathHashKeyCompare(const void *key1, const void *key2, Size keysize PG_USED_FOR_ASSERTS_ONLY)
{
	Assert(keysize == sizeof(char *));

	const char *path1 = *(const char *const *) key1;
	const char *path2 = *(const char *const *) key2;

	return strcmp(path1, path2);
}
