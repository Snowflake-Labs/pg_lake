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
 * Hash tables keyed on an object storage path.
 *
 * The straightforward way to key a dynahash on a path is a
 * char[MAX_S3_PATH_LENGTH] key with HASH_STRINGS, but that reserves 1095
 * bytes per entry for paths that are ~120 bytes in practice. On the
 * pre-commit path we build such a hash over every file in the table, so the
 * padding, not the data, dominated the peak.
 *
 * These helpers key on a char * instead: the entry holds only the pointer and
 * the hash and match functions dereference it. There is deliberately no
 * HASH_KEYCOPY, so the string is not copied and must outlive the hash. That
 * is what callers want in the common case, where the path was already
 * pstrdup'd out of SPI or Avro into a context at least as long-lived as the
 * hash. A caller whose path lives in a scratch context it resets (one
 * manifest at a time, say) has to copy the path itself before inserting it.
 */

#pragma once

#include "utils/hsearch.h"
#include "utils/palloc.h"

/*
 * PathHashEntry is the entry type of a path hash that carries no payload,
 * i.e. a set of paths. Entries with a payload declare their own struct with a
 * char * path as its first member.
 */
typedef struct PathHashEntry
{
	char	   *path;
}			PathHashEntry;

extern PGDLLEXPORT HTAB *CreatePathHash(const char *name, Size entrySize,
										long nelem, MemoryContext context);
extern PGDLLEXPORT void *PathHashSearch(HTAB *pathHash, const char *path,
										HASHACTION action, bool *foundPtr);
