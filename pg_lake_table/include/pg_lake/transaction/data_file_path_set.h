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

#pragma once

#include "utils/hsearch.h"

/*
 * DataFilePathSet is a set of data file paths, each carrying one caller-defined
 * boolean flag.
 *
 * Keys are pointers to strings the caller owns, hashed by content rather than
 * by pointer. That makes an entry 32 bytes, where a dynahash with an inline
 * MAX_S3_PATH_LENGTH key needs ~1.2 KB for paths that are a couple of hundred
 * bytes in practice, and it removes the fixed key's failure mode of truncating
 * an over-length path into a key that collides with a different path.
 *
 * The caller owns the strings: they must outlive the set.
 */
typedef struct DataFilePathSet DataFilePathSet;

extern DataFilePathSet * CreateDataFilePathSet(const char *name, long initialSize);
extern void DestroyDataFilePathSet(DataFilePathSet * set);

/* adds path with a false flag; returns false if it was already present */
extern bool DataFilePathSetAdd(DataFilePathSet * set, const char *path);

/* returns the entry's flag, writable, or NULL if the path is not in the set */
extern bool *DataFilePathSetFlag(DataFilePathSet * set, const char *path);
