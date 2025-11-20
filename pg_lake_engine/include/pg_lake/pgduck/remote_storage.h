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

#include "datatype/timestamp.h"
#include "nodes/pg_list.h"

/*
 * RemoteFileDesc holds a descriptions of a remote file.
 */
typedef struct RemoteFileDesc
{
	char	   *path;

	bool		hasFileSize;
	int64		fileSize;

	bool		hasLastModifiedTime;
	TimestampTz lastModifiedTime;

	char	   *etag;
}			RemoteFileDesc;

extern PGDLLEXPORT int64 GetRemoteFileSize(char *path);
extern PGDLLEXPORT int64 GetRemoteParquetFileRowCount(char *path);
extern PGDLLEXPORT List *ListRemoteFileDescriptions(char *pattern);
extern PGDLLEXPORT List *ListRemoteFileNames(char *pattern);
extern PGDLLEXPORT bool RemoteFileExists(char *path);
extern PGDLLEXPORT bool DeleteRemoteFile(char *path);
extern PGDLLEXPORT bool DeleteRemotePrefix(char *path);
