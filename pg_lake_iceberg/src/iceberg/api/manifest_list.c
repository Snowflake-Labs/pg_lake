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

#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/iceberg/api/manifest_list.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/s3_writer_utils.h"

#include "utils/lsyscache.h"

/*
* UploadIcebergManifestListToURI writes the manifest list to a file
* and uploads it to given uri. It returns the size of the local manifest list file.
*/
int64_t
UploadIcebergManifestListToURI(List *manifestList, char *manifestListURI)
{
	char	   *localManifestListPath = GenerateTempFileName(PG_LAKE_ICEBERG, true);

	WriteIcebergManifestList(localManifestListPath, manifestList);

	bool		autoDeleteRecord = true;

	ScheduleFileCopyToS3WithCleanup(localManifestListPath, manifestListURI, autoDeleteRecord);

	int64		manifestListSize = GetLocalFileSize(localManifestListPath);

	return manifestListSize;
}

/*
 * GenerateRemoteManifestListPath generates the remote path for the manifest list.
 */
char *
GenerateRemoteManifestListPath(int64_t snapshotId, const char *location, const char *snapshotUUID, int manifestListIndex, char *queryArguments)
{
	StringInfo	remoteFilePath = makeStringInfo();

	appendStringInfo(remoteFilePath, "%s/metadata/snap-%" PRId64 "-%d-%s.avro%s",
					 location, snapshotId, manifestListIndex, snapshotUUID, queryArguments);
	return remoteFilePath->data;
}
