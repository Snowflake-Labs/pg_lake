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

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/cache_control.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"


PG_FUNCTION_INFO_V1(pg_lake_add_file_to_cache);
PG_FUNCTION_INFO_V1(pg_lake_remove_file_from_cache);
PG_FUNCTION_INFO_V1(pg_lake_list_files_in_cache);

/*
 * pg_lake_add_file_to_cache adds a single file to the cache.
 *
 * The cache is shared by all users, so the URL goes through the same checks
 * as any other read. Otherwise a parameter like ?s3_endpoint= could fetch
 * content from another host and store it under the plain s3:// URL.
 */
Datum
pg_lake_add_file_to_cache(PG_FUNCTION_ARGS)
{
	char	   *url = ResolveStageURL(text_to_cstring(PG_GETARG_TEXT_P(0)));
	bool		refresh = PG_GETARG_BOOL(1);

	CheckURLReadAccess(url);

	int64		fileSize = AddFileToCache(url, refresh);

	PG_RETURN_INT64(fileSize);
}


/*
 * pg_lake_remove_file_from_cache removes a single file from the cache.
 */
Datum
pg_lake_remove_file_from_cache(PG_FUNCTION_ARGS)
{
	char	   *url = ResolveStageURL(text_to_cstring(PG_GETARG_TEXT_P(0)));

	CheckURLReadAccess(url);

	bool		isRemoved = RemoveFileFromCache(url);

	PG_RETURN_BOOL(isRemoved);
}


/*
 * pg_lake_list_files_in_cache lists all the files in the cache.
 */
Datum
pg_lake_list_files_in_cache(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	List	   *filesInCache = ListFilesInCache();
	ListCell   *fileCell = NULL;

	/* convert the list of files to tuples in a tuple store */
	foreach(fileCell, filesInCache)
	{
		FileInCache *file = lfirst(fileCell);
		Datum		values[3];
		bool		nulls[3];

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		values[0] = CStringGetTextDatum(file->url);
		values[1] = Int64GetDatum(file->fileSize);
		values[2] = TimestampGetDatum(file->lastAccessTime);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}
