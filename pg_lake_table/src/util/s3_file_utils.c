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

#include "catalog/pg_type.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/describe/describe.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/util/s3_file_utils.h"
#include "pg_lake/util/string_utils.h"
#include "nodes/makefuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"
#include "utils/typcache.h"


PG_FUNCTION_INFO_V1(pg_lake_file_size);
PG_FUNCTION_INFO_V1(pg_lake_file_exists);
PG_FUNCTION_INFO_V1(pg_lake_file_preview);
PG_FUNCTION_INFO_V1(pg_lake_list_files);
PG_FUNCTION_INFO_V1(pg_lake_delete_file);

static bool IsFileListSupportedUrl(char *path);

/* lake_file.enable_delete_function setting */
bool		EnableDeleteFileFunction = false;


/*
 * pg_lake_file_size returns the size of a file in S3.
 */
Datum
pg_lake_file_size(PG_FUNCTION_ARGS)
{
	char	   *path = text_to_cstring(PG_GETARG_TEXT_P(0));

	/* sanity-check URL */
	if (!IsSupportedURL(path))
		ereport(ERROR, (errmsg("file_size: only s3://, gs://, az://, azure://, and abfss:// urls are supported"),
						errcode(ERRCODE_INVALID_PARAMETER_VALUE)));

	CheckURLReadAccess();

	int64		fileSize = GetRemoteFileSize(path);

	PG_RETURN_INT64(fileSize);
}


/*
 * pg_lake_list_files returns the contents of the given S3 bucket with the given
 * filters applied.
 *
 * It emulates the behavior of the aws s3 ls command. It relies on the glob()
 * function to list the files in the bucket from DuckDB.
 */
Datum
pg_lake_list_files(PG_FUNCTION_ARGS)
{
	char	   *globURL = text_to_cstring(PG_GETARG_TEXT_P(0));

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	/* sanity-check URL */
	if (!IsSupportedURL(globURL) || !IsFileListSupportedUrl(globURL))
		ereport(ERROR, (errmsg("list_files: only s3://, gs://, az://, azure://, abfss://, and hf:// urls are supported"),
						errcode(ERRCODE_INVALID_PARAMETER_VALUE)));

	CheckURLReadAccess();

	List	   *files = ListRemoteFileDescriptions(globURL);
	ListCell   *fileCell = NULL;

	foreach(fileCell, files)
	{
		RemoteFileDesc *fileDesc = lfirst(fileCell);

		Datum		values[] = {
			CStringGetTextDatum(fileDesc->path),
			fileDesc->hasFileSize ? Int64GetDatum(fileDesc->fileSize) : 0,
			fileDesc->hasLastModifiedTime ? TimestampTzGetDatum(fileDesc->lastModifiedTime) : 0,
			fileDesc->etag != NULL ? CStringGetTextDatum(fileDesc->etag) : 0
		};
		bool		nulls[] = {
			false,
			!fileDesc->hasFileSize,
			!fileDesc->hasLastModifiedTime,
			fileDesc->etag == NULL
		};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}


/*
 * IsFileListSupportedURL returns whether the given path uses a protocol that
 * supports list operations.
 */
static bool
IsFileListSupportedUrl(char *path)
{
	if (path == NULL)
	{
		return false;
	}

	return strncmp(path, S3_URL_PREFIX, strlen(S3_URL_PREFIX)) == 0 ||
		strncmp(path, GCS_URL_PREFIX, strlen(GCS_URL_PREFIX)) == 0 ||
		strncmp(path, AZURE_URL_PREFIX, strlen(AZURE_URL_PREFIX)) == 0 ||
		strncmp(path, AZURE_BLOB_URL_PREFIX, strlen(AZURE_BLOB_URL_PREFIX)) == 0 ||
		strncmp(path, AZURE_DLS_URL_PREFIX, strlen(AZURE_DLS_URL_PREFIX)) == 0 ||
		strncmp(path, HUGGING_FACE_URL_PREFIX, strlen(HUGGING_FACE_URL_PREFIX)) == 0;
}


/*
 * pg_lake_file_exists returns whether the given file exists in S3.
 */
Datum
pg_lake_file_exists(PG_FUNCTION_ARGS)
{
	char	   *path = text_to_cstring(PG_GETARG_TEXT_P(0));

	/* sanity-check URL */
	if (!IsSupportedURL(path))
		ereport(ERROR, (errmsg("file_exists: only s3://, gs://, az://, azure://, and abfss:// urls are supported"),
						errcode(ERRCODE_INVALID_PARAMETER_VALUE)));

	CheckURLReadAccess();

	bool		fileExists = RemoteFileExists(path);

	PG_RETURN_BOOL(fileExists);
}


/*
 * pg_lake_file_preview() returns a structured description of the source file, as would
 * be returned in the content if we created it as a table. First argument is the
 * URL of the file to preview. The second argument is the key/value option for
 * compression and is optional. The third argument is a string of one or more
 * CSV key/value options and is optional.
 */
Datum
pg_lake_file_preview(PG_FUNCTION_ARGS)
{
	if (PG_ARGISNULL(0))
		ereport(ERROR, errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				(errmsg("file_preview: url is required")));

	char	   *url = text_to_cstring(PG_GETARG_TEXT_PP(0));
	List	   *previewOptionsList = NIL;

	/*
	 * Add format as an option in previewOptionsList.
	 */
	if (!PG_ARGISNULL(1))
	{
		char	   *formatName = text_to_cstring(PG_GETARG_TEXT_PP(1));
		DefElem    *formatOption =
			makeDefElem("format", (Node *) makeString(formatName), -1);

		previewOptionsList = lappend(previewOptionsList, formatOption);
	}

	/*
	 * Add compression as an option in previewOptionsList.
	 */
	if (!PG_ARGISNULL(2))
	{
		char	   *compressionName = text_to_cstring(PG_GETARG_TEXT_PP(2));
		DefElem    *compressionOption =
			makeDefElem("compression", (Node *) makeString(compressionName), -1);

		previewOptionsList = lappend(previewOptionsList, compressionOption);
	}

	/* sanity-check URL */
	if (!IsSupportedURL(url))
		ereport(ERROR, (errmsg("file_preview: only s3://, gs://, az://, azure://, and abfss:// urls are supported"),
						errcode(ERRCODE_INVALID_PARAMETER_VALUE)));

	CheckURLReadAccess();

	InitMaterializedSRF(fcinfo, 0);

	CopyDataFormat dataFormat;
	CopyDataCompression dataCompression;

	FindDataFormatAndCompression(PG_LAKE_INVALID_TABLE_TYPE, url, previewOptionsList,
								 &dataFormat, &dataCompression);

	List	   *columns = DescribeColumnsForURL(url, dataFormat, dataCompression,
												previewOptionsList);
	ListCell   *columnCell = NULL;

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	foreach(columnCell, columns)
	{
		ColumnDef  *column = lfirst(columnCell);
		TypeName   *type = column->typeName;
		char	   *typeName = format_type_be(type->typeOid);

		Datum		values[] = {
			CStringGetTextDatum(column->colname),
			CStringGetTextDatum(typeName)
		};
		bool		nulls[2] = {false, false};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}


/*
 * pg_lake_delete_file deletes a remote file.
 */
Datum
pg_lake_delete_file(PG_FUNCTION_ARGS)
{
	char	   *path = text_to_cstring(PG_GETARG_TEXT_P(0));

	/* sanity-check URL */
	if (!IsSupportedURL(path))
		ereport(ERROR, (errmsg("delete_file: only s3://, gs://, az://, azure://, and abfss:// urls are supported"),
						errcode(ERRCODE_INVALID_PARAMETER_VALUE)));

	CheckURLWriteAccess();

	if (!EnableDeleteFileFunction)
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("delete_file: file deletion has been disabled "
							   "by the administrator"),
						errdetail("Deletion is a dangerous operation that cannot "
								  "be undone")));

	DeleteRemoteFile(path);

	PG_RETURN_VOID();
}
