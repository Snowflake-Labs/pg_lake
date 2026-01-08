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
#include "miscadmin.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/remote_data_file_stats.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/util/array_utils.h"
#include "pg_lake/util/rel_utils.h"
#include "utils/builtins.h"

static List *GenerateMetadataOperationList(Oid relationId, List *fileList, char *fileType);
static void EnsureAllFilesAreParquet(List *fileList);
static bool IsFileParquet(char *s3Uri);

PG_FUNCTION_INFO_V1(add_files_to_table);

/*
 * add_files_to_table Let users add files to an Iceberg table.
 * This procedure is only for testing purposes and not for production use yet.
 */
Datum
add_files_to_table(PG_FUNCTION_ARGS)
{
	Oid			relationId = InvalidOid;
	ArrayType  *filePathsArray = NULL;

	/*
	 * There is no STRICT option for procedures, so we prefer to throw error
	 * for simplicity.
	 */
	if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
		ereport(ERROR, (errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED), errmsg("parameters cannot be NULL")));

	relationId = PG_GETARG_OID(0);
	filePathsArray = PG_GETARG_ARRAYTYPE_P(1);

	/*
	 * For regular modifications, Postgres already checks the permissions. For
	 * this procedure, we need to check the permissions manually.
	 */
	EnsureTableOwner(relationId);

	List	   *fileList = StringArrayToList(filePathsArray);

	EnsureAllFilesAreParquet(fileList);

	/* convert the user input to the common API of TableMetadataOperation */
	char	   *fileType = "DATA";
	List	   *metadataOperations =
		GenerateMetadataOperationList(relationId, fileList, fileType);

	ApplyDataFileCatalogChanges(relationId, metadataOperations);

	/* track metadata changes */
	List	   *operationTypes = GetMetadataOperationTypes(metadataOperations);

	TrackIcebergMetadataChangesInTx(relationId, operationTypes);

	PG_RETURN_VOID();
}


/*
* EnsureAllFilesAreParquet throws an error if any of the files are not
* Parquet files.
*/
static void
EnsureAllFilesAreParquet(List *fileList)
{
	ListCell   *fileCell = NULL;

	foreach(fileCell, fileList)
	{
		char	   *s3Uri = lfirst(fileCell);

		if (!IsFileParquet(s3Uri))
		{
			ereport(ERROR, (errmsg("File format is not parquet: %s", s3Uri),
							errdetail("lake_iceberg.add_files_to_table() only supports files "
									  "in parquet format")));
		}
	}

}


/*
* IsFileParquet checks if a file is a Parquet file.
*/
static bool
IsFileParquet(char *s3Uri)
{
	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "SELECT * FROM parquet_metadata(%s);",
					 quote_literal_cstr(s3Uri));


	volatile bool isParquet = true;

	PG_TRY();
	{
		/* would throw error for non-parquet files */
		ExecuteCommandInPGDuck(command.data);
	}
	PG_CATCH();
	{
		isParquet = false;
	}
	PG_END_TRY();

	return isParquet;
}


/*
* GenerateMetadataOperationList converts the file list to a list of
* metadata operations.
*/
static List *
GenerateMetadataOperationList(Oid relationId, List *fileList, char *fileType)
{
	List	   *metadataOperations = NIL;

	ListCell   *fileCell = NULL;

	foreach(fileCell, fileList)
	{
		char	   *filePath = lfirst(fileCell);
		TableMetadataOperation *operation = NULL;

		if (strcmp(fileType, "DATA") == 0)
		{
			DataFileStats *dataFileStats = GetRemoteDataFileStatsForTable(filePath, DATA_FORMAT_PARQUET, DATA_COMPRESSION_NONE, NIL, NIL);

			/* we don't support partitioned writes, and default spec id is 0 */
			int32		partitionSpecId = 0;

			operation =
				AddDataFileOperation(filePath, CONTENT_DATA, dataFileStats, NULL, partitionSpecId);
		}
		else
		{
			ereport(ERROR, (errmsg("Unsupported file type: %s", fileType)));
		}

		metadataOperations = lappend(metadataOperations, operation);
	}

	return metadataOperations;
}
