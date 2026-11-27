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
#include "utils/builtins.h"
#include "utils/tuplestore.h"

#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api/datafile.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/storage/storage_credentials.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/table_type.h"

PG_FUNCTION_INFO_V1(external_column_stats);

/*
 * external_column_stats returns the column stats that pruning uses for each
 * data file in the current snapshot of an Iceberg table pg_lake did not write,
 * as they are read from the manifests: one row per data file and field, with
 * a NULL null_count when the manifest has none for the field.
 */
Datum
external_column_stats(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);

	if (!IsExternalIcebergTable(relationId))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("relation is not an external Iceberg table")));

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	char	   *metadataLocation = GetIcebergMetadataLocation(relationId, false);

	EnsureStorageCredentialsForRelation(relationId);

	IcebergTableMetadata *metadata =
		ReadIcebergTableMetadataForRelation(relationId, metadataLocation);

	if (LowercasesColumnNames(relationId))
		LowercaseIcebergTableMetadataNames(metadata);

	List	   *dataFiles = NIL;
	List	   *deleteFiles = NIL;

	FetchAllDataAndDeleteFilesFromCurrentSnapshot(metadata, &dataFiles, &deleteFiles);

	List	   *leafFields = GetLeafFieldsForTable(relationId);
	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);
		List	   *columnStats = GetIcebergDataFileColumnStats(dataFile, leafFields);
		ListCell   *columnStatsCell = NULL;

		foreach(columnStatsCell, columnStats)
		{
			DataFileColumnStats *stats = lfirst(columnStatsCell);
			Datum		values[5];
			bool		nulls[5] = {false, false, false, false, false};

			values[0] = CStringGetTextDatum(dataFile->file_path);
			values[1] = Int32GetDatum(stats->leafField.fieldId);
			values[2] = CStringGetTextDatum(stats->lowerBoundText);
			values[3] = CStringGetTextDatum(stats->upperBoundText);
			values[4] = Int64GetDatum(stats->nullCount);
			nulls[4] = !stats->hasNullCount;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
		}
	}

	PG_RETURN_VOID();
}
