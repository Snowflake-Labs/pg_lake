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

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/parquet/leaf_field.h"

#include "utils/builtins.h"
#include "utils/tuplestore.h"


PG_FUNCTION_INFO_V1(pg_lake_get_leaf_field_ids);

 /*
  * pg_lake_get_leaf_field_ids reads all leaf field ids from the current
  * schema in the given metadata uri.
  */
Datum
pg_lake_get_leaf_field_ids(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	char	   *metadataUri = text_to_cstring(PG_GETARG_TEXT_P(0));

	Datum		values[3];
	bool		nulls[3];

	memset(values, 0, sizeof(values));
	memset(nulls, 0, sizeof(nulls));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataUri);

	List	   *leafFields = GetLeafFieldsFromIcebergMetadata(metadata);

	ListCell   *leafFieldCell = NULL;

	foreach(leafFieldCell, leafFields)
	{
		LeafField  *leafField = lfirst(leafFieldCell);

		PGType		pgType = IcebergFieldToPostgresType(leafField->field);

		values[0] = Int32GetDatum(leafField->fieldId);
		values[1] = ObjectIdGetDatum(pgType.postgresTypeOid);
		values[2] = Int32GetDatum(pgType.postgresTypeMod);

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}
