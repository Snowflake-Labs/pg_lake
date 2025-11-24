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

#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/pgduck/write_data.h"

#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(initial_metadata_for_table);
PG_FUNCTION_INFO_V1(iceberg_table_fieldids);


/*
* initial_metadata_for_table is a test function that gets a table oid and
* returns the initial metadata for the table.
*/
Datum
initial_metadata_for_table(PG_FUNCTION_ARGS)
{
	Oid			foreignTableOid = PG_GETARG_OID(0);
	char	   *location = "test_location";

	DataFileSchema *schema = GetDataFileSchemaForTable(foreignTableOid);
	IcebergPartitionSpec *partitionSpec = palloc0(sizeof(IcebergPartitionSpec));

	/* both spark and us require empty spec even if there are no partitions */
	partitionSpec->spec_id = DEFAULT_SPEC_ID;
	partitionSpec->fields_length = 0;
	partitionSpec->fields = NULL;

	IcebergTableMetadata *metadata = GenerateEmptyTableMetadata(location);

	AppendCurrentPostgresSchema(foreignTableOid, metadata, schema);
	AppendPartitionSpec(metadata, partitionSpec);

	char	   *serializedMetadataText = WriteIcebergTableMetadataToJson(metadata);

	PG_RETURN_TEXT_P(cstring_to_text(serializedMetadataText));
}


/*
* iceberg_table_fieldids is a test function that gets a table oid and
* returns the field ids for the table.
*/
Datum
iceberg_table_fieldids(PG_FUNCTION_ARGS)
{
	Oid			foreignTableOid = PG_GETARG_OID(0);

	DataFileSchema *schema = GetDataFileSchemaForTable(foreignTableOid);
	StringInfo	command = makeStringInfo();

	appendStringInfoString(command, "{");
	AppendFields(command, schema);
	appendStringInfoString(command, "}");

	PG_RETURN_TEXT_P(cstring_to_text(command->data));
}
