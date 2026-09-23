/*
 * Copyright 2026 Snowflake Inc.
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
 * jsonb_storage.c
 *  Per-table accessor for the `jsonb_storage` Iceberg table option.
 *
 * The option picks how the table's jsonb columns are encoded in Parquet:
 * `string` (canonical jsonb text, readable by everything) or `variant` (the
 * parsed binary encoding, readable only by engines that implement it).
 *
 * Unlike compatibility_mode, which is fixed for the life of a table, this
 * option may be changed later: it is read once per column, when that column
 * is registered, and the resulting storage type is persisted per column in
 * lake_table.field_id_mappings. Changing the option therefore only affects
 * columns added afterwards, leaving existing columns -- and the data files
 * already written for them -- alone. That is what lets a table created
 * before variant existed grow a variant column.
 */

#include "postgres.h"

#include "foreign/foreign.h"

#include "pg_lake/iceberg/jsonb_storage.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/table_type.h"


/*
 * IcebergJsonbStorageFromRelation reads the option from an existing
 * relation's stored foreign-table options.
 */
JsonbStorage
IcebergJsonbStorageFromRelation(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		return JSONB_STORAGE_STRING;

	ForeignTable *foreignTable = GetForeignTable(relationId);
	const char *optionValue =
		GetStringOption(foreignTable->options, ICEBERG_JSONB_STORAGE_OPTION, false);

	if (optionValue == NULL)
		return JSONB_STORAGE_STRING;

	return ParseJsonbStorage(optionValue);
}
