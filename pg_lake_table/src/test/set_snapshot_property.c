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

#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/util/rel_utils.h"
#include "utils/builtins.h"

PG_FUNCTION_INFO_V1(set_iceberg_snapshot_property);

/*
 * set_iceberg_snapshot_property exposes SetIcebergSnapshotProperty to tests.
 * There is no SQL definition in the extension; tests create one.
 */
Datum
set_iceberg_snapshot_property(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	char	   *key = text_to_cstring(PG_GETARG_TEXT_PP(1));
	char	   *value = text_to_cstring(PG_GETARG_TEXT_PP(2));

	EnsureTableOwner(relationId);

	SetIcebergSnapshotProperty(relationId, key, value);

	PG_RETURN_VOID();
}
