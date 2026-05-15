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
#include "utils/builtins.h"

#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/rest_catalog/rest_catalog.h"

PG_FUNCTION_INFO_V1(test_rest_catalog_add_snapshot_body);


/*
 * test_rest_catalog_add_snapshot_body builds the JSON body for an
 * add-snapshot REST catalog request from a manifest_list path supplied by
 * the caller. Used to verify that hostile characters in the manifest list
 * path (e.g. embedded double quotes or backslashes) are JSON-escaped
 * rather than producing malformed or injectable JSON.
 */
Datum
test_rest_catalog_add_snapshot_body(PG_FUNCTION_ARGS)
{
	char	   *manifestList = text_to_cstring(PG_GETARG_TEXT_P(0));
	IcebergSnapshot snapshot = {0};

	snapshot.snapshot_id = 1;
	snapshot.parent_snapshot_id = 0;
	snapshot.sequence_number = 1;
	snapshot.manifest_list = manifestList;
	snapshot.schema_id = 0;

	RestCatalogRequest *request = GetAddSnapshotCatalogRequest(&snapshot, InvalidOid);

	PG_RETURN_TEXT_P(cstring_to_text(request->body));
}
