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

#include "pg_lake/transaction/track_iceberg_metadata_changes.h"

PG_FUNCTION_INFO_V1(test_rest_catalog_identifier_json);


/*
 * test_rest_catalog_identifier_json exposes IdentifierJson() for unit tests
 * that need to verify hostile namespace / table names (containing JSON
 * meta-characters like '"' or '\\') are properly escaped before being
 * embedded in REST catalog request bodies.
 */
Datum
test_rest_catalog_identifier_json(PG_FUNCTION_ARGS)
{
	char	   *namespaceFlat = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *tableName = text_to_cstring(PG_GETARG_TEXT_P(1));

	char	   *result = IdentifierJson(namespaceFlat, tableName);

	PG_RETURN_TEXT_P(cstring_to_text(result));
}
