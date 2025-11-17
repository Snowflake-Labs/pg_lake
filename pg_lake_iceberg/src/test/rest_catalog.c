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
#include "miscadmin.h"

#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"

#include "pg_lake/rest_catalog/rest_catalog.h"

PG_FUNCTION_INFO_V1(register_namespace_to_rest_catalog);

/*
* register_namespace_to_rest_catalog is a test function that registers
* a namespace to the rest catalog.
*/
Datum
register_namespace_to_rest_catalog(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(1));

	RegisterNamespaceToRestCatalog(catalogName, namespaceName);
	PG_RETURN_VOID();
}
