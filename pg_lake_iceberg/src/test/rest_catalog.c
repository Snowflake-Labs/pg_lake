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
#include "lib/stringinfo.h"
#include "utils/builtins.h"

#include "pg_lake/rest_catalog/rest_catalog.h"

PG_FUNCTION_INFO_V1(register_namespace_to_rest_catalog);
PG_FUNCTION_INFO_V1(get_rest_metadata_location);
PG_FUNCTION_INFO_V1(get_rest_vended_credentials);

/*
* register_namespace_to_rest_catalog is a test function that registers
* a namespace to the rest catalog.
*/
Datum
register_namespace_to_rest_catalog(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(1));

	RestCatalogOptions *opts = ResolveRestCatalogOptions(REST_CATALOG_NAME);

	RegisterNamespaceToRestCatalog(opts, catalogName, namespaceName);
	PG_RETURN_VOID();
}


/*
 * get_rest_metadata_location is a test function that calls
 * GetMetadataLocationFromRestCatalog and returns the metadata location.
 * This exercises the full LoadTableFromRestCatalog path including
 * vended credential extraction and caching.
 */
Datum
get_rest_metadata_location(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char	   *tableName = text_to_cstring(PG_GETARG_TEXT_P(2));

	RestCatalogOptions *opts = ResolveRestCatalogOptions(REST_CATALOG_NAME);

	char	   *metadataLocation =
		GetMetadataLocationFromRestCatalog(opts, catalogName, namespaceName,
										   tableName);

	PG_RETURN_TEXT_P(cstring_to_text(metadataLocation));
}


/*
 * get_rest_vended_credentials is a test function that loads a table from
 * the REST catalog and returns the extracted vended credentials as a
 * pipe-delimited summary:
 *
 *     "<access-key-id>|<scope>|<yes|no session token>|<expiry|noexpiry>"
 *
 * Returns NULL when the loadTable response carried no vended
 * credentials.  This exercises ExtractVendedCredentials, including
 * storage-credentials parsing, scope resolution, and expiry parsing.
 */
Datum
get_rest_vended_credentials(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *namespaceName = text_to_cstring(PG_GETARG_TEXT_P(1));
	char	   *tableName = text_to_cstring(PG_GETARG_TEXT_P(2));

	RestCatalogOptions *opts = ResolveRestCatalogOptions(REST_CATALOG_NAME);

	RestCatalogLoadTableResult result =
		LoadTableFromRestCatalog(opts, catalogName, namespaceName, tableName);

	if (result.vendedCredentials == NULL)
		PG_RETURN_NULL();

	VendedCredentials *creds = result.vendedCredentials;
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s|%s|%s|%s",
					 creds->accessKeyId ? creds->accessKeyId : "",
					 creds->scope ? creds->scope : "",
					 creds->sessionToken ? "yes" : "no",
					 creds->expiresAt > 0 ? "expiry" : "noexpiry");

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}
