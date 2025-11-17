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

#include "common/base64.h"
#include "commands/dbcommands.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"

#include "pg_lake/http/http_client.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/url_encode.h"
#include "pg_lake/util/rel_utils.h"

/* determined by GUC */
char	   *RestCatalogHost = "http://localhost:8181";
char	   *RestCatalogClientId = NULL;
char	   *RestCatalogClientSecret = NULL;


static void CreateNamespaceOnRestCatalog(const char *catalogName, const char *namespaceName);
static char *EncodeBasicAuth(const char *clientId, const char *clientSecret);
static char *JsonbGetStringByPath(const char *jsonb_text, int nkeys,...);
static List *PostHeadersWithAuth(void);
static List *GetHeadersWithAuth(void);
static void ReportHTTPError(HttpResult httpResult, int level);


/*
* Register a namespace in the Rest Catalog.
* If the catalog exists, and the allowedLocations is different,
* an error is raised. This  is used to ensure that the same
* namespace is not registered multiple times as we define
* allowed locations as part of the namespace.
*/
void
RegisterNamespaceToRestCatalog(const char *catalogName, const char *namespaceName)
{
	/*
	 * First, we need to check if the namespace already exists in Rest Catalog
	 * via a GET request.
	 */
	char	   *getUrl =
		psprintf(REST_CATALOG_NAMESPACE_NAME,
				 RestCatalogHost, URLEncodePath(catalogName),
				 URLEncodePath(namespaceName));
	HttpResult	httpResult = HttpGet(getUrl, GetHeadersWithAuth());

	switch (httpResult.status)
	{
			/* namespace not found */
		case 404:
			{
				/*
				 * For debugging purposes
				 */
				ReportHTTPError(httpResult, DEBUG2);

				/*
				 * Does not exists, we'll create it.
				 */
				CreateNamespaceOnRestCatalog(catalogName, namespaceName);
				break;
			}

			/* namespace already exists */
		case 200:
			{
				/*
				 * Verify allowed location matches, otherwise raise an error.
				 * We raise error because we use the default location as the
				 * place where tables are stored. So, we cannot afford to have
				 * different locations for the same namespace.
				 */
				char	   *serverAllowedLocation =
					JsonbGetStringByPath(httpResult.body, 2, "properties", "location");

				const char *defaultAllowedLocation =
					psprintf("%s/%s/%s", IcebergDefaultLocationPrefix, catalogName, namespaceName);


				/*
				 * Compare by ignoring the trailing `/` char that the server
				 * might have for internal iceberg tables. For external ones,
				 * we don't have any control over.
				 */
				if ((strlen(serverAllowedLocation) - strlen(defaultAllowedLocation) > 1 ||
					 strncmp(serverAllowedLocation, defaultAllowedLocation, strlen(defaultAllowedLocation)) != 0))
				{
					ereport(DEBUG1,
							(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
							 errmsg("namespace \"%s\" is already registered with a different location than the default expected location based on default location prefix",
									namespaceName),
							 errdetail_internal("Expected location: %s, but got: %s",
												defaultAllowedLocation, serverAllowedLocation)));
				}

				break;
			}

		default:
			{
				/*
				 * Report the error to the user. Expected errors: 400 - Bad
				 * Request 401 - Unauthorized 403 - Forbidden 419 -
				 * Credentials timed out 503 - Slowdown 5XX - Internal Server
				 * Error
				 */
				ReportHTTPError(httpResult, ERROR);

				break;
			}

	}
}


/*
* ErrorIfRestNamespaceDoesNotExist checks if the namespace exists in the Rest Catalog.
* If it does not exist, an error is raised. This is used to ensure that the
* namespace exists when creating a table in the given namespace.
*/
void
ErrorIfRestNamespaceDoesNotExist(const char *catalogName, const char *namespaceName)
{
	/*
	 * First, we need to check if the namespace already exists in Rest Catalog
	 * via a GET request.
	 */
	char	   *getUrl =
		psprintf(REST_CATALOG_NAMESPACE_NAME,
				 RestCatalogHost, URLEncodePath(catalogName),
				 URLEncodePath(namespaceName));
	HttpResult	httpResult = HttpGet(getUrl, GetHeadersWithAuth());


	/* namespace not found */
	if (httpResult.status == 404)
	{
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("namespace \"%s\" does not exist in the rest catalog while creating on catalog \"%s\"",
						namespaceName, catalogName)));
	}
	else if (httpResult.status != 200)
	{
		/*
		 * Report the error to the user. Expected errors: 400 - Bad Request
		 * 401 - Unauthorized 403 - Forbidden 419 - Credentials timed out 503
		 * - Slowdown 5XX - Internal Server Error
		 */
		ReportHTTPError(httpResult, ERROR);
	}
}


/*
* Gets the metadata location for a relation from the external rest catalog.
*/
char *
GetMetadataLocationForRestCatalogForIcebergTable(Oid relationId)
{
	const char *restCatalogName = GetRestCatalogName(relationId);
	const char *relationName = GetRestCatalogTableName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);

	return GetMetadataLocationFromRestCatalog(restCatalogName, namespaceName, relationName);
}


/*
* Gets the metadata location for a relation from the external catalog.
*/
char *
GetMetadataLocationFromRestCatalog(const char *restCatalogName, const char *namespaceName, const char *relationName)
{
	char	   *getUrl =
		psprintf(GET_REST_CATALOG_METADATA_LOCATION,
				 RestCatalogHost, URLEncodePath(restCatalogName), URLEncodePath(namespaceName), URLEncodePath(relationName));

	List	   *headers = GetHeadersWithAuth();
	HttpResult	hr = HttpGet(getUrl, headers);

	if (hr.status != 200)
	{
		ReportHTTPError(hr, ERROR);
	}

	return JsonbGetStringByPath(hr.body, 1, "metadata-location");
}


/*
* CreateNamespaceOnRestCatalog creates a namespace on the rest catalog. On any failure,
* an error is raised.
*/
static void
CreateNamespaceOnRestCatalog(const char *catalogName, const char *namespaceName)
{
	/* POST create */
	StringInfoData body;

	initStringInfo(&body);
	appendStringInfoChar(&body, '{');	/* start body */
	appendJsonKey(&body, "namespace");

	appendStringInfoChar(&body, '[');	/* start namespace array */
	appendJsonValue(&body, namespaceName);
	appendStringInfoChar(&body, ']');	/* close namespace array */

	appendStringInfoChar(&body, ',');	/* close namespace array */

	/* set properties location */
	appendJsonKey(&body, "properties");

	appendStringInfoChar(&body, '{');	/* start properties object */
	appendStringInfoChar(&body, '}');	/* close properties object */

	appendStringInfoChar(&body, '}');	/* close body */

	char	   *postUrl =
		psprintf(REST_CATALOG_NAMESPACE, RestCatalogHost,
				 URLEncodePath(catalogName));

	HttpResult	httpResult = HttpPost(postUrl, body.data, PostHeadersWithAuth());

	if (httpResult.status != 200)
	{
		ReportHTTPError(httpResult, ERROR);
	}
}

/*
* Creates the headers for a POST request with authentication.
*/
static List *
PostHeadersWithAuth(void)
{
	return list_make3(psprintf("Authorization: Bearer %s", RestCatalogFetchAccessToken()),
					  pstrdup("Accept: application/json"),
					  pstrdup("Content-Type: application/json"));
}

/*
* Creates the headers for a GET request with authentication.
*/
static List *
GetHeadersWithAuth(void)
{
	return list_make2(psprintf("Authorization: Bearer %s", RestCatalogFetchAccessToken()),
					  pstrdup("Accept: application/json"));
}

/*
* Reports an HTTP error by raising an appropriate error message.
* The error format of rest catalog is follows:
* {
*  "error": {
*    "message": "Malformed request",
*    "type": "BadRequestException",
*    "code": 400
*  }
*/
static void
ReportHTTPError(HttpResult httpResult, int level)
{
	/*
	 * This is a curl error, so we don't have a proper HttpResult, don't even
	 * try to parse the response.
	 */
	if (httpResult.status == 0)
	{
		ereport(level,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
				 errmsg("HTTP request failed %s", httpResult.errorMsg ? httpResult.errorMsg : "unknown error")));

		return;
	}

	const char *message = httpResult.body ? JsonbGetStringByPath(httpResult.body, 2, "error", "message") : NULL;
	const char *type = httpResult.body ? JsonbGetStringByPath(httpResult.body, 2, "error", "type") : NULL;

	ereport(level,
			(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
			 errmsg("HTTP request failed (HTTP %ld)", httpResult.status),
			 message ? errdetail_internal("%s", message) : 0,
			 type ? errhint("The rest catalog returned error type: %s", type) : 0));
}

/*
* Fetches an access token from rest catalog using client credentials that are
* configured via GUC variables.
*/
char *
RestCatalogFetchAccessToken(void)
{
	if (!RestCatalogHost || !*RestCatalogHost)
		ereport(ERROR, (errmsg("pg_lake_iceberg.rest_catalog_host should be set")));
	if (!RestCatalogClientId || !*RestCatalogClientId)
		ereport(ERROR, (errmsg("pg_lake_iceberg.rest_catalog_client_id should be set")));
	if (!RestCatalogClientSecret || !*RestCatalogClientSecret)
		ereport(ERROR, (errmsg("pg_lake_iceberg.rest_catalog_client_secret should be set")));

	char	   *accessTokenUrl = psprintf(REST_CATALOG_AUTH_TOKEN_PATH, RestCatalogHost);

	/* Build Authorization: Basic <base64(clientId:clientSecret)> */
	char	   *encodedAuth = EncodeBasicAuth(RestCatalogClientId, RestCatalogClientSecret);
	char	   *authHeader = psprintf("Authorization: Basic %s", encodedAuth);

	/* Form-encoded body */
	const char *body = "grant_type=client_credentials&scope=PRINCIPAL_ROLE:ALL";

	/* Headers */
	List	   *headers = NIL;

	headers = lappend(headers, authHeader);
	headers = lappend(headers, "Content-Type: application/x-www-form-urlencoded");

	/* POST */
	HttpResult	httpResponse = HttpPost(accessTokenUrl, body, headers);

	if (httpResponse.status != 200)
		ereport(ERROR,
				(errmsg("Rest Catalog OAuth token request failed (HTTP %ld)", httpResponse.status),
				 httpResponse.body ? errdetail_internal("%s", httpResponse.body) : 0));

	if (!httpResponse.body || !*httpResponse.body)
		ereport(ERROR, (errmsg("Rest Catalog OAuth token response body is empty")));


	return JsonbGetStringByPath(httpResponse.body, 1, "access_token");
}


/*
 * Get a string value at the given JSON path: key1 -> key2 -> ... -> keyN
 * - jsonb_text: input JSON text (e.g., from an HTTP response)
 * - nkeys: number of keys in the path
 * - ...: const char* keys, in order
 *
 * On success: returns palloc'd C-string in the current memory context.
 * On failure: ERROR (missing key, non-object mid-level, non-string leaf).
 */
static char *
JsonbGetStringByPath(const char *jsonb_text, int nkeys,...)
{
	if (nkeys <= 0)
		ereport(ERROR, (errmsg("invalid jsonb path: number of keys must be > 0")));

	Datum		jsonbDatum = DirectFunctionCall1(jsonb_in, CStringGetDatum(jsonb_text));
	Jsonb	   *jb = DatumGetJsonbP(jsonbDatum);

	JsonbContainer *container = &jb->root;

	va_list		variableArgList;

	va_start(variableArgList, nkeys);

	for (int argIndex = 0; argIndex < nkeys; argIndex++)
	{
		const char *key = va_arg(variableArgList, const char *);
		JsonbValue	keyVal;
		JsonbValue *val;

		if (!JsonContainerIsObject(container))
			ereport(ERROR, (errmsg("json path step %d: not an object", argIndex + 1)));

		keyVal.type = jbvString;
		keyVal.val.string.val = (char *) key;
		keyVal.val.string.len = strlen(key);

		val = findJsonbValueFromContainer(container, JB_FOBJECT, &keyVal);
		if (val == NULL)
			ereport(ERROR, (errmsg("key \"%s\" missing in json response", key)));

		if (argIndex < nkeys - 1)
		{
			if (val->type != jbvBinary || !JsonContainerIsObject(val->val.binary.data))
				ereport(ERROR, (errmsg("json path step %d: key \"%s\" is not an object", argIndex + 1, key)));

			container = val->val.binary.data;	/* descend */
		}
		else
		{
			if (val->type != jbvString)
				ereport(ERROR, (errmsg("leaf \"%s\" is not a string", key)));

			va_end(variableArgList);
			return pnstrdup(val->val.string.val, val->val.string.len);
		}
	}

	va_end(variableArgList);
	ereport(ERROR, (errmsg("unexpected json path handling error")));
}


/*
* Encodes the client ID and secret into a Base64-encoded string
* suitable for use in the Authorization header.
*/
static char *
EncodeBasicAuth(const char *clientId, const char *clientSecret)
{
	StringInfoData src;

	initStringInfo(&src);
	appendStringInfo(&src, "%s:%s", clientId, clientSecret);

	/* dst length per RFC: 4 * ceil(n/3) + 1 for '\0' */
	int			srcLen = (int) strlen(src.data);
	int			dstLen = 4 * ((srcLen + 2) / 3) + 1;

	char	   *dst = (char *) palloc(dstLen);
#if PG_VERSION_NUM >= 180000
	int			out = pg_b64_encode((uint8 *) src.data, srcLen, dst, dstLen);
#else
	int			out = pg_b64_encode(src.data, srcLen, dst, dstLen);
#endif

	if (out < 0)
		ereport(ERROR, (errmsg("failed to base64-encode client credentials")));

	dst[out] = '\0';
	return dst;
}


IcebergCatalogType
GetIcebergCatalogType(Oid relationId)
{
	if (!IsPgLakeIcebergForeignTableById(relationId))
		return NOT_ICEBERG_TABLE;

	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;

	bool		hasRestCatalogOption = HasRestCatalogTableOption(options);
	bool		hasObjectStoreCatalogOption = HasObjectStoreCatalogTableOption(options);
	bool		hasReadOnlyOption = HasReadOnlyOption(options);

	if (hasRestCatalogOption && hasReadOnlyOption)
	{
		return REST_CATALOG_READ_ONLY;
	}
	else if (hasRestCatalogOption && !hasReadOnlyOption)
	{
		return REST_CATALOG_READ_WRITE;
	}
	else if (hasObjectStoreCatalogOption && hasReadOnlyOption)
	{
		return OBJECT_STORE_READ_ONLY;
	}
	else if (hasObjectStoreCatalogOption && !hasReadOnlyOption)
	{
		return OBJECT_STORE_READ_WRITE;
	}
	else
	{
		return POSTGRES_CATALOG;
	}
}


/*
* Readable rest catalog tables always use the catalog_table_name option
* as the table name in the external catalog. Writable rest catalog tables
* use the Postgres table name as the catalog table name.
*/
char *
GetRestCatalogTableName(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	if (catalogType == REST_CATALOG_READ_ONLY)
	{
		ForeignTable *foreignTable = GetForeignTable(relationId);
		List	   *options = foreignTable->options;

		char	   *catalogTableName = GetStringOption(options, "catalog_table_name", false);

		/* user provided the custom catalog table name */
		if (!catalogTableName)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("catalog_table_name option is required for rest catalog iceberg tables")));

		return catalogTableName;
	}
	else
	{
		/* for writable rest catalog tables, we use the Postgres table name */
		return get_rel_name(relationId);
	}
}


/*
* Readable rest catalog tables always use the catalog_namespace option
* as the namespace in the external catalog. Writable rest catalog tables
* use the Postgres schema name as the namespace.
*/
char *
GetRestCatalogNamespace(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	if (catalogType == REST_CATALOG_READ_ONLY)
	{

		ForeignTable *foreignTable = GetForeignTable(relationId);
		List	   *options = foreignTable->options;

		char	   *catalogNamespace = GetStringOption(options, "catalog_namespace", false);

		/* user provided the custom catalog namespace */
		if (!catalogNamespace)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("catalog_namespace option is required for rest catalog iceberg tables")));

		return catalogNamespace;
	}
	else
	{
		/* for writable rest catalog tables, we use the Postgres schema name */
		return get_namespace_name(get_rel_namespace(relationId));
	}
}

bool
HasRestCatalogTableOption(List *options)
{
	char	   *catalog = GetStringOption(options, "catalog", false);

	return catalog ? strncasecmp(catalog, "rest", strlen("rest")) == 0 : false;
}


bool
HasReadOnlyOption(List *options)
{
	char	   *readOnly = GetStringOption(options, "read_only", false);

	return readOnly ? strncasecmp(readOnly, "true", strlen("true")) == 0 : false;
}


/*
* Readable rest catalog tables always use the catalog_name option
* as the catalog name in the external catalog. Writable rest catalog tables
* use the current database name as the catalog name.
*/
char *
GetRestCatalogName(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	if (catalogType == REST_CATALOG_READ_ONLY)
	{

		Assert(GetIcebergCatalogType(relationId) == REST_CATALOG_READ_ONLY ||
			   GetIcebergCatalogType(relationId) == REST_CATALOG_READ_WRITE);

		ForeignTable *foreignTable = GetForeignTable(relationId);
		List	   *options = foreignTable->options;

		char	   *catalogName = GetStringOption(options, "catalog_name", false);

		/* user provided the custom catalog name */
		if (!catalogName)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("catalog_name option is required for rest catalog iceberg tables")));

		return catalogName;
	}

	return get_database_name(MyDatabaseId);
}
