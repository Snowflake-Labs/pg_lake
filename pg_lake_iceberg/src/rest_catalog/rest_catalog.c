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

#include <inttypes.h>

#include "common/base64.h"
#include "commands/dbcommands.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pg_extension_base/base_workers.h"
#include "pg_lake/http/http_client.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/temporal_utils.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/url_encode.h"
#include "pg_lake/util/rel_utils.h"


/* determined by GUC */
char	   *RestCatalogHost = "http://localhost:8181";
char	   *RestCatalogOauthHostPath = "";
char	   *RestCatalogClientId = NULL;
char	   *RestCatalogClientSecret = NULL;


/*
* Should always be accessed via GetRestCatalogAccessToken()
*/
char	   *RestCatalogAccessToken = NULL;
TimestampTz RestCatalogAccessTokenExpiry = 0;

static char *GetRestCatalogAccessToken(bool forceRefreshToken);
static void FetchRestCatalogAccessToken(char **accessToken, int *expiresIn);
static void CreateNamespaceOnRestCatalog(const char *catalogName, const char *namespaceName);
static char *EncodeBasicAuth(const char *clientId, const char *clientSecret);
static char *JsonbGetStringByPath(const char *jsonb_text, int nkeys,...);
static List *GetHeadersWithAuth(void);
static char *AppendIcebergPartitionSpecForRestCatalog(List *partitionSpecs);

/*
* StartStageRestCatalogIcebergTableCreate stages the creation of an iceberg table
* in the rest catalog. On any failure, an error is raised. If the table exists,
* an error is raised as well.
*
* As per REST catalog spec, we need to provide an empty schema when creating
* a table. The schema will be updated when we make this table visible/committed.
* The main reason for staging early is to be able to get the vended credentials
* for writable tables.
*/
void
StartStageRestCatalogIcebergTableCreate(Oid relationId)
{
	const char *relationName = GetRestCatalogTableName(relationId);

	StringInfo	body = makeStringInfo();

	appendStringInfoChar(body, '{');	/* start body */
	appendJsonString(body, "name", relationName);

	appendStringInfoString(body, ", ");
	appendJsonKey(body, "schema");

	appendStringInfoChar(body, '{');	/* start schema object */

	appendJsonString(body, "type", "struct");
	appendStringInfoString(body, ", ");
	appendJsonKey(body, "fields");
	appendStringInfoString(body, "[]"); /* empty fields array, we don't know
										 * the schema yet */

	appendStringInfoChar(body, '}');	/* close schema object */
	appendStringInfoString(body, ", ");

	appendJsonString(body, "stage-create", "true");

	appendStringInfoChar(body, '}');	/* close body */

	const char *catalogName = GetRestCatalogName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);

	char	   *postUrl =
		psprintf(REST_CATALOG_TABLES, RestCatalogHost,
				 URLEncodePath(catalogName), URLEncodePath(namespaceName));
	List	   *headers = PostHeadersWithAuth();

	/*
	 * TODO: Should we make this configurable? Some object stores may require
	 * different headers or authentication methods. TODO: We currently do not
	 * use vended credentials, but should we?
	 */
	char	   *vendedCreds = pstrdup("X-Iceberg-Access-Delegation: vended-credentials");

	headers = lappend(headers, vendedCreds);

	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_POST, postUrl, body->data, headers);

	if (httpResult.status != 200)
	{
		ReportHTTPError(httpResult, ERROR);
	}
}


/*
* FinishStageRestCatalogIcebergTableCreateRestRequest creates the REST catalog
* request to finalize the staging of an iceberg table creation in the rest
* catalog.
*/
char *
FinishStageRestCatalogIcebergTableCreateRestRequest(Oid relationId, DataFileSchema * dataFileSchema, List *partitionSpecs)
{
	StringInfo	body = makeStringInfo();

	appendStringInfoChar(body, '{');

	appendJsonKey(body, "requirements");
	appendStringInfoChar(body, '[');	/* start requirements array */
	appendStringInfoChar(body, '{');	/* start requirements element */

	appendJsonString(body, "type", "assert-create");

	appendStringInfoChar(body, '}');	/* close requirements element */
	appendStringInfoChar(body, ']');	/* close requirements array */

	appendStringInfoChar(body, ',');

	appendJsonKey(body, "updates");
	appendStringInfoChar(body, '[');	/* start updates array */
	appendStringInfoChar(body, '{');	/* start updates element */

	appendJsonString(body, "action", "add-schema");

	appendStringInfoChar(body, ',');

	int			lastColumnId = 0;
	IcebergTableSchema *newSchema =
		RebuildIcebergSchemaFromDataFileSchema(relationId, dataFileSchema, &lastColumnId);
	int			schemaCount = 1;

	AppendIcebergTableSchemaForRestCatalog(body, newSchema, schemaCount);
	appendStringInfoChar(body, '}');	/* close updates element */

	appendStringInfoChar(body, ',');
	appendStringInfoChar(body, '{');	/* start add-sort-order */
	appendJsonString(body, "action", "add-sort-order");
	appendStringInfoString(body, ", ");
	appendJsonKey(body, "sort-order");
	appendStringInfoChar(body, '{');	/* start sort-order object */
	appendJsonInt32(body, "order-id", 0);
	appendStringInfoString(body, ", ");
	appendJsonKey(body, "fields");
	appendStringInfoString(body, "[]"); /* empty fields array */
	appendStringInfoChar(body, '}');	/* finish sort-order object */
	appendStringInfoChar(body, '}');	/* finish add-sort-order */
	appendStringInfoChar(body, ',');
	appendStringInfoChar(body, '{');	/* start add-sort-order */
	appendJsonString(body, "action", "set-default-sort-order");
	appendStringInfoString(body, ", ");
	appendJsonInt32(body, "sort-order-id", 0);
	appendStringInfoChar(body, '}');	/* finish add-sort-order */

	appendStringInfoString(body, ", ");
	appendStringInfoChar(body, '{');	/* start set-location */
	appendJsonString(body, "action", "set-location");
	appendStringInfoChar(body, ',');

	/* construct location */
	StringInfo	location = makeStringInfo();
	const char *catalogName = GetRestCatalogName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);
	const char *relationName = GetRestCatalogTableName(relationId);

	appendStringInfo(location, "%s/%s/%s/%s/%d", IcebergDefaultLocationPrefix, catalogName, namespaceName, relationName, relationId);
	appendJsonString(body, "location", location->data);
	appendStringInfoChar(body, '}');	/* end set-location */

	/* add partition spec */
	appendStringInfoChar(body, ',');

	ListCell   *partitionSpecCell = NULL;

	foreach(partitionSpecCell, partitionSpecs)
	{
		IcebergPartitionSpec *spec = (IcebergPartitionSpec *) lfirst(partitionSpecCell);

		appendStringInfoChar(body, '{');	/* start add-partition-spec */
		appendJsonString(body, "action", "add-spec");
		appendStringInfoString(body, ", ");

		appendStringInfoString(body, AppendIcebergPartitionSpecForRestCatalog(list_make1(spec)));

		appendStringInfoChar(body, '}');	/* finish add-partition-spec */
		appendStringInfoString(body, ", ");
	}

	if (list_length(partitionSpecs) == 0)
		appendStringInfoChar(body, ',');

	appendStringInfoChar(body, '{');	/* start set-default-spec */
	appendJsonString(body, "action", "set-default-spec");
	appendStringInfoString(body, ", ");
	appendJsonInt32(body, "spec-id", -1);	/* -1 means latest */
	appendStringInfoChar(body, '}');	/* finish set-default-spec */
	appendStringInfoChar(body, ']');	/* end updates array */
	appendStringInfoChar(body, '}');

	return body->data;
}


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
	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_GET, getUrl, NULL, GetHeadersWithAuth());

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
	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_GET, getUrl, NULL, GetHeadersWithAuth());


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
		psprintf(REST_CATALOG_TABLE,
				 RestCatalogHost, URLEncodePath(restCatalogName), URLEncodePath(namespaceName), URLEncodePath(relationName));

	List	   *headers = GetHeadersWithAuth();
	HttpResult	hr = SendRequestToRestCatalog(HTTP_GET, getUrl, NULL, headers);

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

	HttpResult	httpResult = SendRequestToRestCatalog(HTTP_POST, postUrl, body.data, PostHeadersWithAuth());

	if (httpResult.status != 200)
	{
		ReportHTTPError(httpResult, ERROR);
	}
}

/*
* Creates the headers for a POST request with authentication.
*/
List *
PostHeadersWithAuth(void)
{
	bool		forceRefreshToken = false;

	return list_make3(psprintf("Authorization: Bearer %s", GetRestCatalogAccessToken(forceRefreshToken)),
					  pstrdup("Accept: application/json"),
					  pstrdup("Content-Type: application/json"));
}



/*
* Creates the headers for a DELETE request with authentication.
*/
List *
DeleteHeadersWithAuth(void)
{
	bool		forceRefreshToken = false;

	return list_make1(psprintf("Authorization: Bearer %s", GetRestCatalogAccessToken(forceRefreshToken)));
}



/*
* Creates the headers for a GET request with authentication.
*/
static List *
GetHeadersWithAuth(void)
{
	bool		forceRefreshToken = false;

	return list_make2(psprintf("Authorization: Bearer %s", GetRestCatalogAccessToken(forceRefreshToken)),
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
void
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
* Gets an access token from rest catalog using client credentials that are
* configured via GUC variables. Caches the token until it is about to expire.
*/
static char *
GetRestCatalogAccessToken(bool forceRefreshToken)
{
	/*
	 * Calling initial time or token will expire in 1 minute, fetch a new
	 * token.
	 */
	TimestampTz now = GetCurrentTimestamp();
	const int	MINUTE_IN_MSECS = 60 * 1000;

	if (forceRefreshToken || RestCatalogAccessTokenExpiry == 0 ||
		!TimestampDifferenceExceeds(now, RestCatalogAccessTokenExpiry, MINUTE_IN_MSECS))
	{
		if (RestCatalogAccessToken)
			pfree(RestCatalogAccessToken);

		char	   *accessToken = NULL;
		int			expiresIn = 0;

		FetchRestCatalogAccessToken(&accessToken, &expiresIn);

		/* Polaris default is 3600 seconds */
		Assert(expiresIn > 0);

		RestCatalogAccessToken = MemoryContextStrdup(TopMemoryContext, accessToken);
		RestCatalogAccessTokenExpiry = now + (int64_t) expiresIn * 1000000; /* expiresIn is in
																			 * seconds */
	}

	Assert(RestCatalogAccessToken != NULL);

	return RestCatalogAccessToken;
}


/*
* Fetches an access token from rest catalog using client credentials that are
* configured via GUC variables.
*/
static void
FetchRestCatalogAccessToken(char **accessToken, int *expiresIn)
{
	if (!RestCatalogHost || !*RestCatalogHost)
		ereport(ERROR, (errmsg("pg_lake_iceberg.rest_catalog_host should be set")));
	if (!RestCatalogClientId || !*RestCatalogClientId)
		ereport(ERROR, (errmsg("pg_lake_iceberg.rest_catalog_client_id should be set")));
	if (!RestCatalogClientSecret || !*RestCatalogClientSecret)
		ereport(ERROR, (errmsg("pg_lake_iceberg.rest_catalog_client_secret should be set")));

	char	   *accessTokenUrl = RestCatalogOauthHostPath;

	/*
	 * if pg_lake_iceberg.rest_catalog_oauth_host_path is not set, use
	 * Polaris' default oauth token endpoint
	 */
	if (*accessTokenUrl == '\0')
		accessTokenUrl = psprintf(REST_CATALOG_AUTH_TOKEN_PATH, RestCatalogHost);

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
	HttpResult	httpResponse = SendRequestToRestCatalog(HTTP_POST, accessTokenUrl, body, headers);

	if (httpResponse.status != 200)
		ereport(ERROR,
				(errmsg("Rest Catalog OAuth token request failed (HTTP %ld)", httpResponse.status),
				 httpResponse.body ? errdetail_internal("%s", httpResponse.body) : 0));

	if (!httpResponse.body || !*httpResponse.body)
		ereport(ERROR, (errmsg("Rest Catalog OAuth token response body is empty")));

	*accessToken = JsonbGetStringByPath(httpResponse.body, 1, "access_token");
	*expiresIn = atoi(JsonbGetStringByPath(httpResponse.body, 1, "expires_in"));
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
			if (!(val->type == jbvString || val->type == jbvNumeric))
				ereport(ERROR, (errmsg("leaf \"%s\" is not a string or numeric", key)));

			va_end(variableArgList);

			if (val->type == jbvString)
				return pnstrdup(val->val.string.val, val->val.string.len);
			else
			{
				bool		haveError = false;

				int			valInt = numeric_int4_opt_error(val->val.numeric,
															&haveError);

				if (haveError)
				{
					ereport(ERROR, (errmsg("integer out of range")));
				}

				return psprintf("%d", valInt);
			}
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


/*
* Appends the given IcebergPartitionSpec list as JSON to the given StringInfo, specifically
* for use in Rest Catalog requests.
*/
static char *
AppendIcebergPartitionSpecForRestCatalog(List *partitionSpecs)
{
	StringInfo	command = makeStringInfo();

	ListCell   *partitionSpecCell = NULL;

	foreach(partitionSpecCell, partitionSpecs)
	{
		IcebergPartitionSpec *spec = (IcebergPartitionSpec *) lfirst(partitionSpecCell);

		appendJsonKey(command, "spec");
		appendStringInfoString(command, "{");

		/* append spec-id */
		appendJsonInt32(command, "spec-id", spec->spec_id);

		/* Append fields */
		appendStringInfoString(command, ", \"fields\":");
		AppendIcebergPartitionSpecFields(command, spec->fields, spec->fields_length);

		appendStringInfoString(command, "}");
	}
	return command->data;
}


/*
* GetAddSnapshotCatalogRequest creates a RestCatalogRequest to add a snapshot
* to the rest catalog for the given new snapshot.
*/
RestCatalogRequest *
GetAddSnapshotCatalogRequest(IcebergSnapshot * newSnapshot, Oid relationId)
{
	StringInfo	body = makeStringInfo();

	appendStringInfoString(body,
						   "{\"action\":\"add-snapshot\",\"snapshot\":{");

	appendStringInfo(body, "\"snapshot-id\":%" PRId64, newSnapshot->snapshot_id);
	if (newSnapshot->parent_snapshot_id > 0)
		appendStringInfo(body, ",\"parent-snapshot-id\":%" PRId64, newSnapshot->parent_snapshot_id);

	appendStringInfo(body, ",\"sequence-number\":%" PRId64, newSnapshot->sequence_number);
	appendStringInfo(body, ",\"timestamp-ms\":%ld", (long) (PostgresTimestampToIcebergTimestampMs()));	/* coarse ms */
	appendStringInfo(body, ",\"manifest-list\":\"%s\"", newSnapshot->manifest_list);
	appendStringInfoString(body, ",\"summary\":{\"operation\": \"append\"}");
	appendStringInfo(body, ",\"schema-id\":%d", newSnapshot->schema_id);
	appendStringInfoString(body, "}}, ");	/* end add-snapshot */

	appendStringInfo(body, "{\"action\":\"set-snapshot-ref\", \"type\":\"branch\", \"ref-name\":\"main\", \"snapshot-id\":%lld}", newSnapshot->snapshot_id);

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_ADD_SNAPSHOT;
	request->body = body->data;

	return request;
}


/*
 * GetAddSchemaCatalogRequest creates a RestCatalogRequest that adds a schema
 * to the table and sets it as the current schema (schema-id = -1 means
 * "the last added schema" per the REST spec).
 */
RestCatalogRequest *
GetAddSchemaCatalogRequest(Oid relationId, DataFileSchema * dataFileSchema)
{
	StringInfo	body = makeStringInfo();

	/* add-schema */
	appendStringInfoString(body, "{\"action\":\"add-schema\",");

	int			lastColumnId = 0;
	IcebergTableSchema *newSchema =
		RebuildIcebergSchemaFromDataFileSchema(relationId, dataFileSchema, &lastColumnId);

	int			schemaCount = 1;

	AppendIcebergTableSchemaForRestCatalog(body, newSchema, schemaCount);

	/* set-current-schema to the one we just added */
	appendStringInfoString(body, "}, {\"action\":\"set-current-schema\",\"schema-id\":-1}");

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_ADD_SCHEMA;
	request->body = body->data;

	return request;
}

/*
 * GetSetCurrentSchemaCatalogRequest creates a RestCatalogRequest that sets
 * the current schema to the given schema ID.
 */
RestCatalogRequest *
GetSetCurrentSchemaCatalogRequest(Oid relationId, int32_t schemaId)
{
	StringInfo	body = makeStringInfo();

	/* set-current-schema to the given schema ID */
	appendStringInfo(body, "{\"action\":\"set-current-schema\",\"schema-id\":%d}", schemaId);

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_SET_CURRENT_SCHEMA;
	request->body = body->data;

	return request;
}


/*
 * GetAddPartitionCatalogRequest creates a RestCatalogRequest that adds a
 * partition spec and sets it as the default (spec-id = -1 means "last added").
 */
RestCatalogRequest *
GetAddPartitionCatalogRequest(Oid relationId, List *partitionSpecs)
{
	StringInfo	body = makeStringInfo();

	/* add-spec */
	appendStringInfoString(body, "{\"action\":\"add-spec\",");

	char	   *bodyPart = AppendIcebergPartitionSpecForRestCatalog(partitionSpecs);

	appendStringInfoString(body, bodyPart);
	appendStringInfoChar(body, '}');

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_ADD_PARTITION;
	request->body = body->data;

	return request;
}


/*
 * GetAddPartitionCatalogRequest creates a RestCatalogRequest that adds a
 * partition spec and sets it as the default (spec-id = -1 means "last added").
 */
RestCatalogRequest *
GetSetPartitionDefaultIdCatalogRequest(Oid relationId, int specId)
{
	StringInfo	body = makeStringInfo();

	/* set-default-spec to the one we just added */
	appendStringInfo(body, "{\"action\":\"set-default-spec\",\"spec-id\":%d}", specId);

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_SET_DEFAULT_PARTITION_ID;
	request->body = body->data;

	return request;
}


/*
 * GetRemoveSnapshotCatalogRequest creates a RestCatalogRequest that removes
 * a list of snapshots from the REST catalog.
 */
RestCatalogRequest *
GetRemoveSnapshotCatalogRequest(List *removedSnapshotIds, Oid relationId)
{
	StringInfo	body = makeStringInfo();
	bool		first = true;

	appendStringInfoString(body,
						   "{\"action\":\"remove-snapshots\",\"snapshot-ids\":[");
	ListCell   *lc;

	foreach(lc, removedSnapshotIds)
	{
		int64_t		snapshotId = *((int64_t *) lfirst(lc));

		if (!first)
			appendStringInfoChar(body, ',');

		appendStringInfo(body, "%" PRId64, snapshotId);

		first = false;
	}

	appendStringInfoString(body, "]}");

	RestCatalogRequest *request = palloc0(sizeof(RestCatalogRequest));

	request->relationId = relationId;
	request->operationType = REST_CATALOG_REMOVE_SNAPSHOT;
	request->body = body->data;

	return request;
}


/*
 * SendRequestToRestCatalog sends an HTTP request to the rest catalog
 * with retry logic for retriable errors, attempting up to MAX_HTTP_RETRY_FOR_REST_CATALOG
 * times.
 */
HttpResult
SendRequestToRestCatalog(HttpMethod method, const char *url, const char *body, List *headers)
{
	const int	MAX_HTTP_RETRY_FOR_REST_CATALOG = 3;

	return SendHttpRequestWithRetry(method, url, body, headers, ShouldRetryRequestToRestCatalog, MAX_HTTP_RETRY_FOR_REST_CATALOG);
}


/*
 * ShouldRetryRequestToRestCatalog checks if the given HTTP result status is retriable.
 * If it is retriable, it performs necessary actions (like sleeping or refreshing token)
 * and returns true. Otherwise, it returns false.
 */
bool
ShouldRetryRequestToRestCatalog(long status, int maxRetry, int retryNo)
{
	if (retryNo > maxRetry)
		return false;

	const int	TOO_MANY_REQUEST_STATUS = 429;
	const int	SERVER_UNAVAILABLE_STATUS = 503;
	const int	TOKEN_EXPIRED_STATUS = 419;

	/* too many request, wait some time */
	if (status == TOO_MANY_REQUEST_STATUS)
	{
		int			baseMs = 500;

		/*
		 * LightSleep reacts to signals, and can easily throw an error (e.g.,
		 * cancel backend). This function can be called at post-commit hook,
		 * so normally we wouldn't want any errors to happen, but then
		 * Postgres already prevents post-commit backends to receive signals.
		 */
		LightSleep(LinearBackoffSleepMs(baseMs, retryNo));
		return true;
	}

	/* server unavailable, lets wait a bit more */
	else if (status == SERVER_UNAVAILABLE_STATUS)
	{
		int			baseMs = 5000;

		LightSleep(LinearBackoffSleepMs(baseMs, retryNo));
		return true;
	}

	/* token expired, retry after refreshing token */
	else if (status == TOKEN_EXPIRED_STATUS)
	{
		/*
		 * We normally refresh the token only when it is about to expire
		 * (forceRefreshToken = false), just 1 minute before the expiration
		 * for each request. Retry logic makes it safer by ensuring we get a
		 * fresh token for unforeseen circumstances.
		 */
		bool		forceRefreshToken = true;

		GetRestCatalogAccessToken(forceRefreshToken);
		return true;
	}

	/* successful or other error, no retry */
	return false;
}
