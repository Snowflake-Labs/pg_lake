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
 * REST catalog API operations.
 *
 * Three flavours of work live here, all running on top of the
 * SendRequestToRestCatalog transport (rest_catalog_http.c) and the
 * Get/Post/Delete auth-header builders (rest_catalog_auth.c):
 *
 *  1. Synchronous REST verbs that hit the catalog at call time --
 *     table create staging, namespace register/check, metadata-
 *     location lookup.
 *
 *  2. Table identity helpers (GetRestCatalogTableName/Namespace/Name)
 *     that read pg_foreign_table options to derive the catalog/
 *     namespace/table tuple.
 *
 *  3. Request body builders (Get*CatalogRequest) that construct the
 *     JSON action records merged into the per-transaction REST commit
 *     emitted by the metadata-change pipeline.
 */

#include <inttypes.h>

#include "postgres.h"
#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "common/hashfn.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"

#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/temporal_utils.h"
#include "pg_lake/util/url_encode.h"


static void CreateNamespaceOnRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName);
static char *AppendIcebergPartitionSpecForRestCatalog(List *partitionSpecs);


/*
 * Per-table vended credentials cache.
 *
 * Keyed by (serverOid, prefixHash) so each Iceberg table backed by a
 * REST catalog gets its own cache slot.  Entries expire based on the
 * REST catalog's credential TTL.  The cache is invalidated on
 * ALTER/DROP SERVER alongside the token cache.
 */
typedef struct VendedCredentialsCacheKey
{
	Oid			serverOid;
	uint32		prefixHash;
}			VendedCredentialsCacheKey;

typedef struct VendedCredentialsCacheEntry
{
	VendedCredentialsCacheKey key;	/* hash key */
	VendedCredentials *credentials;
	TimestampTz expiryTime;
}			VendedCredentialsCacheEntry;

static HTAB *VendedCredsCache = NULL;
static MemoryContext VendedCredsCacheCtx = NULL;

/*
 * Conservative TTL for vended credentials when the REST catalog does
 * not provide an explicit expiry.  AWS STS temporary credentials
 * typically last 1 hour; we default to 55 minutes to refresh early.
 */
#define VENDED_CREDS_DEFAULT_TTL_SECS 3300

static VendedCredentials * ExtractVendedCredentials(const char *responseBody,
													RestCatalogOptions * opts);
static void InitVendedCredsCacheIfNeeded(void);
static void StoreVendedCredentialsInCache(VendedCredentials * creds,
										  const char *restCatalogName,
										  const char *namespaceName,
										  const char *relationName);
static VendedCredentials * LookupVendedCredentialsInCache(Oid serverOid,
														  const char *restCatalogName,
														  const char *namespaceName,
														  const char *tableName);


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

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	char	   *postUrl =
		psprintf(REST_CATALOG_TABLES, opts->host,
				 URLEncodePath(catalogName), URLEncodePath(namespaceName));
	List	   *headers = PostHeadersWithAuth(opts);

	if (opts->enableVendedCredentials)
	{
		char	   *vendedCreds = pstrdup("X-Iceberg-Access-Delegation: vended-credentials");

		headers = lappend(headers, vendedCreds);
	}

	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_POST, postUrl, body->data,
													  headers);

	if (httpResult.status != 200)
	{
		ReportHTTPError(httpResult, ERROR);
	}

	/*
	 * If the stage-create response includes vended credentials, cache them
	 * for subsequent writes to this table's S3 prefix.
	 */
	if (opts->enableVendedCredentials && httpResult.body != NULL)
	{
		VendedCredentials *creds = ExtractVendedCredentials(httpResult.body, opts);

		StoreVendedCredentialsInCache(creds, catalogName, namespaceName,
									  relationName);
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
	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	appendStringInfo(location, "%s/%s/%s/%s/%d", opts->locationPrefix, catalogName, namespaceName, relationName, relationId);
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
RegisterNamespaceToRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName)
{
	/*
	 * First, we need to check if the namespace already exists in Rest Catalog
	 * via a GET request.
	 */
	char	   *getUrl =
		psprintf(REST_CATALOG_NAMESPACE_NAME,
				 opts->host, URLEncodePath(catalogName),
				 URLEncodePath(namespaceName));
	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL,
													  GetHeadersWithAuth(opts));

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
				CreateNamespaceOnRestCatalog(opts, catalogName, namespaceName);
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

				if (serverAllowedLocation)
				{
					const char *defaultAllowedLocation =
						psprintf("%s/%s/%s", opts->locationPrefix, catalogName, namespaceName);


					/*
					 * Compare by ignoring the trailing `/` char that the
					 * server might have for internal iceberg tables. For
					 * external ones, we don't have any control over.
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
ErrorIfRestNamespaceDoesNotExist(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName)
{
	/*
	 * First, we need to check if the namespace already exists in Rest Catalog
	 * via a GET request.
	 */
	char	   *getUrl =
		psprintf(REST_CATALOG_NAMESPACE_NAME,
				 opts->host, URLEncodePath(catalogName),
				 URLEncodePath(namespaceName));
	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL,
													  GetHeadersWithAuth(opts));

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

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	return GetMetadataLocationFromRestCatalog(opts, restCatalogName, namespaceName, relationName);
}


/*
 * StoreVendedCredentialsInCache caches freshly-extracted vended
 * credentials so subsequent GetVendedCredentialsForRelation calls
 * can return them without a redundant REST round-trip.
 */
static void
StoreVendedCredentialsInCache(VendedCredentials * creds,
							  const char *restCatalogName,
							  const char *namespaceName,
							  const char *relationName)
{
	if (creds == NULL)
		return;

	char	   *tablePrefix = psprintf("%s/%s/%s",
									   restCatalogName, namespaceName,
									   relationName);

	VendedCredentialsCacheKey cacheKey;

	memset(&cacheKey, 0, sizeof(cacheKey));
	cacheKey.serverOid = creds->serverOid;
	cacheKey.prefixHash = hash_bytes((const unsigned char *) tablePrefix,
									 strlen(tablePrefix));

	InitVendedCredsCacheIfNeeded();

	bool		found = false;
	VendedCredentialsCacheEntry *entry =
		hash_search(VendedCredsCache, &cacheKey, HASH_ENTER, &found);

	MemoryContext oldCtx = MemoryContextSwitchTo(VendedCredsCacheCtx);
	VendedCredentials *cached = palloc0(sizeof(VendedCredentials));

	cached->accessKeyId = pstrdup(creds->accessKeyId);
	cached->secretAccessKey = pstrdup(creds->secretAccessKey);
	cached->sessionToken = creds->sessionToken ?
		pstrdup(creds->sessionToken) : NULL;
	cached->region = creds->region ?
		pstrdup(creds->region) : NULL;
	cached->scope = creds->scope ?
		pstrdup(creds->scope) : NULL;
	cached->serverOid = creds->serverOid;
	cached->fetchedAt = creds->fetchedAt;

	MemoryContextSwitchTo(oldCtx);

	entry->credentials = cached;
	entry->expiryTime = GetCurrentTimestamp() +
		(int64) VENDED_CREDS_DEFAULT_TTL_SECS * 1000000;

	pfree(tablePrefix);
}


/*
 * LoadTableFromRestCatalog issues a GET loadTable request to the REST
 * catalog, requesting vended credentials if enabled.  Returns a result
 * struct containing the metadata location and optional vended storage
 * credentials extracted from the response's "config" map.
 *
 * If vended credentials are obtained, they are also stored in the
 * vended credentials cache so that GetVendedCredentialsForRelation
 * can return them without a second REST round-trip.
 */
RestCatalogLoadTableResult
LoadTableFromRestCatalog(RestCatalogOptions * opts, const char *restCatalogName,
						 const char *namespaceName, const char *relationName)
{
	char	   *getUrl =
		psprintf(REST_CATALOG_TABLE,
				 opts->host, URLEncodePath(restCatalogName),
				 URLEncodePath(namespaceName),
				 URLEncodePath(relationName));

	List	   *headers = GetHeadersWithAuth(opts);

	if (opts->enableVendedCredentials)
		headers = lappend(headers,
						  pstrdup("X-Iceberg-Access-Delegation: vended-credentials"));

	HttpResult	hr = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL, headers);

	if (hr.status != 200)
		ReportHTTPError(hr, ERROR);

	RestCatalogLoadTableResult result = {0};

	result.metadataLocation = JsonbGetStringByPath(hr.body, 1, "metadata-location");
	if (result.metadataLocation == NULL)
		ereport(ERROR,
				(errmsg("key \"metadata-location\" missing in json response")));

	if (opts->enableVendedCredentials)
	{
		result.vendedCredentials = ExtractVendedCredentials(hr.body, opts);

		StoreVendedCredentialsInCache(result.vendedCredentials,
									  restCatalogName, namespaceName,
									  relationName);
	}

	return result;
}


/*
 * GetMetadataLocationFromRestCatalog is the legacy API that returns only
 * the metadata location string.  Callers that also need vended
 * credentials should use LoadTableFromRestCatalog instead.
 */
char *
GetMetadataLocationFromRestCatalog(RestCatalogOptions * opts, const char *restCatalogName, const char *namespaceName, const char *relationName)
{
	RestCatalogLoadTableResult result =
		LoadTableFromRestCatalog(opts, restCatalogName, namespaceName,
								 relationName);

	return result.metadataLocation;
}


/*
 * ExtractVendedCredentials parses S3 vended credentials from the
 * REST catalog loadTable response body's "config" map.
 *
 * Returns a palloc'd VendedCredentials if at minimum the access key
 * and secret are present; NULL otherwise.
 */
static VendedCredentials *
ExtractVendedCredentials(const char *responseBody, RestCatalogOptions * opts)
{
	if (responseBody == NULL || *responseBody == '\0')
		return NULL;

	char	   *accessKeyId =
		JsonbGetOptionalStringByPath(responseBody, 2, "config", "s3.access-key-id");
	char	   *secretAccessKey =
		JsonbGetOptionalStringByPath(responseBody, 2, "config", "s3.secret-access-key");

	if (accessKeyId == NULL || secretAccessKey == NULL)
	{
		elog(DEBUG2, "REST catalog loadTable response did not contain "
			 "vended S3 credentials in config map");
		return NULL;
	}

	VendedCredentials *creds = palloc0(sizeof(VendedCredentials));

	creds->accessKeyId = accessKeyId;
	creds->secretAccessKey = secretAccessKey;
	creds->sessionToken =
		JsonbGetOptionalStringByPath(responseBody, 2, "config", "s3.session-token");
	creds->region =
		JsonbGetOptionalStringByPath(responseBody, 2, "config", "client.region");
	creds->serverOid = opts->serverOid;
	creds->fetchedAt = GetCurrentTimestamp();

	return creds;
}


/*
 * Initialize the vended credentials cache hash table if needed.
 * Shares the invalidation callback registration with the token cache.
 */
static void
InitVendedCredsCacheIfNeeded(void)
{
	if (VendedCredsCache != NULL)
		return;

	if (VendedCredsCacheCtx == NULL)
		VendedCredsCacheCtx = AllocSetContextCreate(CacheMemoryContext,
													"VendedCredsCacheCtx",
													ALLOCSET_DEFAULT_SIZES);

	HASHCTL		ctl;

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(VendedCredentialsCacheKey);
	ctl.entrysize = sizeof(VendedCredentialsCacheEntry);
	ctl.hcxt = VendedCredsCacheCtx;

	VendedCredsCache = hash_create("Vended Credentials Cache",
								   16, &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}


/*
 * InvalidateVendedCredentialsCache drops the entire cache.  Called from
 * the syscache invalidation callback alongside the token cache, and also
 * available for explicit invalidation on table drop.
 */
void
InvalidateVendedCredentialsCache(void)
{
	if (VendedCredsCache == NULL)
		return;

	MemoryContextReset(VendedCredsCacheCtx);
	VendedCredsCache = NULL;
}


/*
 * LookupVendedCredentialsInCache checks the cache for valid vended
 * credentials for the given table path under the given server.
 * Returns the cached credentials or NULL if not found/expired.
 */
static VendedCredentials *
LookupVendedCredentialsInCache(Oid serverOid,
							   const char *restCatalogName,
							   const char *namespaceName,
							   const char *tableName)
{
	char	   *tablePrefix = psprintf("%s/%s/%s",
									   restCatalogName, namespaceName,
									   tableName);

	VendedCredentialsCacheKey cacheKey;

	memset(&cacheKey, 0, sizeof(cacheKey));
	cacheKey.serverOid = serverOid;
	cacheKey.prefixHash = hash_bytes((const unsigned char *) tablePrefix,
									 strlen(tablePrefix));

	pfree(tablePrefix);

	InitVendedCredsCacheIfNeeded();

	bool		found = false;
	VendedCredentialsCacheEntry *entry =
		hash_search(VendedCredsCache, &cacheKey, HASH_FIND, &found);

	if (!found || entry->credentials == NULL)
		return NULL;

	TimestampTz now = GetCurrentTimestamp();
	const int64 FIVE_MINUTES_USEC = (int64) 5 * 60 * 1000000;

	if (entry->expiryTime <= now + FIVE_MINUTES_USEC)
		return NULL;

	return entry->credentials;
}


/*
 * GetVendedCredentialsForRelation returns cached vended credentials
 * for the given REST catalog Iceberg table, or NULL if none are
 * available.
 *
 * Returns NULL if:
 *  - the table is not backed by a REST catalog
 *  - vended credentials are disabled
 *  - no credentials are cached (cache-only lookup, no REST call)
 *
 * The cache is populated as a side effect of LoadTableFromRestCatalog
 * during snapshot creation (the normal read path).  We intentionally
 * do not trigger a REST round-trip here: the caller may be on the
 * modify path (INSERT/UPDATE/DELETE) where an OAuth failure during
 * the loadTable call would abort the statement prematurely.  Falling
 * back to the pre-existing S3 secret is safe in all cases.
 */
VendedCredentials *
GetVendedCredentialsForRelation(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	if (catalogType != REST_CATALOG_READ_ONLY &&
		catalogType != REST_CATALOG_READ_WRITE)
		return NULL;

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	if (!opts->enableVendedCredentials)
		return NULL;

	const char *restCatalogName = GetRestCatalogName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);
	const char *tableName = GetRestCatalogTableName(relationId);

	return LookupVendedCredentialsInCache(opts->serverOid, restCatalogName,
										  namespaceName, tableName);
}


/*
* CreateNamespaceOnRestCatalog creates a namespace on the rest catalog. On any failure,
* an error is raised.
*/
static void
CreateNamespaceOnRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName)
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
		psprintf(REST_CATALOG_NAMESPACE, opts->host,
				 URLEncodePath(catalogName));

	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_POST, postUrl, body.data,
													  PostHeadersWithAuth(opts));

	if (httpResult.status != 200)
	{
		ReportHTTPError(httpResult, ERROR);
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


/*
 * Returns the catalog name to use for REST API calls.
 *
 * Writable tables always use the current database name so that a
 * subsequent ALTER SERVER ? ADD/SET catalog_name cannot silently
 * re-route an existing table to a different REST namespace.
 *
 * Read-only tables always have catalog_name baked into their table
 * options at CREATE TABLE time (inherited from the server option or
 * defaulted to the database name).
 */
char *
GetRestCatalogName(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	Assert(catalogType == REST_CATALOG_READ_ONLY ||
		   catalogType == REST_CATALOG_READ_WRITE);

	if (catalogType == REST_CATALOG_READ_WRITE)
		return get_database_name(MyDatabaseId);

	ForeignTable *foreignTable = GetForeignTable(relationId);
	char	   *catalogName = GetStringOption(foreignTable->options, "catalog_name", false);

	if (catalogName != NULL)
		return catalogName;

	elog(ERROR, "catalog_name missing on read-only REST catalog table %u", relationId);
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
	appendStringInfoString(body, ",\"manifest-list\":");
	appendStringInfoString(body, EscapeJson(newSnapshot->manifest_list));
	appendStringInfoString(body, ",\"summary\":{\"operation\": \"append\"}");
	appendStringInfo(body, ",\"schema-id\":%d", newSnapshot->schema_id);
	appendStringInfoString(body, "}}, ");	/* end add-snapshot */

	appendStringInfo(body, "{\"action\":\"set-snapshot-ref\", \"type\":\"branch\", \"ref-name\":\"main\", \"snapshot-id\":%" PRId64 "}", newSnapshot->snapshot_id);

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
