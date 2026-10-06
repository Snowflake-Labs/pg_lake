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
#include "nodes/miscnodes.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/json/json_utils.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/permissions/roles.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/storage/storage_credentials.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/temporal_utils.h"
#include "pg_lake/util/url_encode.h"


static void CreateNamespaceOnRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName);
static bool RestCatalogExists(RestCatalogOptions * opts, const char *catalogName);
static bool RestNamespaceExists(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName);
static char *UppercaseIdentifierCandidate(const char *name);
static bool TryLoadTableFromRestCatalog(RestCatalogOptions * opts, const char *restCatalogName,
										const char *namespaceName, const char *relationName,
										bool missingOk, RestCatalogLoadTableResult * result);
static char *AppendIcebergPartitionSpecForRestCatalog(List *partitionSpecs);


/*
 * Per-table vended credentials cache.
 *
 * Keyed by (serverOid, userMappingOid, identityHash) so each Iceberg
 * table backed by a REST catalog gets its own cache slot, per principal.
 * The user mapping is part of the key because a catalog vends different
 * credentials to different principals: without it, a backend that
 * changes role mid-session would serve one principal's credentials under
 * the other's name.  The identity hash is 64-bit, matching the secret
 * name; the entry also keeps the identity it was taken of, so a
 * collision is a miss rather than the wrong table's credentials.
 *
 * Entries expire based on the REST catalog's credential TTL.  The cache
 * is invalidated on ALTER/DROP SERVER alongside the token cache.
 */
typedef struct VendedCredentialsCacheKey
{
	Oid			serverOid;
	Oid			userMappingOid;
	uint64		identityHash;
}			VendedCredentialsCacheKey;

typedef struct VendedCredentialsCacheEntry
{
	VendedCredentialsCacheKey key;	/* hash key */
	char	   *identity;		/* what identityHash was taken of */
	List	   *credentials;	/* one per vended scope */
	TimestampTz expiryTime;		/* earliest expiry across the list */
}			VendedCredentialsCacheEntry;

static HTAB *VendedCredsCache = NULL;
static MemoryContext VendedCredsCacheCtx = NULL;

/*
 * The problems with what catalogs vend that this backend has warned
 * about, each named by a key its FirstVendedReport caller built.  Kept
 * in the cache's context, so ALTER or DROP SERVER, which may be what
 * fixes one, lets it be reported afresh.
 */
static List *ReportedVendedProblems = NIL;

/*
 * Conservative TTL for vended credentials when the REST catalog does
 * not provide an explicit expiry.  AWS STS temporary credentials
 * typically last 1 hour; we default to 55 minutes to refresh early.
 */
#define VENDED_CREDS_DEFAULT_TTL_SECS 3300

static List *ExtractVendedCredentials(Jsonb *response,
									  RestCatalogOptions * opts,
									  const char *restCatalogName,
									  const char *namespaceName,
									  const char *relationName);
static bool FirstVendedReport(const char *reportKey);
static void InitVendedCredsCacheIfNeeded(void);
static void FreeCachedVendedCredentials(VendedCredentials * creds);
static void FreeCachedVendedCredentialsList(List *credentials);
static void StoreVendedCredentialsInCache(List *credentials,
										  Oid userMappingOid,
										  const char *restCatalogName,
										  const char *namespaceName,
										  const char *relationName);
static List *LookupVendedCredentialsInCache(Oid serverOid,
											Oid userMappingOid,
											const char *restCatalogName,
											const char *namespaceName,
											const char *tableName);
static char *BuildVendedCredentialsIdentity(const char *restCatalogName,
											const char *namespaceName,
											const char *tableName);
static VendedCredentialsCacheKey BuildVendedCredentialsCacheKey(Oid serverOid,
																Oid userMappingOid,
																const char *identity);


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
		psprintf(REST_CATALOG_TABLES, opts->baseUri,
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
	 * for subsequent writes to this table's storage prefix.
	 */
	if (opts->enableVendedCredentials && httpResult.body != NULL &&
		*httpResult.body != '\0')
	{
		Datum		bodyDatum = DirectFunctionCall1(jsonb_in,
													CStringGetDatum(httpResult.body));
		List	   *credentials =
			ExtractVendedCredentials(DatumGetJsonbP(bodyDatum), opts,
									 catalogName, namespaceName, relationName);

		StoreVendedCredentialsInCache(credentials, opts->userMappingOid,
									  catalogName, namespaceName,
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
 * RestCatalogExists checks whether the catalog exists in the REST catalog
 * server by listing its namespaces.  Per the Iceberg REST spec,
 * GET /v1/{prefix}/namespaces returns 200 (with an array of namespaces,
 * possibly empty) if the prefix/catalog exists, or 404 if the catalog does
 * not exist.
 */
static bool
RestCatalogExists(RestCatalogOptions * opts, const char *catalogName)
{
	char	   *catalogUrl =
		psprintf(REST_CATALOG_NAMESPACE, opts->baseUri,
				 URLEncodePath(catalogName));
	HttpResult	catalogResult =
		SendRequestToRestCatalog(opts, HTTP_GET, catalogUrl, NULL,
								 GetHeadersWithAuth(opts));

	if (catalogResult.status == 200)
		return true;
	else if (catalogResult.status == 404)
		return false;

	ReportHTTPError(catalogResult, ERROR);
	return false;
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
				 opts->baseUri, URLEncodePath(catalogName),
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
				 * A 404 could mean the namespace doesn't exist, or the
				 * catalog itself doesn't exist. Verify that the catalog
				 * exists before attempting to create the namespace.
				 */
				if (!RestCatalogExists(opts, catalogName))
				{
					ereport(ERROR,
							(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
							 errmsg("catalog \"%s\" does not exist in the rest catalog server",
									catalogName),
							 errhint("Create the catalog in the rest catalog server, "
									 "or check the database name (used as the catalog name).")));
				}

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
 * RestNamespaceExists checks whether the namespace exists in the given
 * catalog of the REST catalog server.
 */
static bool
RestNamespaceExists(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName)
{
	char	   *getUrl =
		psprintf(REST_CATALOG_NAMESPACE_NAME,
				 opts->baseUri, URLEncodePath(catalogName),
				 URLEncodePath(namespaceName));
	HttpResult	httpResult = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL,
													  GetHeadersWithAuth(opts));

	if (httpResult.status == 200)
		return true;
	else if (httpResult.status == 404)
		return false;

	ReportHTTPError(httpResult, ERROR);
	return false;
}


/*
 * UppercaseIdentifierCandidate returns the ASCII-uppercase form of name when
 * name has lowercase letters and no uppercase ones, and NULL otherwise.
 * Snowflake stores unquoted identifiers in uppercase, and those are ASCII
 * only, so a lowercase name typed in Postgres usually means the uppercase one.
 */
static char *
UppercaseIdentifierCandidate(const char *name)
{
	bool		hasLowercase = false;

	for (const char *cursor = name; *cursor != '\0'; cursor++)
	{
		if (*cursor >= 'A' && *cursor <= 'Z')
			return NULL;
		if (*cursor >= 'a' && *cursor <= 'z')
			hasLowercase = true;
	}

	if (!hasLowercase)
		return NULL;

	char	   *uppercaseName = pstrdup(name);

	for (char *cursor = uppercaseName; *cursor != '\0'; cursor++)
		*cursor = pg_ascii_toupper((unsigned char) *cursor);

	return uppercaseName;
}


/*
 * ResolveReadOnlyRestCatalogTable loads a table that a read-only REST catalog
 * table is about to attach to. Each of the catalog, namespace and table names
 * is used as given when it exists, and otherwise in uppercase when that
 * exists, in which case the pointer is replaced with the uppercase name so
 * the caller can store the name that matched.
 */
RestCatalogLoadTableResult
ResolveReadOnlyRestCatalogTable(RestCatalogOptions * opts, char **catalogName,
								char **namespaceName, char **tableName)
{
	bool		namespaceExists = RestNamespaceExists(opts, *catalogName, *namespaceName);

	/* a 404 on the namespace can also mean the catalog does not exist */
	if (!namespaceExists && !RestCatalogExists(opts, *catalogName))
	{
		char	   *uppercaseCatalogName = UppercaseIdentifierCandidate(*catalogName);

		if (uppercaseCatalogName == NULL ||
			!RestCatalogExists(opts, uppercaseCatalogName))
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("catalog \"%s\" does not exist in the rest catalog server",
							*catalogName),
					 errhint("Create the catalog in the rest catalog server, "
							 "or check the catalog_name option.")));

		*catalogName = uppercaseCatalogName;
		namespaceExists = RestNamespaceExists(opts, *catalogName, *namespaceName);
	}

	if (!namespaceExists)
	{
		char	   *uppercaseNamespaceName = UppercaseIdentifierCandidate(*namespaceName);

		if (uppercaseNamespaceName == NULL ||
			!RestNamespaceExists(opts, *catalogName, uppercaseNamespaceName))
			ereport(ERROR,
					(errcode(ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
					 errmsg("namespace \"%s\" does not exist in the rest catalog while creating on catalog \"%s\"",
							*namespaceName, *catalogName)));

		*namespaceName = uppercaseNamespaceName;
	}

	RestCatalogLoadTableResult result = {0};
	bool		missingOk = true;

	if (TryLoadTableFromRestCatalog(opts, *catalogName, *namespaceName,
									*tableName, missingOk, &result))
		return result;

	char	   *uppercaseTableName = UppercaseIdentifierCandidate(*tableName);

	if (uppercaseTableName != NULL &&
		TryLoadTableFromRestCatalog(opts, *catalogName, *namespaceName,
									uppercaseTableName, missingOk, &result))
	{
		*tableName = uppercaseTableName;
		return result;
	}

	/* repeat the load as given to report the catalog's own error */
	return LoadTableFromRestCatalog(opts, *catalogName, *namespaceName, *tableName);
}


/*
 * LoadTableFromRestCatalogForIcebergTable performs a loadTable request for
 * an existing relation and returns the whole response, including the inlined
 * metadata document.
 */
RestCatalogLoadTableResult
LoadTableFromRestCatalogForIcebergTable(Oid relationId)
{
	const char *restCatalogName = GetRestCatalogName(relationId);
	const char *relationName = GetRestCatalogTableName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	return LoadTableFromRestCatalog(opts, restCatalogName, namespaceName, relationName);
}


/*
* Gets the metadata location for a relation from the external rest catalog.
*/
char *
GetMetadataLocationForRestCatalogForIcebergTable(Oid relationId)
{
	return LoadTableFromRestCatalogForIcebergTable(relationId).metadataLocation;
}


/*
 * FreeCachedVendedCredentials releases a cache-owned VendedCredentials
 * and every string hanging off it.  Tolerates NULL.
 */
static void
FreeCachedVendedCredentials(VendedCredentials * creds)
{
	if (creds == NULL)
		return;

	if (creds->accessKeyId != NULL)
		pfree(creds->accessKeyId);
	if (creds->secretAccessKey != NULL)
		pfree(creds->secretAccessKey);
	if (creds->sessionToken != NULL)
		pfree(creds->sessionToken);
	if (creds->region != NULL)
		pfree(creds->region);
	if (creds->endpoint != NULL)
		pfree(creds->endpoint);
	if (creds->urlStyle != NULL)
		pfree(creds->urlStyle);
	if (creds->useSsl != NULL)
		pfree(creds->useSsl);
	if (creds->accountName != NULL)
		pfree(creds->accountName);
	if (creds->sasToken != NULL)
		pfree(creds->sasToken);
	if (creds->blobEndpoint != NULL)
		pfree(creds->blobEndpoint);
	if (creds->dfsEndpoint != NULL)
		pfree(creds->dfsEndpoint);
	if (creds->scope != NULL)
		pfree(creds->scope);

	pfree(creds);
}


/*
 * FreeCachedVendedCredentialsList releases a whole cached list.
 */
static void
FreeCachedVendedCredentialsList(List *credentials)
{
	ListCell   *credsCell = NULL;

	foreach(credsCell, credentials)
		FreeCachedVendedCredentials(lfirst(credsCell));

	list_free(credentials);
}


/*
 * BuildVendedCredentialsIdentity names the table a credential belongs
 * to.  Kept whole in the entry as well as hashed into the key, because
 * a hash alone cannot tell a hit from a collision, and the price of
 * getting that wrong is serving one table's credentials for another.
 */
static char *
BuildVendedCredentialsIdentity(const char *restCatalogName,
							   const char *namespaceName,
							   const char *tableName)
{
	return psprintf("%s/%s/%s", restCatalogName, namespaceName, tableName);
}


static VendedCredentialsCacheKey
BuildVendedCredentialsCacheKey(Oid serverOid, Oid userMappingOid,
							   const char *identity)
{
	VendedCredentialsCacheKey cacheKey;

	/* Zero the padding too: the whole struct is hashed as the key. */
	memset(&cacheKey, 0, sizeof(cacheKey));
	cacheKey.serverOid = serverOid;
	cacheKey.userMappingOid = userMappingOid;
	cacheKey.identityHash =
		hash_bytes_extended((const unsigned char *) identity,
							strlen(identity), 0);

	return cacheKey;
}


/*
 * StoreVendedCredentialsInCache caches freshly-extracted vended
 * credentials so a later resolve for the same table can reuse them
 * without a redundant REST round-trip.
 *
 * The whole list shares one expiry: the earliest the catalog stated, so
 * no member is served past its own lifetime.
 */
static void
StoreVendedCredentialsInCache(List *credentials,
							  Oid userMappingOid,
							  const char *restCatalogName,
							  const char *namespaceName,
							  const char *relationName)
{
	if (credentials == NIL)
		return;

	VendedCredentials *first = linitial(credentials);
	char	   *identity = BuildVendedCredentialsIdentity(restCatalogName,
														  namespaceName,
														  relationName);
	VendedCredentialsCacheKey cacheKey =
		BuildVendedCredentialsCacheKey(first->serverOid, userMappingOid,
									   identity);

	InitVendedCredsCacheIfNeeded();

	/*
	 * Own the identity before the entry can reference it, so a failure to
	 * allocate it cannot leave a live entry pointing at freed memory.
	 */
	char	   *ownedIdentity = MemoryContextStrdup(VendedCredsCacheCtx,
													identity);

	pfree(identity);

	bool		found = false;
	VendedCredentialsCacheEntry *entry =
		hash_search(VendedCredsCache, &cacheKey, HASH_ENTER, &found);

	/*
	 * A newly entered entry holds uninitialized memory, so put it in a
	 * consistent state before the allocations below, any of which can throw.
	 * Otherwise the entry survives the error with a garbage credentials
	 * pointer that the lookup's NULL guard happily passes through.
	 */
	if (!found)
	{
		entry->identity = NULL;
		entry->credentials = NIL;
		entry->expiryTime = 0;
	}

	/*
	 * On the vanishing chance that another table hashed to this key, take the
	 * entry over rather than append to it: the lookup compares identities, so
	 * the table left without a cached entry re-fetches instead of being
	 * served these credentials.
	 */
	if (entry->identity != NULL)
		pfree(entry->identity);

	entry->identity = ownedIdentity;

	MemoryContext oldCtx = MemoryContextSwitchTo(VendedCredsCacheCtx);
	List	   *cachedList = NIL;
	ListCell   *credsCell = NULL;
	TimestampTz earliestExpiry = 0;

	foreach(credsCell, credentials)
	{
		VendedCredentials *creds = lfirst(credsCell);
		VendedCredentials *cached = palloc0(sizeof(VendedCredentials));

		cached->provider = creds->provider;
		cached->accessKeyId = creds->accessKeyId ?
			pstrdup(creds->accessKeyId) : NULL;
		cached->secretAccessKey = creds->secretAccessKey ?
			pstrdup(creds->secretAccessKey) : NULL;
		cached->sessionToken = creds->sessionToken ?
			pstrdup(creds->sessionToken) : NULL;
		cached->region = creds->region ?
			pstrdup(creds->region) : NULL;
		cached->endpoint = creds->endpoint ?
			pstrdup(creds->endpoint) : NULL;
		cached->urlStyle = creds->urlStyle ?
			pstrdup(creds->urlStyle) : NULL;
		cached->useSsl = creds->useSsl ?
			pstrdup(creds->useSsl) : NULL;
		cached->accountName = creds->accountName ?
			pstrdup(creds->accountName) : NULL;
		cached->sasToken = creds->sasToken ?
			pstrdup(creds->sasToken) : NULL;
		cached->blobEndpoint = creds->blobEndpoint ?
			pstrdup(creds->blobEndpoint) : NULL;
		cached->dfsEndpoint = creds->dfsEndpoint ?
			pstrdup(creds->dfsEndpoint) : NULL;
		cached->scope = creds->scope ?
			pstrdup(creds->scope) : NULL;
		cached->serverOid = creds->serverOid;
		cached->fetchedAt = creds->fetchedAt;
		cached->expiresAt = creds->expiresAt;

		cachedList = lappend(cachedList, cached);

		if (creds->expiresAt > 0 &&
			(earliestExpiry == 0 || creds->expiresAt < earliestExpiry))
			earliestExpiry = creds->expiresAt;
	}

	MemoryContextSwitchTo(oldCtx);

	/*
	 * Release the copy we are replacing.  The cache context is only reset on
	 * invalidation, and read-only REST tables re-extract on every statement,
	 * so an in-place overwrite leaks a session token per statement.
	 */
	FreeCachedVendedCredentialsList(entry->credentials);

	entry->credentials = cachedList;

	/*
	 * Honor the catalog-provided expiry when present; otherwise fall back to
	 * a conservative default TTL.  The lookup applies an additional
	 * early-refresh margin on top of this.
	 */
	if (earliestExpiry > 0)
		entry->expiryTime = earliestExpiry;
	else
		entry->expiryTime = GetCurrentTimestamp() +
			(int64) VENDED_CREDS_DEFAULT_TTL_SECS * 1000000;
}


/*
 * LoadTableFromRestCatalog issues a GET loadTable request to the REST
 * catalog, requesting vended credentials if enabled.  Returns a result
 * struct containing the metadata location, the inlined table metadata
 * document, and optional vended storage credentials extracted from the
 * response's "config" map.
 *
 * If vended credentials are obtained, they are also stored in the
 * vended credentials cache, so a resolve triggered later in the same
 * statement reuses them instead of issuing a second REST round-trip.
 */
RestCatalogLoadTableResult
LoadTableFromRestCatalog(RestCatalogOptions * opts, const char *restCatalogName,
						 const char *namespaceName, const char *relationName)
{
	RestCatalogLoadTableResult result = {0};
	bool		missingOk = false;

	TryLoadTableFromRestCatalog(opts, restCatalogName, namespaceName,
								relationName, missingOk, &result);

	return result;
}


/*
 * TryLoadTableFromRestCatalog is LoadTableFromRestCatalog that, when
 * missingOk is set, returns false instead of raising an error when the
 * catalog answers 404.
 */
static bool
TryLoadTableFromRestCatalog(RestCatalogOptions * opts, const char *restCatalogName,
							const char *namespaceName, const char *relationName,
							bool missingOk, RestCatalogLoadTableResult * result)
{
	char	   *getUrl =
		psprintf(REST_CATALOG_TABLE,
				 opts->baseUri, URLEncodePath(restCatalogName), URLEncodePath(namespaceName), URLEncodePath(relationName));

	List	   *headers = GetHeadersWithAuth(opts);

	if (opts->enableVendedCredentials)
		headers = lappend(headers,
						  pstrdup("X-Iceberg-Access-Delegation: vended-credentials"));

	HttpResult	hr = SendRequestToRestCatalog(opts, HTTP_GET, getUrl, NULL, headers);

	if (hr.status == 404 && missingOk)
		return false;

	if (hr.status != 200)
		ReportHTTPError(hr, ERROR);

	result->metadataLocation = JsonbGetStringByPath(hr.body, 1, "metadata-location");
	if (result->metadataLocation == NULL)
		ereport(ERROR,
				(errmsg("key \"metadata-location\" missing in json response")));

	/*
	 * A loadTable response carries the table's whole metadata document, and
	 * both the schema and a credential are read out of it a field at a time,
	 * so it is parsed once here and navigated from there.
	 */
	Datum		bodyDatum = DirectFunctionCall1(jsonb_in, CStringGetDatum(hr.body));
	Jsonb	   *body = DatumGetJsonbP(bodyDatum);

	result->metadata = JsonbGetObject(body, "metadata");

	if (opts->enableVendedCredentials)
	{
		result->vendedCredentials = ExtractVendedCredentials(body, opts,
															 restCatalogName,
															 namespaceName,
															 relationName);

		StoreVendedCredentialsInCache(result->vendedCredentials,
									  opts->userMappingOid,
									  restCatalogName, namespaceName,
									  relationName);
	}

	return true;
}


/*
 * LoadRestCatalogMetadataLocation performs a REST loadTable request and
 * returns only the metadata location string.  Despite the historical
 * "get" phrasing this is not a cheap accessor: it issues a network call
 * (and, on the vended-credentials path, populates the credential cache
 * via LoadTableFromRestCatalog).  Callers that also need the vended
 * credentials should call LoadTableFromRestCatalog directly.
 */
char *
LoadRestCatalogMetadataLocation(RestCatalogOptions * opts, const char *restCatalogName, const char *namespaceName, const char *relationName)
{
	RestCatalogLoadTableResult result =
		LoadTableFromRestCatalog(opts, restCatalogName, namespaceName,
								 relationName);

	return result.metadataLocation;
}


/*
 * NormalizeScopePrefix returns a copy of prefix guaranteed to end with a
 * trailing slash.  DuckDB selects a secret by longest-matching SCOPE
 * prefix, so without the trailing slash a scope of ".../t" would also
 * match a sibling table ".../t2".
 */
static char *
NormalizeScopePrefix(const char *prefix)
{
	size_t		len = strlen(prefix);

	if (len > 0 && prefix[len - 1] == '/')
		return pstrdup(prefix);

	return psprintf("%s/", prefix);
}


/*
 * TableRootFromMetadataLocation derives the table's storage directory
 * (with a trailing slash) from a metadata file location such as
 * "s3://bucket/wh/ns/tbl/metadata/00000-uuid.metadata.json", i.e.
 * "s3://bucket/wh/ns/tbl/".  Returns NULL when the location does not
 * follow the ".../metadata/<file>" convention.
 */
static char *
TableRootFromMetadataLocation(const char *metadataLocation)
{
	if (metadataLocation == NULL)
		return NULL;

	const char *needle = "/metadata/";
	const char *found = NULL;
	const char *p = metadataLocation;

	/* Use the last occurrence in case a bucket/prefix also contains it. */
	while ((p = strstr(p, needle)) != NULL)
	{
		found = p;
		p += 1;
	}

	if (found == NULL)
		return NULL;

	/* Keep the '/' that precedes "metadata" so the result ends in '/'. */
	return pnstrdup(metadataLocation, (found - metadataLocation) + 1);
}


/*
 * GetVendedConfigString reads a string value from an Iceberg config
 * map.  When mapKey is NULL, body is the config map itself (leaf is a
 * direct child); otherwise the leaf lives under body->mapKey.
 *
 * A setting the catalog states as "" has stated nothing, and is
 * reported as absent: that keeps NULL the single meaning of "unset"
 * for everything downstream, which decides both what is inherited from
 * the existing secret and what reaches the CREATE SECRET statement.
 */
static char *
GetVendedConfigString(Jsonb *body, const char *mapKey, const char *leafKey)
{
	char	   *value = mapKey == NULL ?
		JsonbGetOptionalString(body, 1, leafKey) :
		JsonbGetOptionalString(body, 2, mapKey, leafKey);

	if (value != NULL && value[0] == '\0')
		return NULL;

	return value;
}


/*
 * ParseVendedExpiresAtMs converts an Iceberg "*-expires-at-ms" value
 * (unix epoch millis) to a timestamp, or returns 0 when there is none to
 * convert, so short-lived credentials are not cached past their real
 * lifetime.
 */
static TimestampTz
ParseVendedExpiresAtMs(const char *expiresMsStr)
{
	if (expiresMsStr == NULL)
		return 0;

	char	   *endptr = NULL;
	long long	expiresMs = strtoll(expiresMsStr, &endptr, 10);

	if (endptr == expiresMsStr || *endptr != '\0' || expiresMs <= 0)
		return 0;

	return (TimestampTz) IcebergTimestampMsToPostgresTimestamp((Timestamp) expiresMs);
}


/*
 * ParseVendedS3CredsFromConfig builds an S3 VendedCredentials from an
 * Iceberg config map (see GetVendedConfigString for the mapKey
 * convention).
 *
 * Returns NULL unless at least the access key and secret are present.
 * The scope field is left unset here; the caller assigns it from the
 * storage-credential prefix or the table location, which an S3
 * credential does not depend on.  The expiry is parsed from
 * "s3.session-token-expires-at-ms" when the catalog provides it.
 */
static VendedCredentials *
ParseVendedS3CredsFromConfig(Jsonb *body, const char *mapKey,
							 const char *scope, Oid serverOid)
{
	char	   *accessKeyId = GetVendedConfigString(body, mapKey, "s3.access-key-id");
	char	   *secretAccessKey = GetVendedConfigString(body, mapKey, "s3.secret-access-key");

	if (accessKeyId == NULL || secretAccessKey == NULL)
		return NULL;

	VendedCredentials *creds = palloc0(sizeof(VendedCredentials));

	creds->accessKeyId = accessKeyId;
	creds->secretAccessKey = secretAccessKey;
	creds->sessionToken = GetVendedConfigString(body, mapKey, "s3.session-token");
	creds->region = GetVendedConfigString(body, mapKey, "client.region");
	if (creds->region == NULL)
		creds->region = GetVendedConfigString(body, mapKey, "s3.region");
	creds->serverOid = serverOid;
	creds->fetchedAt = GetCurrentTimestamp();

	/*
	 * Iceberg states the endpoint as a URL, while DuckDB wants a bare
	 * host[:port] plus a separate USE_SSL.  Split the two apart, so a catalog
	 * pointing at a plaintext store is honored instead of being inherited
	 * from whatever secret happens to cover the prefix.
	 */
	char	   *endpointUrl = GetVendedConfigString(body, mapKey, "s3.endpoint");

	if (endpointUrl != NULL)
	{
		if (pg_strncasecmp(endpointUrl, "https://", 8) == 0)
		{
			creds->endpoint = pstrdup(endpointUrl + 8);
			creds->useSsl = pstrdup("true");
		}
		else if (pg_strncasecmp(endpointUrl, "http://", 7) == 0)
		{
			creds->endpoint = pstrdup(endpointUrl + 7);
			creds->useSsl = pstrdup("false");
		}
		else
		{
			/* No scheme to read SSL from; leave it to be inherited. */
			creds->endpoint = pstrdup(endpointUrl);
		}

		/* A trailing slash is part of a URL, not of a DuckDB endpoint. */
		int			endpointLen = strlen(creds->endpoint);

		while (endpointLen > 0 && creds->endpoint[endpointLen - 1] == '/')
			creds->endpoint[--endpointLen] = '\0';

		if (endpointLen == 0)
		{
			pfree(creds->endpoint);
			creds->endpoint = NULL;
		}

		pfree(endpointUrl);
	}

	/*
	 * Map the Iceberg "s3.path-style-access" boolean onto DuckDB's URL_STYLE.
	 * When the catalog omits it we leave urlStyle NULL so the engine can fall
	 * back to the environment's existing S3 secret.
	 */
	char	   *pathStyle = GetVendedConfigString(body, mapKey,
												  "s3.path-style-access");

	if (pathStyle != NULL)
	{
		bool		usePathStyle = false;

		/*
		 * pstrdup rather than the literal: every other string here is
		 * palloc'd, and the cache frees the whole struct field by field.
		 */
		if (parse_bool(pathStyle, &usePathStyle))
			creds->urlStyle = pstrdup(usePathStyle ? "path" : "vhost");
	}

	creds->expiresAt =
		ParseVendedExpiresAtMs(GetVendedConfigString(body, mapKey,
													 "s3.session-token-expires-at-ms"));

	return creds;
}


/*
 * AzureAccountHostFromLocation returns the storage host a fully
 * qualified Azure URL names -- abfss://<container>@<host>/..., or
 * <host>/<container>/... after az://, azure:// or abfss:// -- and NULL
 * for any other URL.  That includes az://<container>/..., which names no
 * account at all and leaves it to the credential.
 */
static char *
AzureAccountHostFromLocation(const char *location)
{
	static const char *const azurePrefixes[] = {
		AZURE_DLS_URL_PREFIX,
		AZURE_BLOB_URL_PREFIX,
		AZURE_URL_PREFIX,
	};
	const char *authority = NULL;

	if (location == NULL)
		return NULL;

	for (int i = 0; i < lengthof(azurePrefixes); i++)
	{
		size_t		prefixLen = strlen(azurePrefixes[i]);

		if (pg_strncasecmp(location, azurePrefixes[i], prefixLen) == 0)
		{
			authority = location + prefixLen;
			break;
		}
	}

	if (authority == NULL)
		return NULL;

	size_t		authorityLen = strcspn(authority, "/");
	const char *atSign = memchr(authority, '@', authorityLen);
	const char *host = atSign != NULL ? atSign + 1 : authority;
	size_t		hostLen = authorityLen - (host - authority);

	if (memchr(host, '.', hostLen) == NULL)
		return NULL;

	return pnstrdup(host, hostLen);
}


/*
 * IsAzureStorageAccountName returns whether name is one Azure accepts for
 * a storage account: 3 to 24 letters and digits.  The Azure SDK builds
 * https://<account>.blob.core.windows.net itself when no endpoint is set,
 * so anything else -- a '#' or '?', say -- would change which host that
 * URL names.
 */
static bool
IsAzureStorageAccountName(const char *name)
{
	if (name == NULL)
		return false;

	size_t		nameLen = strlen(name);

	if (nameLen < 3 || nameLen > 24)
		return false;

	for (size_t i = 0; i < nameLen; i++)
	{
		if (!isalnum((unsigned char) name[i]))
			return false;
	}

	return true;
}


/*
 * AzureEndpointsFromHost derives the blob and DFS endpoints of the
 * storage account a host such as <account>.dfs.<suffix> names.  The
 * suffix comes from the host rather than being assumed, which is what
 * keeps a sovereign cloud (core.usgovcloudapi.net, say) from being
 * addressed as the public one.  A host that names neither service is
 * used as is for both.
 */
static void
AzureEndpointsFromHost(const char *host, char **blobEndpoint,
					   char **dfsEndpoint)
{
	static const char *const services[] = {".dfs.", ".blob."};
	int			accountLen = strcspn(host, ".");

	for (int i = 0; i < lengthof(services); i++)
	{
		size_t		serviceLen = strlen(services[i]);

		if (pg_strncasecmp(host + accountLen, services[i], serviceLen) == 0)
		{
			const char *suffix = host + accountLen + serviceLen;

			*blobEndpoint = psprintf("https://%.*s.blob.%s", accountLen, host,
									 suffix);
			*dfsEndpoint = psprintf("https://%.*s.dfs.%s", accountLen, host,
									suffix);
			return;
		}
	}

	*blobEndpoint = psprintf("https://%s", host);
	*dfsEndpoint = pstrdup(*blobEndpoint);
}


/*
 * AzureEndpointOverride returns the endpoint a config map states for a
 * storage account under adls.connection-string.<host> or
 * .<account>.  In Iceberg's ADLS FileIO that key holds an endpoint URL
 * rather than a connection string, and is how an emulator or a private
 * endpoint is reached.  Anything but an http(s) URL is ignored, and so
 * is the bare adls.connection-string, which PyIceberg uses for a real
 * connection string.
 *
 * A role that can create a catalog server can make the catalog say
 * anything, and the Azure SDK sends every request to the endpoint the
 * secret carries, whatever host the URL names.  So the endpoint is held
 * to pg_lake.allowed_azure_host_suffixes, the list a user-supplied Azure
 * URL is held to, and one outside it is ignored rather than handed over:
 * it would otherwise send pgduck_server's requests to any host.
 */
static char *
AzureEndpointOverride(Jsonb *body, const char *mapKey, const char *host,
					  const char *accountName, Oid serverOid)
{
	char	   *endpoint = NULL;

	if (host != NULL)
		endpoint = GetVendedConfigString(body, mapKey,
										 psprintf("adls.connection-string.%s",
												  host));

	if (endpoint == NULL)
		endpoint = GetVendedConfigString(body, mapKey,
										 psprintf("adls.connection-string.%s",
												  accountName));

	if (endpoint == NULL ||
		(pg_strncasecmp(endpoint, HTTPS_URL_PREFIX, strlen(HTTPS_URL_PREFIX)) != 0 &&
		 pg_strncasecmp(endpoint, HTTP_URL_PREFIX, strlen(HTTP_URL_PREFIX)) != 0))
		return NULL;

	int			endpointLen = strlen(endpoint);

	while (endpointLen > 0 && endpoint[endpointLen - 1] == '/')
		endpoint[--endpointLen] = '\0';

	if (!IsAllowedAzureEndpoint(endpoint))
	{
		char	   *reportKey = psprintf("endpoint/%u/%s/%s", serverOid,
										 accountName, endpoint);

		ereport(FirstVendedReport(reportKey) ? WARNING : DEBUG2,
				(errmsg("ignoring the endpoint the catalog vended for Azure "
						"storage account \"%s\"", accountName),
				 errdetail("\"%s\" does not name a host under "
						   "pg_lake.allowed_azure_host_suffixes.",
						   RedactSensitiveText(endpoint)),
				 errhint("Add the endpoint's host to "
						 "pg_lake.allowed_azure_host_suffixes to reach it.")));
		return NULL;
	}

	return endpoint;
}


/*
 * SasTokenExpiry returns the expiry a SAS token states for itself in its
 * signed-expiry ("se") field, or 0 when it states none that parses.  Azure
 * writes it in UTC, as a date or a date and time, percent-encoded like the
 * rest of the token.
 */
static TimestampTz
SasTokenExpiry(const char *sasToken)
{
	const char *field = sasToken;

	while (field != NULL && strncmp(field, "se=", 3) != 0)
	{
		field = strchr(field, '&');
		if (field != NULL)
			field++;
	}

	if (field == NULL)
		return 0;

	const char *encoded = field + 3;
	size_t		encodedLen = strcspn(encoded, "&");
	StringInfoData value;

	initStringInfo(&value);

	for (size_t i = 0; i < encodedLen; i++)
	{
		char		c = encoded[i];

		if (c == '%' && i + 2 < encodedLen &&
			isxdigit((unsigned char) encoded[i + 1]) &&
			isxdigit((unsigned char) encoded[i + 2]))
		{
			char		hex[3] = {encoded[i + 1], encoded[i + 2], '\0'};

			c = (char) strtol(hex, NULL, 16);
			i += 2;
		}

		/* timestamptz_in also takes words such as "infinity" */
		if (c == '\0' || (!isdigit((unsigned char) c) && strchr("-:.TZ", c) == NULL))
			return 0;

		appendStringInfoChar(&value, c);
	}

	if (strchr(value.data, 'T') == NULL)
		appendStringInfoString(&value, "T00:00:00");

	if (value.data[value.len - 1] != 'Z')
		appendStringInfoChar(&value, 'Z');

	ErrorSaveContext escontext = {T_ErrorSaveContext};
	Datum		expiry;

	if (!DirectInputFunctionCallSafe(timestamptz_in, value.data, InvalidOid, -1,
									 (Node *) &escontext, &expiry))
		return 0;

	return DatumGetTimestampTz(expiry);
}


/*
 * ParseVendedAzureCredsFromConfig builds an Azure VendedCredentials from
 * the SAS token an Iceberg config map carries for the storage account
 * scope is on, or returns NULL when it carries none for that account.
 *
 * Catalogs state one token under several keys so every client finds its
 * own spelling: adls.sas-token.<host> (Iceberg 1.8 and later),
 * adls.sas-token.<account> (Iceberg 1.7), and a bare adls.sas-token
 * paired with adls.account-name (PyIceberg).  They are tried in that
 * order.  The account is the one scope's host names, or, when scope
 * names none, adls.account-name; a bare token stated for some other
 * account is not this table's credential.  It expires when the catalog
 * says, or else when the token itself says.
 */
static VendedCredentials *
ParseVendedAzureCredsFromConfig(Jsonb *body, const char *mapKey,
								const char *scope, Oid serverOid)
{
	char	   *host = AzureAccountHostFromLocation(scope);
	char	   *statedAccount = GetVendedConfigString(body, mapKey,
													  "adls.account-name");
	char	   *accountName = host != NULL ?
		pnstrdup(host, strcspn(host, ".")) : statedAccount;

	if (!IsAzureStorageAccountName(accountName))
		return NULL;

	char	   *sasToken = NULL;

	if (host != NULL)
		sasToken = GetVendedConfigString(body, mapKey,
										 psprintf("adls.sas-token.%s", host));

	if (sasToken == NULL)
		sasToken = GetVendedConfigString(body, mapKey,
										 psprintf("adls.sas-token.%s",
												  accountName));

	if (sasToken == NULL &&
		(statedAccount == NULL || strcmp(statedAccount, accountName) == 0))
		sasToken = GetVendedConfigString(body, mapKey, "adls.sas-token");

	/* a SAS token is often written as the query string it came from */
	if (sasToken != NULL && sasToken[0] == '?')
		sasToken = sasToken[1] != '\0' ? pstrdup(sasToken + 1) : NULL;

	if (sasToken == NULL)
		return NULL;

	VendedCredentials *creds = palloc0(sizeof(VendedCredentials));

	creds->provider = VENDED_STORAGE_AZURE;
	creds->accountName = accountName;
	creds->sasToken = sasToken;
	creds->serverOid = serverOid;
	creds->fetchedAt = GetCurrentTimestamp();

	char	   *endpoint = AzureEndpointOverride(body, mapKey, host, accountName,
												 serverOid);

	if (endpoint != NULL)
	{
		creds->blobEndpoint = endpoint;
		creds->dfsEndpoint = pstrdup(endpoint);
	}
	else if (host != NULL)
	{
		AzureEndpointsFromHost(host, &creds->blobEndpoint, &creds->dfsEndpoint);

		/*
		 * The host comes from the catalog's location for the table, which
		 * nothing has vetted, so what it yields is held to the same list.
		 * Left unset, the Azure SDK reaches the account at its public
		 * endpoint.
		 */
		if (!IsAllowedAzureEndpoint(creds->blobEndpoint) ||
			!IsAllowedAzureEndpoint(creds->dfsEndpoint))
		{
			creds->blobEndpoint = NULL;
			creds->dfsEndpoint = NULL;
		}
	}

	char	   *expiresMsStr = NULL;

	if (host != NULL)
		expiresMsStr = GetVendedConfigString(body, mapKey,
											 psprintf("adls.sas-token-expires-at-ms.%s",
													  host));

	if (expiresMsStr == NULL)
		expiresMsStr = GetVendedConfigString(body, mapKey,
											 psprintf("adls.sas-token-expires-at-ms.%s",
													  accountName));

	creds->expiresAt = ParseVendedExpiresAtMs(expiresMsStr);

	if (creds->expiresAt == 0)
		creds->expiresAt = SasTokenExpiry(sasToken);

	return creds;
}


/*
 * A kind of storage a credential can be vended for.  parse turns its
 * config map into a credential, NULL when pg_lake cannot use credentials
 * for that storage at all.  settingKeys are the keys an HTTP trace masks
 * that are nonetheless settings rather than a credential, such as an
 * endpoint or an encryption key; an entry ending in '.' is a prefix.
 * requirement says what a usable credential needs, for the report on one
 * that is not.
 */
typedef struct VendedStorage
{
	const char *name;
	const char *keyNamespace;
	const char *const *settingKeys;
	VendedCredentials *(*parse) (Jsonb *body, const char *mapKey,
								 const char *scope, Oid serverOid);
	const char *requirement;
}			VendedStorage;

static const char *const S3SettingKeys[] = {
	"s3.sse.key", NULL
};

static const char *const AzureSettingKeys[] = {
	"adls.connection-string.", NULL
};

static const char *const GcsSettingKeys[] = {
	"gcs.encryption-key", "gcs.decryption-key", NULL
};

static const char *const NoSettingKeys[] = {NULL};

static const VendedStorage S3Storage = {
	"S3", "s3.", S3SettingKeys, ParseVendedS3CredsFromConfig,
	"pg_lake needs an S3 access key and secret."
};

static const VendedStorage AzureStorage = {
	"Azure Data Lake Storage", "adls.", AzureSettingKeys,
	ParseVendedAzureCredsFromConfig,
	"pg_lake needs a SAS token for the storage account the table is on."
};

static const VendedStorage GcsStorage = {
	"Google Cloud Storage", "gcs.", GcsSettingKeys, NULL,
	"pg_lake cannot use vended credentials for Google Cloud Storage."
};

static const VendedStorage UnknownStorage = {
	"an unrecognized kind of storage", NULL, NoSettingKeys, NULL,
	"pg_lake can use vended credentials for S3 and Azure storage only."
};

static const VendedStorage *const KnownVendedStorage[] = {
	&S3Storage, &AzureStorage, &GcsStorage
};

static const struct
{
	const char *urlPrefix;
	const		VendedStorage *storage;
}			VendedStorageSchemes[] = {
	{S3_URL_PREFIX, &S3Storage},
	{AZURE_DLS_URL_PREFIX, &AzureStorage},
	{AZURE_BLOB_URL_PREFIX, &AzureStorage},
	{AZURE_URL_PREFIX, &AzureStorage},
	{GCS_URL_PREFIX, &GcsStorage},
	{GCS_ALT_URL_PREFIX, &GcsStorage},
};


/*
 * VendedStorageForLocation returns the storage a location's scheme
 * names, or NULL when there is no location or it names none pg_lake
 * knows.
 */
static const VendedStorage *
VendedStorageForLocation(const char *location)
{
	if (location == NULL)
		return NULL;

	for (int i = 0; i < lengthof(VendedStorageSchemes); i++)
	{
		const char *urlPrefix = VendedStorageSchemes[i].urlPrefix;

		if (pg_strncasecmp(location, urlPrefix, strlen(urlPrefix)) == 0)
			return VendedStorageSchemes[i].storage;
	}

	return NULL;
}


/*
 * ParseVendedCredsFromConfig builds a VendedCredentials from an Iceberg
 * config map vended for scope.  The storage it is for is the one scope's
 * scheme names, which is what picks the parser, so a credential for any
 * other storage is never taken for this one's.  Only when scope names no
 * storage pg_lake knows is every parser tried.
 *
 * Returns NULL when the map yields no credential pg_lake can use, with
 * *storage set to the storage it was vended for, or NULL when that is
 * unknown.
 */
static VendedCredentials *
ParseVendedCredsFromConfig(Jsonb *body, const char *mapKey,
						   const char *scope, Oid serverOid,
						   const VendedStorage * *storage)
{
	*storage = VendedStorageForLocation(scope);

	if (*storage != NULL)
		return (*storage)->parse != NULL ?
			(*storage)->parse(body, mapKey, scope, serverOid) : NULL;

	for (int i = 0; i < lengthof(KnownVendedStorage); i++)
	{
		const		VendedStorage *candidate = KnownVendedStorage[i];

		if (candidate->parse == NULL)
			continue;

		VendedCredentials *creds = candidate->parse(body, mapKey, scope,
													serverOid);

		if (creds != NULL)
		{
			*storage = candidate;
			return creds;
		}
	}

	return NULL;
}


/*
 * IsVendedSettingKey returns whether key is one of the storage's
 * settingKeys.
 */
static bool
IsVendedSettingKey(const VendedStorage * storage, const char *key)
{
	for (const char *const *setting = storage->settingKeys; *setting; setting++)
	{
		size_t		settingLen = strlen(*setting);

		if ((*setting)[settingLen - 1] == '.' ?
			strncmp(key, *setting, settingLen) == 0 :
			strcmp(key, *setting) == 0)
			return true;
	}

	return false;
}


/*
 * UnusableVendedCredential returns the storage to report a credential
 * for, when a config map that yielded none pg_lake can use vended one
 * anyway, and NULL when it vended nothing.
 *
 * A key is a credential when an HTTP trace masks its value
 * (IsSensitiveKey).  That list has to name every credential a catalog
 * vends, or the trace would leak it, whereas a config map states any
 * number of settings -- "gcs.project-id", "s3.sse.type" -- whether or not
 * anything was vended.  One of the storage's settingKeys is passed over
 * even so.  Every credential in a storage-credentials entry
 * (isEntry) counts, but the legacy config map also carries the catalog's
 * own, such as a table-scoped "token", so there only keys in the
 * storage's namespace do.  With the storage unknown, a key's namespace
 * says which it is for.
 */
static const VendedStorage *
UnusableVendedCredential(Jsonb *body, const char *mapKey,
						 const VendedStorage * storage, bool isEntry)
{
	List	   *keys = JsonbObjectKeys(body, mapKey);
	ListCell   *keyCell = NULL;

	foreach(keyCell, keys)
	{
		const char *key = lfirst(keyCell);
		const		VendedStorage *keyStorage = storage;

		if (!IsSensitiveKey(key))
			continue;

		for (int i = 0; keyStorage == NULL && i < lengthof(KnownVendedStorage); i++)
		{
			const char *keyNamespace = KnownVendedStorage[i]->keyNamespace;

			if (strncmp(key, keyNamespace, strlen(keyNamespace)) == 0)
				keyStorage = KnownVendedStorage[i];
		}

		bool		inNamespace = keyStorage != NULL &&
			strncmp(key, keyStorage->keyNamespace,
					strlen(keyStorage->keyNamespace)) == 0;

		if (!isEntry && !inNamespace)
			continue;

		if (keyStorage != NULL && IsVendedSettingKey(keyStorage, key))
			continue;

		return keyStorage != NULL ? keyStorage : &UnknownStorage;
	}

	return NULL;
}


/*
 * TableRootFromLoadTableResponse returns the table's base directory,
 * normalized with a trailing slash, from a loadTable response: the
 * declared storage location when the response carries one, otherwise
 * derived from the metadata file path.  Returns NULL when neither is
 * available.
 */
static char *
TableRootFromLoadTableResponse(Jsonb *response)
{
	char	   *tableLocation =
		JsonbGetOptionalString(response, 2, "metadata", "location");

	if (tableLocation != NULL && tableLocation[0] != '\0')
		return NormalizeScopePrefix(tableLocation);

	char	   *metadataFile =
		JsonbGetOptionalString(response, 1, "metadata-location");

	return TableRootFromMetadataLocation(metadataFile);
}


/*
 * ResolveVendedScope decides what storage prefix a vended credential
 * covers.
 *
 * The storage-credential's own prefix is the catalog's declared scope,
 * and is preferred: it covers external tables whose data lives outside
 * the metadata directory, and lets a catalog hand out something narrower
 * than the whole table.  It is honored only at or below the table root,
 * though.  A broader scope would hand out more than the table needs, and
 * one pointing at a sibling path would cover a table these credentials
 * have nothing to do with.  Since secrets live in a single process-wide
 * DuckDB instance and are selected by longest matching scope, either
 * could shadow the secret another table depends on.
 *
 * With no table root to check against there is nothing to clamp to, so
 * the catalog's scope stands.  With no scope at all, the table root is
 * the answer; NULL when there is neither.
 */
static char *
ResolveVendedScope(const char *scopePrefix, char *tableRoot)
{
	if (scopePrefix == NULL || scopePrefix[0] == '\0')
		return tableRoot;

	char	   *normScope = NormalizeScopePrefix(scopePrefix);

	if (tableRoot == NULL ||
		strncmp(normScope, tableRoot, strlen(tableRoot)) == 0)
		return normScope;

	return tableRoot;
}


/*
 * ExtractVendedCredentials parses vended credentials -- S3 keys or an
 * Azure SAS token -- from a REST catalog loadTable response body,
 * returning one VendedCredentials per scope the catalog vended for.
 *
 * Two response shapes are supported: the newer "storage-credentials"
 * array, each element carrying its own "prefix" and "config", and the
 * legacy top-level "config" map, which describes a single credential
 * with no scope of its own.  A catalog may vend several credentials --
 * separate ones for the data files and the metadata directory, say --
 * and dropping the extras would leave part of the table unreadable.
 *
 * Elements the scope resolution collapses onto a prefix already taken
 * are skipped: DuckDB selects one secret per path, so a second one at
 * the same scope could only shadow the first.
 *
 * A credential the catalog vended that pg_lake cannot use is reported
 * with a WARNING; see UnusableVendedCredential for how one is told apart
 * from a setting.  Passing over it quietly is the worst outcome
 * available: the table does not fail here but falls back to whatever
 * secret pgduck_server was configured with, which is scoped and expires
 * on its own terms rather than the catalog's, so the table either fails
 * much later at scan time or reads with access the catalog never
 * granted.  A backend warns about a table once; see FirstVendedReport.
 *
 * Returns NIL when the response carries no usable credential.
 */
static List *
ExtractVendedCredentials(Jsonb *response, RestCatalogOptions * opts,
						 const char *restCatalogName,
						 const char *namespaceName,
						 const char *relationName)
{
	if (response == NULL)
		return NIL;

	char	   *tableRoot = TableRootFromLoadTableResponse(response);
	List	   *credentials = NIL;
	const		VendedStorage *unusable = NULL;
	List	   *elements =
		JsonbGetArrayElementObjects(response, "storage-credentials",
									"config", "prefix");
	ListCell   *elementCell = NULL;

	/*
	 * A credential says which keys to use, not where the store is, so some
	 * catalogs state the region once in the table's own config rather than
	 * repeating it in every credential.  Read without it, S3 is addressed at
	 * a host with an empty region in it, which fails to resolve at scan time
	 * and long after the response that omitted it.
	 */
	char	   *tableRegion = GetVendedConfigString(response, "config",
													"client.region");

	if (tableRegion == NULL)
		tableRegion = GetVendedConfigString(response, "config", "s3.region");

	foreach(elementCell, elements)
	{
		JsonbArrayElement *element = lfirst(elementCell);
		char	   *scope = ResolveVendedScope(element->stringValue, tableRoot);
		const		VendedStorage *storage = NULL;
		VendedCredentials *creds =
			ParseVendedCredsFromConfig(element->object, NULL, scope,
									   opts->serverOid, &storage);

		if (creds == NULL)
		{
			if (unusable == NULL)
				unusable = UnusableVendedCredential(element->object, NULL,
													storage, true);
			continue;
		}

		if (creds->provider == VENDED_STORAGE_S3 && creds->region == NULL)
			creds->region = tableRegion;

		creds->scope = scope;

		ListCell   *takenCell = NULL;
		bool		scopeTaken = false;

		foreach(takenCell, credentials)
		{
			VendedCredentials *taken = lfirst(takenCell);

			if (taken->scope != NULL && creds->scope != NULL &&
				strcmp(taken->scope, creds->scope) == 0)
			{
				scopeTaken = true;
				break;
			}
		}

		if (!scopeTaken)
			credentials = lappend(credentials, creds);
	}

	if (credentials == NIL)
	{
		/* Fall back to the legacy top-level "config" map. */
		char	   *legacyScope = ResolveVendedScope(NULL, tableRoot);
		const		VendedStorage *storage = NULL;
		VendedCredentials *legacyCreds =
			ParseVendedCredsFromConfig(response, "config", legacyScope,
									   opts->serverOid, &storage);

		if (legacyCreds != NULL)
		{
			legacyCreds->scope = legacyScope;
			credentials = list_make1(legacyCreds);
		}
		else if (unusable == NULL)
			unusable = UnusableVendedCredential(response, "config", storage,
												false);
	}

	if (unusable != NULL)
	{
		char	   *identity = BuildVendedCredentialsIdentity(restCatalogName,
															  namespaceName,
															  relationName);
		char	   *reportKey = psprintf("unusable/%u/%u/%s", opts->serverOid,
										 opts->userMappingOid, identity);

		ereport(FirstVendedReport(reportKey) ? WARNING : DEBUG2,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("ignoring vended credentials for %s", unusable->name),
				 errdetail("%s  This table falls back to the credentials "
						   "pgduck_server is already configured with, whose "
						   "scope and lifetime may differ from the ones the "
						   "catalog vended.", unusable->requirement),
				 errhint("Configure a secret in pgduck_server for this "
						 "table's storage location.")));
	}
	else if (credentials == NIL)
		elog(DEBUG2, "REST catalog loadTable response did not contain "
			 "usable vended credentials");

	return credentials;
}


/*
 * FirstVendedReport returns whether this backend is yet to warn about the
 * problem reportKey names, and records that it now has.  A read-only table
 * is loaded from the catalog more than once per statement, so a problem
 * with what is vended for it would otherwise be reported on every load;
 * the caller reports the repeats at DEBUG2.
 */
static bool
FirstVendedReport(const char *reportKey)
{
	ListCell   *reportedCell = NULL;

	foreach(reportedCell, ReportedVendedProblems)
	{
		if (strcmp(lfirst(reportedCell), reportKey) == 0)
			return false;
	}

	InitVendedCredsCacheIfNeeded();

	MemoryContext oldContext = MemoryContextSwitchTo(VendedCredsCacheCtx);

	ReportedVendedProblems = lappend(ReportedVendedProblems,
									 pstrdup(reportKey));
	MemoryContextSwitchTo(oldContext);

	return true;
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
	ReportedVendedProblems = NIL;
}


/*
 * LookupVendedCredentialsInCache checks the cache for valid vended
 * credentials for the given table path under the given server.
 * Returns the cached credentials or NIL if not found/expired.
 */
static List *
LookupVendedCredentialsInCache(Oid serverOid,
							   Oid userMappingOid,
							   const char *restCatalogName,
							   const char *namespaceName,
							   const char *tableName)
{
	char	   *identity = BuildVendedCredentialsIdentity(restCatalogName,
														  namespaceName,
														  tableName);
	VendedCredentialsCacheKey cacheKey =
		BuildVendedCredentialsCacheKey(serverOid, userMappingOid, identity);
	bool		mine;

	InitVendedCredsCacheIfNeeded();

	bool		found = false;
	VendedCredentialsCacheEntry *entry =
		hash_search(VendedCredsCache, &cacheKey, HASH_FIND, &found);

	mine = found && entry->identity != NULL &&
		strcmp(entry->identity, identity) == 0;

	pfree(identity);

	/*
	 * Anything but our own entry is a miss.  Two tables whose identities hash
	 * alike would otherwise be served each other's credentials, which a fresh
	 * loadTable is a cheap price to avoid.
	 */
	if (!mine || entry->credentials == NIL)
		return NIL;

	TimestampTz now = GetCurrentTimestamp();
	const int64 FIVE_MINUTES_USEC = (int64) 5 * 60 * 1000000;

	if (entry->expiryTime <= now + FIVE_MINUTES_USEC)
	{
		/*
		 * Evict the stale entry so a fresh loadTable can repopulate it and
		 * the cache does not grow with dead entries in long-lived backends.
		 * Removing the entry only recycles the entry itself, so release what
		 * it owns first.
		 */
		FreeCachedVendedCredentialsList(entry->credentials);
		entry->credentials = NIL;
		pfree(entry->identity);
		entry->identity = NULL;
		hash_search(VendedCredsCache, &cacheKey, HASH_REMOVE, NULL);
		return NIL;
	}

	return entry->credentials;
}


/*
 * IcebergProvideStorageCredentials is the pg_lake_iceberg implementation
 * of the engine's storage-credential provider hook (installed at
 * _PG_init).  Given a relation, it resolves the vended credentials
 * for its REST-catalog Iceberg table and returns them as a
 * List<StorageCredential *> the engine resolver can push to
 * pgduck_server.
 *
 * Two properties are worth calling out:
 *
 *  - It fetches on a cache miss (issues a REST loadTable), so a table
 *    scanned in a fresh backend -- whose cache was never warmed -- still
 *    gets credentials.  The resolver wraps this in a PG_TRY, so a
 *    loadTable failure degrades gracefully instead of aborting the
 *    caller.
 *
 *  - The secret key incorporates the user-mapping OID, so two principals
 *    vended different credentials for the same table get distinct secrets
 *    rather than clobbering one another.
 *
 * Only read-only REST tables are served.  pg_lake owns the files of a
 * writable table, and owning files means deleting them: a DROP only
 * queues its files, and the queue holds them for
 * pg_lake_engine.orphaned_file_retention_period (10 days by default),
 * long after any vended credential has expired and the table has left
 * the catalog that could vend another.  Until that lifecycle has an
 * answer, writable tables keep reaching storage the way they do without
 * vending, and are no worse off than before.
 *
 * Returns NIL for non-REST tables, writable REST tables, when vending is
 * disabled, or when the catalog vends no credentials.
 */
List *
IcebergProvideStorageCredentials(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);
	RestCatalogOptions *opts;
	const char *restCatalogName;
	const char *namespaceName;
	const char *tableName;
	List	   *credentials;
	List	   *storageCredentials = NIL;
	ListCell   *credsCell = NULL;
	StorageCredential *sc;

	if (catalogType != REST_CATALOG_READ_ONLY)
		return NIL;

	/*
	 * Answer "is vending on?" before resolving full options, which validates
	 * the user mapping and would throw when the mapping was dropped in the
	 * same transaction as the table (e.g. DROP TABLE right after DROP USER
	 * MAPPING).  With vending off -- the default -- that drop path must not
	 * touch credentials at all.
	 */
	if (!RestCatalogVendingEnabledForRelation(relationId))
		return NIL;

	opts = GetRestCatalogOptionsForRelation(relationId);

	if (!opts->enableVendedCredentials)
		return NIL;

	restCatalogName = GetRestCatalogName(relationId);
	namespaceName = GetRestCatalogNamespace(relationId);
	tableName = GetRestCatalogTableName(relationId);

	credentials = LookupVendedCredentialsInCache(opts->serverOid,
												 opts->userMappingOid,
												 restCatalogName,
												 namespaceName, tableName);

	if (credentials == NIL)
	{
		/*
		 * Cache miss: pull fresh credentials from the catalog.  The load
		 * caches them as a side effect for the rest of the statement.
		 */
		RestCatalogLoadTableResult result =
			LoadTableFromRestCatalog(opts, restCatalogName, namespaceName,
									 tableName);

		credentials = result.vendedCredentials;
	}

	foreach(credsCell, credentials)
	{
		VendedCredentials *creds = lfirst(credsCell);

		/*
		 * A secret has to be bound to a prefix, and there is none to bind to
		 * here: the catalog named none and the metadata location did not
		 * yield a table root.  Inventing one from the catalog identity would
		 * produce a scope that matches nothing at best, and somebody else's
		 * objects at worst, so skip it and let the static secret decide.
		 */
		if (creds->scope == NULL || creds->scope[0] == '\0')
			continue;

		/*
		 * Return copies: the engine resolver may run syscache lookups between
		 * resolving and using these, which could invalidate cache-owned
		 * memory.
		 */
		sc = palloc0(sizeof(StorageCredential));
		sc->provider = creds->provider;
		sc->serverOid = opts->serverOid;

		/*
		 * The scope is part of the identity because one table can be vended
		 * several credentials -- data and metadata, say -- and each needs a
		 * secret of its own rather than overwriting the last.
		 */
		sc->secretId = psprintf("%u/%s/%s/%s/%s",
								opts->userMappingOid, restCatalogName,
								namespaceName, tableName, creds->scope);
		sc->scopePrefix = pstrdup(creds->scope);
		sc->accessKeyId = creds->accessKeyId ? pstrdup(creds->accessKeyId) : NULL;
		sc->secretAccessKey =
			creds->secretAccessKey ? pstrdup(creds->secretAccessKey) : NULL;
		sc->sessionToken = creds->sessionToken ? pstrdup(creds->sessionToken) : NULL;
		sc->region = creds->region ? pstrdup(creds->region) : NULL;
		sc->endpoint = creds->endpoint ? pstrdup(creds->endpoint) : NULL;
		sc->urlStyle = creds->urlStyle ? pstrdup(creds->urlStyle) : NULL;
		sc->useSsl = creds->useSsl ? pstrdup(creds->useSsl) : NULL;
		sc->accountName = creds->accountName ? pstrdup(creds->accountName) : NULL;
		sc->sasToken = creds->sasToken ? pstrdup(creds->sasToken) : NULL;
		sc->blobEndpoint = creds->blobEndpoint ? pstrdup(creds->blobEndpoint) : NULL;
		sc->dfsEndpoint = creds->dfsEndpoint ? pstrdup(creds->dfsEndpoint) : NULL;

		/*
		 * A catalog that states no expiry still needs one here.  The resolver
		 * only skips re-pushing a secret whose expiry is comfortably ahead,
		 * so leaving this at zero re-pushes on every statement.  Fall back to
		 * the same conservative TTL the credential cache applies, so both
		 * agree on how long these credentials are considered good for.
		 */
		sc->expiresAt = creds->expiresAt > 0
			? creds->expiresAt
			: creds->fetchedAt + (int64) VENDED_CREDS_DEFAULT_TTL_SECS * 1000000;

		storageCredentials = lappend(storageCredentials, sc);
	}

	return storageCredentials;
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
		psprintf(REST_CATALOG_NAMESPACE, opts->baseUri,
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
