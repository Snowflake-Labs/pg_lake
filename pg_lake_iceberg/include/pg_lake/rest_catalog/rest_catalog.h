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

#pragma once

#include "postgres.h"
#include "foreign/foreign.h"
#include "utils/jsonb.h"
#include "utils/timestamp.h"
#include "pg_lake/ddl/utility_hook.h"
#include "pg_lake/http/http_client.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/iceberg/api/snapshot.h"

#define REST_CATALOG_AUTH_TYPE_OAUTH2 (0)
#define REST_CATALOG_AUTH_TYPE_HORIZON (1)

extern PGDLLEXPORT char *RestCatalogHost;
extern char *RestCatalogOauthHostPath;
extern char *RestCatalogClientId;
extern char *RestCatalogClientSecret;
extern char *RestCatalogScope;
extern int	RestCatalogAuthType;
extern bool RestCatalogEnableVendedCredentials;


/*
 * Temporary storage credentials received from an Iceberg REST catalog
 * via the X-Iceberg-Access-Delegation: vended-credentials mechanism.
 *
 * These credentials are scoped to a specific S3 prefix (typically a
 * table's data directory) and have a limited lifetime.
 */
typedef struct VendedCredentials
{
	char	   *accessKeyId;	/* s3.access-key-id */
	char	   *secretAccessKey;	/* s3.secret-access-key */
	char	   *sessionToken;	/* s3.session-token (may be NULL for non-STS
								 * creds) */
	char	   *region;			/* client.region / s3.region (may be NULL) */
	char	   *endpoint;		/* s3.endpoint, scheme stripped (may be NULL) */
	char	   *urlStyle;		/* "path"/"vhost" from s3.path-style-access
								 * (may be NULL) */
	char	   *useSsl;			/* "true"/"false" from the s3.endpoint scheme
								 * (may be NULL) */
	char	   *scope;			/* S3 prefix these creds are scoped to (the
								 * table's storage location; may be NULL) */
	Oid			serverOid;		/* the iceberg_catalog server these came from */
	TimestampTz fetchedAt;		/* when credentials were obtained */
	TimestampTz expiresAt;		/* catalog-provided expiry, or 0 when the
								 * response carried no explicit expiry */
}			VendedCredentials;


/*
 * Result of loading a table from a REST catalog.  Contains the metadata
 * location, the table metadata document the catalog inlined alongside
 * it, and optional vended credentials from the response's "config" map.
 */
typedef struct RestCatalogLoadTableResult
{
	char	   *metadataLocation;

	/*
	 * The "metadata" object of the loadTable response: the same document that
	 * lives at metadataLocation, which the catalog hands us for free. Reading
	 * it here saves a round-trip to storage, and is the only way to see the
	 * schema before the relation exists, since storage credentials are
	 * resolved per relation.  NULL if the catalog omitted it.
	 */
	Jsonb	   *metadata;

	/*
	 * One VendedCredentials per storage-credential the catalog returned, each
	 * with its own scope; NIL when not vended or not requested.  A catalog is
	 * free to vend more than one, e.g. separate credentials for the data
	 * files and the metadata directory.
	 */
	List	   *vendedCredentials;
}			RestCatalogLoadTableResult;


/*
 * Resolved REST catalog connection options.  All REST catalogs --
 * built-in ('rest') and user-created (CREATE SERVER ... FOREIGN DATA
 * WRAPPER iceberg_catalog) -- are backed by a real pg_foreign_server
 * row.
 *
 * Resolution order, lowest to highest priority:
 *   1. GUC defaults                         (ApplyGUCDefaults)
 *   2. Server options                       (ApplyServerOptionOverrides)
 *   3. pg_user_mapping options              (user-created servers only)
 *
 * In-memory identity is the pair (`serverOid`, `userMappingOid`):
 *   - serverOid is the iceberg_catalog server's OID.
 *   - userMappingOid is the OID of the pg_user_mapping row that contributed the
 *     credentials, or InvalidOid when no user mapping was used (built-in
 *     pg_lake_rest_catalog, or a user-created server whose credentials
 *     came entirely from GUCs).
 *
 * `catalog` is the user-visible short name (e.g. 'rest', 'my_polaris')
 * kept purely for error messages.
 */
typedef struct RestCatalogOptions
{
	Oid			serverOid;		/* iceberg_catalog server OID; canonical
								 * identity, never InvalidOid for resolved
								 * opts */
	Oid			userMappingOid; /* pg_user_mapping row OID that supplied
								 * credentials, or InvalidOid if none */
	char	   *catalog;		/* short user-facing name; used in error
								 * messages, never for equality */
	char	   *baseUri;		/* normalized base URI; see
								 * ResolveRestCatalogBaseUri */
	char	   *oauthHostPath;
	char	   *clientId;
	char	   *clientSecret;
	char	   *scope;
	char	   *locationPrefix;
	char	   *catalogName;	/* REST API catalog prefix; defaults to dbname */
	int			authType;
	bool		enableVendedCredentials;
}			RestCatalogOptions;

/*
 * REST catalog URL templates.  The leading "%s" is the resolved base URI
 * (opts->baseUri), which already carries any deployment-specific mount path
 * (e.g. "/api/catalog" for Polaris, "/catalog" for Lakekeeper).  Everything
 * from "/v1/" onward is the Iceberg REST catalog spec.  See
 * ResolveRestCatalogBaseUri for how the base URI is normalized.
 */
#define REST_CATALOG_AUTH_TOKEN_PATH "%s/v1/oauth/tokens"

#define REST_CATALOG_NAMESPACE_NAME "%s/v1/%s/namespaces/%s"
#define REST_CATALOG_NAMESPACE "%s/v1/%s/namespaces"

#define REST_CATALOG_TABLE "%s/v1/%s/namespaces/%s/tables/%s"
#define REST_CATALOG_TABLES "%s/v1/%s/namespaces/%s/tables"

#define REST_CATALOG_TRANSACTION_COMMIT "%s/v1/%s/transactions/commit"

typedef enum RestCatalogOperationType
{
	REST_CATALOG_CREATE_TABLE = 0,
	REST_CATALOG_ADD_SNAPSHOT = 1,
	REST_CATALOG_ADD_SCHEMA = 2,
	REST_CATALOG_SET_CURRENT_SCHEMA = 3,
	REST_CATALOG_ADD_PARTITION = 4,
	REST_CATALOG_REMOVE_SNAPSHOT = 5,
	REST_CATALOG_DROP_TABLE = 6,
	REST_CATALOG_SET_DEFAULT_PARTITION_ID = 7,
}			RestCatalogOperationType;


typedef struct RestCatalogRequest
{
	Oid			relationId;
	RestCatalogOperationType operationType;

	/*
	 * For each request, holds the "action" part of the request body. We
	 * concatenate all requests from multiple tables into a single transaction
	 * commit request. The only exception is CREATE/DROP table, where body
	 * holds the full request body.
	 */
	char	   *body;
}			RestCatalogRequest;


/* Catalog options resolution */
extern PGDLLEXPORT RestCatalogOptions * ResolveRestCatalogOptions(const char *catalog);
extern PGDLLEXPORT RestCatalogOptions * GetRestCatalogOptionsForRelation(Oid relationId);
extern PGDLLEXPORT RestCatalogOptions * CopyRestCatalogOptions(MemoryContext dst, const RestCatalogOptions * src);

/*
 * Normalize a configured REST endpoint into a base URI usable as the
 * "%s" in the URL templates above.  Strips one trailing slash and
 * returns verbatim.  The caller is responsible for including the full
 * mount path in rest_endpoint.  Returns NULL for NULL input.
 */
extern PGDLLEXPORT char *ResolveRestCatalogBaseUri(const char *endpoint);

/*
 * Build options directly from a specific user mapping OID, bypassing
 * the per-current-user resolution path.  Used by the OAT_DROP capture
 * in pg_lake_table to snapshot credentials out of an about-to-vanish
 * mapping into the transaction-local catalogOpts.
 */
extern PGDLLEXPORT RestCatalogOptions * BuildRestCatalogOptionsFromUserMapping(Oid umOid);

/*
 * Server-id-only variants of the resolvers above.  Skip pg_user_mapping
 * lookup and credential validation, so the same-server identity check
 * stays correct in a transaction whose user mapping has already been
 * dropped (e.g. cascade-driven UM removal under DROP SERVER ... CASCADE).
 */
extern PGDLLEXPORT Oid ResolveRestCatalogServerId(const char *catalog);
extern PGDLLEXPORT Oid GetRestCatalogServerIdForRelation(Oid relationId);

/*
 * Credential handed back by PgLakeRestCatalogAuthHook.
 *
 * authorization is the complete Authorization header value including its
 * scheme, e.g. "Bearer eyJ...".  Providers supply the scheme themselves so
 * that catalogs authenticating with something other than a bearer token do
 * not require a change here.
 *
 * expiresIn is the credential's remaining lifetime in seconds.  Zero means
 * "do not cache": the hook is consulted again on the next request, which is
 * what a provider wants when it reads a credential that is rotated
 * underneath it rather than minting one with a known lifetime.
 */
typedef struct RestCatalogAuthMaterial
{
	char	   *authorization;
	int			expiresIn;
}			RestCatalogAuthMaterial;

/*
 * Hook letting another extension supply REST catalog credentials, for
 * catalogs whose authentication pg_lake has no built-in support for.
 *
 * Returning false means "not mine, fall back to the built-in OAuth2 flow",
 * so a provider can claim some servers and ignore others.  Returning true
 * without filling in material->authorization is an error.  pg_lake keeps
 * ownership of caching, refresh and header construction either way.
 */
typedef bool (*PgLakeRestCatalogAuthHookType) (RestCatalogOptions * opts,
											   bool forceRefresh,
											   RestCatalogAuthMaterial * material);

extern PGDLLEXPORT PgLakeRestCatalogAuthHookType PgLakeRestCatalogAuthHook;

/*
 * Module-internal helpers shared across the rest_catalog_*.c files.
 *
 * Declared here (rather than in a private header) only so the split
 * files can call each other; not part of the cross-dylib API surface,
 * so external callers should not depend on these.
 */
void		ApplyServerOptionOverrides(RestCatalogOptions * opts, ForeignServer *server);
void		ApplyUserMappingOverrides(RestCatalogOptions * opts, ForeignServer *server);
void		ApplyUserMappingOptionsList(RestCatalogOptions * opts, List *options, Oid umOid);
List	   *LookupUserMappingOptionsByOid(Oid umOid, Oid *serverOidOut);
char	   *GetRestCatalogAuthorization(RestCatalogOptions * opts, bool forceRefreshToken);
List	   *GetHeadersWithAuth(RestCatalogOptions * opts);
char	   *JsonbGetStringByPath(const char *jsonb_text, int nkeys,...);
char	   *JsonbGetOptionalStringByPath(const char *jsonb_text, int nkeys,...);
char	   *JsonbGetOptionalString(Jsonb *jb, int nkeys,...);

/*
 * One element of a JSON array of objects: the nested object the caller
 * asked for, plus one string field read off the element itself (e.g. a
 * storage-credential's "prefix").
 */
typedef struct JsonbArrayElement
{
	char	   *stringValue;	/* may be NULL when the field is absent */
	Jsonb	   *object;
}			JsonbArrayElement;

Jsonb	   *JsonbGetObject(Jsonb *jb, const char *key);

List	   *JsonbGetArrayElementObjects(Jsonb *jb, const char *arrayKey,
										const char *objectKey,
										const char *elementStringKey);

extern PGDLLEXPORT void RegisterNamespaceToRestCatalog(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName);
extern PGDLLEXPORT void StartStageRestCatalogIcebergTableCreate(Oid relationId);
extern PGDLLEXPORT char *FinishStageRestCatalogIcebergTableCreateRestRequest(Oid relationId, DataFileSchema * dataFileSchema, List *partitionSpecs);
extern PGDLLEXPORT void ErrorIfRestNamespaceDoesNotExist(RestCatalogOptions * opts, const char *catalogName, const char *namespaceName);
extern PGDLLEXPORT char *GetRestCatalogName(Oid relationId);
extern PGDLLEXPORT char *GetRestCatalogNamespace(Oid relationId);
extern PGDLLEXPORT char *GetRestCatalogTableName(Oid relationId);
extern PGDLLEXPORT bool IsReadOnlyRestCatalogIcebergTable(Oid relationId);
extern PGDLLEXPORT char *LoadRestCatalogMetadataLocation(RestCatalogOptions * opts, const char *restCatalogName, const char *namespaceName,
														 const char *relationName);
extern PGDLLEXPORT RestCatalogLoadTableResult LoadTableFromRestCatalog(RestCatalogOptions * opts, const char *restCatalogName,
																	   const char *namespaceName, const char *relationName);
extern PGDLLEXPORT RestCatalogLoadTableResult LoadTableFromRestCatalogForIcebergTable(Oid relationId);
extern PGDLLEXPORT char *GetMetadataLocationForRestCatalogForIcebergTable(Oid relationId);
extern PGDLLEXPORT bool RestCatalogVendingEnabledForRelation(Oid relationId);

/*
 * Resolves a relation's REST-catalog vended credentials, returning a
 * List<StorageCredential *> or NIL.  Called by ResolveStorageCredentials
 * (storage/storage_credentials.c) on the way to pgduck_server.
 */
extern PGDLLEXPORT List *IcebergProvideStorageCredentials(Oid relationId);
extern PGDLLEXPORT void InvalidateVendedCredentialsCache(void);
extern PGDLLEXPORT void ReportHTTPError(HttpResult httpResult, int level);
extern PGDLLEXPORT List *PostHeadersWithAuth(RestCatalogOptions * opts);
extern PGDLLEXPORT List *DeleteHeadersWithAuth(RestCatalogOptions * opts);
extern PGDLLEXPORT HttpResult SendRequestToRestCatalog(RestCatalogOptions * opts, HttpMethod method, const char *url, const char *body, List *headers);
extern PGDLLEXPORT RestCatalogRequest * GetAddSnapshotCatalogRequest(IcebergSnapshot * newSnapshot, Oid relationId);
extern PGDLLEXPORT RestCatalogRequest * GetAddSchemaCatalogRequest(Oid relationId, DataFileSchema * dataFileSchema);
extern PGDLLEXPORT RestCatalogRequest * GetSetCurrentSchemaCatalogRequest(Oid relationId, int32_t schemaId);
extern PGDLLEXPORT RestCatalogRequest * GetAddPartitionCatalogRequest(Oid relationId, List *partitionSpec);
extern PGDLLEXPORT RestCatalogRequest * GetSetPartitionDefaultIdCatalogRequest(Oid relationId, int specId);
extern PGDLLEXPORT RestCatalogRequest * GetRemoveSnapshotCatalogRequest(List *removedSnapshotIds, Oid relationId);

/* ProcessUtility handler for iceberg_catalog server DDL validation */
extern PGDLLEXPORT bool ValidateIcebergCatalogServerDDL(ProcessUtilityParams * processUtilityParams, void *arg);

/*
 * Chains an OAT_DROP hook onto Postgres' object_access_hook for
 * pg_lake_iceberg to react to user-mapping drops on iceberg_catalog
 * servers.  Called once from _PG_init.
 */
extern PGDLLEXPORT void InitializeIcebergCatalogObjectAccessHook(void);

/*
 * Callback invoked by pg_lake_iceberg's OAT_DROP hook when a user
 * mapping on a user-created iceberg_catalog server with dependent
 * iceberg tables is about to be dropped.  Set by pg_lake_table at
 * _PG_init time; the txn-local catalogOpts the callback writes
 * into lives there.  Stays NULL when pg_lake_table is not loaded
 * (the dispatch site skips capture).  May ereport on a malformed
 * user mapping (missing client_id / client_secret); that aborts
 * the cascade transaction, which is the safe outcome.
 */
typedef void (*RestCatalogXactCaptureCallback) (Oid umOid);
extern PGDLLEXPORT RestCatalogXactCaptureCallback PgLake_RestCatalogXactCaptureCallback;

/*
 * ProcessUtility handler that scrubs client_id / client_secret out of
 * queryString in CREATE/ALTER USER MAPPING for iceberg_catalog
 * servers, in place.  Register after ValidateIcebergCatalogServerDDL
 * so it runs first (the handler list is a prepend-LIFO).
 */
extern PGDLLEXPORT bool RedactRestCatalogUserMappingSecrets(ProcessUtilityParams * processUtilityParams, void *arg);
