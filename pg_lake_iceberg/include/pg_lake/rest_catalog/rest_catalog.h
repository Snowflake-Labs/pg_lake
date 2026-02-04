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
#include "pg_lake/http/http_client.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/iceberg/api/snapshot.h"

#define REST_CATALOG_AUTH_TYPE_DEFAULT (0)
#define REST_CATALOG_AUTH_TYPE_HORIZON (1)

extern PGDLLEXPORT char *RestCatalogHost;
extern char *RestCatalogOauthHostPath;
extern char *RestCatalogClientId;
extern char *RestCatalogClientSecret;
extern char *RestCatalogScope;
extern int	RestCatalogAuthType;
extern bool RestCatalogEnableVendedCredentials;

#define REST_CATALOG_AUTH_TOKEN_PATH "%s/api/catalog/v1/oauth/tokens"

#define REST_CATALOG_NAMESPACE_NAME "%s/api/catalog/v1/%s/namespaces/%s"
#define REST_CATALOG_NAMESPACE "%s/api/catalog/v1/%s/namespaces"

#define REST_CATALOG_TABLE "%s/api/catalog/v1/%s/namespaces/%s/tables/%s"
#define REST_CATALOG_TABLES "%s/api/catalog/v1/%s/namespaces/%s/tables"

#define REST_CATALOG_AUTH_TOKEN_PATH "%s/api/catalog/v1/oauth/tokens"

#define REST_CATALOG_TRANSACTION_COMMIT "%s/api/catalog/v1/%s/transactions/commit"

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


#define REST_CATALOG_AUTH_TOKEN_PATH "%s/api/catalog/v1/oauth/tokens"
#define GET_REST_CATALOG_METADATA_LOCATION "%s/api/catalog/v1/%s/namespaces/%s/tables/%s"

extern PGDLLEXPORT void RegisterNamespaceToRestCatalog(const char *catalogName, const char *namespaceName);
extern PGDLLEXPORT void StartStageRestCatalogIcebergTableCreate(Oid relationId);
extern PGDLLEXPORT char *FinishStageRestCatalogIcebergTableCreateRestRequest(Oid relationId, DataFileSchema * dataFileSchema, List *partitionSpecs);
extern PGDLLEXPORT void ErrorIfRestNamespaceDoesNotExist(const char *catalogName, const char *namespaceName);
extern PGDLLEXPORT char *GetRestCatalogName(Oid relationId);
extern PGDLLEXPORT char *GetRestCatalogNamespace(Oid relationId);
extern PGDLLEXPORT char *GetRestCatalogTableName(Oid relationId);
extern PGDLLEXPORT bool IsReadOnlyRestCatalogIcebergTable(Oid relationId);
extern PGDLLEXPORT char *GetMetadataLocationFromRestCatalog(const char *restCatalogName, const char *namespaceName,
															const char *relationName);
extern PGDLLEXPORT char *GetMetadataLocationForRestCatalogForIcebergTable(Oid relationId);
extern PGDLLEXPORT void ReportHTTPError(HttpResult httpResult, int level);
extern PGDLLEXPORT List *PostHeadersWithAuth(void);
extern PGDLLEXPORT List *DeleteHeadersWithAuth(void);
extern PGDLLEXPORT bool ShouldRetryRequestToRestCatalog(long status, int maxRetry, int retryNo);
extern PGDLLEXPORT HttpResult SendRequestToRestCatalog(HttpMethod method, const char *url, const char *body, List *headers);
extern PGDLLEXPORT RestCatalogRequest * GetAddSnapshotCatalogRequest(IcebergSnapshot * newSnapshot, Oid relationId);
extern PGDLLEXPORT RestCatalogRequest * GetAddSchemaCatalogRequest(Oid relationId, DataFileSchema * dataFileSchema);
extern PGDLLEXPORT RestCatalogRequest * GetSetCurrentSchemaCatalogRequest(Oid relationId, int32_t schemaId);
extern PGDLLEXPORT RestCatalogRequest * GetAddPartitionCatalogRequest(Oid relationId, List *partitionSpec);
extern PGDLLEXPORT RestCatalogRequest * GetSetPartitionDefaultIdCatalogRequest(Oid relationId, int specId);
extern PGDLLEXPORT RestCatalogRequest * GetRemoveSnapshotCatalogRequest(List *removedSnapshotIds, Oid relationId);
