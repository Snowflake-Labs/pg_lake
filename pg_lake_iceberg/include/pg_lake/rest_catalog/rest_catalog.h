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

extern char *RestCatalogHost;
extern char *RestCatalogClientId;
extern char *RestCatalogClientSecret;

#define REST_CATALOG_AUTH_TOKEN_PATH "%s/api/catalog/v1/oauth/tokens"

#define REST_CATALOG_NAMESPACE_NAME "%s/api/catalog/v1/%s/namespaces/%s"
#define REST_CATALOG_NAMESPACE "%s/api/catalog/v1/%s/namespaces"

#define REST_CATALOG_AUTH_TOKEN_PATH "%s/api/catalog/v1/oauth/tokens"
#define GET_REST_CATALOG_METADATA_LOCATION "%s/api/catalog/v1/%s/namespaces/%s/tables/%s"

extern PGDLLEXPORT char *RestCatalogFetchAccessToken(void);
extern PGDLLEXPORT void RegisterNamespaceToRestCatalog(const char *catalogName, const char *namespaceName);
extern PGDLLEXPORT void ErrorIfRestNamespaceDoesNotExist(const char *catalogName, const char *namespaceName);
extern PGDLLEXPORT IcebergCatalogType GetIcebergCatalogType(Oid relationId);
extern PGDLLEXPORT char *GetRestCatalogName(Oid relationId);
extern PGDLLEXPORT char *GetRestCatalogNamespace(Oid relationId);
extern PGDLLEXPORT char *GetRestCatalogTableName(Oid relationId);
extern PGDLLEXPORT bool HasRestCatalogTableOption(List *options);
extern PGDLLEXPORT bool HasReadOnlyOption(List *options);
extern PGDLLEXPORT bool IsReadOnlyRestCatalogIcebergTable(Oid relationId);
extern PGDLLEXPORT char *GetMetadataLocationFromRestCatalog(const char *restCatalogName, const char *namespaceName,
															const char *relationName);
extern PGDLLEXPORT char *GetMetadataLocationForRestCatalogForIcebergTable(Oid relationId);
