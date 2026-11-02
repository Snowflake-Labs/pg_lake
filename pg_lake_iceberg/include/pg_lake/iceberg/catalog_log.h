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
 * Catalog error log lines.
 *
 * Each line is CATALOG_LOG_PREFIX followed by a sentence from a fixed set:
 *
 *     pg_lake_catalog: could not load a table from the Horizon catalog: HTTP 403 (ForbiddenException)
 *
 * Log filters match the prefix and the sentences literally, so their wording
 * is part of the interface; docs/iceberg-catalogs.md lists every one.
 *
 * The lines are meant to be safe to collect where the rest of the server log
 * is not.  The only parts of a sentence that vary are numbers, SQLSTATEs and
 * words from a fixed vocabulary.  Never put a table, schema, namespace or
 * catalog name, a URL or path, SQL text, a remote error message, or anything
 * else a user or a remote service controls into one.
 */
#pragma once

#include "pg_lake/rest_catalog/rest_catalog.h"

#define CATALOG_LOG_PREFIX "pg_lake_catalog: "

/* Iceberg features a table may use that pg_lake cannot read */
typedef enum CatalogLogFeature
{
	CATALOG_LOG_FEATURE_DELETION_VECTORS,
	CATALOG_LOG_FEATURE_COLUMN_DEFAULTS
}			CatalogLogFeature;

extern bool LogCatalogEvents;

extern void LogCatalogRequest(const RestCatalogOptions * opts, RestCatalogOp op,
							  const HttpResult * result);
extern void LogCatalogAuthFailure(const RestCatalogOptions * opts, int sqlerrcode);
extern void LogCatalogNoVendedCredentials(const RestCatalogOptions * opts);
extern void LogCatalogUnusableVendedCredentials(const RestCatalogOptions * opts,
												const char *storageName);
extern void LogCatalogAzureEndpointNotAllowed(void);
extern void LogCatalogCredentialResolutionFailure(int sqlerrcode);
extern void LogCatalogUnsupportedFeature(CatalogLogFeature feature);
extern void LogCatalogUnsupportedFormatVersion(int formatVersion, bool forWrite);
extern void LogCatalogUnsupportedType(const char *icebergType);
