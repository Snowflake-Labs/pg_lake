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
 * Counters of REST catalog activity for the whole server, read with
 * lake_iceberg.catalog_stats().
 *
 * Like the catalog log lines, they carry no names: only the kind of catalog
 * (horizon, builtin or user), the request, and fixed words for the outcome.
 * Counting never throws, so it is safe after commit.
 */
#pragma once

#include "pg_lake/http/http_client.h"
#include "pg_lake/rest_catalog/rest_catalog.h"

/* why a request to a REST catalog was sent again */
typedef enum CatalogRetryReason
{
	CATALOG_RETRY_RATE_LIMITED,
	CATALOG_RETRY_UNAVAILABLE,
	CATALOG_RETRY_AUTH_REFRESH,
	CATALOG_RETRY_REASON_COUNT
}			CatalogRetryReason;

/* what a catalog response held when vended credentials were asked for */
typedef enum VendedCredentialsOutcome
{
	VENDED_CREDENTIALS_VENDED,
	VENDED_CREDENTIALS_NONE,
	VENDED_CREDENTIALS_UNUSABLE,
	VENDED_CREDENTIALS_OUTCOME_COUNT
}			VendedCredentialsOutcome;

extern void InitializeCatalogStats(void);
extern void CountCatalogRequest(const RestCatalogOptions * opts, RestCatalogOp op,
								const HttpResult * result, uint64 elapsedMicroseconds);
extern void CountCatalogRetry(const RestCatalogOptions * opts, RestCatalogOp op,
							  CatalogRetryReason reason);
extern void CountCatalogAuthFailure(const RestCatalogOptions * opts, int sqlerrcode);
extern void CountVendedCredentials(const RestCatalogOptions * opts,
								   VendedCredentialsOutcome outcome);
extern PGDLLEXPORT void CountCatalogTableScans(List *rteList);
