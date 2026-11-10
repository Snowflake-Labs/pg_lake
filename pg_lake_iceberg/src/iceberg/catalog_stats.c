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

#include "postgres.h"

#include <curl/curl.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port/atomics.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/tuplestore.h"

#include "pg_lake/iceberg/catalog_stats.h"

#define CATALOG_STATS_SHMEM_NAME "pg_lake_iceberg catalog stats"

typedef enum CatalogStatsCatalog
{
	CATALOG_STATS_HORIZON,
	CATALOG_STATS_BUILTIN,
	CATALOG_STATS_USER,
	CATALOG_STATS_CATALOG_COUNT
}			CatalogStatsCatalog;

typedef enum CatalogRequestOutcome
{
	CATALOG_REQUEST_OK,
	CATALOG_REQUEST_NOT_FOUND,
	CATALOG_REQUEST_CLIENT_ERROR,
	CATALOG_REQUEST_SERVER_ERROR,
	CATALOG_REQUEST_NETWORK_ERROR,
	CATALOG_REQUEST_OUTCOME_COUNT
}			CatalogRequestOutcome;

/*
 * Every member is an array of pg_atomic_uint64, which InitCatalogStatsCounters
 * relies on.
 */
typedef struct CatalogStatsData
{
	pg_atomic_uint64 requests[CATALOG_STATS_CATALOG_COUNT][REST_CATALOG_OP_COUNT][CATALOG_REQUEST_OUTCOME_COUNT];
	pg_atomic_uint64 requestMicroseconds[CATALOG_STATS_CATALOG_COUNT][REST_CATALOG_OP_COUNT];
	pg_atomic_uint64 retries[CATALOG_STATS_CATALOG_COUNT][REST_CATALOG_OP_COUNT][CATALOG_RETRY_REASON_COUNT];
	pg_atomic_uint64 authFailures[CATALOG_STATS_CATALOG_COUNT];
	pg_atomic_uint64 vendedCredentials[CATALOG_STATS_CATALOG_COUNT][VENDED_CREDENTIALS_OUTCOME_COUNT];
}			CatalogStatsData;

StaticAssertDecl(sizeof(CatalogStatsData) % sizeof(pg_atomic_uint64) == 0,
				 "CatalogStatsData must hold only pg_atomic_uint64 counters");

/*
 * The names below are what lake_iceberg.catalog_stats() returns, and metric
 * collectors match them literally, so they are part of the interface.
 */
static const char *const CatalogNames[] = {
	[CATALOG_STATS_HORIZON] = "horizon",
	[CATALOG_STATS_BUILTIN] = "builtin",
	[CATALOG_STATS_USER] = "user",
};

static const char *const OperationNames[] = {
	[REST_CATALOG_OP_CONFIG] = "config",
	[REST_CATALOG_OP_TOKEN] = "token",
	[REST_CATALOG_OP_LIST_NAMESPACES] = "list_namespaces",
	[REST_CATALOG_OP_GET_NAMESPACE] = "get_namespace",
	[REST_CATALOG_OP_CREATE_NAMESPACE] = "create_namespace",
	[REST_CATALOG_OP_LOAD_TABLE] = "load_table",
	[REST_CATALOG_OP_CREATE_TABLE] = "create_table",
	[REST_CATALOG_OP_PUBLISH_CREATE_TABLE] = "publish_create_table",
	[REST_CATALOG_OP_PUBLISH_DROP_TABLE] = "publish_drop_table",
	[REST_CATALOG_OP_PUBLISH_COMMIT] = "publish_commit",
};

static const char *const RequestOutcomeNames[] = {
	[CATALOG_REQUEST_OK] = "ok",
	[CATALOG_REQUEST_NOT_FOUND] = "not_found",
	[CATALOG_REQUEST_CLIENT_ERROR] = "client_error",
	[CATALOG_REQUEST_SERVER_ERROR] = "server_error",
	[CATALOG_REQUEST_NETWORK_ERROR] = "network_error",
};

static const char *const RetryReasonNames[] = {
	[CATALOG_RETRY_RATE_LIMITED] = "rate_limited",
	[CATALOG_RETRY_UNAVAILABLE] = "unavailable",
	[CATALOG_RETRY_AUTH_REFRESH] = "auth_refresh",
};

static const char *const VendedOutcomeNames[] = {
	[VENDED_CREDENTIALS_VENDED] = "vended",
	[VENDED_CREDENTIALS_NONE] = "none",
	[VENDED_CREDENTIALS_UNUSABLE] = "unusable",
};

StaticAssertDecl(lengthof(CatalogNames) == CATALOG_STATS_CATALOG_COUNT,
				 "every catalog kind needs a name");
StaticAssertDecl(lengthof(OperationNames) == REST_CATALOG_OP_COUNT,
				 "every REST catalog operation needs a name");
StaticAssertDecl(lengthof(RequestOutcomeNames) == CATALOG_REQUEST_OUTCOME_COUNT,
				 "every request outcome needs a name");
StaticAssertDecl(lengthof(RetryReasonNames) == CATALOG_RETRY_REASON_COUNT,
				 "every retry reason needs a name");
StaticAssertDecl(lengthof(VendedOutcomeNames) == VENDED_CREDENTIALS_OUTCOME_COUNT,
				 "every vended credentials outcome needs a name");

/* NULL when pg_lake_iceberg was not preloaded, in which case nothing is counted */
static CatalogStatsData * CatalogStats = NULL;

static void InitCatalogStatsCounters(void);
static CatalogStatsCatalog CatalogKind(const RestCatalogOptions * opts);
static void AddCatalogStatsRow(ReturnSetInfo *rsinfo, const char *metric,
							   CatalogStatsCatalog catalog, const char *operation,
							   const char *result, pg_atomic_uint64 *counter);

PG_FUNCTION_INFO_V1(pg_lake_catalog_stats);

#if PG_VERSION_NUM >= 190000

static void CatalogStatsShmemRequest(void *arg);
static void CatalogStatsShmemInit(void *arg);

/* retained by pointer, so it must have static storage duration */
static const ShmemCallbacks CatalogStatsShmemCallbacks = {
	.request_fn = CatalogStatsShmemRequest,
	.init_fn = CatalogStatsShmemInit,
};

static void
CatalogStatsShmemRequest(void *arg)
{
	ShmemRequestStruct(.name = CATALOG_STATS_SHMEM_NAME,
					   .size = sizeof(CatalogStatsData),
					   .ptr = (void **) &CatalogStats);
}

static void
CatalogStatsShmemInit(void *arg)
{
	InitCatalogStatsCounters();
}

#else

static shmem_request_hook_type PreviousShmemRequestHook = NULL;
static shmem_startup_hook_type PreviousShmemStartupHook = NULL;

static void
CatalogStatsShmemRequest(void)
{
	if (PreviousShmemRequestHook != NULL)
		PreviousShmemRequestHook();

	RequestAddinShmemSpace(sizeof(CatalogStatsData));
}

static void
CatalogStatsShmemStartup(void)
{
	bool		found = false;

	if (PreviousShmemStartupHook != NULL)
		PreviousShmemStartupHook();

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	CatalogStats = ShmemInitStruct(CATALOG_STATS_SHMEM_NAME,
								   sizeof(CatalogStatsData), &found);
	if (!found)
		InitCatalogStatsCounters();

	LWLockRelease(AddinShmemInitLock);
}

#endif


/*
 * InitializeCatalogStats reserves the counters in shared memory.  It does
 * nothing unless pg_lake_iceberg is being preloaded, because shared memory
 * can only be reserved then.
 */
void
InitializeCatalogStats(void)
{
	if (!process_shared_preload_libraries_in_progress)
		return;

#if PG_VERSION_NUM >= 190000
	RegisterShmemCallbacks(&CatalogStatsShmemCallbacks);
#else
	PreviousShmemRequestHook = shmem_request_hook;
	shmem_request_hook = CatalogStatsShmemRequest;
	PreviousShmemStartupHook = shmem_startup_hook;
	shmem_startup_hook = CatalogStatsShmemStartup;
#endif
}


static void
InitCatalogStatsCounters(void)
{
	pg_atomic_uint64 *counters = (pg_atomic_uint64 *) CatalogStats;

	for (size_t i = 0; i < sizeof(CatalogStatsData) / sizeof(pg_atomic_uint64); i++)
		pg_atomic_init_u64(&counters[i], 0);
}


/*
 * CatalogKind groups catalogs the same way the catalog log lines do.
 */
static CatalogStatsCatalog
CatalogKind(const RestCatalogOptions * opts)
{
	if (!opts->isBuiltin)
		return CATALOG_STATS_USER;

	return opts->authType == REST_CATALOG_AUTH_TYPE_HORIZON ?
		CATALOG_STATS_HORIZON : CATALOG_STATS_BUILTIN;
}


/*
 * CountCatalogRequest counts the final result of a request, after any
 * retries, and the time spent on it including the retries.  A request that
 * was aborted because the query was cancelled is not counted.
 */
void
CountCatalogRequest(const RestCatalogOptions * opts, RestCatalogOp op,
					const HttpResult * result, uint64 elapsedMicroseconds)
{
	CatalogRequestOutcome outcome;

	if (CatalogStats == NULL || op < 0 || op >= REST_CATALOG_OP_COUNT)
		return;

	if (result->status == 0)
	{
		if (result->curlCode == CURLE_ABORTED_BY_CALLBACK)
			return;

		outcome = CATALOG_REQUEST_NETWORK_ERROR;
	}
	else if (result->status == 404)
		outcome = CATALOG_REQUEST_NOT_FOUND;
	else if (result->status >= 500)
		outcome = CATALOG_REQUEST_SERVER_ERROR;
	else if (result->status >= 400)
		outcome = CATALOG_REQUEST_CLIENT_ERROR;
	else
		outcome = CATALOG_REQUEST_OK;

	CatalogStatsCatalog catalog = CatalogKind(opts);

	pg_atomic_fetch_add_u64(&CatalogStats->requests[catalog][op][outcome], 1);
	pg_atomic_fetch_add_u64(&CatalogStats->requestMicroseconds[catalog][op],
							elapsedMicroseconds);
}


void
CountCatalogRetry(const RestCatalogOptions * opts, RestCatalogOp op,
				  CatalogRetryReason reason)
{
	if (CatalogStats == NULL || op < 0 || op >= REST_CATALOG_OP_COUNT)
		return;

	pg_atomic_fetch_add_u64(&CatalogStats->retries[CatalogKind(opts)][op][reason], 1);
}


/*
 * CountCatalogAuthFailure counts a failure to obtain an access token, other
 * than the query being cancelled or the server shutting down meanwhile.
 */
void
CountCatalogAuthFailure(const RestCatalogOptions * opts, int sqlerrcode)
{
	if (CatalogStats == NULL ||
		sqlerrcode == ERRCODE_QUERY_CANCELED ||
		sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		return;

	pg_atomic_fetch_add_u64(&CatalogStats->authFailures[CatalogKind(opts)], 1);
}


void
CountVendedCredentials(const RestCatalogOptions * opts,
					   VendedCredentialsOutcome outcome)
{
	if (CatalogStats == NULL)
		return;

	pg_atomic_fetch_add_u64(&CatalogStats->vendedCredentials[CatalogKind(opts)][outcome], 1);
}


/*
 * pg_lake_catalog_stats returns every counter that is not zero, one row
 * each.  The counters cover the whole server since it started.
 */
Datum
pg_lake_catalog_stats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	if (CatalogStats == NULL)
		PG_RETURN_VOID();

	for (int catalog = 0; catalog < CATALOG_STATS_CATALOG_COUNT; catalog++)
	{
		for (int op = 0; op < REST_CATALOG_OP_COUNT; op++)
		{
			for (int outcome = 0; outcome < CATALOG_REQUEST_OUTCOME_COUNT; outcome++)
				AddCatalogStatsRow(rsinfo, "requests", catalog,
								   OperationNames[op], RequestOutcomeNames[outcome],
								   &CatalogStats->requests[catalog][op][outcome]);

			AddCatalogStatsRow(rsinfo, "request_microseconds", catalog,
							   OperationNames[op], NULL,
							   &CatalogStats->requestMicroseconds[catalog][op]);

			for (int reason = 0; reason < CATALOG_RETRY_REASON_COUNT; reason++)
				AddCatalogStatsRow(rsinfo, "retries", catalog,
								   OperationNames[op], RetryReasonNames[reason],
								   &CatalogStats->retries[catalog][op][reason]);
		}

		AddCatalogStatsRow(rsinfo, "auth_failures", catalog, NULL, NULL,
						   &CatalogStats->authFailures[catalog]);

		for (int outcome = 0; outcome < VENDED_CREDENTIALS_OUTCOME_COUNT; outcome++)
			AddCatalogStatsRow(rsinfo, "vended_credentials", catalog, NULL,
							   VendedOutcomeNames[outcome],
							   &CatalogStats->vendedCredentials[catalog][outcome]);
	}

	PG_RETURN_VOID();
}


static void
AddCatalogStatsRow(ReturnSetInfo *rsinfo, const char *metric,
				   CatalogStatsCatalog catalog, const char *operation,
				   const char *result, pg_atomic_uint64 *counter)
{
	uint64		value = pg_atomic_read_u64(counter);
	Datum		values[5];
	bool		nulls[5] = {false};

	if (value == 0)
		return;

	values[0] = CStringGetTextDatum(metric);
	values[1] = CStringGetTextDatum(CatalogNames[catalog]);

	if (operation != NULL)
		values[2] = CStringGetTextDatum(operation);
	else
		nulls[2] = true;

	if (result != NULL)
		values[3] = CStringGetTextDatum(result);
	else
		nulls[3] = true;

	values[4] = Int64GetDatum((int64) Min(value, (uint64) PG_INT64_MAX));

	tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
}
