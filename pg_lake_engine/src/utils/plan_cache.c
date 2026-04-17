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

/*
 * Certain queries on the catalog tables are executed frequently and
 * the query plans for these queries can be cached to avoid the overhead
 * of preparing the query plan each time. This cache provides per-backend
 * caching of query plans.
 *
 * The caching is intended for queries on the catalog tables that are executed
 * frequently. If we ever need to cache query plans for user queries, we have to
 * consider the cache size and eviction policy.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "utils/memutils.h"

#include "pg_lake/util/plan_cache.h"


/*
* This is somewhat arbitrary, we currently only have few callers
* of this hash, and their query string length is much smaller than
* this. We can increase this if needed.
*/
#define MAX_ALLOWED_QUERY_LEN 2048

/*
* The plan might change over time, so let's replan the query after
* a certain number of uses.
* This is somewhat arbitrary, but our reasoning is the following: For a
* very short query, the overhead of planning is around ~1% of the total
* query time. By replanning after 100 uses, we add an overhead of 1/100 to
* the total query time. This is a reasonable trade-off for the benefit of
* having a fresh plan.
*/
#define RE_PLAN_QUERY_AFTER 100

typedef struct QueryPlanCacheEntry
{
	/* key of the query plan in the hash */
	char		query[MAX_ALLOWED_QUERY_LEN];

	SPIPlanPtr	plan;
	uint32		planUseCount;
	bool		isValid;
} QueryPlanCacheEntry;

/* internal function declarations */
static void InitializeQueryPlanCache(void);

static HTAB *QueryPlanCache = NULL;


/*
 * InitializeQueryPlanCache initialized the session-level query plan
 * cache.
 */
static void
InitializeQueryPlanCache(void)
{
	if (QueryPlanCache != NULL)
	{
		/* initialize once per backend */
		return;
	}

	HASHCTL		info;

	memset(&info, 0, sizeof(info));
	info.keysize = MAX_ALLOWED_QUERY_LEN;
	info.entrysize = sizeof(QueryPlanCacheEntry);
	info.hcxt =
		AllocSetContextCreate(CacheMemoryContext,
							  "PgLake query cache context",
							  ALLOCSET_DEFAULT_SIZES);;
	int			hashFlags = HASH_ELEM | HASH_STRINGS | HASH_CONTEXT;

	QueryPlanCache = hash_create("PgLake query cache hash", 32, &info, hashFlags);
}


/*
 * GetCachedQueryPlan first checks if the query plan is already cached.
 * If not, it prepares the query plan and caches it. Finally, it returns
 * the query plan.
 */
SPIPlanPtr
GetCachedQueryPlan(const char *query, int argCount, Oid *argTypes)
{
	InitializeQueryPlanCache();

	if (strlen(query) >= MAX_ALLOWED_QUERY_LEN)
	{
		/*
		 * We are comfortable with throwing error here as this is a
		 * programming error. We can increase the MAX_ALLOWED_QUERY_LEN if
		 * needed.
		 */
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Query length exceeds the maximum allowed length")));
	}

	bool		foundInCache = false;
	QueryPlanCacheEntry *entry = hash_search(QueryPlanCache,
											 query,
											 HASH_ENTER,
											 &foundInCache);

	if (foundInCache && entry->planUseCount >= RE_PLAN_QUERY_AFTER)
	{
		SPI_freeplan(entry->plan);
		entry->plan = NULL;

		/* rebuild the entry */
		entry->isValid = false;
	}

	if (!foundInCache || !entry->isValid)
	{
		/* until this entry fully initialized, treat as not valid */
		entry->isValid = false;

		SPIPlanPtr	plan = SPI_prepare(query, argCount, argTypes);

		if (plan == NULL)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("SPI_prepare failed for: %s", query)));
		}

		SPI_keepplan(plan);
		entry->plan = plan;
		entry->planUseCount = 0;
		entry->isValid = true;
	}

	/* increment the use count */
	entry->planUseCount++;

	/*
	 * Currently any caller of this code-path should cache query plan, so
	 * always greater than 1. Also, we only have a handful of callers, so we
	 * can safely assume that the cache size is always less than 32. If
	 * anything goes wrong, we can spot it easily.
	 */
	Assert(hash_get_num_entries(QueryPlanCache) >= 1);
	Assert(hash_get_num_entries(QueryPlanCache) <= 32);

	return entry->plan;
}
