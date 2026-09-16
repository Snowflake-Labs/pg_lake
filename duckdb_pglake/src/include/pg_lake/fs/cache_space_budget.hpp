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
 * The disk space side of file cache management: the byte budget the cache is
 * managed against is a configured number, so nothing ties it to the file system
 * the cache lives on. This is what cache management uses to keep the cache
 * inside that file system, whatever the budget says.
 */

#pragma once

#include "duckdb.hpp"

namespace duckdb {

/*
 * Number of bytes that cache management keeps available on the cache file
 * system, or MIN_FREE_CACHE_BYTES_AUTO to derive it from the file system.
 */
extern const string MIN_FREE_CACHE_BYTES_SETTING;
extern const string MIN_FREE_CACHE_BYTES_AUTO;

/*
 * SpaceBudget is what a round of cache management knows about the disk space on
 * the cache file system: what it found there, and how much of it we want to
 * keep available.
 *
 * A floor of 0 means we manage the cache by its configured budget alone, either
 * because the file system does not give us numbers we can use, or because
 * pg_lake_min_free_cache_bytes turned the free space floor off.
 */
struct SpaceBudget
{
	/* what the cache file system reported at the start of the round */
	int64_t freeBytes = 0;
	int64_t totalBytes = 0;

	/* bytes we want to keep available on the cache file system */
	int64_t floor = 0;

	/*
	 * RoomForCache returns the largest the cache may be for the floor to still
	 * be available, given a cache that currently holds cacheSize bytes.
	 *
	 * A number <= 0 means the floor is out of the cache's reach: even an empty
	 * cache would leave the file system below it, because something other than
	 * the cache is using the space.
	 */
	int64_t RoomForCache(int64_t cacheSize) const
	{
		return cacheSize + freeBytes - floor;
	}
};

void CheckMinFreeCacheBytes(ClientContext &context, SetScope scope,
							Value &value);
SpaceBudget GetSpaceBudget(ClientContext &context, const string &cacheDir);
void LogSpacePressure(const string &cacheDir, SpaceBudget budget,
					  int64_t configuredCacheSize, int64_t effectiveCacheSize,
					  int64_t evictedFiles, int64_t evictedBytes,
					  bool floorOutOfReach);

} // namespace duckdb
