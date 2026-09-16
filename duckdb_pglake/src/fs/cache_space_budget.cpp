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

#include <sys/statvfs.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>

#include "duckdb.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/string_util.hpp"

#include "pg_lake/fs/cache_space_budget.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"

namespace duckdb {

/*
 * Number of bytes that cache management keeps available on the cache file
 * system, or MIN_FREE_CACHE_BYTES_AUTO to derive it from the file system.
 */
const string MIN_FREE_CACHE_BYTES_SETTING = "pg_lake_min_free_cache_bytes";
const string MIN_FREE_CACHE_BYTES_AUTO = "AUTO";

/*
 * How AUTO is represented once pg_lake_min_free_cache_bytes has been parsed, so
 * that the number of bytes to keep available is a single int64_t. Not a value
 * anybody can set: the setting takes AUTO or a non-negative number of bytes,
 * where 0 turns the free space floor off.
 */
static const int64_t MIN_FREE_BYTES_AUTO = -1;

/*
 * By default, we keep 1/DEFAULT_FREE_SPACE_FRACTION of the cache file system
 * available, bounded by DEFAULT_MIN_FREE_BYTES and DEFAULT_MAX_FREE_BYTES, and
 * never more than half of the file system.
 */
static const int64_t DEFAULT_FREE_SPACE_FRACTION = 10;
static const int64_t DEFAULT_MIN_FREE_BYTES = 256LL * 1024 * 1024;
static const int64_t DEFAULT_MAX_FREE_BYTES = 8LL * 1024 * 1024 * 1024;


/*
 * TryParseMinFreeBytes interprets a value of pg_lake_min_free_cache_bytes: the
 * string AUTO, which comes back as MIN_FREE_BYTES_AUTO, or a non-negative
 * number of bytes. Anything else is not a floor we can act on.
 */
static bool
TryParseMinFreeBytes(const string &value, int64_t &minFreeBytes)
{
	if (StringUtil::CIEquals(value, MIN_FREE_CACHE_BYTES_AUTO))
	{
		minFreeBytes = MIN_FREE_BYTES_AUTO;
		return true;
	}

	if (!TryCast::Operation<string_t, int64_t>(string_t(value), minFreeBytes))
		return false;

	return minFreeBytes >= 0;
}


/*
 * CheckMinFreeCacheBytes rejects values of pg_lake_min_free_cache_bytes that we
 * cannot act on, at SET time rather than on the next round of cache management.
 */
void
CheckMinFreeCacheBytes(ClientContext &context, SetScope scope, Value &value)
{
	if (value.IsNull())
		throw InvalidInputException(MIN_FREE_CACHE_BYTES_SETTING +
									" cannot be NULL");

	int64_t minFreeBytes;

	if (!TryParseMinFreeBytes(value.ToString(), minFreeBytes))
		throw InvalidInputException(MIN_FREE_CACHE_BYTES_SETTING +
									" must be a non-negative number of bytes, or " +
									MIN_FREE_CACHE_BYTES_AUTO +
									" to derive it from the cache file system");
}


/*
 * GetMinFreeBytes returns the number of bytes to keep available on the cache
 * file system according to pg_lake_min_free_cache_bytes.
 *
 * The setting has a default and its values are checked at SET time, so we only
 * get here without a number when somebody managed to unset it; derive the floor
 * from the file system in that case.
 */
static int64_t
GetMinFreeBytes(ClientContext &context)
{
	Value setting;
	int64_t minFreeBytes;

	if (!context.TryGetCurrentSetting(MIN_FREE_CACHE_BYTES_SETTING, setting) ||
		setting.IsNull() ||
		!TryParseMinFreeBytes(setting.ToString(), minFreeBytes))
		return MIN_FREE_BYTES_AUTO;

	return minFreeBytes;
}


/*
 * TryGetSpaceStats reports the number of available and total bytes on the file
 * system that contains the given path.
 *
 * Returns false if the file system does not give us numbers we can manage the
 * cache with, in which case the cache is managed by its configured budget
 * alone. The statvfs fields are unsigned and there is no portable value for
 * "unknown", so a file system is free to report something we have to reject.
 */
static bool
TryGetSpaceStats(const string &path, int64_t &freeBytes, int64_t &totalBytes)
{
	struct statvfs stats;

	if (statvfs(path.c_str(), &stats) < 0)
	{
		/*
		 * The cache manager runs every few seconds, so a failure here would
		 * otherwise repeat in the log forever. We do not expect it at all for
		 * an existing local directory, so once is enough to notice.
		 */
		static bool loggedStatvfsFailure = false;

		if (!loggedStatvfsFailure)
		{
			PGDUCK_SERVER_WARN("could not determine the free space on the file "
							   "system of %s, managing the cache by its "
							   "configured size only: %s",
							   path.c_str(), strerror(errno));
			loggedStatvfsFailure = true;
		}

		return false;
	}

	/*
	 * f_frsize is the unit of the block counts, and f_bavail is what is
	 * available to us, which excludes the blocks reserved for root.
	 *
	 * Check the products while they are still unsigned, rather than converting
	 * them first and looking at the sign of the result: what a conversion does
	 * with a value that does not fit in an int64_t is up to the implementation.
	 */
	uint64_t blockSize = (uint64_t) stats.f_frsize;
	uint64_t reportedFreeBlocks = (uint64_t) stats.f_bavail;
	uint64_t reportedTotalBlocks = (uint64_t) stats.f_blocks;

	if (blockSize == 0 || reportedTotalBlocks == 0 ||
		reportedTotalBlocks > (uint64_t) INT64_MAX / blockSize ||
		reportedFreeBlocks > (uint64_t) INT64_MAX / blockSize)
		return false;

	freeBytes = (int64_t) (reportedFreeBlocks * blockSize);
	totalBytes = (int64_t) (reportedTotalBlocks * blockSize);

	return true;
}


/*
 * DeriveSpaceFloor returns the number of bytes to keep available on the cache
 * file system when the caller did not ask for a specific number.
 *
 * We keep a fraction of the file system available, since a bigger file system
 * usually means a bigger cache, with a lower bound to still leave room to write
 * on a small one. Filling a file system is an absolute condition rather than a
 * relative one, so the fraction also gets an absolute bound: on a 2 TB volume,
 * 10% would mean holding 200 GB back from a cache whose configured budget is
 * usually far smaller than that anyway.
 *
 * The upper bound of half the file system is there so that a small volume can
 * hold a cache at all, instead of having everything evicted on every round.
 *
 * Expects totalBytes > 0, which TryGetSpaceStats guarantees.
 */
static int64_t
DeriveSpaceFloor(int64_t totalBytes)
{
	int64_t spaceFloor = totalBytes / DEFAULT_FREE_SPACE_FRACTION;

	if (spaceFloor < DEFAULT_MIN_FREE_BYTES)
		spaceFloor = DEFAULT_MIN_FREE_BYTES;

	if (spaceFloor > DEFAULT_MAX_FREE_BYTES)
		spaceFloor = DEFAULT_MAX_FREE_BYTES;

	if (spaceFloor > totalBytes / 2)
		spaceFloor = totalBytes / 2;

	return spaceFloor;
}


/*
 * GetSpaceBudget determines how much disk space this round of cache management
 * has to work with: what the cache file system reports, and the floor we want
 * to stay above.
 */
SpaceBudget
GetSpaceBudget(ClientContext &context, const string &cacheDir)
{
	SpaceBudget budget;

	if (!TryGetSpaceStats(cacheDir, budget.freeBytes, budget.totalBytes))
		/* manage the cache by its configured size only */
		return budget;

	int64_t minFreeBytes = GetMinFreeBytes(context);

	budget.floor = minFreeBytes == MIN_FREE_BYTES_AUTO ?
		DeriveSpaceFloor(budget.totalBytes) : minFreeBytes;

	return budget;
}


/*
 * LogSpacePressure reports a round of cache management in which the free space
 * on the cache file system, rather than pg_lake_engine.max_cache_size, is what
 * bounded the cache -- or in which the space was gone and the cache was not
 * what was holding it.
 *
 * Either way an operator has to act on it, by giving the cache a larger volume
 * or by lowering max_cache_size to fit the volume it has, so say which of the
 * two it is and give the numbers behind the decision.
 */
void
LogSpacePressure(const string &cacheDir, SpaceBudget budget,
				 int64_t configuredCacheSize, int64_t effectiveCacheSize,
				 int64_t evictedFiles, int64_t evictedBytes,
				 bool floorOutOfReach)
{
	/*
	 * Ask the file system again rather than reporting the numbers the round
	 * started with, which cannot see the space freed by this round's evictions,
	 * nor what other processes did in the meantime. The numbers are the point of
	 * this message, so we want them measured. If the call fails we keep the ones
	 * we started with.
	 */
	int64_t freeBytes = budget.freeBytes;
	int64_t totalBytes = budget.totalBytes;

	TryGetSpaceStats(cacheDir, freeBytes, totalBytes);

	if (floorOutOfReach)
	{
		PGDUCK_SERVER_LOG("cache directory %s is low on disk space, and the "
						  "cache is not what is using it: %" PRIu64 "/%" PRIu64
						  " bytes available, keeping %" PRIu64 " available; the "
						  "cache was left alone",
						  cacheDir.c_str(),
						  (uint64_t) freeBytes,
						  (uint64_t) totalBytes,
						  (uint64_t) budget.floor);
		return;
	}

	PGDUCK_SERVER_LOG("cache directory %s has no room for the configured cache "
					  "size: %" PRIu64 "/%" PRIu64 " bytes available, keeping %"
					  PRIu64 " available, so the cache is managed against %"
					  PRIu64 " bytes instead of %" PRIu64 "; evicted %" PRIu64
					  " files (%" PRIu64 " bytes) this round",
					  cacheDir.c_str(),
					  (uint64_t) freeBytes,
					  (uint64_t) totalBytes,
					  (uint64_t) budget.floor,
					  (uint64_t) effectiveCacheSize,
					  (uint64_t) configuredCacheSize,
					  (uint64_t) evictedFiles,
					  (uint64_t) evictedBytes);
}

} // namespace duckdb
