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
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>

#include "duckdb.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/unordered_set.hpp"

#include "pg_lake/fs/cache_inode_budget.hpp"
#include "pg_lake/fs/file_utils.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"

namespace duckdb {

/*
 * Number of inodes that cache management keeps available on the cache file
 * system, or MIN_FREE_CACHE_INODES_AUTO to derive it from the file system.
 */
const string MIN_FREE_CACHE_INODES_SETTING = "pg_lake_min_free_cache_inodes";
const string MIN_FREE_CACHE_INODES_AUTO = "AUTO";

/*
 * How AUTO is represented once pg_lake_min_free_cache_inodes has been parsed, so
 * that the number of inodes to keep available is a single int64_t. Not a value
 * anybody can set: the setting takes AUTO or a non-negative number of inodes,
 * where 0 turns inode management off.
 */
static const int64_t MIN_FREE_INODES_AUTO = -1;

/*
 * By default, we keep 1/DEFAULT_FREE_INODE_FRACTION of the inodes on the cache
 * file system available, bounded by DEFAULT_MIN_FREE_INODES and
 * DEFAULT_MAX_FREE_INODES, and never more than half of the inodes on the file
 * system.
 */
static const int64_t DEFAULT_FREE_INODE_FRACTION = 100;
static const int64_t DEFAULT_MIN_FREE_INODES = 1000;
static const int64_t DEFAULT_MAX_FREE_INODES = 100000;


/*
 * TryParseMinFreeInodes interprets a value of pg_lake_min_free_cache_inodes: the
 * string AUTO, which comes back as MIN_FREE_INODES_AUTO, or a non-negative
 * number of inodes. Anything else is not a floor we can act on.
 */
static bool
TryParseMinFreeInodes(const string &value, int64_t &minFreeInodes)
{
	if (StringUtil::CIEquals(value, MIN_FREE_CACHE_INODES_AUTO))
	{
		minFreeInodes = MIN_FREE_INODES_AUTO;
		return true;
	}

	if (!TryCast::Operation<string_t, int64_t>(string_t(value), minFreeInodes))
		return false;

	return minFreeInodes >= 0;
}


/*
 * CheckMinFreeCacheInodes rejects values of pg_lake_min_free_cache_inodes that
 * we cannot act on, at SET time rather than on the next round of cache
 * management.
 */
void
CheckMinFreeCacheInodes(ClientContext &context, SetScope scope, Value &value)
{
	if (value.IsNull())
		throw InvalidInputException(MIN_FREE_CACHE_INODES_SETTING +
									" cannot be NULL");

	int64_t minFreeInodes;

	if (!TryParseMinFreeInodes(value.ToString(), minFreeInodes))
		throw InvalidInputException(MIN_FREE_CACHE_INODES_SETTING +
									" must be a non-negative number of inodes, or " +
									MIN_FREE_CACHE_INODES_AUTO +
									" to derive it from the cache file system");
}


/*
 * GetMinFreeInodes returns the number of inodes to keep available on the cache
 * file system according to pg_lake_min_free_cache_inodes.
 *
 * The setting has a default and its values are checked at SET time, so we only
 * get here without a number when somebody managed to unset it; derive the floor
 * from the file system in that case.
 */
static int64_t
GetMinFreeInodes(ClientContext &context)
{
	Value setting;
	int64_t minFreeInodes;

	if (!context.TryGetCurrentSetting(MIN_FREE_CACHE_INODES_SETTING, setting) ||
		setting.IsNull() ||
		!TryParseMinFreeInodes(setting.ToString(), minFreeInodes))
		return MIN_FREE_INODES_AUTO;

	return minFreeInodes;
}


/*
 * TryGetInodeStats reports the number of available and total inodes on the file
 * system that contains the given path.
 *
 * Returns false if the file system does not give us numbers we can manage the
 * cache with, in which case we manage it by size only. File systems that
 * allocate inodes dynamically (e.g. btrfs, ZFS) report 0. The statvfs fields
 * are unsigned and there is no portable value for "unknown", so a file system
 * is also free to report something like (fsfilcnt_t) -1, which we have to
 * reject as well: an inode floor derived from it would put us under permanent
 * inode pressure.
 */
static bool
TryGetInodeStats(const string &path, int64_t &freeInodes, int64_t &totalInodes)
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
			PGDUCK_SERVER_WARN("could not determine the number of available "
							   "inodes on the file system of %s, managing the "
							   "cache by size only: %s",
							   path.c_str(), strerror(errno));
			loggedStatvfsFailure = true;
		}

		return false;
	}

	/* f_favail is what is available to us, which excludes reserved inodes */
	uint64_t reportedFreeInodes = (uint64_t) stats.f_favail;
	uint64_t reportedTotalInodes = (uint64_t) stats.f_files;

	/*
	 * Check the numbers while they are still unsigned, rather than converting
	 * them first and looking at the sign of the result: what a conversion does
	 * with a value that does not fit in an int64_t is up to the implementation,
	 * and every value we could not represent is one we would have to reject
	 * anyway.
	 */
	if (reportedTotalInodes == 0 ||
		reportedTotalInodes > (uint64_t) INT64_MAX ||
		reportedFreeInodes > (uint64_t) INT64_MAX)
		return false;

	freeInodes = (int64_t) reportedFreeInodes;
	totalInodes = (int64_t) reportedTotalInodes;

	return true;
}


/*
 * DeriveInodeFloor returns the number of inodes to keep available on the cache
 * file system when the caller did not ask for a specific number.
 *
 * We keep a fraction of the file system available, since a bigger file system
 * usually means a bigger cache, with a lower bound to still leave room on a
 * small one. The upper bound of half the file system is there so that a file
 * system with very few inodes can hold a cache at all, instead of having
 * everything evicted on every round.
 *
 * Running out of inodes is an absolute condition rather than a relative one, so
 * the fraction also gets an absolute bound: on a file system with 100M inodes,
 * 1% would mean declaring inode pressure while a million of them are still
 * available, which is plenty for a cache that adds files in batches of at most
 * a few thousand per round.
 *
 * Expects totalInodes > 0, which TryGetInodeStats guarantees.
 */
static int64_t
DeriveInodeFloor(int64_t totalInodes)
{
	int64_t inodeFloor = totalInodes / DEFAULT_FREE_INODE_FRACTION;

	if (inodeFloor < DEFAULT_MIN_FREE_INODES)
		inodeFloor = DEFAULT_MIN_FREE_INODES;

	if (inodeFloor > DEFAULT_MAX_FREE_INODES)
		inodeFloor = DEFAULT_MAX_FREE_INODES;

	if (inodeFloor > totalInodes / 2)
		inodeFloor = totalInodes / 2;

	return inodeFloor;
}


/*
 * WithoutTrailingSlash returns the path with a single trailing slash removed,
 * which is what rmdir wants, and what ExtractDirName needs to go up a level.
 *
 * Erasing a single slash is enough because the only paths we use this on come
 * from the directory walk in ManageCache, which does not produce empty path
 * components, and IsCacheableURL requires a non-empty scheme, so a cache path
 * cannot start with a double slash either.
 */
static string
WithoutTrailingSlash(const string &path)
{
	if (StringUtil::EndsWith(path, "/"))
		return path.substr(0, path.length() - 1);

	return path;
}


/*
 * PruneEmptyCacheDirectory removes the directory that contained an evicted
 * cache file, and its now-empty parents, up to (but not including) the cache
 * directory itself.
 *
 * Each directory occupies an inode, and the cache mirrors the object store
 * layout, so a cache that saw many prefixes keeps holding inodes even after
 * all its files were evicted.
 *
 * rmdir fails when the directory is not empty, which includes the case where a
 * concurrent operation added a file to it, so we simply stop pruning then. The
 * one window that remains is a concurrent cache write that created its
 * directory but has not created its staging file in it yet, and therefore loses
 * the directory under it. Both paths that get there on their own report that as
 * a file they could not cache and carry on (ADD_FAILED in ManageCache,
 * cache-on-write in PGLakeCachingFileSystem::OpenFile), and the file becomes a
 * cache candidate again the next time it is read.
 *
 * The StartsWith check is what keeps us inside the cache directory.
 */
int64_t
PruneEmptyCacheDirectory(const string &cacheDir, string directory)
{
	int64_t removed = 0;

	while (StringUtil::StartsWith(directory, cacheDir) && directory != cacheDir)
	{
		string dirWithoutSlash = WithoutTrailingSlash(directory);

		if (rmdir(dirWithoutSlash.c_str()) < 0)
			/* not empty (anymore), or not ours to remove */
			break;

		PGDUCK_SERVER_DEBUG("removed empty cache directory %s",
							dirWithoutSlash.c_str());
		removed++;

		directory = FileUtils::ExtractDirName(dirWithoutSlash);
	}

	return removed;
}


/*
 * CountMissingCacheDirectories counts the cache directories that do not exist
 * yet, but that downloading the queued candidates will create.
 *
 * Each of those directories takes an inode, in the same way that pruning one in
 * ManageCache gives an inode back, so the reservation we make for the download
 * queue has to include them. Candidates that share a prefix also share its
 * directories, so we count each directory once.
 */
static int64_t
CountMissingCacheDirectories(FileSystem &fileSystem, const string &cacheDir,
							 const vector<CacheItem> &cacheFiles)
{
	unordered_set<string> missingDirectories;

	for (const CacheItem& cacheFile : cacheFiles)
	{
		if (!cacheFile.needsDownload)
			continue;

		string directory = FileUtils::ExtractDirName(cacheFile.cacheFilePath);

		/*
		 * Walk up towards the cache directory, which exists already. We can
		 * stop at the first directory that exists or that another candidate
		 * already accounted for, because everything above it is covered too.
		 */
		while (StringUtil::StartsWith(directory, cacheDir) && directory != cacheDir)
		{
			if (fileSystem.DirectoryExists(directory))
				break;

			if (!missingDirectories.insert(directory).second)
				break;

			/* ExtractDirName returns the path itself when it ends in a slash */
			directory = FileUtils::ExtractDirName(WithoutTrailingSlash(directory));
		}
	}

	return (int64_t) missingDirectories.size();
}


/*
 * GetInodeBudget determines how many inodes this round of cache management has
 * to work with: what the cache file system reports, the floor we want to stay
 * above, the cache files we could evict to get there, and the inodes the queued
 * downloads are going to take.
 *
 * We reserve an inode for every file we are about to download, the same way
 * queueSize reserves bytes for them. Without that we would evict to exactly the
 * floor, add files, and be back under it in the next round. Downloading also
 * creates the cache directories that lead to a new prefix, so those have to be
 * part of the reservation.
 */
InodeBudget
GetInodeBudget(ClientContext &context, FileSystem &fileSystem,
			   const string &cacheDir, const vector<CacheItem> &cacheFiles)
{
	InodeBudget budget;

	if (!TryGetInodeStats(cacheDir, budget.freeInodes, budget.totalInodes))
		/* manage the cache by size only */
		return budget;

	int64_t minFreeInodes = GetMinFreeInodes(context);

	budget.floor = minFreeInodes == MIN_FREE_INODES_AUTO ?
		DeriveInodeFloor(budget.totalInodes) : minFreeInodes;

	if (budget.floor <= 0)
		/* inode management is off, so there is nothing left to count */
		return budget;

	int64_t queuedFiles = 0;

	for (const CacheItem& cacheFile : cacheFiles)
	{
		if (cacheFile.needsDownload)
			queuedFiles++;
		else if (!cacheFile.isCandidate)
			budget.evictableFiles++;
	}

	budget.reserved = queuedFiles +
		CountMissingCacheDirectories(fileSystem, cacheDir, cacheFiles);

	return budget;
}


/*
 * LogInodePressure reports a round of cache management that ran while the cache
 * file system was low on inodes, which is an unusual situation that operators
 * may need to act on (e.g. by giving the cache its own volume, or a file system
 * with dynamically allocated inodes).
 */
void
LogInodePressure(const string &cacheDir, InodeBudget budget,
				 int64_t evictedFiles, int64_t prunedDirectories,
				 bool skippedDownloads)
{
	/*
	 * Ask the file system again rather than reporting our own bookkeeping, which
	 * cannot see inodes that are still held by an open file descriptor on a
	 * cache file we unlinked, or what other processes did in the meantime. The
	 * counts are the point of this message, so we want them measured. If the
	 * call fails we keep the numbers we started with.
	 */
	int64_t measuredFreeInodes = 0;
	int64_t measuredTotalInodes = 0;

	if (TryGetInodeStats(cacheDir, measuredFreeInodes, measuredTotalInodes))
	{
		budget.freeInodes = measuredFreeInodes;
		budget.totalInodes = measuredTotalInodes;
	}

	PGDUCK_SERVER_LOG("cache directory %s is low on inodes: %" PRIu64
					  "/%" PRIu64 " available, keeping %" PRIu64
					  " available; freed %" PRIu64 " inodes this round by "
					  "evicting %" PRIu64 " files and %" PRIu64 " empty "
					  "directories%s",
					  cacheDir.c_str(),
					  (uint64_t) budget.freeInodes,
					  (uint64_t) budget.totalInodes,
					  (uint64_t) budget.floor,
					  (uint64_t) budget.freed,
					  (uint64_t) evictedFiles,
					  (uint64_t) prunedDirectories,
					  skippedDownloads ?
					  ", cache files alone cannot free enough inodes so "
					  "nothing was added" : "");
}

} // namespace duckdb
