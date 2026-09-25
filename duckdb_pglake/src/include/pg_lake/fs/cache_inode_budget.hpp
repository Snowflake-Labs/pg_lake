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
 * The inode side of file cache management: the cache keeps one local file per
 * object, so on a file system with a fixed inode table it can run out of inodes
 * long before it reaches the byte budget. This is what cache management uses to
 * see that coming, plus the directory pruning that gives inodes back.
 */

#pragma once

#include "duckdb.hpp"

#include "pg_lake/fs/file_cache_manager.hpp"

namespace duckdb {

/*
 * Number of inodes that cache management keeps available on the cache file
 * system, or MIN_FREE_CACHE_INODES_AUTO to derive it from the file system.
 */
extern const string MIN_FREE_CACHE_INODES_SETTING;
extern const string MIN_FREE_CACHE_INODES_AUTO;

/*
 * InodeBudget is what a round of cache management knows about the inodes on the
 * cache file system: what it found there, how many we want to keep available,
 * how many the files we are about to download will take, and how many we have
 * freed so far.
 *
 * A floor of 0 means we manage the cache by size only, either because the file
 * system does not report inode counts we can use, or because
 * pg_lake_min_free_cache_inodes turned inode management off.
 */
struct InodeBudget
{
  /* what the cache file system reported at the start of the round */
  int64_t freeInodes = 0;
  int64_t totalInodes = 0;

  /* inodes we want to keep available on the cache file system */
  int64_t floor = 0;

  /*
   * Inodes we evict toward once floor is breached, well above floor so that a
   * round of eviction buys the next several rounds of headroom instead of
   * landing exactly back on the trigger. Cache-on-write has no reservation of
   * its own (unlike queued downloads, below), so whatever it adds between
   * rounds has to fit in this margin rather than push us under floor again
   * immediately. Always >= floor.
   */
  int64_t purgeTarget = 0;

  /* inodes reserved for the queued downloads and the directories they need */
  int64_t reserved = 0;

  /* inodes freed by evicting cache files and empty cache directories */
  int64_t freed = 0;

  /* cache files we could evict, each of which holds an inode */
  int64_t evictableFiles = 0;

  /*
   * Deficit returns the number of inodes we are still short of the floor once
   * the queued downloads have taken theirs, and a number <= 0 when we are not
   * under inode pressure. This is what decides whether to act at all.
   */
  int64_t Deficit() const
  {
    return floor + reserved - (freeInodes + freed);
  }

  /*
   * PurgeDeficit returns the number of inodes still needed to reach
   * purgeTarget rather than floor. This is what decides how much to evict
   * once Deficit() has said we should act: evicting only up to floor would
   * leave no margin for cache-on-write between this round and the next.
   */
  int64_t PurgeDeficit() const
  {
    return purgeTarget + reserved - (freeInodes + freed);
  }
};

void CheckMinFreeCacheInodes(ClientContext &context, SetScope scope,
							 Value &value);
InodeBudget GetInodeBudget(ClientContext &context, FileSystem &fileSystem,
						   const string &cacheDir,
						   const vector<CacheItem> &cacheFiles);
int64_t PruneEmptyCacheDirectory(const string &cacheDir, string directory);
void LogInodePressure(const string &cacheDir, InodeBudget budget,
					  int64_t evictedFiles, int64_t prunedDirectories,
					  bool skippedDownloads);

} // namespace duckdb
