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

#include "duckdb.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"

namespace duckdb {

extern const string CACHE_DIR_SETTING;
extern const string CACHE_ON_WRITE_MAX_SIZE;
extern const string NO_CACHE_PREFIX;

/*
* To ensure that object store directories do not conflict with existing files on disk, include this
* unlikely-to-conflict suffix on cache files, allowing sub-paths to exist side-by-side:
*
* s3://mybucket/data.parquet
* s3://mybucket/data.parquet/data_0.parquet
*
* These will be turned into non-conflicting local cache paths:
*
* s3/mybucket/pgl-cache.data.parquet
* s3/mybucket/data.parquet/pgl-cache.data_0.parquet
*/
const string CACHE_FILE_PREFIX = "pgl-cache.";

/* special values for cache file return code */
const int64_t LOCK_CANNOT_BE_ACQUIRED = -1;
const int64_t DIRECTORY_CANNOT_CREATED = -2;

/*
 * CacheItem represents a file in cache or the cache queue.
 */
struct CacheItem
{
public:
	/* URL of the file */
	string url;

	/* path of the local file in cache (might not exist yet) */
	string cacheFilePath;

	/* size of the file as reported by S3 */
	int64_t fileSize;

	/* last access time of the URL */
	time_t lastAccessTime;

	/* whether this is a cache candidate (true) or already cached (false) */
	bool isCandidate;

	/* whether the cache candidate needs to be downloaded */
	bool needsDownload;

	/* item1 < item2 means item1 is older than item2 */
	bool operator<(const CacheItem& other) const
	{
		double diff = difftime(lastAccessTime, other.lastAccessTime);
		if (diff < 0)
			return true;
		else if (diff > 0)
			return false;
		else
			/*
			 * If access times are the same, somewhat arbitrarily use the
			 * file name.
			 */
			return cacheFilePath < other.cacheFilePath;
	}
};

/*
 * CacheActionType represents the type of action performed by ManageCache.
 */
enum CacheActionType
{
	ADDED,
	ADD_FAILED,
	REMOVED,
	SKIPPED_TOO_OLD,
	SKIPPED_TOO_LARGE,
	SKIPPED_CONCURRENT_MODIFY,
	SKIPPED_DIRECTORY_DOES_NOT_EXIST
};

/*
 * CacheAction represents an action performed by ManageCache.
 */
struct CacheAction
{
public:
	/* URL of the file that was cached/removed */
	string url;

	/* size of the file that was cached/removed */
	int64_t fileSize;

	/* whether the file was cached or removed (or caching failed) */
	CacheActionType action;
};

/*
 * FileCacheQueue tracks a set of files that were recently accessed and therefore
 * should perhaps be cached.
 */
class FileCacheQueue
{
public:

	/*
	 * RecordCacheCandidate reports that a not-yet-cached URL has been accessed.
	 */
	void RecordCacheCandidate(string url, string cacheFilePath, int64_t fileSize)
	{
		lock_guard<mutex> glock(lock);

		CacheItem &entry = queue[url];
		if (entry.fileSize == 0)
		{
			/* initialize new entry */
			entry.url = url;
			entry.cacheFilePath = cacheFilePath;
			entry.fileSize = fileSize;
			entry.isCandidate = true;
			entry.needsDownload = false;
		}

		/* always update last access time */
		entry.lastAccessTime = time(NULL);
	}

	/*
	 * ConsumeCandidates copies all the recently access cache items to a
	 * vector and clears the queue.
	 */
	vector<CacheItem> ConsumeCandidates()
	{
		lock_guard<mutex> glock(lock);
		vector<CacheItem> result;

		for (const auto& entry : queue)
			result.push_back(entry.second);

		queue.clear();
		return result;
	}

private:
    unordered_map<string, CacheItem> queue;

    mutex lock;
};

class FileCacheActivity {

private:

	/*
	* Any operation on the cache should be protected by this mutex.
	* This mutex is per file in the cache.
	*/
    mutex fileCacheActivityMutex;

public:
	FileCacheActivity() {}

	mutex& GetFileCacheActivityMutex()
	{
		return fileCacheActivityMutex;
	}
};

struct CacheLockStatus {
	string cacheFilePath;
    unique_lock<mutex> lock;
    bool lockAcquired;

    CacheLockStatus(string cacheFilePath, unique_lock<mutex> lock, bool acquired)
        : cacheFilePath(cacheFilePath), lock(std::move(lock)), lockAcquired(acquired) {}
};

/*
 * FileCacheManager is a singleton that manages the cache directory.
 *
 * We registered it in the global object cache, hence it extends ObjectCacheEntry.
 */
class FileCacheManager : public ObjectCacheEntry {
public:

	enum class CacheRemoveStatus {
		FILE_EXISTS = 0,
		FILE_NOT_EXISTS,
		LOCK_NOT_ACQUIRED
	};
	/*
	* To prevent truncation, we first write to a file with the .pgl-stage suffix
	* and then move to the regular path.
	*/
	const string STAGING_SUFFIX  = ".pgl-stage";

	/* queue of recently accessed items */
	FileCacheQueue queue;

	static shared_ptr<FileCacheManager> Get(ClientContext &context);

	bool TryGetCacheDir(optional_ptr<FileOpener> opener, string &cacheDir);
	bool TryGetCacheFilePath(string cacheDir, string url, string &cache_path);
	bool IsCacheableURL(string cacheDir, string url);
	int64_t CacheFile(ClientContext &context, string url, bool force, bool waitForLock);
	CacheRemoveStatus RemoveCacheFile(ClientContext &context, string url, bool waitForLock);
	CacheRemoveStatus RemoveCacheFileInternal(FileSystem& file_system, string filePath, string finalCacheFilePath, bool waitForLock);
	vector<CacheAction> ManageCache(ClientContext &context, int64_t maxCacheSize);
	vector<CacheItem> ListCache(ClientContext &context);
	void ErrorIfPathHasGlob(ClientContext &context, string url);
	string GetURLForCacheFilePath(string &cacheDir, const string &cacheFilePath);
	CacheLockStatus GetCacheStatusWithLock(string cacheFilePath, bool waitForLock);
	void RemoveCacheFileActivityFromMapIfNeeded(const string& cacheFilePath);

	static void UpdateAccessTime(string &filePath);

	/* required ObjectCacheEntry functions */
	static string ObjectType() {
		return "pg_lake_file_cache_manager";
	}

	string GetObjectType() override {
		return ObjectType();
	}

private:
	/* 
	 * FileCacheActivity is a wrapper for both a mutex and state of a 
	 * file in the cache. 
	 * Adding/removing of each path to the cache is protected by 
	 * a mutex.
	 */
	unordered_map<string, shared_ptr<FileCacheActivity>> cacheActivityMap;

	/* any access to the cacheActivityMap should be protected by this */
	mutex cacheActivityMapAccessLock;

	/*
	* Our current concurrency model is prevent concurrent ManageCache operations.
	* This mutex is used to enforce that.
	*
	* But not that we allow multiple CacheFile and UncacheFile operations to run
	* concurrently with each other and with ManageCache. Note that operations
	* on the same files always block each other via cacheActivityMapAccessLock.
	*
	* In the future, we might consider restricting concurrent ManageCache and
	* CacheFile/UncacheFile operations as it might give more precise control
	* for the cache size. For now, we prefer to keep it simple.
	*/
	mutex manageCacheLock;

	shared_ptr<FileCacheActivity> GetFileCacheActivity(const string& path);
	unique_lock<mutex> TryAcquireCachePathLock(const string& path, bool waitForLock, bool &acquired);

	int64_t CacheFileInternal(ClientContext &context, string url, bool force);
};

} // namespace duckdb
