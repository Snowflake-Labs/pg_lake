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

#include <utime.h>
#include <sys/stat.h>
#include <inttypes.h>

#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"

#include "pg_lake/fs/file_cache_manager.hpp"
#include "pg_lake/fs/file_utils.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"

namespace duckdb {

/*
 * Name of the setting that can be used to configure the cache directory.
 */
const string CACHE_DIR_SETTING = "pg_lake_cache_dir";

/*
 * Maximum size of the cache-on-write in bytes. If the file size exceeds this
 * value, the file will not be cached.
*/
const string CACHE_ON_WRITE_MAX_SIZE = "pg_lake_cache_on_write_max_size";


/*
 * The file cache is shared across clients by retrieving a global instance
 * from the object cache with the following key.
 */
const string CACHE_MANAGER_KEY = "PG_LAKE-CACHE-MANAGER";

/*
 * We allow URLs to be prefixed with nocache (e.g. nocaches3://), to avoid
 * reading from cache. An alternative implementation could be a query parameter,
 * but this was a bit simpler to implement and mostly internal.
 */
const string NO_CACHE_PREFIX = "nocache";


/*
 * TryGetCacheDir retrieves and normalizes the pg_lake_cache_dir setting.
 */
bool
FileCacheManager::TryGetCacheDir(optional_ptr<FileOpener> opener, string &cacheDir)
{
	Value setting;

	if (!opener->TryGetCurrentSetting(CACHE_DIR_SETTING, setting))
		return false;

	cacheDir = setting.ToString();

	if (!StringUtil::EndsWith(cacheDir, "/"))
		cacheDir += "/";

	return true;
}


/*
 * IsFinalizedCachePath returns whether the given file path belongs
 * to a finalized cache file path.
 */
static bool
IsFinalizedCachePath(string cacheFilePath)
{
    string fileName = FileUtils::ExtractFileName(cacheFilePath);

	return StringUtil::StartsWith(fileName, CACHE_FILE_PREFIX);
}


/*
 * GetURLForCacheFilePath converts a cache file path to a URL.
 */
string
FileCacheManager::GetURLForCacheFilePath(string &cacheDir, const string &cacheFilePath)
{
	if (!StringUtil::StartsWith(cacheFilePath, cacheDir))
		throw InvalidInputException(cacheFilePath + " is not a cache file path");

	string relativeCacheFilePath = cacheFilePath.substr(cacheDir.length());
	size_t firstSlashIndex = relativeCacheFilePath.find("/");

	if (firstSlashIndex == std::string::npos)
		throw InvalidInputException(cacheFilePath + " is not a valid cache file path");

	if (!IsFinalizedCachePath(cacheFilePath))
		throw InvalidInputException(cacheFilePath + " is not a cache file path");

	string protocol = relativeCacheFilePath.substr(0, firstSlashIndex);
	string remainder = relativeCacheFilePath.substr(firstSlashIndex);

	/* remove our completion suffix */
	string directory = FileUtils::ExtractDirName(remainder);
	string cachedFileName = FileUtils::ExtractFileName(remainder);
	string originalFileName = cachedFileName.substr(CACHE_FILE_PREFIX.length());

	return protocol + ":/" + directory + originalFileName;
}


/*
 * TryGetCacheFilePath returns whether caching is enabled and if so sets the
 * cacheFilePath for the given URL.
 *
 * The cache file path is constructed as:
 * <cache_dir>/<protocol>/<host>/<directory>/pgl-cache.<filename>
 *
 * The query string arguments are stripped for s3/gs, since they do not affect
 * contents, but for https it is preserved as part of the filename.
 *
 * For instance,
 * URL s3://mybucket/prefix/data.parquet?s3_region=us-east-1 will have
 * cache path /<cache dir>/s3/mybucket/prefix/data.parquet
 *
 * URL https://example.com/php/web1?format=json&token=19873 will have
 * cache path /<cache dir>/https/example.com/php/web1?format=json&token=19873
 */
bool
FileCacheManager::TryGetCacheFilePath(string cacheDir, string url, string &cacheFilePath)
{
	if (!IsCacheableURL(cacheDir, url))
		/* not cacheable */
		return false;

	size_t colonPos = url.find("://");

	if (colonPos == std::string::npos)
		throw InvalidInputException("not a valid URL: " + url);

	/* extract the s3 from s3:// */
	string urlProtocol = url.substr(0, colonPos);

	/* take the host + path by starting from the second slash (included) */
	string fullUrlPath = url.substr(colonPos + 2);

	/* check for the / after host */
	if (!StringUtil::StartsWith(fullUrlPath, "/"))
		throw InvalidInputException("not a valid URL: " + url);

	string queryArgs;

	/* find query arguments */
	size_t questionMarkPos = fullUrlPath.find('?');
	if (questionMarkPos != std::string::npos)
	{
		queryArgs = fullUrlPath.substr(questionMarkPos);

		/* strip the query arguments */
		fullUrlPath = fullUrlPath.substr(0, questionMarkPos);
	}

	/* construct "s3/bucket/file.parquet" string */
	string relativePath = urlProtocol + fullUrlPath;

	/* construct "s3/bucket/" string */
	string dirName = FileUtils::ExtractDirName(relativePath);

	/* construct "pgl-cache.file.parquet" string */
	string fileName = CACHE_FILE_PREFIX + FileUtils::ExtractFileName(relativePath);

	/*
	 * We append the query arguments to the file name in case of http(s).
	 *
	 * We strip query arguments from s3/gs, since they are not expected to
	 * change the output, so including them would cause redundant caching.
	 */
	if (StringUtil::StartsWith(urlProtocol, "http"))
		fileName += queryArgs;

	/* cannot write files with >255 characters */
	if (fileName.size() + CACHE_FILE_PREFIX.size() + STAGING_SUFFIX.size() > 255)
		return false;

	/* construct "/cache/dir/s3/bucket/pgl-cache.file.parquet" string */
	cacheFilePath = cacheDir + dirName + fileName;

	return true;
}


/*
 * IsCacheableURL returns whether we support caching this URL.
 */
bool
FileCacheManager::IsCacheableURL(string cacheDir, string url)
{
	/* cannot exceed maximum file path length */
	if (cacheDir.size() + url.size() >= 4095)
		return false;

	vector<string> splits = StringUtil::Split(url, "/");

	for (string &split : splits)
	{
		/* cannot have directory names that exceed 255 characters */
		if (split.size() > 255)
			return false;

		/* cannot use parent directory */
		if (split == "..")
			return false;
	}

	return StringUtil::StartsWith(url, "s3://") ||
		   StringUtil::StartsWith(url, "gs://") ||
		   StringUtil::StartsWith(url, "azure://") ||
		   StringUtil::StartsWith(url, "az://") ||
		   StringUtil::StartsWith(url, "abfss://") ||
		   StringUtil::StartsWith(url, "https://") ||
		   StringUtil::StartsWith(url, "http://") ||
		   StringUtil::StartsWith(url, "hf://");
}


/*
 * CacheFile wraps around CacheFileInternal with a FileCacheManager-level lock.
 */
int64_t
FileCacheManager::CacheFile(ClientContext &context, string url, bool force, bool waitForLock)
{
	FileOpener *opener = context.client_data->file_opener.get();

	string cacheDir;

	if (!TryGetCacheDir(opener, cacheDir))
		/* caching is not configured */
		throw InvalidInputException("caching is currently disabled");

	/* where we want to cache the file when done */
	string finalCacheFilePath;

	if (!TryGetCacheFilePath(cacheDir, url, finalCacheFilePath))
		throw InvalidInputException("URL cannot be cached: " + url);

	int64_t size = 0;

	try
	{
		/*
		 * We lock the file in this block in order to release the lock before
		 * RemoveCacheFileActivityFromMapIfNeeded.
		 */
		CacheLockStatus cacheLockStatus = GetCacheStatusWithLock(finalCacheFilePath, waitForLock);
		if (!cacheLockStatus.lockAcquired)
		{
			PGDUCK_SERVER_LOG("Skipping to cache %s because it is modified concurrently", finalCacheFilePath.c_str());

			return LOCK_CANNOT_BE_ACQUIRED;
		}

		size = CacheFileInternal(context, url, force);
		if (size == DIRECTORY_CANNOT_CREATED)
		{
			/* directory does not exist */
			RemoveCacheFileActivityFromMapIfNeeded(finalCacheFilePath);
			return DIRECTORY_CANNOT_CREATED;
		}
	}
	catch(const std::exception& e)
	{
		RemoveCacheFileActivityFromMapIfNeeded(finalCacheFilePath);
		throw;
	}

	RemoveCacheFileActivityFromMapIfNeeded(finalCacheFilePath);
	return size;
}

/*
 * CacheFileInternal caches the file at the given path and returns the file size if the
 * file was downloaded or, 0 if the file already exists and is not re-downloaded
 * (force is disabled).
 *
 * The function should be called while holding a lock on the cache file path via
 * GetCacheStatusWithLock().
 */
int64_t
FileCacheManager::CacheFileInternal(ClientContext &context, string url, bool force)
{
	FileOpener *opener = context.client_data->file_opener.get();
	FileSystem &file_system = FileSystem::GetFileSystem(context);
	ObjectCache &parquetMetadataCache = ObjectCache::GetObjectCache(context);

	/* prevent caching of glob patterns in the path */
	ErrorIfPathHasGlob(context, url);

	string cacheDir;

	if (!TryGetCacheDir(opener, cacheDir))
		/* caching is not configured */
		throw InvalidInputException("caching is currently disabled");

	/* where we want to cache the file when done */
	string finalCacheFilePath;

	if (!TryGetCacheFilePath(cacheDir, url, finalCacheFilePath))
		throw InvalidInputException("URL cannot be cached: " + url);

	if (!force && file_system.FileExists(finalCacheFilePath))
	{
		/* count explicit cache operation as an access */
		UpdateAccessTime(finalCacheFilePath);

		/* we do not want to cache the file again, so 0 bytes copied */
		return 0;
	}

	PGDUCK_SERVER_LOG("adding %s to cache", finalCacheFilePath.c_str());

	/* create the directory if it does not exist */
	string cacheFileDir = FileUtils::ExtractDirName(finalCacheFilePath);
	bool directoryExists = FileUtils::EnsureLocalDirectoryExists(context, cacheFileDir);
	if (!directoryExists)
	{
		PGDUCK_SERVER_DEBUG("cannot add file %s to cache as the cache directory cannot be created %s",
						    finalCacheFilePath.c_str(), cacheFileDir.c_str());

		return DIRECTORY_CANNOT_CREATED;
	}

	/* prefix the URL with nocache to prevent copying an already-cached file to itself */
	string sourceUrl = NO_CACHE_PREFIX + url;

	/* first, copy the file from the URL into a staging file */
	string stagingCacheFilePath = finalCacheFilePath + STAGING_SUFFIX;
	int64_t size = FileUtils::CopyFile(context, sourceUrl, stagingCacheFilePath);

	/* completed copy successfully, do an atomic rename */
	file_system.MoveFile(stagingCacheFilePath, finalCacheFilePath);

	PGDUCK_SERVER_LOG("successfully added %s to cache (%" PRIu64 \
					  " bytes)", finalCacheFilePath.c_str(), size);

	/*
	 * Clear Parquet metadata cache. The file may have changed since we
	 * last cached it, in which case we're now switching to the new
	 * version of the file and we should use the new metadata.
	 */
	parquetMetadataCache.Delete(url);

	return size;
}


/*
* ErrorIfPathHasGlob checks if the given URL has a glob pattern on the path
* and throws an exception if it does.
*
* Note that we allow the URL to have a glob pattern, such as
* s3://mybucket/prefix/hits.parquet?s3_region=us-east-1&recursive=true
* In the above URL example, we have "?" as part of parameters,
* which are not considered as part of the path.
*/
void
FileCacheManager::ErrorIfPathHasGlob(ClientContext &context, string url)
{
	FileSystem &file_system = FileSystem::GetFileSystem(context);

	/* strip the query arguments */
	size_t questionMarkPos = url.find('?');
	if (questionMarkPos != std::string::npos)
		url = url.substr(0, questionMarkPos);

	/* we do not want to cache wildcard URLs */
	if (file_system.HasGlob(url))
		throw InvalidInputException("cannot cache paths with wildcard");
}

/*
 * RemoveCacheFile removes the given URL from the cache, and returns whether
 * it existed.
 */
FileCacheManager::CacheRemoveStatus
FileCacheManager::RemoveCacheFile(ClientContext &context, string url, bool waitForLock)
{
	FileOpener *opener = context.client_data->file_opener.get();

	string cacheDir;

	if (!TryGetCacheDir(opener, cacheDir))
		/* caching is not configured */
		return CacheRemoveStatus::FILE_NOT_EXISTS;

	/* where we want to cache the file when done */
	string finalCacheFilePath;

	if (!TryGetCacheFilePath(cacheDir, url, finalCacheFilePath))
		throw InvalidInputException("URL cannot be cached: " + url);

	FileSystem &file_system = FileSystem::GetFileSystem(context);
	ObjectCache &parquetMetadataCache = ObjectCache::GetObjectCache(context);

	/* prevent caching of glob patterns in the path */
	ErrorIfPathHasGlob(context, url);

	/* remove the file from the cache */
	CacheRemoveStatus status =
		RemoveCacheFileInternal(file_system, finalCacheFilePath, finalCacheFilePath, waitForLock);
	if (status == CacheRemoveStatus::FILE_EXISTS)
	{
		/*
		* Also remove Parquet metadata from in-memory cache to not leak
		* memory and in case the user is doing an explicit removal because
		* the file changed. We do this after removing the file, to ensure
		* future readers do not see metadata. We also do it if there was
		* no file, to not leave any cache remnants when explicitly uncaching
		* a file.
		*/
		parquetMetadataCache.Delete(url);
	}

	return status;
}


/*
 * RemoveCacheFileInternal does the actual work of removing the file from the cache.
 * It also acquires the relevant cache lock and removes the cache activity from the map
 * if the lock is no longer needed.
 *
 * Note that we pass both "finalCacheFilePath" and "filePath" as parameters. The "finalCacheFilePath"
 * is used to acquire the lock and to remove the cache activity from the map. The "filePath" is the actual
 * file to remove from the cache. Both are the same for already cached files, but different for staging files.
 */
FileCacheManager::CacheRemoveStatus
FileCacheManager::RemoveCacheFileInternal(FileSystem &file_system, string finalCacheFilePath, string filePath, bool waitForLock)
{
	bool fileExists = false;

	try
	{
		/*
		 * We lock the file in this block in order to release the lock before
		 * RemoveCacheFileActivityFromMapIfNeeded.
		 */
		CacheLockStatus cacheLockStatus = GetCacheStatusWithLock(finalCacheFilePath, waitForLock);

		if (!cacheLockStatus.lockAcquired)
		{
			PGDUCK_SERVER_LOG("Skipping to remove from the cache %s because it is modified concurrently", filePath.c_str());

			return FileCacheManager::CacheRemoveStatus::LOCK_NOT_ACQUIRED;
		}

		fileExists = file_system.FileExists(filePath);
		if (fileExists)
		{
			PGDUCK_SERVER_LOG("removing %s from cache", filePath.c_str());
			file_system.RemoveFile(filePath);
		}
	}
	catch(const std::exception& e)
	{
		RemoveCacheFileActivityFromMapIfNeeded(filePath);
		throw;
	}

	RemoveCacheFileActivityFromMapIfNeeded(filePath);

	return fileExists ? FileCacheManager::CacheRemoveStatus::FILE_EXISTS
					  : FileCacheManager::CacheRemoveStatus::FILE_NOT_EXISTS;
}



/*
 * ManageCache implements a simple cache management approach where we prune
 * the least recently used files, until there is enough space for all recently
 * accessed candidates, and then we download them one by one.
 */
vector<CacheAction>
FileCacheManager::ManageCache(ClientContext &context, int64_t maxCacheSize)
{
	lock_guard<mutex> lock(manageCacheLock);

	FileOpener *opener = context.client_data->file_opener.get();
	FileSystem &file_system = FileSystem::GetFileSystem(context);

	/* determine the cache directory */
	string cacheDir;

	if (!TryGetCacheDir(opener, cacheDir))
		/* caching not configured */
		throw InvalidInputException("caching is currently disabled");

	/* determine the maximum cache size */
	vector<CacheItem> cacheCandidates = queue.ConsumeCandidates();

	/* keep track of actions taken by this function */
	vector<CacheAction> actions;

	uint64_t queueSize = 0;

	for (CacheItem& cacheCandidate : cacheCandidates)
	{
		/* we only consider cache candidates that actually fit in the cache */
		if (cacheCandidate.fileSize >= maxCacheSize)
		{
			actions.push_back({
				.url = cacheCandidate.url,
				.fileSize = cacheCandidate.fileSize,
				.action = SKIPPED_TOO_LARGE
			});
			continue;
		}

		/*
		 * We only consider cache candidates that don't have files.
		 *
		 * Since candidates might be recorded while ManageCache is running
		 * we might see candidates that already have cache files.
		 */
		if (file_system.FileExists(cacheCandidate.cacheFilePath))
			continue;

		queueSize += cacheCandidate.fileSize;
		cacheCandidate.needsDownload = true;
	}

	/* get all the files in the cache directory */
	vector<OpenFileInfo> cachedFileNames = file_system.Glob(cacheDir + "**");

	/* construct a combined list of cached files and candidates */
	vector<CacheItem> cacheFiles = cacheCandidates;

	int64_t totalCacheSize = 0;

	for (const OpenFileInfo& cachedFilePath : cachedFileNames)
	{
		/* skip and remove files that failed during staging */
		if (StringUtil::EndsWith(cachedFilePath.path, STAGING_SUFFIX))
		{
			/* remove the staging suffix from a copy of the path */
			string finalCacheFilePath = cachedFilePath.path;
			finalCacheFilePath.erase(finalCacheFilePath.length() - STAGING_SUFFIX.length());

			bool waitForLock = false;

			/*
			* We pass finalCacheFilePath as the locking is based on the finalCacheFilePath.
			* We also pass the staging file (e.g., cachedFilePath) as the actual file to remove
			* from the cache.
			*/
			FileCacheManager::CacheRemoveStatus status =
				RemoveCacheFileInternal(file_system, finalCacheFilePath, cachedFilePath.path, waitForLock);

			/*
			 * A concurrent cache operation is happening on the same file, so we have not removed
			 * the file from the cache. We skip the file from the cache management in this iteration.
			 */
			if (status == FileCacheManager::CacheRemoveStatus::LOCK_NOT_ACQUIRED)
			{
				actions.push_back({
					.url = GetURLForCacheFilePath(cacheDir, finalCacheFilePath),
					.fileSize = 0,
					.action = SKIPPED_CONCURRENT_MODIFY
				});
			}

			continue;
		}

		if (!IsFinalizedCachePath(cachedFilePath.path))
		{
			/* unexpected file in cache; maybe warn, but probably just skip */
			continue;
		}

		struct stat cachedFileStat;
		if (stat(cachedFilePath.path.c_str(), &cachedFileStat) < 0)
		{
			/* file was concurrently removed/finished caching? */
			continue;
		}

		/* construct file metadata */
		CacheItem cachedFile = {
			.url = GetURLForCacheFilePath(cacheDir, cachedFilePath.path),
			.cacheFilePath = cachedFilePath.path,
			.fileSize = cachedFileStat.st_size,
			.lastAccessTime = cachedFileStat.st_atime,
			.isCandidate = false,
			.needsDownload = false
		};

		/* make a deep copy and push it onto the vector */
		cacheFiles.push_back(cachedFile);

		/* count the total size of the cache */
		totalCacheSize += cachedFile.fileSize;
	}

	/* sort from oldest to newest access time */
	std::sort(cacheFiles.begin(), cacheFiles.end());

	/* remove items until we have enough space for all candidates */
	for (CacheItem& cacheFile : cacheFiles)
	{
		if (totalCacheSize + queueSize < maxCacheSize)
			/* we have enough space to fit our queue */
			break;

		if (!cacheFile.isCandidate)
		{
			PGDUCK_SERVER_LOG("removing %s from cache (%" PRIu64 \
							  " bytes)", cacheFile.cacheFilePath.c_str(), cacheFile.fileSize);

			/* for background tasks, we skip if lock cannot be acquired */
			bool waitForLock = false;
			CacheActionType action = REMOVED;

			/* remove the file to free up space */
			FileCacheManager::CacheRemoveStatus status =
				RemoveCacheFile(context, cacheFile.url, waitForLock);
			if (status == FileCacheManager::CacheRemoveStatus::LOCK_NOT_ACQUIRED)
			{
				action = SKIPPED_CONCURRENT_MODIFY;
			}
			else
			{
				totalCacheSize -= cacheFile.fileSize;
			}

			actions.push_back({
				.url = cacheFile.url,
				.fileSize = cacheFile.fileSize,
				.action = action
			});
		}
		else if (cacheFile.needsDownload)
		{
			/* a candidate is among the older files, don't download it */
			cacheFile.needsDownload = false;
			queueSize -= cacheFile.fileSize;

			actions.push_back({
				.url = cacheFile.url,
				.fileSize = cacheFile.fileSize,
				.action = SKIPPED_TOO_OLD
			});
		}
	}

	/* download all the remaining candidates */
	for (const CacheItem& cacheFile : cacheFiles)
	{
		if (!cacheFile.needsDownload)
			continue;

		try
		{
			/* we already took the lock */
			bool withLock = false;
			bool force = false;
			CacheActionType action = ADDED;

			/* for background tasks, we skip if lock cannot be acquired */
			bool waitForLock = false;

			int64_t cached = CacheFile(context, cacheFile.url, force, waitForLock);
			if (cached == LOCK_CANNOT_BE_ACQUIRED)
			{
				action = SKIPPED_CONCURRENT_MODIFY;
			}
			else if (cached == DIRECTORY_CANNOT_CREATED)
			{
				action = SKIPPED_DIRECTORY_DOES_NOT_EXIST;
			}

			actions.push_back({
				.url = cacheFile.url,
				.fileSize = cacheFile.fileSize,
				.action = action
			});
		}
		catch (std::exception &ex)
		{
			actions.push_back({
				.url = cacheFile.url,
				.fileSize = cacheFile.fileSize,
				.action = ADD_FAILED
			});
		}
	}

	return actions;
}


/*
 * ListCache returns a list of cached files.
 */
vector<CacheItem>
FileCacheManager::ListCache(ClientContext &context)
{
	FileOpener *opener = context.client_data->file_opener.get();
	FileSystem &file_system = FileSystem::GetFileSystem(context);

	/* determine the cache directory */
	string cacheDir;

	if (!TryGetCacheDir(opener, cacheDir))
		/* caching not configured */
		throw InvalidInputException("caching is currently disabled");

	/* construct a combined list of cached files */
	vector<CacheItem> cacheFiles;

	/* get all the files in the cache directory */
	vector<OpenFileInfo> cachedFileNames = file_system.Glob(cacheDir + "**");

	for (const OpenFileInfo& cachedFilePath : cachedFileNames)
	{
		/* skip files that are in progress or failed during staging */
		if (!IsFinalizedCachePath(cachedFilePath.path))
		{
			continue;
		}

		struct stat cachedFileStat;
		if (stat(cachedFilePath.path.c_str(), &cachedFileStat) < 0)
		{
			/* file was concurrently removed? */
			continue;
		}

		/* construct file metadata */
		CacheItem cachedFile = {
			.url = GetURLForCacheFilePath(cacheDir, cachedFilePath.path),
			.cacheFilePath = cachedFilePath.path,
			.fileSize = cachedFileStat.st_size,
			.lastAccessTime = cachedFileStat.st_atime,
			.isCandidate = false,
			.needsDownload = false
		};

		/* make a deep copy and push it onto the vector */
		cacheFiles.push_back(cachedFile);
	}

	return cacheFiles;
}


/*
 * FileCacheManager gets a pointer to the main file cache manager that
 * is shared across client contexts.
 */
shared_ptr<FileCacheManager>
FileCacheManager::Get(ClientContext &context)
{
	ObjectCache &cache = ObjectCache::GetObjectCache(context);
	return cache.GetOrCreate<FileCacheManager>(CACHE_MANAGER_KEY);
}


/*
* GetFileCacheActivity returns a shared pointer to a FileCacheActivity
* for a given path. If the path is not in the map, a new FileCacheActivity
* is created and added to the map.
*
* The caller should hold the lock on the FileCacheActivity to perform
* any operation on the cache.
*/
shared_ptr<FileCacheActivity>
FileCacheManager::GetFileCacheActivity(const string& path)
{
	lock_guard<mutex> lock(cacheActivityMapAccessLock);

	auto it = cacheActivityMap.find(path);
	if (it == cacheActivityMap.end())
	{
		shared_ptr<FileCacheActivity> activity = make_shared_ptr<FileCacheActivity>();
		cacheActivityMap[path] = activity;
		return activity;
	}
	else
	{
		return it->second;
	}
}

/*
* GetCacheStatusWithLock returns a tuple of a lock and the state of a file in the cache.
* The lock is acquired on the FileCacheActivity for the given path.
*/
CacheLockStatus
FileCacheManager::GetCacheStatusWithLock(string cacheFilePath, bool waitForLock)
{
	bool lockAcquired = true;
	unique_lock<mutex> lock = TryAcquireCachePathLock(cacheFilePath, waitForLock, lockAcquired);

	if (!lockAcquired)
	{
		/*
		 * We cannot pass the ownership of lock to the caller as we have not acquired the lock. Instead, give
		 * an a lock that is not used by anyone. The callers never use this lock as localAcquired = false.
		 */
		return CacheLockStatus(cacheFilePath, unique_lock<mutex>(), false);
	}

	return CacheLockStatus(cacheFilePath, std::move(lock), true);
}


/*
* TryAcquireCachePathLock tries to acquire a lock on the FileCacheActivity for a given path.
* If dontWait is true, it will try to acquire the lock without waiting.
*/
unique_lock<mutex>
FileCacheManager::TryAcquireCachePathLock(const string& path, bool waitForLock, bool &acquired)
{
	PGDUCK_SERVER_DEBUG("Trying to acquire a lock on %s for cache", path.c_str());

	shared_ptr<FileCacheActivity> activity = GetFileCacheActivity(path);

	unique_lock<mutex> lock(activity->GetFileCacheActivityMutex(), std::defer_lock);
	if (!waitForLock)
	{
		acquired = lock.try_lock();
		if (!acquired)
			PGDUCK_SERVER_DEBUG("Could not acquire the lock on %s for cache", path.c_str());
	}
	else
	{
		lock.lock();
		PGDUCK_SERVER_DEBUG("Acquired a lock on %s for cache", path.c_str());
	}

	return std::move(lock);
}


/*
* We keep track of the cache activity for each file in the cache via a Map.
* This function removes the cache activity from the map if the mutex is no longer
* needed.
*/
void
FileCacheManager::RemoveCacheFileActivityFromMapIfNeeded(const string& cacheFilePath)
{
	lock_guard<mutex> lock(cacheActivityMapAccessLock);
	auto it = cacheActivityMap.find(cacheFilePath);

	/*
	 * Check if the mutex exists in the map and it is referenced once.
	 * The reference is hold by the cacheActivityMap, it means that we
	 * are safe to remove the mutex from the cacheActivityMap.
	 *
	 * In case there are more than 1 references, it means that the
	 * mutex is used by another process, waiting on the mutex. In
	 * such cases, we should not remove the mutex from cacheActivityMap.
	 */
	if (it != cacheActivityMap.end() && it->second.use_count() == 1)
	{
		/*
		 * Check if the mutex is no longer in use elsewhere, the use_count() should be 2.
		 * One pointer is hold by the caller and the other is hold by the map.
		 */
		cacheActivityMap.erase(it);
	}
}


/*
 * UpdateAccessTime sets the access time of a file, since file systems won't
 * do this reliably. This causes some additional I/O, though it shouldn't be
 * an issue on a fast (e.g. Nvme) cache drive.
 */
void
FileCacheManager::UpdateAccessTime(string &filePath)
{
	struct utimbuf newTimes;

	/* set access and modification times to current time */
	newTimes.actime = time(NULL);
	newTimes.modtime = time(NULL);

	/*
	 * Update the access and modification times for the file. We
	 * ignore errors, since it's not critical and if something is
	 * wrong we will error soon after.
	 */
	utime(filePath.c_str(), &newTimes);
}


} // namespace duckdb
