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
#include <sys/statvfs.h>
#include <unistd.h>
#include <inttypes.h>
#include <errno.h>
#include <string.h>

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
		   StringUtil::StartsWith(url, "hf://") ||
		   StringUtil::StartsWith(url, "r2://");
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

	if (!force && FileUtils::IsOwnedByCurrentUser(finalCacheFilePath))
	{
		/* count explicit cache operation as an access */
		UpdateAccessTime(finalCacheFilePath);

		/* we do not want to cache the file again, so 0 bytes copied */
		return 0;
	}

	PGDUCK_SERVER_DEBUG("adding %s to cache", finalCacheFilePath.c_str());

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
	if (status != CacheRemoveStatus::LOCK_NOT_ACQUIRED)
	{
		/*
		* Also remove Parquet metadata from in-memory cache to not leak
		* memory and in case the user is doing an explicit removal because
		* the file changed. We do this after removing the file, to ensure
		* future readers do not see metadata. We also do it if there was
		* no file, to not leave any cache remnants when explicitly uncaching
		* a file. If we could not acquire the lock, then a concurrent cache
		* mutation is still in progress and we must not invalidate its
		* metadata view underneath it.
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
			PGDUCK_SERVER_DEBUG("removing %s from cache", filePath.c_str());
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
 * TryGetInodeStats reports the number of available and total inodes on the file
 * system that contains the given path.
 *
 * Returns false if the file system does not give us numbers we can manage the
 * cache with, in which case we manage it by size only. File systems that
 * allocate inodes dynamically (e.g. btrfs, ZFS) report 0. The statvfs fields
 * are unsigned and there is no portable value for "unknown", so a file system
 * that reports (fsfilcnt_t) -1 shows up here as a negative number, which we
 * also have to reject: an inode floor derived from it would put us under
 * permanent inode pressure.
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
	freeInodes = (int64_t) stats.f_favail;
	totalInodes = (int64_t) stats.f_files;

	return totalInodes > 0 && freeInodes >= 0;
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
 * Expects totalInodes > 0, which TryGetInodeStats guarantees.
 */
static int64_t
DeriveInodeFloor(int64_t totalInodes)
{
	int64_t inodeFloor = totalInodes / DEFAULT_FREE_INODE_FRACTION;

	if (inodeFloor < DEFAULT_MIN_FREE_INODES)
		inodeFloor = DEFAULT_MIN_FREE_INODES;

	if (inodeFloor > totalInodes / 2)
		inodeFloor = totalInodes / 2;

	return inodeFloor;
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
 * concurrent operation added a file to it, so we simply stop pruning then.
 *
 * The StartsWith check is what keeps us inside the cache directory. Erasing a
 * single trailing slash is enough because the only paths we get here come from
 * the directory walk in ManageCache, which does not produce empty path
 * components, and IsCacheableURL requires a non-empty scheme, so a cache path
 * cannot start with a double slash either.
 */
static int64_t
PruneEmptyCacheDirectory(const string &cacheDir, string directory)
{
	int64_t removed = 0;

	while (StringUtil::StartsWith(directory, cacheDir) && directory != cacheDir)
	{
		/* rmdir does not want the trailing slash */
		string dirWithoutSlash = directory;
		if (StringUtil::EndsWith(dirWithoutSlash, "/"))
			dirWithoutSlash.erase(dirWithoutSlash.length() - 1);

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
 * ManageCache implements a simple cache management approach where we prune
 * the least recently used files, until there is enough space for all recently
 * accessed candidates, and then we download them one by one.
 *
 * Space is both bytes and inodes: the cache can hold a large number of small
 * files, so on a file system with a fixed inode table (e.g. ext4) we can run
 * out of inodes long before we reach maxCacheSize, at which point writes to
 * the cache start failing. We therefore also evict when the number of
 * available inodes drops below minFreeInodes, which is derived from the cache
 * file system when set to MIN_FREE_INODES_AUTO, and switched off by 0.
 *
 * We only evict for inodes when the cache itself holds enough of them to get
 * back above the floor, since the file system is usually shared and we cannot
 * fix somebody else's inode use by emptying our cache.
 */
vector<CacheAction>
FileCacheManager::ManageCache(ClientContext &context, int64_t maxCacheSize,
							  int64_t minFreeInodes)
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

	/*
	 * Determine how many inodes are available on the cache file system, and
	 * how many we want to keep available.
	 */
	int64_t freeInodes = 0;
	int64_t totalInodes = 0;
	int64_t inodeFloor = 0;
	bool trackInodes = TryGetInodeStats(cacheDir, freeInodes, totalInodes);

	if (trackInodes)
		inodeFloor = minFreeInodes == MIN_FREE_INODES_AUTO ?
			DeriveInodeFloor(totalInodes) : minFreeInodes;

	/*
	 * Each cache file holds an inode, so count what we could free and what we
	 * are about to add.
	 */
	int64_t queuedFiles = 0;
	int64_t evictableFiles = 0;

	for (const CacheItem& cacheFile : cacheFiles)
	{
		if (cacheFile.needsDownload)
			queuedFiles++;
		else if (!cacheFile.isCandidate)
			evictableFiles++;
	}

	/*
	 * Reserve an inode for every file we are about to download, the same way
	 * queueSize reserves bytes for them. Without that we would evict to exactly
	 * the floor, add files, and be back under it in the next round.
	 */
	int64_t inodesNeeded =
		trackInodes ? inodeFloor + queuedFiles - freeInodes : 0;
	bool inodePressure = inodesNeeded > 0;

	/*
	 * The cache normally shares a file system with other things, so the inodes
	 * can also run out for reasons that have nothing to do with us. Evicting
	 * the whole cache does not help then: it throws away a cache we can still
	 * read from and leaves us just as far below the floor. So we only evict for
	 * inodes when our own cache files can close the gap, and otherwise leave
	 * the cache alone and stop adding to it.
	 */
	bool canRelieveInodePressure = inodePressure && evictableFiles >= inodesNeeded;
	int64_t inodeEvictionBudget = canRelieveInodePressure ? inodesNeeded : 0;

	/*
	 * Adding files to the cache consumes inodes, so when we cannot free enough
	 * of them we only evict in this round. The next round can add files again.
	 */
	bool skipDownloads = inodePressure && !canRelieveInodePressure;

	if (skipDownloads)
	{
		for (CacheItem& cacheFile : cacheFiles)
		{
			if (!cacheFile.needsDownload)
				continue;

			cacheFile.needsDownload = false;

			actions.push_back({
				.url = cacheFile.url,
				.fileSize = cacheFile.fileSize,
				.action = SKIPPED_INODE_PRESSURE
			});
		}

		queueSize = 0;
	}

	/* sort from oldest to newest access time */
	std::sort(cacheFiles.begin(), cacheFiles.end());

	int64_t prunedDirectories = 0;
	int64_t inodesFreed = 0;

	/* remove items until we have enough space for all candidates */
	for (CacheItem& cacheFile : cacheFiles)
	{
		bool needBytes = totalCacheSize + queueSize >= maxCacheSize;
		bool needInodes = inodesFreed < inodeEvictionBudget;

		if (!needBytes && !needInodes)
			/* we have enough space to fit our queue */
			break;

		if (!cacheFile.isCandidate)
		{
			PGDUCK_SERVER_DEBUG("removing %s from cache (%" PRIu64 \
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

				/*
				 * Directories also hold inodes, so reclaim the ones that no
				 * longer have any cache files in them. We do this regardless of
				 * why we are evicting, since a cache that saw many prefixes
				 * would otherwise keep holding those inodes until it does run
				 * low on them.
				 */
				int64_t pruned =
					PruneEmptyCacheDirectory(cacheDir,
											 FileUtils::ExtractDirName(cacheFile.cacheFilePath));

				prunedDirectories += pruned;

				/* removing the file freed up its inode, plus any directories */
				inodesFreed += 1 + pruned;
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

	int64_t added = 0, addedBytes = 0;
	int64_t removed = 0, removedBytes = 0;
	int64_t skippedTooLarge = 0, skippedTooOld = 0;
	int64_t skippedConcurrent = 0, addFailed = 0;
	int64_t skippedInodePressure = 0;

	for (const CacheAction &a : actions)
	{
		switch (a.action)
		{
			case ADDED:             added++; addedBytes += a.fileSize; break;
			case REMOVED:           removed++; removedBytes += a.fileSize; break;
			case SKIPPED_TOO_LARGE: skippedTooLarge++; break;
			case SKIPPED_TOO_OLD:   skippedTooOld++; break;
			case SKIPPED_CONCURRENT_MODIFY: skippedConcurrent++; break;
			case SKIPPED_INODE_PRESSURE: skippedInodePressure++; break;
			case ADD_FAILED:        addFailed++; break;
			default:                break;
		}
	}

	if (added > 0 || removed > 0 || skippedTooOld > 0 ||
		skippedTooLarge > 0 || addFailed > 0 || skippedInodePressure > 0)
	{
		PGDUCK_SERVER_LOG("cache pressure: added %" PRIu64 " files (%" PRIu64
						  " bytes), evicted %" PRIu64 " files (%" PRIu64 " bytes), "
						  "skipped %" PRIu64 " (too old), %" PRIu64 " (too large), "
						  "%" PRIu64 " (concurrent), %" PRIu64 " (add failed), "
						  "%" PRIu64 " (low on inodes); "
						  "cache now %" PRIu64 "/%" PRIu64 " bytes",
						  (uint64_t) added, (uint64_t) addedBytes,
						  (uint64_t) removed, (uint64_t) removedBytes,
						  (uint64_t) skippedTooOld,
						  (uint64_t) skippedTooLarge,
						  (uint64_t) skippedConcurrent,
						  (uint64_t) addFailed,
						  (uint64_t) skippedInodePressure,
						  (uint64_t) totalCacheSize,
						  (uint64_t) maxCacheSize);
	}

	/*
	 * Report inode pressure separately, since it is an unusual situation that
	 * operators may need to act on (e.g. by giving the cache its own volume, or
	 * a file system with dynamically allocated inodes).
	 */
	if (inodePressure)
	{
		/*
		 * Ask the file system again rather than reporting our own bookkeeping,
		 * which cannot see inodes that are still held by an open file
		 * descriptor on a cache file we unlinked, or what other processes did
		 * in the meantime. The counts are the point of this message, so we want
		 * them measured. If the call fails we keep the numbers we started with.
		 */
		int64_t measuredFreeInodes = 0;
		int64_t measuredTotalInodes = 0;

		if (TryGetInodeStats(cacheDir, measuredFreeInodes, measuredTotalInodes))
		{
			freeInodes = measuredFreeInodes;
			totalInodes = measuredTotalInodes;
		}

		PGDUCK_SERVER_LOG("cache directory %s is low on inodes: %" PRIu64
						  "/%" PRIu64 " available, keeping %" PRIu64
						  " available; freed %" PRIu64 " inodes this round by "
						  "evicting %" PRIu64 " files and %" PRIu64 " empty "
						  "directories%s",
						  cacheDir.c_str(),
						  (uint64_t) freeInodes,
						  (uint64_t) totalInodes,
						  (uint64_t) inodeFloor,
						  (uint64_t) inodesFreed,
						  (uint64_t) removed,
						  (uint64_t) prunedDirectories,
						  skipDownloads ?
						  ", cache files alone cannot free enough inodes so "
						  "nothing was added" : "");
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
