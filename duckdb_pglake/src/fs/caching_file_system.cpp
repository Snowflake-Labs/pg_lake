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
#include <inttypes.h>
#include <regex>

#include "crypto.hpp"
#include "duckdb.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/types/blob.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httpfs.hpp"
#include "httplib.hpp"

#include "pg_lake/fs/caching_file_system.hpp"
#include "pg_lake/fs/file_cache_manager.hpp"
#include "pg_lake/fs/file_utils.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"

namespace duckdb {

/*
 * OpenFile opens a file handle that wraps around an remote FileHandle or a local
 * FileHandle, depending on whether the file is cached.
 *
 * It returns a unique_ptr, which means the FileHandle is destroyed as soon
 * as the return value goes out of scope on the caller side.
 */
unique_ptr<FileHandle>
CachingFileSystem::OpenFile(const string &fullUrl,
							FileOpenFlags openFlags,
							optional_ptr<FileOpener> opener)
{
	if (!opener)
		/* this probably cannot happen, but let's be defensive and let DuckDB handle it */
		return remoteFs->OpenFile(fullUrl, openFlags, opener);

	optional_ptr<ClientContext> context = opener->TryGetClientContext();

	if (!context)
		/* we're outside of a client context, let remote FS handle it directly */
		return remoteFs->OpenFile(fullUrl, openFlags, opener);

	/* check whether caching is allowed */
	string url = fullUrl;
	bool isCacheAllowed = true;

	if (StringUtil::StartsWith(url, NO_CACHE_PREFIX))
	{
		/* URL is prefixed like nocaches3:// , we should not cache */
		url = url.substr(NO_CACHE_PREFIX.length());
		isCacheAllowed = false;
	}

	shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(*context);

	string cacheDir;
	string cacheFilePath;

	/*
	 * cache only if the URL does not start with nocache, and caching is
	 * enabled on the system.
	 */
	bool requestCache = isCacheAllowed &&
						cacheManager->TryGetCacheDir(opener, cacheDir) &&
						cacheManager->TryGetCacheFilePath(cacheDir, url, cacheFilePath);

	unique_ptr<FileHandle> wrappedHandle;
	unique_ptr<FileHandle> cacheOnWriteHandle;
	string cacheOnWritePath;
	unique_lock<mutex> cacheOnWriteFileLock;

	/* the file is already in cache, read from the cache */
	if (requestCache && openFlags.OpenForReading() != 0 &&
		localfs.FileExists(cacheFilePath))
	{
		/* we track access times in the file system */
		FileCacheManager::UpdateAccessTime(cacheFilePath);

		/*
		 * S3 files may be opened with FILE_FLAGS_DIRECT_IO to skip internal
		 * buffering in httpfs, but passing on that flag to the local file
		 * system would cause us to use Linux' direct I/O, which requires reads
		 * to align with the disk block size. Since DuckDB does not (expect to
		 * have to) do that, the read might fail.
		 *
		 * Hence, we reconstruct the flags without FILE_FLAGS_DIRECT_IO. The
		 * local file system does not do any internal buffering that we'd need
		 * to worry about.
		 *
		 * See https://github.com/PgLakeData/pg_lake_data_warehouse/pull/218 for
		 * details.
		 */
		FileOpenFlags localFlags(FileOpenFlags::FILE_FLAGS_READ,
								 openFlags.Lock(),
								 openFlags.Compression());

		/* create a handle for the file in cache */
		wrappedHandle = localfs.OpenFile(cacheFilePath, localFlags);

		PGDUCK_SERVER_DEBUG("using local cache for %s", cacheFilePath.c_str());
	}
	else
	{
		/* create a handle for the remote file */
		wrappedHandle = remoteFs->OpenFile(url, openFlags, opener);

		if (requestCache)
		{
			if (openFlags.OpenForReading())
			{
				/*
				* File is eligible for caching, but not yet in the cache. Register it
				* as a cache candidate.
				*
				* We do this after OpenFile has had the opportunity to throw an exception
				* if the file is not accessible, in which case we do not want to try
				* caching.
				*/
				cacheManager->queue.RecordCacheCandidate(url, cacheFilePath,
														 remoteFs->GetFileSize(*wrappedHandle));
			}
			else if (openFlags.OpenForWriting())
			{
				shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(*context);
				bool waitForLock = true;

				CacheLockStatus writeCacheLockStatus =
					cacheManager->GetCacheStatusWithLock(cacheFilePath, waitForLock);

				/*
				 * We have to keep the lock until we finish writing to the file
				 * such that no concurrent modification can happen to the same
				 * file.
				*/
				cacheOnWriteFileLock = std::move(writeCacheLockStatus.lock);

				/*
				* We want to write the file to the cache as well. We need to create a
				* local file handle for the cache file, so we can write to it.
				*/
				string cacheFileDir = FileUtils::ExtractDirName(cacheFilePath);
				FileUtils::EnsureLocalDirectoryExists(*context, cacheFileDir);

				cacheOnWriteHandle =
					localfs.OpenFile(cacheFilePath + cacheManager->STAGING_SUFFIX,
									 FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE);

				cacheOnWritePath = cacheFilePath;
			}
		}
	}

	/* wrap the file handles */
	return make_uniq<CachingFSFileHandle>(*this,
										url,
										openFlags,
										context,
										std::move(wrappedHandle),
										std::move(cacheOnWriteHandle),
										cacheOnWritePath,
										std::move(cacheOnWriteFileLock));
}


/*
* Checks if the file is eligible for cache-on-write. If the file is eligible,
* return true; otherwise, return false.
*/
bool
CachingFileSystem::ShouldCacheOnWrite(CachingFSFileHandle &pg_lakeHandle, int64_t additionalByteCount)
{
	if (pg_lakeHandle.cacheOnWriteHandle == nullptr)
		return false;

	Value setting;
	ClientContext &context = *pg_lakeHandle.context;
	FileOpener *opener = context.client_data->file_opener.get();

	if (!opener->TryGetCurrentSetting(CACHE_ON_WRITE_MAX_SIZE, setting))
		return false;

	int64_t cacheOnWriteMaxAllowedSize = setting.GetValue<uint64_t>();
	if (pg_lakeHandle.cacheOnWriteWrittenBytes + additionalByteCount >= cacheOnWriteMaxAllowedSize)
	{
		PGDUCK_SERVER_DEBUG(
			"Total number of bytes that will be written (%" PRId64 ") is greater than "
			"pg_lake_cache_on_write_max_size (%" PRId64 "). "
			"Disabling cache-on-write for this file %s%s.",
			pg_lakeHandle.cacheOnWriteWrittenBytes + additionalByteCount,
			cacheOnWriteMaxAllowedSize,
			pg_lakeHandle.cacheOnWritePath.c_str(),
			".pgl-stage"
		);

		return false;
	}

	return true;
}

/*
* Helper function to remove the cache-on-write staged file and release the
* file cache lock.
*/
void
CachingFileSystem::CleanUpCacheOnWriteFile(CachingFSFileHandle &pg_lakeHandle)
{
	if (pg_lakeHandle.cacheOnWriteHandle != nullptr)
	{
		shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(*pg_lakeHandle.context);
		pg_lakeHandle.cacheOnWriteHandle->Close();
		localfs.RemoveFile(pg_lakeHandle.cacheOnWritePath + ".pgl-stage");

		/* make sure we release the lock and remove its references */
		pg_lakeHandle.cacheOnWriteFileLock.unlock();
		pg_lakeHandle.cacheOnWriteFileLock.release();
		cacheManager->RemoveCacheFileActivityFromMapIfNeeded(pg_lakeHandle.cacheOnWritePath);
		pg_lakeHandle.cacheOnWriteHandle = nullptr;
	}
}


/*
 * Glob is the file system function for listing files.
 *
 * If a caller uses the nocache prefix, then we should strip it before calling
 * Glob on the remote file system, since it would not know what to do with it.
 * We also need to re-add it afterwards because the caller might use the result
 * to call OpenFile and that's when we actually use the nocache prefix.
 *
 * We do not currently never cache Glob results, so we always get an up-to-date
 * view of the remote file list and can selectively use caching when opening
 * them.
 */
vector<OpenFileInfo>
CachingFileSystem::Glob(const string &urlPattern, FileOpener *opener)
{
	if (StringUtil::StartsWith(urlPattern, NO_CACHE_PREFIX))
	{
		/* do the Glob without the prefix */
		vector<OpenFileInfo> result = remoteFs->Glob(urlPattern.substr(NO_CACHE_PREFIX.length()), opener);

		/* add the prefix back to the results to pass nocache through */
		for (OpenFileInfo& url : result)
			url.path = NO_CACHE_PREFIX + url.path;

		return result;
	}
	else
		return remoteFs->Glob(urlPattern, opener);
}


/*
 * Copied from S3FileSystem because we want to call our own Glob
 * with some modifications to avoid showing nocache prefix.
 */
bool
CachingFileSystem::ListFiles(const string &directory,
							 const std::function<void(const string &, bool)> &callback,
							 FileOpener *opener)
{
	string trimmed_dir = directory;
	StringUtil::RTrim(trimmed_dir, PathSeparator(trimmed_dir));
	auto globResult = Glob(JoinPath(trimmed_dir, "**"), opener);

	if (globResult.empty()) {
		return false;
	}

	bool noCache = StringUtil::StartsWith(directory, NO_CACHE_PREFIX);

	for (const OpenFileInfo &file : globResult) {
		string url = noCache ? file.path.substr(NO_CACHE_PREFIX.length()) : file.path;
		callback(url, false);
	}

	return true;
}


/*
 * RemoveFile ensures that a file is also removed from cached after
 * removal from the remote file system.
 */
void
CachingFileSystem::RemoveFile(const string &filename,
							  optional_ptr<FileOpener> opener)
{
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(*context);
	string cacheDir;
	string cacheFilePath;

	remoteFs->RemoveFile(filename, opener);

	/*
	 * Even if the file no longer exists, we always remove from cache,
	 * since we may have failed to do so last time.
	 *
	 * If the file is not cached then this is a noop.
	 */
	if (cacheManager->TryGetCacheDir(opener, cacheDir) &&
		cacheManager->TryGetCacheFilePath(cacheDir, filename, cacheFilePath))
	{
		bool waitForLock = true;
		cacheManager->RemoveCacheFile(*context, filename, waitForLock);
	}
}


} // namespace duckdb
