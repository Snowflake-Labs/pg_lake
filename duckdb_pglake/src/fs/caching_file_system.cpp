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
#include "pg_lake/fs/region_aware_s3fs.hpp"
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
PGLakeCachingFileSystem::OpenFile(const string &fullUrl,
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
		FileUtils::IsOwnedByCurrentUser(cacheFilePath))
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

		/*
		 * Nothing holds the per-path cache lock across the decision above and
		 * this open, so the entry can stop being usable in between -- cache
		 * management evicting it under pressure is the ordinary case, and the
		 * check itself only establishes that a regular file owned by us was
		 * there a moment ago. Fall back to the remote file rather than failing
		 * the statement: a re-read costs a download, whereas propagating the
		 * open failure kills a query over an object that is perfectly readable
		 * in storage, and reports it as a bare "Cannot open file" naming an
		 * internal cache path.
		 */
		try
		{
			/* create a handle for the file in cache */
			wrappedHandle = localfs.OpenFile(cacheFilePath, localFlags);

			PGDUCK_SERVER_DEBUG("using local cache for %s", cacheFilePath.c_str());
		}
		catch (IOException &ex)
		{
			/*
			 * Only IOException: that is what LocalFileSystem::OpenFile raises
			 * when open() fails, which is the case worth absorbing. Anything
			 * else out of this call -- an InternalException, an unsupported
			 * compression type -- is a bug rather than a lost race, and
			 * quietly turning it into a remote read would hide it.
			 */
			ErrorData error(ex);

			PGDUCK_SERVER_DEBUG("cannot use local cache for %s, reading from "
								"the remote file instead: %s",
								cacheFilePath.c_str(), error.Message().c_str());
		}
	}

	if (wrappedHandle == nullptr)
	{
		/* create a handle for the remote file */
		try
		{
			wrappedHandle = remoteFs->OpenFile(url, openFlags, opener);
		}
		catch (Exception &ex)
		{
			ErrorData error(ex);
			if (error.Type() == ExceptionType::HTTP)
				PGDUCK_SERVER_WARN("%.500s", error.Message().c_str());
			throw;
		}

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
				bool directoryExists = FileUtils::EnsureLocalDirectoryExists(*context, cacheFileDir);

				if (directoryExists)
				{
					try
					{
						/*
						 * FILE_FLAGS_FILE_CREATE_NEW (O_CREAT|O_TRUNC), not
						 * FILE_FLAGS_FILE_CREATE (O_CREAT): a staging file
						 * orphaned by an earlier write may still sit at this
						 * exact path, and the path is fully deterministic.
						 * Without O_TRUNC we write the new contents over its
						 * prefix and leave whatever of the older, longer file
						 * extends past them, then rename that hybrid in as a
						 * complete cache entry. Readers trust a finalized cache
						 * file without revalidating it against object storage,
						 * so that poisons every later read of this URL until
						 * the entry is evicted.
						 *
						 * Do not assume orphans are rare. ~CachingFSFileHandle()
						 * only removes one when it actually runs, so a hard
						 * crash or kill escapes it -- and before that cleanup
						 * existed, so did every ordinary caught abort: a
						 * runtime error mid-write, a cancellation, or an ENOSPC
						 * that DuckDB throws and pgduck_server catches. That
						 * leaves an orphan with the process still running and
						 * its pid unchanged, which is what was observed in the
						 * field.
						 */
						cacheOnWriteHandle =
							localfs.OpenFile(cacheFilePath + cacheManager->STAGING_SUFFIX,
											 FileOpenFlags::FILE_FLAGS_WRITE | FileOpenFlags::FILE_FLAGS_FILE_CREATE_NEW);

						cacheOnWritePath = cacheFilePath;
					}
					catch (Exception &ex)
					{
						ErrorData error(ex);

						PGDUCK_SERVER_DEBUG("cannot use local cache for %s because the file "
											"cannot be opened for write: %s", cacheFilePath.c_str(), error.Message().c_str());
						cacheOnWriteHandle = nullptr;
					}
				}
				else
				{
					PGDUCK_SERVER_DEBUG("cannot use local cache for %s because the cache directory cannot "
										"be created", cacheFilePath.c_str());
				}
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
PGLakeCachingFileSystem::ShouldCacheOnWrite(CachingFSFileHandle &pg_lakeHandle, int64_t additionalByteCount)
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
PGLakeCachingFileSystem::CleanUpCacheOnWriteFile(CachingFSFileHandle &pg_lakeHandle)
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
PGLakeCachingFileSystem::Glob(const string &urlPattern, FileOpener *opener)
{
	string url = urlPattern;
	bool noCache = false;

	if (StringUtil::StartsWith(url, NO_CACHE_PREFIX))
	{
		url = url.substr(NO_CACHE_PREFIX.length());
		noCache = true;
	}

	vector<OpenFileInfo> result = remoteFs->Glob(url, opener);

	/*
	 * Iceberg partition paths can contain glob characters like * as literal
	 * characters in directory names (e.g., "specialChars!@#$%^&*()_+").
	 * DuckDB's glob machinery interprets these as wildcards, causing S3
	 * ListObjects to search with a truncated prefix and find nothing.
	 *
	 * Only when the glob found nothing, check whether the pattern refers to
	 * an actual file. A pattern that expands to at least one file keeps its
	 * usual meaning, and ordinary paths without glob characters never pay for
	 * the extra existence check.
	 */
	if (result.empty() && HasGlob(url) && remoteFs->FileExists(url, opener))
		result.push_back(OpenFileInfo(url));

	if (noCache)
	{
		for (OpenFileInfo& fileInfo : result)
			fileInfo.path = NO_CACHE_PREFIX + fileInfo.path;
	}

	return result;
}


/*
 * Copied from S3FileSystem because we want to call our own Glob
 * with some modifications to avoid showing nocache prefix.
 */
bool
PGLakeCachingFileSystem::ListFiles(const string &directory,
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
 * RemoveCachedCopy drops the local cache entry for a file that has been removed
 * from the remote file system.
 *
 * Even if the file no longer exists remotely, we always remove from cache,
 * since we may have failed to do so last time.
 *
 * If the file is not cached then this is a noop.
 */
static void
RemoveCachedCopy(ClientContext &context, const string &filename,
				 optional_ptr<FileOpener> opener)
{
	shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(context);
	string cacheDir;
	string cacheFilePath;

	if (cacheManager->TryGetCacheDir(opener, cacheDir) &&
		cacheManager->TryGetCacheFilePath(cacheDir, filename, cacheFilePath))
	{
		bool waitForLock = true;
		cacheManager->RemoveCacheFile(context, filename, waitForLock);
	}
}


/*
 * IsAzureUrl returns whether a URL is handled by the azure extension. Matching
 * on the scheme rather than asking the file system keeps this usable without an
 * instance of it, which is what the static RemoveFiles below has.
 */
static bool
IsAzureUrl(const string &url)
{
	return StringUtil::StartsWith(url, "azure://") ||
		   StringUtil::StartsWith(url, "az://") ||
		   StringUtil::StartsWith(url, "abfss://") ||
		   StringUtil::StartsWith(url, "abfs://");
}


/*
 * RemoveFile ensures that a file is also removed from cached after
 * removal from the remote file system.
 */
void
PGLakeCachingFileSystem::RemoveFile(const string &filename,
							  optional_ptr<FileOpener> opener)
{
	optional_ptr<ClientContext> context = opener->TryGetClientContext();

	remoteFs->RemoveFile(filename, opener);

	RemoveCachedCopy(*context, filename, opener);
}


/*
 * RemoveFiles removes many files, batching the requests where the back end
 * supports it.
 *
 * It is declared static in the class (C++ does not let the definition repeat
 * that): a caller holding only a ClientContext cannot reach the
 * PGLakeCachingFileSystem instance registered in the virtual file system, so
 * this looks up what it needs from the context, as the file system functions in
 * functions.cpp do.
 */
void
PGLakeCachingFileSystem::RemoveFiles(ClientContext &context, const vector<string> &paths)
{
	if (paths.empty())
		return;

	FileOpener *opener = context.client_data->file_opener.get();
	DatabaseInstance &db = DatabaseInstance::GetDatabase(context);
	RegionAwareS3FileSystem s3fs(BufferManager::GetBufferManager(db));
	FileSystem &virtualFs = FileSystem::GetFileSystem(context);

	vector<string> s3Paths;

	for (const string &path : paths)
	{
		/*
		 * Only S3 has a bulk delete API. Everything else -- Azure, HTTP, local
		 * -- goes through the ordinary per-file RemoveFile, which is one round
		 * trip each and evicts the cache on the way out. Paths that opt out of
		 * caching land here too: s3fs does not recognize the prefix, and the
		 * virtual file system strips it.
		 *
		 * No opener here, unlike the s3fs call below: what a ClientContext hands
		 * out is a ClientContextFileSystem, an OpenerFileSystem, which pushes
		 * its own opener into every call and rejects one from the caller with
		 * "OpenerFileSystem cannot take an opener". That is an InternalException,
		 * which takes the whole server down rather than failing the statement.
		 */
		if (s3fs.CanHandleFile(path))
			s3Paths.push_back(path);
		else if (IsAzureUrl(path))
			/*
			 * On Azure, an object that is already gone counts as removed, the
			 * same as for the S3 batch delete below, so that a deletion queue
			 * record for an externally removed object gets retired instead of
			 * retried. TryRemoveFile is DeleteIfExists there, so it costs no
			 * extra request. Other back ends keep RemoveFile, whose failure is
			 * what makes an unremovable path retryable.
			 */
			virtualFs.TryRemoveFile(path);
		else
			virtualFs.RemoveFile(path);
	}

	if (s3Paths.empty())
		return;

	/*
	 * The batch delete goes straight to S3 rather than through this wrapper, so
	 * evict the cache entries here instead, both before and after.
	 *
	 * Before, because RemoveFiles sends the keys in batches and throws as soon as
	 * one batch reports an error: evicting only afterwards would leave the
	 * already-deleted batches holding a local copy, which is the stale cache this
	 * is meant to prevent. Dropping the copy of a file whose delete then fails
	 * only costs a re-download.
	 *
	 * After, because a concurrent reader can cache the file again in the window
	 * between the eviction and the delete. Eviction of an uncached file is a
	 * cheap no-op, so the second pass costs little.
	 */
	for (const string &path : s3Paths)
		RemoveCachedCopy(context, path, opener);

	s3fs.RemoveFiles(s3Paths, opener);

	for (const string &path : s3Paths)
		RemoveCachedCopy(context, path, opener);
}


} // namespace duckdb
