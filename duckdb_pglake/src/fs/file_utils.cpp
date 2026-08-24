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

#include "duckdb.hpp"
#include "s3fs.hpp"
#include "duckdb/common/local_file_system.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include "pg_lake/fs/caching_file_system.hpp"
#include "pg_lake/fs/file_utils.hpp"
#include "pg_lake/fs/httpfs_extended.hpp"
#include "pg_lake/fs/region_aware_s3fs.hpp"

namespace duckdb {


/*
 * ExtractDirName returns the directory name of a path, so for /tmp/cache/abc
 * it would return /tmp/cache/
 */
string
FileUtils::ExtractDirName(const string &path)
{
    if (path.empty())
		return string();

	auto last_slash_index = path.rfind('/');

	if (last_slash_index == std::string::npos)
		/* No slash, return empty string */
		return string();

    return path.substr(0, last_slash_index + 1);
}


/*
 * ExtractFileName returns the file name of a path, so for /tmp/cache/abc
 * it would return abc
 */
string
FileUtils::ExtractFileName(const string &path)
{
    if (path.empty())
		return string();

	auto last_slash_index = path.rfind('/');

	if (last_slash_index == std::string::npos)
		/* No slash, return whole path */
		return path;

    return path.substr(last_slash_index + 1);
}


/*
 * EnsureLocalDirectoryExists ensures the directory pointed to
 * by dirPath exists, by repeatedly calling mkdir for all parts (like mkdir -p).
 *
 * This pattern is copied from DuckDB code (e.g. LocalFileSecretStorage::WriteSecret)
 */
bool
FileUtils::EnsureLocalDirectoryExists(ClientContext &context,
									  string dirPath)
{
	if (dirPath.empty())
		return false;

	LocalFileSystem fs;

	if (!fs.DirectoryExists(dirPath)) {
		string separator = fs.PathSeparator(dirPath);

		/* split a path like /tmp/cache/abc/ into [tmp, cache, abc] */
		vector<string> splits = StringUtil::Split(dirPath, separator);

		/* add back the / at the start (if any) */
		string directoryPrefix;
		if (StringUtil::StartsWith(dirPath, separator)) {
			directoryPrefix = separator; // slash is swallowed by Split otherwise
		}

		for (auto &split : splits) {
			/* keep appending each part to directoryPrefix */
			directoryPrefix = directoryPrefix + split + separator;

			if (!fs.DirectoryExists(directoryPrefix))
			{
				try
				{
					fs.CreateDirectory(directoryPrefix);
				}
				catch (Exception &ex)
				{
					ErrorData error(ex);

					PGDUCK_SERVER_LOG("Creating the directory %s failed with an error %s", dirPath.c_str(), error.Message().c_str());
					return false;
				}
			}
		}
	}

	/* either already exists or newly created */
	return true;
}


/*
 * CopyFile copies a file from sourcePath to destinationPath via
 * the virtual file system, currently using a single thread.
 */
int64_t
FileUtils::CopyFile(ClientContext &context,
					string &sourcePath,
					string &destinationPath)
{
	FileSystem &fileSystem = FileSystem::GetFileSystem(context);

	unique_ptr<FileHandle> sourceHandle =
		fileSystem.OpenFile(sourcePath, FileFlags::FILE_FLAGS_READ);

	unique_ptr<FileHandle> destinationHandle =
		fileSystem.OpenFile(destinationPath, FileFlags::FILE_FLAGS_WRITE  | FileFlags::FILE_FLAGS_FILE_CREATE_NEW);

	int64_t totalBytesWritten = 0L;

	/*
	 * Taken before the copy so it can be compared against what we actually
	 * transferred. Zero means the source does not report a size -- an http
	 * server that sends no Content-Length, for instance -- in which case there
	 * is nothing to compare against.
	 */
	idx_t sourceSize = sourceHandle->GetFileSize();

	if (sourceHandle->file_system.GetName() == "HTTPFileSystem")
	{
		/* when opening http(s), we go through CachedFileSystem */
		FileHandle &sourceHandleRef = *sourceHandle;
		CachingFSFileHandle &cachedHandleRef = sourceHandleRef.Cast<CachingFSFileHandle>();
		FileHandle &wrappedHandleRef = *cachedHandleRef.wrappedHandle;

		/*
		 * Use a specialized download function for http(s) requests,
		 * to avoid range requests and for faster cancellations.
		 */
		PgLakeHTTPFileSystem httpfs;
		totalBytesWritten = httpfs.Download(context, wrappedHandleRef, wrappedHandleRef.path, {}, *destinationHandle);
	}
	else if (sourceHandle->file_system.GetName() == "RegionAwareS3FileSystem")
	{
		/* when opening s3/gs, we go through CachedFileSystem */
		FileHandle &sourceHandleRef = *sourceHandle;
		CachingFSFileHandle &cachedHandleRef = sourceHandleRef.Cast<CachingFSFileHandle>();
		FileHandle &wrappedHandleRef = *cachedHandleRef.wrappedHandle;

		/*
		 * Use a specialized download function for S3 requests for
		 * to avoid range requests and for faster cancellations.
		 */
		RegionAwareS3FileSystem &s3fs = wrappedHandleRef.file_system.Cast<RegionAwareS3FileSystem>();
		totalBytesWritten = s3fs.Download(context, wrappedHandleRef, *destinationHandle);
	}
	else
	{
		/*
		 * We allocate a rather huge buffer here because it will cause s3fs
		 * to make a range request of this size. By default, s3fs will make
		 * 1 MiB range requests, which on high bandwidth connections will
		 * experience substantial overhead and significant underutilization
		 * (due to connection establishment, SSL handshakes, TCP warm-up, etc.).
		 * Consider 15Gbps (e.g. m7gd.4xlarge) and 10-20ms overhead per request,
		 * then we would have <4% utilization.
		 *
		 * We pick a value that's >>128MiB such that we can typically download
		 * Parquet files that are cut off when they reach >128MiB in a single
		 * request. On a 15Gbps connection we would do 10 range requests per
		 * second. Assuming 10-20ms overhead per request, we would get 80-90%
		 * utilization.
		 *
		 * While this is a lot of memory, it is still a lot less than
		 * what a run-of-the-mill OLAP query will use.
		 */
		constexpr const idx_t BUFFER_SIZE = 150*1024*1024;
		unique_ptr<char[]> buffer(new char [BUFFER_SIZE]);

		int64_t bytesRead = 0L;

		while ((bytesRead = sourceHandle->Read(buffer.get(), BUFFER_SIZE)) > 0)
		{
			totalBytesWritten += destinationHandle->Write(buffer.get(), bytesRead);
		}
	}

	/*
	 * A transfer that reports success is not the same as one that delivered
	 * everything: a retried http/s3 body can leave a partial attempt followed by
	 * a complete one, and the generic loop stops at the first zero-length read.
	 * Since the result may be renamed into the cache, and nothing revalidates a
	 * cache entry afterwards, either shape would be served from then on.
	 *
	 * Checked before finalizing, so nothing bad is published: a cache fill
	 * leaves its staging file for the sweep in ManageCache, and an upload never
	 * appears because Sync() is what completes the multipart upload.
	 */
	if (sourceSize > 0 && (idx_t) totalBytesWritten != sourceSize)
	{
		/* a cache fill asks for nocache<url>, which is no use to the reader */
		string reportedPath =
			StringUtil::StartsWith(sourcePath, NO_CACHE_PREFIX)
			? sourcePath.substr(NO_CACHE_PREFIX.length())
			: sourcePath;

		throw IOException("Copied %llu bytes of '%s' but it is %llu bytes; the "
						  "transfer was incomplete or was retried",
						  (unsigned long long) totalBytesWritten, reportedPath,
						  (unsigned long long) sourceSize);
	}

	destinationHandle->Sync();
	destinationHandle->Close();
	sourceHandle->Close();

	return totalBytesWritten;
}


/*
 * IsOwnedByCurrentUser returns true iff the path exists as a regular file
 * owned by the effective UID of this process.
 *
 * Cache paths are fully deterministic
 * (<cache_dir>/<proto>/<bucket>/.../pgl-cache.<file>), so a local user who
 * pre-creates a file at that path before pgduck_server downloads the real
 * object would otherwise serve arbitrary content for every pg_lake query
 * that hits the cache.  Checking ownership before trusting a cached file
 * closes this window: a file owned by another user fails the check and
 * pgduck_server re-downloads and replaces it.
 */
bool
FileUtils::IsOwnedByCurrentUser(const string &path)
{
	struct stat st;

	if (lstat(path.c_str(), &st) != 0)
		return false;   /* does not exist or cannot stat */

	if (!S_ISREG(st.st_mode))
		return false;   /* symlink, directory, device, etc. */

	return st.st_uid == geteuid();
}


} // namespace duckdb
