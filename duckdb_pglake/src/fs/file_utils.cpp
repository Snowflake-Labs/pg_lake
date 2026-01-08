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

	destinationHandle->Sync();
	destinationHandle->Close();
	sourceHandle->Close();

	return totalBytesWritten;
}

} // namespace duckdb
