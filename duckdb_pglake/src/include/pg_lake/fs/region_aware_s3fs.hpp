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
#include "duckdb/common/local_file_system.hpp"

#include "httpfs.hpp"
#include "s3fs.hpp"

#include "pg_lake/fs/pg_lake_s3fs.hpp"

namespace duckdb {

bool AddS3ExpressRegionEndpoint(const string &url, string &expressUrl);


/*
 * RegionAwareS3FileSystem wraps around S3FileSystem to add region awareness
 * and deletion.
 *
 * The reason for wrapping, not extending, is that our variant of OpenFile
 * catches exceptions from HTTPFileSystem::OpenFile and OpenFile cannot
 * be overridden.
 */
class RegionAwareS3FileSystem : public FileSystem {
public:
	/*
	 * We also override some S3FileSystem functionality to inject additional headers,
	 * so use PgLakeS3FileSystem.
	 */
	PgLakeS3FileSystem s3fs;

	RegionAwareS3FileSystem(BufferManager &bufferManager) : s3fs(bufferManager) {
	}

	/* Custom functions */
	vector<OpenFileInfo> ListWithRegion(const string &urlPattern, const string &region,
										bool isGlob, FileOpener *opener);
	string GetBucketUrl(const string &url, optional_ptr<FileOpener> opener);
	string GetCachedRegion(const string &bucketUrl,
						   optional_ptr<FileOpener> opener);
	void PutCachedRegion(const string &bucketUrl, const string &regionName,
						 optional_ptr<FileOpener> opener);
	void ClearCachedRegion(const string &bucketUrl,
						   optional_ptr<FileOpener> opener);
	string GetBucketRegionFromS3(const string &url, optional_ptr<FileOpener> opener);
	string GetBucketRegion(const string &url, optional_ptr<FileOpener> opener);
	vector<OpenFileInfo> List(const string &globPattern, bool isGlob, FileOpener *opener);

	/*
	 * Resolve the region for url (cache or S3 headers), then call s3Operation
	 * with the region-adjusted URL. If s3Operation throws a 400/301 that
	 * looks like a region mismatch, look up the actual region and retry
	 * once. Used by FileExists / DirectoryExists / RemoveFile so they
	 * self-heal on cross-region buckets the same way OpenFile / Glob do.
	 *
	 * s3Operation receives the URL with ?s3_region=... appended (or the
	 * original URL if no cached/discovered region is available).
	 */
	void WithResolvedRegion(const string &url, optional_ptr<FileOpener> opener,
							std::function<void(const string &)> s3Operation);

	/*
	 * Download performs similar logic to GetRequest, except writing the output
	 * to a destination file rather than an in-memory buffer.
	 */
	int64_t Download(ClientContext &context, FileHandle &inputHandle, FileHandle &outputHandle) {
		return s3fs.Download(context, inputHandle, outputHandle);
	}

	/* Custom overrides */
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags openFlags,
	                                        optional_ptr<FileOpener> opener = nullptr) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

    /* pass through to wrapped handle */
	void Read(FileHandle &handle, void *buffer, int64_t byteCount, idx_t location) override {
		s3fs.Read(handle, buffer, byteCount, location);
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t byteCount) override {
		return s3fs.Read(handle, buffer, byteCount);
	}

	void Write(FileHandle &handle, void *buffer, int64_t byteCount, idx_t location) override {
		s3fs.Write(handle, buffer, byteCount, location);
    }

	int64_t Write(FileHandle &handle, void *buffer, int64_t byteCount) override {
		return s3fs.HTTPFileSystem::Write(handle, buffer, byteCount);
    }

	bool CanHandleFile(const string &fpath) override {
		return s3fs.CanHandleFile(fpath);
	}

	void FileSync(FileHandle &handle) override {
		s3fs.FileSync(handle);
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		/*
		 * HTTPFileSystem::FileExists calls OpenFile + catches all exceptions.
		 * If we delegated to it on a cross-region bucket, the 400/301 would
		 * be swallowed inside s3fs and WithResolvedRegion would never see
		 * the failure to retry. Call our own region-aware OpenFile instead
		 * so the retry fires, and map not-found back to false here.
		 */
		try
		{
			auto handle = OpenFile(filename, FileFlags::FILE_FLAGS_READ, opener);
			auto &sfh = handle->Cast<HTTPFileHandle>();
			return sfh.length != 0;
		}
		catch (...)
		{
			return false;
		}
	}

	int64_t GetFileSize(FileHandle &handle) override {
		return s3fs.GetFileSize(handle);
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		return s3fs.GetLastModifiedTime(handle);
	}

	void Seek(FileHandle &handle, idx_t location) override {
		s3fs.Seek(handle, location);
	}

	idx_t SeekPosition(FileHandle &handle) override {
		return s3fs.SeekPosition(handle);
	}

	bool CanSeek() override {
		return s3fs.CanSeek();
	}

	bool OnDiskFile(FileHandle &handle) override {
		return s3fs.OnDiskFile(handle);
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		bool result = false;
		WithResolvedRegion(directory, opener,
			[&](const string &regionUrl) { result = s3fs.DirectoryExists(regionUrl, opener); });
		return result;
	}

	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		WithResolvedRegion(filename, opener,
			[&](const string &regionUrl) { s3fs.RemoveFile(regionUrl, opener); });
	}

	bool IsPipe(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		return s3fs.IsPipe(filename, opener);
	}

	string GetName() const override {
		return "RegionAwareS3FileSystem";
	}
};

/*
 * BucketRegion is used to cache bucket -> region mappings.
 */
class BucketRegion : public ObjectCacheEntry {
public:
	string bucketUrl;
	string regionName;

	/* required ObjectCacheEntry functions */
	static string ObjectType() {
		return "pg_lake_bucket_region";
	}

	string GetObjectType() override {
		return ObjectType();
	}
};


} // namespace duckdb
