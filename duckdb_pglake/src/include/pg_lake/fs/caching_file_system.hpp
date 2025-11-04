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

#include "pg_lake/fs/file_cache_manager.hpp"

namespace duckdb {


/*
 * CachingFSFileHandle is the FileHandle returned by CachingFileSystem::OpenFile.
 *
 * It wraps around a remote file handle or a LocalFileSystem handle.
 */
class CachingFSFileHandle : public FileHandle {
public:
	/* Wrapped S3 or local file handle */
	unique_ptr<FileHandle> wrappedHandle;

	/*
	 * When a file is written, we fork the byte stream to
	 * also create the cache file locally. We use "cacheOnWrite"
	 * to refer to this local file.
	 */
	unique_ptr<FileHandle> cacheOnWriteHandle;
	string cacheOnWritePath;
	unique_lock<mutex> cacheOnWriteFileLock;
	int64_t cacheOnWriteWrittenBytes = 0;

	/* client context to which this file handle belongs */
	optional_ptr<ClientContext> context;

	explicit CachingFSFileHandle(FileSystem &fs,
							   string path,
							   FileOpenFlags flags,
							   optional_ptr<ClientContext> context_p,
							   unique_ptr<FileHandle> wrapped_handle_p,
							   unique_ptr<FileHandle> local_cache_handle_on_write_p,
							   string local_cache_handle_path,
							   unique_lock<mutex> localCacheFileLockP) : FileHandle(fs, path, flags)
	{
		wrappedHandle = std::move(wrapped_handle_p);
		cacheOnWriteHandle = std::move(local_cache_handle_on_write_p);
		cacheOnWritePath = local_cache_handle_path;
		context = context_p;
		cacheOnWriteFileLock = std::move(localCacheFileLockP);
	}

	~CachingFSFileHandle()
	{
		/*
		 * We allocate a lock per cacheOnWriteHandle file in an unordered map
		 * and the lock's ownership is moved to CachingFSFileHandle().
		 *
		 * When the relevant file handle is destroyed here, the lock is automatically
		 * released. And here we also remove the cache file activity from the
		 * unordered map.
		 */
		if (cacheOnWriteHandle != nullptr)
		{
			shared_ptr<FileCacheManager> cacheManager = FileCacheManager::Get(*context);
			cacheManager->RemoveCacheFileActivityFromMapIfNeeded(cacheOnWritePath);
		}
	}

	void Close()
	{
		wrappedHandle->Close();

		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) *this;

		/*
		 * CSV and JSON files are finalized with a Close() call. When cacheOnWrite
		 * file is a non-Parquet file (e.g., CSV or JSON), we need to rename it to
		 * the final name at Close().
		 *
		 * You can read WriteCSVFinalize() for the details.
		*/
		if (cacheOnWriteHandle != nullptr)
		{
			cacheOnWriteHandle->Close();

			LocalFileSystem localfs;
			localfs.MoveFile(pg_lakeHandle.cacheOnWritePath + ".pgl-stage", pg_lakeHandle.cacheOnWritePath);

			cacheOnWriteHandle = nullptr;
		}
	 }
};

/*
 * CachingFileSystem is a wrapper around S3FileSystem that replaces
 * the original to be able to intercept and modify certain calls. In
 * particular we use OpenFile to implement caching.
 */
class CachingFileSystem : public FileSystem {
public:
	/* remote file system */
    unique_ptr<FileSystem> remoteFs;

	/* local file system (mainly for convenience) */
    LocalFileSystem localfs;

	CachingFileSystem(unique_ptr<FileSystem> remoteFsParam)
	{
		remoteFs = std::move(remoteFsParam);
	}

	/* Custom functions */
	bool ShouldCacheOnWrite(CachingFSFileHandle &pg_lakeHandle, int64_t additionalByteCount);
	void CleanUpCacheOnWriteFile(CachingFSFileHandle &pg_lakeHandle);

	/* Custom overrides */
	duckdb::unique_ptr<FileHandle> OpenFile(const string &path, FileOpenFlags openFlags,
	                                        optional_ptr<FileOpener> opener = nullptr) override;
	vector<OpenFileInfo> Glob(const string &path, FileOpener *opener = nullptr) override;

	bool ListFiles(const string &directory, const std::function<void(const string &, bool)> &callback,
	               FileOpener *opener = nullptr) override;

	/* Overrides that are not in s3fs */
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

    /* pass through to wrapped handle */
	void Read(FileHandle &handle, void *buffer, int64_t byteCount, idx_t location) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		/* add an additional cancellation check */
		if (pg_lakeHandle.context->interrupted)
			throw InterruptException();

		wrappedHandle.file_system.Read(wrappedHandle, buffer, byteCount, location);
	}

	int64_t Read(FileHandle &handle, void *buffer, int64_t byteCount) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		/* add an additional cancellation check */
		if (pg_lakeHandle.context->interrupted)
			throw InterruptException();

		return wrappedHandle.file_system.Read(wrappedHandle, buffer, byteCount);
	}

	void Write(FileHandle &handle, void *buffer, int64_t byteCount, idx_t location) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		/* add an additional cancellation check */
		if (pg_lakeHandle.context->interrupted)
			throw InterruptException();

		wrappedHandle.file_system.Write(wrappedHandle, buffer, byteCount, location);
    }

	int64_t Write(FileHandle &handle, void *buffer, int64_t byteCount) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		/* add an additional cancellation check */
		if (pg_lakeHandle.context->interrupted)
			throw InterruptException();

		if (ShouldCacheOnWrite(pg_lakeHandle, byteCount))
		{
			pg_lakeHandle.cacheOnWriteHandle->Write(buffer, byteCount);
			pg_lakeHandle.cacheOnWriteWrittenBytes += byteCount;
		}
		else if (pg_lakeHandle.cacheOnWriteHandle != nullptr)
		{
			/*
			 * We are disabling (and removing) cache-on-write
			 * for this  file as the total bytes written is
			 * greater than pg_lake_cache_on_write_max_size.
			 */
			CleanUpCacheOnWriteFile(pg_lakeHandle);
		}

		return pg_lakeHandle.wrappedHandle->Write(buffer, byteCount);
    }

	bool CanHandleFile(const string &fpath) override {
		if (StringUtil::StartsWith(fpath, NO_CACHE_PREFIX))
			/* support nocachexxx:// by checking whether xxx:// is supported */
			return remoteFs->CanHandleFile(fpath.substr(NO_CACHE_PREFIX.length()));
		else
			return remoteFs->CanHandleFile(fpath);
	}

	void FileSync(FileHandle &handle) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		wrappedHandle.file_system.FileSync(wrappedHandle);

		/*
		 * Parquet files are finalized with a Sync() call. When cacheOnWrite
		 * file is a Parquet file, we need to rename it to the final name at
		 * Sync.
		 * You can read ParquetWriter::Finalize() for the details.
		*/
		if (pg_lakeHandle.cacheOnWriteHandle != nullptr)
		{
			/*
			 * This is the only place for Closing cacheOnWriteHandle
			 * for parquet files. We'll close it here and rename it
			 * to the final name.
			 */
			pg_lakeHandle.cacheOnWriteHandle->Close();

			localfs.MoveFile(pg_lakeHandle.cacheOnWritePath + ".pgl-stage", pg_lakeHandle.cacheOnWritePath);

			pg_lakeHandle.cacheOnWriteHandle = nullptr;
		}
	}

	bool FileExists(const string &filename, optional_ptr<FileOpener> opener = nullptr) override {
		if (StringUtil::StartsWith(filename, NO_CACHE_PREFIX))
			return remoteFs->FileExists(filename.substr(NO_CACHE_PREFIX.length()), opener);
		else
			return remoteFs->FileExists(filename, opener);
	}

	int64_t GetFileSize(FileHandle &handle) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		return wrappedHandle.file_system.GetFileSize(wrappedHandle);
	}

	timestamp_t GetLastModifiedTime(FileHandle &handle) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		return wrappedHandle.file_system.GetLastModifiedTime(wrappedHandle);
	}

	void Seek(FileHandle &handle, idx_t location) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		wrappedHandle.file_system.Seek(wrappedHandle, location);
	}

	idx_t SeekPosition(FileHandle &handle) override {
		CachingFSFileHandle &pg_lakeHandle = (CachingFSFileHandle &) handle;
		FileHandle &wrappedHandle = *pg_lakeHandle.wrappedHandle;

		return wrappedHandle.file_system.SeekPosition(wrappedHandle);
	}

	bool CanSeek() override {
		return remoteFs->CanSeek();
	}

	bool DirectoryExists(const string &directory, optional_ptr<FileOpener> opener = nullptr) override {
		return remoteFs->DirectoryExists(directory, opener);
	}

	void CreateDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		return remoteFs->CreateDirectory(directory, opener);
	}

	void RemoveDirectory(const string &directory, optional_ptr<FileOpener> opener) override {
		return remoteFs->RemoveDirectory(directory, opener);
	}

	/*
	 * We give the same answer that a remote file system would give, even the file
	 * is cached, because some DuckDB logic depends on this.
	 *
	 * We cannot pass the call through to S3FileSystem directly, since the handle might
	 * not be an S3FileHandle.
	 */
	bool OnDiskFile(FileHandle &handle) override {
		return false;
	}

	/* we do not adjust the name, because this file system is meant to replace the wrapped one */
	string GetName() const override {
		return remoteFs->GetName();
	}
};

} // namespace duckdb
