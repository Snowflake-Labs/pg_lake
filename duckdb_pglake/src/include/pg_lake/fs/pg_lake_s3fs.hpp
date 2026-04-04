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

#include <mutex>
#include <unordered_map>

#include "duckdb.hpp"
#include "duckdb/common/local_file_system.hpp"

#include "httpfs.hpp"
#include "s3fs.hpp"

namespace duckdb {

extern const string MANAGED_STORAGE_BUCKET_SETTING;
extern const string MANAGED_STORAGE_KEY_ID_SETTING;


/*
 * PgLakeS3FileSystem extends S3FileSystem to override certain functions
 * with the goal of injecting customer headers.
 */
class PgLakeS3FileSystem : public S3FileSystem {
public:
	PgLakeS3FileSystem(BufferManager &bufferManager) : S3FileSystem(bufferManager) {
	}

	/* Custom functions */
	void RemoveFileFromS3(string path, optional_ptr<FileOpener> opener);
	int64_t Download(ClientContext &context, FileHandle &inputHandle, FileHandle &outputHandle);
	vector<OpenFileInfo> List(const string &glob_pattern, bool is_glob, FileOpener *opener);

	/* Custom replacement for S3FileSystem functions */
	unique_ptr<HTTPResponse> PostRequest(HTTPInput &input, string s3_url, HTTPHeaders header_map,
	                                     string &buffer_out, char *buffer_in, idx_t buffer_in_len,
	                                     string http_params = "") override;
	unique_ptr<HTTPResponse> PutRequest(HTTPInput &input, string s3_url, HTTPHeaders header_map,
	                                    char *buffer_in, idx_t buffer_in_len,
	                                    string http_params = "") override;

	/* Overrides that are not in S3FileSystem */
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	string GetName() const override {
		return "PgLakeS3FileSystem";
	}

	void RegisterContext(const shared_ptr<HTTPInput> &input, optional_ptr<ClientContext> context);
	optional_ptr<ClientContext> LookupContext(HTTPInput *input);

protected:
	unique_ptr<HTTPFileHandle> CreateHandle(const OpenFileInfo &path, FileOpenFlags flags,
	                                        optional_ptr<FileOpener> opener) override;

private:
	mutable mutex context_mutex_;
	struct ContextEntry {
		weak_ptr<HTTPInput> input_ref;
		optional_ptr<ClientContext> context;
	};
	unordered_map<HTTPInput *, ContextEntry> context_map_;
};

class PgLakeS3FileHandle : public S3FileHandle {
public:
	optional_ptr<ClientContext> context;

	PgLakeS3FileHandle(FileSystem &fs,
						const OpenFileInfo &info_p,
						FileOpenFlags flags,
						optional_ptr<ClientContext> context_p,
						unique_ptr<HTTPParams> &http_params,
						const S3AuthParams &auth_params_p,
						const S3ConfigParams &config_params_p)
		: S3FileHandle(fs, info_p, flags, std::move(http_params), auth_params_p, config_params_p)
	{
		context = std::move(context_p);
	}
};

} // namespace duckdb
