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

#include "http/httpfs.hpp"
#include "s3/s3fs.hpp"

namespace duckdb {

extern const string MANAGED_STORAGE_BUCKET_SETTING;
extern const string MANAGED_STORAGE_KEY_ID_SETTING;


/*
 * PgLakeIsValidS3Region returns whether region is a syntactically valid AWS
 * region name: a non-empty token of lowercase letters, digits, and single
 * hyphens (e.g. "us-east-1").
 *
 * A region is interpolated into an S3 endpoint host as
 * "s3.<region>.amazonaws.com", so it has to be a single host label. Check it
 * before caching a region or formatting one into a URL.
 */
bool PgLakeIsValidS3Region(const string &region);


/*
 * PgLakeS3AuthConfigCallback adjusts the S3 configuration httpfs reads for a
 * path. It sets the KMS key for writes to the managed storage bucket and
 * validates the region. Registered as S3AuthResolver::config_callback.
 */
void PgLakeS3AuthConfigCallback(optional_ptr<FileOpener> opener, const string &path,
																S3AuthConfig &config);


/*
 * PgLakeS3FileSystem extends S3FileSystem with the operations pg_lake needs
 * that httpfs does not offer.
 */
class PgLakeS3FileSystem : public S3FileSystem {
public:
	PgLakeS3FileSystem(BufferManager &bufferManager) : S3FileSystem(bufferManager) {
	}

	/* Custom functions */
	int64_t Download(ClientContext &context, FileHandle &inputHandle, FileHandle &outputHandle);
	vector<OpenFileInfo> List(const string &glob_pattern, bool is_glob, FileOpener *opener);

	/* Overrides that evict the HTTP metadata cache */
	void RemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;
	void RemoveFiles(const vector<string> &paths, optional_ptr<FileOpener> opener = nullptr) override;

	/* Overrides that are not in S3FileSystem */
	bool TryRemoveFile(const string &filename, optional_ptr<FileOpener> opener = nullptr) override;

	string GetName() const override {
		return "PgLakeS3FileSystem";
	}
};

} // namespace duckdb
