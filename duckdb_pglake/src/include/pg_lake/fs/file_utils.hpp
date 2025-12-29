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
#include "s3fs.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

/*
 * FileUtils has a set of utility functions for working with files.
 */
class FileUtils {
public:

	static string ExtractDirName(const string &path);
	static string ExtractFileName(const string &path);
	static bool EnsureLocalDirectoryExists(ClientContext &context, string dir_path);
	static int64_t CopyFile(ClientContext &context, string &source_path, string &destination_path);
	static string GetMD5Base64(const char *buffer, idx_t bufferLength);

};

} // namespace duckdb
