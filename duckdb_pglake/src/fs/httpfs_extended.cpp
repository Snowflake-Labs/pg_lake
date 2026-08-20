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

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httplib.hpp"

#include "httpfs.hpp"
#include "pg_lake/fs/httpfs_extended.hpp"


namespace duckdb {

/*
 * initialize_http_headers is a private function copied ad verbatim from httpfs.cp
 */
static unique_ptr<duckdb_httplib_openssl::Headers>
initialize_http_headers(HTTPHeaders &header_map)
{
	auto headers = make_uniq<duckdb_httplib_openssl::Headers>();
	for (auto &entry : header_map) {
		headers->insert(entry);
	}
	return headers;
}

/*
 * Download performs similar logic to GetRequest, except writing the output
 * to a destination file rather than an in-memory buffer.
 *
 * Compared to regular Read, this helps us avoid range requests, which not
 * all servers support and introduces unnecessary buffering, and it also lets
 * us cancel quickly (every 4KB).
 */
int64_t
PgLakeHTTPFileSystem::Download(ClientContext &context, FileHandle &inputHandle, string inputUrl, HTTPHeaders headerMap, FileHandle &outputHandle)
{
    auto &hfh = inputHandle.Cast<HTTPFileHandle>();
	auto &http_util = hfh.http_params.http_util;

	D_ASSERT(hfh.cached_file_handle);

    long unsigned int total_request_bytes = 0;

	auto http_client = hfh.GetClient();
	GetRequestInfo get_request(
							   inputUrl, headerMap, hfh.http_params,
							   [&](const HTTPResponse &response) {
								   if (static_cast<int>(response.status) >= 400) {
									                   string error =
														   "HTTP GET error on '" + inputUrl + "' (HTTP " + to_string(static_cast<int>(response.status)) + ")";
													   throw HTTPException(error);
								   }
								   return true;
							   },
							   [&](const_data_ptr_t data, idx_t data_length) {
								   total_request_bytes += data_length;
								   if (context.interrupted)
									   throw InterruptException();

								   outputHandle.Write((void *) data, data_length);
								   return true;
							   });

	auto response = http_util.Request(get_request, http_client);

	hfh.StoreClient(std::move(http_client));

	/*
	 * The byte count is verified by FileUtils::CopyFile, which does it for every
	 * transfer rather than only the ones that come through here. Note that a
	 * request reporting success does not mean the whole body arrived: see the
	 * comment there.
	 */
	return total_request_bytes;
}

} // namespace duckdb
