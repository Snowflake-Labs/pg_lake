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
#include "duckdb/main/http/http_util.hpp"

#include "http/httpfs.hpp"
#include "http/http_request_session.hpp"
#include "pg_lake/fs/httpfs_extended.hpp"


namespace duckdb {

/*
 * GetHTTPError describes a failed request.
 *
 * When a response callback throws, HTTPUtil::RunRequestWithRetry replaces the
 * response with a synthetic one whose status is INVALID (0) and keeps the
 * original message in request_error. Reporting that status instead would turn
 * an HTTP 401 or 403 into "HTTP 0 Internal Server Error".
 */
HTTPException
PgLakeHTTPFileSystem::GetHTTPError(FileHandle &handle, const HTTPResponse &response, RequestType request_type,
																	 const string &url)
{
	if (response.status == HTTPStatusCode::INVALID && response.HasRequestError()) {
		return HTTPException(response, "%s", response.GetRequestError());
	}

	return HTTPFileSystem::GetHTTPError(handle, response, request_type, url);
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

	long unsigned int total_request_bytes = 0;

	auto captured = hfh.request_session->Capture();
	auto session_request = captured.snapshot->CreateRequest(std::move(headerMap));

	GetRequestInfo get_request(
								 inputUrl, session_request.headers, *session_request.params,
								 [&](const HTTPResponse &response) {
									 if (static_cast<int>(response.status) >= 400) {
										 throw GetHTTPError(inputHandle, response, RequestType::GET_REQUEST, inputUrl);
									 }

									 /*
										* The client streams the body as it arrives and retries a
										* transfer that broke off, which calls us again for the new
										* response. Start the output over so the retry does not
										* append to what the broken attempt wrote.
										*/
									 if (total_request_bytes > 0) {
										 outputHandle.Truncate(0);
										 outputHandle.Seek(0);
										 total_request_bytes = 0;
									 }
									 return true;
								 },
								 [&](const_data_ptr_t data, idx_t data_length) {
									 total_request_bytes += data_length;
									 if (context.IsInterrupted())
										 throw InterruptException();

									 outputHandle.Write((void *) data, data_length);
									 return true;
								 });

	auto response = hfh.request_session->Request(get_request);

	if (response->HasRequestError())
		throw IOException("HTTP download of \"%s\" could not be completed: %s",
							inputUrl, response->GetRequestError());

	/*
	 * The byte count is verified by FileUtils::CopyFile, which does it for every
	 * transfer rather than only the ones that come through here. Note that a
	 * request reporting success does not mean the whole body arrived: see the
	 * comment there.
	 */
	return total_request_bytes;
}

} // namespace duckdb
