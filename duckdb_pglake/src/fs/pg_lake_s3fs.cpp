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

#include <regex>

#include "duckdb.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/function/scalar/string_common.hpp"
#include "duckdb/main/http/http_util.hpp"

#include "http/httpfs.hpp"
#include "s3/s3fs.hpp"
#include "s3/s3_list.hpp"
#include "s3/s3_request.hpp"
#include "s3/s3_url.hpp"

#include "pg_lake/fs/pg_lake_s3fs.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"


namespace duckdb {

/*
 * Name of the setting that specifies the location of pg_lake managed storage bucket.
 */
const string MANAGED_STORAGE_BUCKET_SETTING = "pg_lake_managed_storage_bucket";

/*
 * Name of the setting that specifies the Amazon KMS key ID to use when writing to
 * the managed storage bucket.
 */
const string MANAGED_STORAGE_KEY_ID_SETTING = "pg_lake_managed_storage_key_id";


/*
 * PgLakeIsValidS3Region returns whether region is a syntactically valid AWS
 * region name: a non-empty run of lowercase letters and digits, separated by
 * single hyphens (e.g. "us-east-1", "eu-west-3", "us-gov-east-1").
 *
 * A region is interpolated into the S3 endpoint host as
 * "s3.<region>.amazonaws.com", so it has to be a single host label. Anything
 * carrying a delimiter such as '/', '.', ':' or '@' is rejected, as is a value
 * too long to be a region name.
 */
bool
PgLakeIsValidS3Region(const string &region)
{
	/* one or more alnum segments joined by single hyphens; no other chars */
	static const std::regex regionPattern("^[a-z0-9]+(-[a-z0-9]+)*$");

	/* keep it bounded; a real region is short, this only guards pathological input */
	if (region.empty() || region.size() > 64)
		return false;

	return std::regex_match(region, regionPattern);
}



/*
 * IsPgLakeManagedStoragePath returns whether the given URL is in the
 * managed storage bucket.
 */
static bool
IsPgLakeManagedStoragePath(optional_ptr<ClientContext> context, const string &path)
{
	if (context == nullptr)
		return false;

	Value setting;

	if (!context->TryGetCurrentSetting(Identifier(MANAGED_STORAGE_BUCKET_SETTING), setting))
		return false;

	string managedStorageBucket = setting.ToString();

	/* we ignore empty string and "NULL", the latter is used in case of reset */
	if (managedStorageBucket.empty() || managedStorageBucket == "NULL")
		return false;

	/* remove trailing slash */
		if (managedStorageBucket.back() == '/')
				managedStorageBucket.pop_back();

	/* the path is in the bucket if the bucket URL is followed by a / or nothing */
	return StringUtil::StartsWith(path, managedStorageBucket) &&
			 (path.size() == managedStorageBucket.size() ||
				path[managedStorageBucket.size()] == '/' ||
				path[managedStorageBucket.size()] == '?');
}



/*
 * GetManagedStorageKeyId returns the KMS key ID configured for the managed
 * storage bucket, or an empty string if there is none.
 */
static string
GetManagedStorageKeyId(ClientContext &context)
{
	Value setting;

	if (!context.TryGetCurrentSetting(Identifier(MANAGED_STORAGE_KEY_ID_SETTING), setting) ||
		setting.IsNull())
		return string();

	string keyId = setting.ToString();

	/*
	 * If the setting has been disabled via RESET, the value becomes "NULL" (?),
	 * we then treat it as an empty string (ignored).
	 */
	if (keyId == "NULL")
		return string();

	return keyId;
}


/*
 * IsAmazonS3Endpoint returns whether an endpoint is a regional AWS S3
 * endpoint, that is "s3.<region>.amazonaws.com".
 */
static bool
IsAmazonS3Endpoint(const string &endpoint)
{
	return StringUtil::StartsWith(endpoint, "s3.") &&
			 StringUtil::EndsWith(endpoint, ".amazonaws.com");
}


/*
 * PgLakeS3AuthConfigCallback adjusts the S3 configuration httpfs reads for a
 * path before it is resolved. httpfs calls it on every resolution with a
 * FileOpener, including the one a credential refresh does, so what it sets
 * also holds for requests sent after a refresh.
 *
 * It does three things:
 *
 * - Writes to the managed storage bucket use the customer managed key, if one
 *   is configured. httpfs only sends the key on PutObject and
 *   CreateMultipartUpload, as we did before.
 *
 * - An explicit AWS endpoint follows the region, the way the automatic one
 *   does. RegionAwareS3FileSystem injects the region of the bucket as
 *   ?s3_region, and the endpoint from the secret would otherwise still point
 *   at whatever region the secret named.
 *
 * - The region is validated before httpfs formats it into the endpoint host
 *   as "s3.<region>.amazonaws.com".
 */
void
PgLakeS3AuthConfigCallback(optional_ptr<FileOpener> opener, const string &path,
							 S3AuthConfig &config)
{
	if (config.route.type != S3ProviderType::S3)
		return;

	/* ?s3_region and ?s3_endpoint are only applied after this callback */
	S3Url::ApplyAuthQueryParameters(path, config);

	bool regionInHost = config.endpoint_mode == S3EndpointMode::AUTOMATIC ||
						IsAmazonS3Endpoint(config.endpoint);

	if (regionInHost && !config.credentials.region.empty())
	{
		if (!PgLakeIsValidS3Region(config.credentials.region))
			throw InvalidInputException(
				"invalid S3 region \"%s\": a region name may only contain "
				"lowercase letters, digits, and hyphens",
				config.credentials.region);

		/* let httpfs derive the host from the region */
		config.endpoint_mode = S3EndpointMode::AUTOMATIC;
	}

	optional_ptr<ClientContext> context = FileOpener::TryGetClientContext(opener);

	if (context == nullptr)
		return;

	string keyId = GetManagedStorageKeyId(*context);

	if (!keyId.empty() && IsPgLakeManagedStoragePath(context, path))
		config.request_options.kms_key_id = keyId;
}


/*
 * IsNotFoundError determines whether an HTTP error reports a file that is not
 * there. It reads the status code rather than the message, whose wording
 * changes between DuckDB releases.
 */
static bool
IsNotFoundError(const ErrorData &error)
{
	auto statusCode = error.ExtraInfo().find("status_code");

	return statusCode != error.ExtraInfo().end() && statusCode->second == "404";
}



/*
 * EvictFromMetadataCache drops the HTTP metadata cache entries for a removed
 * file, so a stale entry cannot make it look readable.
 *
 * Reads inject ?s3_region from cache, so the entry can be under the URL with
 * or without its query arguments. Evict both.
 *
 * Even if HTTP metadata cache is disabled, GetGlobalCache returns a value
 * and we still remove because it might be re-enabled later.
 */
static void
EvictFromMetadataCache(HTTPFileSystem &fs, const string &path)
{
	optional_ptr<HTTPMetadataCache> metadataCache = fs.GetGlobalCache();

	if (metadataCache == nullptr)
		return;

	metadataCache->Erase(path);

	auto queryPos = path.find('?');
	if (queryPos != string::npos)
		metadataCache->Erase(path.substr(0, queryPos));
}


/*
 * RemoveFile removes a file from S3 and evicts it from the HTTP metadata cache.
 */
void
PgLakeS3FileSystem::RemoveFile(const string &filename,
								optional_ptr<FileOpener> opener)
{
	EvictFromMetadataCache(*this, filename);

	S3FileSystem::RemoveFile(filename, opener);

	/* a concurrent reader can cache the entry again before the delete */
	EvictFromMetadataCache(*this, filename);
}


/*
 * TryRemoveFile removes a file from S3 and reports whether it was there to
 * begin with, so that a caller deleting a file that is already gone does not
 * have to treat that as a failure. Every other error, a permissions error in
 * particular, is still raised.
 *
 * S3 reports success on deleting a key that does not exist, so we find out
 * whether it is there with a HEAD request first.
 */
bool
PgLakeS3FileSystem::TryRemoveFile(const string &filename,
									optional_ptr<FileOpener> opener)
{
	try
	{
		unique_ptr<FileHandle> handle =
			OpenFile(filename, FileFlags::FILE_FLAGS_READ | FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS, opener);

		if (handle == nullptr)
		{
			EvictFromMetadataCache(*this, filename);
			return false;
		}
	}
	catch (HTTPException &ex)
	{
		ErrorData error(ex);

		if (!IsNotFoundError(error))
			throw;

		PGDUCK_SERVER_DEBUG("Remove failed: %s", error.Message().c_str());
		EvictFromMetadataCache(*this, filename);
		return false;
	}

	RemoveFile(filename, opener);
	return true;
}


/*
 * RemoveFiles deletes many files with batched DeleteObjects requests, and
 * evicts them from the HTTP metadata cache.
 *
 * One request carries one signature, so S3FileSystem::RemoveFiles sends a
 * request per set of credentials the paths resolve to, but in no particular
 * order. We hand it one such set at a time instead, in the order of the first
 * path of each, so that when one set is refused the ones before it were
 * deleted, and which ones those are does not depend on hashing.
 *
 * We evict both before and after each request, for the same reasons the file
 * cache does it in PGLakeCachingFileSystem::RemoveFiles: the request throws
 * when S3 reports a per-key error, and the keys it did delete are gone whether
 * or not we get to the second eviction; a concurrent reader can also cache an
 * entry again in the window between the eviction and the delete. Evicting an
 * entry for a file that is still there only costs a HEAD.
 */
void
PgLakeS3FileSystem::RemoveFiles(const vector<string> &paths,
								optional_ptr<FileOpener> opener)
{
	/*
	 * A linear scan: a set of paths to delete is one table's files or one
	 * batch off the deletion queue, so the number of distinct credentials in
	 * it is a handful at worst.
	 */
	vector<pair<S3AuthParams, vector<string>>> groups;

	for (const string &path : paths)
	{
		FileOpenerInfo info = {path};
		S3AuthParams authParams = S3AuthResolver::Resolve(opener, info);
		vector<string> *groupPaths = nullptr;

		for (auto &group : groups)
		{
			if (group.first == authParams)
			{
				groupPaths = &group.second;
				break;
			}
		}

		if (groupPaths == nullptr)
		{
			groups.emplace_back(std::move(authParams), vector<string>());
			groupPaths = &groups.back().second;
		}

		groupPaths->push_back(path);
	}

	for (auto &group : groups)
	{
		const vector<string> &groupPaths = group.second;

		for (const string &path : groupPaths)
			EvictFromMetadataCache(*this, path);

		S3FileSystem::RemoveFiles(groupPaths, opener);

		for (const string &path : groupPaths)
			EvictFromMetadataCache(*this, path);
	}
}


/*
 * Download performs similar logic to GetRequest, except writing the output
 * to a destination file rather than an in-memory buffer.
 *
 * The request goes through S3RequestExecutor, which signs it and handles
 * credential refresh and region redirects.
 */
int64_t
PgLakeS3FileSystem::Download(ClientContext &context, FileHandle &inputHandle, FileHandle &outputHandle)
{
	auto &s3Handle = inputHandle.Cast<S3FileHandle>();
	idx_t totalBytes = 0;

	S3RequestSpec spec {s3Handle.path, S3RequestOperation::GET_OBJECT, {}, "", "", ""};

	S3RequestResult result = S3RequestExecutor::RunHandle(
		GetEncryptionUtil(), s3Handle, spec,
		[&](S3RequestData &requestData) {
			/*
			 * The executor retries failed requests. A retry that starts after we
			 * wrote part of the body would append the object a second time.
			 */
			if (totalBytes > 0)
				throw IOException("download of %s failed after %llu bytes",
									requestData.display_url, (uint64_t) totalBytes);

			auto &params = requestData.http_params->Cast<HTTPFSParams>();

			GetRequestInfo getRequest(
				requestData.http_url, requestData.headers, params,
				[&](const HTTPResponse &response) {
					if (static_cast<int>(response.status) >= 400)
						throw S3RequestUtil::GetRequestError(requestData, response);
					return true;
				},
				[&](const_data_ptr_t data, idx_t dataLength) {
					totalBytes += dataLength;
					if (context.IsInterrupted())
						throw InterruptException();

					outputHandle.Write((void *) data, dataLength);
					return true;
				});

			return S3RequestExecutor::SendHandleRequest(s3Handle, requestData, getRequest);
		});

	if (result.response->HasRequestError())
		throw IOException("S3 download of \"%s\" could not be completed: %s",
							result.context.display_url, result.response->GetRequestError());

	/*
	 * The byte count is verified by FileUtils::CopyFile, which does it for every
	 * transfer rather than only the ones that come through here.
	 */
	return totalBytes;
}


/*
 * Match is copied ad verbatim from from s3fs.cpp in DuckDB to apply glob filtering to
 * S3 list output.
 */
static bool
Match(vector<string>::const_iterator key, vector<string>::const_iterator key_end,
			vector<string>::const_iterator pattern, vector<string>::const_iterator pattern_end)
{
	while (key != key_end && pattern != pattern_end) {
		if (*pattern == "**") {
			if (std::next(pattern) == pattern_end) {
				return true;
			}
			while (key != key_end) {
				if (Match(key, key_end, std::next(pattern), pattern_end)) {
					return true;
				}
				key++;
			}
			return false;
		}
		if (!Glob(key->data(), key->length(), pattern->data(), pattern->length())) {
			return false;
		}
		key++;
		pattern++;
	}
	return key == key_end && pattern == pattern_end;
}



/*
 * List returns a list of file descriptions that match the given glob
 * pattern, including size, last modified time and, outside globs, etag.
 *
 * Based on Glob in s3fs.cpp, modified to return a vector of OpenFileInfo
 * instead of keys only, and to list without a delimiter so that a single
 * request set returns every key under the prefix.
 */
vector<OpenFileInfo>
PgLakeS3FileSystem::List(const string &glob_pattern, bool is_glob, FileOpener *opener)
{
	if (opener == nullptr) {
		throw InternalException("Cannot S3 Glob without FileOpener");
	}

	optional_ptr<ClientContext> context = opener->TryGetClientContext();

	FileOpenerInfo info = {glob_pattern};
	S3AuthParams s3_auth_params = S3AuthResolver::Resolve(opener, info);

	// In url compatibility mode, we ignore globs allowing users to query files with the glob chars
	if (s3_auth_params.GetURLParams().compatibility_mode && is_glob) {
		return {OpenFileInfo(glob_pattern)};
	}

	ParsedS3Url parsed_s3_url = S3Url::Parse(glob_pattern, s3_auth_params);
	string bucket_url = parsed_s3_url.GetPrefix() + parsed_s3_url.GetBucket();
	const string &glob_key = parsed_s3_url.GetKey();

	// AWS matches on prefix, not glob pattern, so we take a substring until the first wildcard char for the aws calls
	auto first_wildcard_pos = glob_key.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos && is_glob) {
		return {OpenFileInfo(glob_pattern)};
	}

	string shared_path = bucket_url + "/" + glob_key.substr(0, first_wildcard_pos);

	// The session refreshes expired credentials and follows region redirects
	auto session = S3RequestExecutor::CreateSession(opener, glob_pattern, s3_auth_params);

	vector<OpenFileInfo> s3_file_descs;
	string continuation_token;

	// Paging loop
	do {
		if (context && context->IsInterrupted())
			throw InterruptException();

		S3ListObjectsV2Result response = AWSListObjectV2::Request(GetEncryptionUtil(), *session, shared_path,
																														 continuation_token, S3ListMode::FLAT);
		AWSListObjectV2::AppendFileList(response, s3_file_descs);
		continuation_token = response.continuation_token;
	} while (!continuation_token.empty());

	vector<string> pattern_splits = StringUtil::Split(glob_key, "/");
	vector<OpenFileInfo> result;

	for (OpenFileInfo &s3_file_desc : s3_file_descs) {
		vector<string> key_splits = StringUtil::Split(s3_file_desc.path, "/");
		bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end());

		if (!is_match)
			continue;

		string result_full_url = bucket_url + "/" + s3_file_desc.path;

		// if a ? char was present, we re-add it here as the url parsing will have trimmed it.
		if (is_glob && !parsed_s3_url.GetQueryString().empty()) {
			result_full_url += '?' + parsed_s3_url.GetQueryString();
		}

		s3_file_desc.path = result_full_url;

		auto &options = s3_file_desc.extended_info->options;

		/* pg_lake_list_files returns the size as a bigint */
		auto file_size = options.find("file_size");
		if (file_size != options.end())
			file_size->second = file_size->second.DefaultCastAs(LogicalType::BIGINT);

		/* only pg_lake list wants the etag */
		if (is_glob)
			options.erase("etag");

		result.push_back(std::move(s3_file_desc));
	}

	return result;
}

} // namespace duckdb
