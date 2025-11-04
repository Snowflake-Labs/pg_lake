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

#include "crypto.hpp"
#include "duckdb.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/function/scalar/string_common.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httpfs.hpp"
#include "s3fs.hpp"
#include "httplib.hpp"

#include "pg_lake/fs/pg_lake_s3fs.hpp"
#include "pg_lake/fs/httpfs_extended.hpp"
#include "pg_lake/fs/file_cache_manager.hpp"
#include "pg_lake/fs/file_utils.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"


namespace duckdb {

static constexpr idx_t MD5_HASH_LENGTH_BASE64 = 24;

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
 * CreateHandle is copy-pasted from s3fs.cpp, but using PgLakeS3FileHandle which includes
 * a pointer to the ClientContext.
 */
unique_ptr<HTTPFileHandle> PgLakeS3FileSystem::CreateHandle(const OpenFileInfo &fileInfo,
															 FileOpenFlags flags,
															 optional_ptr<FileOpener> opener)
{
	optional_ptr<ClientContext> context = opener->TryGetClientContext();

	FileOpenerInfo info = {fileInfo.path};
	S3AuthParams auth_params = S3AuthParams::ReadFrom(opener, info);

	// Scan the query string for any s3 authentication parameters
	auto parsed_s3_url = S3UrlParse(fileInfo.path, auth_params);
	ReadQueryParams(parsed_s3_url.query_param, auth_params);

	// Work around incomplete change made in https://github.com/duckdb/duckdb-httpfs/pull/83/files
	// The endpoint is not adapted to the s3_region query parameter, which we rely on for
	// region injection.
	if (StringUtil::EndsWith(auth_params.endpoint, ".amazonaws.com"))
		auth_params.endpoint = StringUtil::Format("s3.%s.amazonaws.com", auth_params.region);

	auto http_util = HTTPFSUtil::GetHTTPUtil(opener);
	auto params = http_util->InitializeParameters(opener, info);

	return duckdb::make_uniq<PgLakeS3FileHandle>(*this, fileInfo.path, flags, context,
												  params,
												  auth_params,
			                                      S3ConfigParams::ReadFrom(opener));
}


/*
 * RemoveFile removes a file from S3 via the batch delete API, mainly because
 * there is no implementation of regular DELETE requests in s3fs.cpp
 */
void
PgLakeS3FileSystem::RemoveFile(const string &filename,
								optional_ptr<FileOpener> opener)
{
	try
	{
		RemoveFileFromS3(filename, opener);
	}
	catch (HTTPException &ex)
	{
		ErrorData error(ex);

		PGDUCK_SERVER_DEBUG("Remove failed: %s", error.Message().c_str());

		/*
		 * If the file is not found, we can consider it removed, but still
		 * clear the cache below.
		 *
		 * The reason is that the last invocation may have failed before
		 * removing from cache, so if we return here then the file would
		 * remain readable no matter how many times we try to remove it.
		 *
		 * Checking for 404 error is cheaper and more reliable than FileExists,
		 * which opens the file and returns false in case of any exception,
		 * but we do want to surface permissions errors.
		 *
		 * Note: The capitalized form comes from moto.
		 */
		if (error.Message().find("404 (Not Found)") == std::string::npos &&
			error.Message().find("404 (NOT FOUND)") == std::string::npos)
			throw;
	}
}


/*
 * RemoveFileFromS3 deletes set of keys from a bucket using the batch
 * deletion API and returns the deleted path.
 */
void
PgLakeS3FileSystem::RemoveFileFromS3(string path, optional_ptr<FileOpener> opener)
{
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	FileSystem &fileSystem = FileSystem::GetFileSystem(*context);

	/* parse the S3 URL */
	FileOpenerInfo s3UrlInfo = {path};
	S3AuthParams authParams = S3AuthParams::ReadFrom(opener, s3UrlInfo);
	ParsedS3Url parsedUrl = S3UrlParse(path, authParams);

	/* get the s3://<bucket name> */
	string bucketUrl = parsedUrl.prefix + parsedUrl.bucket;
	string key = parsedUrl.key;

	/*
	 * Following S3FileSystem::FinalizeMultipartUpload
	 */
	std::stringstream ss;
	ss << "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
	ss << "<Object><Key>" << key << "</Key></Object>";
	ss << "</Delete>";
	string body = ss.str();

	/* Initialize buffer at 1000 characters (will get resized if needed) */
	string responseBuffer(1000, '\0');

	/*
	 * Open the file via the regular (region-aware) file system.
	 *
	 * This tells us the region-resolved path.
	 */
	unique_ptr<FileHandle> regionAwareFileHandle =
		fileSystem.OpenFile(path, FileFlags::FILE_FLAGS_READ);

	/* Store the region-resolved path with the auto-detected ?s3_region (if applicable) */
	string regionResolvedPath = regionAwareFileHandle->path;

	/*
	 * Open the file via (this) PgLakeS3FileSystem.
	 *
	 * This gives us a file handle that we can adjust to POST to /, since
	 * we cannot construct such a file handle directly.
	 */
	unique_ptr<FileHandle> fileHandle =
		OpenFile(path, FileFlags::FILE_FLAGS_READ, opener);

	S3FileHandle *s3Handle = (S3FileHandle *) fileHandle.get();

	/* Change the file handle to / to POST to /?delete */
	s3Handle->path = bucketUrl + "/";

	/* Perform the "batch" deletion */
	unique_ptr<HTTPResponse> postResponse =
		PostRequest(*s3Handle, s3Handle->path, {}, responseBuffer,
		            (char *) body.c_str(), body.length(), "delete=");

	/* Construct body of the POST response */
	string result(responseBuffer);

	if (result.find("<DeleteResult", 0) == string::npos)
		throw HTTPException(*postResponse,
							"Unexpected response during S3 DeleteObjects: %d\n\n%s",
							postResponse->status,
		                    result);

	/*
	 * Remove the file from HTTP metadata cache now that it has been deleted.
	 *
	 * Even if HTTP metadata cache is disabled, GetGlobalCache returns a value
	 * and we still remove because it might be re-enabled later.
	 *
	 * TODO: We should consider a more general cleanup approach, since most
	 * files that are read are never removed.
	 */
	optional_ptr<HTTPMetadataCache> metadataCache = GetGlobalCache();
	metadataCache->Erase(regionResolvedPath);
}

/*
 * create_s3_header is mostly copy-pasted from s3fs.cpp with some custom
 * additions for Content-MD5.
 *
 * We need it for our custom PostRequest implementation.
 */
static HTTPHeaders create_s3_header(string url, string query, string host, string service, string method,
                                  const S3AuthParams &auth_params, string date_now = "", string datetime_now = "",
                                  string payload_hash = "", string content_type = "", string content_md5 = "",
								  string encryption = "", string customer_key_id = "") {

	HTTPHeaders res;
	res["Host"] = host;
	// If access key is not set, we don't set the headers at all to allow accessing public files through s3 urls
	if (auth_params.secret_access_key.empty() && auth_params.access_key_id.empty()) {
		return res;
	}

	if (payload_hash == "") {
		payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"; // Empty payload hash
	}

	// we can pass date/time but this is mostly useful in testing. normally we just get the current datetime here.
	if (datetime_now.empty()) {
		auto timestamp = Timestamp::GetCurrentTimestamp();
		date_now = StrfTimeFormat::Format(timestamp, "%Y%m%d");
		datetime_now = StrfTimeFormat::Format(timestamp, "%Y%m%dT%H%M%SZ");
	}

	res["x-amz-date"] = datetime_now;
	res["x-amz-content-sha256"] = payload_hash;
	if (auth_params.session_token.length() > 0) {
		res["x-amz-security-token"] = auth_params.session_token;
	}

	/* Custom addition: Add customer managed key */
	if (encryption.length() > 0)
		res["x-amz-server-side-encryption"] = encryption;

	if (customer_key_id.length() > 0)
		res["x-amz-server-side-encryption-aws-kms-key-id"] = customer_key_id;

	string signed_headers = "";
	hash_bytes canonical_request_hash;
	hash_str canonical_request_hash_str;

	/* Custom addition: Add md5 if requested (needs to be before content-type) */
	if (content_md5.length() > 0) {
		res["content-md5"] = content_md5;
		signed_headers += "content-md5;";
	}

	if (content_type.length() > 0) {
		signed_headers += "content-type;";
	}
	signed_headers += "host;x-amz-content-sha256;x-amz-date";
	if (auth_params.session_token.length() > 0) {
		signed_headers += ";x-amz-security-token";
	}

	/* Custom addition: Add customer managed key */
	if (encryption.length() > 0)
		signed_headers += ";x-amz-server-side-encryption";
	if (customer_key_id.length() > 0)
		signed_headers += ";x-amz-server-side-encryption-aws-kms-key-id";

	auto canonical_request = method + "\n" + S3FileSystem::UrlEncode(url) + "\n" + query;

	/* Custom addition: Add md5 if requested (needs to be before content-type) */
	if (content_md5.length() > 0) {
		canonical_request += "\ncontent-md5:" + content_md5;
	}

	if (content_type.length() > 0) {
		canonical_request += "\ncontent-type:" + content_type;
	}

	canonical_request += "\nhost:" + host + "\nx-amz-content-sha256:" + payload_hash + "\nx-amz-date:" + datetime_now;
	if (auth_params.session_token.length() > 0) {
		canonical_request += "\nx-amz-security-token:" + auth_params.session_token;
	}

	/* Custom addition: Add customer managed key */
	if (encryption.length() > 0)
		canonical_request += "\nx-amz-server-side-encryption:" + encryption;
	if (customer_key_id.length() > 0)
		canonical_request += "\nx-amz-server-side-encryption-aws-kms-key-id:" + customer_key_id;

	canonical_request += "\n\n" + signed_headers + "\n" + payload_hash;
	sha256(canonical_request.c_str(), canonical_request.length(), canonical_request_hash);

	hex256(canonical_request_hash, canonical_request_hash_str);
	auto string_to_sign = "AWS4-HMAC-SHA256\n" + datetime_now + "\n" + date_now + "/" + auth_params.region + "/" +
	                      service + "/aws4_request\n" + string((char *)canonical_request_hash_str, sizeof(hash_str));
	// compute signature
	hash_bytes k_date, k_region, k_service, signing_key, signature;
	hash_str signature_str;
	auto sign_key = "AWS4" + auth_params.secret_access_key;
	hmac256(date_now, sign_key.c_str(), sign_key.length(), k_date);
	hmac256(auth_params.region, k_date, k_region);
	hmac256(service, k_region, k_service);
	hmac256("aws4_request", k_service, signing_key);
	hmac256(string_to_sign, signing_key, signature);
	hex256(signature, signature_str);

	res["Authorization"] = "AWS4-HMAC-SHA256 Credential=" + auth_params.access_key_id + "/" + date_now + "/" +
	                       auth_params.region + "/" + service + "/aws4_request, SignedHeaders=" + signed_headers +
	                       ", Signature=" + string((char *)signature_str, sizeof(hash_str));

	return res;
}


/*
 * GetPayloadHash is directly copy-pasted from s3fs.cpp, where it
 * declared static.
 *
 * We need it for our custom PostRequest implementation.
 */
static string
GetPayloadHash(char *buffer, idx_t buffer_len)
{
	if (buffer_len > 0) {
		hash_bytes payload_hash_bytes;
		hash_str payload_hash_str;
		sha256(buffer, buffer_len, payload_hash_bytes);
		hex256(payload_hash_bytes, payload_hash_str);
		return string((char *)payload_hash_str, sizeof(payload_hash_str));
	} else {
		return "";
	}
}


/*
 * MD5 calculates an MD5 hash for a given buffer using DuckDB functions.
 */
static string
GetMD5(char *buffer, idx_t bufferLength)
{
	data_t md5Blob[MD5Context::MD5_HASH_LENGTH_BINARY];
   	MD5Context md5Context;
	md5Context.Add((const_data_ptr_t) buffer, bufferLength);
	md5Context.Finish(md5Blob);
	string_t md5String((const char *) md5Blob, MD5Context::MD5_HASH_LENGTH_BINARY);

	char md5Base64[MD5_HASH_LENGTH_BASE64];
	Blob::ToBase64(md5String, md5Base64);

	return string(md5Base64, MD5_HASH_LENGTH_BASE64);
}


/*
 * IsPgLakeManagedStorageBucket returns whether the given bucket is the
 * managed storage bucket.
 */
static bool
IsPgLakeManagedStorageBucket(optional_ptr<ClientContext> context, string prefix, string bucket)
{
	if (context == nullptr)
		return false;

	Value setting;

	if (!context->TryGetCurrentSetting(MANAGED_STORAGE_BUCKET_SETTING, setting))
		return false;

	string managedStorageBucket = setting.ToString();

	/* we ignore empty string and "NULL", the latter is used in case of reset */
	if (managedStorageBucket.empty() || managedStorageBucket == "NULL")
		return false;

	/* remove trailing slash */
    if (managedStorageBucket.back() == '/')
        managedStorageBucket.pop_back();

   return prefix + bucket == managedStorageBucket;
}


/*
 * SetEncryptionFields determines the encryption and customer_key_id for a given
 * request. In particular, it sets the customer_key_id option for writes to the
 * managed storage bucket if a key ID is configured.
 */
static void
SetEncryptionFields(PgLakeS3FileHandle &s3Handle, ParsedS3Url &parsed_s3_url,
					string &encryption, string &customer_key_id)
{
	Value setting;

	if (s3Handle.context->TryGetCurrentSetting(MANAGED_STORAGE_KEY_ID_SETTING, setting) &&
		IsPgLakeManagedStorageBucket(s3Handle.context, parsed_s3_url.prefix, parsed_s3_url.bucket))
	{
		/* use customer managed key */
		customer_key_id = setting.ToString();

		/*
		 * If the setting has been disabled via RESET, the value becomes "NULL" (?),
		 * we then treat it as an empty string (ignored).
		 */
		if (customer_key_id == "NULL")
			customer_key_id = "";

		if (!customer_key_id.empty())
			encryption = "aws:kms";
	}
}


/*
 * PostRequest is mostly copy-pasted from S3FileSystem::PostRequest,
 * but with the addition of Content-MD5, which is required for DeleteObjects.
 */
unique_ptr<HTTPResponse>
PgLakeS3FileSystem::PostRequest(FileHandle &handle, string url, HTTPHeaders header_map,
                                 string &buffer_out,
                                 char *buffer_in, idx_t buffer_in_len, string http_params)
{
	PgLakeS3FileHandle &s3Handle = handle.Cast<PgLakeS3FileHandle>();
	auto auth_params = s3Handle.auth_params;
	auto parsed_s3_url = S3UrlParse(url, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params, http_params);
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	string content_md5 = "";
	string encryption = "";
	string customer_key_id = "";

	/*
	 * For CreateMultipartUpload operations (?uploads=...), use the customer-managed key, if any.
	 */
	if (http_params.find("uploads=") != std::string::npos)
		SetEncryptionFields(s3Handle, parsed_s3_url, encryption, customer_key_id);

	/*
	 * For DeleteObjects operations we need to specify the Content-MD5 header.
	 */
	if (http_params.find("delete=") != std::string::npos)
		content_md5 = GetMD5(buffer_in, buffer_in_len);

	auto headers = create_s3_header(parsed_s3_url.path, http_params, parsed_s3_url.host, "s3", "POST", auth_params, "",
	                                "", payload_hash, "application/octet-stream", content_md5, encryption, customer_key_id);

	return HTTPFileSystem::PostRequest(handle, http_url, headers, buffer_out, buffer_in, buffer_in_len);
}

unique_ptr<HTTPResponse>
PgLakeS3FileSystem::PutRequest(FileHandle &handle, string url, HTTPHeaders header_map,
								char *buffer_in, idx_t buffer_in_len, string http_params)
{
	PgLakeS3FileHandle &s3Handle = handle.Cast<PgLakeS3FileHandle>();
	auto auth_params = s3Handle.auth_params;
	auto parsed_s3_url = S3UrlParse(url, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params, http_params);
	auto content_type = "application/octet-stream";
	auto payload_hash = GetPayloadHash(buffer_in, buffer_in_len);

	string encryption = "";
	string customer_key_id = "";

	/*
	 * For PutObject operations (no params), use the customer-managed key, if any.
	 */
	if (http_params.empty())
		SetEncryptionFields(s3Handle, parsed_s3_url, encryption, customer_key_id);

	auto headers = create_s3_header(parsed_s3_url.path, http_params, parsed_s3_url.host, "s3", "PUT", auth_params, "",
	                                "", payload_hash, content_type, "", encryption, customer_key_id);
	return HTTPFileSystem::PutRequest(handle, http_url, headers, buffer_in, buffer_in_len);
}

/*
 * Download performs similar logic to GetRequest, except writing the output
 * to a destination file rather than an in-memory buffer.
 */
int64_t
PgLakeS3FileSystem::Download(ClientContext &context, FileHandle &inputHandle, FileHandle &outputHandle)
{
	auto auth_params = inputHandle.Cast<PgLakeS3FileHandle>().auth_params;
	auto parsed_s3_url = S3UrlParse(inputHandle.path, auth_params);
	string http_url = parsed_s3_url.GetHTTPUrl(auth_params);
	auto headers =
	    create_s3_header(parsed_s3_url.path, "", parsed_s3_url.host, "s3", "GET", auth_params, "", "", "", "");

	PgLakeHTTPFileSystem httpfs;
	return httpfs.Download(context, inputHandle, http_url, headers, outputHandle);
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
 * ParseXmlValue is a simple parsing function for extracting a value from an
 * XML field.
 */
static string
ParseXmlValue(string &xmlFragment, string key)
{
	string openTag = "<" + key + ">";
	string closeTag = "</" + key + ">";

	auto openTagPos = xmlFragment.find(openTag);
	if (openTagPos == string::npos)
		throw InternalException("Failed to parse S3 result: " + openTag + " not found");

	auto closeTagPos = xmlFragment.find(closeTag, openTag.length());
	if (closeTagPos == string::npos)
		throw InternalException("Failed to parse S3 result: " + closeTag + " not found");

	return xmlFragment.substr(openTagPos + openTag.length(), closeTagPos - openTagPos - openTag.length());
}


/*
 * ETag is quoted, and due to encoding-type=url in the request we
 * get funky etag quoting, which is not quite consistent across
 * implementations (e.g. moto vs. S3).
 *
 * The string might look like &quot;...&quot; or &#34;...&#34;
 *
 * We replace with regular quotes in this function.
 */
static void
UnescapeEtag(string &etag, string quote)
{
	if (StringUtil::StartsWith(etag, quote))
		etag = etag.replace(0, quote.length(), "\"");

	if (StringUtil::EndsWith(etag, quote))
		etag = etag.replace(etag.length() - quote.length(), quote.length(), "\"");
}

/*
 * ParseOpenFileInfo is based on AWSListObjectV2::ParseKey, but also parses
 * size and last modified time.
 */
static void
ParseOpenFileInfo(string &awsResponse, bool isGlob, vector<OpenFileInfo> &result)
{
	string openTag = "<Contents>";
	string closeTag = "</Contents>";
	idx_t currentPos = 0;

	while (true) {
		auto openTagPos = awsResponse.find(openTag, currentPos);
		if (openTagPos == string::npos)
			break;

		auto closeTagPos = awsResponse.find(closeTag, openTagPos + openTag.length());
		if (closeTagPos == string::npos)
			throw InternalException("Failed to parse S3 result: " + closeTag + " not found");

		string xmlFragment =
			awsResponse.substr(openTagPos + openTag.length(), closeTagPos - openTagPos - openTag.length());

		currentPos = closeTagPos + closeTag.length();

		string path = S3FileSystem::UrlDecode(ParseXmlValue(xmlFragment, "Key"));

		/* we exclude directories from the result */
		if (path.back() == '/')
			continue;

		/* construct file metadata */
		OpenFileInfo fileDesc(path);

		fileDesc.extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		auto &options = fileDesc.extended_info->options;
		auto timestampStr = ParseXmlValue(xmlFragment, "LastModified");
		options.emplace("file_size", Value::BIGINT(std::stol(ParseXmlValue(xmlFragment, "Size"))));
		options.emplace("last_modified", Value::TIMESTAMP(Timestamp::FromCString(timestampStr.c_str(), timestampStr.length())));

		/* for pg_lake list we also want etag */
		if (!isGlob)
		{
			auto etag = ParseXmlValue(xmlFragment, "ETag");
			UnescapeEtag(etag, "&quot;");
			UnescapeEtag(etag, "&#34;");
			options.emplace("etag", etag);
		}

		result.push_back(fileDesc);
	}
}


/*
 * List returns a list of file descriptions that match the given glob
 * pattern, including size and last modified time.
 *
 * Mostly copy-pasted from Glob in s3fs.cpp (code style preserved),
 * modified to return a vector of OpenFileInfo instead of keys only.
 */
vector<OpenFileInfo>
PgLakeS3FileSystem::List(const string &glob_pattern, bool is_glob, FileOpener *opener)
{
	if (opener == nullptr) {
		throw InternalException("Cannot S3 Glob without FileOpener");
	}

	optional_ptr<ClientContext> context = opener->TryGetClientContext();

	FileOpenerInfo info = {glob_pattern};

	// Trim any query parameters from the string
	S3AuthParams s3_auth_params = S3AuthParams::ReadFrom(opener, info);

	// In url compatibility mode, we ignore globs allowing users to query files with the glob chars
	if (s3_auth_params.s3_url_compatibility_mode && is_glob) {
		OpenFileInfo fileDesc(glob_pattern);
		return {fileDesc};
	}

	auto parsed_s3_url = S3UrlParse(glob_pattern, s3_auth_params);
	auto parsed_glob_url = parsed_s3_url.trimmed_s3_url;

	// AWS matches on prefix, not glob pattern, so we take a substring until the first wildcard char for the aws calls
	auto first_wildcard_pos = parsed_glob_url.find_first_of("*[\\");
	if (first_wildcard_pos == string::npos && is_glob) {
		OpenFileInfo fileDesc(glob_pattern);

		return {fileDesc};
	}

	string shared_path = parsed_glob_url.substr(0, first_wildcard_pos);

	auto db = opener->TryGetDatabase();
	auto &http_util = HTTPUtil::Get(*db);
	auto http_params = http_util.InitializeParameters(*context, glob_pattern);

	ReadQueryParams(parsed_s3_url.query_param, s3_auth_params);

	// Work around incomplete change made in https://github.com/duckdb/duckdb-httpfs/pull/83/files
	// The endpoint is not adapted to the s3_region query parameter, which we rely on for
	// region injection.
	if (StringUtil::EndsWith(s3_auth_params.endpoint, ".amazonaws.com"))
		s3_auth_params.endpoint = StringUtil::Format("s3.%s.amazonaws.com", s3_auth_params.region);

	// Do main listobjectsv2 request
	vector<OpenFileInfo> s3_file_descs;
	string main_continuation_token;

	// Main paging loop
	do {
		if (context->interrupted)
			throw InterruptException();

		string response_str = AWSListObjectV2::Request(shared_path, *http_params, s3_auth_params,
		                                               main_continuation_token, HTTPState::TryGetState(opener).get());
		if (response_str.empty())
			throw HTTPException("no list response (most likely the wrong region)");

		main_continuation_token = AWSListObjectV2::ParseContinuationToken(response_str);
		ParseOpenFileInfo(response_str, is_glob, s3_file_descs);

		// Repeat requests until the keys of all common prefixes are parsed.
		auto common_prefixes = AWSListObjectV2::ParseCommonPrefix(response_str);
		while (!common_prefixes.empty()) {
			auto prefix_path = parsed_s3_url.prefix + parsed_s3_url.bucket + '/' + common_prefixes.back();
			common_prefixes.pop_back();

			// TODO we could optimize here by doing a match on the prefix, if it doesn't match we can skip this prefix
			// Paging loop for common prefix requests
			string common_prefix_continuation_token;
			do {
				auto prefix_res =
				    AWSListObjectV2::Request(prefix_path, *http_params, s3_auth_params, common_prefix_continuation_token,
				                             HTTPState::TryGetState(opener).get());
				ParseOpenFileInfo(prefix_res, is_glob, s3_file_descs);
				auto more_prefixes = AWSListObjectV2::ParseCommonPrefix(prefix_res);
				common_prefixes.insert(common_prefixes.end(), more_prefixes.begin(), more_prefixes.end());
				common_prefix_continuation_token = AWSListObjectV2::ParseContinuationToken(prefix_res);
			} while (!common_prefix_continuation_token.empty());
		}
	} while (!main_continuation_token.empty());

	vector<string> pattern_splits = StringUtil::Split(parsed_s3_url.key, "/");
	vector<OpenFileInfo> result;

	for (OpenFileInfo &s3_file_desc : s3_file_descs) {
        string s3_key = s3_file_desc.path;
		vector<string> key_splits = StringUtil::Split(s3_key, "/");
		bool is_match = Match(key_splits.begin(), key_splits.end(), pattern_splits.begin(), pattern_splits.end());

		if (is_match) {
			string result_full_url = parsed_s3_url.prefix + parsed_s3_url.bucket + "/" + s3_key;

			// if a ? char was present, we re-add it here as the url parsing will have trimmed it.
			if (is_glob && !parsed_s3_url.query_param.empty()) {
				result_full_url += '?' + parsed_s3_url.query_param;
			}

            s3_file_desc.path = result_full_url;
			result.push_back(s3_file_desc);
		}
	}
	return result;
}



} // namespace duckdb
