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
#include "duckdb/main/client_context_file_opener.hpp"
#include "duckdb/main/secret/secret_manager.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "create_secret_functions.hpp"
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
 * S3 DeleteObjects accepts at most 1000 keys in a single request.
 */
static constexpr idx_t S3_DELETE_OBJECTS_MAX_KEYS = 1000;

/*
 * A DeleteObjects response can report an error per key, so up to 1000 of them.
 * Name the first few in the error we throw and only count the rest.
 */
static constexpr idx_t S3_DELETE_OBJECTS_MAX_REPORTED_ERRORS = 3;

/*
 * Name of the setting that specifies the location of pg_lake managed storage bucket.
 */
const string MANAGED_STORAGE_BUCKET_SETTING = "pg_lake_managed_storage_bucket";

/*
 * Name of the setting that specifies the Amazon KMS key ID to use when writing to
 * the managed storage bucket.
 */
const string MANAGED_STORAGE_KEY_ID_SETTING = "pg_lake_managed_storage_key_id";



void
PgLakeS3FileSystem::RegisterContext(const shared_ptr<HTTPInput> &input, optional_ptr<ClientContext> context)
{
	lock_guard<mutex> guard(context_mutex_);

	/* Lazily clean up entries for destroyed handles */
	for (auto it = context_map_.begin(); it != context_map_.end();) {
		if (it->second.input_ref.expired())
			it = context_map_.erase(it);
		else
			++it;
	}

	context_map_[input.get()] = {input, context};
}

optional_ptr<ClientContext>
PgLakeS3FileSystem::LookupContext(HTTPInput *input)
{
	lock_guard<mutex> guard(context_mutex_);
	auto it = context_map_.find(input);
	if (it != context_map_.end() && !it->second.input_ref.expired())
		return it->second.context;
	return nullptr;
}

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

	auto &http_util = HTTPFSUtil::GetHTTPUtil(opener);
	auto params = http_util.InitializeParameters(opener, info);

	auto handle = duckdb::make_uniq<PgLakeS3FileHandle>(*this, fileInfo.path, flags, context,
	                                                     params,
	                                                     auth_params,
	                                                     S3ConfigParams::ReadFrom(opener));

	RegisterContext(handle->http_input, context);

	return unique_ptr_cast<PgLakeS3FileHandle, HTTPFileHandle>(std::move(handle));
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
		 */
		if (!IsNotFoundError(error))
			throw;
	}
}


/*
 * EscapeXmlText escapes a string for use as XML character data. S3 object keys
 * may contain any UTF-8, including '&' and '<', so a key cannot be pasted into
 * a request body verbatim: an unescaped '&' makes the whole DeleteObjects body
 * malformed XML and S3 rejects the batch, and a key holding '<' could otherwise
 * inject elements and change which objects the request targets.
 *
 * Not handled: S3 also allows bytes 0x00-0x1f in a key, and XML 1.0 cannot
 * represent those at all, not even as a character reference. Such a key makes
 * the body malformed and so takes its whole batch down with it, where the
 * one-key-per-request version only failed its own delete.
 */
static string
EscapeXmlText(const string &text)
{
	string escaped;
	escaped.reserve(text.size());

	for (char c : text)
	{
		switch (c)
		{
			case '&':
				escaped += "&amp;";
				break;
			case '<':
				escaped += "&lt;";
				break;
			case '>':
				escaped += "&gt;";
				break;
			default:
				escaped += c;
				break;
		}
	}

	return escaped;
}


/*
 * ExtractXmlElementText returns the character data of the first <tag>...</tag>
 * in xml, or an empty string if there is none.
 *
 * This is not an XML parser: we only use it on the elements of a DeleteResult
 * body, which S3 generates and which carry no attributes or CDATA sections. The
 * text comes back as S3 escaped it.
 */
static string
ExtractXmlElementText(const string &xml, const string &tag)
{
	string openTag = "<" + tag + ">";
	string closeTag = "</" + tag + ">";

	size_t textStart = xml.find(openTag);

	if (textStart == string::npos)
		return string();

	textStart += openTag.size();

	size_t textEnd = xml.find(closeTag, textStart);

	if (textEnd == string::npos)
		return string();

	return xml.substr(textStart, textEnd - textStart);
}


/*
 * PostDeleteObjects issues a single S3 DeleteObjects request for the keys in
 * [begin, end). s3Handle must already be pointed at "<prefix><bucket>/" so the
 * POST targets /?delete, and every key must live in that bucket. The caller is
 * responsible for keeping the range within S3_DELETE_OBJECTS_MAX_KEYS.
 *
 * Throws on a malformed response or if S3 reports per-key <Error> entries.
 * Deleting a key that does not exist is not an error in S3, so a well-formed
 * DeleteResult that still carries an <Error> means a real failure (e.g. access
 * denied) that we must surface rather than silently dropping keys.
 *
 * The response also lists the keys that were deleted, but for a request without
 * errors that is simply the keys we asked for, and when there are errors we
 * throw. So we only read the <Error> entries.
 */
static void
PostDeleteObjects(PgLakeS3FileSystem &fs, S3FileHandle *s3Handle,
				  const vector<string> &keys, idx_t begin, idx_t end)
{
	/*
	 * Following S3FileSystem::FinalizeMultipartUpload
	 */
	std::stringstream ss;
	ss << "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">";
	for (idx_t i = begin; i < end; i++)
		ss << "<Object><Key>" << EscapeXmlText(keys[i]) << "</Key></Object>";
	ss << "</Delete>";
	string body = ss.str();

	/* PostRequest assigns the response body, so it needs no room up front */
	string responseBuffer;

	/* Perform the batch deletion */
	unique_ptr<HTTPResponse> postResponse =
		fs.PostRequest(*s3Handle->http_input, s3Handle->path, {}, responseBuffer,
					   (char *) body.c_str(), body.length(), "delete=");

	/* Body of the POST response */
	const string &result = responseBuffer;

	if (result.find("<DeleteResult", 0) == string::npos)
		throw HTTPException(*postResponse,
							"Unexpected response during S3 DeleteObjects: %d\n\n%s",
							postResponse->status,
		                    result);

	/*
	 * Name the keys that failed and why, instead of handing the caller a body
	 * that can hold a thousand entries.
	 */
	idx_t errorCount = 0;
	string errorDetails;

	for (size_t errorPos = result.find("<Error>");
		 errorPos != string::npos;
		 errorPos = result.find("<Error>", errorPos + 1))
	{
		errorCount++;

		if (errorCount > S3_DELETE_OBJECTS_MAX_REPORTED_ERRORS)
			continue;

		/* limit the search to this entry, in case one of the elements is absent */
		size_t errorEnd = result.find("</Error>", errorPos);
		string errorEntry = result.substr(errorPos, errorEnd == string::npos
												   ? string::npos : errorEnd - errorPos);

		if (!errorDetails.empty())
			errorDetails += ", ";

		errorDetails += StringUtil::Format("%s: %s (%s)",
										   ExtractXmlElementText(errorEntry, "Key"),
										   ExtractXmlElementText(errorEntry, "Code"),
										   ExtractXmlElementText(errorEntry, "Message"));
	}

	if (errorCount > 0)
	{
		if (errorCount > S3_DELETE_OBJECTS_MAX_REPORTED_ERRORS)
			errorDetails += StringUtil::Format(", and %llu more",
											   (uint64_t) (errorCount - S3_DELETE_OBJECTS_MAX_REPORTED_ERRORS));

		throw HTTPException(*postResponse,
							"S3 DeleteObjects failed for %llu of %llu keys: %s",
							(uint64_t) errorCount, (uint64_t) (end - begin), errorDetails);
	}
}


/*
 * RemoveFileFromS3 deletes a single key from a bucket using the batch
 * deletion API.
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

	vector<string> keys = {parsedUrl.key};

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

	PostDeleteObjects(*this, s3Handle, keys, 0, keys.size());

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
 * RemoveFilesFromS3 deletes many keys using the S3 DeleteObjects batch API,
 * sending up to S3_DELETE_OBJECTS_MAX_KEYS keys per request instead of one key
 * per request. It is the bulk counterpart of RemoveFileFromS3 and is meant for
 * removing a whole prefix's worth of objects (e.g. a dropped Iceberg table).
 *
 * The request goes to bucketUrl, which is the only URL involved: the keys
 * travel in the request body. So bucketUrl is the one that has to carry the
 * query arguments the request needs, region included, and the paths can be
 * plain s3:// URLs. RegionAwareS3FileSystem::RemoveFiles groups them by bucket
 * and resolves the region.
 */
void
PgLakeS3FileSystem::RemoveFilesFromS3(const string &bucketUrl,
									  const vector<string> &paths,
									  optional_ptr<FileOpener> opener)
{
	if (paths.empty())
		return;

	optional_ptr<HTTPMetadataCache> metadataCache = GetGlobalCache();

	/*
	 * Read the auth parameters once instead of per path: ReadFrom does a dozen
	 * secret manager and setting lookups, all paths are in the same bucket, and
	 * the only field the bucket/key split reads is s3_url_compatibility_mode.
	 */
	FileOpenerInfo s3UrlInfo = {bucketUrl};
	S3AuthParams authParams = S3AuthParams::ReadFrom(opener, s3UrlInfo);

	ParsedS3Url parsedBucketUrl = S3UrlParse(bucketUrl, authParams);
	string bareBucketUrl = parsedBucketUrl.prefix + parsedBucketUrl.bucket;

	vector<string> keys;

	keys.reserve(paths.size());

	for (const string &path : paths)
	{
		ParsedS3Url parsedUrl = S3UrlParse(path, authParams);

		if (parsedUrl.prefix + parsedUrl.bucket != bareBucketUrl)
			throw InternalException("cannot delete %s in a DeleteObjects request for %s",
									path, bareBucketUrl);

		keys.push_back(parsedUrl.key);
	}

	/*
	 * Build one POST-capable handle and reuse it for every batch. It comes from
	 * the bucket URL, which is where the request goes and which carries the
	 * region the caller resolved.
	 *
	 * CreateHandle rather than OpenFile: we only need the auth and HTTP
	 * parameters, and OpenFile would additionally HEAD the URL, which costs a
	 * round trip and, on a bucket URL, has nothing to report anyway.
	 */
	unique_ptr<HTTPFileHandle> fileHandle =
		CreateHandle(bucketUrl, FileFlags::FILE_FLAGS_READ, opener);

	S3FileHandle *s3Handle = (S3FileHandle *) fileHandle.get();

	/*
	 * Point the handle at / to POST to /?delete. The query arguments can come
	 * off here: PostRequest signs with the handle's auth parameters, which
	 * CreateHandle already read them into.
	 */
	s3Handle->path = bareBucketUrl + "/";

	/*
	 * Best-effort HTTP metadata cache hygiene, as in RemoveFileFromS3: drop any
	 * cached entry for the deleted objects so a stale entry cannot make a
	 * removed file look readable. Reads inject ?s3_region from cache, so evict
	 * both the bare URL and the region-resolved form, which is the bucket URL's
	 * query string on the key.
	 */
	string querySuffix;
	auto queryPos = bucketUrl.find('?');
	if (queryPos != string::npos)
		querySuffix = bucketUrl.substr(queryPos);

	auto evictBatch = [&](idx_t begin, idx_t end) {
		for (idx_t i = begin; i < end; i++)
		{
			string fullUrl = bareBucketUrl + "/" + keys[i];

			metadataCache->Erase(fullUrl);
			if (!querySuffix.empty())
				metadataCache->Erase(fullUrl + querySuffix);
		}
	};

	for (idx_t begin = 0; begin < keys.size(); begin += S3_DELETE_OBJECTS_MAX_KEYS)
	{
		/*
		 * Both arguments must be the same type for MinValue to deduce one: idx_t
		 * is uint64_t, which is "unsigned long long" on macOS/arm64 but
		 * "unsigned long" -- the same type vector::size_type already is -- on
		 * Linux/x86_64, so leaving keys.size() unconverted only compiles there.
		 */
		idx_t end = MinValue<idx_t>(begin + S3_DELETE_OBJECTS_MAX_KEYS,
									static_cast<idx_t>(keys.size()));

		/*
		 * Evict per batch rather than once at the end, and both before and after
		 * the request, for the same reasons the file cache does it in
		 * PGLakeCachingFileSystem::RemoveFiles: PostDeleteObjects throws when S3
		 * reports a per-key error, and the keys it did delete are gone whether or
		 * not we get to the second eviction; a concurrent reader can also cache
		 * an entry again in the window between the eviction and the delete.
		 * Evicting an entry for a file that is still there only costs a HEAD.
		 */
		evictBatch(begin, end);

		PostDeleteObjects(*this, s3Handle, keys, begin, end);

		evictBatch(begin, end);
	}
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
SetEncryptionFields(optional_ptr<ClientContext> context, ParsedS3Url &parsed_s3_url,
					string &encryption, string &customer_key_id)
{
	/*
	 * Without the context we cannot tell whether this write goes to managed
	 * storage, nor which key it should use. Writing unencrypted would be worse
	 * than failing. This has to stay an IOException: an INTERNAL one invalidates
	 * the database, which pgduck_server shares across all its sessions.
	 */
	if (context == nullptr)
		throw IOException("cannot determine encryption settings for write to %s: "
						  "no client context registered for the file handle",
						  parsed_s3_url.bucket);

	Value setting;

	if (context->TryGetCurrentSetting(MANAGED_STORAGE_KEY_ID_SETTING, setting) &&
		IsPgLakeManagedStorageBucket(context, parsed_s3_url.prefix, parsed_s3_url.bucket))
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
 * IsAuthError determines whether a response failed in a way that expired
 * credentials would explain.
 */
static bool
IsAuthError(const HTTPResponse &response)
{
	return response.status == HTTPStatusCode::Unauthorized_401 ||
		   response.status == HTTPStatusCode::Forbidden_403;
}


/*
 * TryRefreshAuthParams refreshes the secret that applies to the given URL and
 * copies the resulting credentials into auth_params. Returns whether the
 * credentials changed.
 *
 * S3FileSystem does this for the requests it signs itself, but PostRequest and
 * PutRequest below sign their own requests, so they refresh their own
 * credentials. We do not write the result back into the S3HTTPInput, since
 * upstream guards that copy with a private mutex, meaning that each request
 * refreshes for itself.
 */
static bool
TryRefreshAuthParams(optional_ptr<ClientContext> context, const string &url, S3AuthParams &auth_params)
{
	if (context == nullptr)
		return false;

	auto transaction = CatalogTransaction::GetSystemCatalogTransaction(*context);
	auto &secretManager = context->db->GetSecretManager();
	bool refreshedSecret = false;

	for (const string type : {"s3", "r2", "gcs", "aws"})
	{
		auto match = secretManager.LookupSecret(transaction, url, type);

		if (match.HasMatch())
			refreshedSecret |= CreateS3SecretFunctions::TryRefreshS3Secret(*context, *match.secret_entry);
	}

	if (!refreshedSecret)
		return false;

	ClientContextFileOpener opener(*context);
	FileOpenerInfo info = {url};
	auto refreshed = S3AuthParams::ReadFrom(opener, info);

	if (refreshed.access_key_id == auth_params.access_key_id &&
		refreshed.secret_access_key == auth_params.secret_access_key &&
		refreshed.session_token == auth_params.session_token)
		return false;

	/*
	 * Only take over the credentials, since the region and endpoint may have
	 * come from the query string rather than from the secret.
	 */
	auth_params.access_key_id = refreshed.access_key_id;
	auth_params.secret_access_key = refreshed.secret_access_key;
	auth_params.session_token = refreshed.session_token;

	return true;
}


/*
 * PostRequest is mostly copy-pasted from S3FileSystem::PostRequest,
 * but with the addition of Content-MD5, which is required for DeleteObjects.
 */
unique_ptr<HTTPResponse>
PgLakeS3FileSystem::PostRequest(HTTPInput &input, string url, HTTPHeaders header_map,
                                 string &buffer_out,
                                 char *buffer_in, idx_t buffer_in_len, string http_params)
{
	auto &s3_input = input.Cast<S3HTTPInput>();
	auto auth_params = s3_input.auth_params;
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
		SetEncryptionFields(LookupContext(&input), parsed_s3_url, encryption, customer_key_id);

	/*
	 * For DeleteObjects operations we need to specify the Content-MD5 header.
	 */
	if (http_params.find("delete=") != std::string::npos)
		content_md5 = GetMD5(buffer_in, buffer_in_len);

	auto sendRequest = [&]() {
		auto headers = create_s3_header(parsed_s3_url.path, http_params, parsed_s3_url.host, "s3", "POST", auth_params, "",
		                                "", payload_hash, "application/octet-stream", content_md5, encryption, customer_key_id);

		return HTTPFileSystem::PostRequest(input, http_url, headers, buffer_out, buffer_in, buffer_in_len);
	};

	auto response = sendRequest();

	/* credentials can expire in the middle of a multi-part upload */
	if (IsAuthError(*response) && TryRefreshAuthParams(LookupContext(&input), url, auth_params))
		response = sendRequest();

	return response;
}

unique_ptr<HTTPResponse>
PgLakeS3FileSystem::PutRequest(HTTPInput &input, string url, HTTPHeaders header_map,
								char *buffer_in, idx_t buffer_in_len, string http_params)
{
	auto &s3_input = input.Cast<S3HTTPInput>();
	auto auth_params = s3_input.auth_params;
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
		SetEncryptionFields(LookupContext(&input), parsed_s3_url, encryption, customer_key_id);

	auto sendRequest = [&]() {
		auto headers = create_s3_header(parsed_s3_url.path, http_params, parsed_s3_url.host, "s3", "PUT", auth_params, "",
		                                "", payload_hash, content_type, "", encryption, customer_key_id);

		return HTTPFileSystem::PutRequest(input, http_url, headers, buffer_in, buffer_in_len);
	};

	auto response = sendRequest();

	/* credentials can expire in the middle of a multi-part upload */
	if (IsAuthError(*response) && TryRefreshAuthParams(LookupContext(&input), url, auth_params))
		response = sendRequest();

	return response;
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

		// List without a delimiter so that a single request set returns every key
		// under the prefix, and pass the opener so expired credentials can be
		// refreshed for list requests too.
		string response_str = AWSListObjectV2::Request(shared_path, *http_params, s3_auth_params,
		                                               main_continuation_token, false, optional_idx(), opener);
		if (response_str.empty())
			throw HTTPException("no list response (most likely the wrong region)");

		main_continuation_token = AWSListObjectV2::ParseContinuationToken(response_str);
		ParseOpenFileInfo(response_str, is_glob, s3_file_descs);

		// Repeat requests until the keys of all common prefixes are parsed.
		auto common_prefixes = AWSListObjectV2::ParseCommonPrefix(response_str);
		while (!common_prefixes.empty()) {
			// ListObjectsV2 is called with encoding-type=url, so the prefixes come back
			// percent-encoded while AWSListObjectV2::Request encodes the path again.
			// Decode here or the follow-up request lists a prefix that does not exist,
			// e.g. a partition directory named "a b" becomes "a%2520b". Upstream does the
			// same decode for its common prefixes in S3GlobResult.
			auto prefix_path = S3FileSystem::UrlDecode(
			    parsed_s3_url.prefix + parsed_s3_url.bucket + '/' + common_prefixes.back());
			common_prefixes.pop_back();

			// TODO we could optimize here by doing a match on the prefix, if it doesn't match we can skip this prefix
			// Paging loop for common prefix requests
			string common_prefix_continuation_token;
			do {
				auto prefix_res = AWSListObjectV2::Request(prefix_path, *http_params, s3_auth_params,
				                                           common_prefix_continuation_token, false, optional_idx(),
				                                           opener);
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
