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

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "httpfs.hpp"
#include "s3fs.hpp"
#include "httplib.hpp"

#include "pg_lake/fs/httpfs_extended.hpp"
#include "pg_lake/fs/file_cache_manager.hpp"
#include "pg_lake/fs/file_utils.hpp"
#include "pg_lake/fs/region_aware_s3fs.hpp"
#include "pg_lake/utils/pgduck_log_utils.h"

namespace duckdb {

/* from https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-express-networking.html */
map<string, string> S3ExpressRegionShorthand = {
	{"use1", "us-east-1"},
	{"use2", "us-east-2"},
	{"usw2", "us-west-2"},
	{"aps1", "ap-south-1"},
	{"apne1", "ap-northeast-1"},
	{"euw1", "eu-west-1"},
	{"eun1", "eu-north-1"}
};

static std::regex S3_EXPRESS_URL_PATTERN("s3://[a-z0-9-]+-([a-z0-9]+)-([a-z0-9]+)--x-s3/.*");

static bool UrlHasQueryArgument(const string &url, const string &queryArgument);
static string AddQueryArgumentToUrl(const string &url, const string &name, const string &value);
static string RemoveQueryArgumentFromUrl(string url, const string &name);


/*
 * Glob is the file system function for listing files. We add logic to
 * auto-select the region.
 *
 * We override the s3fs Glob with our own implementation. The reason is
 * mainly pragmatism: We needed a more complete List operation, and we
 * needed automatic region selection for that. It's easier to reuse the
 * the complete implementation than to implement Glob than to implement
 * automatic region selection in multiple places.
 */
vector<OpenFileInfo>
RegionAwareS3FileSystem::Glob(const string &urlPattern, FileOpener *opener)
{
	/* get all the file details */
	bool isGlob = true;
	return List(urlPattern, isGlob, opener);
}


/*
 * List is a generic API around the ListObjectV2 operation on S3, which returns
 * names, size, etag, and last modified time.
 */
vector<OpenFileInfo>
RegionAwareS3FileSystem::List(const string &urlPattern, bool isGlob, FileOpener *opener)
{
	/* as far as we know, looking up bucket region only works for S3 */
	if (!StringUtil::StartsWith(urlPattern, "s3://"))
		return s3fs.List(urlPattern, isGlob, opener);

	/* user explicitly asked for a region, don't try to be smart */
	if (UrlHasQueryArgument(urlPattern, "s3_region"))
		return s3fs.List(urlPattern, isGlob, opener);

	/* extract the bucket URL */
	string bucketUrl = GetBucketUrl(urlPattern, opener);

	/* S3 express buckets encode the region and endpoint in the name */
	if (StringUtil::EndsWith(bucketUrl, "--x-s3") &&
		!UrlHasQueryArgument(urlPattern, "s3_endpoint"))
	{
		string expressUrl;
		if (AddS3ExpressRegionEndpoint(urlPattern, expressUrl))
			return s3fs.List(expressUrl, isGlob, opener);
	}

	/* if we previously looked up the region, use it */
	string cachedRegion = GetCachedRegion(bucketUrl, opener);

	idx_t tries = 0;

	/*
	 * We retry several times in case of 500 error. Note that we currently retry
	 * the entire Glob which may consist of multiple pagination requests.
	 *
	 * It would be preferable to retry individual request, but that would require
	 * a deeper fork of httpfs.
	 */
	while (true)
	{
		try
		{
			if (!cachedRegion.empty())
			{
				/* use the cached region, we may still fail and refresh the region */
				return ListWithRegion(urlPattern, cachedRegion, isGlob, opener);
			}
			else
				/* optimistically list the files, even if we don't know region */
				return s3fs.List(urlPattern, isGlob, opener);
		}
		catch (Exception &ex)
		{
			/* wuh oh, something went wrong, could it be the region? */
			ErrorData error(ex);

			if (error.Type() != ExceptionType::HTTP)
				throw;

			/*
			 * Slightly hacky to check for error messages, but we don't get
			 * detailed error codes.
			 */
			if (error.Message().find("HTTP 400") != std::string::npos ||
				error.Message().find("no list response") != std::string::npos ||
				error.Message().find("301") != std::string::npos)
			{
				PGDUCK_SERVER_DEBUG("Glob failed: %s", error.Message().c_str());

				/* get the actual region from S3 headers */
				string actualRegion = GetBucketRegionFromS3(urlPattern, opener);

				/* could not determine region, fall back to original error */
				if (actualRegion.empty())
					throw;

				/* cache the result (could replace existing one if region changed) */
				PutCachedRegion(bucketUrl, actualRegion, opener);

				/* open with the actual region (and hope for the best) */
				return ListWithRegion(urlPattern, actualRegion, isGlob, opener);
			}

			/*
			 * 500 indicates an internal error and we should retry.
			 *
			 * Another error that S3 could return is 503, but that means "Slow down"
			 * so currently we prefer to error out.
			 */
			else if (error.Message().find("HTTP 500") != std::string::npos)
			{
				tries += 1;

				if (tries <= HTTPParams::DEFAULT_RETRIES)
				{
					/* use similar logic to RunRequestWithRetry */
					if (tries > 1)
					{
						uint64_t sleep_amount =
							(uint64_t)((float)HTTPParams::DEFAULT_RETRY_WAIT_MS * pow(HTTPParams::DEFAULT_RETRY_BACKOFF, tries - 2));

						std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
					}
					continue;
				}
			}

			throw;
		}
	}
}


/*
 * ListWithRegion performs a list operation with a custom region.
 */
vector<OpenFileInfo>
RegionAwareS3FileSystem::ListWithRegion(const string &urlPattern, const string &region,
										bool isGlob, FileOpener *opener)
{
	/* add an ?s3_region=... argument to the URL pattern */
	string urlPatternWithRegion =
		AddQueryArgumentToUrl(urlPattern, "s3_region", region);

	vector<OpenFileInfo> items = s3fs.List(urlPatternWithRegion, isGlob, opener);

	/*
	 * Remove the s3_region argument we injected from the list. Any requests
	 * that use the returned URLs will have it auto-injected from cache.
	 */
	for (OpenFileInfo &fileInfo: items)
		fileInfo.path = RemoveQueryArgumentFromUrl(fileInfo.path, "s3_region");

	return items;
}


unique_ptr<FileHandle>
RegionAwareS3FileSystem::OpenFile(const string &url,
							  FileOpenFlags openFlags,
							  optional_ptr<FileOpener> opener)
{
	if (!opener || !opener->TryGetClientContext())
		/* this probably cannot happen, but let's be defensive and let regular S3FS handle it */
		return s3fs.OpenFile(url, openFlags, opener);

	/* as far as we know, looking up bucket region only works for S3 */
	if (!StringUtil::StartsWith(url, "s3://"))
		return s3fs.OpenFile(url, openFlags, opener);

	/* user explicitly asked for a region, don't try to be smart */
	if (UrlHasQueryArgument(url, "s3_region"))
		return s3fs.OpenFile(url, openFlags, opener);

	/* extract the bucket URL */
	string bucketUrl = GetBucketUrl(url, opener);

	/* S3 express buckets encode the region and endpoint in the name */
	if (StringUtil::EndsWith(bucketUrl, "--x-s3") &&
		!UrlHasQueryArgument(url, "s3_endpoint"))
	{
		string expressUrl;
		if (AddS3ExpressRegionEndpoint(url, expressUrl))
			return s3fs.OpenFile(expressUrl, openFlags, opener);
	}

	/* if we previously looked up the region, use it */
	string cachedRegion = GetCachedRegion(bucketUrl, opener);

	try
	{
		if (!cachedRegion.empty())
			/* use the cached region, we may still fail and refresh the region */
			return s3fs.OpenFile(AddQueryArgumentToUrl(url, "s3_region", cachedRegion), openFlags, opener);
		else
			/* optimistically list the files, even if we don't know region */
			return s3fs.OpenFile(url, openFlags, opener);
	}
	catch (Exception &ex)
	{
		/* wuh oh, something went wrong, could it be the region? */
		ErrorData error(ex);

		if (error.Type() != ExceptionType::HTTP)
			throw;

		/*
		 * A 400 error usually indicates wrong region, we get slightly different
		 * errors for GET and PUT.
		 */
		if (error.Message().find("HTTP 400") == std::string::npos &&
			error.Message().find("400 (Bad Request)") == std::string::npos &&
			error.Message().find("301") == std::string::npos)
			throw;

		PGDUCK_SERVER_DEBUG("OpenFile failed: %s", error.Message().c_str());

		/* get the actual region from S3 headers */
		string actualRegion = GetBucketRegionFromS3(url, opener);

		/* could not determine region, fall back to original error */
		if (actualRegion.empty())
			throw;

		/* cache the result (could replace existing one if region changed) */
		PutCachedRegion(bucketUrl, actualRegion, opener);

		/* open with the actual region (and hope for the best) */
		return s3fs.OpenFile(AddQueryArgumentToUrl(url, "s3_region", actualRegion), openFlags, opener);
	}
}


/*
 * UrlHasQueryArgument determines whether a URL contains a
 * query argument.
 */
static bool
UrlHasQueryArgument(const string &url, const string &queryArgument)
{
	std::regex pattern("[?&]" + queryArgument + "=");

	return std::regex_search(url.c_str(), pattern);
}


/*
 * GetBucketUrl returns the bucket URL for an s3:// URL.
 */
string
RegionAwareS3FileSystem::GetBucketUrl(const string &url, optional_ptr<FileOpener> opener)
{
	/* get the S3 configuration (not very useful, but the S3UrlParse API requires it) */
	FileOpenerInfo s3UrlInfo = {url};
	S3AuthParams authParams = S3AuthParams::ReadFrom(opener, s3UrlInfo);

	/* parse the S3 URL */
	ParsedS3Url parsedUrl = s3fs.S3UrlParse(url, authParams);

	/* get the s3://<bucket name> */
	return parsedUrl.prefix + parsedUrl.bucket;
}


/*
 * AddS3ExpressRegionEndpoint returns whether the url is an S3 express URL and
 * if so, adds the appropriate region and endpoint.
 */
bool
AddS3ExpressRegionEndpoint(const string &url, string &expressUrl)
{
	std::smatch match;

	if (!std::regex_match(url, match, S3_EXPRESS_URL_PATTERN))
		/* not quite an S3 express URL */
		return false;

	string shortRegion = match[1];
	string az = match[2];

	if (!S3ExpressRegionShorthand.count(shortRegion))
		/* not a recognized region */
		return false;

	string region = S3ExpressRegionShorthand[shortRegion];
	string endpoint = "s3express-" + shortRegion + "-" + az + "." + region + ".amazonaws.com";

	expressUrl = AddQueryArgumentToUrl(url, "s3_region", region);
	expressUrl = AddQueryArgumentToUrl(expressUrl, "s3_endpoint", endpoint);

	return true;
}


/*
 * IsS3ExpressUrlWithRegion returns whether the given URL is an S3 express
 * URL and sets the long region name.
 */
static bool
IsS3ExpressUrlWithRegion(const string &url, string &region)
{
	std::smatch match;

	if (!std::regex_match(url, match, S3_EXPRESS_URL_PATTERN))
		/* not quite an S3 express URL */
		return false;

	string shortRegion = match[1];

	if (!S3ExpressRegionShorthand.count(shortRegion))
		/* not a recognized region */
		return false;

	region = S3ExpressRegionShorthand[shortRegion];

	return true;
}


/*
 * AddQueryArgumentToUrl adds the a query argument to a given URL
 * and returns the new URL.
 */
static string
AddQueryArgumentToUrl(const string &url, const string &name, const string &value)
{
	string regionUrl = url;

	/*
	 * Append ? if there are no query arguments.
	 * Append & if there is already a query argument.
	 * Do nothing if the URL ends in ? but has no arguments.
	 */
	int questionMarkPos = regionUrl.find("?");
	if (questionMarkPos == std::string::npos)
		regionUrl.append("?");
	if (questionMarkPos < regionUrl.size() - 1)
		regionUrl.append("&");

	regionUrl.append(name);
	regionUrl.append("=");
	regionUrl.append(S3FileSystem::UrlEncode(value));

	return regionUrl;
}


/*
 * RemoveQueryArgumentFromUrl removes a query argument from the
 * given URL.
 */
static string
RemoveQueryArgumentFromUrl(string url, const string &name)
{
	size_t argPosition = url.find("?" + name + "=");

	if (argPosition == string::npos)
		argPosition = url.find("&" + name + "=");

	if (argPosition == string::npos)
		return url;

	size_t argEndPosition = url.find("&", argPosition + 1);
	size_t argLength = string::npos;

	if (argEndPosition != string::npos)
		argLength = argEndPosition - argPosition;

	return url.erase(argPosition, argLength);
}


/*
 * GetBucketRegion returns the region for a URL, using the cache when possible.
 */
string
RegionAwareS3FileSystem::GetBucketRegion(const string &url, optional_ptr<FileOpener> opener)
{
	/* only know how to get the region for S3 */
	if (!StringUtil::StartsWith(url, "s3://"))
		return "";

	/* extract the bucket URL */
	string bucketUrl = GetBucketUrl(url, opener);
	string s3ExpressRegion;

	/* S3 express buckets encode the region in the name */
	if (IsS3ExpressUrlWithRegion(url, s3ExpressRegion))
		return s3ExpressRegion;

	/* if we previously looked up the region, use it */
	string cachedRegion = GetCachedRegion(bucketUrl, opener);

	if (!cachedRegion.empty())
		return cachedRegion;

	/* get the actual region from S3 headers */
	string actualRegion = GetBucketRegionFromS3(url, opener);

	/*
	 * If we are talking to an endpoint that does not return a region header
	 * than we'll return an empty string and don't cache in that case.
	 */
	if (!actualRegion.empty())
	{
		PutCachedRegion(bucketUrl, actualRegion, opener);
	}

	return actualRegion;
}


/*
 * GetCachedRegion looks up a cached region for a given bucket URL or an
 * empty string if the region cannot be found.
 */
string
RegionAwareS3FileSystem::GetCachedRegion(const string &bucketUrl, optional_ptr<FileOpener> opener)
{
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	ObjectCache &cache = ObjectCache::GetObjectCache(*context);

	string regionName;

	shared_ptr<BucketRegion> bucketRegion =
		cache.Get<BucketRegion>("pg_lake_region::" + bucketUrl);

	if (bucketRegion)
		regionName = bucketRegion->regionName;

	return regionName;
}


/*
 * PutCachedRegion sets the cached region for a given bucket URL.
 */
void
RegionAwareS3FileSystem::PutCachedRegion(const string &bucketUrl, const string &regionName, optional_ptr<FileOpener> opener)
{
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	ObjectCache &cache = ObjectCache::GetObjectCache(*context);

	/* cache the result (could replace existing one if region changed) */
	shared_ptr<BucketRegion> bucketRegion =
		cache.GetOrCreate<BucketRegion>("pg_lake_region::" + bucketUrl);
	bucketRegion->bucketUrl = bucketUrl;
	bucketRegion->regionName = regionName;

	PGDUCK_SERVER_LOG("requests for %s will use region %s",
					  bucketUrl.c_str(), regionName.c_str());
}


/*
 * GetBucketRegionFromS3 returns the region for a given S3 URL by reading it via
 * the public HTTPS endpoint.
 */
string
RegionAwareS3FileSystem::GetBucketRegionFromS3(const string &url, optional_ptr<FileOpener> opener)
{
	shared_ptr<HTTPUtil> httpUtil = HTTPFSUtil::GetHTTPUtil(opener);

	/* Get the configuration */
	FileOpenerInfo s3UrlInfo = {url};
	S3AuthParams authParams = S3AuthParams::ReadFrom(opener, s3UrlInfo);

	/* Scan the query string for any s3 authentication parameters */
	ParsedS3Url parsedUrl = s3fs.S3UrlParse(url, authParams);

	/* Look for query parameters that override the defaults (mainly s3_endpoint) */
	s3fs.ReadQueryParams(parsedUrl.query_param, authParams);

	/* Construct the https:// URL from the S3 URL */
	string httpUrl = parsedUrl.GetHTTPUrl(authParams, string(""));

	/* parse the https:// URL and obtain protocol/host/port (the base URL) */
	string path, baseUrl;
	HTTPUtil::DecomposeURL(httpUrl, path, baseUrl);

	/* Get a client for the base URL */
	FileOpenerInfo httpUrlInfo = {httpUrl};
	unique_ptr<HTTPParams> httpParams = httpUtil->InitializeParameters(opener, httpUrlInfo);
	unique_ptr<HTTPClient> client = httpUtil->InitializeClient(*httpParams, baseUrl);

	HTTPHeaders requestHeaders;
	HeadRequestInfo headRequest(baseUrl + "/", requestHeaders, *httpParams);
	unique_ptr<HTTPResponse> response = httpUtil->Request(headRequest, client);

	if (!response->headers.HasHeader("x-amz-bucket-region"))
		return string();

	return response->headers.GetHeaderValue("x-amz-bucket-region");
}

} // namespace duckdb
