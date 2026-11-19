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


#include "crypto.hpp"
#include "duckdb.hpp"
#include "duckdb/common/crypto/md5.hpp"
#include "duckdb/common/exception/http_exception.hpp"
#include "duckdb/common/types/blob.hpp"

#define CPPHTTPLIB_OPENSSL_SUPPORT
#include "http/httpfs.hpp"
#include "s3/s3fs.hpp"
#include "httplib.hpp"
#include "s3/s3_url.hpp"

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

static bool ParseS3ExpressUrl(const string &url, string &shortRegion, string &az);
static bool UrlHasQueryArgument(const string &url, const string &queryArgument);
static string AddQueryArgumentToUrl(const string &url, const string &name, const string &value);
static string RemoveQueryArgumentFromUrl(string url, const string &name);
static bool LooksLikeRegionMismatch(const ErrorData &error);


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

			if (LooksLikeRegionMismatch(error) ||
				error.Message().find("no list response") != std::string::npos)
			{
				PGDUCK_SERVER_DEBUG("Glob failed: %s", error.Message().c_str());

				/* get the actual region from S3 headers */
				string actualRegion = TryGetBucketRegionFromS3(urlPattern, opener);

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
	unique_ptr<FileHandle> handle;
	WithResolvedRegion(url, opener,
		[&](const string &resolvedUrl) { handle = s3fs.OpenFile(resolvedUrl, openFlags, opener); });
	return handle;
}


/*
 * WithResolvedRegion runs s3Operation against an S3 URL, picking the region
 * from cache when available and falling back to TryGetBucketRegionFromS3 on a
 * 400/301.
 *
 * Same shape as the old OpenFile: try once (with cached region or bare URL),
 * and if the error looks like a region mismatch, look up the actual region
 * and retry. All region-aware entry points (OpenFile, FileExists,
 * DirectoryExists, RemoveFile, RemoveFiles) go through here so the retry +
 * cache logic lives in one place.
 */
void
RegionAwareS3FileSystem::WithResolvedRegion(const string &url,
											optional_ptr<FileOpener> opener,
											std::function<void(const string &)> s3Operation)
{
	if (!opener || !opener->TryGetClientContext())
	{
		/* no client context, no cache; just pass through */
		s3Operation(url);
		return;
	}

	/* as far as we know, looking up bucket region only works for S3 */
	if (!StringUtil::StartsWith(url, "s3://"))
	{
		s3Operation(url);
		return;
	}

	/* user explicitly asked for a region, don't try to be smart */
	if (UrlHasQueryArgument(url, "s3_region"))
	{
		s3Operation(url);
		return;
	}

	/* extract the bucket URL */
	string bucketUrl = GetBucketUrl(url, opener);

	/* S3 express buckets encode the region and endpoint in the name */
	if (StringUtil::EndsWith(bucketUrl, "--x-s3") &&
		!UrlHasQueryArgument(url, "s3_endpoint"))
	{
		string expressUrl;
		if (AddS3ExpressRegionEndpoint(url, expressUrl))
		{
			s3Operation(expressUrl);
			return;
		}
	}

	/* if we previously looked up the region, use it */
	string cachedRegion = GetCachedRegion(bucketUrl, opener);

	try
	{
		if (!cachedRegion.empty())
			/* use the cached region, we may still fail and refresh the region */
			s3Operation(AddQueryArgumentToUrl(url, "s3_region", cachedRegion));
		else
			/* optimistically run the call, even if we don't know the region */
			s3Operation(url);
		return;
	}
	catch (Exception &ex)
	{
		/* wuh oh, something went wrong, could it be the region? */
		ErrorData error(ex);

		if (error.Type() != ExceptionType::HTTP)
			throw;

		if (!LooksLikeRegionMismatch(error))
			throw;

		PGDUCK_SERVER_DEBUG("S3 operation failed (retrying with refreshed region): %s", error.Message().c_str());

		/* get the actual region from S3 headers */
		string actualRegion = TryGetBucketRegionFromS3(url, opener);

		/* could not determine region, fall back to original error */
		if (actualRegion.empty())
			throw;

		/* cache the result (could replace existing one if region changed) */
		PutCachedRegion(bucketUrl, actualRegion, opener);

		/* retry with the actual region (and hope for the best) */
		s3Operation(AddQueryArgumentToUrl(url, "s3_region", actualRegion));
	}
}


/*
 * LooksLikeRegionMismatch determines whether an HTTP error is one that S3
 * returns when the request went to the wrong region: 400 for the legacy
 * endpoint, 301 for a redirect to the right one.
 *
 * httpfs attaches the status code to the error, so use that when it is there.
 * Some error paths only give us a message, in which case we have to look for
 * the status in the text. That text also contains the URL we tried to read, so
 * the patterns have to be specific enough not to match an object key: a bare
 * "301" matches roughly 1 in 100 of the uuid-named data files that Iceberg
 * writes, and misreading an "access denied" as a region mismatch replaces it
 * with whatever the region probe happens to return.
 */
static bool
LooksLikeRegionMismatch(const ErrorData &error)
{
	const unordered_map<string, string> &extraInfo = error.ExtraInfo();
	auto statusCode = extraInfo.find("status_code");

	if (statusCode != extraInfo.end())
		return statusCode->second == "400" || statusCode->second == "301";

	const string &message = error.Message();

	return message.find("HTTP 400") != std::string::npos ||
		   message.find("400 (Bad Request)") != std::string::npos ||
		   message.find("HTTP 301") != std::string::npos ||
		   message.find("301 (Moved Permanently)") != std::string::npos;
}


/*
 * UrlHasQueryArgument determines whether a URL contains a
 * query argument.
 */
static bool
UrlHasQueryArgument(const string &url, const string &queryArgument)
{
	return url.find("?" + queryArgument + "=") != string::npos ||
		url.find("&" + queryArgument + "=") != string::npos;
}


/*
 * ParseS3ExpressUrl returns whether the url looks like
 * s3://<name>-<short region>-<az>--x-s3/<key> and if so, sets the short
 * region and availability zone.
 *
 * This is parsed by hand rather than with std::regex: libstdc++ matches
 * with recursion per character, so a long URL overflows the stack.
 */
static bool
ParseS3ExpressUrl(const string &url, string &shortRegion, string &az)
{
	const string scheme = "s3://";
	const string suffix = "--x-s3";

	if (url.compare(0, scheme.size(), scheme) != 0)
		return false;

	size_t slash = url.find('/', scheme.size());

	if (slash == string::npos)
		return false;

	string bucket = url.substr(scheme.size(), slash - scheme.size());

	if (bucket.size() <= suffix.size() ||
		bucket.compare(bucket.size() - suffix.size(), suffix.size(), suffix) != 0)
		return false;

	string baseName = bucket.substr(0, bucket.size() - suffix.size());

	for (char ch : baseName)
	{
		if (!((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-'))
			return false;
	}

	/* the last two dash-separated parts, after a non-empty name */
	size_t azDash = baseName.rfind('-');

	if (azDash == string::npos || azDash == 0)
		return false;

	size_t regionDash = baseName.rfind('-', azDash - 1);

	if (regionDash == string::npos || regionDash == 0)
		return false;

	shortRegion = baseName.substr(regionDash + 1, azDash - regionDash - 1);
	az = baseName.substr(azDash + 1);

	return !shortRegion.empty() && !az.empty();
}


/*
 * GetBucketUrl returns the bucket URL for an s3:// URL.
 */
string
RegionAwareS3FileSystem::GetBucketUrl(const string &url, optional_ptr<FileOpener> opener)
{
	/* get the S3 configuration (not very useful, but the S3Url::Parse API requires it) */
	FileOpenerInfo s3UrlInfo = {url};
	S3AuthParams authParams = S3AuthResolver::Resolve(opener, s3UrlInfo);

	/* parse the S3 URL */
	ParsedS3Url parsedUrl = S3Url::Parse(url, authParams);

	/* get the s3://<bucket name> */
	return parsedUrl.GetPrefix() + parsedUrl.GetBucket();
}


/*
 * AddS3ExpressRegionEndpoint returns whether the url is an S3 express URL and
 * if so, adds the appropriate region and endpoint.
 */
bool
AddS3ExpressRegionEndpoint(const string &url, string &expressUrl)
{
	string shortRegion;
	string az;

	if (!ParseS3ExpressUrl(url, shortRegion, az))
		/* not quite an S3 express URL */
		return false;

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
	string shortRegion;
	string az;

	if (!ParseS3ExpressUrl(url, shortRegion, az))
		/* not quite an S3 express URL */
		return false;

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
	regionUrl.append(S3Url::Encode(value, S3URLEncodeMode::QUERY_COMPONENT));

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
 * ClearCachedRegion evicts the cached region entry for a given bucket URL,
 * if one exists. Used by pg_lake_clear_region_cache() so operators (and
 * tests) can force the next request to re-discover the bucket region.
 */
void
RegionAwareS3FileSystem::ClearCachedRegion(const string &bucketUrl, optional_ptr<FileOpener> opener)
{
	optional_ptr<ClientContext> context = opener->TryGetClientContext();
	ObjectCache &cache = ObjectCache::GetObjectCache(*context);

	cache.Delete("pg_lake_region::" + bucketUrl);
}


/*
 * GetBucketRegionFromS3 returns the region for a given S3 URL by reading it via
 * the public HTTPS endpoint.
 */
string
RegionAwareS3FileSystem::GetBucketRegionFromS3(const string &url, optional_ptr<FileOpener> opener)
{
	auto &httpUtil = HTTPFSUtil::GetHTTPUtil(opener);

	/* Get the configuration, including query parameters that override the defaults (mainly s3_endpoint) */
	FileOpenerInfo s3UrlInfo = {url};
	S3AuthParams authParams = S3AuthResolver::Resolve(opener, s3UrlInfo);

	/*
	 * Construct the https:// URL of the bucket. The bucket is in the host for
	 * virtual-hosted URLs and in the path otherwise, which httpfs picks for
	 * bucket names that cannot be a host label, such as ones containing dots.
	 */
	ParsedS3Url parsedUrl = S3Url::Parse(url, authParams);
	string httpUrl = parsedUrl.GetBucketHTTPUrl();

	/* parse the https:// URL and obtain protocol/host/port (the base URL) */
	string path, baseUrl;
	HTTPUtil::DecomposeURL(httpUrl, path, baseUrl);

	/* Get a client for the base URL */
	FileOpenerInfo httpUrlInfo = {httpUrl};
	unique_ptr<HTTPParams> httpParams = httpUtil.InitializeParameters(opener, httpUrlInfo);
	unique_ptr<HTTPClient> client = httpUtil.InitializeClient(*httpParams, baseUrl);

	HTTPHeaders requestHeaders;

	/* HeadRequestInfo keeps a reference to the URL, so it has to outlive the request */
	string bucketRootUrl = StringUtil::EndsWith(httpUrl, "/") ? httpUrl : httpUrl + "/";

	HeadRequestInfo headRequest(bucketRootUrl, requestHeaders, *httpParams);

	/*
	 * The request is unauthenticated, so S3 answers 403, or 301 when the
	 * endpoint belongs to another region. httpfs returns those rather than
	 * throwing, and the region header comes with them.
	 */
	unique_ptr<HTTPResponse> response = httpUtil.Request(headRequest, client);

	/* a request that got no response at all reports why on the response */
	if (response->HasRequestError())
		throw IOException("%s", response->GetRequestError());

	if (!response->headers.HasHeader("x-amz-bucket-region"))
		return string();

	string region = response->headers.GetHeaderValue("x-amz-bucket-region");

	/*
	 * The endpoint that answered is whichever one the secret or setting covering
	 * this URL names, and the region it reports is cached per bucket and reused
	 * for later requests. Only accept a plain region name, and treat anything
	 * else like a response with no region header at all, so nothing is cached.
	 */
	if (!PgLakeIsValidS3Region(region))
	{
		PGDUCK_SERVER_WARN("ignoring invalid x-amz-bucket-region \"%s\" returned for %s",
						   region.c_str(), url.c_str());
		return string();
	}

	return region;
}


/*
 * TryGetBucketRegionFromS3 is GetBucketRegionFromS3 for callers that still hold
 * the error that made them ask. The probe can fail for reasons of its own, up to
 * the endpoint not allowing it at all, and an unknown region makes those callers
 * rethrow their own error instead of one about the probe.
 */
string
RegionAwareS3FileSystem::TryGetBucketRegionFromS3(const string &url, optional_ptr<FileOpener> opener)
{
	try
	{
		return GetBucketRegionFromS3(url, opener);
	}
	catch (std::exception &ex)
	{
		PGDUCK_SERVER_DEBUG("could not determine region of %s: %s", url.c_str(), ex.what());
		return string();
	}
}


/*
 * RemoveFiles deletes many files, using batched DeleteObjects requests instead
 * of one request per file.
 *
 * DeleteObjects targets one bucket at a time, and the region is a property of
 * the bucket, so resolve the region once per bucket and pass every path of the
 * bucket with it. S3FileSystem::RemoveFiles splits them further by the
 * credentials each path resolves to, since a secret can be scoped to a prefix
 * rather than to the whole bucket.
 *
 * WithResolvedRegion adds s3_region and s3_endpoint, so we skip it for URLs
 * that set either one explicitly, and for anything that is not S3, and use
 * the simpler single-file path.
 */
void
RegionAwareS3FileSystem::RemoveFiles(const vector<string> &paths,
									 optional_ptr<FileOpener> opener)
{
	/*
	 * A linear scan: a set of paths to delete is one table's files or one
	 * batch off the deletion queue, so the number of distinct buckets in it is
	 * a handful at worst.
	 */
	vector<pair<string, vector<string>>> buckets;

	for (const string &path : paths)
	{
		if (!StringUtil::StartsWith(path, "s3://") ||
			UrlHasQueryArgument(path, "s3_region") ||
			UrlHasQueryArgument(path, "s3_endpoint"))
		{
			RemoveFile(path, opener);
			continue;
		}

		string bucketUrl = GetBucketUrl(path, opener);
		vector<string> *bucketPaths = nullptr;

		for (auto &bucket : buckets)
		{
			if (bucket.first == bucketUrl)
			{
				bucketPaths = &bucket.second;
				break;
			}
		}

		if (bucketPaths == nullptr)
		{
			buckets.emplace_back(bucketUrl, vector<string>());
			bucketPaths = &buckets.back().second;
		}

		bucketPaths->push_back(path);
	}

	/*
	 * Resolve the region from the first path of the bucket, the way every other
	 * entry point does: it gets the cached region, and a mismatch refreshes the
	 * region and retries the bucket. WithResolvedRegion hands us that path with
	 * the query arguments it added, which we copy onto the other paths.
	 */
	for (auto &bucket : buckets)
	{
		const vector<string> &bucketPaths = bucket.second;

		WithResolvedRegion(bucketPaths[0], opener,
							 [&](const string &resolvedFirstPath) {
								 string querySuffix;
								 auto queryPos = resolvedFirstPath.find('?');
								 if (queryPos != string::npos)
									 querySuffix = resolvedFirstPath.substr(queryPos);

								 vector<string> resolvedPaths;
								 resolvedPaths.reserve(bucketPaths.size());
								 for (const string &path : bucketPaths)
									 resolvedPaths.push_back(path + querySuffix);

								 s3fs.RemoveFiles(resolvedPaths, opener);
							 });
	}
}

} // namespace duckdb
