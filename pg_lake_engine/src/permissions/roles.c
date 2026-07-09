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

/*
 * Functions for handling permissions.
 */
#include "postgres.h"
#include "miscadmin.h"

#include <string.h>

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/permissions/roles.h"
#include "utils/acl.h"

#define lake_read_ROLE_NAME "lake_read"
#define lake_write_ROLE_NAME "lake_write"


static void CheckUserSuppliedURL(const char *url);
static bool IsAllowedQueryParamKey(const char *param, size_t paramLen);


/*
 * PgLakeReadRoleId returns the OID of the global read role,
 * if it exists.
 */
Oid
PgLakeReadRoleId(void)
{
	bool		missingOK = true;

	return get_role_oid(lake_read_ROLE_NAME, missingOK);
}


/*
 * PgLakeWriteRoleId returns the OID of the global write role,
 * if it exists.
 */
Oid
PgLakeWriteRoleId(void)
{
	bool		missingOK = true;

	return get_role_oid(lake_write_ROLE_NAME, missingOK);
}


/*
 * CheckURLReadAccess gates a read on a user-supplied URL.  When url is
 * non-NULL it also enforces the supported-scheme allowlist and rejects any
 * cloud-storage URL whose query string contains a parameter outside the
 * narrow allowlist of known-safe DuckDB params, closing the s3_endpoint=
 * SSRF vector.  http(s):// URLs are exempt from the query-parameter check.
 * Pass NULL when the caller only needs the role check (e.g. the top-level
 * FDW option validator that has not yet inspected individual options).
 */
void
CheckURLReadAccess(const char *url)
{
	Oid			userId = GetUserId();
	Oid			readRoleId = PgLakeReadRoleId();

	/*
	 * If the role does not exist, only superuser can read from URLs.
	 * Otherwise, the user needs to have the read access role.
	 */
	if (!superuser() &&
		(readRoleId == InvalidOid || !has_privs_of_role(userId, readRoleId)))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to read from URL"),
						errhint("grant %s to %s to enable reading from URLs",
								GetUserNameFromId(readRoleId, false),
								GetUserNameFromId(userId, false))));
	}

	if (url == NULL)
		return;

	if (!IsSupportedURL(url))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake: unsupported URL: \"%s\"", url)));

	CheckUserSuppliedURL(url);
}


/*
 * CheckURLWriteAccess gates a write on a user-supplied URL.  When url is
 * non-NULL it also enforces the supported-scheme allowlist, unconditionally
 * rejects http(s):// targets (no legitimate cloud-storage write goes there;
 * the only realistic use is exfil to internal services), and applies the
 * same query-parameter allowlist as the read path so a write target like
 * 's3://bucket/key?s3_endpoint=...' cannot redirect the upload to an
 * attacker-controlled host.  Pass NULL for role-only checks.
 */
void
CheckURLWriteAccess(const char *url)
{
	Oid			userId = GetUserId();
	Oid			writeRoleId = PgLakeWriteRoleId();

	/*
	 * If the role does not exist, only superuser can write to URLs.
	 * Otherwise, the user needs to have the write access role.
	 */
	if (!superuser() &&
		(writeRoleId == InvalidOid || !has_privs_of_role(userId, writeRoleId)))
	{
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("permission denied to write to URL"),
						errhint("grant %s to %s to enable writing to URLs",
								GetUserNameFromId(writeRoleId, false),
								GetUserNameFromId(userId, false))));
	}

	if (url == NULL)
		return;

	if (!IsSupportedURL(url))
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("pg_lake: unsupported URL: \"%s\"", url)));

	/*
	 * Bare http(s):// URLs have no legitimate write target in pg_lake.  The
	 * only realistic use is exfiltrating data through DuckDB's HTTP client to
	 * an internal service.  Hard reject for everyone, including superusers.
	 */
	if (strncmp(url, HTTP_URL_PREFIX, strlen(HTTP_URL_PREFIX)) == 0 ||
		strncmp(url, HTTPS_URL_PREFIX, strlen(HTTPS_URL_PREFIX)) == 0)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("pg_lake: writes to http:// or https:// URLs are not permitted"),
				 errhint("Use a cloud storage URL (s3://, gs://, az://, ...) instead.")));

	CheckUserSuppliedURL(url);
}


/*
 * CheckUserSuppliedURL rejects any DuckDB query parameter that is not on a
 * narrow allowlist of known-safe params.  The primary vector this closes is
 * s3_endpoint=, which DuckDB's ReadQueryParams() reads from the query string
 * and uses to override the server-configured endpoint:
 *     s3://bucket/key?s3_endpoint=169.254.169.254&s3_use_ssl=false
 * lets any lake_read user redirect requests to internal hosts and read the
 * response back as query rows.
 *
 * DuckDB's S3FileSystem reads many params off the URL (s3_url_style,
 * s3_use_ssl, s3_access_key_id, s3_secret_access_key, s3_session_token,
 * s3_kms_key_id, ...) and the set grows over time, so a denylist is
 * bypass-prone.  Allowlist only the params that cannot redirect the request
 * or weaken transport, and reject everything else (including unknown ones).
 *
 * http(s):// URLs are exempt entirely: '?' is a normal query-string
 * separator there and the s3_endpoint trick does not apply.
 *
 * NOTE: this does not cover Azure (abfss://, az://).  Those schemes encode
 * the storage endpoint in the URL host (<account>.<endpoint>), not the
 * query string, so abfss://c@account.<internal-host>/... bypasses this
 * check.  A separate host check is needed to close that.
 */
static void
CheckUserSuppliedURL(const char *url)
{
	if (url == NULL)
		return;

	if (strncmp(url, HTTP_URL_PREFIX, strlen(HTTP_URL_PREFIX)) == 0 ||
		strncmp(url, HTTPS_URL_PREFIX, strlen(HTTPS_URL_PREFIX)) == 0)
		return;

	const char *queryStart = strchr(url, '?');

	if (queryStart == NULL)
		return;

	for (const char *param = queryStart + 1; param != NULL; /* advance below */ )
	{
		const char *amp = strchr(param, '&');
		size_t		paramLen = amp ? (size_t) (amp - param) : strlen(param);

		/* skip empty params (e.g. trailing '&') */
		if (paramLen == 0)
		{
			param = amp ? amp + 1 : NULL;
			continue;
		}

		if (!IsAllowedQueryParamKey(param, paramLen))
		{
			const char *eq = memchr(param, '=', paramLen);
			int			keyLen = eq ? (int) (eq - param) : (int) paramLen;

			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("pg_lake: query parameter \"%.*s\" is not permitted in cloud storage URLs",
							keyLen, param),
					 errdetail("URL contains a disallowed parameter: \"%s\".", url),
					 errhint("Only s3_region and s3_requester_pays are allowed in the URL query string.")));
		}

		param = amp ? amp + 1 : NULL;
	}
}


/*
 * Returns true if param starts with one of the allowlisted "key=" prefixes.
 * paramLen bounds the key=value substring; we never read past it.
 */
static bool
IsAllowedQueryParamKey(const char *param, size_t paramLen)
{
	/*
	 * Allowlist of safe DuckDB query parameter keys (with trailing '=').
	 * Anything else is rejected.
	 */
	static const char *const allowlist[] = {
		"s3_region=",
		"s3_requester_pays=",
		NULL,
	};

	for (const char *const *allow = allowlist; *allow; allow++)
	{
		size_t		allowLen = strlen(*allow);

		if (paramLen >= allowLen && strncmp(param, *allow, allowLen) == 0)
			return true;
	}

	return false;
}
