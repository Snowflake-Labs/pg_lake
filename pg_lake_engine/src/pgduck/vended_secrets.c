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
 * vended_secrets.c
 *
 * Manages DuckDB scoped secrets for vended S3 credentials from
 * Iceberg REST catalogs.  Secrets are pushed to pgduck_server via
 * CREATE OR REPLACE SECRET with a URL-scoped SCOPE, so DuckDB's
 * secret manager automatically selects the most specific match.
 */

#include "postgres.h"

#include "common/hashfn.h"
#include "lib/stringinfo.h"

#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/vended_secrets.h"


/*
 * GenerateVendedSecretName produces a deterministic name for a vended
 * secret: pglake_vended_<serverOid>_<hash(prefix)>.
 *
 * The name is stable across calls for the same (serverOid, prefix),
 * so CREATE OR REPLACE SECRET is idempotent.
 */
char *
GenerateVendedSecretName(Oid serverOid, const char *s3Prefix)
{
	uint32		prefixHash = hash_bytes((const unsigned char *) s3Prefix,
										strlen(s3Prefix));

	return psprintf("pglake_vended_%u_%08x", serverOid, prefixHash);
}


/*
 * EscapeSingleQuotes doubles any embedded single quotes in the input
 * string for safe interpolation into DuckDB SQL literals.
 */
static char *
EscapeSingleQuotes(const char *input)
{
	if (input == NULL)
		return NULL;

	if (strchr(input, '\'') == NULL)
		return pstrdup(input);

	StringInfoData escaped;

	initStringInfo(&escaped);

	for (const char *p = input; *p != '\0'; p++)
	{
		if (*p == '\'')
			appendStringInfoChar(&escaped, '\'');
		appendStringInfoChar(&escaped, *p);
	}

	return escaped.data;
}


/*
 * Inherited S3 settings from the pre-existing secret that covers the
 * same bucket.  When a vended secret overrides the existing one (by
 * having a more specific SCOPE), it must carry over the ENDPOINT,
 * URL_STYLE, and USE_SSL settings; otherwise DuckDB would fall back
 * to real-AWS defaults, bypassing any local S3 mock (e.g. Moto).
 */
typedef struct S3InheritedSettings
{
	char	   *endpoint;
	char	   *urlStyle;
	char	   *useSsl;
}			S3InheritedSettings;


/*
 * ExtractFieldFromSecretString extracts a value from the semicolon-
 * separated key=value representation that DuckDB's duckdb_secrets()
 * returns in its secret_string column.
 *
 * Returns a palloc'd copy of the value, or NULL when the key is
 * absent or has an empty value.
 */
static char *
ExtractFieldFromSecretString(const char *secretString, const char *key)
{
	if (secretString == NULL)
		return NULL;

	size_t		keyLen = strlen(key);
	const char *p = secretString;

	while (*p != '\0')
	{
		if (strncmp(p, key, keyLen) == 0 && p[keyLen] == '=')
		{
			const char *valStart = p + keyLen + 1;
			const char *valEnd = strchr(valStart, ';');
			int			len = valEnd ? (valEnd - valStart) : (int) strlen(valStart);

			if (len == 0)
				return NULL;

			return pnstrdup(valStart, len);
		}

		const char *next = strchr(p, ';');

		if (next == NULL)
			break;

		p = next + 1;
	}

	return NULL;
}


/*
 * LookupInheritedS3Settings queries pgduck_server for the existing
 * (non-vended) S3 secret whose scope best matches s3Prefix and
 * returns its ENDPOINT, URL_STYLE, and USE_SSL settings.
 *
 * This ensures vended secrets inherit environment-specific connection
 * parameters (e.g. a Moto endpoint in tests) from the underlying
 * secret they override.
 */
static S3InheritedSettings
LookupInheritedS3Settings(PGDuckConnection * conn, const char *s3Prefix)
{
	S3InheritedSettings settings = {0};
	const char *query =
		"SELECT secret_string FROM duckdb_secrets() "
		"WHERE type = 's3' AND name NOT LIKE 'pglake_vended_%'";

	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, query);

	PG_TRY();
	{
		ThrowIfPGDuckResultHasError(conn, result);

		int			bestLen = -1;
		char	   *bestString = NULL;

		for (int i = 0; i < PQntuples(result); i++)
		{
			char	   *ss = PQgetvalue(result, i, 0);
			char	   *scope = ExtractFieldFromSecretString(ss, "scope");

			if (scope != NULL)
			{
				/*
				 * The scope field may contain comma-separated prefixes; check
				 * each one and keep the longest match.
				 */
				char	   *tok = strtok(pstrdup(scope), ",");

				while (tok != NULL)
				{
					int			tokLen = strlen(tok);

					if (strncmp(s3Prefix, tok, tokLen) == 0 &&
						tokLen > bestLen)
					{
						bestLen = tokLen;
						bestString = pstrdup(ss);
					}

					tok = strtok(NULL, ",");
				}
			}
			else if (bestLen < 0)
			{
				bestLen = 0;
				bestString = pstrdup(ss);
			}
		}

		if (bestString != NULL)
		{
			settings.endpoint = ExtractFieldFromSecretString(bestString, "endpoint");
			settings.urlStyle = ExtractFieldFromSecretString(bestString, "url_style");
			settings.useSsl = ExtractFieldFromSecretString(bestString, "use_ssl");
		}
	}
	PG_FINALLY();
	{
		PQclear(result);
	}
	PG_END_TRY();

	return settings;
}


/*
 * BuildCreateSecretSQL constructs the DuckDB SQL statement for
 * creating or replacing a vended S3 secret.
 *
 * All credential values are treated as untrusted input and properly
 * single-quote-escaped to prevent SQL injection.
 *
 * If inherited settings are provided, ENDPOINT / URL_STYLE / USE_SSL
 * are included so the vended secret connects to the same S3 endpoint
 * as the existing matching secret.
 */
static char *
BuildCreateSecretSQL(const char *secretName,
					 const char *s3Prefix,
					 const char *accessKeyId,
					 const char *secretAccessKey,
					 const char *sessionToken,
					 const char *region,
					 const S3InheritedSettings * inherited)
{
	StringInfoData sql;

	initStringInfo(&sql);

	char	   *escapedKeyId = EscapeSingleQuotes(accessKeyId);
	char	   *escapedSecret = EscapeSingleQuotes(secretAccessKey);

	appendStringInfo(&sql,
					 "CREATE OR REPLACE SECRET \"%s\" ("
					 "TYPE S3, "
					 "KEY_ID '%s', "
					 "SECRET '%s'",
					 secretName, escapedKeyId, escapedSecret);

	if (sessionToken != NULL && sessionToken[0] != '\0')
	{
		char	   *escapedToken = EscapeSingleQuotes(sessionToken);

		appendStringInfo(&sql, ", SESSION_TOKEN '%s'", escapedToken);
		pfree(escapedToken);
	}

	if (region != NULL && region[0] != '\0')
	{
		char	   *escapedRegion = EscapeSingleQuotes(region);

		appendStringInfo(&sql, ", REGION '%s'", escapedRegion);
		pfree(escapedRegion);
	}

	if (inherited != NULL && inherited->endpoint != NULL)
	{
		char	   *escapedEndpoint = EscapeSingleQuotes(inherited->endpoint);

		appendStringInfo(&sql, ", ENDPOINT '%s'", escapedEndpoint);
		pfree(escapedEndpoint);
	}

	if (inherited != NULL && inherited->urlStyle != NULL)
	{
		char	   *escapedUrlStyle = EscapeSingleQuotes(inherited->urlStyle);

		appendStringInfo(&sql, ", URL_STYLE '%s'", escapedUrlStyle);
		pfree(escapedUrlStyle);
	}

	if (inherited != NULL && inherited->useSsl != NULL)
	{
		appendStringInfo(&sql, ", USE_SSL %s",
						 strcmp(inherited->useSsl, "true") == 0 ? "true" : "false");
	}

	char	   *escapedScope = EscapeSingleQuotes(s3Prefix);

	appendStringInfo(&sql, ", SCOPE '%s'", escapedScope);

	appendStringInfoChar(&sql, ')');

	pfree(escapedKeyId);
	pfree(escapedSecret);
	pfree(escapedScope);

	return sql.data;
}


/*
 * PushVendedSecretOnConnection looks up inherited S3 settings from
 * the best-matching existing secret, then creates the vended secret.
 */
static void
PushVendedSecretOnConnection(PGDuckConnection * conn,
							 const char *secretName,
							 const char *s3Prefix,
							 const char *accessKeyId,
							 const char *secretAccessKey,
							 const char *sessionToken,
							 const char *region)
{
	S3InheritedSettings inherited =
		LookupInheritedS3Settings(conn, s3Prefix);

	char	   *sql = BuildCreateSecretSQL(secretName, s3Prefix,
										   accessKeyId, secretAccessKey,
										   sessionToken, region,
										   &inherited);

	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, sql);

	CheckPGDuckResult(conn, result);
	PQclear(result);

	pfree(sql);
}


void
EnsureVendedSecretInPGDuck(Oid serverOid,
						   const char *s3Prefix,
						   const char *accessKeyId,
						   const char *secretAccessKey,
						   const char *sessionToken,
						   const char *region)
{
	char	   *secretName = GenerateVendedSecretName(serverOid, s3Prefix);

	elog(DEBUG2, "pushing vended secret \"%s\" to pgduck_server", secretName);

	PGDuckConnection *conn = GetPGDuckConnection();

	PG_TRY();
	{
		PushVendedSecretOnConnection(conn, secretName, s3Prefix,
									 accessKeyId, secretAccessKey,
									 sessionToken, region);
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(conn);
	}
	PG_END_TRY();

	pfree(secretName);
}


void
EnsureVendedSecretOnConnection(PGDuckConnection * conn,
							   Oid serverOid,
							   const char *s3Prefix,
							   const char *accessKeyId,
							   const char *secretAccessKey,
							   const char *sessionToken,
							   const char *region)
{
	char	   *secretName = GenerateVendedSecretName(serverOid, s3Prefix);

	elog(DEBUG2, "pushing vended secret \"%s\" on existing connection",
		 secretName);

	PushVendedSecretOnConnection(conn, secretName, s3Prefix,
								 accessKeyId, secretAccessKey,
								 sessionToken, region);

	pfree(secretName);
}


void
DropVendedSecretFromPGDuck(Oid serverOid, const char *s3Prefix)
{
	char	   *secretName = GenerateVendedSecretName(serverOid, s3Prefix);
	char	   *sql = psprintf("DROP SECRET IF EXISTS \"%s\"", secretName);

	elog(DEBUG2, "dropping vended secret \"%s\" from pgduck_server",
		 secretName);

	ExecuteOptionalCommandInPGDuck(sql);

	pfree(sql);
	pfree(secretName);
}
