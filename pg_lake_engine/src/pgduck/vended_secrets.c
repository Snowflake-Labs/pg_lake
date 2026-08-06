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
#include "miscadmin.h"
#include "utils/builtins.h"

#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/vended_secrets.h"


/*
 * GenerateVendedSecretName produces a deterministic name for a vended
 * secret: pglake_vended_<dbOid>_<serverOid>_<hash(secretKey)>.
 *
 * secretKey is a stable, principal-scoped identity (e.g.
 * "<userMappingOid>/catalog/ns/table"), so the name stays constant as
 * the underlying credentials and their S3 scope rotate, keeping CREATE
 * OR REPLACE SECRET idempotent.  The database OID is included because a
 * single pgduck_server is shared by all databases in the cluster, and a
 * 64-bit hash is used so distinct identities do not collide on the
 * shared secret namespace.
 */
char *
GenerateVendedSecretName(Oid serverOid, const char *secretKey)
{
	uint64		keyHash = hash_bytes_extended((const unsigned char *) secretKey,
											  strlen(secretKey), 0);

	return psprintf("pglake_vended_%u_%u_%016llx",
					MyDatabaseId, serverOid, (unsigned long long) keyHash);
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
 * AppendS3ConnectionSetting adds ", <keyword> '<value>'" to sql when
 * value is non-empty, escaping the value for safe interpolation.
 */
static void
AppendS3ConnectionSetting(StringInfo sql, const char *keyword,
						  const char *value)
{
	if (value == NULL || value[0] == '\0')
		return;

	char	   *escaped = EscapeSingleQuotes(value);

	appendStringInfo(sql, ", %s '%s'", keyword, escaped);
	pfree(escaped);
}


/*
 * BuildCreateSecretSQL constructs the DuckDB SQL statement for
 * creating or replacing a vended S3 secret.
 *
 * All values are treated as untrusted input and single-quote-escaped to
 * prevent SQL injection.  endpoint / urlStyle / useSsl are the already-
 * resolved connection settings (catalog-provided value, or the inherited
 * fallback); each is emitted only when present.
 */
static char *
BuildCreateSecretSQL(const char *secretName,
					 const VendedS3Secret * secret,
					 const char *endpoint,
					 const char *urlStyle,
					 const char *useSsl)
{
	StringInfoData sql;

	initStringInfo(&sql);

	char	   *escapedKeyId = EscapeSingleQuotes(secret->accessKeyId);
	char	   *escapedSecret = EscapeSingleQuotes(secret->secretAccessKey);

	appendStringInfo(&sql,
					 "CREATE OR REPLACE SECRET \"%s\" ("
					 "TYPE S3, "
					 "KEY_ID '%s', "
					 "SECRET '%s'",
					 secretName, escapedKeyId, escapedSecret);

	AppendS3ConnectionSetting(&sql, "SESSION_TOKEN", secret->sessionToken);
	AppendS3ConnectionSetting(&sql, "REGION", secret->region);
	AppendS3ConnectionSetting(&sql, "ENDPOINT", endpoint);
	AppendS3ConnectionSetting(&sql, "URL_STYLE", urlStyle);

	if (useSsl != NULL && useSsl[0] != '\0')
	{
		/*
		 * A vended value ("true"/"false") or an inherited one from
		 * duckdb_secrets() (which may render "1"/"0" depending on version);
		 * parse_bool handles both so we don't silently coerce SSL off (which
		 * would break real-AWS access).
		 */
		bool		ssl = true;

		(void) parse_bool(useSsl, &ssl);
		appendStringInfo(&sql, ", USE_SSL %s", ssl ? "true" : "false");
	}

	AppendS3ConnectionSetting(&sql, "SCOPE", secret->scope);

	appendStringInfoChar(&sql, ')');

	pfree(escapedKeyId);
	pfree(escapedSecret);

	return sql.data;
}


/*
 * PushVendedSecretOnConnection resolves the S3 connection settings
 * (preferring the catalog-provided values on the secret, falling back to
 * the pre-existing bucket secret for any the catalog omitted) and then
 * creates the vended secret on conn.
 *
 * The inheritance query only runs when at least one connection setting
 * is missing, so a catalog that vends a full set of settings costs no
 * extra round-trip.
 */
static void
PushVendedSecretOnConnection(PGDuckConnection * conn,
							 const char *secretName,
							 const VendedS3Secret * secret)
{
	const char *endpoint = secret->endpoint;
	const char *urlStyle = secret->urlStyle;
	const char *useSsl = secret->useSsl;

	if (endpoint == NULL || urlStyle == NULL || useSsl == NULL)
	{
		S3InheritedSettings inherited =
			LookupInheritedS3Settings(conn, secret->scope);

		if (endpoint == NULL)
			endpoint = inherited.endpoint;
		if (urlStyle == NULL)
			urlStyle = inherited.urlStyle;
		if (useSsl == NULL)
			useSsl = inherited.useSsl;
	}

	char	   *sql = BuildCreateSecretSQL(secretName, secret,
										   endpoint, urlStyle, useSsl);

	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, sql);

	CheckPGDuckResult(conn, result);
	PQclear(result);

	pfree(sql);
}


/*
 * The vended secret in pgduck_server is a temporary (in-memory) secret:
 * process-wide, shared across connections, and lost only when
 * pgduck_server restarts.  The storage-credential resolver tracks which
 * secrets this backend has pushed and, using the catalog-provided
 * expiry, re-pushes with CREATE OR REPLACE only when a secret is new or
 * nearing expiry -- so a warm scan needs no secret round-trip at all,
 * while rotating STS credentials are still refreshed before they lapse.
 * (Trade-off: if pgduck_server restarts, a backend that believes its
 * secret is still fresh will not re-push until the next reconcile that
 * has other work to do; the resolver batches CREATE and DROP for a
 * relation onto a single connection to keep that cost minimal.)
 */
void
PushVendedSecretToPGDuck(const VendedS3Secret * secret)
{
	PGDuckConnection *conn = GetPGDuckConnection();

	PG_TRY();
	{
		PushVendedSecretToPGDuckOnConnection(conn, secret);
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(conn);
	}
	PG_END_TRY();
}


void
PushVendedSecretToPGDuckOnConnection(PGDuckConnection * conn,
									 const VendedS3Secret * secret)
{
	char	   *secretName = GenerateVendedSecretName(secret->serverOid,
													  secret->secretKey);

	elog(DEBUG2, "pushing vended secret \"%s\" to pgduck_server", secretName);

	PushVendedSecretOnConnection(conn, secretName, secret);

	pfree(secretName);
}


void
DropVendedSecretFromPGDuck(Oid serverOid, const char *secretKey)
{
	PGDuckConnection *conn = GetPGDuckConnection();

	PG_TRY();
	{
		DropVendedSecretFromPGDuckOnConnection(conn, serverOid, secretKey);
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(conn);
	}
	PG_END_TRY();
}


void
DropVendedSecretFromPGDuckOnConnection(PGDuckConnection * conn,
									   Oid serverOid, const char *secretKey)
{
	char	   *secretName = GenerateVendedSecretName(serverOid, secretKey);
	char	   *sql = psprintf("DROP SECRET IF EXISTS \"%s\"", secretName);

	elog(DEBUG2, "dropping vended secret \"%s\" from pgduck_server",
		 secretName);

	/* Best-effort: a failed drop must not abort the caller. */
	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, sql);

	PQclear(result);

	pfree(sql);
	pfree(secretName);
}
