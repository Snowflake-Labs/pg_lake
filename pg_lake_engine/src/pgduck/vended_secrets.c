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
 * Connection settings carried over from the secret that was already
 * serving this prefix.  A vended secret takes over from it by having a
 * more specific SCOPE, so anything it does not state itself -- where
 * the store actually is, above all -- would otherwise revert to
 * real-AWS defaults and leave the deployment.
 */
typedef struct S3InheritedSettings
{
	char	   *endpoint;
	char	   *urlStyle;
	char	   *useSsl;
}			S3InheritedSettings;


/*
 * GenerateVendedSecretName produces a deterministic name for a vended
 * secret: pglake_vended_<dbOid>_<serverOid>_<hash(secretKey)>.
 *
 * secretKey is a stable identity that names both the principal and what
 * the credential is good for (e.g.
 * "<userMappingOid>/catalog/ns/table/<scope>"), so the name stays
 * constant as the underlying credentials rotate, keeping CREATE OR
 * REPLACE SECRET idempotent.  The database OID is included because a
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
 * LookupInheritedS3Settings returns the ENDPOINT, URL_STYLE and USE_SSL
 * of the existing (non-vended) S3 secret that covers s3Prefix.
 *
 * Which secret that is, is decided by DuckDB's own rule -- longest
 * matching scope -- expressed in the query rather than reimplemented
 * here, so what we inherit is what the read would have used had nothing
 * been vended at all.  Vended secrets are excluded, since one of those
 * is likely this very secret from an earlier push.
 *
 * This is what lets a vended secret reach an S3-compatible store whose
 * catalog states only part of the connection: without it DuckDB falls
 * back to real-AWS defaults and the scan leaves the deployment
 * entirely.  What it cannot do is move a credential somewhere the
 * catalog did not point: the caller keeps every value the catalog
 * stated, so a secret in a namespace other tenants can write to can
 * only supply a setting nobody else has.
 */
static S3InheritedSettings
LookupInheritedS3Settings(PGDuckConnection * conn, const char *s3Prefix)
{
	S3InheritedSettings settings = {0};

	/* Without a prefix there is no secret to inherit from. */
	if (s3Prefix == NULL || s3Prefix[0] == '\0')
		return settings;

	char	   *escapedPrefix = EscapeSingleQuotes(s3Prefix);
	char	   *query =
		psprintf("SELECT secret_string FROM ("
				 "SELECT secret_string, unnest(scope) AS prefix "
				 "FROM duckdb_secrets() "
				 "WHERE type = 's3' AND name NOT LIKE 'pglake_vended_%%') s "
				 "WHERE starts_with('%s', prefix) "
				 "ORDER BY length(prefix) DESC LIMIT 1",
				 escapedPrefix);

	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, query);

	PG_TRY();
	{
		ThrowIfPGDuckResultHasError(conn, result);

		if (PQntuples(result) > 0)
		{
			char	   *secretString = PQgetvalue(result, 0, 0);

			settings.endpoint = ExtractFieldFromSecretString(secretString,
															 "endpoint");
			settings.urlStyle = ExtractFieldFromSecretString(secretString,
															 "url_style");
			settings.useSsl = ExtractFieldFromSecretString(secretString,
														   "use_ssl");
		}
	}
	PG_FINALLY();
	{
		PQclear(result);
	}
	PG_END_TRY();

	pfree(query);
	pfree(escapedPrefix);

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
 * PushVendedSecretOnConnection resolves the S3 connection settings and
 * creates the vended secret on conn.
 *
 * The catalog is the authority on where its own storage lives, so its
 * values are taken as given.  Each setting is resolved on its own,
 * though: a catalog that states an endpoint but not an addressing style
 * is the ordinary case for an S3-compatible store, and dropping the
 * URL_STYLE that the existing secret carries leaves DuckDB addressing
 * the bucket as a subdomain of a host that has no such name.
 *
 * Inheriting cannot redirect the credentials: an endpoint the catalog
 * stated is never overwritten, and SSL is already pinned by that
 * endpoint's scheme, so the fallback only fills in what nobody has
 * stated.  The query itself is skipped entirely when there is nothing
 * left to fill in.
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
