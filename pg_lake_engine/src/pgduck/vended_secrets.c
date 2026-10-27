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
 * Manages DuckDB scoped secrets for vended credentials from Iceberg
 * REST catalogs.  Secrets are pushed to pgduck_server via
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

/* The start of a vended secret's name: its database and server OIDs. */
#define VENDED_SECRET_NAME_PREFIX "pglake_vended_%u_%u_"


/*
 * GenerateVendedSecretName produces a deterministic name for a vended
 * secret: pglake_vended_<dbOid>_<serverOid>_<hash(secretId)>.
 *
 * secretId is a stable identity that names both the principal and what
 * the credential is good for (e.g.
 * "<userMappingOid>/catalog/ns/table/<scope>"), so the name stays
 * constant as the underlying credentials rotate, keeping CREATE OR
 * REPLACE SECRET idempotent.  The database OID is included because a
 * single pgduck_server is shared by all databases in the cluster, and a
 * 64-bit hash is used so distinct identities do not collide on the
 * shared secret namespace.
 */
char *
GenerateVendedSecretName(Oid serverOid, const char *secretId)
{
	uint64		keyHash = hash_bytes_extended((const unsigned char *) secretId,
											  strlen(secretId), 0);

	return psprintf(VENDED_SECRET_NAME_PREFIX "%016llx",
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
 * This is what lets a vended secret reach an S3-compatible store: the
 * catalog's endpoint is never used, so without it DuckDB falls back to
 * real-AWS defaults and the scan leaves the deployment entirely.  It is
 * safe to take the endpoint from here because non-vended secrets come
 * only from whoever runs pgduck_server (its startup defaults and init
 * file), not from a catalog or a database role.
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
 * AppendSecretSetting adds ", <keyword> '<value>'" to sql, escaping the
 * value for safe interpolation.
 */
static void
AppendSecretSetting(StringInfo sql, const char *keyword, const char *value)
{
	char	   *escaped = EscapeSingleQuotes(value);

	appendStringInfo(sql, ", %s '%s'", keyword, escaped);
	pfree(escaped);
}


/*
 * BuildCreateS3SecretSQL constructs the DuckDB SQL statement for
 * creating or replacing a vended S3 secret.
 *
 * All values are treated as untrusted input and single-quote-escaped to
 * prevent SQL injection.  endpoint / urlStyle / useSsl are the already-
 * resolved connection settings (catalog-provided value, or the inherited
 * fallback); each is emitted only when present.
 *
 * Everything optional here is NULL when unset, never the empty string:
 * both sources of these values -- the catalog config map and the secret
 * inherited from duckdb_secrets() -- report a stated-but-empty setting
 * as unset.
 */
static char *
BuildCreateS3SecretSQL(const char *secretName,
					   const VendedSecret * secret,
					   const char *endpoint,
					   const char *urlStyle,
					   const char *useSsl)
{
	StringInfoData sql;

	Assert(secret->accessKeyId != NULL && secret->secretAccessKey != NULL);

	initStringInfo(&sql);

	char	   *escapedKeyId = EscapeSingleQuotes(secret->accessKeyId);
	char	   *escapedSecret = EscapeSingleQuotes(secret->secretAccessKey);

	appendStringInfo(&sql,
					 "CREATE OR REPLACE SECRET \"%s\" ("
					 "TYPE S3, "
					 "KEY_ID '%s', "
					 "SECRET '%s'",
					 secretName, escapedKeyId, escapedSecret);

	if (secret->sessionToken != NULL)
		AppendSecretSetting(&sql, "SESSION_TOKEN", secret->sessionToken);

	if (secret->region != NULL)
		AppendSecretSetting(&sql, "REGION", secret->region);

	if (endpoint != NULL)
		AppendSecretSetting(&sql, "ENDPOINT", endpoint);

	if (urlStyle != NULL)
		AppendSecretSetting(&sql, "URL_STYLE", urlStyle);

	if (useSsl != NULL)
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

	if (secret->scope != NULL)
		AppendSecretSetting(&sql, "SCOPE", secret->scope);

	appendStringInfoChar(&sql, ')');

	pfree(escapedKeyId);
	pfree(escapedSecret);

	return sql.data;
}


/*
 * PushNamedS3Secret resolves the S3 connection settings and creates the
 * vended secret under the given name.
 *
 * Where the request goes -- the endpoint, and whether it uses SSL -- is
 * always taken from the secret that already serves the prefix, never
 * from the catalog.  Secrets live in one DuckDB instance shared by every
 * database and role, and a vended secret wins over the static one by
 * having a more specific scope, so an endpoint the catalog chose would
 * move every session's requests under that scope, not just the ones for
 * this catalog's table.  On AWS nothing is lost: the region-aware file
 * system finds the right endpoint from the bucket.  A store elsewhere is
 * reached by a static secret pointing at it, as without vending.
 *
 * The addressing style is the catalog's to state, since it cannot send
 * a request anywhere else, and is inherited when the catalog omits it.
 */
static void
PushNamedS3Secret(PGDuckConnection * conn,
				  const char *secretName,
				  const VendedSecret * secret)
{
	S3InheritedSettings inherited =
		LookupInheritedS3Settings(conn, secret->scope);
	const char *urlStyle = secret->urlStyle != NULL ?
		secret->urlStyle : inherited.urlStyle;

	char	   *sql = BuildCreateS3SecretSQL(secretName, secret,
											 inherited.endpoint, urlStyle,
											 inherited.useSsl);

	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, sql);

	CheckPGDuckResult(conn, result);
	PQclear(result);

	pfree(sql);
}


/*
 * AppendConnectionStringField adds "<field>=<value>" to an Azure
 * connection string, skipping a NULL value.
 *
 * The value comes from the catalog, and the Azure SDK splits a
 * connection string at ';', so a value containing one would add a field
 * of its own -- a BlobEndpoint of its choosing, say, which would send
 * the SAS token to that host.  Refuse it rather than escape it: the SDK
 * has no escape for ';'.  The value itself stays out of the error, since
 * it may be the token.
 */
static void
AppendConnectionStringField(StringInfo connectionString, const char *field,
							const char *value)
{
	if (value == NULL)
		return;

	if (strchr(value, ';') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("vended Azure credential has a %s containing \";\"",
						field)));

	appendStringInfo(connectionString, "%s%s=%s",
					 connectionString->len > 0 ? ";" : "", field, value);
}


/*
 * BuildCreateAzureSecretSQL constructs the DuckDB SQL statement for
 * creating or replacing a vended Azure secret.  DuckDB's Azure secret
 * has no SAS parameter, but passes CONNECTION_STRING to the Azure SDK,
 * which accepts SharedAccessSignature there.  AccountName is required
 * whenever a URL names its account, since DuckDB checks the two agree.
 */
static char *
BuildCreateAzureSecretSQL(const char *secretName, const VendedSecret * secret)
{
	StringInfoData connectionString;
	StringInfoData sql;

	Assert(secret->accountName != NULL && secret->sasToken != NULL);

	initStringInfo(&connectionString);
	AppendConnectionStringField(&connectionString, "AccountName",
								secret->accountName);
	AppendConnectionStringField(&connectionString, "BlobEndpoint",
								secret->blobEndpoint);
	AppendConnectionStringField(&connectionString, "DfsEndpoint",
								secret->dfsEndpoint);
	AppendConnectionStringField(&connectionString, "SharedAccessSignature",
								secret->sasToken);

	initStringInfo(&sql);
	appendStringInfo(&sql, "CREATE OR REPLACE SECRET \"%s\" (TYPE AZURE",
					 secretName);
	AppendSecretSetting(&sql, "CONNECTION_STRING", connectionString.data);

	if (secret->scope != NULL)
		AppendSecretSetting(&sql, "SCOPE", secret->scope);

	appendStringInfoChar(&sql, ')');

	pfree(connectionString.data);

	return sql.data;
}


/*
 * PushNamedAzureSecret creates the vended Azure secret under the given
 * name.  Unlike an S3 secret it inherits nothing from the secret already
 * covering the scope, since DuckDB redacts an Azure secret's connection
 * string.
 */
static void
PushNamedAzureSecret(PGDuckConnection * conn,
					 const char *secretName,
					 const VendedSecret * secret)
{
	char	   *sql = BuildCreateAzureSecretSQL(secretName, secret);
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
PushVendedSecretToPGDuck(PGDuckConnection * conn,
						 const VendedSecret * secret)
{
	char	   *secretName = GenerateVendedSecretName(secret->serverOid,
													  secret->secretId);

	elog(DEBUG2, "pushing vended secret \"%s\" to pgduck_server", secretName);

	if (secret->provider == VENDED_STORAGE_AZURE)
		PushNamedAzureSecret(conn, secretName, secret);
	else
		PushNamedS3Secret(conn, secretName, secret);

	pfree(secretName);
}


/*
 * DropNamedSecret removes one secret from pgduck_server, if it exists.
 */
static void
DropNamedSecret(PGDuckConnection * conn, const char *secretName)
{
	char	   *sql = psprintf("DROP SECRET IF EXISTS %s",
							   quote_identifier(secretName));

	elog(DEBUG2, "dropping vended secret \"%s\" from pgduck_server",
		 secretName);

	/* Best-effort: a failed drop must not abort the caller. */
	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, sql);

	PQclear(result);

	pfree(sql);
}


void
DropVendedSecretFromPGDuck(PGDuckConnection * conn,
						   Oid serverOid, const char *secretId)
{
	char	   *secretName = GenerateVendedSecretName(serverOid, secretId);

	DropNamedSecret(conn, secretName);
	pfree(secretName);
}


/*
 * DropVendedSecretsUnderScopeFromPGDuck looks the secrets up in
 * duckdb_secrets() rather than naming them, since neither the principal
 * nor the scope a secret another backend pushed was named after is known
 * here.  Only the name prefix ties a secret to its database and server.
 */
List *
DropVendedSecretsUnderScopeFromPGDuck(PGDuckConnection * conn, Oid serverOid,
									  const char *scopePrefix)
{
	List	   *names = NIL;

	/* An empty prefix would cover every secret of the server. */
	if (scopePrefix == NULL || scopePrefix[0] == '\0')
		return NIL;

	char	   *namePrefix = psprintf(VENDED_SECRET_NAME_PREFIX, MyDatabaseId,
									  serverOid);
	char	   *escapedScope = EscapeSingleQuotes(scopePrefix);
	char	   *query =
		psprintf("SELECT DISTINCT name FROM ("
				 "SELECT name, unnest(scope) AS prefix "
				 "FROM duckdb_secrets() "
				 "WHERE starts_with(name, '%s')) s "
				 "WHERE starts_with(prefix, '%s')",
				 namePrefix, escapedScope);

	PGresult   *result = ExecuteQueryOnPGDuckConnection(conn, query);

	PG_TRY();
	{
		ThrowIfPGDuckResultHasError(conn, result);

		for (int row = 0; row < PQntuples(result); row++)
			names = lappend(names, pstrdup(PQgetvalue(result, row, 0)));
	}
	PG_FINALLY();
	{
		PQclear(result);
	}
	PG_END_TRY();

	ListCell   *nameCell;

	foreach(nameCell, names)
		DropNamedSecret(conn, lfirst(nameCell));

	pfree(query);
	pfree(escapedScope);
	pfree(namePrefix);

	return names;
}
