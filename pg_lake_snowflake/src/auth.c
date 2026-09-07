/*
 * Copyright 2026 Snowflake Inc.
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
 * auth.c
 *
 * The Authorization header of a SQL API request.
 *
 * A programmatic access token or an OAuth token is passed through as a bearer
 * token. Key-pair authentication signs a JWT with the private key of the user
 * mapping; those are minted for slightly less than an hour and cached per
 * (server, user) for the lifetime of the session, because signing is expensive
 * relative to a statement and the same token serves every request.
 */

#include "postgres.h"
#include "miscadmin.h"

#include "lib/stringinfo.h"
#include "utils/formatting.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include "pg_lake_snowflake/auth.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"

/*
 * A JWT is minted for this long and treated as expired a little earlier, so
 * that a statement submitted just before the edge is not rejected. Snowflake
 * accepts a lifetime of at most one hour.
 */
#define SNOWFLAKE_JWT_LIFETIME_SECONDS 3300
#define SNOWFLAKE_JWT_RENEW_MARGIN_SECONDS 300

typedef struct SnowflakeTokenCacheKey
{
	Oid			serverId;
	Oid			userId;
}			SnowflakeTokenCacheKey;

typedef struct SnowflakeTokenCacheEntry
{
	SnowflakeTokenCacheKey key;
	char	   *jwt;
	TimestampTz mintedAt;
}			SnowflakeTokenCacheEntry;

static HTAB *SnowflakeTokenCache = NULL;

static char *SnowflakeBearerToken(SnowflakeConnection * connection);
static const char *SnowflakeTokenTypeHeaderValue(SnowflakeConnection * connection);
static char *BuildKeyPairJwt(SnowflakeConnection * connection);
static EVP_PKEY *ReadPrivateKey(SnowflakeConnection * connection);
static char *PublicKeyFingerprint(EVP_PKEY *privateKey);
static char *SignWithRsaSha256(EVP_PKEY *privateKey, const char *message);
static char *Base64Encode(const uint8 *bytes, int byteCount, bool urlSafe);
static char *OpenSslErrorString(void);
static HTAB *CreateTokenCache(void);


/*
 * SnowflakeRequestHeaders returns the headers of a SQL API request, including
 * the Authorization header for the credentials of the connection.
 */
List *
SnowflakeRequestHeaders(SnowflakeConnection * connection, bool hasBody)
{
	List	   *headers = NIL;

	headers = lappend(headers, psprintf("Authorization: Bearer %s",
										SnowflakeBearerToken(connection)));
	headers = lappend(headers, psprintf("X-Snowflake-Authorization-Token-Type: %s",
										SnowflakeTokenTypeHeaderValue(connection)));
	headers = lappend(headers, pstrdup("Accept: application/json"));
	headers = lappend(headers, pstrdup("User-Agent: pg_lake_snowflake"));

	if (hasBody)
		headers = lappend(headers, pstrdup("Content-Type: application/json"));

	return headers;
}


/*
 * SnowflakeForgetCachedToken drops the cached JWT of a connection, so that the
 * next request signs a new one. The account rejecting a token we still consider
 * valid is the case this exists for.
 */
void
SnowflakeForgetCachedToken(SnowflakeConnection * connection)
{
	if (SnowflakeTokenCache == NULL)
		return;

	SnowflakeTokenCacheKey key = {
		.serverId = connection->serverId,
		.userId = connection->userId
	};
	bool		found = false;

	SnowflakeTokenCacheEntry *entry = hash_search(SnowflakeTokenCache, &key,
												  HASH_FIND, &found);

	if (!found)
		return;

	if (entry->jwt != NULL)
		pfree(entry->jwt);

	hash_search(SnowflakeTokenCache, &key, HASH_REMOVE, NULL);
}


/*
 * SnowflakeBearerToken returns the token that authorizes a request.
 */
static char *
SnowflakeBearerToken(SnowflakeConnection * connection)
{
	if (connection->authMethod != SNOWFLAKE_AUTH_KEYPAIR)
		return connection->token;

	if (SnowflakeTokenCache == NULL)
		SnowflakeTokenCache = CreateTokenCache();

	SnowflakeTokenCacheKey key = {
		.serverId = connection->serverId,
		.userId = connection->userId
	};
	bool		found = false;
	SnowflakeTokenCacheEntry *entry = hash_search(SnowflakeTokenCache, &key,
												  HASH_ENTER, &found);

	if (found)
	{
		int			ageSeconds = (int) ((GetCurrentTimestamp() - entry->mintedAt) /
										USECS_PER_SEC);

		if (ageSeconds < SNOWFLAKE_JWT_LIFETIME_SECONDS - SNOWFLAKE_JWT_RENEW_MARGIN_SECONDS)
			return entry->jwt;

		pfree(entry->jwt);
	}

	char	   *jwt = BuildKeyPairJwt(connection);
	MemoryContext previousContext = MemoryContextSwitchTo(CacheMemoryContext);

	entry->jwt = pstrdup(jwt);
	entry->mintedAt = GetCurrentTimestamp();

	MemoryContextSwitchTo(previousContext);

	return entry->jwt;
}


/*
 * SnowflakeTokenTypeHeaderValue returns the token type that tells Snowflake how
 * to interpret the bearer token.
 */
static const char *
SnowflakeTokenTypeHeaderValue(SnowflakeConnection * connection)
{
	switch (connection->authMethod)
	{
		case SNOWFLAKE_AUTH_OAUTH:
			return "OAUTH";
		case SNOWFLAKE_AUTH_KEYPAIR:
			return "KEYPAIR_JWT";
		case SNOWFLAKE_AUTH_PAT:
		case SNOWFLAKE_AUTH_UNSPECIFIED:
		default:
			return "PROGRAMMATIC_ACCESS_TOKEN";
	}
}


/*
 * CreateTokenCache creates the session-lifetime hash of minted JWTs.
 */
static HTAB *
CreateTokenCache(void)
{
	HASHCTL		hashInfo;

	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(SnowflakeTokenCacheKey);
	hashInfo.entrysize = sizeof(SnowflakeTokenCacheEntry);
	hashInfo.hcxt = CacheMemoryContext;

	return hash_create("pg_lake_snowflake tokens", 8, &hashInfo,
					   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}


/*
 * BuildKeyPairJwt signs a JWT that identifies the user of the connection by the
 * fingerprint of the public key belonging to its private key.
 */
static char *
BuildKeyPairJwt(SnowflakeConnection * connection)
{
	EVP_PKEY   *privateKey = ReadPrivateKey(connection);

	/* volatile because it is set inside PG_TRY and read after it */
	char	   *volatile jwt = NULL;

	PG_TRY();
	{
		char	   *fingerprint = PublicKeyFingerprint(privateKey);
		char	   *account = asc_toupper(connection->account,
										  strlen(connection->account));
		char	   *userName = asc_toupper(connection->userName,
										   strlen(connection->userName));
		int64		unixEpochOffset = (int64) (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) *
			SECS_PER_DAY;
		int64		issuedAt = GetCurrentTimestamp() / USECS_PER_SEC + unixEpochOffset;

		StringInfo	header = makeStringInfo();

		appendStringInfoString(header, "{\"alg\":\"RS256\",\"typ\":\"JWT\"}");

		StringInfo	claims = makeStringInfo();

		appendStringInfo(claims,
						 "{\"iss\":\"%s.%s.%s\",\"sub\":\"%s.%s\","
						 "\"iat\":" INT64_FORMAT ",\"exp\":" INT64_FORMAT "}",
						 account, userName, fingerprint,
						 account, userName,
						 issuedAt, issuedAt + SNOWFLAKE_JWT_LIFETIME_SECONDS);

		char	   *encodedHeader = Base64Encode((const uint8 *) header->data,
												 header->len, true);
		char	   *encodedClaims = Base64Encode((const uint8 *) claims->data,
												 claims->len, true);
		char	   *signingInput = psprintf("%s.%s", encodedHeader, encodedClaims);
		char	   *signature = SignWithRsaSha256(privateKey, signingInput);

		jwt = psprintf("%s.%s", signingInput, signature);
	}
	PG_FINALLY();
	{
		EVP_PKEY_free(privateKey);
	}
	PG_END_TRY();

	return jwt;
}


/*
 * ReadPrivateKey parses the PEM private key of the user mapping.
 */
static EVP_PKEY *
ReadPrivateKey(SnowflakeConnection * connection)
{
	BIO		   *keyBio = BIO_new_mem_buf(connection->privateKeyPem,
										 (int) strlen(connection->privateKeyPem));

	if (keyBio == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not allocate a buffer for the private key")));
	}

	/*
	 * With no callback, OpenSSL uses the pointer we pass as the passphrase
	 * itself, which is NULL for an unencrypted key.
	 */
	EVP_PKEY   *privateKey = PEM_read_bio_PrivateKey(keyBio, NULL, NULL,
													 connection->privateKeyPassphrase);

	BIO_free(keyBio);

	if (privateKey == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not read the private key of the user mapping on "
						"server \"%s\"", connection->serverName),
				 errdetail("%s", OpenSslErrorString()),
				 errhint("The key must be an unencrypted or passphrase-protected "
						 "PEM private key, as produced by "
						 "\"openssl genrsa\" plus \"openssl pkcs8\".")));
	}

	return privateKey;
}


/*
 * PublicKeyFingerprint returns the fingerprint Snowflake stores for a public
 * key, which is the SHA-256 digest of its DER encoding in base64.
 */
static char *
PublicKeyFingerprint(EVP_PKEY *privateKey)
{
	unsigned char *derEncoding = NULL;
	int			derLength = i2d_PUBKEY(privateKey, &derEncoding);

	if (derLength <= 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("could not encode the public key of the private key"),
				 errdetail("%s", OpenSslErrorString())));
	}

	unsigned char digest[EVP_MAX_MD_SIZE];
	unsigned int digestLength = 0;
	int			digestResult = EVP_Digest(derEncoding, derLength, digest,
										  &digestLength, EVP_sha256(), NULL);

	OPENSSL_free(derEncoding);

	if (digestResult != 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not compute the public key fingerprint"),
				 errdetail("%s", OpenSslErrorString())));
	}

	return psprintf("SHA256:%s", Base64Encode(digest, (int) digestLength, false));
}


/*
 * SignWithRsaSha256 returns the base64url encoded RS256 signature of a message.
 */
static char *
SignWithRsaSha256(EVP_PKEY *privateKey, const char *message)
{
	EVP_MD_CTX *signContext = EVP_MD_CTX_new();

	if (signContext == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("could not allocate a signing context")));
	}

	/* volatile because it is set inside PG_TRY and read after it */
	char	   *volatile encodedSignature = NULL;

	PG_TRY();
	{
		size_t		signatureLength = 0;

		if (EVP_DigestSignInit(signContext, NULL, EVP_sha256(), NULL, privateKey) != 1 ||
			EVP_DigestSignUpdate(signContext, message, strlen(message)) != 1 ||
			EVP_DigestSignFinal(signContext, NULL, &signatureLength) != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not sign the authentication token"),
					 errdetail("%s", OpenSslErrorString())));
		}

		unsigned char *signature = palloc(signatureLength);

		if (EVP_DigestSignFinal(signContext, signature, &signatureLength) != 1)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					 errmsg("could not sign the authentication token"),
					 errdetail("%s", OpenSslErrorString())));
		}

		encodedSignature = Base64Encode(signature, (int) signatureLength, true);
	}
	PG_FINALLY();
	{
		EVP_MD_CTX_free(signContext);
	}
	PG_END_TRY();

	return encodedSignature;
}


/*
 * Base64Encode encodes bytes in base64, in the URL-safe unpadded alphabet that
 * JWTs use when urlSafe is set and in the standard padded alphabet otherwise.
 */
static char *
Base64Encode(const uint8 *bytes, int byteCount, bool urlSafe)
{
	static const char standardAlphabet[] =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	static const char urlSafeAlphabet[] =
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

	const char *alphabet = urlSafe ? urlSafeAlphabet : standardAlphabet;
	StringInfo	encoded = makeStringInfo();

	for (int offset = 0; offset < byteCount; offset += 3)
	{
		int			remaining = byteCount - offset;
		uint32		group = (uint32) bytes[offset] << 16;

		if (remaining > 1)
			group |= (uint32) bytes[offset + 1] << 8;
		if (remaining > 2)
			group |= (uint32) bytes[offset + 2];

		appendStringInfoChar(encoded, alphabet[(group >> 18) & 0x3f]);
		appendStringInfoChar(encoded, alphabet[(group >> 12) & 0x3f]);

		if (remaining > 1)
			appendStringInfoChar(encoded, alphabet[(group >> 6) & 0x3f]);
		else if (!urlSafe)
			appendStringInfoChar(encoded, '=');

		if (remaining > 2)
			appendStringInfoChar(encoded, alphabet[group & 0x3f]);
		else if (!urlSafe)
			appendStringInfoChar(encoded, '=');
	}

	return encoded->data;
}


/*
 * OpenSslErrorString drains the OpenSSL error queue into one message.
 */
static char *
OpenSslErrorString(void)
{
	StringInfo	message = makeStringInfo();
	unsigned long errorCode = 0;

	while ((errorCode = ERR_get_error()) != 0)
	{
		char		errorBuffer[256];

		ERR_error_string_n(errorCode, errorBuffer, sizeof(errorBuffer));

		if (message->len > 0)
			appendStringInfoString(message, "; ");

		appendStringInfoString(message, errorBuffer);
	}

	if (message->len == 0)
		appendStringInfoString(message, "no OpenSSL error was reported");

	return message->data;
}
