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
 * storage_credentials.c
 *
 * Storage-credential resolver driven by a provider hook.  See
 * storage_credentials.h for the design and its known limitations.
 */

#include "postgres.h"

#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"

#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/vended_secrets.h"
#include "pg_lake/storage/storage_credentials.h"

/* Installed by pg_lake_iceberg at _PG_init. */
PgLakeStorageCredentialProviderHookType PgLakeStorageCredentialProviderHook = NULL;

/* Deterministic vended-secret names fit comfortably below this. */
#define SECRET_NAME_MAXLEN 128

/*
 * Do not re-push a secret whose expiry is still comfortably in the
 * future; this keeps repeated EnsureStorageCredentials calls within a
 * single statement free.  Matches the vended-credential cache margin.
 */
#define REFRESH_MARGIN_USEC ((int64) 5 * 60 * 1000000)

/*
 * Per-backend record of the vended secrets this backend pushed to the
 * (shared) pgduck_server, keyed by the generated secret name.  Lets
 * EnsureStorageCredentials skip still-valid secrets and drop secrets the
 * provider no longer returns (revoked credentials / vending disabled).
 */
typedef struct PushedSecretEntry
{
	char		secretName[SECRET_NAME_MAXLEN]; /* hash key -- must be first */
	Oid			relationId;
	Oid			serverOid;
	char	   *secretKey;
	TimestampTz expiresAt;
}			PushedSecretEntry;

static HTAB *PushedSecrets = NULL;
static MemoryContext PushedSecretsContext = NULL;


static void
InitPushedSecretsIfNeeded(void)
{
	HASHCTL		ctl;

	if (PushedSecrets != NULL)
		return;

	PushedSecretsContext =
		AllocSetContextCreate(TopMemoryContext,
							  "PgLake pushed vended secrets",
							  ALLOCSET_SMALL_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = SECRET_NAME_MAXLEN;
	ctl.entrysize = sizeof(PushedSecretEntry);
	ctl.hcxt = PushedSecretsContext;

	PushedSecrets = hash_create("PgLake pushed vended secrets", 32, &ctl,
								HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}


/*
 * SwallowBestEffortError handles an error caught inside a best-effort
 * step of the resolver.  Truly unrecoverable conditions (out of memory,
 * query cancellation) are re-thrown so we never mask them; anything else
 * (a transient OAuth error, a user mapping dropped mid-transaction, a
 * pgduck hiccup) is logged and swallowed, because resolving/pushing
 * storage credentials is advisory: if a credential was genuinely
 * required, the storage read/write that follows fails on its own with an
 * authoritative error.
 *
 * Must be called from within a PG_CATCH block, and only after the caller
 * has switched CurrentMemoryContext back out of ErrorContext -- both
 * CopyErrorData() and the elog below assert they do not run in
 * ErrorContext.
 *
 * The relation is reported by OID rather than by name: looking the name
 * up would mean touching the catalogs after an error was caught outside
 * a subtransaction, and the relation is often being dropped anyway.
 */
static void
SwallowBestEffortError(const char *what, Oid relationId)
{
	ErrorData  *edata;

	Assert(CurrentMemoryContext != ErrorContext);

	edata = CopyErrorData();

	if (edata->sqlerrcode == ERRCODE_OUT_OF_MEMORY ||
		edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
		edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
	{
		FreeErrorData(edata);
		PG_RE_THROW();
	}

	/*
	 * Warn rather than whisper.  Whatever follows this will fail on its own
	 * with a bare HTTP 403 from object storage, which says nothing about the
	 * credential that could not be resolved; without this line there is no
	 * trace of the real cause at default log levels.
	 */
	ereport(WARNING,
			(errmsg("could not resolve storage credentials for relation %u",
					relationId),
			 errdetail("%s failed: %s", what, edata->message),
			 errhint("Access to this relation's storage falls back to any "
					 "credentials already configured in pgduck_server.")));

	FlushErrorState();
	FreeErrorData(edata);
}


/*
 * ResolveStorageCredentials calls the provider hook, tolerating expected
 * errors (see SwallowBestEffortError).  Returns NIL when the relation has
 * no vended credentials or the resolution failed recoverably.
 */
static List *
ResolveStorageCredentials(Oid relationId)
{
	List	   *creds = NIL;
	MemoryContext callerContext = CurrentMemoryContext;

	if (PgLakeStorageCredentialProviderHook == NULL)
		return NIL;

	PG_TRY();
	{
		creds = PgLakeStorageCredentialProviderHook(relationId);
	}
	PG_CATCH();
	{
		/* Leave ErrorContext before inspecting/flushing the error. */
		MemoryContextSwitchTo(callerContext);
		SwallowBestEffortError("credential resolution", relationId);
		creds = NIL;
	}
	PG_END_TRY();

	return creds;
}


/*
 * MakeVendedS3Secret projects a resolved StorageCredential onto the
 * VendedS3Secret the pgduck secret layer consumes.
 */
static VendedS3Secret
MakeVendedS3Secret(const StorageCredential * sc)
{
	VendedS3Secret secret = {0};

	secret.serverOid = sc->serverOid;
	secret.secretKey = sc->secretKey;
	secret.scope = sc->scopePrefix;
	secret.accessKeyId = sc->accessKeyId;
	secret.secretAccessKey = sc->secretAccessKey;
	secret.sessionToken = sc->sessionToken;
	secret.region = sc->region;
	secret.endpoint = sc->endpoint;
	secret.urlStyle = sc->urlStyle;
	secret.useSsl = sc->useSsl;

	return secret;
}


/*
 * ReconcileSecretsOnConnection performs the actual pgduck work for one
 * relation on a single connection: (re)push every credential in toPush,
 * then drop every stale secret in toDrop, updating the per-backend
 * registry to match.  Batching onto one connection keeps a reconcile
 * that has work to do to a single round-trip's worth of setup.
 *
 * Any error propagates out (the connection is still released); the
 * best-effort swallow lives in the callers.
 */
static void
ReconcileSecretsOnConnection(Oid relationId, List *toPush, List *toDrop)
{
	PGDuckConnection *conn = GetPGDuckConnection();

	PG_TRY();
	{
		ListCell   *lc;

		foreach(lc, toPush)
		{
			StorageCredential *sc = (StorageCredential *) lfirst(lc);
			VendedS3Secret secret = MakeVendedS3Secret(sc);
			char	   *name = GenerateVendedSecretName(sc->serverOid,
														sc->secretKey);
			char		key[SECRET_NAME_MAXLEN];
			bool		found = false;
			PushedSecretEntry *entry;

			PushVendedSecretToPGDuckOnConnection(conn, &secret);

			strlcpy(key, name, SECRET_NAME_MAXLEN);
			entry = hash_search(PushedSecrets, key, HASH_FIND, &found);

			if (!found)
			{
				/*
				 * Copy the key before the entry exists.  A new entry holds
				 * uninitialized memory, so an allocation failure between
				 * inserting it and filling it in would leave a live entry
				 * pointing at garbage.
				 */
				MemoryContext old = MemoryContextSwitchTo(PushedSecretsContext);
				char	   *ownedKey = pstrdup(sc->secretKey);

				MemoryContextSwitchTo(old);

				entry = hash_search(PushedSecrets, key, HASH_ENTER, NULL);
				entry->secretKey = ownedKey;
			}
			entry->relationId = relationId;
			entry->serverOid = sc->serverOid;
			entry->expiresAt = sc->expiresAt;
		}

		foreach(lc, toDrop)
		{
			PushedSecretEntry *entry = (PushedSecretEntry *) lfirst(lc);
			char		key[SECRET_NAME_MAXLEN];

			DropVendedSecretFromPGDuckOnConnection(conn, entry->serverOid,
												   entry->secretKey);

			strlcpy(key, entry->secretName, SECRET_NAME_MAXLEN);
			pfree(entry->secretKey);
			hash_search(PushedSecrets, key, HASH_REMOVE, NULL);
		}
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(conn);
	}
	PG_END_TRY();
}


/*
 * CollectExpiredOrphans appends the secrets left behind by dropped
 * tables, once their credentials have expired, to toDrop.
 *
 * An orphan is kept alive only for as long as it can still authorize the
 * dropped table's queued deletes.  Past its expiry it can no longer do
 * that, and it turns into a hazard: DuckDB picks a secret by longest
 * matching scope regardless of whether its credentials are any good, so
 * an expired secret would deny access to a table later created under the
 * same prefix.  Sweeping costs nothing on the common path -- there is
 * usually nothing to sweep, and when there is, the caller already has
 * pgduck work to do.
 */
static List *
CollectExpiredOrphans(List *toDrop, TimestampTz now)
{
	HASH_SEQ_STATUS seq;
	PushedSecretEntry *entry;

	hash_seq_init(&seq, PushedSecrets);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		if (OidIsValid(entry->relationId))
			continue;

		if (entry->expiresAt == 0 || entry->expiresAt > now)
			continue;

		toDrop = lappend(toDrop, entry);
	}

	return toDrop;
}


/*
 * BestEffortReconcile wraps ReconcileSecretsOnConnection so a pgduck
 * failure (server down, transient error) never aborts the caller's
 * statement.  Skips acquiring a connection entirely when there is no
 * work -- the common warm-cache scan path.
 */
static void
BestEffortReconcile(Oid relationId, List *toPush, List *toDrop)
{
	MemoryContext callerContext = CurrentMemoryContext;

	if (toPush == NIL && toDrop == NIL)
		return;

	PG_TRY();
	{
		ReconcileSecretsOnConnection(relationId, toPush, toDrop);
	}
	PG_CATCH();
	{
		/* Leave ErrorContext before inspecting/flushing the error. */
		MemoryContextSwitchTo(callerContext);
		SwallowBestEffortError("secret reconcile", relationId);
	}
	PG_END_TRY();
}


void
EnsureStorageCredentialsForRelation(Oid relationId)
{
	List	   *creds;
	List	   *desiredNames = NIL;
	List	   *toPush = NIL;
	List	   *toDrop = NIL;
	ListCell   *lc;
	TimestampTz now;
	HASH_SEQ_STATUS seq;
	PushedSecretEntry *entry;

	creds = ResolveStorageCredentials(relationId);

	InitPushedSecretsIfNeeded();
	now = GetCurrentTimestamp();

	/* Decide which resolved credentials still need a (re)push. */
	foreach(lc, creds)
	{
		StorageCredential *sc = (StorageCredential *) lfirst(lc);
		char	   *name = GenerateVendedSecretName(sc->serverOid, sc->secretKey);
		char		key[SECRET_NAME_MAXLEN];
		bool		found = false;

		desiredNames = lappend(desiredNames, name);

		strlcpy(key, name, SECRET_NAME_MAXLEN);
		entry = hash_search(PushedSecrets, key, HASH_FIND, &found);

		/* Skip a secret we already pushed whose expiry is still safe. */
		if (found && entry->expiresAt > 0 &&
			entry->expiresAt > now + REFRESH_MARGIN_USEC)
			continue;

		toPush = lappend(toPush, sc);
	}

	/*
	 * Drop any secret we previously pushed for this relation that the
	 * provider no longer returns.  This is what prevents a stale, expired
	 * secret from lingering on the shared pgduck_server and denying access to
	 * a later scan (the instance-wide 403 failure mode).
	 */
	hash_seq_init(&seq, PushedSecrets);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		bool		desired = false;

		if (entry->relationId != relationId)
			continue;

		foreach(lc, desiredNames)
		{
			if (strcmp((char *) lfirst(lc), entry->secretName) == 0)
			{
				desired = true;
				break;
			}
		}

		if (!desired)
			toDrop = lappend(toDrop, entry);
	}

	toDrop = CollectExpiredOrphans(toDrop, now);

	BestEffortReconcile(relationId, toPush, toDrop);
}


void
ForgetStorageCredentials(Oid relationId)
{
	HASH_SEQ_STATUS seq;
	PushedSecretEntry *entry;
	List	   *toDrop = NIL;

	if (PushedSecrets == NULL)
		return;

	/*
	 * An orphan records InvalidOid as its relation, so forgetting InvalidOid
	 * would match every orphan here and again in the sweep below, collecting
	 * each of them twice.
	 */
	if (!OidIsValid(relationId))
		return;

	hash_seq_init(&seq, PushedSecrets);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		if (entry->relationId == relationId)
			toDrop = lappend(toDrop, entry);
	}

	toDrop = CollectExpiredOrphans(toDrop, GetCurrentTimestamp());

	BestEffortReconcile(relationId, NIL, toDrop);
}


void
OrphanStorageCredentials(Oid relationId)
{
	HASH_SEQ_STATUS seq;
	PushedSecretEntry *entry;

	if (PushedSecrets == NULL)
		return;

	hash_seq_init(&seq, PushedSecrets);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		if (entry->relationId == relationId)
			entry->relationId = InvalidOid;
	}
}
