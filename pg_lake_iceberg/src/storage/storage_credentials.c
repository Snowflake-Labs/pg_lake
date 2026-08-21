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
 * Storage-credential resolver.  See storage_credentials.h for the design
 * and its known limitations.
 */

#include "postgres.h"

#include "access/xact.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"

#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/vended_secrets.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/storage/storage_credentials.h"

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
	char	   *secretId;
	TimestampTz expiresAt;
}			PushedSecretEntry;

static HTAB *PushedSecrets = NULL;
static MemoryContext PushedSecretsContext = NULL;

static void InitPushedSecretsIfNeeded(void);
static List *ResolveStorageCredentials(Oid relationId);
static void BestEffortReconcile(Oid relationId, List *toPush, List *toDrop);
static void ReconcileSecrets(Oid relationId, List *toPush, List *toDrop);
static VendedS3Secret MakeVendedS3Secret(const StorageCredential * sc);
static void ReportOrRethrowBestEffortError(ErrorData *edata, const char *what,
										   Oid relationId);


/*
 * EnsureStorageCredentialsForRelation brings the vended secrets this
 * backend has pushed for one relation in line with what the catalog
 * vends for it right now.  See storage_credentials.h for the contract.
 *
 * The work is decided before any of it is done: ask the catalog, then
 * compare what it returned against the registry of what this backend
 * already pushed, collecting the credentials that need a (re)push and
 * the secrets that are no longer vended.  Only if that leaves something
 * to do does the reconcile reach pgduck_server, so a warm scan -- the
 * common case, since a vended credential outlives many statements --
 * costs nothing beyond the catalog's own answer.
 */
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

	if (!OidIsValid(relationId))
		return;

	creds = ResolveStorageCredentials(relationId);

	/*
	 * A backend that has never held a vended secret and is not being given
	 * one now has nothing to reconcile, and no reason to build the table that
	 * tracks them.
	 */
	if (creds == NIL && PushedSecrets == NULL)
		return;

	InitPushedSecretsIfNeeded();
	now = GetCurrentTimestamp();

	/* Decide which resolved credentials still need a (re)push. */
	foreach(lc, creds)
	{
		StorageCredential *sc = (StorageCredential *) lfirst(lc);
		char	   *name = GenerateVendedSecretName(sc->serverOid, sc->secretId);
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
	 * Drop any secret we previously pushed for this relation that the catalog
	 * no longer vends.  This is what prevents a stale, expired secret from
	 * lingering on the shared pgduck_server and denying access to a later
	 * scan (the instance-wide 403 failure mode).
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

	if (toPush != NIL || toDrop != NIL)
		BestEffortReconcile(relationId, toPush, toDrop);
}


/*
 * ForgetStorageCredentials drops every secret this backend pushed for
 * the relation.  See storage_credentials.h for why a dropped relation
 * must not leave its secrets behind.
 */
void
ForgetStorageCredentials(Oid relationId)
{
	HASH_SEQ_STATUS seq;
	PushedSecretEntry *entry;
	List	   *toDrop = NIL;

	if (PushedSecrets == NULL || !OidIsValid(relationId))
		return;

	hash_seq_init(&seq, PushedSecrets);
	while ((entry = hash_seq_search(&seq)) != NULL)
	{
		if (entry->relationId == relationId)
			toDrop = lappend(toDrop, entry);
	}

	if (toDrop != NIL)
		BestEffortReconcile(relationId, NIL, toDrop);
}


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
 * ReportOrRethrowBestEffortError swallows the error a best-effort step
 * failed with, unless it is one we must never mask (out of memory,
 * query cancellation, shutdown): resolving and pushing storage
 * credentials is advisory, so a transient OAuth error, a user mapping
 * dropped mid-transaction or a pgduck hiccup should not abort the
 * caller's statement.  If a credential was genuinely required, the
 * storage read that follows fails on its own with an authoritative
 * error.
 *
 * The caller aborts the step's subtransaction and copies the error out
 * of ErrorContext before getting here, so this only decides what to do
 * with what is left.
 *
 * The relation is reported by OID rather than by name, because by the
 * time this reports, the relation may already be gone (a drop is one of
 * the paths that reconciles secrets).
 */
static void
ReportOrRethrowBestEffortError(ErrorData *edata, const char *what,
							   Oid relationId)
{
	Assert(CurrentMemoryContext != ErrorContext);

	if (edata->sqlerrcode == ERRCODE_OUT_OF_MEMORY ||
		edata->sqlerrcode == ERRCODE_QUERY_CANCELED ||
		edata->sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		ReThrowError(edata);

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

	FreeErrorData(edata);
}


/*
 * ResolveStorageCredentials asks the catalog what it vends for this
 * relation, tolerating expected errors (see
 * ReportOrRethrowBestEffortError).  Returns NIL when the relation has
 * no vended credentials or the resolution failed recoverably.
 *
 * It runs in a subtransaction of its own because catching the error is
 * not enough to carry on with the caller's statement: the attempt that
 * failed may have left a lock, an open catalog scan or a half-registered
 * resource behind, and only a subtransaction abort gives those back.
 * The subtransaction takes over the current memory context and resource
 * owner while it runs, so both are saved and put back -- and the
 * credentials are built in the caller's context, where they outlive it.
 */
static List *
ResolveStorageCredentials(Oid relationId)
{
	List	   *volatile creds = NIL;
	MemoryContext callerContext = CurrentMemoryContext;
	ResourceOwner callerOwner = CurrentResourceOwner;

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(callerContext);

	PG_TRY();
	{
		creds = IcebergProvideStorageCredentials(relationId);

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(callerContext);
		CurrentResourceOwner = callerOwner;
	}
	PG_CATCH();
	{
		/* Leave ErrorContext before inspecting/flushing the error. */
		MemoryContextSwitchTo(callerContext);

		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(callerContext);
		CurrentResourceOwner = callerOwner;

		ReportOrRethrowBestEffortError(edata, "credential resolution",
									   relationId);
		creds = NIL;
	}
	PG_END_TRY();

	return (List *) creds;
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
	secret.secretId = sc->secretId;
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
 * ReconcileSecrets performs the actual pgduck work for one relation:
 * (re)push every credential in toPush, then drop every stale secret in
 * toDrop, updating the per-backend registry to match.
 *
 * The secrets themselves are pgduck_server-wide, not per connection; a
 * single connection is taken out here only so that a relation with
 * several of them to reconcile spends one connection on the lot rather
 * than one apiece.
 *
 * Any error propagates out (the connection is still released); the
 * best-effort swallow lives in the caller.
 */
static void
ReconcileSecrets(Oid relationId, List *toPush, List *toDrop)
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
														sc->secretId);
			char		key[SECRET_NAME_MAXLEN];
			bool		found = false;
			PushedSecretEntry *entry;

			PushVendedSecretToPGDuck(conn, &secret);

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
				char	   *ownedId = pstrdup(sc->secretId);

				MemoryContextSwitchTo(old);

				entry = hash_search(PushedSecrets, key, HASH_ENTER, NULL);
				entry->secretId = ownedId;
			}
			entry->relationId = relationId;
			entry->serverOid = sc->serverOid;
			entry->expiresAt = sc->expiresAt;
		}

		foreach(lc, toDrop)
		{
			PushedSecretEntry *entry = (PushedSecretEntry *) lfirst(lc);
			char		key[SECRET_NAME_MAXLEN];

			DropVendedSecretFromPGDuck(conn, entry->serverOid,
									   entry->secretId);

			strlcpy(key, entry->secretName, SECRET_NAME_MAXLEN);
			pfree(entry->secretId);
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
 * BestEffortReconcile wraps ReconcileSecrets so a pgduck failure (server
 * down, transient error) never aborts the caller's statement.  Callers
 * establish that there is work to do; this always takes a connection.
 *
 * Runs in a subtransaction of its own, for the reason given in
 * ResolveStorageCredentials.
 */
static void
BestEffortReconcile(Oid relationId, List *toPush, List *toDrop)
{
	MemoryContext callerContext = CurrentMemoryContext;
	ResourceOwner callerOwner = CurrentResourceOwner;

	Assert(toPush != NIL || toDrop != NIL);

	BeginInternalSubTransaction(NULL);
	MemoryContextSwitchTo(callerContext);

	PG_TRY();
	{
		ReconcileSecrets(relationId, toPush, toDrop);

		ReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(callerContext);
		CurrentResourceOwner = callerOwner;
	}
	PG_CATCH();
	{
		/* Leave ErrorContext before inspecting/flushing the error. */
		MemoryContextSwitchTo(callerContext);

		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		RollbackAndReleaseCurrentSubTransaction();
		MemoryContextSwitchTo(callerContext);
		CurrentResourceOwner = callerOwner;

		ReportOrRethrowBestEffortError(edata, "secret reconcile", relationId);
	}
	PG_END_TRY();
}
