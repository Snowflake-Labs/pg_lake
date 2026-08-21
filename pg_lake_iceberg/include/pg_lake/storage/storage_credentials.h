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
 * storage_credentials.h
 *
 * Resolves the credentials needed to reach a relation's storage and
 * reconciles them into pgduck_server as scoped DuckDB secrets.
 *
 * Credentials come from an Iceberg REST catalog, so this lives with the
 * rest of the catalog code rather than in pg_lake_engine: the engine
 * offers the mechanism for creating a scoped secret (see
 * pg_lake/pgduck/vended_secrets.h) and knows nothing of catalogs, and
 * every caller here sits at or above pg_lake_iceberg.
 *
 * Resolution is a pull, not a push.  EnsureStorageCredentialsForRelation
 * is called where a storage path is about to be touched; it resolves the
 * credentials on demand -- issuing a REST loadTable on a cache miss --
 * and reconciles the secret set, (re)pushing fresh credentials and
 * dropping secrets the catalog no longer vends.  Resolving at the point
 * of use, rather than pushing from a cache that something has to
 * remember to warm, is what makes each storage entry point covered by
 * construction.
 *
 * Only read-only REST tables are served; see
 * IcebergProvideStorageCredentials for why writable tables wait.
 *
 * Two limitations are inherent to a shared pgduck_server holding global,
 * in-memory secrets:
 *
 *  1. pgduck_server restart.  A backend tracks the secrets it pushed and
 *     skips a re-push while the cached expiry is still safe.  If
 *     pgduck_server restarts, those secrets are gone while the backend
 *     still believes them present, so scans can fail with HTTP 403 until
 *     the next reconcile that has other work -- bounded by the credential
 *     TTL (<=1h for STS).  Closing this needs either a pgduck boot-id
 *     handshake (free on connect) or treating an S3 403 as a signal to
 *     re-push.
 *
 *  2. Same-scope, multi-principal selection.  DuckDB selects a secret by
 *     longest-matching SCOPE, not by name.  Principal-scoped secret names
 *     stop one backend from clobbering another's secret, but two secrets
 *     with the *same* scope and different credentials (two roles, one S3
 *     prefix) are ambiguous to DuckDB's tie-break.
 */

#ifndef PG_LAKE_STORAGE_CREDENTIALS_H
#define PG_LAKE_STORAGE_CREDENTIALS_H

#include "postgres.h"

#include "datatype/timestamp.h"
#include "nodes/pg_list.h"

/*
 * One credential and the storage prefix it authorizes.  A REST
 * "storage-credentials" array can yield several of these per table, so
 * resolution returns a List<StorageCredential *>.
 *
 * secretId is the stable identity used to name the pgduck_server
 * secret.  It MUST incorporate the user-mapping OID, so that two roles
 * vended different credentials for the same table do not collide on a
 * single secret, and the scope, so that two credentials of the same
 * table do not overwrite each other.
 *
 * IcebergProvideStorageCredentials returns freshly-allocated copies (not
 * pointers into the credential cache), because the caller may run
 * syscache lookups between resolving and using these values.
 */
typedef struct StorageCredential
{
	Oid			serverOid;		/* iceberg_catalog server OID */
	char	   *secretId;		/* identity, e.g.
								 * "<umOid>/<catalog>/<ns>/<table>/<scope>" */
	char	   *scopePrefix;	/* normalized S3 scope, trailing '/' */
	char	   *accessKeyId;
	char	   *secretAccessKey;
	char	   *sessionToken;	/* NULL for non-STS credentials */
	char	   *region;			/* NULL when the catalog omits it */
	char	   *endpoint;		/* catalog s3.endpoint; NULL -> inherit */
	char	   *urlStyle;		/* "path"/"vhost"; NULL -> inherit */
	char	   *useSsl;			/* "true"/"false"; NULL -> inherit */
	TimestampTz expiresAt;		/* 0 when the catalog gave no expiry */
}			StorageCredential;

/*
 * EnsureStorageCredentialsForRelation reconciles the pgduck_server
 * secrets for the given relation against what the catalog currently
 * vends: it (re)pushes fresh credentials and drops secrets the catalog
 * no longer returns.  Best-effort: a failure to resolve (e.g. a
 * transient OAuth error) never aborts the caller's statement -- the
 * storage operation itself will fail authoritatively if a credential was
 * truly required.
 */
extern PGDLLEXPORT void EnsureStorageCredentialsForRelation(Oid relationId);

/*
 * ForgetStorageCredentials drops any secrets this backend pushed for the
 * relation.  Call it once the relation's storage no longer needs to be
 * reached, which for a read-only table is the moment it is dropped: it
 * owns none of the files it reads, so its drop queues no deletes.  Left
 * behind, an expired secret would still win DuckDB's longest-scope match
 * for everything under that prefix.
 */
extern PGDLLEXPORT void ForgetStorageCredentials(Oid relationId);

#endif							/* PG_LAKE_STORAGE_CREDENTIALS_H */
