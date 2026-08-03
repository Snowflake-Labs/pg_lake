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

#ifndef PGDUCK_VENDED_SECRETS_H
#define PGDUCK_VENDED_SECRETS_H

#include "postgres.h"
#include "pg_lake/pgduck/client.h"

/*
 * VendedS3Secret describes one scoped S3 secret to (re)create in
 * pgduck_server.  The credential fields are supplied by the caller
 * (ultimately from the REST catalog loadTable response).
 *
 * endpoint / urlStyle / useSsl carry the catalog-provided S3 connection
 * settings.  Any of them left NULL is filled in from the pre-existing
 * (non-vended) S3 secret that covers the same bucket, so vended secrets
 * keep working against local S3 mocks (Moto/MinIO) and custom endpoints
 * even when the catalog omits them.  Catalog-provided values always win
 * over the inherited fallback.
 */
typedef struct VendedS3Secret
{
	Oid			serverOid;		/* iceberg_catalog server OID */
	const char *secretKey;		/* stable, principal-scoped identity */
	const char *scope;			/* normalized S3 scope (trailing '/') */
	const char *accessKeyId;
	const char *secretAccessKey;
	const char *sessionToken;	/* NULL for non-STS credentials */
	const char *region;			/* NULL when the catalog omits it */
	const char *endpoint;		/* NULL -> inherit from existing secret */
	const char *urlStyle;		/* "path"/"vhost"; NULL -> inherit */
	const char *useSsl;			/* "true"/"false"; NULL -> inherit */
}			VendedS3Secret;

/*
 * PushVendedSecretToPGDuck creates or replaces a DuckDB scoped secret
 * for vended S3 credentials on the shared pgduck_server instance.  The
 * call is mutating on purpose: it issues CREATE OR REPLACE SECRET, so
 * callers should treat this as a write, not a getter.
 *
 * The secret name is deterministic (see GenerateVendedSecretName).
 * Keeping the name independent of the S3 scope makes CREATE OR REPLACE
 * idempotent as credentials rotate and lets DropVendedSecretFromPGDuck
 * reconstruct the name without the credentials.  The secret's SCOPE is
 * set to secret->scope (the table's storage location) so DuckDB's secret
 * manager automatically selects it for matching URLs.
 */
extern PGDLLEXPORT void PushVendedSecretToPGDuck(const VendedS3Secret * secret);

/*
 * PushVendedSecretToPGDuckOnConnection is like PushVendedSecretToPGDuck
 * but sends the CREATE SECRET on an already-open pgduck connection
 * rather than acquiring a fresh one.  Use this when the caller batches
 * several secret operations onto one connection.
 */
extern PGDLLEXPORT void PushVendedSecretToPGDuckOnConnection(PGDuckConnection * conn,
															 const VendedS3Secret * secret);

/*
 * DropVendedSecretFromPGDuck removes a previously-created vended secret
 * from DuckDB.  Safe to call even if the secret does not exist.
 * The *OnConnection variant reuses an already-open connection.
 */
extern PGDLLEXPORT void DropVendedSecretFromPGDuck(Oid serverOid,
												   const char *secretKey);
extern PGDLLEXPORT void DropVendedSecretFromPGDuckOnConnection(PGDuckConnection * conn,
															   Oid serverOid,
															   const char *secretKey);

/*
 * GenerateVendedSecretName produces the deterministic secret name for
 * the given server OID and stable per-table secret key.  The name is
 * scoped to the current database so secrets pushed by backends of
 * different databases (which share one pgduck_server) never collide.
 */
extern PGDLLEXPORT char *GenerateVendedSecretName(Oid serverOid,
												  const char *secretKey);

#endif
