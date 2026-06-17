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
 * EnsureVendedSecretInPGDuck creates or replaces a DuckDB scoped secret
 * for vended S3 credentials on the shared pgduck_server instance.
 *
 * The secret name is deterministic (pglake_vended_{serverOid}_{prefixHash})
 * so CREATE OR REPLACE is idempotent and does not leak entries.
 *
 * The secret's SCOPE is set to the S3 prefix so DuckDB's secret
 * manager automatically selects it for matching URLs.
 */
extern PGDLLEXPORT void EnsureVendedSecretInPGDuck(Oid serverOid,
												   const char *s3Prefix,
												   const char *accessKeyId,
												   const char *secretAccessKey,
												   const char *sessionToken,
												   const char *region);

/*
 * EnsureVendedSecretOnConnection is like EnsureVendedSecretInPGDuck but
 * sends the CREATE SECRET on an already-open pgduck connection rather
 * than acquiring a fresh one.  Use this when the caller already holds
 * a connection that will be used for the subsequent data query.
 */
extern PGDLLEXPORT void EnsureVendedSecretOnConnection(PGDuckConnection * conn,
													   Oid serverOid,
													   const char *s3Prefix,
													   const char *accessKeyId,
													   const char *secretAccessKey,
													   const char *sessionToken,
													   const char *region);

/*
 * DropVendedSecretFromPGDuck removes a previously-created vended secret
 * from DuckDB.  Safe to call even if the secret does not exist.
 */
extern PGDLLEXPORT void DropVendedSecretFromPGDuck(Oid serverOid,
												   const char *s3Prefix);

/*
 * GenerateVendedSecretName produces the deterministic secret name
 * for the given server OID and S3 prefix.
 */
extern PGDLLEXPORT char *GenerateVendedSecretName(Oid serverOid,
												  const char *s3Prefix);

#endif
