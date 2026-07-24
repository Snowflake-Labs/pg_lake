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
 * vended_credentials.c
 *
 * Bridges the Iceberg REST catalog vended credentials (from
 * pg_lake_iceberg) with the DuckDB scoped secret infrastructure
 * (from pg_lake_engine).  Provides convenience functions to push
 * vended credentials for relations before issuing data queries to
 * pgduck_server.
 */

#include "postgres.h"

#include "nodes/parsenodes.h"

#include "pg_lake/fdw/vended_credentials.h"
#include "pg_lake/pgduck/vended_secrets.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/catalog_type.h"


/*
 * VendedSecretKeyForRelation returns the stable per-table identity used
 * as the DuckDB secret name key.  It matches the vended credentials
 * cache key in pg_lake_iceberg ("catalog/namespace/table"), so the
 * secret name is independent of the (rotating) S3 scope and can be
 * reconstructed at DROP time without the credentials.
 */
static char *
VendedSecretKeyForRelation(Oid relationId)
{
	return psprintf("%s/%s/%s",
					GetRestCatalogName(relationId),
					GetRestCatalogNamespace(relationId),
					GetRestCatalogTableName(relationId));
}


/*
 * VendedSecretScope resolves the S3 SCOPE for the pushed secret.  The
 * credential's own scope (the table's storage location, as reported by
 * the REST catalog loadTable response) is authoritative and covers
 * external tables whose data lives outside the configured location
 * prefix.  We only synthesize a prefix as a last resort.
 */
static char *
VendedSecretScope(Oid relationId, VendedCredentials * creds)
{
	if (creds->scope != NULL && creds->scope[0] != '\0')
		return creds->scope;

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);
	const char *restCatalogName = GetRestCatalogName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);
	const char *tableName = GetRestCatalogTableName(relationId);

	if (opts->locationPrefix != NULL && opts->locationPrefix[0] != '\0')
		return psprintf("%s/%s/%s/%s/",
						opts->locationPrefix,
						restCatalogName, namespaceName, tableName);

	return psprintf("s3://%s/%s/%s/",
					restCatalogName, namespaceName, tableName);
}


/*
 * PushVendedCredsInternal is the shared implementation for pushing
 * vended credentials.  If conn is NULL, acquires a fresh connection;
 * otherwise uses the provided one.
 *
 * loadOnMiss controls the cache behavior: the read/scan path leaves it
 * false (the cache is already warmed by snapshot creation), while the
 * modify path sets it true so a table first touched for writing in this
 * backend still gets its vended secret instead of silently falling back
 * to a (possibly absent) static S3 secret.
 */
static void
PushVendedCredsInternal(PGDuckConnection * conn, Oid relationId, bool loadOnMiss)
{
	VendedCredentials *creds = loadOnMiss ?
		GetVendedCredentialsForRelationLoadOnMiss(relationId) :
		GetVendedCredentialsForRelation(relationId);

	if (creds == NULL)
		return;

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);
	char	   *secretKey = VendedSecretKeyForRelation(relationId);
	char	   *s3Scope = VendedSecretScope(relationId, creds);

	if (conn != NULL)
		EnsureVendedSecretOnConnection(conn, opts->serverOid,
									   secretKey, s3Scope,
									   creds->accessKeyId,
									   creds->secretAccessKey,
									   creds->sessionToken,
									   creds->region);
	else
		EnsureVendedSecretInPGDuck(opts->serverOid,
								   secretKey, s3Scope,
								   creds->accessKeyId,
								   creds->secretAccessKey,
								   creds->sessionToken,
								   creds->region);
}


void
PushVendedCredentialsForRelation(Oid relationId)
{
	PushVendedCredsInternal(NULL, relationId, true);
}


void
PushVendedCredentialsForRelationOnConnection(PGDuckConnection * conn,
											 Oid relationId)
{
	PushVendedCredsInternal(conn, relationId, true);
}


void
PushVendedCredentialsForRelations(List *rteList)
{
	ListCell   *cell;

	foreach(cell, rteList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(cell);

		if (rte->rtekind != RTE_RELATION)
			continue;

		PushVendedCredsInternal(NULL, rte->relid, false);
	}
}


/*
 * DropVendedCredentialsForRelation removes the vended secret previously
 * pushed for the given relation from pgduck_server.  Best-effort and
 * safe for non-REST tables; intended to be called when a REST-backed
 * Iceberg table is dropped so vended secrets do not accumulate on the
 * shared server for tables that no longer exist.
 */
void
DropVendedCredentialsForRelation(Oid relationId)
{
	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	if (catalogType != REST_CATALOG_READ_ONLY &&
		catalogType != REST_CATALOG_READ_WRITE)
		return;

	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);
	char	   *secretKey = VendedSecretKeyForRelation(relationId);

	DropVendedSecretFromPGDuck(opts->serverOid, secretKey);
}
