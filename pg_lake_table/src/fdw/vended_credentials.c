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
 * PushVendedCredsInternal is the shared implementation for pushing
 * vended credentials.  If conn is NULL, acquires a fresh connection;
 * otherwise uses the provided one.
 */
static void
PushVendedCredsInternal(PGDuckConnection * conn, Oid relationId)
{
	VendedCredentials *creds = GetVendedCredentialsForRelation(relationId);

	if (creds == NULL)
		return;

	/*
	 * Determine the S3 prefix for scoping.  Use the location prefix from the
	 * REST catalog options if available; otherwise construct from the vended
	 * credential's scope.
	 */
	RestCatalogOptions *opts = GetRestCatalogOptionsForRelation(relationId);

	const char *restCatalogName = GetRestCatalogName(relationId);
	const char *namespaceName = GetRestCatalogNamespace(relationId);
	const char *tableName = GetRestCatalogTableName(relationId);

	char	   *s3Prefix;

	if (opts->locationPrefix != NULL && opts->locationPrefix[0] != '\0')
		s3Prefix = psprintf("%s/%s/%s/%s/",
							opts->locationPrefix,
							restCatalogName, namespaceName, tableName);
	else if (creds->scope != NULL && creds->scope[0] != '\0')
		s3Prefix = creds->scope;
	else
		s3Prefix = psprintf("s3://%s/%s/%s/",
							restCatalogName, namespaceName, tableName);

	if (conn != NULL)
		EnsureVendedSecretOnConnection(conn, opts->serverOid,
									   s3Prefix,
									   creds->accessKeyId,
									   creds->secretAccessKey,
									   creds->sessionToken,
									   creds->region);
	else
		EnsureVendedSecretInPGDuck(opts->serverOid,
								   s3Prefix,
								   creds->accessKeyId,
								   creds->secretAccessKey,
								   creds->sessionToken,
								   creds->region);
}


void
PushVendedCredentialsForRelation(Oid relationId)
{
	PushVendedCredsInternal(NULL, relationId);
}


void
PushVendedCredentialsForRelationOnConnection(PGDuckConnection * conn,
											 Oid relationId)
{
	PushVendedCredsInternal(conn, relationId);
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

		PushVendedCredsInternal(NULL, rte->relid);
	}
}
