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

#ifndef PG_LAKE_TABLE_VENDED_CREDENTIALS_H
#define PG_LAKE_TABLE_VENDED_CREDENTIALS_H

#include "postgres.h"
#include "pg_lake/pgduck/client.h"

/*
 * PushVendedCredentialsForRelation fetches (or returns cached) vended
 * credentials from the REST catalog for the given relation and pushes
 * them as a DuckDB scoped secret on the shared pgduck_server instance.
 *
 * Safe to call for any relation -- no-ops for non-REST-catalog tables
 * or when vended credentials are disabled.
 */
extern void PushVendedCredentialsForRelation(Oid relationId);

/*
 * PushVendedCredentialsForRelationOnConnection is like
 * PushVendedCredentialsForRelation but uses the given pgduck
 * connection instead of acquiring a new one.
 */
extern void PushVendedCredentialsForRelationOnConnection(PGDuckConnection * conn,
														 Oid relationId);

/*
 * PushVendedCredentialsForRelations pushes vended credentials for
 * all REST catalog Iceberg tables in the given RTE list.
 */
extern void PushVendedCredentialsForRelations(List *rteList);

/*
 * DropVendedCredentialsForRelation removes the vended secret previously
 * pushed for the given relation from pgduck_server.  No-op for non-REST
 * tables; call it when a REST-backed Iceberg table is dropped.
 */
extern void DropVendedCredentialsForRelation(Oid relationId);

#endif
