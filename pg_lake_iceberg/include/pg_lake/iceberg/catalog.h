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

#pragma once

#include "nodes/primnodes.h"

#include "pg_lake/util/rel_utils.h"

/*
 * Default location prefix for pg_lake_iceberg tables. Used when the location
 * option is not specified at "CREATE FOREIGN SERVER pg_lake_iceberg OPTIONS ()".
 */
extern char *IcebergDefaultLocationPrefix;


/*
* From the external view perspective, the pg_catalog.iceberg_tables
* (and pg_lake_iceberg.tables) includes both the internal and
* external iceberg tables.
*
* We define internal tables as the tables that are created on this
* Postgres server, external tables are the tables that are created
* elsewhere (e.g., spark) but uses our server as the catalog.
*
* This enables us to use Oids in the internal tables, hence we can
* support DDLs like ALTER TABLE .. RENAME TO, or
* ALTER TABLE .. SET SCHEMA etc.
*/
#define ICEBERG_INTERNAL_CATALOG_TABLE_NAME "tables_internal"
#define ICEBERG_EXTERNAL_CATALOG_TABLE_NAME "tables_external"

#define ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED PG_LAKE_ICEBERG_SCHEMA "." ICEBERG_INTERNAL_CATALOG_TABLE_NAME
#define ICEBERG_EXTERNAL_CATALOG_TABLE_QUALIFIED PG_LAKE_ICEBERG_SCHEMA "." ICEBERG_EXTERNAL_CATALOG_TABLE_NAME

extern PGDLLEXPORT void InsertInternalIcebergCatalogTable(Oid relationId, const char *metadataLocation, bool hasCustomLocation);
extern PGDLLEXPORT void InsertExternalIcebergCatalogTable(const char *catalogName, const char *tableNamespace,
														  const char *tableName, const char *metadataLocation);
extern PGDLLEXPORT void DeleteExternalIcebergCatalogTable(char *catalogName, char *schemaName, char *tableName);
extern PGDLLEXPORT void DeleteInternalIcebergCatalogTable(Oid relationId);

extern PGDLLEXPORT List *GetAllInternalIcebergRelationIds(void);
extern PGDLLEXPORT char *GetIcebergMetadataLocation(Oid relationId, bool forUpdate);
extern PGDLLEXPORT void LockIcebergPgLakeCatalogForUpdate(Oid relationId);
extern PGDLLEXPORT char *GetIcebergCatalogMetadataLocation(Oid relationId, bool forUpdate);
extern PGDLLEXPORT char *GetIcebergCatalogPreviousMetadataLocation(Oid relationId, bool forUpdate);
extern PGDLLEXPORT void UpdateExternalCatalogMetadataLocation(char *catalogName, char *schemaName, char *tableName, const char *metadataLocation,
															  const char *previousMetadataLocation);
extern PGDLLEXPORT void UpdateInternalCatalogMetadataLocation(Oid relationId, const char *metadataLocation, const char *previousMetadataLocation);
extern PGDLLEXPORT void UpdateAllInternalIcebergTablesToReadOnly(void);
extern PGDLLEXPORT char *GetIcebergDefaultLocationPrefix(void);
extern PGDLLEXPORT bool IcebergTablesCatalogExists(void);
extern PGDLLEXPORT void ErrorIfReadOnlyIcebergTable(Oid relationId);
extern PGDLLEXPORT bool RelationExistsInTheIcebergCatalog(Oid relationId);
extern PGDLLEXPORT bool HasCustomLocation(Oid relationId);
extern PGDLLEXPORT bool IsWritableIcebergTable(Oid relationId);
extern PGDLLEXPORT bool IsReadOnlyIcebergTable(Oid relationId);
