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

/*
* The allowed values for IcebergDefaultCatalog, case insensitive.
*/
#define POSTGRES_CATALOG_NAME "postgres"
#define OBJECT_STORE_CATALOG_NAME "object_store"
#define REST_CATALOG_NAME "rest"

typedef enum IcebergCatalogType
{
	NONE_CATALOG = 0,

	/* catalog='postgres' */
	POSTGRES_CATALOG = 1,

	/*
	 * catalog='rest', read_only=True Always treat like external iceberg
	 * table, read the metadata location from the external catalog and never
	 * modify.
	 */
	REST_CATALOG_READ_ONLY = 2,

	/*
	 * catalog='rest', read_only=False Treat like internal iceberg table, use
	 * all the catalog tables like lake_table.files.
	 */
	REST_CATALOG_READ_WRITE = 3,

	/*
	 * Similar to REST_CATALOG_READ_ONLY, but using an object store compatible
	 * API instead of a REST catalog server.
	 */
	OBJECT_STORE_READ_ONLY = 4,

	/*
	 * Similar to REST_CATALOG_READ_WRITE, but using an object store
	 * compatible API instead of a REST catalog server.
	 */
	OBJECT_STORE_READ_WRITE = 5
} IcebergCatalogType;

extern PGDLLEXPORT IcebergCatalogType GetIcebergCatalogType(Oid relationId);
extern PGDLLEXPORT bool HasRestCatalogTableOption(List *options);
extern PGDLLEXPORT bool HasObjectStoreCatalogTableOption(List *options);
extern PGDLLEXPORT bool HasReadOnlyOption(List *options);
