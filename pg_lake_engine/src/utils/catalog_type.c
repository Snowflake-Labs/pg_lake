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

#include "postgres.h"
#include "miscadmin.h"
#include "foreign/foreign.h"
#include "catalog/pg_foreign_table.h"


#include "pg_lake/copy/copy_format.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/rel_utils.h"


/*
 * GetIcebergCatalogType returns the IcebergCatalogType for the given
 * relation ID.
 */
IcebergCatalogType
GetIcebergCatalogType(Oid relationId)
{
	if (!IsPgLakeIcebergForeignTableById(relationId))
		return NONE_CATALOG;

	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;

	bool		hasRestCatalogOption = HasRestCatalogTableOption(options);
	bool		hasObjectStoreCatalogOption = HasObjectStoreCatalogTableOption(options);
	bool		hasReadOnlyOption = HasReadOnlyOption(options);

	if (hasRestCatalogOption && hasReadOnlyOption)
	{
		return REST_CATALOG_READ_ONLY;
	}
	else if (hasRestCatalogOption && !hasReadOnlyOption)
	{
		return REST_CATALOG_READ_WRITE;
	}
	else if (hasObjectStoreCatalogOption && hasReadOnlyOption)
	{
		return OBJECT_STORE_READ_ONLY;
	}
	else if (hasObjectStoreCatalogOption && !hasReadOnlyOption)
	{
		return OBJECT_STORE_READ_WRITE;
	}
	else
	{
		return POSTGRES_CATALOG;
	}
}


/*
 * HasRestCatalogTableOption returns true if the catalog option indicates a
 * REST catalog: either the literal value 'rest' or the name of an
 * iceberg_catalog foreign server with TYPE 'rest'.
 */
bool
HasRestCatalogTableOption(List *options)
{
	char	   *catalog = GetStringOption(options, "catalog", false);

	if (catalog == NULL)
		return false;

	if (pg_strncasecmp(catalog, REST_CATALOG_NAME, strlen(catalog)) == 0)
		return true;

	return IsServerBasedRestCatalog(options);
}


/*
 * HasObjectStoreCatalogTableOption returns true if the options contain
 * catalog='object_store'.
 */
bool
HasObjectStoreCatalogTableOption(List *options)
{
	char	   *catalog = GetStringOption(options, "catalog", false);

	return catalog ? pg_strncasecmp(catalog, OBJECT_STORE_CATALOG_NAME, strlen(catalog)) == 0 : false;
}


/*
 * HasReadOnlyOption returns true if the options contain
 * catalog='read_only'.
 */
bool
HasReadOnlyOption(List *options)
{
	char	   *readOnly = GetStringOption(options, "read_only", false);

	return readOnly ? pg_strncasecmp(readOnly, "true", strlen("true")) == 0 : false;
}


/*
 * IsServerBasedRestCatalog returns true if the catalog option refers to a
 * ForeignServer created with the iceberg_catalog FDW whose TYPE is 'rest'.
 * Returns false if the catalog value is a known literal ('rest',
 * 'object_store', 'postgres') or if no matching server is found.
 */
bool
IsServerBasedRestCatalog(List *options)
{
	char	   *catalog = GetStringOption(options, "catalog", false);

	if (catalog == NULL)
		return false;

	/* Skip known literal catalog names */
	if (pg_strncasecmp(catalog, REST_CATALOG_NAME, strlen(REST_CATALOG_NAME)) == 0 ||
		pg_strncasecmp(catalog, OBJECT_STORE_CATALOG_NAME, strlen(OBJECT_STORE_CATALOG_NAME)) == 0 ||
		pg_strncasecmp(catalog, POSTGRES_CATALOG_NAME, strlen(POSTGRES_CATALOG_NAME)) == 0)
		return false;

	/* Try to look up a server with this name */
	bool		missingOK = true;
	ForeignServer *server = GetForeignServerByName(catalog, missingOK);

	if (server == NULL)
		return false;

	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	if (strcmp(fdw->fdwname, "iceberg_catalog") != 0)
		return false;

	/* Check server TYPE if set */
	if (server->servertype != NULL && *server->servertype != '\0')
		return pg_strncasecmp(server->servertype, "rest", strlen("rest")) == 0;

	/* No TYPE specified, assume rest (the only supported type for now) */
	return true;
}
