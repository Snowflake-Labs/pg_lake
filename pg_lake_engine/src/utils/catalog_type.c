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

	return IsRestCatalog(catalog);
}


/*
 * HasObjectStoreCatalogTableOption returns true if the options contain
 * catalog='object_store'.
 */
bool
HasObjectStoreCatalogTableOption(List *options)
{
	char	   *catalog = GetStringOption(options, "catalog", false);

	return catalog ? pg_strcasecmp(catalog, OBJECT_STORE_CATALOG_NAME) == 0 : false;
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
 * IsCatalogOwnedByExtension returns true if the catalog name is one of
 * the reserved built-in names: 'rest', 'object_store', or 'postgres'.
 * Comparison is case-insensitive.
 */
bool
IsCatalogOwnedByExtension(const char *catalog)
{
	return pg_strcasecmp(catalog, REST_CATALOG_NAME) == 0 ||
		pg_strcasecmp(catalog, OBJECT_STORE_CATALOG_NAME) == 0 ||
		pg_strcasecmp(catalog, POSTGRES_CATALOG_NAME) == 0;
}


/*
 * IsRestCatalog returns true if the catalog name identifies a REST catalog.
 * This includes the built-in 'rest' literal and any user-created
 * iceberg_catalog server whose TYPE is 'rest'.
 *
 * The internal built-in server names (e.g. "pg_lake_rest_catalog") are
 * deliberately rejected: they are implementation details and must not be
 * usable as catalog= option values on CREATE TABLE.  Users always type
 * the short name "rest", which is mapped to the long server name only
 * inside the resolution layer.
 */
bool
IsRestCatalog(const char *catalog)
{
	if (catalog == NULL)
		return false;

	if (pg_strcasecmp(catalog, REST_CATALOG_NAME) == 0)
		return true;

	if (IsBuiltinCatalogServerName(catalog))
		return false;

	/* Try to look up a server with this name */
	bool		missingOK = true;
	ForeignServer *server = GetForeignServerByName(catalog, missingOK);

	if (server == NULL)
		return false;

	ForeignDataWrapper *fdw = GetForeignDataWrapper(server->fdwid);

	if (strcmp(fdw->fdwname, ICEBERG_CATALOG_FDW_NAME) != 0)
		return false;

	/*
	 * Any iceberg_catalog server reaching this point is user-created, and
	 * ValidateIcebergCatalogServerDDL forces all user-created iceberg_catalog
	 * servers to TYPE 'rest'.
	 */
	Assert(pg_strcasecmp(server->servertype, REST_CATALOG_NAME) == 0);
	return true;
}


/*
 * ResolveCatalogServerName maps a user-facing catalog identifier to the
 * actual pg_foreign_server.srvname.
 *
 * For the three reserved short names ('postgres', 'object_store', 'rest')
 * the result is the corresponding pre-created built-in server name.
 * Any other input is returned unchanged (user-created server names match
 * their catalog= option value verbatim).
 *
 * The returned pointer is either a string literal or the input pointer;
 * callers must not free it.
 */
const char *
ResolveCatalogServerName(const char *catalog)
{
	if (catalog == NULL)
		return NULL;

	if (pg_strcasecmp(catalog, REST_CATALOG_NAME) == 0)
		return PG_LAKE_REST_CATALOG_SERVER_NAME;
	if (pg_strcasecmp(catalog, POSTGRES_CATALOG_NAME) == 0)
		return PG_LAKE_POSTGRES_CATALOG_SERVER_NAME;
	if (pg_strcasecmp(catalog, OBJECT_STORE_CATALOG_NAME) == 0)
		return PG_LAKE_OBJECT_STORE_CATALOG_SERVER_NAME;

	return catalog;
}


/*
 * IsBuiltinCatalogServerName returns true if the given name matches one
 * of the three pre-created built-in iceberg_catalog servers.
 *
 * Comparison is case-insensitive: both PostgreSQL-parsed identifiers
 * (already downcased by the parser unless quoted) and free-form string
 * literals supplied as catalog= option values flow through this helper,
 * and we want to reject typos like 'PG_LAKE_REST_CATALOG' just as
 * firmly as the canonical form.
 *
 * This is the long-name counterpart to IsCatalogOwnedByExtension, which
 * operates on the user-facing short names.  Used by the DDL protection
 * hook to lock down ALTER/RENAME/OWNER on the extension's structural
 * anchors and by create_table.c to reject the long names as catalog=
 * option values.
 */
bool
IsBuiltinCatalogServerName(const char *serverName)
{
	if (serverName == NULL)
		return false;

	return pg_strcasecmp(serverName, PG_LAKE_REST_CATALOG_SERVER_NAME) == 0 ||
		pg_strcasecmp(serverName, PG_LAKE_POSTGRES_CATALOG_SERVER_NAME) == 0 ||
		pg_strcasecmp(serverName, PG_LAKE_OBJECT_STORE_CATALOG_SERVER_NAME) == 0;
}
