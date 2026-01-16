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

#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "foreign/foreign.h"
#include "utils/lsyscache.h"
#include "utils/guc.h"


char	   *IcebergDefaultLocationPrefix = NULL;
char	   *IcebergDefaultCatalog = POSTGRES_CATALOG_NAME;

static char *GetIcebergExternalMetadataLocation(Oid relationId);
static char *GetIcebergCatalogMetadataLocationInternal(Oid relationId, bool isPrevMetadata, bool forUpdate);
static char *GetIcebergCatalogColumnInternal(Oid relationId, char *columnName, bool forUpdate, bool errorIfNotFound);
static void ErrorIfSameTableExistsInExternalCatalog(Oid relationId);

/*
 * InsertExternalIcebergCatalogTable inserts a record into the Iceberg
 * table catalog.
 */
void
InsertInternalIcebergCatalogTable(Oid relationId, const char *metadataLocation, bool hasCustomLocation)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	/* first, make sure a table with the same info doesn't exist */
	ErrorIfSameTableExistsInExternalCatalog(relationId);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "insert into %s "
					 "(table_name,metadata_location, has_custom_location) "
					 "values ($1,$2,$3)", ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED);

	DECLARE_SPI_ARGS(3);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, metadataLocation, metadataLocation == NULL);
	SPI_ARG_VALUE(3, BOOLOID, hasCustomLocation, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * HasCustomLocation checks if the given iceberg table is using
 * a custom location, not the default location prefix.
 */
bool
HasCustomLocation(Oid relationId)
{
	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "SELECT has_custom_location FROM %s WHERE table_name OPERATOR(pg_catalog.=) $1",
					 ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED);

	/* add context security etc */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	bool		readOnly = true;

	SPI_EXECUTE(query->data, readOnly);

	bool		isNull = false;

	bool		hasCustomLocation = GET_SPI_VALUE(BOOLOID, 0, 1, &isNull);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	/*
	 * If it is null, it means the table is created before we introduced the
	 * has_custom_location column. So, we assume it has a custom location to
	 * be on the safe side.
	 */
	return hasCustomLocation;
}


/*
* ErrorIfSameTableExistsInExternalCatalog checks if the table with the same name
* exists in the iceberg external catalog table.
*/
static void
ErrorIfSameTableExistsInExternalCatalog(Oid relationId)
{
	const char *catalogName = get_database_name(MyDatabaseId);
	const char *schemaName = get_namespace_name(get_rel_namespace(relationId));
	const char *tableName = get_rel_name(relationId);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "SELECT 1 FROM %s WHERE catalog_name OPERATOR(pg_catalog.=) $1 AND "
					 "table_namespace OPERATOR(pg_catalog.=) $2 AND "
					 "table_name OPERATOR(pg_catalog.=) $3",
					 ICEBERG_EXTERNAL_CATALOG_TABLE_QUALIFIED);

	DECLARE_SPI_ARGS(3);

	SPI_ARG_VALUE(1, TEXTOID, catalogName, false);
	SPI_ARG_VALUE(2, TEXTOID, schemaName, false);
	SPI_ARG_VALUE(3, TEXTOID, tableName, false);

	SPI_START();

	bool		readOnly = true;

	SPI_EXECUTE(query->data, readOnly);

	bool		exists = SPI_processed > 0;

	SPI_END();

	if (exists)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("table \"%s\" already exists in the iceberg catalog", tableName)));
	}
}


/*
 * InsertExternalIcebergCatalogTable inserts a record into the Iceberg
 * table catalog.
 */
void
InsertExternalIcebergCatalogTable(const char *catalogName, const char *tableNamespace,
								  const char *tableName, const char *metadataLocation)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "insert into %s "
					 "(catalog_name,table_namespace, table_name,metadata_location) "
					 "values ($1,$2,$3,$4)", ICEBERG_EXTERNAL_CATALOG_TABLE_QUALIFIED);

	DECLARE_SPI_ARGS(4);

	SPI_ARG_VALUE(1, TEXTOID, catalogName, false);
	SPI_ARG_VALUE(2, TEXTOID, tableNamespace, false);
	SPI_ARG_VALUE(3, TEXTOID, tableName, false);
	SPI_ARG_VALUE(4, TEXTOID, metadataLocation, false);
	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	SPI_END();


	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
 * DeleteInternalIcebergCatalogTable delete a record into the Iceberg
 * table catalog.
 */
void
DeleteInternalIcebergCatalogTable(Oid relationId)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "delete from %s"
					 " WHERE table_name OPERATOR(pg_catalog.=) $1;",
					 ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED);

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}

/*
 * DeleteExternalIcebergCatalogTable delete a record into the Iceberg
 * table catalog.
 */
void
DeleteExternalIcebergCatalogTable(char *catalogName, char *schemaName, char *tableName)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);
	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "delete from %s "
					 "WHERE catalog_name OPERATOR(pg_catalog.=) $1 AND "
					 "table_namespace OPERATOR(pg_catalog.=) $2 AND "
					 "table_name OPERATOR(pg_catalog.=) $3;",
					 ICEBERG_EXTERNAL_CATALOG_TABLE_QUALIFIED);

	DECLARE_SPI_ARGS(3);
	SPI_ARG_VALUE(1, TEXTOID, catalogName, false);
	SPI_ARG_VALUE(2, TEXTOID, schemaName, false);
	SPI_ARG_VALUE(3, TEXTOID, tableName, false);


	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}

/*
 * GetAllInternalIcebergRelationIds returns all the foreign table
 * relation ids that are in pg_lake_iceberg.tables_internal.
 *
 * As the relationId in the name of the function suggests, this function
 * only returns the relation ids that are stored in Postgres. In other
 * words, it does not return the external iceberg tables that are
 * accessed via the iceberg catalog.
 */
List *
GetAllInternalIcebergRelationIds(void)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	MemoryContext oldcontext = CurrentMemoryContext;
	StringInfo	query = makeStringInfo();

	/* filter out dropped tables */
	appendStringInfo(query,
					 "select table_name FROM %s t JOIN pg_class "
					 "c ON t.table_name OPERATOR(pg_catalog.=) c.oid;",
					 ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED);

	SPI_START();

	bool		readOnly = true;

	SPI_execute(query->data, readOnly, 0);

	List	   *relationIds = NIL;

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull = false;
		Oid			relationId = GET_SPI_VALUE(OIDOID, rowIndex, 1, &isNull);

		Assert(!isNull);

		MemoryContext currentContext = MemoryContextSwitchTo(oldcontext);

		relationIds = lappend_oid(relationIds, relationId);
		MemoryContextSwitchTo(currentContext);
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return relationIds;
}


/*
 * GetIcebergMetadataLocation returns the metadata location for a iceberg table
 * from either the catalog table for internal tables or metadata for external tables.
 * Throws error if the record is not found.
 *
 * If the metadata row for the table is going to be updated, the caller should
 * pass forUpdate as true.
 */
char *
GetIcebergMetadataLocation(Oid relationId, bool forUpdate)
{
	Assert(IsIcebergTable(relationId));

	IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

	if (IsExternalIcebergTable(relationId) || catalogType == REST_CATALOG_READ_WRITE)
	{
		/*
		 * We get the metadata location from the iceberg catalog table for
		 * internal iceberg tables, except for REST_CATALOG_READ_WRITE.
		 *
		 * Writable rest catalog iceberg tables are a bit different. They are
		 * internal iceberg tables, all their metadata is stored in our
		 * catalogs, such as data files, column stats, etc. However, their
		 * metadata location is stored in the rest catalog itself. So, we need
		 * to fetch the metadata location from the rest catalog for those
		 * tables, see below.
		 */
		if (forUpdate && catalogType != REST_CATALOG_READ_WRITE)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Updating iceberg metadata is not supported for external Iceberg tables")));
		else if (forUpdate && catalogType == REST_CATALOG_READ_WRITE)
		{
			/*
			 * But we still need to acquire locks on the iceberg catalog table
			 * row for internal iceberg tables to prevent concurrent updates.
			 * So, we call GetIcebergCatalogMetadataLocation().
			 */
			LockIcebergPgLakeCatalogForUpdate(relationId);
		}

		return GetIcebergExternalMetadataLocation(relationId);
	}
	else
	{
		return GetIcebergCatalogMetadataLocation(relationId, forUpdate);
	}
}


/*
* GetIcebergCatalogMetadataLocation returns the metadata location for a table
* in the iceberg catalog table. Throws error if the record is not found.
*
* If the metadata row for the table is going to be updated, the caller should
* pass forUpdate as true.
*/
char *
GetIcebergCatalogMetadataLocation(Oid relationId, bool forUpdate)
{
	Assert(IsInternalIcebergTable(relationId));

	return GetIcebergCatalogMetadataLocationInternal(relationId, false, forUpdate);
}


/*
* LockIcebergPgLakeCatalogForUpdate acquires necessary locks on the
* iceberg catalog table row for the given relation to serialize concurrent
* updates. Note that normally GetIcebergCatalogMetadataLocation() already does
* the trick for us. This function is only needed in cases where we need to
* acquire the locks but don't have the metadata location itself, such as
* writable rest catalog iceberg tables.
*/
void
LockIcebergPgLakeCatalogForUpdate(Oid relationId)
{
	/*
	 * We don't actually need the metadata location here. We just need to
	 * acquire the necessary locks on the iceberg catalog table row to
	 * serialize concurrent updates.
	 *
	 * Note that we cannot skip reading the actual metadata location and
	 * returning it here as other callers might depend on the returned value.
	 * So, we call GetIcebergCatalogMetadataLocation() as usual.
	 */
	bool		forUpdate = true;

	GetIcebergCatalogMetadataLocationInternal(relationId, false, forUpdate);
}


/*
 * GetIcebergExternalMetadataLocation returns the metadata location for an external iceberg table.
 */
static char *
GetIcebergExternalMetadataLocation(Oid relationId)
{
	PgLakeTableProperties tableProperties = GetPgLakeTableProperties(relationId);

	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	char	   *currentMetadataPath = NULL;

	if (icebergCatalogType == REST_CATALOG_READ_ONLY || icebergCatalogType == REST_CATALOG_READ_WRITE)
	{
		currentMetadataPath = GetMetadataLocationForRestCatalogForIcebergTable(relationId);
	}
	else if (icebergCatalogType == OBJECT_STORE_READ_ONLY)
	{
		currentMetadataPath = GetMetadataLocationFromExternalObjectStoreCatalogForTable(relationId);
	}
	else if (icebergCatalogType == NONE_CATALOG && tableProperties.tableType == PG_LAKE_TABLE_TYPE && tableProperties.format == DATA_FORMAT_ICEBERG)
	{
		currentMetadataPath = GetForeignTablePath(relationId);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported iceberg external table type for relation %s",
						get_rel_name(relationId))));
	}

	return currentMetadataPath;
}


/*
* GetIcebergCatalogPreviousMetadataLocation returns the previous metadata location for a table
* in the iceberg catalog table. Returns NULL if the record is not found.
*/
char *
GetIcebergCatalogPreviousMetadataLocation(Oid relationId, bool forUpdate)
{
	return GetIcebergCatalogMetadataLocationInternal(relationId, true, forUpdate);
}

/*
* ErrorIfReadOnlyIcebergTable checks if the iceberg table is read-only and
* throws an error if it is.
*/
void
ErrorIfReadOnlyIcebergTable(Oid relationId)
{
	if (IsReadOnlyIcebergTable(relationId))
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("modifications on read-only iceberg tables are not supported")));
	}
}

/*
* GetIcebergCatalogMetadataLocationInternal returns the metadata or previous metadata
* location for a table in the iceberg catalog table.
*/
static char *
GetIcebergCatalogMetadataLocationInternal(Oid relationId, bool isPrevMetadata, bool forUpdate)
{
	char	   *columnName = isPrevMetadata ? "previous_metadata_location" : "metadata_location";
	bool		errorIfNotFound = true;

	return GetIcebergCatalogColumnInternal(relationId, columnName, forUpdate, errorIfNotFound);
}



/*
* RelationExistsInTheIcebergCatalog checks if the relation exists in the iceberg
* catalog table. This could only happen if user interferes with the catalog
* or we have a bug or external rest catalog tables.
*/
bool
RelationExistsInTheIcebergCatalog(Oid relationId)
{
	bool		forUpdate = false;
	char	   *columnName = "table_name";
	bool		errorIfNotFound = false;

	char	   *tableName =
		GetIcebergCatalogColumnInternal(relationId, columnName, forUpdate, errorIfNotFound);

	return tableName != NULL;
}



/*
* GetIcebergCatalogMetadataLocationInternal returns the metadata or previous metadata
* location for a table in the iceberg catalog table.
*/
static char *
GetIcebergCatalogColumnInternal(Oid relationId, char *columnName, bool forUpdate, bool errorIfNotFound)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	MemoryContext oldcontext = CurrentMemoryContext;
	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "select %s from " ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED
					 " where table_name OPERATOR(pg_catalog.=) $1",
					 columnName);

	if (forUpdate)
	{
		appendStringInfo(query, " FOR UPDATE");
	}

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	if (SPI_processed == 0 && errorIfNotFound)
	{
		elog(ERROR, "Iceberg table catalog record not found for relation %s.%s",
			 get_namespace_name(get_rel_namespace(relationId)),
			 get_rel_name(relationId));
	}
	else if (SPI_processed == 0)
	{
		SPI_END();
		SetUserIdAndSecContext(savedUserId, savedSecurityContext);

		return NULL;
	}

	char	   *location = SPI_getvalue(SPI_tuptable->vals[0], SPI_tuptable->tupdesc, 1);

	char	   *metadataLocation = NULL;

	if (location != NULL)
	{
		metadataLocation = MemoryContextStrdup(oldcontext, location);
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);

	return metadataLocation;
}

void
UpdateExternalCatalogMetadataLocation(char *catalogName, char *schemaName, char *tableName, const char *metadataLocation,
									  const char *previousMetadataLocation)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "update %s "
					 "set metadata_location = $1, previous_metadata_location = $2 "
					 "where catalog_name OPERATOR(pg_catalog.=) $3 AND "
					 "table_namespace OPERATOR(pg_catalog.=) $4 AND "
					 "table_name OPERATOR(pg_catalog.=) $5",
					 ICEBERG_EXTERNAL_CATALOG_TABLE_QUALIFIED);

	DECLARE_SPI_ARGS(5);
	SPI_ARG_VALUE(1, TEXTOID, metadataLocation, false);
	SPI_ARG_VALUE(2, TEXTOID, previousMetadataLocation, (previousMetadataLocation == NULL));
	SPI_ARG_VALUE(3, TEXTOID, catalogName, false);
	SPI_ARG_VALUE(4, TEXTOID, schemaName, false);
	SPI_ARG_VALUE(5, TEXTOID, tableName, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}

/*
* UpdateInternalCatalogMetadataLocation updates the metadata location for a table
* in the iceberg catalog table.
* It is used for convenience when the relationId is already known.
*/
void
UpdateInternalCatalogMetadataLocation(Oid relationId, const char *metadataLocation,
									  const char *previousMetadataLocation)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "update %s "
					 " set metadata_location = $1, previous_metadata_location = $2 "
					 " where table_name OPERATOR(pg_catalog.=) $3",
					 ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED);

	DECLARE_SPI_ARGS(3);
	SPI_ARG_VALUE(1, TEXTOID, metadataLocation, false);
	SPI_ARG_VALUE(2, TEXTOID, previousMetadataLocation, (previousMetadataLocation == NULL));
	SPI_ARG_VALUE(3, OIDOID, relationId, false);

	SPI_START();

	bool		readOnly = false;

	SPI_EXECUTE(query->data, readOnly);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}

/*
 * UpdateAllInternalIcebergTablesToReadOnlyQuery updates all the internal iceberg tables
 * to read-only.
*/
void
UpdateAllInternalIcebergTablesToReadOnly(void)
{
	/* switch to schema owner */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeIceberg), SECURITY_LOCAL_USERID_CHANGE);

	StringInfo	query = makeStringInfo();

	appendStringInfo(query,
					 "UPDATE %s SET read_only = true; ",
					 ICEBERG_INTERNAL_CATALOG_TABLE_QUALIFIED);

	SPI_START();

	bool		readOnly = false;

	SPI_execute(query->data, readOnly, 0);

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}

/*
 * GetIcebergDefaultLocationPrefix returns the default location prefix
 * for iceberg tables. Trailing slash is removed, if present.
 */
char *
GetIcebergDefaultLocationPrefix(void)
{
	if (IcebergDefaultLocationPrefix == NULL)
	{
		return NULL;
	}

	size_t		len = strlen(IcebergDefaultLocationPrefix);

	if (len > 0 && IcebergDefaultLocationPrefix[len - 1] == '/')
	{
		/* remove trailing "/" */
		char	   *locationPrefixRemovedTrailingSlash = pstrdup(IcebergDefaultLocationPrefix);

		locationPrefixRemovedTrailingSlash[len - 1] = '\0';

		return locationPrefixRemovedTrailingSlash;
	}

	return IcebergDefaultLocationPrefix;
}


/*
 * IcebergTablesCatalogExists returns whether the pg_lake_iceberg.tables
 * table exists.
 */
bool
IcebergTablesCatalogExists(void)
{
	bool		missingOk = true;

	Oid			namespaceId = get_namespace_oid(PG_LAKE_ICEBERG_SCHEMA, missingOk);

	if (namespaceId == InvalidOid)
		return false;

	return get_relname_relid(ICEBERG_INTERNAL_CATALOG_TABLE_NAME, namespaceId) != InvalidOid;
}


/*
 * IsWritableIcebergTable - check if the iceberg table is writable.
 */
bool
IsWritableIcebergTable(Oid relationId)
{
	/* only internal iceberg tables can be writable */
	if (!IsInternalIcebergTable(relationId))
		return false;

	/* check if writes are allowed to the internal iceberg table */
	bool		forUpdate = false;
	char	   *columnName = "read_only";
	bool		errorIfNotFound = false;

	char	   *readOnlyValue =
		GetIcebergCatalogColumnInternal(relationId, columnName, forUpdate, errorIfNotFound);

	if (readOnlyValue == NULL)
	{
		/* if not found, assume it is writable for backward compatibility */
		return true;
	}

	return (pg_strcasecmp(readOnlyValue, "f") == 0);
}


/*
 * IsReadOnlyIcebergTable - check if the iceberg table is read-only.
 */
bool
IsReadOnlyIcebergTable(Oid relationId)
{
	return IsIcebergTable(relationId) && !IsWritableIcebergTable(relationId);
}
