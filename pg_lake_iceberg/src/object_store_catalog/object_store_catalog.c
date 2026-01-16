#include "postgres.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "commands/dbcommands.h"
#include "foreign/foreign.h"
#include "utils/inval.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"

#include "pg_lake/json/json_utils.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/url_encode.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/storage/local_storage.h"

char	   *ObjectStoreCatalogLocationPrefix = NULL;
char	   *ExternalObjectStorePrefix = "fromsf";
char	   *InternalObjectStorePrefix = "frompg";

PG_FUNCTION_INFO_V1(list_object_store_tables);
PG_FUNCTION_INFO_V1(trigger_object_store_catalog_generation);
PG_FUNCTION_INFO_V1(force_push_object_store_catalog);


/* pg_lake_iceberg.enable_object_store_catalog setting */
bool		EnableObjectStoreCatalog = true;


/* whether to export the catalog to object storage, always do so on start-up */
static List *InvalidatedRelationIds = NULL;

static bool CatalogNeedsExport(void);
static void PushMetadataLocationToObjectStoreCatalog(void);
static void TrackInvalidateCatalogExport(Datum argument, Oid relationId);
static char *GetExternalObjectStoreCatalogFilePath(const char *catalogName);
static char *GetInternalObjectStoreCatalogFilePath(const char *catalogName);
static bool CheckIfExternalObjectStoreCatalogExists(const char *catalogName);
static void GetObjectStoreCatalogInfoFromCatalog(Oid relationId, char **catalogName, char **catalogNamespace, char **catalogTableName);

/*
* Lists all tables registered in the given object store catalog.
* If internalTables is true, lists tables from the internal catalog,
* otherwise from the read-only external catalog.
*/
Datum
list_object_store_tables(PG_FUNCTION_ARGS)
{
	char	   *catalogName = text_to_cstring(PG_GETARG_TEXT_P(0));
	bool		internalTables = PG_GETARG_BOOL(1);

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	if (!CheckIfExternalObjectStoreCatalogExists(catalogName))
	{
		ereport(NOTICE,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("object store catalog \"%s\" does not exist", catalogName)));

		PG_RETURN_VOID();
	}

	const char *catalogPath =
		internalTables ?
		GetInternalObjectStoreCatalogFilePath(catalogName) :
		GetExternalObjectStoreCatalogFilePath(catalogName);

	char	   *catalogContent = GetTextFromURI(catalogPath);

	StringInfo	getObjectStoreTables = makeStringInfo();

	appendStringInfo(getObjectStoreTables,
					 "WITH doc AS ( "
					 "  SELECT %s::jsonb AS j "
					 ") "
					 "SELECT t->>'metadata-location', "
					 "       t->>'table-name', "
					 "       t->>'namespace' "
					 "FROM doc, jsonb_array_elements(j->'tables') AS t "
					 "WHERE true = $1 ", quote_literal_cstr(catalogContent));


	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, BOOLOID, true, false);

	SPI_START_EXTENSION_OWNER(PgLakeIceberg);

	bool		readOnly = true;

	SPI_EXECUTE(getObjectStoreTables->data, readOnly);

	for (int i = 0; i < SPI_processed; i++)
	{
		char	   *metadataLocation = SPI_getvalue(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													1);
		char	   *catalogTableName = SPI_getvalue(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													2);
		char	   *catalogNamespace = SPI_getvalue(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													3);

		Datum		values[] = {
			metadataLocation != NULL ? CStringGetTextDatum(metadataLocation) : 0,
			catalogTableName != NULL ? CStringGetTextDatum(catalogTableName) : 0,
			catalogNamespace != NULL ? CStringGetTextDatum(catalogNamespace) : 0
		};
		bool		nulls[] = {metadataLocation == NULL,
			catalogTableName == NULL,
			catalogNamespace == NULL
		};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	SPI_END();

	PG_RETURN_VOID();
}


/*
* Triggers an export of the iceberg object store catalog.
*/
Datum
trigger_object_store_catalog_generation(PG_FUNCTION_ARGS)
{
	CacheInvalidateRelcacheByRelid(IcebergTablesInternalTableId());

	PG_RETURN_VOID();
}


/*
* Force pushes the current iceberg catalog to object store.
*/
Datum
force_push_object_store_catalog(PG_FUNCTION_ARGS)
{
	PushMetadataLocationToObjectStoreCatalog();

	PG_RETURN_VOID();
}


/*
 * InitObjectStoreCatalog initializes the shared memory structures
 * used for object store catalog.
 */
void
InitObjectStoreCatalog(void)
{
	CacheRegisterRelcacheCallback(TrackInvalidateCatalogExport, (Datum) 0);
}

/*
 * TrackInvalidateCatalogExport keeps track of relation IDs that have been
 * invalidated.
 */
static void
TrackInvalidateCatalogExport(Datum argument, Oid relationId)
{
	/*
	 * We may not be in a transaction here, so we cannot check whether the
	 * invalidated relation is actually what we are looking for,
	 * IcebergTablesInternalTableId(). Instead, we just remember the
	 * invalidated relation IDs and check them later.
	 */
	MemoryContext oldcontext = MemoryContextSwitchTo(CacheMemoryContext);

	InvalidatedRelationIds = list_append_unique_oid(InvalidatedRelationIds, relationId);

	MemoryContextSwitchTo(oldcontext);
}

/*
 * ExportIcebergCatalogIfChanged efficiently determines whether
 * tables_internal might have changed via invalidations, and if
 * so it re-exports the catalog.
 */
void
ExportIcebergCatalogIfChanged(void)
{
	AcceptInvalidationMessages();

	if (!CatalogNeedsExport())
		return;

	/* can see everything for which we received invalidations */
	PushActiveSnapshot(GetLatestSnapshot());
	PushMetadataLocationToObjectStoreCatalog();
	PopActiveSnapshot();
}


/*
* CatalogNeedsExport checks if the iceberg catalog needs to be exported
* to object storage.
*/
static bool
CatalogNeedsExport(void)
{
	bool		catalogNeedsExport = false;
	ListCell   *cell;

	foreach(cell, InvalidatedRelationIds)
	{
		Oid			relationId = lfirst_oid(cell);

		if (relationId == IcebergTablesInternalTableId())
		{
			catalogNeedsExport = true;
			break;
		}
	}

	/*
	 * Always clean up the invalidated list, we are done with it now.
	 */
	if (InvalidatedRelationIds != NIL)
	{
		list_free(InvalidatedRelationIds);
		InvalidatedRelationIds = NIL;
	}

	return catalogNeedsExport;
}


/*
* GetObjectStoreCatalogInfoFromCatalog sets the catalog name, namespace and table name
* from the foreign table options for the given relationId.
*/
static void
GetObjectStoreCatalogInfoFromCatalog(Oid relationId, char **catalogName, char **catalogNamespace, char **catalogTableName)
{
	ForeignTable *foreignTable = GetForeignTable(relationId);
	List	   *options = foreignTable->options;

	*catalogName = GetStringOption(options, "catalog_name", true);

	if (!*catalogName)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("catalog_name option is missing for " OBJECT_STORE_CATALOG_NAME " catalog iceberg table %s",
						get_rel_name(relationId))));
	}

	*catalogNamespace = GetStringOption(options, "catalog_namespace", true);
	if (!*catalogNamespace)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("catalog_namespace option is missing for " OBJECT_STORE_CATALOG_NAME " catalog iceberg table %s",
						get_rel_name(relationId))));
	}

	*catalogTableName = GetStringOption(options, "catalog_table_name", true);
	if (!*catalogTableName)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("catalog_table_name option is missing for for " OBJECT_STORE_CATALOG_NAME " catalog iceberg table %s",
						get_rel_name(relationId))));
	}
}



/*
* GetMetadataLocationFromExternalObjectStoreCatalogForTable returns the metadata location
* for the given relationId by looking up the object store catalog using the catalog
* information from the foreign table options.
*/
char *
GetMetadataLocationFromExternalObjectStoreCatalogForTable(Oid relationId)
{
	char	   *catalogName = NULL;
	char	   *catalogNamespace = NULL;
	char	   *catalogTableName = NULL;

	GetObjectStoreCatalogInfoFromCatalog(relationId, &catalogName, &catalogNamespace, &catalogTableName);

	/* otherwise we should have thrown an error */
	Assert(catalogName != NULL && catalogNamespace != NULL && catalogTableName != NULL);

	return GetTableMetadataLocationFromExternalObjectStoreCatalog(catalogName, catalogNamespace, catalogTableName);
}


/*
* PushMetadataLocationToObjectStoreCatalog exports the current contents of
* tables_internal to the object store catalog file in S3.
*/
static void
PushMetadataLocationToObjectStoreCatalog(void)
{
	/*
	 * allocate before SPI so that future expansions use the current memory
	 * context
	 */
	StringInfo	objectStoreCatalogFileContent = makeStringInfo();

	/* Start JSON object */
	appendStringInfoString(objectStoreCatalogFileContent, "{");
	appendJsonKey(objectStoreCatalogFileContent, "tables");
	appendStringInfoString(objectStoreCatalogFileContent, "[");

	StringInfo	fetchObjectStoreMetadata = makeStringInfo();

	appendStringInfo(fetchObjectStoreMetadata,
					 "  SELECT "
					 "  c.metadata_location,"
					 "  lake_table.get_table_name(c.table_name),"
					 "  lake_table.get_table_schema(c.table_name)"
					 "  FROM lake_iceberg.tables_internal c"
					 "  JOIN pg_foreign_table f ON (c.table_name = f.ftrelid)"
					 "  WHERE EXISTS ("
					 "		SELECT 1"
					 "		FROM unnest(f.ftoptions) opt"
					 "		WHERE LOWER(opt) = LOWER('catalog=" OBJECT_STORE_CATALOG_NAME "')"
					 "		)");

	/*
	 * we don't use this param, but our SPI Apis rely on at least one arg, so
	 * we do true=true
	 */
	SPI_START_EXTENSION_OWNER(PgLakeIceberg);

	bool		readOnly = false;

	SPI_execute(fetchObjectStoreMetadata->data, readOnly, 0);

	for (int i = 0; i < SPI_processed; i++)
	{
		char	   *metadataLocation = SPI_getvalue(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													1);
		char	   *catalogTableName = SPI_getvalue(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													2);
		char	   *catalogNamespace = SPI_getvalue(SPI_tuptable->vals[i],
													SPI_tuptable->tupdesc,
													3);

		if (i > 0)
			appendStringInfoString(objectStoreCatalogFileContent, ",\n");

		appendStringInfoString(objectStoreCatalogFileContent, "{");

		appendJsonString(objectStoreCatalogFileContent, "metadata-location", metadataLocation);
		appendStringInfoString(objectStoreCatalogFileContent, ",");
		appendJsonString(objectStoreCatalogFileContent, "table-name", catalogTableName);
		appendStringInfoString(objectStoreCatalogFileContent, ",");
		appendJsonString(objectStoreCatalogFileContent, "namespace", catalogNamespace);
		appendStringInfoString(objectStoreCatalogFileContent, "}\n");
	}


	SPI_END();

	/* End JSON array and object */
	appendStringInfoString(objectStoreCatalogFileContent, "]\n");
	appendStringInfoString(objectStoreCatalogFileContent, "}");

	char	   *localFilePath = GenerateTempFileName("catalog_json", true);

	WriteStringToFilePath(objectStoreCatalogFileContent->data, localFilePath);

	CopyLocalFileToS3(localFilePath,
					  GetInternalObjectStoreCatalogFilePath(get_database_name(MyDatabaseId)));
}


/*
* GetTableMetadataLocationFromExternalObjectStoreCatalog returns the metadata location
* for the table with the given catalog namespace and catalog table name by
* looking up the object store catalog file in S3.
* We look at the external object store catalog, because this function is used
* for read-only object store catalog iceberg tables.
*/
char *
GetTableMetadataLocationFromExternalObjectStoreCatalog(const char *catalogName, const char *catalogNamespace, const char *catalogTableName)
{
	const char *catalogPath =
		GetExternalObjectStoreCatalogFilePath(catalogName);
	char	   *catalogContent = GetTextFromURI(catalogPath);

	StringInfo	metadataLocationStrInfo = makeStringInfo();
	StringInfo	getMetadataLocation = makeStringInfo();

	appendStringInfo(getMetadataLocation,
					 "WITH doc AS ( "
					 "  SELECT $$%s$$::jsonb AS j "
					 ") "
					 "SELECT t->>'metadata-location' AS metadata_location "
					 "FROM doc, jsonb_array_elements(j->'tables') AS t "
					 "WHERE t->>'namespace' = $1 AND t->>'table-name' = $2", catalogContent);


	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, TEXTOID, catalogNamespace, false);
	SPI_ARG_VALUE(2, TEXTOID, catalogTableName, false);

	SPI_START_EXTENSION_OWNER(PgLakeIceberg);

	bool		readOnly = true;

	SPI_EXECUTE(getMetadataLocation->data, readOnly);

	if (SPI_processed == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no table found with catalog table namespace \"%s\" and catalog table name \"%s\" in object store catalog", catalogNamespace, catalogTableName)));
	}
	else if (SPI_processed > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("multiple tables found with catalog table namespace \"%s\" and catalog table name \"%s\" in object store catalog", catalogNamespace, catalogTableName)));
	}

	bool		isNull = false;
	char	   *metadataLocation = GET_SPI_VALUE(TEXTOID, 0, 1, &isNull);

	if (!isNull)
	{
		appendStringInfoString(metadataLocationStrInfo, metadataLocation);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("no metadata location found for catalog table namespace \"%s\" and catalog table name \"%s\" in object store catalog", catalogNamespace, catalogTableName)));
	}

	SPI_END();

	return metadataLocationStrInfo->data;
}



/*
* ErrorIfExternalObjectStoreCatalogDoesNotExist checks if the object store catalog file
* exists in S3, and raises an error if it does not.
*/
void
ErrorIfExternalObjectStoreCatalogDoesNotExist(const char *catalogName)
{
	if (!CheckIfExternalObjectStoreCatalogExists(catalogName))
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg(OBJECT_STORE_CATALOG_NAME " catalog does not exist for \"%s\"", catalogName)));
	}
}


/*
* CheckIfExternalObjectStoreCatalogExists checks if the object store catalog file
* exists in S3.
*/
static bool
CheckIfExternalObjectStoreCatalogExists(const char *catalogName)
{
	char	   *filePath = GetExternalObjectStoreCatalogFilePath(catalogName);

	return RemoteFileExists(filePath);
}

/*
 * GetExternalObjectStoreCatalogFilePath returns the path to use for the catalog file
 * of the current database.
 */
static char *
GetExternalObjectStoreCatalogFilePath(const char *catalogName)
{
	const char *defaultPrefix = GetObjectStoreDefaultLocationPrefix();

	if (defaultPrefix == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_lake_iceberg.object_store_catalog_location_prefix is not set"),
				 errdetail("Set the GUC to use catalog=" OBJECT_STORE_CATALOG_NAME)));
	}

	if (ExternalObjectStorePrefix == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_lake_iceberg.external_object_store_prefix is not set"),
				 errdetail("Set the GUC to use catalog=" OBJECT_STORE_CATALOG_NAME)));
	}

	return psprintf("%s/%s/%s/catalog.json", defaultPrefix,
					ExternalObjectStorePrefix, URLEncodePath(catalogName));
}

static char *
GetInternalObjectStoreCatalogFilePath(const char *catalogName)
{
	const char *defaultPrefix = GetObjectStoreDefaultLocationPrefix();

	if (defaultPrefix == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_lake_iceberg.object_store_catalog_location_prefix is not set"),
				 errdetail("Set the GUC to use catalog=" OBJECT_STORE_CATALOG_NAME)));
	}

	if (InternalObjectStorePrefix == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_lake_iceberg.internal_object_store_prefix is not set"),
				 errdetail("Set the GUC to use catalog=" OBJECT_STORE_CATALOG_NAME)));
	}

	return psprintf("%s/%s/%s/catalog.json", defaultPrefix,
					InternalObjectStorePrefix, URLEncodePath(catalogName));
}

/*
* TriggerCatalogExportIfObjectStoreTable triggers an export of the iceberg
* object store catalog if the given relationId is an iceberg table
* that uses the object store catalog.
*/
void
TriggerCatalogExportIfObjectStoreTable(Oid relationId)
{

	if (!EnableObjectStoreCatalog)
		return;

	/* signal that we made changes and need to re-export the catalog */
	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	if (icebergCatalogType == OBJECT_STORE_READ_WRITE)
		CacheInvalidateRelcacheByRelid(IcebergTablesInternalTableId());
}


/*
 * GetObjectStoreDefaultLocationPrefix returns the default location prefix
 * for object store catalogs, removing any trailing "/" if present.
 */
const char *
GetObjectStoreDefaultLocationPrefix(void)
{
	if (ObjectStoreCatalogLocationPrefix == NULL)
	{
		return NULL;
	}

	size_t		len = strlen(ObjectStoreCatalogLocationPrefix);

	if (len > 0 && ObjectStoreCatalogLocationPrefix[len - 1] == '/')
	{
		/* remove trailing "/" */
		char	   *locationPrefixRemovedTrailingSlash = pstrdup(ObjectStoreCatalogLocationPrefix);

		locationPrefixRemovedTrailingSlash[len - 1] = '\0';

		return locationPrefixRemovedTrailingSlash;
	}

	return ObjectStoreCatalogLocationPrefix;
}
