#pragma once

#include "postgres.h"

/* crunchy_iceberg.enable_object_store_catalog setting */
extern PGDLLEXPORT bool EnableObjectStoreCatalog;

extern PGDLLEXPORT char *ObjectStoreCatalogLocationPrefix;
extern PGDLLEXPORT char *ExternalObjectStorePrefix;
extern PGDLLEXPORT char *InternalObjectStorePrefix;

/*
 * Resolved object store catalog options.  For the built-in 'object_store'
 * catalog the fields come from GUC settings.  For user-created servers
 * (CREATE SERVER ... TYPE 'object_store') the server options override the
 * GUC defaults.
 */
typedef struct ObjectStoreCatalogOptions
{
	char	   *catalog;		/* server name or "object_store" */
	char	   *locationPrefix; /* object store catalog location prefix */
	bool		readOnly;		/* server-level read_only default */
}			ObjectStoreCatalogOptions;

extern PGDLLEXPORT ObjectStoreCatalogOptions * GetObjectStoreCatalogOptionsFromCatalog(const char *catalog);

extern PGDLLEXPORT void InitObjectStoreCatalog(void);
extern PGDLLEXPORT void ExportIcebergCatalogIfChanged(void);
extern PGDLLEXPORT const char *GetObjectStoreDefaultLocationPrefix(void);
extern PGDLLEXPORT char *GetMetadataLocationFromExternalObjectStoreCatalogForTable(Oid relationId);
extern PGDLLEXPORT void ErrorIfExternalObjectStoreCatalogDoesNotExist(const char *catalogName);
extern PGDLLEXPORT char *GetTableMetadataLocationFromExternalObjectStoreCatalog(const char *catalogName, const char *catalogNamespace, const char *catalogTableName);
extern PGDLLEXPORT void TriggerCatalogExportIfObjectStoreTable(Oid relationId);
