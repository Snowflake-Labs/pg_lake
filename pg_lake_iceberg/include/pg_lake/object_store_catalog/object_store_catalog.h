#pragma once

#include "postgres.h"

#pragma once

/* crunchy_iceberg.enable_object_store_catalog setting */
extern PGDLLEXPORT bool EnableObjectStoreCatalog;

extern PGDLLEXPORT char *ObjectStoreCatalogLocationPrefix;
extern PGDLLEXPORT char *ExternalObjectStorePrefix;
extern PGDLLEXPORT char *InternalObjectStorePrefix;

extern PGDLLEXPORT void InitObjectStoreCatalog(void);
extern PGDLLEXPORT void ExportIcebergCatalogIfChanged(void);
extern PGDLLEXPORT const char *GetObjectStoreDefaultLocationPrefix(void);
extern PGDLLEXPORT char *GetMetadataLocationFromExternalObjectStoreCatalogForTable(Oid relationId);
extern PGDLLEXPORT void ErrorIfExternalObjectStoreCatalogDoesNotExist(const char *catalogName);
extern PGDLLEXPORT char *GetTableMetadataLocationFromExternalObjectStoreCatalog(const char *catalogName, const char *catalogNamespace, const char *catalogTableName);
extern PGDLLEXPORT void TriggerCatalogExportIfObjectStoreTable(Oid relationId);
