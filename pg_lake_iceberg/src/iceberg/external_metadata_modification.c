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
#include "fmgr.h"

#include "access/htup_details.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/acl.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/util/catalog_type.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/table_type.h"
#include "pg_extension_base/spi_helpers.h"

PG_FUNCTION_INFO_V1(external_catalog_modification);

IcebergExternalWriteCallback PgLake_IcebergExternalWriteCallback = NULL;

static void HandleInternalCatalogUpdate(char *namespaceName, char *tableName,
										char *metadataLocation, char *prevMetadataLocation);


/*
* external_catalog_modification is an INSTEAD OF trigger that currently
* aims to prevent external catalog modifications to the iceberg catalog,
* namely pg_catalog.iceberg_tables view.
*
* Note that external tools such as spark or pyiceberg knows only about
* pg_catalog.iceberg_tables. The underlying pg_lake_iceberg.tables is not
* known to the external world.
*
* In the future, we might want to use this trigger to allow external tools
* to modify the iceberg catalog, and reflect those changes in the pg_lake
* catalog tables such as lake_table.data_files.
*/
Datum
external_catalog_modification(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple	rettuple;

	/* make sure it's called as a trigger at all */
	if (!CALLED_AS_TRIGGER(fcinfo))
	{
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as a trigger")));
	}

	/* make sure called as INSTEAD OF as we do it for a VIEW */
	if (!TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
	{
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as an INSTEAD OF trigger")));
	}

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		rettuple = trigdata->tg_newtuple;
	else
		rettuple = trigdata->tg_trigtuple;

	bool		isnull = false;
	Datum		catalogNameDatum = heap_getattr(rettuple, 1, trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "catalog_name cannot be NULL");

	Datum		namespaceDatum = heap_getattr(rettuple, 2, trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_namespace cannot be NULL");
	Datum		tableNameDatum = heap_getattr(rettuple, 3, trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_name cannot be NULL");

	bool		metadataLocationIsNull = false;
	Datum		metadataLocationDatum = heap_getattr(rettuple, 4, trigdata->tg_relation->rd_att, &metadataLocationIsNull);
	bool		prevMetadataLocationIsNull = false;
	Datum		prevMetadataLocationDatum = heap_getattr(rettuple, 5, trigdata->tg_relation->rd_att, &prevMetadataLocationIsNull);

	char	   *catalogName = TextDatumGetCString(catalogNameDatum);
	char	   *namespaceName = TextDatumGetCString(namespaceDatum);
	char	   *tableName = TextDatumGetCString(tableNameDatum);
	char	   *metadataLocation =
		metadataLocationIsNull ? NULL : TextDatumGetCString(metadataLocationDatum);
	char	   *prevMetadataLocation =
		prevMetadataLocationIsNull ? NULL : TextDatumGetCString(prevMetadataLocationDatum);

	char	   *databaseName = get_database_name(MyDatabaseId);
	bool		isInternalCatalog = (strcmp(catalogName, databaseName) == 0);

	/* For UPDATE, check if catalog_name is being changed */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		Datum		oldCatalogNameDatum = heap_getattr(trigdata->tg_trigtuple, 1,
													   trigdata->tg_relation->rd_att, &isnull);
		char	   *oldCatalogName = TextDatumGetCString(oldCatalogNameDatum);
		bool		wasInternalCatalog = (strcmp(oldCatalogName, databaseName) == 0);

		if (isInternalCatalog != wasInternalCatalog)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("modifying the internal catalog is currently only supported via pg_lake_iceberg tables")));
		}

		/* renaming goes through ALTER TABLE, not the catalog */
		if (isInternalCatalog)
		{
			Datum		oldNamespaceDatum = heap_getattr(trigdata->tg_trigtuple, 2,
														 trigdata->tg_relation->rd_att, &isnull);
			Datum		oldTableNameDatum = heap_getattr(trigdata->tg_trigtuple, 3,
														 trigdata->tg_relation->rd_att, &isnull);

			if (strcmp(TextDatumGetCString(oldNamespaceDatum), namespaceName) != 0 ||
				strcmp(TextDatumGetCString(oldTableNameDatum), tableName) != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot change table_namespace or table_name of a table in the %s catalog",
								databaseName)));
		}
	}

	if (isInternalCatalog)
	{
		/*
		 * For the current database catalog, only UPDATE is supported. This
		 * allows external Iceberg clients to write new metadata and then
		 * update the metadata_location, triggering a sync of the internal
		 * pg_lake catalog state.
		 */
		if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		{
			HandleInternalCatalogUpdate(namespaceName, tableName,
										metadataLocation, prevMetadataLocation);
		}
		else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("modifying the internal catalog is currently only supported via pg_lake_iceberg tables")));
		}
		else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("modifying the internal catalog is currently only supported via pg_lake_iceberg tables")));
		}
		else
		{
			pg_unreachable();
		}
	}
	else
	{
		/*
		 * Postgres only allows INSTEAD OF triggers on views. We are using
		 * this trigger to prevent external tools from modifying the iceberg
		 * catalog. But given that we use INSTEAD OF trigger on a view, we
		 * still need to handle the INSERT, UPDATE, DELETE operations on the
		 * base table.
		 */
		if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		{
			UpdateExternalCatalogMetadataLocation(catalogName, namespaceName, tableName, metadataLocation, prevMetadataLocation);
		}
		else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		{
			InsertExternalIcebergCatalogTable(catalogName, namespaceName, tableName, metadataLocation);
		}
		else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		{
			DeleteExternalIcebergCatalogTable(catalogName, namespaceName, tableName);
		}
		else
		{
			/* no other command is supported on view triggers */
			pg_unreachable();
		}
	}

	return PointerGetDatum(rettuple);
}


/*
 * HandleInternalCatalogUpdate handles UPDATE to the iceberg_tables view
 * for tables that belong to the current database catalog (i.e., internal
 * pg_lake iceberg tables).
 *
 * This allows external Iceberg clients (Spark, PyIceberg) to:
 * 1. Write new data/metadata files to object storage
 * 2. UPDATE iceberg_tables SET metadata_location = <new>, previous_metadata_location = <old>
 *
 * The function validates optimistic concurrency via previous_metadata_location,
 * updates the internal catalog, and triggers a sync of the pg_lake catalog
 * state (data files, schema, partition specs) from the new metadata.
 */
static void
HandleInternalCatalogUpdate(char *namespaceName, char *tableName,
							char *metadataLocation, char *prevMetadataLocation)
{
	if (metadataLocation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("metadata_location cannot be NULL")));

	if (prevMetadataLocation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("previous_metadata_location is required for optimistic concurrency control")));

	/* resolve namespace + table name to a relation OID */
	bool		missingOk = false;
	Oid			namespaceOid = get_namespace_oid(namespaceName, missingOk);
	Oid			relationId = get_relname_relid(tableName, namespaceOid);

	if (!OidIsValid(relationId) || !IsIcebergTable(relationId) ||
		IsExternalIcebergTable(relationId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table \"%s.%s\" does not exist",
						namespaceName, tableName)));

	if (GetIcebergCatalogType(relationId) != POSTGRES_CATALOG)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("external writes are only supported for tables in the %s catalog",
						get_database_name(MyDatabaseId))));

	ErrorIfReadOnlyIcebergTable(relationId);

	/* the new metadata can rewrite all data and the schema */
	EnsureTableOwner(relationId);

	if (PgLake_IcebergExternalWriteCallback == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("external writes require the pg_lake_table library to be loaded")));

	/*
	 * Lock the row and get the current metadata_location for optimistic
	 * concurrency validation.
	 */
	bool		forUpdate = true;
	char	   *currentMetadataLocation =
		GetIcebergMetadataLocation(relationId, forUpdate);

	if (currentMetadataLocation == NULL ||
		strcmp(currentMetadataLocation, prevMetadataLocation) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("metadata_location has been modified concurrently"),
				 errdetail("Expected previous_metadata_location \"%s\" but found \"%s\".",
						   prevMetadataLocation,
						   currentMetadataLocation ? currentMetadataLocation : "(null)")));

	/*
	 * The new metadata is read with the extension's credentials during the
	 * sync, so it must be directly inside the table's own metadata directory.
	 * The sync checks the files it references against the table location.
	 */
	const char *metadataDirectory = NULL;

	for (const char *match = strstr(currentMetadataLocation, "/metadata/");
		 match != NULL;
		 match = strstr(match + 1, "/metadata/"))
		metadataDirectory = match;

	size_t		prefixLength = metadataDirectory == NULL ? 0 :
		metadataDirectory - currentMetadataLocation + strlen("/metadata/");

	if (!IsSupportedURL(metadataLocation) ||
		metadataDirectory == NULL ||
		strncmp(metadataLocation, currentMetadataLocation, prefixLength) != 0 ||
		strchr(metadataLocation + prefixLength, '/') != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("metadata_location must be in the table's metadata directory")));

	UpdateInternalCatalogMetadataLocation(relationId, metadataLocation,
										  prevMetadataLocation);

	/* sync data files, schema and partition specs from the new metadata */
	PgLake_IcebergExternalWriteCallback(relationId, metadataLocation);
}
