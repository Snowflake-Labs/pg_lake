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
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "commands/trigger.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/rel.h"

#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/extensions/pg_lake_table.h"

PG_FUNCTION_INFO_V1(external_catalog_modification);


/*
 * external_catalog_modification is the INSTEAD OF trigger on
 * pg_catalog.iceberg_tables that pg_lake_iceberg installs. It handles
 * only the *external*-catalog path: rows whose catalog_name is not the
 * current database. The internal-catalog path (which requires DDL on
 * pg_lake foreign tables) is owned by a sibling trigger in
 * pg_lake_table — see lake_table.internal_catalog_modification.
 *
 * Postgres fires both triggers alphabetically on each iceberg_tables
 * write; each one no-ops on the half that's not its responsibility.
 *
 * Cross-boundary catalog renames (UPDATE that flips catalog_name across
 * the current_database() boundary) are rejected here because that is
 * unambiguously not an external-catalog operation, and the symmetric
 * check in pg_lake_table would otherwise fire after we already
 * forwarded.
 */
Datum
external_catalog_modification(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple	rettuple;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as a trigger")));

	if (!TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as an INSTEAD OF trigger")));

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		rettuple = trigdata->tg_newtuple;
	else
		rettuple = trigdata->tg_trigtuple;

	bool		isnull = false;
	Datum		catalogNameDatum = heap_getattr(rettuple, 1,
												trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "catalog_name cannot be NULL");

	Datum		namespaceDatum = heap_getattr(rettuple, 2,
											  trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_namespace cannot be NULL");

	Datum		tableNameDatum = heap_getattr(rettuple, 3,
											  trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_name cannot be NULL");

	bool		metadataLocationIsNull = false;
	Datum		metadataLocationDatum = heap_getattr(rettuple, 4,
													 trigdata->tg_relation->rd_att,
													 &metadataLocationIsNull);
	bool		prevMetadataLocationIsNull = false;
	Datum		prevMetadataLocationDatum = heap_getattr(rettuple, 5,
														 trigdata->tg_relation->rd_att,
														 &prevMetadataLocationIsNull);

	char	   *catalogName = TextDatumGetCString(catalogNameDatum);
	char	   *namespaceName = TextDatumGetCString(namespaceDatum);
	char	   *tableName = TextDatumGetCString(tableNameDatum);
	char	   *metadataLocation = metadataLocationIsNull ? NULL :
		TextDatumGetCString(metadataLocationDatum);
	char	   *prevMetadataLocation = prevMetadataLocationIsNull ? NULL :
		TextDatumGetCString(prevMetadataLocationDatum);

	char	   *databaseName = get_database_name(MyDatabaseId);
	bool		isInternalCatalog = (strcmp(catalogName, databaseName) == 0);

	/*
	 * Reject cross-boundary catalog renames here so the error surfaces
	 * uniformly regardless of which sibling trigger fires first.
	 */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
	{
		Datum		oldCatalogNameDatum = heap_getattr(trigdata->tg_trigtuple, 1,
													   trigdata->tg_relation->rd_att,
													   &isnull);
		char	   *oldCatalogName = TextDatumGetCString(oldCatalogNameDatum);
		bool		wasInternalCatalog = (strcmp(oldCatalogName, databaseName) == 0);

		if (isInternalCatalog != wasInternalCatalog)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("changing catalog_name across the internal/external "
							"catalog boundary is not supported")));
	}

	/*
	 * Internal-catalog rows are pg_lake_table's responsibility — its
	 * sibling trigger does the actual work. We just no-op here, but error
	 * clearly if pg_lake_table isn't installed: otherwise the INSERT silently
	 * disappears (Postgres treats a returned rettuple from any INSTEAD OF
	 * trigger as "operation handled").
	 */
	if (isInternalCatalog)
	{
		if (!OidIsValid(get_extension_oid(PG_LAKE_TABLE, true)))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("modifying iceberg_tables for the internal "
							"catalog requires the pg_lake_table extension")));

		return PointerGetDatum(rettuple);
	}

	/* External-catalog rows: forward to the tables_external helpers. */
	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		UpdateExternalCatalogMetadataLocation(catalogName, namespaceName,
											  tableName, metadataLocation,
											  prevMetadataLocation);
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		InsertExternalIcebergCatalogTable(catalogName, namespaceName,
										  tableName, metadataLocation);
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		DeleteExternalIcebergCatalogTable(catalogName, namespaceName, tableName);
	else
		pg_unreachable();

	return PointerGetDatum(rettuple);
}
