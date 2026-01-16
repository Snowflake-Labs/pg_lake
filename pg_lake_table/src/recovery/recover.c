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

#include "access/heapam.h"
#include "catalog/pg_database.h"
#include "catalog/namespace.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_extension_base/spi_helpers.h"
#include "utils/builtins.h"


PG_FUNCTION_INFO_V1(pg_lake_finish_postgres_recovery);
PG_FUNCTION_INFO_V1(pg_lake_finish_postgres_recovery_in_db);

static void RunAttachedCommand(char *command, char *databaseName);
static List *GetDatabaseNameList(void);

/*
 * pg_lake_finish_postgres_recovery is a function that runs
 * pg_lake_finish_postgres_recovery_in_db on all databases.
 * If the extension is not installed in a database, the function
 * will silently ignore the database.
 */
Datum
pg_lake_finish_postgres_recovery(PG_FUNCTION_ARGS)
{
	List	   *databaseList = GetDatabaseNameList();
	ListCell   *cell;

	foreach(cell, databaseList)
	{
		char	   *databaseName = (char *) lfirst(cell);

		StringInfo	command = makeStringInfo();

		appendStringInfo(command,
						 "DO $$ BEGIN "
						 "IF EXISTS (select 1 from pg_catalog.pg_proc where pronamespace::regnamespace::text operator(pg_catalog.=) %s AND proname operator(pg_catalog.=) %s) THEN "
						 "CALL lake_table.finish_postgres_recovery_in_db();"
						 "END IF; "
						 "END $$;",
						 quote_literal_cstr(PG_LAKE_TABLE_SCHEMA),
						 quote_literal_cstr("finish_postgres_recovery_in_db"));
		RunAttachedCommand(command->data, databaseName);
	}

	PG_RETURN_VOID();
}


/*
* RunAttachedCommand runs the given command on the given database.
* It relies on the extension_base.run_attached_in_db run_attached.
*/
static void
RunAttachedCommand(char *command, char *databaseName)
{
	StringInfo	runAttachedQuery = makeStringInfo();

	appendStringInfo(runAttachedQuery,
					 "select * from extension_base.run_attached(%s, %s)",
					 quote_literal_cstr(command), quote_literal_cstr(databaseName));

	/* switch to schema owner, we assume callers checked permissions */
	Oid			savedUserId = InvalidOid;
	int			savedSecurityContext = 0;

	GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
	SetUserIdAndSecContext(ExtensionOwnerId(PgLakeTable),
						   SECURITY_LOCAL_USERID_CHANGE);

	SPI_START();

	bool		readOnly = false;

	SPI_execute(runAttachedQuery->data, readOnly, 0);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errmsg("failed to insert in progress file record")));
	}

	SPI_END();

	SetUserIdAndSecContext(savedUserId, savedSecurityContext);
}


/*
* pg_lake_finish_postgres_recovery_in_db updates all internal iceberg tables
* to read-only in the database where the function is called.
*/
Datum
pg_lake_finish_postgres_recovery_in_db(PG_FUNCTION_ARGS)
{
	UpdateAllInternalIcebergTablesToReadOnly();

	PG_RETURN_VOID();
}


/*
 * GetDatabaseList returns a list of all databases.
 */
static List *
GetDatabaseNameList(void)
{
	List	   *databaseList = NIL;
	HeapTuple	databaseTuple;

	Relation	pgDatabaseRelation = table_open(DatabaseRelationId, AccessShareLock);
	TableScanDesc scan = table_beginscan_catalog(pgDatabaseRelation, 0, NULL);

	while (HeapTupleIsValid(databaseTuple = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database databaseRecord = (Form_pg_database) GETSTRUCT(databaseTuple);

		/* if connection not possible, skip */
		if (databaseRecord->datistemplate || !databaseRecord->datallowconn)
			continue;

		char	   *dbName = pstrdup(NameStr(databaseRecord->datname));

		databaseList = lappend(databaseList, dbName);
	}

	table_endscan(scan);
	table_close(pgDatabaseRelation, AccessShareLock);

	return databaseList;
}
