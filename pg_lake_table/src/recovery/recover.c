/*
 * Copyright 2026 Snowflake Inc.
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
#include "access/htup_details.h"
#include "miscadmin.h"
#include "fmgr.h"

#include "catalog/namespace.h"
#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/recovery/recover.h"
#include "pg_lake/util/database_utils.h"
#include "pg_extension_base/spi_helpers.h"
#include "utils/builtins.h"


PG_FUNCTION_INFO_V1(pg_lake_finish_postgres_recovery);
PG_FUNCTION_INFO_V1(pg_lake_finish_postgres_recovery_in_db);

PgLakeFinishPostgresRecoveryHookType PgLakeFinishPostgresRecoveryHook = NULL;

static void RunAttachedCommand(char *command, char *databaseName);

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

		/*
		 * Only call the recovery procedure when it is a genuine member of the
		 * pg_lake_table extension in the target database: the join to
		 * pg_depend/pg_extension requires an extension-member dependency
		 * (deptype 'e'), which only the extension install creates. Matching
		 * on the schema and procedure name alone is not enough, because this
		 * command runs in every connectable database, including the ones
		 * where the extension was never installed.
		 *
		 * Keep every catalog reference, cast and operator
		 * pg_catalog-qualified. The command is parsed and executed in the
		 * target database, whose search_path and non-pg_catalog objects are
		 * outside our control.
		 */
		appendStringInfo(command,
						 "DO $$ BEGIN "
						 "IF EXISTS ("
						 "select 1 from pg_catalog.pg_proc p "
						 "join pg_catalog.pg_depend d on "
						 "d.classid operator(pg_catalog.=) 'pg_catalog.pg_proc'::pg_catalog.regclass "
						 "and d.objid operator(pg_catalog.=) p.oid "
						 "and d.refclassid operator(pg_catalog.=) 'pg_catalog.pg_extension'::pg_catalog.regclass "
						 "and d.deptype operator(pg_catalog.=) 'e' "
						 "join pg_catalog.pg_extension e on e.oid operator(pg_catalog.=) d.refobjid "
						 "where p.pronamespace::pg_catalog.regnamespace::pg_catalog.text operator(pg_catalog.=) %s "
						 "and p.proname operator(pg_catalog.=) %s "
						 "and e.extname operator(pg_catalog.=) %s) THEN "
						 "CALL lake_table.finish_postgres_recovery_in_db();"
						 "END IF; "
						 "END $$;",
						 quote_literal_cstr(PG_LAKE_TABLE_SCHEMA),
						 quote_literal_cstr("finish_postgres_recovery_in_db"),
						 quote_literal_cstr(PG_LAKE_TABLE));
		RunAttachedCommand(command->data, databaseName);
	}

	if (PgLakeFinishPostgresRecoveryHook != NULL)
		PgLakeFinishPostgresRecoveryHook();

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
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_execute(runAttachedQuery->data, readOnly, 0);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errmsg("failed to insert in progress file record")));
	}

	SPI_END();
}


/*
* pg_lake_finish_postgres_recovery_in_db updates all internal iceberg tables
* to read-only in the database where the function is called.
*
* It also empties the deletion queue and the in-progress file table. Every row
* in either names a file in storage that the instance this one was restored
* from still owns and cleans up itself, so VACUUM must not act on them here. A
* deletion queue row becomes reachable once a read-only table is dropped and
* its rows fall to the dropped-table drain. An in-progress row is reachable
* right away: VACUUM (ICEBERG) sweeps that table with an empty location
* prefix, which matches every row and never looks at read_only.
*
* An in-progress row is only held back from cleanup while its operation id is
* locked, and those locks lived in the source instance's lock table. Here
* every inherited row is lockable, so it looks like an aborted transaction's
* leftover even when the source is about to commit the file it names.
*/
Datum
pg_lake_finish_postgres_recovery_in_db(PG_FUNCTION_ARGS)
{
	UpdateAllInternalIcebergTablesToReadOnly();
	ClearDeletionQueue();
	ClearInProgressFiles();

	PG_RETURN_VOID();
}
