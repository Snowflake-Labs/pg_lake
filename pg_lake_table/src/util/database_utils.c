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

#include "access/heapam.h"
#include "access/table.h"
#include "catalog/pg_database.h"
#include "utils/builtins.h"

#include "pg_lake/util/database_utils.h"


/*
 * GetDatabaseNameList returns a list of names of all databases that allow
 * connections.
 */
List *
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
