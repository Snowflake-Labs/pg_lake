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
#include "commands/defrem.h"

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/table_type.h"
#include "pg_lake/util/rel_utils.h"


/* mapping of pg_lake table type name to enum */
typedef struct PgLakeTableTypeName
{
	char	   *name;
	PgLakeTableType tableType;
}			PgLakeTableTypeName;

static PgLakeTableTypeName PgLakeTableTypeNames[] =
{
	{
		PG_LAKE_ICEBERG_SERVER_NAME, PG_LAKE_ICEBERG_TABLE_TYPE
	},
	{
		PG_LAKE_SERVER_NAME, PG_LAKE_TABLE_TYPE
	},
	{
		NULL, PG_LAKE_INVALID_TABLE_TYPE
	},
};


/*
 * PgLakeTableTypeToName - convert the PgLakeTableType to a string.
 */
const char *
PgLakeTableTypeToName(PgLakeTableType tableType)
{
	const char *name = NULL;

	for (int tableTypeIndex = 0; PgLakeTableTypeNames[tableTypeIndex].name != NULL; tableTypeIndex++)
	{
		if (PgLakeTableTypeNames[tableTypeIndex].tableType == tableType)
		{
			name = PgLakeTableTypeNames[tableTypeIndex].name;
			break;
		}
	}

	return name;
}


/*
* GetPgLakeTableType - get the type of the pg_lake table.
*/
PgLakeTableType
GetPgLakeTableType(Oid foreignTableId)
{
	char	   *serverName = GetPgLakeForeignServerName(foreignTableId);

	if (serverName == NULL)
	{
		return PG_LAKE_INVALID_TABLE_TYPE;
	}

	return GetPgLakeTableTypeViaServerName(serverName);
}


/*
 * IsWritablePgLakeTable determines whether the given
 * relation ID belongs to a writable pg_lake table.
 */
bool
IsWritablePgLakeTable(Oid relationId)
{
	if (!IsPgLakeForeignTableById(relationId))
	{
		return false;
	}

	ForeignTable *table = GetForeignTable(relationId);
	DefElem    *writableOption = GetOption(table->options, "writable");

	return writableOption != NULL && defGetBoolean(writableOption);
}


/*
* IsIcebergTable - check if the table is a managed or unmanaged iceberg table.
*/
bool
IsIcebergTable(Oid relationId)
{
	if (!IsPgLakeForeignTableById(relationId) &&
		!IsPgLakeIcebergForeignTableById(relationId))
	{
		return false;
	}

	PgLakeTableProperties properties = GetPgLakeTableProperties(relationId);

	PgLakeTableType tableType = properties.tableType;

	return tableType == PG_LAKE_ICEBERG_TABLE_TYPE ||
		(tableType == PG_LAKE_TABLE_TYPE && properties.format == DATA_FORMAT_ICEBERG);
}


/*
 * IsInternalIcebergTable - check if the table is an internal iceberg table (managed by us).
 */
bool
IsInternalIcebergTable(Oid relationId)
{
	PgLakeTableType tableType = GetPgLakeTableType(relationId);

	if (tableType != PG_LAKE_ICEBERG_TABLE_TYPE)
		return false;

	IcebergCatalogType icebergCatalogType = GetIcebergCatalogType(relationId);

	return icebergCatalogType == POSTGRES_CATALOG ||
		icebergCatalogType == REST_CATALOG_READ_WRITE ||
		icebergCatalogType == OBJECT_STORE_READ_WRITE;
}


/*
 * IsExternalIcebergTable - check if the table is an external iceberg table (managed by others).
 */
bool
IsExternalIcebergTable(Oid relationId)
{
	return IsIcebergTable(relationId) && !IsInternalIcebergTable(relationId);
}
