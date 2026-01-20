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
#include "fmgr.h"

#include "access/tableam.h"

#include "pg_lake/access_method/access_method.h"

PG_FUNCTION_INFO_V1(pg_lake_iceberg_am_handler);
PG_FUNCTION_INFO_V1(pg_lake_ducklake_am_handler);


Datum
pg_lake_iceberg_am_handler(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s access method is a placeholder "
					"and should not be used", PG_LAKE_ICEBERG_AM)));
}


Datum
pg_lake_ducklake_am_handler(PG_FUNCTION_ARGS)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("%s access method is a placeholder "
					"and should not be used", PG_LAKE_DUCKLAKE_AM)));
}


/*
 * GetPgLakeTableTypeViaAccessMethod returns the PgLakeTableType based on the
 * given access method.
 *
 * If the access method is not set, it uses the default access method.
 * Currently, the only supported access method is pg_lake_iceberg.
 */
PgLakeTableType
GetPgLakeTableTypeViaAccessMethod(const char *accessMethod)
{
	PgLakeTableType tableType = PG_LAKE_INVALID_TABLE_TYPE;

	const char *currentAccessMethod = accessMethod;

	/* use default access method if given access method is not set */
	if (currentAccessMethod == NULL)
	{
		currentAccessMethod = default_table_access_method;
	}

	if (IsPgLakeIcebergAccessMethod(currentAccessMethod))
	{
		tableType = PG_LAKE_ICEBERG_TABLE_TYPE;
	}
	else if (IsPgLakeDucklakeAccessMethod(currentAccessMethod))
	{
		tableType = PG_LAKE_DUCKLAKE_TABLE_TYPE;
	}
	else
	{
		tableType = PG_LAKE_INVALID_TABLE_TYPE;
	}

	return tableType;
}


/*
 * IsPgLakeIcebergAccessMethod returns whether the given access method
 * name belongs to pg_lake_iceberg.
 */
bool
IsPgLakeIcebergAccessMethod(const char *accessMethod)
{
	return strcmp(accessMethod, PG_LAKE_ICEBERG_AM) == 0 ||
		strcmp(accessMethod, PG_LAKE_ICEBERG_AM_ALIAS) == 0;
}


/*
 * IsPgLakeDucklakeAccessMethod returns whether the given access method
 * name belongs to pg_lake_ducklake.
 */
bool
IsPgLakeDucklakeAccessMethod(const char *accessMethod)
{
	return strcmp(accessMethod, PG_LAKE_DUCKLAKE_AM) == 0 ||
		strcmp(accessMethod, PG_LAKE_DUCKLAKE_AM_ALIAS) == 0;
}
