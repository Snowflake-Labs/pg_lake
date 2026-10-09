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
#include "miscadmin.h"
#include "utils/guc.h"

#include "pg_lake/pg_lake_polaris.h"

PG_MODULE_MAGIC;

void		_PG_init(void);

PG_FUNCTION_INFO_V1(pg_lake_polaris_is_suppressed);
PG_FUNCTION_INFO_V1(pg_lake_polaris_set_suppressed);

bool		PgLakePolarisSuppress = false;

char	   *PgLakePolarisRealmId = NULL;


/*
 * _PG_init registers the realm_id GUC. Loaded on first use of the extension
 * library.
 */
void
_PG_init(void)
{
	if (IsBinaryUpgrade)
		return;

	DefineCustomStringVariable("pg_lake_polaris.realm_id",
							   "Polaris realm_id used when writing to "
							   "polaris_schema.entities. Snapshotted at "
							   "register_catalog() time into "
							   "pg_lake_polaris.catalog_mapping.",
							   NULL,
							   &PgLakePolarisRealmId,
							   "POLARIS",
							   PGC_USERSET,
							   0,
							   NULL, NULL, NULL);
}


/*
 * pg_lake_polaris_is_suppressed returns the current value of the trigger
 * suppression flag. Used by both trigger functions to short-circuit during
 * a sync round-trip.
 */
Datum
pg_lake_polaris_is_suppressed(PG_FUNCTION_ARGS)
{
	PG_RETURN_BOOL(PgLakePolarisSuppress);
}


/*
 * pg_lake_polaris_set_suppressed sets the flag. Returns the previous value
 * so a SQL caller can restore it under EXCEPTION/cleanup blocks.
 */
Datum
pg_lake_polaris_set_suppressed(PG_FUNCTION_ARGS)
{
	bool		newValue = PG_GETARG_BOOL(0);
	bool		previous = PgLakePolarisSuppress;

	PgLakePolarisSuppress = newValue;
	PG_RETURN_BOOL(previous);
}
