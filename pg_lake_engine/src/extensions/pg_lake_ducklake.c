/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

/*
 * Utility functions for pg_lake_ducklake extension OIDs. Mirrors the
 * pg_lake_iceberg cache: pg_lake_ducklake.so doesn't initialise this
 * itself; pg_lake_engine.so does, since pg_lake_engine is preloaded
 * for all pg_lake databases and SPI helpers from pg_extension_base
 * need ExtensionOwnerId(PgLakeDucklake) to be queryable.
 */
#include "postgres.h"

#include "pg_extension_base/extension_ids.h"
#include "pg_lake/extensions/pg_lake_ducklake.h"


typedef struct PgLakeDucklakeIds
{
	int			placeholder;	/* nothing cached today; reserved for future. */
}			PgLakeDucklakeIds;

static void ClearIds(void *ids);

static PgLakeDucklakeIds CachedIds;

CachedExtensionIds *PgLakeDucklake;


void
InitializePgLakeDucklakeIdCache(void)
{
	PgLakeDucklake = CreateExtensionIdsCache(PG_LAKE_DUCKLAKE,
											 ClearIds, &CachedIds);
}


static void
ClearIds(void *ids)
{
	Assert(ids != NULL);
	memset(ids, '\0', sizeof(PgLakeDucklakeIds));
}
