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

/*
 * Every write to timeseries.tables and timeseries.partitions goes through the
 * functions in this file. They are the only reason those catalogs can grant
 * nothing to anybody: the caller needs no privilege on them, only ownership of
 * the table whose metadata it is changing.
 *
 * The pattern is the same in each: check ownership against GetUserId() *before*
 * switching, then SPI_START_EXTENSION_OWNER / SPI_END around one statement.
 *
 * These have to be C rather than plpgsql SECURITY DEFINER functions. Inside a
 * SECURITY DEFINER function current_user is the definer, and plpgsql has no way
 * to ask who called it, so the ownership check could not be made there at all.
 * Here GetUserId() is still the real caller.
 *
 * The check is not a formality. A registration is what the superuser-owned
 * maintenance worker acts on, and acting on it means dropping heap partitions and
 * overwriting an Iceberg table. Registering a relation you do not own would aim
 * that at someone else's data.
 */
#include "postgres.h"

#include "catalog/objectaddress.h"
#include "catalog/pg_class.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include "pg_extension_base/spi_helpers.h"
#include "pg_lake_timeseries/metadata.h"
#include "pg_lake_timeseries/registry.h"


static void InvalidateTieredTablePlans(Oid relationId);


PG_FUNCTION_INFO_V1(timeseries_record_sync);
PG_FUNCTION_INFO_V1(timeseries_record_seal);
PG_FUNCTION_INFO_V1(timeseries_forget_ranges);
PG_FUNCTION_INFO_V1(timeseries_forget_dropped);


/*
 * EnsureTieredTableOwner errors out unless the current user owns a relation.
 *
 * Call it before the switch to the extension owner, or it will be checking the
 * extension owner against itself.
 */
void
EnsureTieredTableOwner(Oid relationId)
{
	if (!object_ownercheck(RelationRelationId, relationId, GetUserId()))
	{
		aclcheck_error(ACLCHECK_NOT_OWNER,
					   get_relkind_objtype(get_rel_relkind(relationId)),
					   get_rel_name(relationId));
	}
}


/*
 * InvalidateTieredTablePlans forces plans over a relation to be built again.
 *
 * Whether a relation is tiered, and where its boundary is, are decided when a
 * query is planned (src/planner.c), and neither is something PostgreSQL knows a
 * plan depends on. A cached plan built before the relation was registered would
 * keep reading the heap alone, and one built before the boundary moved would
 * look for sealed rows in a partition that seal() has since dropped.
 *
 * Invalidating the relation itself is what the plan cache does watch, and it
 * reaches every backend at commit.
 */
static void
InvalidateTieredTablePlans(Oid relationId)
{
	CacheInvalidateRelcacheByRelid(relationId);
}


/*
 * RegisterTieredTable records a tiered table.
 *
 * Called from the CREATE TABLE ... USING timeseries path (src/ddl.c), which is
 * the only way a table becomes tiered: everything this row promises the planner
 * is true by construction there.
 *
 * Ownership of both tiers is required, because a registration puts them under
 * common management: the heap's partitions are dropped into the Iceberg table,
 * and rows are deleted from the Iceberg table by retention.
 */
void
RegisterTieredTable(TieredTable * tieredTable)
{
	EnsureTieredTableOwner(tieredTable->relationId);
	EnsureTieredTableOwner(tieredTable->coldTableId);

	DECLARE_SPI_ARGS(8);

	SPI_ARG_VALUE(1, OIDOID, tieredTable->relationId, false);
	SPI_ARG_VALUE(2, OIDOID, tieredTable->coldTableId, false);
	SPI_ARG_VALUE(3, NAMEOID, &tieredTable->timeColumn, false);
	SPI_ARG_DATUM(4, INTERVALOID,
				  IntervalPGetDatum(&tieredTable->partitionInterval));
	SPI_ARG_VALUE(5, TIMESTAMPTZOID, tieredTable->boundary, false);
	SPI_ARG_DATUM(6, INTERVALOID, IntervalPGetDatum(&tieredTable->hotRetention));
	if (tieredTable->coldRetentionIsNull)
	{
		SPI_ARG_NULL(7, INTERVALOID);
	}
	else
	{
		SPI_ARG_DATUM(7, INTERVALOID,
					  IntervalPGetDatum(&tieredTable->coldRetention));
	}
	SPI_ARG_VALUE(8, INT4OID, tieredTable->precreateAhead, false);

	uint64		rowsInserted = 0;

	SPI_START_EXTENSION_OWNER(PgLakeTimeseries);

	SPI_EXECUTE("INSERT INTO timeseries.tables "
				"(relation, cold_table, time_column, partition_interval, "
				" boundary, hot_retention, cold_retention, precreate_ahead) "
				"VALUES ($1, $2, $3, $4, $5, $6, $7, $8) "
				"ON CONFLICT (relation) DO NOTHING", false);

	rowsInserted = SPI_processed;

	SPI_END();

	/*
	 * ON CONFLICT DO NOTHING rather than a prior lookup, so that two sessions
	 * registering the same relation cannot both get past the check. Reaching
	 * this means an OID was reused by a relation a dropped registration still
	 * names, which forget_dropped() should have swept away.
	 */
	if (rowsInserted == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_OBJECT),
				 errmsg("%s is already a tiered table",
						get_rel_name(tieredTable->relationId))));
	}

	InvalidateTieredTablePlans(tieredTable->relationId);
}


/*
 * UpdateTieredTableSettings writes back the settings of a tiered table: the ones
 * ALTER TABLE ... SET (...) can change.
 *
 * The two tiers, the time column and the boundary are not settings. Which tables a
 * tiered table is made of and which column it is divided on are fixed when it is
 * created, and the boundary is moved only by seal(), which has proven the range is
 * in Iceberg first.
 */
void
UpdateTieredTableSettings(TieredTable * tieredTable)
{
	EnsureTieredTableOwner(tieredTable->relationId);

	DECLARE_SPI_ARGS(6);

	SPI_ARG_VALUE(1, OIDOID, tieredTable->relationId, false);
	SPI_ARG_VALUE(2, NAMEOID, &tieredTable->timeColumn, false);
	SPI_ARG_DATUM(3, INTERVALOID,
				  IntervalPGetDatum(&tieredTable->partitionInterval));
	SPI_ARG_DATUM(4, INTERVALOID, IntervalPGetDatum(&tieredTable->hotRetention));
	if (tieredTable->coldRetentionIsNull)
	{
		SPI_ARG_NULL(5, INTERVALOID);
	}
	else
	{
		SPI_ARG_DATUM(5, INTERVALOID,
					  IntervalPGetDatum(&tieredTable->coldRetention));
	}
	SPI_ARG_VALUE(6, INT4OID, tieredTable->precreateAhead, false);

	uint64		rowsUpdated = 0;

	SPI_START_EXTENSION_OWNER(PgLakeTimeseries);

	SPI_EXECUTE("UPDATE timeseries.tables "
				"SET time_column = $2, partition_interval = $3, "
				"    hot_retention = $4, cold_retention = $5, "
				"    precreate_ahead = $6 "
				"WHERE relation = $1", false);

	rowsUpdated = SPI_processed;

	SPI_END();

	if (rowsUpdated == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				 errmsg("%s is not a tiered table",
						get_rel_name(tieredTable->relationId))));
	}

	InvalidateTieredTablePlans(tieredTable->relationId);
}


/*
 * timeseries_record_sync records that Iceberg now holds a copy of a range.
 *
 * A range is synced repeatedly while it is still hot -- the copy is replaced
 * every time -- so this is an upsert on the range, not an append.
 */
Datum
timeseries_record_sync(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	TimestampTz partitionStart = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz partitionEnd = PG_GETARG_TIMESTAMPTZ(2);

	EnsureTieredTableOwner(relationId);

	DECLARE_SPI_ARGS(3);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TIMESTAMPTZOID, partitionStart, false);
	SPI_ARG_VALUE(3, TIMESTAMPTZOID, partitionEnd, false);

	SPI_START_EXTENSION_OWNER(PgLakeTimeseries);

	SPI_EXECUTE("INSERT INTO timeseries.partitions "
				"(relation, part_start, part_end, synced_at) "
				"VALUES ($1, $2, $3, pg_catalog.now()) "
				"ON CONFLICT (relation, part_start) DO UPDATE "
				"SET part_end = excluded.part_end, "
				"    synced_at = excluded.synced_at", false);

	SPI_END();

	PG_RETURN_VOID();
}


/*
 * timeseries_record_seal records that Iceberg is authoritative for a range, and
 * moves the boundary to the end of it.
 *
 * One function because it is one fact, and it has to be one transaction with the
 * DROP of the heap partition that seal() has just done: a boundary that moved
 * without the copy, or a partition dropped without the boundary moving, loses
 * rows.
 */
Datum
timeseries_record_seal(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	TimestampTz partitionStart = PG_GETARG_TIMESTAMPTZ(1);
	TimestampTz partitionEnd = PG_GETARG_TIMESTAMPTZ(2);
	TimestampTz newBoundary = PG_GETARG_TIMESTAMPTZ(3);

	EnsureTieredTableOwner(relationId);

	DECLARE_SPI_ARGS(4);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TIMESTAMPTZOID, partitionStart, false);
	SPI_ARG_VALUE(3, TIMESTAMPTZOID, partitionEnd, false);
	SPI_ARG_VALUE(4, TIMESTAMPTZOID, newBoundary, false);

	uint64		rowsUpdated = 0;

	SPI_START_EXTENSION_OWNER(PgLakeTimeseries);

	SPI_EXECUTE("INSERT INTO timeseries.partitions "
				"(relation, part_start, part_end, synced_at, sealed_at) "
				"VALUES ($1, $2, $3, pg_catalog.now(), pg_catalog.now()) "
				"ON CONFLICT (relation, part_start) DO UPDATE "
				"SET part_end = excluded.part_end, "
				"    synced_at = excluded.synced_at, "
				"    sealed_at = excluded.sealed_at", false);

	/*
	 * The boundary may only advance. It is the one value the planner reads,
	 * so moving it back would put ranges under an authority that no longer
	 * holds them.
	 */
	SPI_EXECUTE("UPDATE timeseries.tables SET boundary = $4 "
				"WHERE relation = $1 AND boundary <= $4", false);

	rowsUpdated = SPI_processed;

	SPI_END();

	if (rowsUpdated == 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("cannot move the boundary of %s backwards",
						get_rel_name(relationId))));
	}

	InvalidateTieredTablePlans(relationId);

	PG_RETURN_VOID();
}


/*
 * timeseries_forget_ranges forgets the ranges of a table that end at or before a
 * cutoff, which is what retention having removed them from Iceberg means.
 */
Datum
timeseries_forget_ranges(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	TimestampTz cutoff = PG_GETARG_TIMESTAMPTZ(1);

	EnsureTieredTableOwner(relationId);

	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TIMESTAMPTZOID, cutoff, false);

	uint64		rowsDeleted = 0;

	SPI_START_EXTENSION_OWNER(PgLakeTimeseries);

	SPI_EXECUTE("DELETE FROM timeseries.partitions "
				"WHERE relation = $1 AND part_end <= $2", false);

	rowsDeleted = SPI_processed;

	SPI_END();

	PG_RETURN_INT32((int32) rowsDeleted);
}


/*
 * timeseries_forget_dropped unregisters tables whose relations are gone.
 *
 * An event trigger calls this after every DROP in the database, so the cheap
 * path matters more than anything else here: an unused registry costs a cached
 * boolean, and a used one costs one scan of a table with as many rows as there
 * are tiered tables plus a syscache probe each. Only if that finds a dangling
 * registration is SPI used at all.
 *
 * Unlike its neighbours this checks no ownership, and needs none: a row it
 * deletes names a relation that is not in pg_class, so it describes no table and
 * nobody owns it.
 */
Datum
timeseries_forget_dropped(PG_FUNCTION_ARGS)
{
	if (PgLakeTimeseries == NULL || !IsExtensionCreated(PgLakeTimeseries) ||
		!AnyTieredTables())
	{
		PG_RETURN_INT32(0);
	}

	List	   *danglingRelationIds = NIL;
	List	   *tieredTables = AllTieredTables();
	ListCell   *tieredTableCell = NULL;

	foreach(tieredTableCell, tieredTables)
	{
		TieredTable *tieredTable = (TieredTable *) lfirst(tieredTableCell);

		if (!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tieredTable->relationId)) ||
			!SearchSysCacheExists1(RELOID, ObjectIdGetDatum(tieredTable->coldTableId)))
		{
			danglingRelationIds = lappend_oid(danglingRelationIds,
											  tieredTable->relationId);
		}
	}

	if (danglingRelationIds == NIL)
		PG_RETURN_INT32(0);

	int32		rowsDeleted = 0;
	ListCell   *relationIdCell = NULL;

	foreach(relationIdCell, danglingRelationIds)
	{
		Oid			relationId = lfirst_oid(relationIdCell);

		DECLARE_SPI_ARGS(1);

		SPI_ARG_VALUE(1, OIDOID, relationId, false);

		SPI_START_EXTENSION_OWNER(PgLakeTimeseries);

		SPI_EXECUTE("DELETE FROM timeseries.tables WHERE relation = $1", false);

		rowsDeleted += (int32) SPI_processed;

		SPI_END();
	}

	PG_RETURN_INT32(rowsDeleted);
}
