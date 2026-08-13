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
 * Tiered-table metadata: the C-side lookup path (DESIGN.md section 11),
 * where the user's own range-partitioned heap *is* the table and a planner hook
 * adds the Iceberg tier to a query that needs it.
 *
 * The registration is read with systable_beginscan rather than through SPI,
 * because the planner hook cannot use SPI: that would re-enter the planner. The
 * catalog OIDs it needs come from the pg_extension_base ID cache, which resets
 * them on CREATE/DROP EXTENSION.
 *
 * What is cached and what is not
 * ------------------------------
 * Cached: the whole registration, including the authority boundary. None of it
 * changes often -- registration only through CREATE TABLE and DROP TABLE, the
 * boundary only through seal(), which drops a heap partition anyway.
 *
 * Not cached: the per-range sync state in timeseries.partitions. The planner does
 * not read it: it splits a query on the boundary alone, and the boundary is one
 * value on the row above. Which ranges Iceberg holds a *non-authoritative* copy
 * of is a maintenance concern, and maintenance can afford SPI.
 *
 * A query that touches no tiered table must cost as close to nothing as
 * possible, so IsTieredTable() short-circuits three times before it looks at
 * any catalog: extension not created, registry empty, relation not a
 * partitioned table.
 *
 * Nothing here checks privileges, deliberately. Whether a relation is tiered and
 * where its boundary sits is a property of the relation that every backend has to
 * agree about, whoever runs the query; a user who may not read the Iceberg tier
 * gets a permission error on it rather than a silently different answer.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "commands/trigger.h"
#include "common/hashfn.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/tuplestore.h"

#include "pg_extension_base/extension_ids.h"
#include "pg_lake_timeseries/metadata.h"


/*
 * RegistryStatus is a cached answer to "is timeseries.tables empty?", which lets
 * a query on an unregistered table skip the catalog entirely.
 */
typedef enum RegistryStatus
{
	REGISTRY_UNKNOWN,
	REGISTRY_EMPTY,
	REGISTRY_NONEMPTY
}			RegistryStatus;

/*
 * PgLakeTimeseriesIds contains OIDs from the pg_lake_timeseries extension.
 */
typedef struct PgLakeTimeseriesIds
{
	Oid			tablesId;
	Oid			tablesPkeyId;
	Oid			partitionsId;
	Oid			partitionsPkeyId;
}			PgLakeTimeseriesIds;

/*
 * TieredTableCacheEntry is a membership answer for one relation OID, positive
 * or negative. Negative entries are what keep a repeated query on an ordinary
 * table off the catalog, so they are cached too, which is why the whole hash is
 * dropped whenever the registry changes.
 */
typedef struct TieredTableCacheEntry
{
	/* hash key, must be first */
	Oid			relationId;

	bool		isRegistered;

	/* only meaningful when isRegistered */
	TieredTable tieredTable;
}			TieredTableCacheEntry;


/* cached extension IDs for pg_lake_timeseries */
static PgLakeTimeseriesIds CachedIds;

/* generic extension state */
CachedExtensionIds *PgLakeTimeseries = NULL;

/* backend-local membership cache, allocated in CacheMemoryContext */
static HTAB *TieredTableCache = NULL;

static RegistryStatus CachedRegistryStatus = REGISTRY_UNKNOWN;

static void ClearIds(void *timeseriesIds);
static void InvalidateTieredTableCache(Datum argument, Oid relationId);
static Oid	TimeseriesRelationId(Oid *cachedId, const char *relationName);
static bool LookupTieredTable(Oid relationId, TieredTable * result);


/*
 * InitializePgLakeTimeseriesMetadata sets up extension ID caching and the
 * invalidation of the membership cache. Called from _PG_init.
 */
void
InitializePgLakeTimeseriesMetadata(void)
{
	PgLakeTimeseries = CreateExtensionIdsCache("pg_lake_timeseries",
											   ClearIds, &CachedIds);

	/*
	 * DML on timeseries.tables does not invalidate anything by itself, so a
	 * statement trigger on it calls invalidate_cache_trigger(). This is where
	 * the other backends hear about it.
	 */
	CacheRegisterRelcacheCallback(InvalidateTieredTableCache, (Datum) 0);
}


/*
 * ClearIds clears the cached OIDs on CREATE/DROP EXTENSION.
 */
static void
ClearIds(void *timeseriesIds)
{
	Assert(timeseriesIds != NULL);

	memset(timeseriesIds, '\0', sizeof(PgLakeTimeseriesIds));

	/*
	 * The membership cache is keyed on relation OIDs, not on the catalog, so
	 * dropping the extension does not invalidate it by itself. Do it here:
	 * this also preserves the invariant InvalidateTieredTableCache() relies
	 * on, that a non-empty cache implies a resolved tablesId.
	 */
	ResetTieredTableCache();
}


/*
 * InvalidateTieredTableCache drops the membership cache when the registry
 * changed.
 *
 * This can run outside a transaction, so we cannot resolve the catalog OID
 * here to compare against; we compare against the one we already resolved. If
 * we never resolved it, we never populated the cache either -- ClearIds()
 * keeps that true across CREATE/DROP EXTENSION -- so there is nothing to drop.
 */
static void
InvalidateTieredTableCache(Datum argument, Oid relationId)
{
	if (relationId == InvalidOid ||
		(OidIsValid(CachedIds.tablesId) &&
		 relationId == CachedIds.tablesId))
	{
		ResetTieredTableCache();
	}
}


/*
 * ResetTieredTableCache forgets every membership answer this backend has
 * cached, including whether the registry is empty.
 */
void
ResetTieredTableCache(void)
{
	if (TieredTableCache != NULL)
	{
		hash_destroy(TieredTableCache);
		TieredTableCache = NULL;
	}

	CachedRegistryStatus = REGISTRY_UNKNOWN;
}


/*
 * TimeseriesRelationId resolves and caches the OID of a relation in the
 * timeseries schema.
 */
static Oid
TimeseriesRelationId(Oid *cachedId, const char *relationName)
{
	if (!OidIsValid(*cachedId))
	{
		EnsureExtensionExists(PgLakeTimeseries);

		Oid			namespaceId = get_namespace_oid(TIMESERIES_NSP, false);

		*cachedId = get_relname_relid(relationName, namespaceId);

		if (!OidIsValid(*cachedId))
			elog(ERROR, "could not find relation %s.%s", TIMESERIES_NSP, relationName);
	}

	return *cachedId;
}


Oid
TimeseriesTablesRelationId(void)
{
	return TimeseriesRelationId(&CachedIds.tablesId, TIMESERIES_TABLES_TABLE);
}


Oid
TimeseriesTablesPrimaryKeyId(void)
{
	return TimeseriesRelationId(&CachedIds.tablesPkeyId, TIMESERIES_TABLES_PKEY);
}


Oid
TimeseriesPartitionsRelationId(void)
{
	return TimeseriesRelationId(&CachedIds.partitionsId, TIMESERIES_PARTITIONS_TABLE);
}


Oid
TimeseriesPartitionsPrimaryKeyId(void)
{
	return TimeseriesRelationId(&CachedIds.partitionsPkeyId,
								TIMESERIES_PARTITIONS_PKEY);
}


/*
 * AnyTieredTables returns whether any table is registered at all.
 *
 * This is the short-circuit that keeps the feature free for clusters that do
 * not use it: one scan of an empty table per backend, then a cached boolean
 * until the registry changes.
 */
bool
AnyTieredTables(void)
{
	if (CachedRegistryStatus != REGISTRY_UNKNOWN)
		return CachedRegistryStatus == REGISTRY_NONEMPTY;

	Relation	tieredTables = table_open(TimeseriesTablesRelationId(), AccessShareLock);

	bool		indexOK = false;
	SysScanDesc scan = systable_beginscan(tieredTables, InvalidOid, indexOK,
										  NULL, 0, NULL);

	bool		anyRow = HeapTupleIsValid(systable_getnext(scan));

	systable_endscan(scan);
	table_close(tieredTables, AccessShareLock);

	CachedRegistryStatus = anyRow ? REGISTRY_NONEMPTY : REGISTRY_EMPTY;

	return anyRow;
}


/*
 * DeformTieredTable fills *result from one timeseries.tables tuple.
 *
 * Interval and timestamp values are copied by value, because the tuple goes away
 * with the scan that produced it.
 */
static void
DeformTieredTable(HeapTuple tuple, TupleDesc tupleDesc, TieredTable * result)
{
	Datum		values[Natts_tables];
	bool		nulls[Natts_tables];

	heap_deform_tuple(tuple, tupleDesc, values, nulls);

	/* cold_retention is the only nullable column */
	Assert(!nulls[Anum_tables_relation - 1]);
	Assert(!nulls[Anum_tables_cold_table - 1]);
	Assert(!nulls[Anum_tables_time_column - 1]);
	Assert(!nulls[Anum_tables_partition_interval - 1]);
	Assert(!nulls[Anum_tables_boundary - 1]);
	Assert(!nulls[Anum_tables_hot_retention - 1]);
	Assert(!nulls[Anum_tables_precreate_ahead - 1]);

	result->relationId = DatumGetObjectId(values[Anum_tables_relation - 1]);
	result->coldTableId = DatumGetObjectId(values[Anum_tables_cold_table - 1]);
	namestrcpy(&result->timeColumn,
			   NameStr(*DatumGetName(values[Anum_tables_time_column - 1])));
	result->partitionInterval =
		*DatumGetIntervalP(values[Anum_tables_partition_interval - 1]);
	result->boundary = DatumGetTimestampTz(values[Anum_tables_boundary - 1]);
	result->hotRetention =
		*DatumGetIntervalP(values[Anum_tables_hot_retention - 1]);

	result->coldRetentionIsNull = nulls[Anum_tables_cold_retention - 1];
	if (!result->coldRetentionIsNull)
		result->coldRetention =
			*DatumGetIntervalP(values[Anum_tables_cold_retention - 1]);

	result->precreateAhead = DatumGetInt32(values[Anum_tables_precreate_ahead - 1]);
}


/*
 * LookupTieredTable reads one timeseries.tables row, without SPI so that it is
 * safe to call from a planner hook.
 */
static bool
LookupTieredTable(Oid relationId, TieredTable * result)
{
	Relation	tieredTables = table_open(TimeseriesTablesRelationId(), AccessShareLock);

	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_tables_relation, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relationId));

	bool		indexOK = true;
	SysScanDesc scan = systable_beginscan(tieredTables,
										  TimeseriesTablesPrimaryKeyId(),
										  indexOK, NULL, 1, scanKey);

	HeapTuple	tuple = systable_getnext(scan);
	bool		isRegistered = HeapTupleIsValid(tuple);

	if (isRegistered)
		DeformTieredTable(tuple, RelationGetDescr(tieredTables), result);

	systable_endscan(scan);
	table_close(tieredTables, AccessShareLock);

	return isRegistered;
}


/*
 * AllTieredTables returns every registration, as a list of palloc'd TieredTable
 * in the current memory context.
 */
List *
AllTieredTables(void)
{
	List	   *tieredTables = NIL;

	Relation	catalog = table_open(TimeseriesTablesRelationId(), AccessShareLock);

	bool		indexOK = false;
	SysScanDesc scan = systable_beginscan(catalog, InvalidOid, indexOK, NULL, 0, NULL);
	HeapTuple	tuple = NULL;

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		TieredTable *tieredTable = palloc0(sizeof(TieredTable));

		DeformTieredTable(tuple, RelationGetDescr(catalog), tieredTable);

		tieredTables = lappend(tieredTables, tieredTable);
	}

	systable_endscan(scan);
	table_close(catalog, AccessShareLock);

	return tieredTables;
}


/*
 * TieredTableCacheLookup returns the cached membership answer for a relation,
 * reading the catalog on a miss.
 */
static TieredTableCacheEntry *
TieredTableCacheLookup(Oid relationId)
{
	if (TieredTableCache == NULL)
	{
		HASHCTL		info;

		memset(&info, 0, sizeof(info));
		info.keysize = sizeof(Oid);
		info.entrysize = sizeof(TieredTableCacheEntry);
		info.hcxt = CacheMemoryContext;

		TieredTableCache = hash_create("pg_lake_timeseries tiered tables", 32, &info,
									   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
	}

	bool		foundInCache = false;
	TieredTableCacheEntry *entry = hash_search(TieredTableCache, &relationId,
											   HASH_FIND, &foundInCache);

	if (foundInCache)
		return entry;

	/*
	 * Read the catalog before entering the relation into the hash, so that an
	 * error in the scan does not leave an uninitialised entry behind.
	 */
	TieredTable tieredTable = {0};
	bool		isRegistered = LookupTieredTable(relationId, &tieredTable);

	entry = hash_search(TieredTableCache, &relationId, HASH_ENTER, &foundInCache);
	entry->isRegistered = isRegistered;
	entry->tieredTable = tieredTable;

	return entry;
}


/*
 * IsTieredTable returns whether a relation is registered as a tiered
 * time-series table, i.e. whether the planner hook has anything to do with it.
 */
bool
IsTieredTable(Oid relationId)
{
	if (PgLakeTimeseries == NULL || !IsExtensionCreated(PgLakeTimeseries))
		return false;

	if (!AnyTieredTables())
		return false;

	/*
	 * Only a partitioned table can be registered, and this is a syscache hit
	 * where the catalog scan below is not, so it is worth filtering here.
	 */
	if (get_rel_relkind(relationId) != RELKIND_PARTITIONED_TABLE)
		return false;

	return TieredTableCacheLookup(relationId)->isRegistered;
}


/*
 * GetTieredTable fills *result with the registration of a tiered table and
 * returns whether it is registered at all.
 *
 * The caller gets a copy rather than the cached entry: entries do not survive
 * an invalidation, which can happen at any command boundary.
 */
bool
GetTieredTable(Oid relationId, TieredTable * result)
{
	if (!IsTieredTable(relationId))
		return false;

	*result = TieredTableCacheLookup(relationId)->tieredTable;

	return true;
}


/* ------------------------------------------------------------------------- *
 * SQL interface
 * ------------------------------------------------------------------------- */

PG_FUNCTION_INFO_V1(timeseries_invalidate_cache_trigger);
PG_FUNCTION_INFO_V1(timeseries_is_tiered);
PG_FUNCTION_INFO_V1(timeseries_tiered_table);
PG_FUNCTION_INFO_V1(timeseries_tiered_tables);
PG_FUNCTION_INFO_V1(timeseries_synced_ranges);

/*
 * timeseries_invalidate_cache_trigger tells every backend to forget what it
 * cached about the registry. It is the statement trigger on timeseries.tables, so
 * that any change is picked up whether it came through the C writers or by hand.
 */
Datum
timeseries_invalidate_cache_trigger(PG_FUNCTION_ARGS)
{
	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("timeseries.invalidate_cache_trigger() "
						"can only be called as a trigger")));

	CacheInvalidateRelcacheByRelid(TimeseriesTablesRelationId());

	/* AFTER STATEMENT triggers ignore the return value */
	return PointerGetDatum(NULL);
}


/*
 * timeseries_is_tiered exposes IsTieredTable to SQL. It answers from the same
 * cache the planner hook uses, so it is a probe of the lookup path rather than
 * of the catalog.
 */
Datum
timeseries_is_tiered(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);

	PG_RETURN_BOOL(IsTieredTable(relationId));
}


/*
 * TieredTableValues lays a registration out as the eight columns of
 * timeseries.tiered_tables(), in catalog attribute order.
 */
static void
TieredTableValues(TieredTable * tieredTable, Datum *values, bool *nulls)
{
	Interval   *partitionInterval = palloc(sizeof(Interval));
	Interval   *hotRetention = palloc(sizeof(Interval));

	*partitionInterval = tieredTable->partitionInterval;
	*hotRetention = tieredTable->hotRetention;

	memset(nulls, false, sizeof(bool) * Natts_tables);

	values[Anum_tables_relation - 1] = ObjectIdGetDatum(tieredTable->relationId);
	values[Anum_tables_cold_table - 1] = ObjectIdGetDatum(tieredTable->coldTableId);
	values[Anum_tables_time_column - 1] = NameGetDatum(&tieredTable->timeColumn);
	values[Anum_tables_partition_interval - 1] = IntervalPGetDatum(partitionInterval);
	values[Anum_tables_boundary - 1] = TimestampTzGetDatum(tieredTable->boundary);
	values[Anum_tables_hot_retention - 1] = IntervalPGetDatum(hotRetention);
	values[Anum_tables_precreate_ahead - 1] = Int32GetDatum(tieredTable->precreateAhead);

	if (tieredTable->coldRetentionIsNull)
	{
		values[Anum_tables_cold_retention - 1] = (Datum) 0;
		nulls[Anum_tables_cold_retention - 1] = true;
	}
	else
	{
		Interval   *coldRetention = palloc(sizeof(Interval));

		*coldRetention = tieredTable->coldRetention;
		values[Anum_tables_cold_retention - 1] = IntervalPGetDatum(coldRetention);
	}
}


/*
 * timeseries_tiered_table exposes GetTieredTable to SQL, returning NULL for an
 * unregistered relation. Its result is the registration without the relation
 * itself, which the caller passed in.
 */
Datum
timeseries_tiered_table(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	TieredTable tieredTable = {0};

	if (!GetTieredTable(relationId, &tieredTable))
		PG_RETURN_NULL();

	TupleDesc	tupleDesc;

	if (get_call_result_type(fcinfo, NULL, &tupleDesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupleDesc = BlessTupleDesc(tupleDesc);

	Datum		allValues[Natts_tables];
	bool		allNulls[Natts_tables];

	TieredTableValues(&tieredTable, allValues, allNulls);

	/* everything but the relation column, which is the argument */
	PG_RETURN_DATUM(HeapTupleGetDatum(
									  heap_form_tuple(tupleDesc,
													  &allValues[Anum_tables_cold_table - 1],
													  &allNulls[Anum_tables_cold_table - 1])));
}


/*
 * timeseries_tiered_tables returns every registration.
 *
 * This is how a table owner sees the registry: the catalog itself grants SELECT
 * to nobody but pg_monitor, and this scan applies no ACL check, for the reason at
 * the top of this file.
 */
Datum
timeseries_tiered_tables(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	List	   *tieredTables = AllTieredTables();
	ListCell   *tieredTableCell = NULL;

	foreach(tieredTableCell, tieredTables)
	{
		TieredTable *tieredTable = (TieredTable *) lfirst(tieredTableCell);
		Datum		values[Natts_tables];
		bool		nulls[Natts_tables];

		TieredTableValues(tieredTable, values, nulls);

		tuplestore_putvalues(resultInfo->setResult, resultInfo->setDesc,
							 values, nulls);
	}

	PG_RETURN_VOID();
}


/*
 * timeseries_synced_ranges returns the ranges of one table that Iceberg holds a
 * copy of, and when each was copied and sealed.
 *
 * Unlike the registration this is not cached: the planner never asks for it, and
 * maintenance asks once per pass.
 */
Datum
timeseries_synced_ranges(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	Relation	partitions = table_open(TimeseriesPartitionsRelationId(),
										AccessShareLock);

	ScanKeyData scanKey[1];

	ScanKeyInit(&scanKey[0], Anum_partitions_relation, BTEqualStrategyNumber,
				F_OIDEQ, ObjectIdGetDatum(relationId));

	bool		indexOK = true;
	SysScanDesc scan = systable_beginscan(partitions,
										  TimeseriesPartitionsPrimaryKeyId(),
										  indexOK, NULL, 1, scanKey);
	HeapTuple	tuple = NULL;

	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		Datum		row[Natts_partitions];
		bool		rowNulls[Natts_partitions];

		heap_deform_tuple(tuple, RelationGetDescr(partitions), row, rowNulls);

		/* part_start, part_end, synced_at, sealed_at */
		Datum		values[Natts_partitions - 1];
		bool		nulls[Natts_partitions - 1];

		memcpy(values, &row[Anum_partitions_part_start - 1], sizeof(values));
		memcpy(nulls, &rowNulls[Anum_partitions_part_start - 1], sizeof(nulls));

		tuplestore_putvalues(resultInfo->setResult, resultInfo->setDesc,
							 values, nulls);
	}

	systable_endscan(scan);
	table_close(partitions, AccessShareLock);

	PG_RETURN_VOID();
}
