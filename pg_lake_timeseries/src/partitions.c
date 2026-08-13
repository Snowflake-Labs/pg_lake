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
 * Which ranges of time the PostgreSQL tier holds, read from the partitions
 * themselves rather than from this extension's metadata.
 *
 * That is deliberate. The heap is the authority on its own shape: a partition
 * someone attached by hand counts, one dropped by hand stops counting, and
 * nothing this extension records can disagree with pg_class. It also means a
 * table can be marked tiered long after it was created and partitioned, with no
 * backfill of metadata.
 *
 * timeseries.partitions records something else entirely -- which ranges Iceberg
 * holds a copy of -- and the two are joined in SQL by maintenance.
 */
#include "postgres.h"

#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "funcapi.h"
#include "nodes/nodes.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/tuplestore.h"


/* columns of timeseries.heap_ranges() */
#define Anum_heap_ranges_partition 1
#define Anum_heap_ranges_part_start 2
#define Anum_heap_ranges_part_end 3
#define Natts_heap_ranges 3

static void RangeBoundValue(Oid partitionId, List *rangeDatums, Datum *value,
							bool *isNull);


PG_FUNCTION_INFO_V1(timeseries_heap_ranges);


/*
 * RangeBoundValue reads the one bound of a single-column range partition.
 *
 * MINVALUE and MAXVALUE come out as NULL: they are not timestamps, and a range
 * that is unbounded on either side has no place in a tiered table, so callers
 * filter on the NULL rather than being handed something misleading.
 */
static void
RangeBoundValue(Oid partitionId, List *rangeDatums, Datum *value, bool *isNull)
{
	PartitionRangeDatum *rangeDatum =
		castNode(PartitionRangeDatum, linitial(rangeDatums));

	if (rangeDatum->kind != PARTITION_RANGE_DATUM_VALUE)
	{
		*value = (Datum) 0;
		*isNull = true;

		return;
	}

	Const	   *bound = castNode(Const, rangeDatum->value);

	if (bound->consttype != TIMESTAMPTZOID)
	{
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("partition %s is not bounded by timestamp with time zone",
						get_rel_name(partitionId))));
	}

	*value = bound->constvalue;
	*isNull = bound->constisnull;
}


/*
 * timeseries_heap_ranges returns one row per partition of a range-partitioned
 * table: the partition and the range of time it covers.
 *
 * Partitions come back in no particular order, which is why every caller in
 * pg_lake_timeseries--3.4.sql sorts on part_start. DEFAULT partitions are left
 * out: a default partition covers whatever is left over, so it bounds no range.
 */
Datum
timeseries_heap_ranges(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);
	ReturnSetInfo *resultInfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	List	   *partitionIds = find_inheritance_children(relationId, AccessShareLock);
	ListCell   *partitionIdCell = NULL;

	foreach(partitionIdCell, partitionIds)
	{
		Oid			partitionId = lfirst_oid(partitionIdCell);

		HeapTuple	classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(partitionId));

		if (!HeapTupleIsValid(classTuple))
			continue;

		bool		boundIsNull = false;
		Datum		boundDatum = SysCacheGetAttr(RELOID, classTuple,
												 Anum_pg_class_relpartbound,
												 &boundIsNull);
		PartitionBoundSpec *bound = NULL;

		if (!boundIsNull)
		{
			bound = (PartitionBoundSpec *)
				stringToNode(TextDatumGetCString(boundDatum));
		}

		ReleaseSysCache(classTuple);

		if (bound == NULL || !IsA(bound, PartitionBoundSpec) ||
			bound->strategy != PARTITION_STRATEGY_RANGE || bound->is_default)
			continue;

		Datum		values[Natts_heap_ranges];
		bool		nulls[Natts_heap_ranges];

		memset(nulls, false, sizeof(nulls));

		values[Anum_heap_ranges_partition - 1] = ObjectIdGetDatum(partitionId);

		RangeBoundValue(partitionId, bound->lowerdatums,
						&values[Anum_heap_ranges_part_start - 1],
						&nulls[Anum_heap_ranges_part_start - 1]);
		RangeBoundValue(partitionId, bound->upperdatums,
						&values[Anum_heap_ranges_part_end - 1],
						&nulls[Anum_heap_ranges_part_end - 1]);

		tuplestore_putvalues(resultInfo->setResult, resultInfo->setDesc,
							 values, nulls);
	}

	PG_RETURN_VOID();
}
