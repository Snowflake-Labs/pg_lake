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
 * SQL-callable functions exposed by the pg_lake_log extension:
 *
 *   pg_lake_log.buffer_status() → record
 *       Returns diagnostic counters from the shared-memory ring buffer.
 *
 *   pg_lake_log.flush() → bigint
 *       Drains the ring buffer and writes pending entries to the Iceberg
 *       table synchronously in the calling session (useful in tests).
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "utils/builtins.h"

#include "pg_lake_log/log_buffer.h"
#include "pg_lake_log/log_flush.h"
#include "pg_lake_log/log_worker.h"

PG_FUNCTION_INFO_V1(pg_lake_log_buffer_status);
PG_FUNCTION_INFO_V1(pg_lake_log_flush);


/*
 * pg_lake_log_buffer_status returns a composite row with four bigint columns:
 *   write_pos, read_pos, buffered_count, dropped_count
 */
Datum
pg_lake_log_buffer_status(PG_FUNCTION_ARGS)
{
	TupleDesc	tupdesc;
	Datum		values[4];
	bool		nulls[4] = {false, false, false, false};

	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	tupdesc = BlessTupleDesc(tupdesc);

	if (PgLakeLogBuffer == NULL)
	{
		for (int i = 0; i < 4; i++)
			nulls[i] = true;
	}
	else
	{
		uint64		write_pos,
					read_pos,
					dropped;

		SpinLockAcquire(&PgLakeLogBuffer->lock);
		write_pos = PgLakeLogBuffer->write_pos;
		read_pos = PgLakeLogBuffer->read_pos;
		dropped = PgLakeLogBuffer->dropped_count;
		SpinLockRelease(&PgLakeLogBuffer->lock);

		values[0] = Int64GetDatum((int64) write_pos);
		values[1] = Int64GetDatum((int64) read_pos);
		values[2] = Int64GetDatum((int64) (write_pos - read_pos));
		values[3] = Int64GetDatum((int64) dropped);
	}

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(tupdesc, values, nulls)));
}


/*
 * pg_lake_log_flush drains the ring buffer and writes all pending entries to
 * all registered Iceberg log tables.  Returns the total number of entries
 * written (count × number of tables).
 */
Datum
pg_lake_log_flush(PG_FUNCTION_ARGS)
{
	int64		total_written = 0;

	if (!PgLakeLogEnabled)
		PG_RETURN_INT64(0);

	if (PgLakeLogBuffer == NULL)
		PG_RETURN_INT64(0);

	List	   *tableOids = GetLogTableOids();

	if (tableOids == NIL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("no log tables registered in lake_log.log_tables")));

	LogEntry   *batch = (LogEntry *) palloc(sizeof(LogEntry) * PgLakeLogBatchSize);

	for (;;)
	{
		int			drained = LogBufferDrain(batch, PgLakeLogBatchSize);
		ListCell   *lc;

		if (drained == 0)
			break;

		foreach(lc, tableOids)
			InsertBatchToIceberg(batch, drained, lfirst_oid(lc));

		total_written += drained;
	}

	pfree(batch);

	PG_RETURN_INT64(total_written);
}
