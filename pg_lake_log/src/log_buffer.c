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
 * Shared-memory ring buffer for pg_lake_log.
 *
 * The buffer is written by any backend that fires the emit_log_hook and
 * drained by the single background worker.  A spinlock guards all accesses
 * so the critical section is as short as possible.
 */
#include "postgres.h"
#include "storage/shmem.h"

#include "pg_lake_log/log_buffer.h"

/* Shared-memory pointer; NULL until shmem_startup completes. */
LogBuffer  *PgLakeLogBuffer = NULL;


/*
 * LogBufferShmemSize returns the number of bytes needed in shared memory.
 */
Size
LogBufferShmemSize(void)
{
	return sizeof(LogBuffer);
}


/*
 * LogBufferShmemInit attaches to (or initialises) the shared-memory region.
 * Called from the shmem_startup_hook.
 */
void
LogBufferShmemInit(void)
{
	bool		found;

	PgLakeLogBuffer = (LogBuffer *)
		ShmemInitStruct("pg_lake_log buffer", sizeof(LogBuffer), &found);

	if (!found)
	{
		/* First call in this postmaster lifetime: zero everything out. */
		memset(PgLakeLogBuffer, 0, sizeof(LogBuffer));
		SpinLockInit(&PgLakeLogBuffer->lock);
	}
}


/*
 * LogBufferAppend copies *entry into the ring buffer.
 *
 * If the buffer is full the entry is silently discarded and dropped_count
 * is incremented.  The function is designed to be cheap: it holds the
 * spinlock only for the memcpy and counter bumps.
 */
void
LogBufferAppend(const LogEntry *entry)
{
	if (PgLakeLogBuffer == NULL)
		return;

	SpinLockAcquire(&PgLakeLogBuffer->lock);

	if (PgLakeLogBuffer->write_pos - PgLakeLogBuffer->read_pos >= LOG_BUFFER_SIZE)
	{
		/* Buffer full – drop the entry. */
		PgLakeLogBuffer->dropped_count++;
		SpinLockRelease(&PgLakeLogBuffer->lock);
		return;
	}

	uint64		slot = PgLakeLogBuffer->write_pos & LOG_BUFFER_MASK;

	memcpy(&PgLakeLogBuffer->entries[slot], entry, sizeof(LogEntry));
	PgLakeLogBuffer->write_pos++;

	SpinLockRelease(&PgLakeLogBuffer->lock);
}


/*
 * LogBufferDrain copies up to max_entries entries from the ring buffer into
 * out_entries and advances read_pos.  Returns the number of entries copied.
 */
int
LogBufferDrain(LogEntry *out_entries, int max_entries)
{
	int			copied = 0;

	if (PgLakeLogBuffer == NULL || max_entries <= 0)
		return 0;

	SpinLockAcquire(&PgLakeLogBuffer->lock);

	uint64		available = PgLakeLogBuffer->write_pos - PgLakeLogBuffer->read_pos;
	uint64		to_copy = Min((uint64) max_entries, available);

	for (uint64 i = 0; i < to_copy; i++)
	{
		uint64		slot = (PgLakeLogBuffer->read_pos + i) & LOG_BUFFER_MASK;

		memcpy(&out_entries[i], &PgLakeLogBuffer->entries[slot], sizeof(LogEntry));
	}

	PgLakeLogBuffer->read_pos += to_copy;
	copied = (int) to_copy;

	SpinLockRelease(&PgLakeLogBuffer->lock);

	return copied;
}
