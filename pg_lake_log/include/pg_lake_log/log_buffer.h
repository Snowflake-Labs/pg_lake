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

#pragma once

#include "postgres.h"
#include "storage/spin.h"
#include "utils/timestamp.h"

/* Maximum length for each text field in a log entry. */
#define LOG_ENTRY_MAX_MSG_LEN     1024
#define LOG_ENTRY_MAX_DETAIL_LEN   512
#define LOG_ENTRY_MAX_CONTEXT_LEN  256

/*
 * Size of the shared-memory ring buffer (number of slots).
 * Must be a power of two so modulo reduces to a bitwise AND.
 */
#define LOG_BUFFER_SIZE 8192
#define LOG_BUFFER_MASK (LOG_BUFFER_SIZE - 1)

/*
 * One log entry captured from PostgreSQL's emit_log_hook.
 */
typedef struct LogEntry
{
	TimestampTz log_time;
	int32		pid;
	int32		elevel;
	char		message[LOG_ENTRY_MAX_MSG_LEN];
	char		detail[LOG_ENTRY_MAX_DETAIL_LEN];
	char		context[LOG_ENTRY_MAX_CONTEXT_LEN];
} LogEntry;

/*
 * Shared-memory ring buffer.
 *
 * write_pos is the index of the next slot a producer will write into.
 * read_pos  is the index of the next slot the background worker will read from.
 * Both counters grow monotonically; the actual array index is (pos & MASK).
 *
 * When write_pos - read_pos == LOG_BUFFER_SIZE the buffer is full and
 * the incoming entry is dropped (dropped_count is incremented instead).
 */
typedef struct LogBuffer
{
	slock_t		lock;
	uint64		write_pos;
	uint64		read_pos;
	uint64		dropped_count;
	LogEntry	entries[LOG_BUFFER_SIZE];
} LogBuffer;

/* Pointer into shared memory, set during shmem_startup_hook. */
extern LogBuffer *PgLakeLogBuffer;

/* Shared-memory lifecycle (called from _PG_init / shmem hooks). */
Size		LogBufferShmemSize(void);
void		LogBufferShmemInit(void);

/*
 * Append one entry to the ring buffer (called from emit_log_hook).
 * Silently drops the entry when the buffer is full.
 */
void		LogBufferAppend(const LogEntry *entry);

/*
 * Copy up to max_entries entries from the ring buffer into out_entries.
 * Returns the number of entries copied and advances read_pos accordingly.
 */
int			LogBufferDrain(LogEntry *out_entries, int max_entries);
