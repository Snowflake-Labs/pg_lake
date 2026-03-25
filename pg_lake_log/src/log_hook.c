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
 * emit_log_hook implementation for pg_lake_log.
 *
 * This hook runs in every PostgreSQL backend (including bgworkers) whenever
 * a message is emitted.  It converts the ErrorData into a LogEntry and
 * appends it to the shared-memory ring buffer.  The actual write to the
 * Iceberg table is done later by the background worker.
 *
 * Guards against recursion: the hook disables itself while executing so that
 * any log messages generated inside the hook body do not trigger another
 * invocation.
 */
#include "postgres.h"
#include "miscadmin.h"
#include "utils/elog.h"
#include "utils/timestamp.h"

#include "pg_lake_log/log_buffer.h"
#include "pg_lake_log/log_worker.h"

/* Saved previous hook so we can chain. */
emit_log_hook_type PrevEmitLogHook = NULL;

/* Recursion guard: prevents re-entrancy within this hook. */
static bool InPgLakeLogHook = false;


/*
 * PgLakeLogHook is installed as emit_log_hook.
 *
 * It appends qualifying log entries to the shared-memory ring buffer.
 * The previous hook (if any) is always called regardless of whether the
 * entry is captured.
 */
void
PgLakeLogHook(ErrorData *edata)
{
	/* Always chain to the previous hook first. */
	if (PrevEmitLogHook != NULL)
		PrevEmitLogHook(edata);

	/*
	 * Skip if:
	 *   - the extension is disabled
	 *   - the buffer has not been initialised yet (early startup)
	 *   - the severity is below the configured minimum
	 *   - we are already inside this hook (recursion guard)
	 */
	if (!PgLakeLogEnabled ||
		PgLakeLogBuffer == NULL ||
		edata->elevel < PgLakeLogMinElevel ||
		InPgLakeLogHook)
		return;

	InPgLakeLogHook = true;

	PG_TRY();
	{
		LogEntry	entry;

		memset(&entry, 0, sizeof(entry));

		entry.log_time = GetCurrentTimestamp();
		entry.pid = MyProcPid;
		entry.elevel = edata->elevel;

		if (edata->message)
			strlcpy(entry.message, edata->message, sizeof(entry.message));

		if (edata->detail)
			strlcpy(entry.detail, edata->detail, sizeof(entry.detail));

		if (edata->context)
			strlcpy(entry.context, edata->context, sizeof(entry.context));

		LogBufferAppend(&entry);
	}
	PG_CATCH();
	{
		/* Swallow errors – we must not let the hook raise. */
		FlushErrorState();
	}
	PG_END_TRY();

	InPgLakeLogHook = false;
}
