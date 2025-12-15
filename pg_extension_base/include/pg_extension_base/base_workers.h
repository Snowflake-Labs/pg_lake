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

#ifndef WORKER_LAUNCHER_H
#define WORKER_LAUNCHER_H

/*
 * Simplify wrapping operations in a transaction from a background
 * worker using macros
 *
 * The macros can be used as:
 *
 * START_TRANSACTION();
 * {
 *     SPI_connect();
 *     ...
 *     SPI_finish();
 * }
 * END_TRANSACTION();
 */
#define START_TRANSACTION() \
{ \
	MemoryContext OuterContext = CurrentMemoryContext; \
	SetCurrentStatementStartTimestamp();\
	StartTransactionCommand(); \
	PG_TRY(); \
	{ \
		PushActiveSnapshot(GetTransactionSnapshot());

#define END_TRANSACTION() \
		if (ActiveSnapshotSet()) \
			PopActiveSnapshot(); \
		CommitTransactionCommand(); \
	} \
	PG_CATCH(); \
	{ \
		AbortCurrentTransaction(); \
		PG_RE_THROW(); \
	} \
	PG_END_TRY(); \
	MemoryContextSwitchTo(OuterContext); \
}

#define END_TRANSACTION_NO_THROW(ErrorLevel) \
		if (ActiveSnapshotSet()) \
			PopActiveSnapshot(); \
		CommitTransactionCommand(); \
	} \
	PG_CATCH(); \
	{ \
		MemoryContextSwitchTo(OuterContext); \
		ErrorData *edata = CopyErrorData(); \
		FlushErrorState(); \
\
		AbortCurrentTransaction(); \
\
		edata->elevel = ErrorLevel; \
		ThrowErrorData(edata); \
	} \
	PG_END_TRY(); \
	MemoryContextSwitchTo(OuterContext); \
}

/* adjusted based on signals */
extern PGDLLEXPORT volatile sig_atomic_t ReloadRequested;
extern PGDLLEXPORT volatile sig_atomic_t TerminationRequested;

/* utility function for sleeping until SIGHUP or latch */
extern PGDLLEXPORT void LightSleep(long timeoutMs);

/* whether the base worker launcher is enabled */
extern bool EnableBaseWorkerLauncher;

void		BaseWorkerSharedMemoryInit(void);
size_t		BaseWorkerSharedMemorySize(void);

extern PGDLLEXPORT int32 RegisterBaseWorker(char *workerName, Oid entryPointFunctionId,
											Oid extensionId);
extern PGDLLEXPORT int32 DeregisterBaseWorker(char *workerName);
extern PGDLLEXPORT int32 DeregisterBaseWorkerById(int32 workerId);

#endif
