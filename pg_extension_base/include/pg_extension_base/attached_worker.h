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

#include "postmaster/bgworker.h"
#include "storage/dsm.h"
#include "storage/shm_mq.h"
#include "utils/tuplestore.h"

/*
 * AttachedWorker contains references to a background worker that
 * is attached to the current session.
 */
typedef struct AttachedWorker
{
	/* where we keep the shared memory for the worker */
	dsm_segment *sharedMemorySegment;
	shm_mq_handle *outputQueue;

	/* tuple queue for returning query results (NULL if not returning) */
	shm_mq_handle *tupleQueue;

	/* background worker handle */
	pid_t		workerPid;
	BackgroundWorkerHandle *workerHandle;
}			AttachedWorker;

extern PGDLLEXPORT AttachedWorker * StartAttachedWorker(char *command);
extern PGDLLEXPORT AttachedWorker * StartAttachedWorkerInDatabase(char *command,
																  char *databaseName,
																  char *userName);
extern PGDLLEXPORT AttachedWorker * StartAttachedWorkerReturning(char *command);
extern PGDLLEXPORT AttachedWorker * StartAttachedWorkerInDatabaseReturning(char *command,
																		   char *databaseName,
																		   char *userName);
extern PGDLLEXPORT char *ReadFromAttachedWorker(AttachedWorker * worker, bool wait);
extern PGDLLEXPORT void ReadResultsFromAttachedWorker(AttachedWorker * worker,
													  Tuplestorestate *store,
													  TupleDesc *resultDesc);
extern PGDLLEXPORT bool IsAttachedWorkerRunning(AttachedWorker * worker);
extern PGDLLEXPORT void EndAttachedWorker(AttachedWorker * worker);
