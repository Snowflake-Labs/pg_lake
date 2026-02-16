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
 * Functions for starting commands in a background worker that is attached
 * to the current session, and will die if the session terminates. It is
 * primarily useful for running commands in other databases and for
 * autonomous subtransactions.
 *
 * Partly derived from pg_cron.
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/xact.h"
#include "catalog/pg_authid.h"
#include "commands/dbcommands.h"
#include "pg_extension_base/attached_worker.h"
#include "pg_extension_base/pg_compat.h"
#include "libpq/pqformat.h"
#include "libpq/pqmq.h"
#include "nodes/pg_list.h"
#include "parser/analyze.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shm_mq.h"
#include "storage/shm_toc.h"
#include "storage/shmem.h"
#include "tcop/pquery.h"
#include "tcop/tcopprot.h"
#include "tcop/utility.h"
#include "executor/tqueue.h"
#include "utils/acl.h"
#include "utils/backend_status.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/portal.h"
#include "utils/ps_status.h"
#include "utils/snapmgr.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timeout.h"
#include "utils/typcache.h"

#define QUEUE_SIZE ((Size) 65536)
#define ATTACHED_WORKER_MAGIC			0x52004040
#define ATTACHED_WORKER_KEY_DATABASE	0
#define ATTACHED_WORKER_KEY_USERNAME	1
#define ATTACHED_WORKER_KEY_COMMAND		2
#define ATTACHED_WORKER_KEY_QUEUE		3
#define ATTACHED_WORKER_KEY_FLAGS		4
#define ATTACHED_WORKER_KEY_TUPLE_QUEUE	5
#define ATTACHED_WORKER_NKEYS			6

#define ATTACHED_WORKER_FLAG_RETURN_RESULTS	0x01

/* SQL-callable functions */
PG_FUNCTION_INFO_V1(pg_extension_base_run_attached_worker);
PG_FUNCTION_INFO_V1(pg_extension_base_run_attached_worker_returning);

/* library entry points */
extern PGDLLEXPORT void AttachedWorkerMain(Datum mainArg);

/* internal functions */
static AttachedWorker * StartAttachedWorkerInternal(char *command, char *databaseName,
													char *userName, uint32 flags);
static void RunAttachedWorker(char *command, char *databaseName, char *userName, ReturnSetInfo *rsinfo);
static void RunAttachedWorkerReturning(char *command, char *databaseName,
									   char *userName, ReturnSetInfo *rsinfo);
static char *ProcessProtocolMessages(shm_mq_handle *queue, bool nowait,
									 TupleDesc *resultDesc);
static void ValidateWorkerTupleDesc(TupleDesc workerDesc, TupleDesc expectedDesc);
static void ExecuteSqlString(const char *sql, shm_mq_handle *tupleQueue);

#if PG_VERSION_NUM < 170000
static bool UserOidIsLoginRole(Oid userOid);
#endif


/*
 * pg_extension_base_run_attached_worker runs a command in an attached worker.
 */
Datum
pg_extension_base_run_attached_worker(PG_FUNCTION_ARGS)
{
	char	   *command = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *databaseName = text_to_cstring(PG_GETARG_TEXT_P(1));

	/* we set, but do not yet populate the tuple store */
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	Oid			userOid = GetUserId();
	char	   *currentDatabaseName = get_database_name(MyDatabaseId);

	bool		sameDatabase = strcmp(currentDatabaseName, databaseName) == 0;

	if (!sameDatabase && !superuser_arg(userOid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superusers can run attached workers in other databases")));

	char	   *userName = GetUserNameFromId(userOid, false);

	/* versions < 17 cannot use NOLOGIN roles */
#if PG_VERSION_NUM < 170000
	if (!UserOidIsLoginRole(userOid))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("failed to start a background worker as role %s "
							   "because it has NOLOGIN", userName)));
#endif

	RunAttachedWorker(command, databaseName, userName, rsinfo);

	PG_RETURN_VOID();
}


/*
* Work-horse function for running an attached worker for pg_extension_base_run_attached_worker.
*/
static void
RunAttachedWorker(char *command, char *databaseName, char *userName, ReturnSetInfo *rsinfo)
{
	AttachedWorker *worker = StartAttachedWorkerInDatabase(command, databaseName, userName);
	int			commandId = 0;

	PG_TRY();
	{
		do
		{
			CHECK_FOR_INTERRUPTS();

			bool		wait = true;
			char	   *commandTag = ReadFromAttachedWorker(worker, wait);

			if (commandTag == NULL)
				break;

			bool		nulls[] = {false, false};
			Datum		values[] = {
				Int32GetDatum(commandId),
				CStringGetTextDatum(commandTag)
			};

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
			commandId++;
		}
		while (IsAttachedWorkerRunning(worker));
	}
	PG_FINALLY();
	{
		EndAttachedWorker(worker);
	}
	PG_END_TRY();

}


/*
 * pg_extension_base_run_attached_worker_returning runs a command in an attached
 * worker and returns the query results.
 */
Datum
pg_extension_base_run_attached_worker_returning(PG_FUNCTION_ARGS)
{
	char	   *command = text_to_cstring(PG_GETARG_TEXT_P(0));
	char	   *databaseName = text_to_cstring(PG_GETARG_TEXT_P(1));

	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

	Oid			userOid = GetUserId();
	char	   *currentDatabaseName = get_database_name(MyDatabaseId);

	bool		sameDatabase = strcmp(currentDatabaseName, databaseName) == 0;

	if (!sameDatabase && !superuser_arg(userOid))
		ereport(ERROR,
				(errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
				 errmsg("only superusers can run attached workers in other databases")));

	char	   *userName = GetUserNameFromId(userOid, false);

#if PG_VERSION_NUM < 170000
	if (!UserOidIsLoginRole(userOid))
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("failed to start a background worker as role %s "
							   "because it has NOLOGIN", userName)));
#endif

	RunAttachedWorkerReturning(command, databaseName, userName, rsinfo);

	PG_RETURN_VOID();
}


/*
 * RunAttachedWorkerReturning runs an attached worker and returns query results
 * in the tuple store.
 */
static void
RunAttachedWorkerReturning(char *command, char *databaseName, char *userName,
						   ReturnSetInfo *rsinfo)
{
	AttachedWorker *worker = StartAttachedWorkerInDatabaseReturning(command,
																	databaseName,
																	userName);

	PG_TRY();
	{
		/*
		 * Pass the expected TupleDesc so ReadResultsFromAttachedWorker uses
		 * the caller's expected types for parsing text values into Datums.
		 */
		TupleDesc	resultDesc = rsinfo->setDesc;

		ReadResultsFromAttachedWorker(worker, rsinfo->setResult, &resultDesc);
	}
	PG_FINALLY();
	{
		EndAttachedWorker(worker);
	}
	PG_END_TRY();
}


/*
 * StartAttachedWorker runs a command in the current database as the current
 * user.
 */
AttachedWorker *
StartAttachedWorker(char *command)
{
	char	   *databaseName = get_database_name(MyDatabaseId);
	char	   *userName = GetUserNameFromId(GetUserId(), true);

	return StartAttachedWorkerInternal(command, databaseName, userName, 0);
}


/*
 * StartAttachedWorkerInDatabase runs a command in the given database as the given
 * user.
 */
AttachedWorker *
StartAttachedWorkerInDatabase(char *command, char *databaseName, char *userName)
{
	return StartAttachedWorkerInternal(command, databaseName, userName, 0);
}


/*
 * StartAttachedWorkerReturning runs a command in the current database as the
 * current user and enables returning query results.
 */
AttachedWorker *
StartAttachedWorkerReturning(char *command)
{
	char	   *databaseName = get_database_name(MyDatabaseId);
	char	   *userName = GetUserNameFromId(GetUserId(), true);

	return StartAttachedWorkerInternal(command, databaseName, userName,
									   ATTACHED_WORKER_FLAG_RETURN_RESULTS);
}


/*
 * StartAttachedWorkerInDatabaseReturning runs a command in the given database
 * as the given user and enables returning query results.
 */
AttachedWorker *
StartAttachedWorkerInDatabaseReturning(char *command, char *databaseName, char *userName)
{
	return StartAttachedWorkerInternal(command, databaseName, userName,
									   ATTACHED_WORKER_FLAG_RETURN_RESULTS);
}


/*
 * StartAttachedWorkerInternal is the common implementation for starting an
 * attached worker with the given flags.
 */
static AttachedWorker *
StartAttachedWorkerInternal(char *command, char *databaseName, char *userName,
							uint32 flags)
{
	/* store the worker state */
	AttachedWorker *attachedWorker = (AttachedWorker *) palloc0(sizeof(AttachedWorker));

	/* estimate size of the background worker input */
	shm_toc_estimator sharedMemoryEstimator;

	shm_toc_initialize_estimator(&sharedMemoryEstimator);
	shm_toc_estimate_chunk(&sharedMemoryEstimator, strlen(databaseName) + 1);
	shm_toc_estimate_chunk(&sharedMemoryEstimator, strlen(userName) + 1);
	shm_toc_estimate_chunk(&sharedMemoryEstimator, strlen(command) + 1);
	shm_toc_estimate_chunk(&sharedMemoryEstimator, QUEUE_SIZE);
	shm_toc_estimate_chunk(&sharedMemoryEstimator, sizeof(uint32));
	if (flags & ATTACHED_WORKER_FLAG_RETURN_RESULTS)
		shm_toc_estimate_chunk(&sharedMemoryEstimator, QUEUE_SIZE);
	shm_toc_estimate_keys(&sharedMemoryEstimator, ATTACHED_WORKER_NKEYS);
	Size		segmentSize = shm_toc_estimate(&sharedMemoryEstimator);

	/* create the shared memory segment */
	dsm_segment *seg = dsm_create(segmentSize, 0);

	/*
	 * copy the database name, user name and command into the shared memory
	 * segment
	 */
	shm_toc    *toc = shm_toc_create(ATTACHED_WORKER_MAGIC, dsm_segment_address(seg), segmentSize);

	char	   *databaseInShm = shm_toc_allocate(toc, strlen(databaseName) + 1);

	strcpy(databaseInShm, databaseName);
	shm_toc_insert(toc, ATTACHED_WORKER_KEY_DATABASE, databaseInShm);

	char	   *userInShm = shm_toc_allocate(toc, strlen(userName) + 1);

	strcpy(userInShm, userName);
	shm_toc_insert(toc, ATTACHED_WORKER_KEY_USERNAME, userInShm);

	char	   *commandInShm = shm_toc_allocate(toc, strlen(command) + 1);

	strcpy(commandInShm, command);
	shm_toc_insert(toc, ATTACHED_WORKER_KEY_COMMAND, commandInShm);

	/* set up the shared memory queue for responses */
	shm_mq	   *outputQueue = shm_mq_create(shm_toc_allocate(toc, QUEUE_SIZE), QUEUE_SIZE);

	shm_toc_insert(toc, ATTACHED_WORKER_KEY_QUEUE, outputQueue);
	shm_mq_set_receiver(outputQueue, MyProc);

	shm_mq_handle *outputQueueHandle = shm_mq_attach(outputQueue, seg, NULL);

	/* store the flags in shared memory */
	uint32	   *flagsInShm = shm_toc_allocate(toc, sizeof(uint32));

	*flagsInShm = flags;
	shm_toc_insert(toc, ATTACHED_WORKER_KEY_FLAGS, flagsInShm);

	/* set up a tuple queue for returning query results */
	shm_mq_handle *tupleQueueHandle = NULL;

	if (flags & ATTACHED_WORKER_FLAG_RETURN_RESULTS)
	{
		shm_mq	   *tupleQueue = shm_mq_create(shm_toc_allocate(toc, QUEUE_SIZE), QUEUE_SIZE);

		shm_toc_insert(toc, ATTACHED_WORKER_KEY_TUPLE_QUEUE, tupleQueue);
		shm_mq_set_receiver(tupleQueue, MyProc);
		tupleQueueHandle = shm_mq_attach(tupleQueue, seg, NULL);
	}

	/* create the background worker */
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(BackgroundWorker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	sprintf(worker.bgw_library_name, "pg_extension_base");
	sprintf(worker.bgw_function_name, "AttachedWorkerMain");
	snprintf(worker.bgw_type, BGW_MAXLEN, "pg_extension_base attached worker");
	snprintf(worker.bgw_name, BGW_MAXLEN, "pg_extension_base attached worker");
	worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(seg));
	worker.bgw_notify_pid = MyProcPid;

	BackgroundWorkerHandle *workerHandle;
	bool		registered = RegisterDynamicBackgroundWorker(&worker, &workerHandle);

	if (!registered)
	{
		dsm_detach(seg);

		ereport(ERROR, (errcode(ERRCODE_CONFIGURATION_LIMIT_EXCEEDED),
						errmsg("out of background worker slots"),
						errhint("You might need to increase max_worker_processes.")));
	}

	/* wait for the background worker to start */
	pid_t		pid;
	BgwHandleStatus status = WaitForBackgroundWorkerStartup(workerHandle, &pid);

	if (status != BGWH_STARTED && status != BGWH_STOPPED)
	{
		dsm_detach(seg);

		ereport(ERROR, (errmsg("could not start background worker")));
	}

	attachedWorker->workerPid = pid;
	attachedWorker->workerHandle = workerHandle;
	attachedWorker->sharedMemorySegment = seg;
	attachedWorker->outputQueue = outputQueueHandle;
	attachedWorker->tupleQueue = tupleQueueHandle;

	return attachedWorker;
}


/*
 * ProcessProtocolMessages reads messages from the given shared memory queue.
 *
 * Errors and notices are always re-thrown. If a CommandComplete ('C') message
 * is found, the command tag is returned immediately. If resultDesc is
 * non-NULL and *resultDesc is NULL, a RowDescription ('T') message is parsed
 * to build a TupleDesc. All other message types are ignored.
 *
 * When nowait is true the function returns NULL as soon as the queue has
 * no more messages. When nowait is false the function blocks until a
 * message arrives.
 *
 * Returns the command tag on CommandComplete, or NULL when the queue
 * is exhausted or detached.
 */
static char *
ProcessProtocolMessages(shm_mq_handle *queue, bool nowait,
						TupleDesc *resultDesc)
{
	StringInfoData msg;

	initStringInfo(&msg);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		Size		messageLength;
		void	   *data;
		shm_mq_result res = shm_mq_receive(queue, &messageLength,
										   &data, nowait);

		if (res != SHM_MQ_SUCCESS)
			break;

		resetStringInfo(&msg);
		enlargeStringInfo(&msg, messageLength);
		msg.len = messageLength;
		memcpy(msg.data, data, messageLength);
		msg.data[messageLength] = '\0';

		char		msgtype = pq_getmsgbyte(&msg);

		switch (msgtype)
		{
			case 'N':
			case 'E':
				{
					ErrorData	edata;

					pq_parse_errornotice(&msg, &edata);

					/*
					 * Cap FATAL/PANIC to ERROR so the parent session survives
					 * even if the worker hits a fatal error.
					 */
					if (edata.elevel >= FATAL)
						edata.elevel = ERROR;

					ThrowErrorData(&edata);
					break;
				}
			case 'C':
				{
					const char *tag = pq_getmsgstring(&msg);

					return pstrdup(tag);
				}
			case 'T':
				{
					if (resultDesc != NULL && *resultDesc == NULL)
					{
						int			numFields = pq_getmsgint(&msg, 2);
						TupleDesc	tupleDesc = CreateTemplateTupleDesc(numFields);

						for (int i = 0; i < numFields; i++)
						{
							const char *fieldName = pq_getmsgstring(&msg);

							 /* tableOid */ pq_getmsgint(&msg, 4);
							 /* attrNum */ pq_getmsgint(&msg, 2);
							Oid			typeOid = pq_getmsgint(&msg, 4);

							 /* typeLen */ pq_getmsgint(&msg, 2);
							int32		typeMod = pq_getmsgint(&msg, 4);

							 /* formatCode */ pq_getmsgint(&msg, 2);

							TupleDescInitEntry(tupleDesc, i + 1,
											   fieldName,
											   typeOid,
											   typeMod, 0);
						}

						*resultDesc = tupleDesc;
					}
					break;
				}
			case 'A':
			case 'D':
			case 'G':
			case 'H':
			case 'W':
			case 'Z':
				break;
			default:
				elog(WARNING, "unknown message type: %c (%zu bytes)",
					 msg.data[0], messageLength);
				break;
		}
	}

	pfree(msg.data);
	return NULL;
}


/*
 * ReadFromAttachedWorker reads from the response queue of an attached
 * worker until reaching query completion or error.
 */
char *
ReadFromAttachedWorker(AttachedWorker * worker, bool wait)
{
	return ProcessProtocolMessages(worker->outputQueue, !wait, NULL);
}


/*
 * ValidateWorkerTupleDesc checks that the TupleDesc received from the worker
 * matches the caller's expected TupleDesc in column count and types.
 *
 * This is critical because DestTupleQueue transfers binary tuple data that
 * would be misinterpreted if the types differ.
 */
static void
ValidateWorkerTupleDesc(TupleDesc workerDesc, TupleDesc expectedDesc)
{
	if (workerDesc->natts != expectedDesc->natts)
		ereport(ERROR,
				(errcode(ERRCODE_DATATYPE_MISMATCH),
				 errmsg("query returned %d column(s) but %d were expected",
						workerDesc->natts, expectedDesc->natts)));

	for (int i = 0; i < workerDesc->natts; i++)
	{
		Oid			workerType = TupleDescAttr(workerDesc, i)->atttypid;
		Oid			expectedType = TupleDescAttr(expectedDesc, i)->atttypid;

		if (workerType != expectedType)
			ereport(ERROR,
					(errcode(ERRCODE_DATATYPE_MISMATCH),
					 errmsg("column %d has type %s in the query but type %s was expected",
							i + 1,
							format_type_be(workerType),
							format_type_be(expectedType))));
	}
}


/*
 * ReadResultsFromAttachedWorker reads query results from an attached worker
 * that was started with the RETURN_RESULTS flag. Tuples arrive as
 * MinimalTuples through a dedicated tuple queue (DestTupleQueue), while
 * errors, notices, and RowDescription flow through the protocol queue.
 *
 * If *resultDesc is non-NULL on entry, it is used as the TupleDesc for
 * the tuplestore and the worker's RowDescription is validated against it.
 * If *resultDesc is NULL, a TupleDesc is built from the RowDescription.
 *
 * On return, *resultDesc is set to the TupleDesc that was used.
 */
void
ReadResultsFromAttachedWorker(AttachedWorker * worker, Tuplestorestate *store,
							  TupleDesc *resultDesc)
{
	TupleDesc	tupleDesc = *resultDesc;
	TupleQueueReader *reader = CreateTupleQueueReader(worker->tupleQueue);

	for (;;)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Drain protocol queue to pick up RowDescription, errors, notices.
		 * workerDesc is set to a new TupleDesc when a RowDescription arrives.
		 */
		TupleDesc	workerDesc = NULL;

		ProcessProtocolMessages(worker->outputQueue, true, &workerDesc);

		if (workerDesc != NULL)
		{
			if (tupleDesc == NULL)
			{
				/*
				 * No expected TupleDesc was provided — this is used by
				 * internal C callers that do not know the result shape
				 * upfront.  Adopt the worker's descriptor.
				 */
				tupleDesc = workerDesc;
				*resultDesc = workerDesc;
			}
			else
			{
				ValidateWorkerTupleDesc(workerDesc, tupleDesc);
				FreeTupleDesc(workerDesc);
			}
		}

		/* Read the next tuple from the tuple queue */
		bool		done = false;
		MinimalTuple tuple = TupleQueueReaderNext(reader, true, &done);

		if (done)
			break;

		if (tuple != NULL)
		{
			if (tupleDesc == NULL)
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("received tuple data without a TupleDesc")));

			tuplestore_puttuple(store, heap_tuple_from_minimal_tuple(tuple));
		}
	}

	DestroyTupleQueueReader(reader);

	/* Final drain to catch any trailing errors or notices */
	ProcessProtocolMessages(worker->outputQueue, true, NULL);
}


/*
 * IsAttachedWorkerRunning determines whether the attached worker is
 * still running.
 */
bool
IsAttachedWorkerRunning(AttachedWorker * worker)
{
	pid_t		pid;

	return GetBackgroundWorkerPid(worker->workerHandle, &pid) != BGWH_STOPPED;
}


/*
 * EndAttachedWorker should be called when done with an attached
 * worker to clean up the resources.
 */
void
EndAttachedWorker(AttachedWorker * worker)
{
	if (worker->sharedMemorySegment == NULL)
		return;

	if (IsAttachedWorkerRunning(worker))
	{
		TerminateBackgroundWorker(worker->workerHandle);
		WaitForBackgroundWorkerShutdown(worker->workerHandle);
	}

	dsm_detach(worker->sharedMemorySegment);
	worker->sharedMemorySegment = NULL;
}


/*
 * AttachedWorkerMain is the main entry point for an attached worker.
 */
void
AttachedWorkerMain(Datum mainArg)
{
	/* handle SIGTERM like regular backend */
	pqsignal(SIGTERM, die);
	BackgroundWorkerUnblockSignals();

	/* Set up a memory context and resource owner. */
	Assert(CurrentResourceOwner == NULL);
	CurrentResourceOwner = ResourceOwnerCreate(NULL, "pg_extension_base");
	CurrentMemoryContext = AllocSetContextCreate(TopMemoryContext,
												 "pg_extension_base worker",
												 ALLOCSET_DEFAULT_MINSIZE,
												 ALLOCSET_DEFAULT_INITSIZE,
												 ALLOCSET_DEFAULT_MAXSIZE);

	/* Attach to the dynamic shared memory segment */
	dsm_segment *seg = dsm_attach(DatumGetInt32(mainArg));

	if (seg == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("unable to map dynamic shared memory segment")));

	shm_toc    *toc = shm_toc_attach(ATTACHED_WORKER_MAGIC, dsm_segment_address(seg));

	if (toc == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("bad magic number in dynamic shared memory segment")));

	/* Get the input arguments */
	char	   *databaseName = shm_toc_lookup(toc, ATTACHED_WORKER_KEY_DATABASE, false);
	char	   *userName = shm_toc_lookup(toc, ATTACHED_WORKER_KEY_USERNAME, false);
	char	   *command = shm_toc_lookup(toc, ATTACHED_WORKER_KEY_COMMAND, false);
	shm_mq	   *messageQueue = shm_toc_lookup(toc, ATTACHED_WORKER_KEY_QUEUE, false);

	/* Read flags (missing_ok for backward compatibility) */
	uint32	   *flagsPtr = shm_toc_lookup(toc, ATTACHED_WORKER_KEY_FLAGS, true);
	bool		returnResults = (flagsPtr != NULL &&
								 (*flagsPtr & ATTACHED_WORKER_FLAG_RETURN_RESULTS) != 0);

	/* Attach to the response queue (protocol messages: errors, command tags) */
	shm_mq_set_sender(messageQueue, MyProc);
	shm_mq_handle *outputQueue = shm_mq_attach(messageQueue, seg, NULL);

	pq_redirect_to_shm_mq(seg, outputQueue);

	/* Attach to the tuple queue if returning results */
	shm_mq_handle *tupleQueue = NULL;

	if (returnResults)
	{
		shm_mq	   *tupleMq = shm_toc_lookup(toc, ATTACHED_WORKER_KEY_TUPLE_QUEUE, false);

		shm_mq_set_sender(tupleMq, MyProc);
		tupleQueue = shm_mq_attach(tupleMq, seg, NULL);
	}

	/* Connect to the database */
#if PG_VERSION_NUM >= 170000
	BackgroundWorkerInitializeConnection(databaseName, userName, BGWORKER_BYPASS_ROLELOGINCHECK);
#else
	BackgroundWorkerInitializeConnection(databaseName, userName, 0);
#endif

	/* Report status as running */
	SetCurrentStatementStartTimestamp();
	pgstat_report_activity(STATE_RUNNING, command);

	/* Start the transaction */
	StartTransactionCommand();

	if (StatementTimeout > 0)
		enable_timeout_after(STATEMENT_TIMEOUT, StatementTimeout);
	else
		disable_timeout(STATEMENT_TIMEOUT, false);

	/* Execute the query. */
	ExecuteSqlString(command, tupleQueue);

	/* End the transaction */
	disable_timeout(STATEMENT_TIMEOUT, false);
	CommitTransactionCommand();

	/* Report status as idle */
	pgstat_report_activity(STATE_IDLE, command);
	pgstat_report_stat(true);

	/* Signal that we are done */
	ReadyForQuery(DestRemote);

	dsm_detach(seg);
	proc_exit(0);
}

/*
 * Execute given SQL string in the current database as the current user.
 * When tupleQueue is non-NULL, query results are sent as MinimalTuples
 * through the tuple queue (using DestTupleQueue), and a RowDescription
 * is sent through the protocol queue for the reader to build a TupleDesc.
 * Otherwise, results are suppressed (DestNone).
 */
static void
ExecuteSqlString(const char *sql, shm_mq_handle *tupleQueue)
{
	/*
	 * Create a memory context for parsing that survives commands that
	 * internally commit.
	 */
	MemoryContext parseContext = AllocSetContextCreate(TopMemoryContext,
													   "pg_extension_base worker parse",
													   ALLOCSET_DEFAULT_MINSIZE,
													   ALLOCSET_DEFAULT_INITSIZE,
													   ALLOCSET_DEFAULT_MAXSIZE);
	MemoryContext oldContext = MemoryContextSwitchTo(parseContext);

	/*
	 * Parse the query or queries.
	 */
	List	   *parseTreeList = pg_parse_query(sql);
	int			commandsRemaining = list_length(parseTreeList);
	bool		isTopLevel = commandsRemaining == 1;

	MemoryContextSwitchTo(oldContext);

	ListCell   *parseTreeCell = NULL;

	foreach(parseTreeCell, parseTreeList)
	{
		RawStmt    *parsetree = (RawStmt *) lfirst(parseTreeCell);

		if (IsA(parsetree, TransactionStmt))
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("transaction control statements are not allowed in workers")));

		CommandTag	commandTag;
		QueryCompletion qc;

		commandTag = CreateCommandTag(parsetree->stmt);

		set_ps_display(GetCommandTagName(commandTag));

		BeginCommand(commandTag, DestRemote);

		/* Set up a snapshot if parse analysis/planning will need one. */
		bool		snapshotSet = false;

		if (analyze_requires_snapshot(parsetree))
		{
			PushActiveSnapshot(GetTransactionSnapshot());
			snapshotSet = true;
		}

		/*
		 * OK to analyze, rewrite, and plan this query.
		 *
		 * As with parsing, we need to make sure this data outlives the
		 * transaction, because of the possibility that the statement might
		 * perform internal transaction control.
		 */
		oldContext = MemoryContextSwitchTo(parseContext);

		List	   *queryTreeList = pg_analyze_and_rewrite_fixedparams(parsetree, sql, NULL, 0, NULL);
		List	   *planTreeList = pg_plan_queries(queryTreeList, sql, 0, NULL);

		/* Done with the snapshot used for parsing/planning */
		if (snapshotSet)
			PopActiveSnapshot();

		/* If we got a cancel signal in analysis or planning, quit */
		CHECK_FOR_INTERRUPTS();

		/*
		 * Execute the query using the unnamed portal.
		 */
		Portal		portal = CreatePortal("", true, true);

		/* Don't display the portal in pg_cursors */
		portal->visible = false;
		PortalDefineQuery(portal,
						  NULL,
						  sql,
						  commandTag,
						  planTreeList,
						  NULL	/* cplan */
			);

		PortalStart(portal, NULL, 0, InvalidSnapshot);

		int16		format = 1; /* binary format */

		PortalSetResultFormat(portal, 1, &format);

		DestReceiver *receiver;

		if (tupleQueue != NULL)
		{
			/*
			 * Send RowDescription through the protocol queue so the reader
			 * can build a TupleDesc. This must happen before PortalRun sends
			 * any tuples through the tuple queue.
			 */
			TupleDesc	tupdesc = portal->tupDesc;

			if (tupdesc != NULL)
			{
				StringInfoData buf;

				pq_beginmessage(&buf, 'T');
				pq_sendint16(&buf, tupdesc->natts);

				for (int i = 0; i < tupdesc->natts; i++)
				{
					Form_pg_attribute att = TupleDescAttr(tupdesc, i);

					pq_sendstring(&buf, NameStr(att->attname));
					pq_sendint32(&buf, 0);	/* table OID */
					pq_sendint16(&buf, 0);	/* column number */
					pq_sendint32(&buf, att->atttypid);
					pq_sendint16(&buf, att->attlen);
					pq_sendint32(&buf, att->atttypmod);
					pq_sendint16(&buf, 0);	/* text format code */
				}

				pq_endmessage(&buf);
			}

			receiver = CreateTupleQueueDestReceiver(tupleQueue);
		}
		else
		{
			receiver = CreateDestReceiver(DestNone);
		}

		/*
		 * Only once the portal and destreceiver have been established can we
		 * return to the transaction context.  All that stuff needs to survive
		 * an internal commit inside PortalRun!
		 */
		MemoryContextSwitchTo(oldContext);

		/* Here's where we actually execute the command. */
#if PG_VERSION_NUM < 180000
		(void) PortalRun(portal, FETCH_ALL, isTopLevel, true, receiver, receiver, &qc);
#else
		(void) PortalRun(portal, FETCH_ALL, isTopLevel, receiver, receiver, &qc);
#endif

		/* Clean up the receiver. */
		(*receiver->rDestroy) (receiver);

		/*
		 * Send a CommandComplete message even if we suppressed the query
		 * results.  The user backend will report these in the absence of any
		 * true query results.
		 */
		EndCommand(&qc, DestRemote, false);

		/* Clean up the portal. */
		PortalDrop(portal, false);

		commandsRemaining--;
	}

	/* Be sure to advance the command counter after the last script command */
	CommandCounterIncrement();
}


#if PG_VERSION_NUM < 170000
/*
 * Helper function to determine if we are a login role.
 */
static bool
UserOidIsLoginRole(Oid userOid)
{
	HeapTuple	roleTuple = SearchSysCache1(AUTHOID, ObjectIdGetDatum(userOid));

	if (!HeapTupleIsValid(roleTuple))
		return false;

	Form_pg_authid authForm = (Form_pg_authid) GETSTRUCT(roleTuple);

	bool		isLoginRole = authForm->rolcanlogin;

	ReleaseSysCache(roleTuple);

	return isLoginRole;
}
#endif
