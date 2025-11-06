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
 * Base workers are a layer of abstraction on top of background workers to
 * simplify extension development.
 *
 * The aim is to support the following happy path:
 *
 * When an extension creation script calls pg_extension_base.register_worker(...),
 * a background worker is started immediately and the worker is added to
 * pg_extension_base.workers.
 *
 * On server start, a database starter process is started for each database.
 * The database starter will start all base workers that were previously
 * registered in that database.
 *
 * A lot of the complexity deals with the following scenarios:
 *
 * - CREATE EXTENSION pg_extension_base (commit)
 * - CREATE EXTENSION pg_extension_base (abort)
 * - CREATE EXTENSION ext_with_workers (commit)
 * - CREATE EXTENSION ext_with_workers (abort)
 * - DROP EXTENSION pg_extension_base (commit)
 * - DROP EXTENSION pg_extension_base (abort)
 * - DROP EXTENSION ext_with_workers (commit)
 * - DROP EXTENSION ext_with_workers (abort)
 * - DROP DATABASE (commit)
 * - DROP DATABASE (abort)
 * - CREATE DATABASE .. TEMPLATE (commit)
 *
 * and rapid combinations thereof, which can occur while the existing workers
 * are at any arbitrary point in their lifecycle. We need to ensure we always
 * return to the correct situation without leaking into shared memory.
 *
 * For instance, a CREATE EXTENSION might start a worker and then roll back,
 * or a DROP EXTENSION might kill a worker and then roll back. We then need
 * to restore the previous situation and also make sure we remove their records
 * from the shared memory hashes that we use for bookkeeping.
 *
 * To be able to always recover, we keep the "server starter" as a root
 * process and in case of a DROP event we ask it to restart the relevant
 * database starter. The database starter can then determine which base workers
 * should still exist, or whether to clean up their shared memory records.
 */
#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/relscan.h"
#include "access/table.h"
#include "access/tableam.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_database.h"
#include "catalog/pg_extension.h"
#include "catalog/pg_proc.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/extension.h"
#include "common/hashfn.h"
#include "datatype/timestamp.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lockdefs.h"
#include "storage/lmgr.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "tcop/utility.h"

#include "pg_extension_base/base_workers.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_extension_base/spi_helpers.h"

#define INVALID_WORKER_ID (0)
#define MAX_WORKER_NAME_LENGTH (255)

/*
 * When a base worker fails with an error, we avoid a crash loop by waiting at
 * least 5 seconds. We do not currently wake up the server and database starters,
 * so the total wait can be up to 30 seconds.
 */
#define NAP_TIME_AFTER_FAILURE (5000)

#define PG_EXTENSION_BASE_EXTENSION_NAME "pg_extension_base"
#define PG_EXTENSION_BASE_SCHEMA_NAME "extension_base"
#define PG_EXTENSION_BASE_LIBRARY_NAME "pg_extension_base"

#if PG_VERSION_NUM < 170000
#define MyProcNumber MyProc->pgprocno
#endif

/*
 * Avoid conflicting with other advisory locks by picking a different locktag
 * locktag class than lockfuns.c in PostgreSQL.
 */
#define ADV_LOCKTAG_CLASS_BASE_WORKER 20
#define ADV_LOCKTAG_CLASS_DATABASE_STARTER 21
#define SET_LOCKTAG_BASE_WORKER(tag, db, workerId) \
    SET_LOCKTAG_ADVISORY(tag, \
                         db, \
                         (uint32) workerId, \
                         (uint32) 0, \
                         ADV_LOCKTAG_CLASS_BASE_WORKER)
#define SET_LOCKTAG_DATABASE_STARTER(tag, db, databaseId) \
    SET_LOCKTAG_ADVISORY(tag, \
                         db, \
                         (uint32) databaseId, \
                         (uint32) 0, \
                         ADV_LOCKTAG_CLASS_DATABASE_STARTER)


/*
 * BaseWorkerControlData is the top-level shared memory control
 * data structure
 */
typedef struct BaseWorkerControlData
{
	int			trancheId;
	char	   *lockTrancheName;
	LWLock		lock;

	/* current PID of the server starter */
	pid_t		serverStarterPid;
	int			serverStarterProcno;
}			BaseWorkerControlData;


typedef enum WorkerState
{
	WORKER_NOT_STARTED,
	WORKER_STARTING,
	WORKER_RUNNING,
	WORKER_STOPPED,
	WORKER_RESTARTING
}			WorkerState;


/*
 * DatabaseStarterEntry holds the state for database starters.
 */
typedef struct DatabaseStarterEntry
{
	/* database the worker should attach to */
	Oid			databaseId;

	/* PID of the background worker */
	pid_t		workerPid;

	/* current state of the background worker */
	WorkerState state;

	/* whether to start the database starter */
	bool		needsRestart;
}			DatabaseStarterEntry;


/*
 * BaseworkerId uniquely identifies a base worker in the hash.
 */
typedef struct BaseWorkerKey
{
	/* database OID */
	Oid			databaseId;

	/* worker ID */
	int32		workerId;
}			BaseWorkerKey;


/*
 * BaseWorkerEntry holds the state for base worker.
 */
typedef struct BaseWorkerEntry
{
	/* identifier of the base worker */
	BaseWorkerKey key;

	/* extension the base worker belongs to */
	Oid			extensionId;

	/* function the base worker should call */
	Oid			entryPointFunctionId;

	/* PID of the background worker */
	pid_t		workerPid;

	/* current state of the background worker */
	WorkerState state;

	/* whether the base worker still needs to be started */
	bool		needsRestart;

	/* when to restart the base worker (delayed after failure) */
	TimestampTz restartAfter;
}			BaseWorkerEntry;


/*
 * DatabaseEntry contains the OID and name of a database.
 */
typedef struct DatabaseEntry
{
	/* database OID */
	Oid			databaseId;

	/* database name */
	char	   *databaseName;

	/* whether this is a template database */
	bool		isTemplate;
}			DatabaseEntry;


/*
 * BaseWorkerRegistration contains the details of a registered
 * base worker.
 */
typedef struct BaseWorkerRegistration
{
	/* worker ID */
	int32		workerId;

	/* worker name */
	char	   *workerName;

	/* OID of the extension */
	Oid			extensionId;

	/* entry point function */
	Oid			entryPointFunctionId;
}			BaseWorkerRegistration;

/*
 * StartDatabaseStarterResult indicates whether a database starter was
 * started or why not.
 */
typedef enum StartDatabaseStarterResult
{
	DATABASE_STARTER_STARTED,
	DATABASE_STARTER_EXISTS,
	DATABASE_STARTER_DONE,
	DATABASE_STARTER_FAILED,
	DATABASE_STARTER_BLOCKED
}			StartDatabaseStarterResult;

/*
 * StartBaseWorkerResult indicates whether a base worker was
 * started or why not.
 */
typedef enum StartBaseWorkerResult
{
	BASE_WORKER_STARTED,
	BASE_WORKER_EXISTS,
	BASE_WORKER_DONE,
	BASE_WORKER_START_FAILED,
	BASE_WORKER_START_BLOCKED
}			StartBaseWorkerResult;




/* set up and utility functions */
static void BaseWorkerSharedMemoryRequest(void);
static void BaseWorkerSharedMemoryStartup(void);
static void StartServerStarter(void);
static void HandleSigterm(SIGNAL_ARGS);
static void HandleSighup(SIGNAL_ARGS);

/* functions called by server starter */
static void PgBaseExtensionServerStarterSharedMemoryExit(int code, Datum arg);
static void StartDatabaseStarters(List *databaseList);
static List *GetDatabaseList(void);
static void RemoveWorkerEntriesNotInDatabaseList(List *databaseList);
static bool DatabaseExistsInList(List *databaseList, Oid databaseId);
static StartDatabaseStarterResult StartDatabaseStarter(Oid databaseId,
													   char *databaseName);

/* shared memory bookkeeping */
static DatabaseStarterEntry * GetDatabaseStarterEntry(Oid databaseId, bool *isFound);
static DatabaseStarterEntry * GetOrCreateDatabaseStarterEntry(Oid databaseId, bool *isFound);
static BaseWorkerEntry * GetBaseWorkerEntry(Oid databaseId, int32 workerId, bool *isFound);
static BaseWorkerEntry * GetOrCreateBaseWorkerEntry(int32 workerId, bool *isFound);
static void RemoveBaseWorkerEntry(Oid databaseId, int32 workerId);
static void RemoveBaseWorkerEntriesForDatabase(Oid databaseId);
static void RemoveBaseWorkerEntriesNotInRegistrationList(List *workerRegistrationList);
static bool BaseWorkerExistsInRegistrationList(List *workerRegistrationList,
											   int workerId);

/* functions called by database starter */
static void PgExtensionBaseDatabaseStarterSharedMemoryExit(int code, Datum arg);
static bool StartAllBaseWorkers(List *workerRegistrationList);
static List *GetBaseWorkerRegistrationList(void);
static StartBaseWorkerResult StartBaseWorker(int workerId, Oid extensionId,
											 Oid entryPointFunctionId);
static LockAcquireResult LockDatabaseStarter(Oid databaseId, LOCKMODE lockMode,
											 bool waitForLock);

/* functions called by base worker */
static void PgExtensionBaseWorkerSharedMemoryExit(int code, Datum arg);

/* functions called via UDFs */
static int32 InsertBaseWorkerRegistration(char *workerName, Oid extensionId,
										  Oid entryPointFunctionId);
static bool DatabaseIsTemplate(Oid databaseId);
static int32 DeleteBaseWorkerRegistrationByName(char *extensionName);
static void DeleteBaseWorkerRegistrationsByExtensionId(Oid extensionId);
static Oid	PgExtensionBaseWorkersRelationId(void);
static Oid	PgExtensionSchemaId(void);

/* functions called via DDL */
static void BaseWorkerProcessUtility(PlannedStmt *pstmt,
									 const char *queryString,
									 bool readOnlyTree,
									 ProcessUtilityContext context,
									 ParamListInfo params,
									 struct QueryEnvironment *queryEnv,
									 DestReceiver *dest,
									 QueryCompletion *completionTag);
static void BaseWorkerObjectAccessHook(ObjectAccessType access, Oid classId,
									   Oid objectId, int subId,
									   void *arg);
static Oid	PgExtensionBaseExtensionId(void);
static bool ExtensionExists(char *extensionName);
static TimestampTz GetDelayedTimestamp(int64 millis);
static void DatabaseStarterNeedsRestart(Oid databaseId);
static void PrepareForDrop(Oid databaseId, Oid extensionId);

/* functions called by transaction end */
static void BaseWorkerTransactionCallback(XactEvent event, void *arg);

/* SQL-callable functions */
PG_FUNCTION_INFO_V1(pg_extension_base_register_worker);
PG_FUNCTION_INFO_V1(pg_extension_base_deregister_worker);
PG_FUNCTION_INFO_V1(pg_extension_base_list_base_workers);
PG_FUNCTION_INFO_V1(pg_extension_base_list_database_starters);

/* background worker entry points */
PGDLLEXPORT void PgExtensionServerStarterMain(Datum arg);
PGDLLEXPORT void PgExtensionBaseDatabaseStarterMain(Datum arg);
PGDLLEXPORT void PgExtensionBaseWorkerMain(Datum arg);

/* shared memory state */
static BaseWorkerControlData * BaseWorkerControl = NULL;
static HTAB *DatabaseStarterHash = NULL;
static HTAB *BaseWorkerHash = NULL;

/* shared memory hooks */
static shmem_startup_hook_type PreviousSharedMemoryStartupHook = NULL;
static shmem_request_hook_type PreviousSharedMemoryRequestHook = NULL;

/* DDL hooks */
static ProcessUtility_hook_type PreviousProcessUtility = NULL;
static object_access_hook_type PreviousObjectAccessHook = NULL;

/* flags set by signal handlers */
volatile sig_atomic_t ReloadRequested = false;
volatile sig_atomic_t TerminationRequested = false;

/* flags set by DDL commands that want to wake up server starter */
static bool SignalServerStarter = false;


/*
 * InitializeBaseWorkerLauncher sets up hooks used by the base worker launcher.
 */
void
InitializeBaseWorkerLauncher(void)
{
	if (IsBinaryUpgrade)
	{
		/* do not start workers during upgrade */
		return;
	}

	/* set up DDL hooks */
	PreviousProcessUtility =
		ProcessUtility_hook != NULL ? ProcessUtility_hook : standard_ProcessUtility;
	ProcessUtility_hook = BaseWorkerProcessUtility;

	/* set up shared memory hooks */

	PreviousSharedMemoryStartupHook = shmem_startup_hook;
	shmem_startup_hook = BaseWorkerSharedMemoryStartup;

	PreviousSharedMemoryRequestHook = shmem_request_hook;
	shmem_request_hook = BaseWorkerSharedMemoryRequest;

	PreviousObjectAccessHook = object_access_hook;
	object_access_hook = BaseWorkerObjectAccessHook;

	RegisterXactCallback(BaseWorkerTransactionCallback, NULL);

	StartServerStarter();
}


/*
 * BaseWorkerSharedMemorySize computes how much shared memory is required.
 */
size_t
BaseWorkerSharedMemorySize(void)
{
	Size		size = 0;

	size = add_size(size, sizeof(BaseWorkerControlData));

	Size		starterHashSize = hash_estimate_size(max_worker_processes, sizeof(DatabaseStarterEntry));

	size = add_size(size, starterHashSize);

	Size		workerHashSize = hash_estimate_size(max_worker_processes, sizeof(BaseWorkerEntry));

	size = add_size(size, workerHashSize);

	return size;
}


/*
 * BaseWorkerSharedMemoryRequest requests shared memory for the worker launcher.
 */
static void
BaseWorkerSharedMemoryRequest(void)
{
	if (PreviousSharedMemoryRequestHook)
	{
		PreviousSharedMemoryRequestHook();
	}

	RequestAddinShmemSpace(BaseWorkerSharedMemorySize());
}


/*
 * BaseWorkerSharedMemoryStartup is a wrapper around BaseWorkerSharedMemoryInit that
 * allows it to be used as shmem_startup_hook.
 */
static void
BaseWorkerSharedMemoryStartup(void)
{
	BaseWorkerSharedMemoryInit();

	if (PreviousSharedMemoryStartupHook != NULL)
	{
		PreviousSharedMemoryStartupHook();
	}
}


/*
 * BaseWorkerSharedMemoryInit initializes the requested shared memory for the
 * worker hash and starts the server starter.
 */
void
BaseWorkerSharedMemoryInit(void)
{
	bool		alreadyInitialized = false;

	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	BaseWorkerControl =
		(BaseWorkerControlData *) ShmemInitStruct("pg_extension_base server starter",
												  sizeof(BaseWorkerControlData),
												  &alreadyInitialized);

	if (!alreadyInitialized)
	{
		BaseWorkerControl->trancheId = LWLockNewTrancheId();
		BaseWorkerControl->lockTrancheName = "pg_extension_base server starter locks";

		LWLockRegisterTranche(BaseWorkerControl->trancheId,
							  BaseWorkerControl->lockTrancheName);

		LWLockInitialize(&BaseWorkerControl->lock,
						 BaseWorkerControl->trancheId);
	}

	HASHCTL		hashInfo;

	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(Oid);
	hashInfo.entrysize = sizeof(DatabaseStarterEntry);
	hashInfo.hash = oid_hash;
	int			hashFlags = (HASH_ELEM | HASH_FUNCTION);

	DatabaseStarterHash = ShmemInitHash("pg_extension_base database starter hash",
										max_worker_processes, 2 * max_worker_processes,
										&hashInfo, hashFlags);

	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(BaseWorkerKey);
	hashInfo.entrysize = sizeof(BaseWorkerEntry);
	hashInfo.hash = tag_hash;
	hashFlags = (HASH_ELEM | HASH_FUNCTION);

	BaseWorkerHash = ShmemInitHash("pg_extension_base base worker hash",
								   max_worker_processes, 2 * max_worker_processes,
								   &hashInfo, hashFlags);

	LWLockRelease(AddinShmemInitLock);
}


/*
 * StartServerStarter starts the server starter.
 */
static void
StartServerStarter(void)
{
	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));

	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

	/*
	 * The server starter is an important component, so restart it quickly if
	 * it fails.
	 */
	worker.bgw_restart_time = 5;
	worker.bgw_main_arg = Int32GetDatum(0);
	worker.bgw_notify_pid = 0;
	strlcpy(worker.bgw_library_name, PG_EXTENSION_BASE_LIBRARY_NAME, sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_name, "pg_base_extension server starter",
			sizeof(worker.bgw_name));
	strlcpy(worker.bgw_function_name, "PgExtensionServerStarterMain",
			sizeof(worker.bgw_function_name));

	RegisterBackgroundWorker(&worker);
}


/*
 * HandleSigterm handles SIGTERM by requesting the main
 * loop to terminate.
 */
static void
HandleSigterm(SIGNAL_ARGS)
{
	int			save_errno = errno;

	TerminationRequested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * HandleSighup handles SIGHUP by requesting LightSleep
 * to reload configurations.
 */
static void
HandleSighup(SIGNAL_ARGS)
{
	int			save_errno = errno;

	ReloadRequested = true;
	SetLatch(MyLatch);

	errno = save_errno;
}


/*
 * LightSleep sleeps for the given number of milliseconds, but reacts to signals
 * and postmaster exit.
 */
void
LightSleep(long timeoutMs)
{
	if (TerminationRequested)
		return;

	int			waitResult = WaitLatch(MyLatch, WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
									   timeoutMs, WAIT_EVENT_CLIENT_READ);

	if (waitResult & WL_POSTMASTER_DEATH)
	{
		proc_exit(1);
	}

	if (waitResult & WL_LATCH_SET)
	{
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
	}

	if (ReloadRequested)
	{
		ReloadRequested = false;
		ProcessConfigFile(PGC_SIGHUP);
	}
}


/*
 * PgExtensionServerStarterMain is the main entry point for the server starter
 * worker.
 *
 * It only runs on start-up until every known database has a worker running.
 */
void
PgExtensionServerStarterMain(Datum arg)
{
	/* set up signal handlers */
	pqsignal(SIGHUP, HandleSighup);
	pqsignal(SIGTERM, HandleSigterm);
	pqsignal(SIGINT, SIG_IGN);

	before_shmem_exit(PgBaseExtensionServerStarterSharedMemoryExit, 0);

	/* allow signals */
	BackgroundWorkerUnblockSignals();

	/* store the server starter PID for others to find */
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);
	BaseWorkerControl->serverStarterPid = MyProcPid;
	BaseWorkerControl->serverStarterProcno = MyProcNumber;
	LWLockRelease(&BaseWorkerControl->lock);

	/* connect only to shared catalogs */
	BackgroundWorkerInitializeConnection(NULL, NULL, 0);

	/* report application_name in pg_stat_activity */
	pgstat_report_appname("pg base extension server starter");

	/* set up a memory context that resets every iteration */
	MemoryContext loopContext = AllocSetContextCreate(CurrentMemoryContext,
													  "pg base extension server starter",
													  ALLOCSET_DEFAULT_MINSIZE,
													  ALLOCSET_DEFAULT_INITSIZE,
													  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(loopContext);

	ereport(LOG, (errmsg("pg base extension server starter started")));

	/*
	 * The server starter loops forever until killed.
	 *
	 * Once all the database starters have been started it will have very
	 * little to do unless a DROP DATABASE or DROP EXTENSION occur. In those
	 * cases, it will start new database starters mainly to cover the case
	 * where a command kills the background workers and then rolls back.
	 */
	while (!TerminationRequested)
	{
		/* get the current list of databases from pg_database */
		List	   *databaseList = GetDatabaseList();

		/*
		 * Remove dropped databases in every iteration. We find this to be the
		 * most reliable way to ensure they are removed from the hash, which
		 * is also important for database starters that keep restarting
		 * because they cannot connect to their dropped database.
		 */
		RemoveWorkerEntriesNotInDatabaseList(databaseList);

		/* start database starters when needed */
		StartDatabaseStarters(databaseList);

		/* clean up any allocated memory */
		MemoryContextReset(loopContext);

		/* try again in 30 seconds */
		LightSleep(30000);
	}

	ereport(LOG, (errmsg("pg base extension server starter restarting")));

	/* restart after sigterm */
	proc_exit(1);
}


/*
 * PgBaseExtensionServerStarterSharedMemoryExit is called when the server starter exits.
 */
static void
PgBaseExtensionServerStarterSharedMemoryExit(int code, Datum arg)
{
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	/* reset the PIDs */
	BaseWorkerControl->serverStarterPid = 0;
	BaseWorkerControl->serverStarterProcno = 0;

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * StartDatabaseStarters starts a background worker for every database.
 */
static void
StartDatabaseStarters(List *databaseList)
{
	MemoryContext outerContext = CurrentMemoryContext;
	ListCell   *databaseListCell;

	foreach(databaseListCell, databaseList)
	{
		DatabaseEntry *entry = (DatabaseEntry *) lfirst(databaseListCell);

		if (entry->isTemplate)
		{
			/* we currently do not start workers for template database */
			continue;
		}

		Oid			databaseId = entry->databaseId;

		/*
		 * StartDatabaseStarter acquires a lock to wait for concurrent DROP
		 * DATABASE commands to finish. We hold this lock until leaving the
		 * function and committing below.
		 */
		StartTransactionCommand();

		StartDatabaseStarterResult startResult =
			StartDatabaseStarter(databaseId, entry->databaseName);

		CommitTransactionCommand();
		MemoryContextSwitchTo(outerContext);

		if (startResult == DATABASE_STARTER_FAILED)
		{
			/* failed to start, bail out for now */
			ereport(LOG, (errmsg("failed to start pg base extension database starter in database %s",
								 quote_identifier(entry->databaseName)),
						  errhint("You may need to increase max_worker_processes.")));

			return;
		}
	}
}


/*
 * GetDatabaseList returns a list of all databases.
 */
static List *
GetDatabaseList(void)
{
	List	   *databaseList = NIL;
	HeapTuple	databaseTuple;
	MemoryContext originalContext = CurrentMemoryContext;

	StartTransactionCommand();

	Relation	pgDatabaseRelation = table_open(DatabaseRelationId, AccessShareLock);
	TableScanDesc scan = table_beginscan_catalog(pgDatabaseRelation, 0, NULL);

	while (HeapTupleIsValid(databaseTuple = heap_getnext(scan, ForwardScanDirection)))
	{
		Form_pg_database databaseRecord = (Form_pg_database) GETSTRUCT(databaseTuple);
		MemoryContext transactionContext = MemoryContextSwitchTo(originalContext);

		DatabaseEntry *database = (DatabaseEntry *) palloc0(sizeof(DatabaseEntry));

		database->databaseId = databaseRecord->oid;
		database->databaseName = pstrdup(NameStr(databaseRecord->datname));
		database->isTemplate = databaseRecord->datistemplate;

		databaseList = lappend(databaseList, database);

		MemoryContextSwitchTo(transactionContext);
	}

	table_endscan(scan);
	table_close(pgDatabaseRelation, AccessShareLock);

	CommitTransactionCommand();
	MemoryContextSwitchTo(originalContext);

	return databaseList;
}


/*
 * RemoveWorkerEntriesNotInDatabaseList removes non-existent databases from
 * the worker hashes.
 */
static void
RemoveWorkerEntriesNotInDatabaseList(List *databaseList)
{
	HASH_SEQ_STATUS status;

	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	hash_seq_init(&status, DatabaseStarterHash);

	DatabaseStarterEntry *starterEntry;

	while ((starterEntry = (DatabaseStarterEntry *) hash_seq_search(&status)) != 0)
	{
		if (!DatabaseExistsInList(databaseList, starterEntry->databaseId))
		{
			hash_search(DatabaseStarterHash, &starterEntry->databaseId, HASH_REMOVE, NULL);
		}
	}

	hash_seq_init(&status, BaseWorkerHash);

	BaseWorkerEntry *baseWorkerEntry;

	while ((baseWorkerEntry = (BaseWorkerEntry *) hash_seq_search(&status)) != 0)
	{
		if (!DatabaseExistsInList(databaseList, baseWorkerEntry->key.databaseId))
		{
			hash_search(BaseWorkerHash, &baseWorkerEntry->key, HASH_REMOVE, NULL);
		}
	}

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * DatabaseExistsInList returns whether databaseList contains a
 * DatabaseEntry for the database with OID databaseId.
 */
static bool
DatabaseExistsInList(List *databaseList, Oid databaseId)
{
	ListCell   *databaseListCell;

	foreach(databaseListCell, databaseList)
	{
		DatabaseEntry *entry = (DatabaseEntry *) lfirst(databaseListCell);

		if (entry->databaseId == databaseId)
		{
			return true;
		}
	}

	return false;
}


/*
 * StartDatabaseStarter starts a worker for the given database.
 */
static StartDatabaseStarterResult
StartDatabaseStarter(Oid databaseId, char *databaseName)
{
	if (DatabaseStarterHash == NULL)
	{
		ereport(ERROR, (errmsg("pg base extension database starter hash not initialized")));
	}

	/*
	 * In case of a concurrent DROP DATABASE/EXTENSION that affects this
	 * database (which takes ExclusiveLock), we bail out and wait for the next
	 * iteration.
	 *
	 * Otherwise, in case of DROP DATABASE, we might proceed to create a
	 * database starter for a database that is gone by the time the worker is
	 * up and running, which causes an error in the log every time someone
	 * drops a database.
	 *
	 * Or, in case of DROP EXTENSION, we might proceed to restart workers for
	 * an extension that is about to be dropped.
	 *
	 * In case of CREATE EXTENSION (which takes ShareLock), we do not back
	 * off.
	 */
	bool		waitForLock = false;
	LockAcquireResult lockResult =
		LockDatabaseStarter(databaseId, ShareLock, waitForLock);

	if (lockResult == LOCKACQUIRE_NOT_AVAIL)
	{
		/*
		 * A DROP DATABASE/EXTENSION is in progress, wait for it to finish.
		 *
		 * If the DROP DATABASE succeeds, we may never get back here since the
		 * database will not be returned by GetDatabaseList on the next
		 * iteration. If the DROP DATABASE fails, we will start a new database
		 * starter on the next iteration.
		 *
		 * In case of a DROP EXTENSION, we will always get back here and exit
		 * early in the database starter if needed. That's because we have no
		 * way of knowing whether or not a DROP EXTENSION succeeded or failed
		 * from the server starter.
		 */
		return DATABASE_STARTER_BLOCKED;
	}

	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	DatabaseStarterEntry *starterEntry =
		GetOrCreateDatabaseStarterEntry(databaseId, &isFound);

	if (isFound)
	{
		/* breaking down 2 cases for readability */

		if (starterEntry->workerPid > 0 || starterEntry->state == WORKER_STARTING)
		{
			/* database starter is already running */
			LWLockRelease(&BaseWorkerControl->lock);
			return DATABASE_STARTER_EXISTS;
		}
		else if (!starterEntry->needsRestart)
		{
			/*
			 * database starter is already finished and does not need a
			 * restart
			 */
			LWLockRelease(&BaseWorkerControl->lock);
			return DATABASE_STARTER_DONE;
		}
	}

	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));

	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main_arg = ObjectIdGetDatum(databaseId);
	worker.bgw_notify_pid = MyProcPid;
	strlcpy(worker.bgw_library_name, PG_EXTENSION_BASE_LIBRARY_NAME, sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_function_name, "PgExtensionBaseDatabaseStarterMain", sizeof(worker.bgw_function_name));
	strlcpy(worker.bgw_name, "pg base extension database starter", sizeof(worker.bgw_name));

	ereport(LOG, (errmsg("starting pg base extension database starter in database %s",
						 quote_identifier(databaseName))));

	BackgroundWorkerHandle *handle;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		/*
		 * Out of max_worker_processes
		 *
		 * If needsRestart is true, it will remain true, so we will try again
		 * next time, hoping that another background worker stopped.
		 */
		LWLockRelease(&BaseWorkerControl->lock);
		return DATABASE_STARTER_FAILED;
	}

	/*
	 * When a worker exits, it will mark itself as needsRestart. For now we
	 * set it to false, since it will not restart if it exits gracefully.
	 */
	starterEntry->needsRestart = false;
	starterEntry->state = WORKER_STARTING;

	LWLockRelease(&BaseWorkerControl->lock);

	/*
	 * We wait for the worker to start mainly to stagger background worker
	 * creation. We let the process set its own PID.
	 */
	pid_t		workerPid = 0;

	WaitForBackgroundWorkerStartup(handle, &workerPid);

	return DATABASE_STARTER_STARTED;
}


/*
 * PgExtensionBaseDatabaseStarterMain is the main entry-point for the background worker that
 * starts base workers for a given database.
 */
void
PgExtensionBaseDatabaseStarterMain(Datum databaseIdDatum)
{
	Oid			databaseId = DatumGetObjectId(databaseIdDatum);

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, HandleSighup);
	pqsignal(SIGINT, SIG_IGN);
	pqsignal(SIGTERM, HandleSigterm);

	/* Set our exit handler before any calls to proc_exit */
	before_shmem_exit(PgExtensionBaseDatabaseStarterSharedMemoryExit,
					  ObjectIdGetDatum(databaseId));

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* lock the shared hash, because we might change it */
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	DatabaseStarterEntry *starterEntry = GetDatabaseStarterEntry(databaseId, &isFound);

	if (!isFound)
	{
		LWLockRelease(&BaseWorkerControl->lock);

		/*
		 * If the database is dropped just after this database starter was
		 * started AND the cleanup already occurred, then we would not see
		 * ourselves here, and we're not supposed to exist.
		 */
		ereport(LOG, (errmsg("no pg base extension database starter entry found for database %d",
							 databaseId)));
		proc_exit(0);
	}

	if (starterEntry->workerPid != 0)
	{
		LWLockRelease(&BaseWorkerControl->lock);

		/* another worker is running? (should not happen) */
		ereport(WARNING, (errmsg("another pg base extension database starter is running "
								 "for database %d?",
								 databaseId)));
		proc_exit(0);
	}

	if (starterEntry->needsRestart)
	{
		LWLockRelease(&BaseWorkerControl->lock);

		/*
		 * If the database or an extension is dropped just after this database
		 * starter was started, then we might have missed the SIGTERM, but can
		 * still tell it happened from needsRestart. We exit here and let
		 * server starter decide what to do.
		 */
		ereport(DEBUG2, (errmsg("a restart of pg base extension database starter "
								"in database %d was requested, exiting",
								databaseId)));
		proc_exit(0);
	}

	/* show my presence */
	starterEntry->workerPid = MyProcPid;
	starterEntry->state = WORKER_RUNNING;

	LWLockRelease(&BaseWorkerControl->lock);

	/* connect to our database */
	BackgroundWorkerInitializeConnectionByOid(databaseId, InvalidOid, 0);

	MemoryContext outerContext = CurrentMemoryContext;

	StartTransactionCommand();

	/*
	 * We take the LockDatabaseStarter lock here to wait for any concurrent
	 * DROP EXTENSION pg_extension_base (which takes ExclusiveLock) to finish.
	 * If it succeeds, we will exit immediately below.
	 *
	 * We also wait for CREATE EXTENSION (which takes ShareLock) to finish,
	 * since we may have been started specifically for a newly registered
	 * worker and we want to make sure we can see it in
	 * GetBaseWorkerRegistrationList.
	 *
	 * We also wait for regular DROP EXTENSION (which takes ExclusiveLock) to
	 * finish, though in most cases that will prevent the server starter from
	 * starting a database starter. If a database starter had just been
	 * started we may still end up here, and we want to wait for the DROP
	 * EXTENSION result especially to prevent workers from getting prematurely
	 * restarted in case of a successful DROP EXTENSION.
	 *
	 * If any of the above events happen just after we commit the current
	 * transaction, we'll re-enter this function later because needsRestart
	 * will have been set as well.
	 *
	 * We know that there cannot be a concurrent DROP DATABASE, since it will
	 * try to kill() us and block until we're gone, and fail if we stick
	 * around. Or, BackgroundWorkerInitializeConnectionByOid would have failed
	 * if we were already gone.
	 */
	bool		waitForLock = true;

	LockDatabaseStarter(databaseId, RowExclusiveLock, waitForLock);

	/* get the database name and make sure it survives outside the transaction */
	char	   *databaseName = MemoryContextStrdup(outerContext,
												   get_database_name(databaseId));

	/* database cannot have been dropped while we are connected */
	Assert(databaseName != NULL);

	/*
	 * Get the OID of the pg_extension_base extension to check whether it
	 * actually exists.
	 */
	Oid			baseExtensionId = PgExtensionBaseExtensionId();

	/*
	 * Get the OID of the pg_extension_base.workers table, to check whether it
	 * exists. This might not be the case if the administrator has not run
	 * ALTER EXTENSION pg_extension_base UPDATE yet.
	 */
	Oid			workerTableId = PgExtensionBaseWorkersRelationId();

	CommitTransactionCommand();
	MemoryContextSwitchTo(outerContext);

	if (!OidIsValid(baseExtensionId) || !OidIsValid(workerTableId))
	{
		/* pg_extension_base extension does not exist in this database */

		/*
		 * In case the pg_extension_base extension was dropped, we may not
		 * have removed the entries from the hash yet. Do so now, although
		 * this will usually be a noop.
		 */
		RemoveBaseWorkerEntriesForDatabase(databaseId);

		/*
		 * No pg_extension_base extension, so no workers to start. We do not
		 * LOG a message since we redundantly start workers for all databases
		 * so this will be very common.
		 */
		ereport(DEBUG2, (errmsg("pg_extension_base database starter sees no pg_extension_base "
								"extension in database %s, exiting",
								databaseName)));

		proc_exit(0);
	}

	/* report application_name in pg_stat_activity */
	pgstat_report_appname("pg_extension_base database starter");

	ereport(LOG, (errmsg("pg_extension_base database starter for database %s started",
						 databaseName)));

	/* set up a memory context that resets every iteration */
	MemoryContext loopContext = AllocSetContextCreate(CurrentMemoryContext,
													  "pg_extension_base base database starter",
													  ALLOCSET_DEFAULT_MINSIZE,
													  ALLOCSET_DEFAULT_INITSIZE,
													  ALLOCSET_DEFAULT_MAXSIZE);

	MemoryContextSwitchTo(loopContext);

	while (!TerminationRequested)
	{
		/* read pg_extension_base.workers contents */
		List	   *workerRegistrationList = GetBaseWorkerRegistrationList();

		/*
		 * Clean up shared memory in case of DROP EXTENSION ext_with_workers
		 * (commit) by removing entries for base workers that are no longer
		 * registered.
		 *
		 * We deferred this in BaseWorkerObjectAccessHook, in case the
		 * transaction aborted.
		 */
		RemoveBaseWorkerEntriesNotInRegistrationList(workerRegistrationList);

		if (StartAllBaseWorkers(workerRegistrationList))
		{
			/*
			 * All base workers started.
			 *
			 * We do not find it necessary to keep the database starter
			 * running because base workers that are added after server start
			 * via pg_extension_base_register_worker are started immediately.
			 *
			 * Only when a critical condition (like possibly failed DROP
			 * EXTENSION) occurs do we restart the database starter.
			 *
			 * Note that we preserve the needsRestart value. If it is false,
			 * we will be gone for a while. If it was already set to true, we
			 * will come back soon.
			 */
			ereport(LOG, (errmsg("pg_extension_base database starter for database %s finished",
								 databaseName)));
			proc_exit(0);
		}

		MemoryContextReset(loopContext);

		/* try again later */
		LightSleep(30000);
	}

	ereport(LOG, (errmsg("pg_extension_base database starter for database %s restarting",
						 databaseName)));

	/*
	 * We restart after sigterm via the server starter instead of setting
	 * bgw_restart_time.
	 *
	 * That way, server starter can tell the difference between a starter that
	 * was killed by the user (needsRestart == true) and one that exited
	 * gracefully above (needsRestart == false). We will not try to start the
	 * database starter again in the latter case.
	 */
	DatabaseStarterNeedsRestart(databaseId);
}


/*
 * PgExtensionBaseDatabaseStarterSharedMemoryExit is called when a database starter exits.
 */
static void
PgExtensionBaseDatabaseStarterSharedMemoryExit(int code, Datum arg)
{
	Oid			databaseId = DatumGetObjectId(arg);

	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	DatabaseStarterEntry *starterEntry = GetDatabaseStarterEntry(databaseId, &isFound);

	if (isFound)
	{
		/* signal to other backends that I'm no longer running */
		starterEntry->workerPid = 0;
		starterEntry->state = WORKER_STOPPED;

		if (code != 0)
		{
			/*
			 * Restart on failure via the server starter which first checks
			 * whether the database exists.
			 */
			starterEntry->needsRestart = true;
		}
	}

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * StartAllBaseWorkers starts all registered base workers.
 */
static bool
StartAllBaseWorkers(List *workerRegistrationList)
{
	ListCell   *workerRegistrationCell;
	bool		hasBlocked = false;

	foreach(workerRegistrationCell, workerRegistrationList)
	{
		BaseWorkerRegistration *registration =
			(BaseWorkerRegistration *) lfirst(workerRegistrationCell);

		/*
		 * We may be getting killed by a DROP DATABASE, stop launching more
		 * workers.
		 */
		if (TerminationRequested)
			return false;

		StartBaseWorkerResult startResult =
			StartBaseWorker(registration->workerId, registration->extensionId,
							registration->entryPointFunctionId);

		if (startResult == BASE_WORKER_START_FAILED)
		{
			ereport(LOG, (errmsg("failed to start base worker %s in database %d",
								 registration->workerName, MyDatabaseId)),
					errhint("You may need to increase max_worker_processes."));

			/* failed to start, bail out for now */
			return false;
		}
		else if (startResult == BASE_WORKER_START_BLOCKED)
		{
			/* cannot start this base worker yet */
			hasBlocked = true;
		}
	}

	return !hasBlocked;
}


/*
 * GetBaseWorkerRegistrationList returns a list of all base worker registrations.
 *
 * We use SPI to directly query the metadata tables. Since this only happens from
 * a background worker, we can assume we run in a superuser context that has access
 * to the table.
 */
static List *
GetBaseWorkerRegistrationList(void)
{
	List	   *registrationList = NIL;
	MemoryContext outerContext = CurrentMemoryContext;

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();

	bool		readOnly = true;
	long		limit = 0;

	SPI_execute("select worker_id, worker_name, pg_extension.oid, pg_proc.oid "
				"from " PG_EXTENSION_BASE_SCHEMA_NAME ".workers "
				"join pg_namespace on (entry_point_schema operator(pg_catalog.=) nspname) "
				"join pg_proc on (entry_point_function operator(pg_catalog.=) proname "
				"and pronamespace = pg_namespace.oid) "
				"join pg_extension on (extension_name operator(pg_catalog.=) extname)",
				readOnly, limit);

	for (uint64 rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		bool		isNull = false;
		Datum		workerIdDatum = GET_SPI_DATUM(rowIndex, 1, &isNull);
		Datum		workerNameDatum = GET_SPI_DATUM(rowIndex, 2, &isNull);
		Datum		extensionIdDatum = GET_SPI_DATUM(rowIndex, 3, &isNull);
		Datum		functionIdDatum = GET_SPI_DATUM(rowIndex, 4, &isNull);

		MemoryContext spiContext = MemoryContextSwitchTo(outerContext);

		BaseWorkerRegistration *registration = palloc0(sizeof(BaseWorkerRegistration));

		registration->workerId = DatumGetInt32(workerIdDatum);
		registration->workerName = TextDatumGetCString(workerNameDatum);
		registration->extensionId = DatumGetObjectId(extensionIdDatum);
		registration->entryPointFunctionId = DatumGetObjectId(functionIdDatum);

		registrationList = lappend(registrationList, registration);

		MemoryContextSwitchTo(spiContext);
	}

	SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();
	MemoryContextSwitchTo(outerContext);

	return registrationList;
}


/*
 * IsWorkerRegistered returns whether the given worker is registered.
 */
static bool
IsWorkerRegistered(int32 workerId)
{
	PushActiveSnapshot(GetTransactionSnapshot());
	SPI_connect();

	int			argCount = 1;
	Oid			argTypes[] = {INT4OID};
	Datum		argValues[] = {Int32GetDatum(workerId)};
	const char *argNulls = " ";
	bool		readOnly = true;
	long		limit = 0;

	SPI_execute_with_args("select worker_id "
						  "from " PG_EXTENSION_BASE_SCHEMA_NAME ".workers "
						  "where worker_id = $1",
						  argCount, argTypes, argValues, argNulls,
						  readOnly, limit);

	bool		isWorkerRegistered = SPI_processed > 0;

	SPI_finish();
	PopActiveSnapshot();

	return isWorkerRegistered;
}


/*
 * StartBaseWorker starts a base worker in the current database.
 *
 * We specify an extension and entry-point function that the base worker
 * should call here because we pass it through shared memory.
 */
static StartBaseWorkerResult
StartBaseWorker(int workerId, Oid extensionId, Oid entryPointFunctionId)
{
	if (BaseWorkerHash == NULL)
	{
		ereport(ERROR, (errmsg("pg_extension_base worker hash not initialized")));
	}

	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	BaseWorkerEntry *workerEntry = GetOrCreateBaseWorkerEntry(workerId, &isFound);

	if (isFound)
	{
		/*
		 * We breaking down the first 2 cases for readability, they have the
		 * same effect of the worker not getting started again.
		 */

		if (workerEntry->workerPid > 0 || workerEntry->state == WORKER_STARTING)
		{
			/* base worker is already running */
			LWLockRelease(&BaseWorkerControl->lock);
			return BASE_WORKER_EXISTS;
		}
		else if (!workerEntry->needsRestart)
		{
			/* base worker is already finished and does not need a restart */
			LWLockRelease(&BaseWorkerControl->lock);
			return BASE_WORKER_DONE;
		}
		else if (workerEntry->restartAfter != 0)
		{
			TimestampTz now = GetCurrentTimestamp();

			/*
			 * If delayed restart requested, we keep cycling in the database
			 * starter until the time has passed.
			 */
			if (TimestampDifferenceExceeds(now, workerEntry->restartAfter, 0))
			{
				LWLockRelease(&BaseWorkerControl->lock);
				return BASE_WORKER_START_BLOCKED;
			}
		}
	}

	workerEntry->entryPointFunctionId = entryPointFunctionId;
	workerEntry->extensionId = extensionId;

	BackgroundWorker worker;

	memset(&worker, 0, sizeof(worker));

	worker.bgw_flags =
		BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;

	/*
	 * We will restart explicitly through database starter (see
	 * PgExtensionBaseWorkerSharedMemoryExit). Otherwise, we will end up
	 * having "hidden" background workers that get restarted by postmaster,
	 * which complicates the logic.
	 */
	worker.bgw_restart_time = BGW_NEVER_RESTART;

	/* pass the database ID and the worker ID via a 64-bit value */
	uint64		workerKey = ((uint64) MyDatabaseId << 32) | workerId;

	worker.bgw_main_arg = UInt64GetDatum(workerKey);

	worker.bgw_notify_pid = MyProcPid;
	strlcpy(worker.bgw_library_name, PG_EXTENSION_BASE_LIBRARY_NAME, sizeof(worker.bgw_library_name));
	strlcpy(worker.bgw_function_name, "PgExtensionBaseWorkerMain", sizeof(worker.bgw_function_name));
	strlcpy(worker.bgw_name, "pg base extension worker", sizeof(worker.bgw_name));

	ereport(LOG, (errmsg("starting pg base extension worker %d in database %d",
						 workerId, MyDatabaseId)));

	BackgroundWorkerHandle *handle;

	if (!RegisterDynamicBackgroundWorker(&worker, &handle))
	{
		/*
		 * Out of max_worker_processes
		 *
		 * needsRestart will remain true, so we will try again next time,
		 * hoping that another background worker stopped
		 */
		LWLockRelease(&BaseWorkerControl->lock);

		ereport(LOG, (errmsg("failed to start pg extension base worker in database %d",
							 MyDatabaseId),
					  errhint("You may need to increase max_worker_processes.")));

		return BASE_WORKER_START_FAILED;
	}

	/*
	 * If the database starter restarts, it should not try to start the base
	 * worker again. A failure of the base worker will anyway trigger it to
	 * restart.
	 */
	workerEntry->needsRestart = false;
	workerEntry->state = WORKER_STARTING;
	workerEntry->restartAfter = 0;

	LWLockRelease(&BaseWorkerControl->lock);

	/*
	 * We wait for the worker to start mainly to stagger background worker
	 * creation. We let the process set its own PID.
	 */
	pid_t		workerPid = 0;

	WaitForBackgroundWorkerStartup(handle, &workerPid);

	return BASE_WORKER_STARTED;
}


/*
 * DatabaseStarterNeedsRestart signals that the database starter for the
 * database needs to be (re)started.
 */
static void
DatabaseStarterNeedsRestart(Oid databaseId)
{
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	DatabaseStarterEntry *starterEntry = GetDatabaseStarterEntry(databaseId, &isFound);

	if (isFound)
	{
		/* signal to other backends that we need to restart */
		starterEntry->needsRestart = true;
	}

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * LockDatabaseStarter takes an advisory lock on the database OID.
 */
static LockAcquireResult
LockDatabaseStarter(Oid databaseId, LOCKMODE lockMode, bool waitForLock)
{
	LOCKTAG		tag;
	const bool	sessionLock = false;

	SET_LOCKTAG_DATABASE_STARTER(tag, databaseId, databaseId);

	return LockAcquire(&tag, lockMode, sessionLock, !waitForLock);
}


/*
 * GetDatabaseStarterEntry returns the database starter entry for a given database.
 */
static DatabaseStarterEntry *
GetDatabaseStarterEntry(Oid databaseId, bool *isFound)
{
	return hash_search(DatabaseStarterHash, &databaseId, HASH_FIND, isFound);
}


/*
 * GetOrCreateDatabaseStarterEntry returns the database starter entry for a given database.
 */
static DatabaseStarterEntry *
GetOrCreateDatabaseStarterEntry(Oid databaseId, bool *isFound)
{
	DatabaseStarterEntry *workerEntry = hash_search(DatabaseStarterHash, &databaseId,
													HASH_ENTER, isFound);

	if (!*isFound)
	{
		memset(((char *) workerEntry) + sizeof(Oid), 0,
			   sizeof(DatabaseStarterEntry) - sizeof(Oid));

		workerEntry->needsRestart = true;
	}

	return workerEntry;
}


/*
 * GetBaseWorkerEntry returns the base worker entry with the given worker ID.
 */
static BaseWorkerEntry *
GetBaseWorkerEntry(Oid databaseId, int32 workerId, bool *isFound)
{
	BaseWorkerKey key;

	key.databaseId = databaseId;
	key.workerId = workerId;

	return hash_search(BaseWorkerHash, &key, HASH_FIND, isFound);
}


/*
 * GetOrCreateBaseWorkerEntry returns the base worker entry with the given worker ID.
 */
static BaseWorkerEntry *
GetOrCreateBaseWorkerEntry(int32 workerId, bool *isFound)
{
	BaseWorkerKey key;

	key.databaseId = MyDatabaseId;
	key.workerId = workerId;

	BaseWorkerEntry *workerEntry = hash_search(BaseWorkerHash, &key,
											   HASH_ENTER, isFound);

	if (!*isFound)
	{
		memset(((char *) workerEntry) + sizeof(BaseWorkerKey), 0,
			   sizeof(BaseWorkerEntry) - sizeof(BaseWorkerKey));

		workerEntry->needsRestart = true;
	}

	return workerEntry;
}


/*
 * RemoveBaseWorkerEntry removes the base worker entry with the given worker ID.
 */
static void
RemoveBaseWorkerEntry(Oid databaseId, int32 workerId)
{
	BaseWorkerKey key;

	key.databaseId = databaseId;
	key.workerId = workerId;

	bool		isFound = false;

	hash_search(BaseWorkerHash, &key, HASH_REMOVE, &isFound);
}


/*
 * RemoveBaseWorkerEntriesForDatabase removes all base worker entries belonging to
 * a specific database after it has been dropped or the pg_extension_base
 * extension has been dropped.
 */
static void
RemoveBaseWorkerEntriesForDatabase(Oid databaseId)
{
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, BaseWorkerHash);

	BaseWorkerEntry *baseWorkerEntry;

	while ((baseWorkerEntry = (BaseWorkerEntry *) hash_seq_search(&status)) != 0)
	{
		if (baseWorkerEntry->key.databaseId == databaseId)
		{
			hash_search(BaseWorkerHash, &baseWorkerEntry->key, HASH_REMOVE, NULL);
		}
	}

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * RemoveBaseWorkerEntriesNotInRegistrationList removes base worker entries from
 * the hash that do not have a corresponding registration in the current database.
 *
 * This can happen after a DROP EXTENSION, which should have already killed the
 * workers, but does not immediately clear the hash.
 */
static void
RemoveBaseWorkerEntriesNotInRegistrationList(List *workerRegistrationList)
{
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, BaseWorkerHash);

	BaseWorkerEntry *baseWorkerEntry;

	while ((baseWorkerEntry = (BaseWorkerEntry *) hash_seq_search(&status)) != 0)
	{
		if (baseWorkerEntry->key.databaseId == MyDatabaseId &&
			!BaseWorkerExistsInRegistrationList(workerRegistrationList,
												baseWorkerEntry->key.workerId))
		{
			hash_search(BaseWorkerHash, &baseWorkerEntry->key, HASH_REMOVE, NULL);
		}
	}

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * BaseWorkerExistsInRegistrationList returns whether the given worker ID exists
 * in the worker registration list.
 */
static bool
BaseWorkerExistsInRegistrationList(List *workerRegistrationList, int workerId)
{
	ListCell   *workerRegistrationCell;

	foreach(workerRegistrationCell, workerRegistrationList)
	{
		BaseWorkerRegistration *registration =
			(BaseWorkerRegistration *) lfirst(workerRegistrationCell);


		if (registration->workerId == workerId)
		{
			return true;
		}
	}

	return false;
}



/*
 * PgExtensionBaseWorkerMain is the main entry point for all base workers.
 */
void
PgExtensionBaseWorkerMain(Datum arg)
{
	uint64		workerKey = DatumGetUInt64(arg);
	Oid			databaseId = workerKey >> 32;
	int32		workerId = workerKey & 0xFFFF;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, HandleSighup);

	/*
	 * Let Postgres handle the SIGTERM signal, so we can clean up properly.
	 * Otherwise, the extension authors have to be careful about cleaning up
	 * shared memory, transaction state, etc.
	 */
	pqsignal(SIGTERM, die);

	/* reset workerPid on exit */
	before_shmem_exit(PgExtensionBaseWorkerSharedMemoryExit, arg);

	/*
	 * We are now ready to receive signals. However, we may have already
	 * missed a SIGTERM (e.g. from PrepareDropDatabase) before this point.
	 *
	 * To catch that issue, we check workerEntry->needsRestart below, which
	 * would have been set before sending the kill signal.
	 */
	BackgroundWorkerUnblockSignals();

	/* lock the shared hash, because we might change it */
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	BaseWorkerEntry *workerEntry = GetBaseWorkerEntry(databaseId, workerId, &isFound);

	if (!isFound)
	{
		LWLockRelease(&BaseWorkerControl->lock);

		/*
		 * If the database or extension was dropped AND cleanup already
		 * occurred, then we do not see ourselves here, and we're not supposed
		 * to exist.
		 */
		ereport(LOG, (errmsg("no base worker entry found for worker %d in database %d?",
							 workerId, databaseId)));
		proc_exit(0);
	}

	if (workerEntry->workerPid != 0)
	{
		LWLockRelease(&BaseWorkerControl->lock);

		/* another worker is running? (should not happen) */
		ereport(WARNING, (errmsg("another base worker with id %d in database %d "
								 "is running?",
								 workerId, databaseId)));
		proc_exit(0);
	}

	if (workerEntry->needsRestart)
	{
		LWLockRelease(&BaseWorkerControl->lock);

		/*
		 * If the database or extension is dropped just after this base worker
		 * was started, then we might have missed the SIGTERM, but can still
		 * tell it happened from needsRestart. We exit here and let server
		 * starter decide what to do.
		 */
		ereport(DEBUG2, (errmsg("a restart of base worker %d in database %d was "
								"requested, exiting",
								workerId, databaseId)));
		proc_exit(0);
	}

	workerEntry->state = WORKER_RUNNING;
	workerEntry->workerPid = MyProcPid;

	Oid			entryPointFunctionId = workerEntry->entryPointFunctionId;
	Oid			extensionId = workerEntry->extensionId;

	LWLockRelease(&BaseWorkerControl->lock);

	/*
	 * Connect to the database. The database may have been dropped while we
	 * were still trying to start, in which case we will throw an error and
	 * restart again. Eventually, PgExtensionServerStarterMain will remove the
	 * entries related to a dropped database from the hash and then we will
	 * exit above.
	 */
	BackgroundWorkerInitializeConnectionByOid(databaseId, InvalidOid, 0);

	MemoryContext outerContext = CurrentMemoryContext;

	StartTransactionCommand();

	char	   *databaseName = get_database_name(databaseId);

	char	   *extensionName = get_extension_name(extensionId);

	if (extensionName == NULL || !IsWorkerRegistered(workerId))
	{
		/*
		 * The extension was dropped just after the base worker started. The
		 * worker registration is permanently gone, so we can remove it from
		 * the hash.
		 */

		RemoveBaseWorkerEntry(databaseId, workerId);

		ereport(LOG, (errmsg("pg extension base worker %d in database %s was dropped "
							 "or not created, exiting ",
							 workerId, databaseName)));

		CommitTransactionCommand();
		proc_exit(0);
	}

	MemoryContext transactionContext = MemoryContextSwitchTo(outerContext);

	/* copy into outer memory context */
	databaseName = pstrdup(databaseName);

	/* load the function  */
	FmgrInfo	entryPoint;

	fmgr_info(entryPointFunctionId, &entryPoint);

	MemoryContextSwitchTo(transactionContext);
	CommitTransactionCommand();

	/* report application_name in pg_stat_activity */
	pgstat_report_appname(psprintf("pg extension base worker %d", workerId));

	ereport(LOG, (errmsg("pg extension base worker %d in database %s started",
						 workerId, databaseName)));

	if (!CacheMemoryContext)
	{
		/* make sure CacheMemoryContext exists */
		CreateCacheMemoryContext();
	}

	/*
	 * Call the entry-point function with the worker ID as the argument,
	 * outside of a transaction.
	 *
	 * Our current design is not to loop here, but let the function decide
	 * whether to loop.
	 */
	FunctionCall1(&entryPoint, Int32GetDatum(workerId));

	ereport(LOG, (errmsg("pg extension base worker %d in database %s finished",
						 workerId, databaseName)));

	proc_exit(0);
}


/*
 * PgExtensionBaseWorkerSharedMemoryExit is called when a base worker exits.
 */
static void
PgExtensionBaseWorkerSharedMemoryExit(int code, Datum arg)
{
	uint64		workerKey = DatumGetUInt64(arg);
	Oid			databaseId = workerKey >> 32;
	int32		workerId = workerKey & 0xFFFF;

	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	BaseWorkerEntry *workerEntry = GetBaseWorkerEntry(databaseId, workerId, &isFound);

	if (isFound)
	{
		/* signal to other backends that I'm no longer running */
		workerEntry->workerPid = 0;

		if (code == 0)
		{
			/* clean exit */
			workerEntry->state = WORKER_STOPPED;
		}
		else if (workerEntry->needsRestart)
		{
			/*
			 * We got killed by a command (e.g. DROP DATABASE), if the command
			 * fails we will get restarted.
			 */
			workerEntry->state = WORKER_RESTARTING;
		}
		else if (!workerEntry->needsRestart)
		{
			/*
			 * needsRestart being false means that we got killed due to an
			 * internal error. Bring back the database starter to restore us.
			 */
			bool		isFound = false;
			DatabaseStarterEntry *starterEntry = GetDatabaseStarterEntry(databaseId, &isFound);

			if (isFound)
			{
				/*
				 * Signal to the server starter that the database starter is
				 * needed.
				 */
				starterEntry->needsRestart = true;
			}

			/*
			 * Also tell the database starter that we want to restart
			 */
			workerEntry->needsRestart = true;
			workerEntry->state = WORKER_RESTARTING;
			workerEntry->restartAfter = GetDelayedTimestamp(NAP_TIME_AFTER_FAILURE);
		}
	}
	else
	{
		/* already removed from the hash, that's ok */
	}

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * pg_extension_base_register_worker implements the pg_extension_base.register_worker
 * function.
 *
 * It inserts a base worker registration and starts the worker.
 */
Datum
pg_extension_base_register_worker(PG_FUNCTION_ARGS)
{
	char	   *workerName = text_to_cstring(PG_GETARG_TEXT_P(0));
	Oid			entryPointFunctionId = PG_GETARG_OID(1);

	int32		workerId =
		RegisterBaseWorker(workerName, entryPointFunctionId, CurrentExtensionObject);

	PG_RETURN_INT32(workerId);
}


int32
RegisterBaseWorker(char *workerName, Oid entryPointFunctionId, Oid extensionId)
{
	if (strlen(workerName) > MAX_WORKER_NAME_LENGTH)
	{
		ereport(ERROR, (errcode(ERRCODE_NAME_TOO_LONG),
						errmsg("worker name can be at most %d characters",
							   MAX_WORKER_NAME_LENGTH)));
	}

	/* get the pg_proc entry */
	HeapTuple	procedureTuple =
		SearchSysCache1(PROCOID, ObjectIdGetDatum(entryPointFunctionId));

	if (!HeapTupleIsValid(procedureTuple))
	{
		elog(ERROR, "cache lookup failed for function %u", entryPointFunctionId);
	}

	Form_pg_proc procedureStruct = (Form_pg_proc) GETSTRUCT(procedureTuple);

	/*
	 * We require a single internal argument to make the function not-callable
	 * from SQL. We only want to be called from C.
	 */
	if (procedureStruct->pronargs != 1)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("function must have a single argument")));
	}

	if (procedureStruct->proargtypes.values[0] != INTERNALOID)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("function argument of must have type internal")));
	}

	if (procedureStruct->prorettype != INTERNALOID)
	{
		ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						errmsg("function must have internal return type")));
	}

	if (!creating_extension)
	{
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("can only register workers from extensions")));
	}

	int32		workerId = InsertBaseWorkerRegistration(workerName,
														extensionId,
														entryPointFunctionId);

	/*
	 * If the base worker launcher is disabled, we can still register
	 * everything for the database starter to be ready, except for the actual
	 * database launcher restart.
	 */
	if (EnableBaseWorkerLauncher && !DatabaseIsTemplate(MyDatabaseId))
	{
		/*
		 * Wake up a database starter to create our background worker and let
		 * it wait until the current transaction finishes.
		 *
		 * We could create a worker here, but that creates some hairy
		 * synchronization issues. For instance, if we put something in shared
		 * memory that's not committed, then
		 * RemoveBaseWorkerEntriesNotInRegistrationList might remove it.
		 *
		 * We use ShareLock, which allows the database starter to get started,
		 * since server starter also uses ShareLock, but makes the database
		 * starter (which uses RowExclusiveLock) wait before its main loop to
		 * make sure it creates the worker if we commit.
		 *
		 * We technically also block concurrent DROP EXTENSION/DATABASE, but
		 * since we're currently in a CREATE EXTENSION, that would be blocked
		 * anyway.
		 */
		bool		waitForLock = true;

		LockDatabaseStarter(MyDatabaseId, ShareLock, waitForLock);
		DatabaseStarterNeedsRestart(MyDatabaseId);

		SignalServerStarter = true;
	}

	ReleaseSysCache(procedureTuple);

	return workerId;
}


/*
 * pg_extension_base_deregister_worker implements the extension_base.deregister_worker
 * function.
 */
Datum
pg_extension_base_deregister_worker(PG_FUNCTION_ARGS)
{
	char	   *workerName = text_to_cstring(PG_GETARG_TEXT_P(0));

	DeregisterBaseWorker(workerName);

	PG_RETURN_VOID();
}


/*
 * DeregisterBaseWorker stops and removes a base worker by name.
 */
int32
DeregisterBaseWorker(char *workerName)
{
	/*
	 * Delete base worker from the database.
	 */
	int			workerId = DeleteBaseWorkerRegistrationByName(workerName);

	/*
	 * Prepare for rollback by restarting the database starter. It will
	 * resurrect our base worker if it still sees it in the database.
	 */
	if (EnableBaseWorkerLauncher && !DatabaseIsTemplate(MyDatabaseId))
	{
		bool		waitForLock = true;

		/*
		 * Block the database starter on start-up, such that it waits for us
		 * to commit before reading from the database.
		 */
		LockDatabaseStarter(MyDatabaseId, ShareLock, waitForLock);
		DatabaseStarterNeedsRestart(MyDatabaseId);

		SignalServerStarter = true;
	}

	/* lock the shared hash, because we might change it */
	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	BaseWorkerEntry *baseWorkerEntry = GetBaseWorkerEntry(MyDatabaseId, workerId, &isFound);

	if (isFound)
	{
		/*
		 * If the worker is not running/started, there's nothing to do. The
		 * database starter will remove it when it sees no entry.
		 */
		if (baseWorkerEntry->workerPid > 0 || baseWorkerEntry->state == WORKER_STARTING)
		{
			/*
			 * Mark the base worker for restart in case the transaction fails.
			 */
			baseWorkerEntry->needsRestart = true;

			/*
			 * Kill the worker if it has a known PID. If its PID is not known,
			 * it is in a starting state and will kill itself in
			 * PgExtensionBaseWorkerMain when it sees needsRestart is true.
			 */
			if (baseWorkerEntry->workerPid > 0)
				kill(baseWorkerEntry->workerPid, SIGTERM);
		}
	}

	LWLockRelease(&BaseWorkerControl->lock);

	PG_RETURN_VOID();
}


/*
 * InsertBaseWorkerRegistration inserts an entry into pg_extension_base.workers.
 */
static int32
InsertBaseWorkerRegistration(char *workerName, Oid extensionId, Oid entryPointFunctionId)
{
	SPI_connect();

	char	   *extensionName = get_extension_name(extensionId);

	int			argCount = 3;
	Oid			argTypes[] = {TEXTOID, TEXTOID, OIDOID};
	Datum		argValues[] = {
		CStringGetTextDatum(workerName),
		CStringGetTextDatum(extensionName),
		ObjectIdGetDatum(entryPointFunctionId)
	};
	const char *argNulls = "   ";
	bool		readOnly = false;
	long		limit = 0;

	SPI_execute_with_args("insert into " PG_EXTENSION_BASE_SCHEMA_NAME ".workers "
						  "(worker_name, extension_name, entry_point_schema, entry_point_function) "
						  "select $1, $2, nspname, proname "
						  "from pg_namespace "
						  "join pg_proc on (pronamespace operator(pg_catalog.=) pg_namespace.oid) "
						  "where pg_proc.oid operator(pg_catalog.=) $3 "
						  "returning worker_id",
						  argCount, argTypes, argValues, argNulls,
						  readOnly, limit);

	if (SPI_processed != 1)
	{
		ereport(ERROR, (errmsg("could not find procedure %d", entryPointFunctionId)));
	}

	bool		isNull = false;
	Datum		workerIdDatum = GET_SPI_DATUM(0, 1, &isNull);

	if (isNull)
	{
		ereport(ERROR, (errmsg("unexpected NULL worker id")));
	}

	int32		workerId = DatumGetInt32(workerIdDatum);

	SPI_finish();

	return workerId;
}


/*
 * DatabaseIsTemplate returns whether the database is a template.
 */
static bool
DatabaseIsTemplate(Oid databaseId)
{
	HeapTuple	dbTuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));

	if (!HeapTupleIsValid(dbTuple))
	{
		elog(ERROR, "cache lookup failed for database %u", MyDatabaseId);
	}

	Form_pg_database dbForm = (Form_pg_database) GETSTRUCT(dbTuple);
	bool		isTemplate = dbForm->datistemplate;

	ReleaseSysCache(dbTuple);

	return isTemplate;
}


/*
 * DeleteBaseWorkerRegistrationByName deletes an entry from pg_extension_base.workers
 * by name.
 */
static int32
DeleteBaseWorkerRegistrationByName(char *workerName)
{
	SPI_connect();

	int			argCount = 1;
	Oid			argTypes[] = {TEXTOID};
	Datum		argValues[] = {
		CStringGetTextDatum(workerName)
	};
	const char *argNulls = " ";
	bool		readOnly = false;
	long		limit = 0;

	SPI_execute_with_args("delete from " PG_EXTENSION_BASE_SCHEMA_NAME ".workers "
						  "where worker_name operator(pg_catalog.=) $1 "
						  "returning worker_id",
						  argCount, argTypes, argValues, argNulls,
						  readOnly, limit);

	if (SPI_processed != 1)
		ereport(ERROR, (errmsg("could not find worker %s", workerName)));

	bool		isNull = false;
	Datum		workerIdDatum = GET_SPI_DATUM(0, 1, &isNull);

	if (isNull)
		ereport(ERROR, (errmsg("unexpected NULL worker id")));

	int32		workerId = DatumGetInt32(workerIdDatum);

	SPI_finish();

	return workerId;
}


/*
 * DeleteBaseWorkerRegistrationByExtensionId deletes all entries from pg_extension_base.workers
 * for a given extension.
 *
 * We use simple heap functions here instead of SPI, because we need to be able
 * to call this while dropping an extension, and extensions with planner/executor
 * hooks may run into trouble.
 */
static void
DeleteBaseWorkerRegistrationsByExtensionId(Oid extensionId)
{
	static const int Anum_pg_extension_base_workers_extension_name = 3;

	Oid			relationId = PgExtensionBaseWorkersRelationId();

	/*
	 * If the table does not exist, it typically means we are still on an
	 * older version of the extension and we have nothing to do here.
	 */
	if (relationId == InvalidOid)
		return;

	/*
	 * The extname is obtained as a Name, but get_extension_name converts to
	 * char * and it's more convenient to convert back than to reimplement.
	 */
	char	   *extensionNameStr = get_extension_name(extensionId);
	NameData	extensionName = {0};

	strlcpy(extensionName.data, extensionNameStr, NAMEDATALEN);

	int			scanKeyCount = 1;
	ScanKeyData scanKey[1];
	Relation	baseWorkersTable = table_open(PgExtensionBaseWorkersRelationId(), RowExclusiveLock);

	ScanKeyInit(&scanKey[0], Anum_pg_extension_base_workers_extension_name,
				BTEqualStrategyNumber, F_NAMEEQ, NameGetDatum(&extensionName));

	SysScanDesc scanDescriptor = systable_beginscan(baseWorkersTable, InvalidOid, false,
													NULL,
													scanKeyCount, scanKey);

	for (HeapTuple heapTuple = systable_getnext(scanDescriptor);
		 HeapTupleIsValid(heapTuple);
		 heapTuple = systable_getnext(scanDescriptor))
	{
		simple_heap_delete(baseWorkersTable, &heapTuple->t_self);
	}

	systable_endscan(scanDescriptor);

	CommandCounterIncrement();

	table_close(baseWorkersTable, NoLock);
}


/*
 * PgExtensionBaseWorkersRelationId returns the OID of the pg_extension_base.workers table.
 */
static Oid
PgExtensionBaseWorkersRelationId(void)
{
	Oid			schemaId = PgExtensionSchemaId();

	if (schemaId == InvalidOid)
		return InvalidOid;

	return get_relname_relid("workers", schemaId);
}


/*
 * PgExtensionSchemaId returns the OID of the pg_extension_base schema.
 */
static Oid
PgExtensionSchemaId(void)
{
	bool		missingOk = true;

	return get_namespace_oid(PG_EXTENSION_BASE_SCHEMA_NAME, missingOk);
}


/*
 * BaseWorkerProcessUtility ensures database-specific workers
 * are stopped when running DROP DATABASE commands.
 */
static void
BaseWorkerProcessUtility(PlannedStmt *pstmt,
						 const char *queryString,
						 bool readOnlyTree,
						 ProcessUtilityContext context,
						 ParamListInfo params,
						 struct QueryEnvironment *queryEnv,
						 DestReceiver *dest,
						 QueryCompletion *completionTag)
{
	Node	   *parsetree = pstmt->utilityStmt;

	/*
	 * DROP DATABASE should stop any associated database workers, otherwise it
	 * will never succeed.
	 *
	 * However, the DROP DATABASE might still succeed in which case the
	 * workers should be restarted.
	 */
	if (IsA(parsetree, DropdbStmt))
	{
		DropdbStmt *dropDbStatement = (DropdbStmt *) parsetree;
		char	   *databaseName = dropDbStatement->dbname;
		Oid			databaseId = get_database_oid(databaseName, true);

		if (databaseId != InvalidOid)
		{
			/*
			 * Stop database starter and all base workers, since the database
			 * is being dropped, but mark them for restart in case we roll
			 * back.
			 */
			PrepareForDrop(databaseId, InvalidOid);
		}
	}
	else if (IsA(parsetree, CreatedbStmt))
	{
		/*
		 * Wake up in case we're creating a database from a template that has
		 * base workers.
		 */
		SignalServerStarter = true;
	}

	PreviousProcessUtility(pstmt, queryString, readOnlyTree, context, params,
						   queryEnv, dest, completionTag);
}


/*
 * PrepareForDrop stops all workers for a given database or extension,
 * but also prepares to restart them in case of failure.
 *
 * This is used for DROP DATABASE, DROP EXTENSION pg_extension_base, and
 * DROP EXTENSION ext_with_workers.
 *
 * It is more crude than necessary for DROP EXTENSION ext_with_workers, since
 * we do not strictly have to kill the database starter when dropping a single
 * extension, but it simplifies the logic because restarting the database starter
 * will be blocked on the current transaction, which means we do not have to worry
 * about base workers getting restarted until the current transaction is done.
 */
static void
PrepareForDrop(Oid databaseId, Oid extensionId)
{
	/*
	 * We will start a new database starter, but block its creation until the
	 * current transaction is done.
	 *
	 * This is useful to prevent starting a database starter for a database
	 * that will be gone by the time it starts, which results in an error in
	 * the log.
	 *
	 * Blocking typically happens in StartDatabaseStarter by skipping over the
	 * database. Or in case of DROP EXTENSION, we may also block in
	 * PgExtensionBaseDatabaseStarterMain, which then exits if DROP EXTENSION
	 * commits.
	 */
	bool		waitForLock = true;

	LockDatabaseStarter(databaseId, ExclusiveLock, waitForLock);

	LWLockAcquire(&BaseWorkerControl->lock, LW_EXCLUSIVE);

	bool		isFound = false;
	DatabaseStarterEntry *starterEntry =
		GetDatabaseStarterEntry(databaseId, &isFound);

	if (isFound)
	{
		/*
		 * In case the DROP DATABASE / DROP EXTENSION, a database starter will
		 * need to come back to restart all the base workers that are
		 * currently running.
		 */
		starterEntry->needsRestart = true;

		/*
		 * If a workerPid is set, then kill the database starter. It's
		 * possible that PgExtensionBaseDatabaseStarterMain has not reached
		 * the point where it sets workerPid yet, but in that case it will
		 * exit as soon as it sees needsRestart.
		 */
		if (starterEntry->workerPid > 0)
		{
			/*
			 * There is already a database starter, possibly creating new base
			 * workers. We should kill it to proceed with the DROP and we no
			 * longer need it in case of success.
			 */
			kill(starterEntry->workerPid, SIGTERM);
		}
	}

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, BaseWorkerHash);

	BaseWorkerEntry *baseWorkerEntry;

	while ((baseWorkerEntry = (BaseWorkerEntry *) hash_seq_search(&status)) != 0)
	{
		if (baseWorkerEntry->key.databaseId == databaseId &&
			(extensionId == InvalidOid || baseWorkerEntry->extensionId == extensionId) &&
			(baseWorkerEntry->workerPid > 0 || baseWorkerEntry->state == WORKER_STARTING))
		{
			/*
			 * Mark the base worker for restart in case DROP DATABASE /
			 * EXTENSION fails.
			 *
			 * Note that we only attempt a restart if the worker is running
			 * right now to make sure long-running base workers survive if the
			 * current transaction aborts, but short-lived base workers are
			 * not restarted.
			 *
			 * This could cause an unexpected restart of a short-lived worker
			 * that was nearing the end of its intended life. The base workers
			 * should ultimately check themselves whether they still want to
			 * exist.
			 */
			baseWorkerEntry->needsRestart = true;

			/*
			 * Kill the worker if it has a known PID. If its PID is not known,
			 * it is in a starting state and will kill itself in
			 * PgExtensionBaseWorkerMain when it sees needsRestart is true.
			 */
			if (baseWorkerEntry->workerPid > 0)
			{
				kill(baseWorkerEntry->workerPid, SIGTERM);
			}
		}
	}

	/*
	 * Post-commit, wake up the server starter to recover more quickly.
	 */
	SignalServerStarter = true;

	LWLockRelease(&BaseWorkerControl->lock);
}


/*
 * BaseWorkerObjectAccessHook ensures workers are dropped when an
 * extension is dropped.
 */
static void
BaseWorkerObjectAccessHook(ObjectAccessType access, Oid classId, Oid objectId, int subId,
						   void *arg)
{
	if (PreviousObjectAccessHook)
	{
		PreviousObjectAccessHook(access, classId, objectId, subId, arg);
	}

	if (access == OAT_DROP && classId == ExtensionRelationId)
	{
		if (objectId == PgExtensionBaseExtensionId())
		{
			/* DROP EXTENSION pg_extension_base */

			/*
			 * Dropping the pg_extension_base extension is akin to dropping
			 * the database. We want all associated workers to stop, but we
			 * also need to prepare for the possibility of rollback.
			 */
			PrepareForDrop(MyDatabaseId, InvalidOid);

			/*
			 * Base worker registrations are implicitly deleted since the
			 * pg_extension_base.workers table is dropped.
			 *
			 * We defer shared memory cleanup until after commit by letting
			 * the database starter call RemoveBaseWorkerEntriesForDatabase.
			 */
		}
		else if (ExtensionExists(PG_EXTENSION_BASE_EXTENSION_NAME))
		{
			/* DROP EXTENSION ext_with_workers */

			/*
			 * Stop base workers for the extension being dropped, but mark
			 * them for restart in case we roll back.
			 */
			PrepareForDrop(MyDatabaseId, objectId);

			/*
			 * delete the base worker registrations from
			 * pg_extension_base.workers
			 */
			DeleteBaseWorkerRegistrationsByExtensionId(objectId);

			/*
			 * We defer shared memory cleanup until after commit by letting
			 * the database starter call
			 * RemoveBaseWorkerEntriesNotInRegistrationList.
			 */
		}
	}
}


/*
 * PgExtensionBaseExtensionId returns the OID of the pg_extension_base extension.
 */
static Oid
PgExtensionBaseExtensionId(void)
{
	bool		missingOk = true;

	return get_extension_oid(PG_EXTENSION_BASE_EXTENSION_NAME, missingOk);
}


/*
 * ExtensionExists returns true when we can find the extension in the
 * pg_extension catalogs.
 *
 * It is meant to be called outside of a transaction.
 */
static bool
ExtensionExists(char *extensionName)
{
	bool		missingOk = true;
	Oid			extensionOid = get_extension_oid(extensionName, missingOk);

	return (extensionOid != InvalidOid);
}


/*
 * GetDelayedTimestamp returns a timestamp n milliseconds into the future.
 */
static TimestampTz
GetDelayedTimestamp(int64 millis)
{
	TimestampTz now = GetCurrentTimestamp();

	return now + 1000 * millis;
}


/*
 * BaseWorkerTransactionCallback is a transaction callback that
 * signals the server starter after a transaction completed.
 *
 * This speeds up recovery in cases where we killed a background
 * worker and then rolled back, or created a database from a template
 * that contains a base worker registration.
 */
static void
BaseWorkerTransactionCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_ABORT:
			{
				if (SignalServerStarter)
				{
					LWLockAcquire(&BaseWorkerControl->lock, LW_SHARED);
					ProcSendSignal(BaseWorkerControl->serverStarterProcno);
					LWLockRelease(&BaseWorkerControl->lock);
					SignalServerStarter = false;
				}
				break;
			}

		default:
			break;
	}
}


/*
 * pg_extension_base_list_base_workers returns the state of the base workers in shared
 * memory.
 */
Datum
pg_extension_base_list_base_workers(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(&BaseWorkerControl->lock, LW_SHARED);

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, BaseWorkerHash);

	BaseWorkerEntry *baseWorkerEntry;

	while ((baseWorkerEntry = (BaseWorkerEntry *) hash_seq_search(&status)) != 0)
	{
		bool		nulls[] = {false, false, false, false, false};
		Datum		values[] = {
			ObjectIdGetDatum(baseWorkerEntry->key.databaseId),
			Int32GetDatum(baseWorkerEntry->key.workerId),
			ObjectIdGetDatum(baseWorkerEntry->extensionId),
			Int32GetDatum(baseWorkerEntry->workerPid),
			BoolGetDatum(baseWorkerEntry->needsRestart),
		};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	LWLockRelease(&BaseWorkerControl->lock);

	PG_RETURN_VOID();
}


/*
 * pg_extension_base_list_database_starters returns the state of the database starters
 * in shared memory.
 */
Datum
pg_extension_base_list_database_starters(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	LWLockAcquire(&BaseWorkerControl->lock, LW_SHARED);

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, DatabaseStarterHash);

	DatabaseStarterEntry *starterEntry;

	while ((starterEntry = (DatabaseStarterEntry *) hash_seq_search(&status)) != 0)
	{
		bool		nulls[] = {false, false, false};
		Datum		values[] = {
			ObjectIdGetDatum(starterEntry->databaseId),
			Int32GetDatum(starterEntry->workerPid),
			BoolGetDatum(starterEntry->needsRestart),
		};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}
	LWLockRelease(&BaseWorkerControl->lock);

	PG_RETURN_VOID();
}
