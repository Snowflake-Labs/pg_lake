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
 * This file contains the implementation of the client thread logic.
 * Each client thread is responsible handling the communication with a single
 * client. The client thread is created when a new client connects to the server
 * and is destroyed when the client disconnects.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#include "c.h"
#include "postgres_fe.h"
#include "storage/procsignal.h"

#include <stdio.h>
#include <unistd.h>
#include <sys/resource.h>

#include "pgserver/client_threadpool.h"
#include "utils/pgduck_log_utils.h"


/*
 * PgClientThreadState contains the state of a thread in the client
 * thread pool that we use to recognize cancellations.
 */
typedef struct PgClientThreadState
{
	/* index in the thread pool */
	int			threadIndex;

	/* whether the thread is started */
	bool		isStarted;

	/* cancellation token of the thread */
	int			cancellationProcId;
#if PG_VERSION_NUM >= 180000
	uint8	   *cancellationToken;
	size_t		cancellationTokenSize;
#else
	int32		cancellationToken;
#endif

	/*
	 * DuckDB connection to interrupt.
	 *
	 * The cancel paths call duckdb_interrupt() on this pointer, so it must be
	 * NULL whenever the connection behind it is not alive.  The thread owning
	 * the slot has to clear it (set_duckdb_conn with NULL) *before* it
	 * disconnects, not when it frees the slot.
	 */
	duckdb_connection duckdbConnection;

}			PgClientThreadState;

int			MaxAllowedClients;
int			MaxThreads;

/*
 * After clamp_cap_to_active() lowers MaxAllowedClients, restore the configured
 * value once live clients fall to this.  5 is arbitrary: low enough that a
 * burst has clearly passed, high enough that we do not wait for a completely
 * idle process.
 */
#define CLAMP_RESTORE_CLIENTS 5

/*
 * max_clients as configured.  MaxAllowedClients can be lowered below this at
 * runtime when the OS refuses a thread, and is restored from here once live
 * clients drop to CLAMP_RESTORE_CLIENTS; see clamp_cap_to_active and free_slot.
 */
static int	ConfiguredMaxClients;

/* all accesses to ClientThreadPool should happen while holding a lock */
static PgClientThreadState * ClientThreadPool;
static int	ActiveClientThreadCount = 0;


/*
 * We currently use a single rwlock for all operations that accesses the
 * shared memory. In practice, the access to the shared memory happens
 * very infrequently, so we do not expect to have a performance problem.
 *
 * We access the shared memory in the following cases:
 * 1. When we add a new client to the shared memory.
 * 2. When we remove a client from the shared memory.
 *
 * So, unless the clients are not frequently added or removed, such
 * as pgbench -C option, we do not expect to have a performance problem.
 */
static pthread_rwlock_t rwlock;

/*
* Keep track of the first available index in the thread array. Always access
* while holding the thread_pool_mutex.
*/
static int	ThreadPoolAvailableIndexStart = 0;


/*
 * pgclient_threadpool_init allocates memory for the client thread pool based on the
 * maximum allowed clients.
 * The allocated memory is initialized to zero using pg_malloc0.
 */
void
pgclient_threadpool_init(int maxAllowedClients)
{
	/*
	 * We multiply by 2 because we might need to cancel a client (e.g.,
	 * thread) and cancellations require a slot in the thread pool.
	 *
	 * We could perhaps use some smaller number, for now let's keep it simple.
	 */
	MaxAllowedClients = maxAllowedClients;
	ConfiguredMaxClients = maxAllowedClients;
	MaxThreads = maxAllowedClients * 2;

	/* pg_malloc0 exists the program in case cannot allocate */
	ClientThreadPool = (PgClientThreadState *) pg_malloc0(sizeof(PgClientThreadState) * MaxThreads);

	int			rwLockCreated = pthread_rwlock_init(&rwlock, NULL);

	if (rwLockCreated != 0)
	{
		PGDUCK_SERVER_ERROR("Failed to create rwlock with %d", rwLockCreated);
		exit(STATUS_ERROR);
	}
}


/*
 * describe_nproc_limit renders RLIMIT_NPROC for the log, so that a reader can
 * tell which wall we hit.  pthread_create reports EAGAIN both for "no more
 * threads allowed" and for "could not get the resources for one", so the
 * errno alone does not say whether the thread limit or memory was the
 * constraint.  A live thread count well under this limit means memory.
 */
static const char *
describe_nproc_limit(char *buf, size_t buflen)
{
	struct rlimit rlim;

	if (getrlimit(RLIMIT_NPROC, &rlim) != 0)
		return "unknown";

	if (rlim.rlim_cur == RLIM_INFINITY)
		return "unlimited";

	snprintf(buf, buflen, "%llu", (unsigned long long) rlim.rlim_cur);

	return buf;
}


/*
 * pgclient_threadpool_clamp_cap_to_active lowers the client cap to the number
 * of client threads we are currently carrying.
 *
 * Called when the OS refuses us a new client thread.  max_clients is a promise
 * we cannot keep once the host runs out of threads or memory first, and the
 * real ceiling is not something we can read up front.  There is no portable
 * way to ask for it (sysconf(_SC_THREAD_THREADS_MAX) is indeterminate),
 * RLIMIT_NPROC is a per-uid budget shared with the user's other processes, a
 * cgroup pids.max is invisible to us, and when memory is the binding
 * constraint none of those numbers describe it at all.  A failed
 * pthread_create is the one moment the OS tells us what this host actually
 * sustains, so record it and let the ordinary capacity check in
 * reserve_slot() enforce it from then on.  Otherwise every later connection
 * repeats the same failed pthread_create and the cap never engages.
 *
 * The cap is restored once live clients drop to CLAMP_RESTORE_CLIENTS; see
 * free_slot.
 */
void
pgclient_threadpool_clamp_cap_to_active(void)
{
	int			loweredTo = 0;
	int			liveThreads = 0;
	int			previousCap = 0;

	pthread_rwlock_wrlock(&rwlock);

	/*
	 * The count still includes the slot reserved for the thread that failed,
	 * so what we have proven we can carry is one less than that.
	 */
	int			sustainable = Max(ActiveClientThreadCount - 1, 1);

	liveThreads = ActiveClientThreadCount;
	previousCap = MaxAllowedClients;

	if (sustainable < MaxAllowedClients)
	{
		MaxAllowedClients = sustainable;
		loweredTo = sustainable;
	}

	pthread_rwlock_unlock(&rwlock);

	if (loweredTo > 0)
	{
		char		limitBuf[32];

		PGDUCK_SERVER_WARN("the OS refused a new client thread: %d client "
						   "threads live, max_clients %d, RLIMIT_NPROC %s. "
						   "Lowering max_clients to %d until clients drop to %d. "
						   "A live count well below RLIMIT_NPROC means memory, "
						   "not the thread limit, is the constraint",
						   liveThreads, previousCap,
						   describe_nproc_limit(limitBuf, sizeof(limitBuf)),
						   loweredTo, CLAMP_RESTORE_CLIENTS);
	}
}


/*
 * pgclient_threadpool_reserve_slot finds an available thread slot and assigns
 * the cancellation token from the given PGClient.
 *
 * It returns the index of the thread slot that was used, or InvalidThreadIndex
 * if no slot was available.
 */
int
pgclient_threadpool_reserve_slot(PGClient * client)
{
	int			usedThreadIndex = InvalidThreadIndex;
	int			cancellationProcId = 0;

	pg_strong_random(&cancellationProcId, sizeof(int32));

	/* generate a cancellation key before reserving a slot */
#if PG_VERSION_NUM >= 180000
	size_t		cancellationTokenSize = 4;
	uint8	   *cancellationToken = palloc0(cancellationTokenSize);

	pg_strong_random(cancellationToken, cancellationTokenSize);
#else
	int32		cancellationToken = 0;

	pg_strong_random(&cancellationToken, sizeof(int32));
#endif

	pthread_rwlock_wrlock(&rwlock);

	if (ActiveClientThreadCount >= MaxAllowedClients)
	{
		pthread_rwlock_unlock(&rwlock);

#if PG_VERSION_NUM >= 180000
		pfree(cancellationToken);
#endif

		return InvalidThreadIndex;
	}

	for (int threadIndex = ThreadPoolAvailableIndexStart; threadIndex < MaxThreads; threadIndex++)
	{
		if (!ClientThreadPool[threadIndex].isStarted)
		{
			usedThreadIndex = threadIndex;
			ClientThreadPool[threadIndex].isStarted = true;
			ClientThreadPool[threadIndex].threadIndex = threadIndex;
			ClientThreadPool[threadIndex].cancellationProcId = cancellationProcId;
			ClientThreadPool[threadIndex].cancellationToken = cancellationToken;
#if PG_VERSION_NUM >= 180000
			ClientThreadPool[threadIndex].cancellationTokenSize = cancellationTokenSize;
#endif

			++ActiveClientThreadCount;

			break;
		}
	}

	/* keep track of the first available index */
	if (usedThreadIndex >= ThreadPoolAvailableIndexStart)
		ThreadPoolAvailableIndexStart = usedThreadIndex + 1;

	pthread_rwlock_unlock(&rwlock);

	/* store the thread index, to assign the DuckDB connection later */
	client->threadIndex = usedThreadIndex;

	if (usedThreadIndex == InvalidThreadIndex)
	{
		/* no slot assigned, so nothing will ever use or free the token */
#if PG_VERSION_NUM >= 180000
		pfree(cancellationToken);
#endif

		return InvalidThreadIndex;
	}

	/* store the cancellation token, to be transmitted to the client later */
	client->cancellationProcId = cancellationProcId;
	client->cancellationToken = cancellationToken;
#if PG_VERSION_NUM >= 180000
	client->cancellationTokenSize = cancellationTokenSize;
#endif

	return usedThreadIndex;
}


/*
 * pgclient_threadpool_free_slot frees the thread pool slot at the given index
 * after a thread has exited.
 */
void
pgclient_threadpool_free_slot(int threadIndex)
{
	pthread_rwlock_wrlock(&rwlock);

	PgClientThreadState *threadState = &ClientThreadPool[threadIndex];

	if (!threadState->isStarted)
	{
		/* thread is in an unexpected state, panic! */
		pthread_rwlock_unlock(&rwlock);
		PGDUCK_SERVER_ERROR("Thread index %d is not in a started state on clean up",
							threadIndex);
		exit(STATUS_ERROR);
	}

	/* clean up the thread state */
	threadState->isStarted = false;
	threadState->cancellationProcId = 0;
	threadState->cancellationToken = 0;
#if PG_VERSION_NUM >= 180000
	threadState->cancellationTokenSize = 0;
#endif
	threadState->duckdbConnection = NULL;

	--ActiveClientThreadCount;

	/* keep track of the first available index */
	if (threadIndex <= ThreadPoolAvailableIndexStart)
		ThreadPoolAvailableIndexStart = threadState->threadIndex;

	/*
	 * Burst has passed.  If clamp_cap_to_active() lowered the cap earlier,
	 * the EAGAIN behind it may well have been passing memory pressure rather
	 * than a standing ceiling, pthread_create reports the same error for
	 * both, so let the configured value apply again instead of staying shrunk
	 * for the life of the process.  If the ceiling is real we relearn it at
	 * the cost of one more failed pthread_create.
	 */
	bool		capRestored = false;

	if (ActiveClientThreadCount <= CLAMP_RESTORE_CLIENTS &&
		MaxAllowedClients < ConfiguredMaxClients)
	{
		MaxAllowedClients = ConfiguredMaxClients;
		capRestored = true;
	}

	pthread_rwlock_unlock(&rwlock);

	if (capRestored)
		PGDUCK_SERVER_LOG("clients dropped to %d; restoring max_clients to %d",
						  ActiveClientThreadCount, ConfiguredMaxClients);
}

/*
* pgclient_threadpool_cancel_thread cancels the thread that is running the query with the given
* cancellation_proc_id and cancellation_token. It returns the index of the thread that was
* cancelled, or InvalidThreadIndex if no thread was found.
*/
#if PG_VERSION_NUM >= 180000
int
pgclient_threadpool_cancel_thread(int cancellationProcId, uint8 *cancellationToken,
								  size_t cancellationTokenSize)
{
	int			usedThreadIndex = InvalidThreadIndex;

	pthread_rwlock_rdlock(&rwlock);
	for (int threadIndex = 0; threadIndex < MaxThreads; threadIndex++)
	{
		if (ClientThreadPool[threadIndex].isStarted &&
			ClientThreadPool[threadIndex].cancellationTokenSize == cancellationTokenSize &&
			memcmp(ClientThreadPool[threadIndex].cancellationToken, cancellationToken, cancellationTokenSize) == 0 &&
			ClientThreadPool[threadIndex].cancellationProcId == cancellationProcId)
		{
			usedThreadIndex = threadIndex;
			duckdb_connection conn = ClientThreadPool[threadIndex].duckdbConnection;

			/*
			 * As per DuckDB docs, duckdb connections are thread safe so we
			 * can safely interrupt it from another thread.
			 *
			 * We do the interrupt while holding the threadpool lock.
			 * Otherwise, a thread could end just before we call
			 * duckdb_interrupt. Luckily, duckdb_interrupt will only set an
			 * atomic<bool> flag.
			 */
			duckdb_interrupt(conn);
			break;
		}
	}

	pthread_rwlock_unlock(&rwlock);

	return usedThreadIndex;
}
#else
int
pgclient_threadpool_cancel_thread(int cancellationProcId, int32 cancellationToken)
{
	int			usedThreadIndex = InvalidThreadIndex;

	pthread_rwlock_rdlock(&rwlock);
	for (int threadIndex = 0; threadIndex < MaxThreads; threadIndex++)
	{
		if (ClientThreadPool[threadIndex].isStarted &&
			ClientThreadPool[threadIndex].cancellationToken == cancellationToken &&
			ClientThreadPool[threadIndex].cancellationProcId == cancellationProcId)
		{
			usedThreadIndex = threadIndex;
			duckdb_connection conn = ClientThreadPool[threadIndex].duckdbConnection;

			/*
			 * As per DuckDB docs, duckdb connections are thread safe so we
			 * can safely interrupt it from another thread.
			 *
			 * We do the interrupt while holding the threadpool lock.
			 * Otherwise, a thread could end just before we call
			 * duckdb_interrupt. Luckily, duckdb_interrupt will only set an
			 * atomic<bool> flag.
			 */
			duckdb_interrupt(conn);
			break;
		}
	}

	pthread_rwlock_unlock(&rwlock);

	return usedThreadIndex;
}
#endif

/*
 * pgclient_threadpool_set_duckdb_conn sets the DuckDB connection for a given
 * thread. We use this do duckdb_interrupt when a cancellation comes in.
 */
void
pgclient_threadpool_set_duckdb_conn(int threadIndex, duckdb_connection conn)
{
	pthread_rwlock_wrlock(&rwlock);

	PgClientThreadState *threadState = &ClientThreadPool[threadIndex];

	threadState->duckdbConnection = conn;

	pthread_rwlock_unlock(&rwlock);
}


/*
 * pgclient_threadpool_cancel_all interrupts every active DuckDB query.
 *
 * Called during server shutdown so that client threads get a clean
 * interruption error instead of an abrupt connection reset when the
 * process exits.  Only sets the DuckDB interrupt flag (an atomic bool),
 * so this is cheap and safe to call from the main thread.
 *
 * Returns the number of active threads that were interrupted.
 */
int
pgclient_threadpool_cancel_all(void)
{
	int			interrupted = 0;

	pthread_rwlock_rdlock(&rwlock);

	for (int i = 0; i < MaxThreads; i++)
	{
		if (ClientThreadPool[i].isStarted &&
			ClientThreadPool[i].duckdbConnection != NULL)
		{
			duckdb_interrupt(ClientThreadPool[i].duckdbConnection);
			interrupted++;
		}
	}

	pthread_rwlock_unlock(&rwlock);

	return interrupted;
}
