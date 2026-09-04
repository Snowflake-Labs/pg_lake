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
 * Threads the process needs for itself (the accept loop, DuckDB's scheduler,
 * etc.) plus slack for the user's other processes.  Reserved out of
 * RLIMIT_NPROC before we derive how many client threads we can admit.
 */
#define THREAD_RLIMIT_HEADROOM 64


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
 * clamp_max_threads_to_rlimit lowers a requested thread count so that it stays
 * within what the OS will actually let us create.
 *
 * Every client, and every in-flight cancellation, runs on its own OS thread,
 * so the pool must not admit more than RLIMIT_NPROC allows -- otherwise
 * pthread_create() starts failing with EAGAIN and admission control never
 * engages.  We reserve headroom for the process's own threads and the user's
 * other processes, then clamp to what remains.
 *
 * This is best effort: RLIMIT_NPROC is counted across the whole real user id,
 * and a cgroup pids.max limit is invisible here, so the accept-loop
 * backpressure in pgserver_run() remains the backstop when the true limit is
 * lower than we can see.  Returns the request unchanged when the limit is
 * unknown or unlimited.
 */
static int
clamp_max_threads_to_rlimit(int requestedMaxThreads)
{
	struct rlimit rlim;

	if (getrlimit(RLIMIT_NPROC, &rlim) != 0 || rlim.rlim_cur == RLIM_INFINITY)
		return requestedMaxThreads;

	rlim_t		reserved = Max(rlim.rlim_cur / 5, THREAD_RLIMIT_HEADROOM);

	/* limit is smaller than our headroom; leave it to the backpressure path */
	if (reserved >= rlim.rlim_cur)
		return requestedMaxThreads;

	rlim_t		available = rlim.rlim_cur - reserved;

	if (available >= (rlim_t) requestedMaxThreads)
		return requestedMaxThreads;

	/* available < requestedMaxThreads, so it fits in an int */
	return (int) available;
}

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
	MaxThreads = clamp_max_threads_to_rlimit(maxAllowedClients * 2);
	MaxAllowedClients = Max(MaxThreads / 2, 1);

	/* keep the 2x cancellation headroom exact after any clamping */
	MaxThreads = MaxAllowedClients * 2;

	if (MaxAllowedClients < maxAllowedClients)
		PGDUCK_SERVER_LOG("max_clients lowered from %d to %d to stay within the "
						  "host thread limit (RLIMIT_NPROC)",
						  maxAllowedClients, MaxAllowedClients);

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

	pthread_rwlock_unlock(&rwlock);
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
