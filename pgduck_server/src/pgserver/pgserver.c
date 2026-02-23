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
 * Main logic for pgduck_server.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#include "c.h"
#include "miscadmin.h"
#include "pg_config_manual.h"
#include "postgres_fe.h"

#include <stdio.h>
#include <unistd.h>
#include <netdb.h>
#include <common/ip.h>
#include <pthread.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <utime.h>
#include <grp.h>

#include "libpq/pqcomm.h"
#include "nodes/pg_list.h"

#include "pgserver/pgserver.h"
#include "pgserver/client_threadpool.h"
#include "pgsession/pgsession.h"
#include "utils/pgduck_log_utils.h"


/*
 * PgClientThreadInitState contains the initial state that is passed to
 * new threads.
 */
typedef struct PgClientThreadInitState
{
	int			threadIndex;
	void	   *(*startFunction) (void *);
	PGClient   *pgClient;

}			PgClientThreadInitState;


/* copied from UNIXSOCK_PATH from PG source */
#define PG_UNIXSOCK_PATH(path, port, sockdir) \
		snprintf(path, sizeof(path), "%s/.s.PGSQL.%d", \
				 (sockdir), (port))

static int	create_and_bind_unix_socket(PGServer * server, char *unixSocketPath,
										char *unixSocketOwningGroup,
										int unixSocketPermissions,
										int port);
static int	acquire_domain_socket_lock_file(PGServer * server, int port);
static int	set_unix_socket_permissions(char *unixSocketPath, char *groupName,
										int permissionsMask);
static int	pgserver_create_client_thread(const PgClientThreadInitState * initState);
static void *pgclient_thread_main(void *arg);
static void pgclient_thread_cleanup(void *arg);
static void touch_internal_files(PGServer * pgServer, time_t now);

/*
 * pgserver_create initializes a PostgreSQL wire compatible server
 * and starts listening on the given port.
 */
int
pgserver_init(PGServer * pgServer,
			  char *unixSocketPath,
			  char *unixSocketOwningGroup,
			  int unixSocketPermissions,
			  int port)
{
	if (create_and_bind_unix_socket(pgServer,
									unixSocketPath,
									unixSocketOwningGroup,
									unixSocketPermissions,
									port) != 0)
		return STATUS_ERROR;

	pgServer->listeningPort = port;
	pgServer->startFunction = pgsession_handle_connection;
	pgServer->last_touch_time = time(NULL);

	PGDUCK_SERVER_LOG("pgduck_server is running with pid: %d", getpid());

	return STATUS_OK;
}

/*
 * Creates the unix domain socket, binds and listens to it. All errors are sent
 * to stderr, and then we return with STATUS_ERROR.
 *
 * This function is inspired by StreamServerPort() from
 * src/backend/libpq/pqcomm.c, but also heavily diverged. We only allow unix
 * domain sockets at the moment, and we do not have as much portability
 * requirements as Postgres has. Hence, our code is simpler than Postgres'.
 */
static int
create_and_bind_unix_socket(PGServer * server,
							char *unixSocketPath,
							char *unixSocketOwningGroup,
							int unixSocketPermissions,
							int port)
{
	/* calculate the socket path and create the socket */
	snprintf(server->unixSocketDir, MAXPGPATH, "%s", unixSocketPath);
	PG_UNIXSOCK_PATH(server->unixSocketPath, port, unixSocketPath);

	/* Initialize hint structure as pg_getaddrinfo_all relies on that */
	struct addrinfo hint;

	MemSet(&hint, 0, sizeof(hint));
	hint.ai_family = AF_UNIX;
	hint.ai_flags = AI_PASSIVE;
	hint.ai_socktype = SOCK_STREAM;

	struct addrinfo *addrs = NULL;

	/*
	 * The 'pg_getaddrinfo_all' function resolves the address information for
	 * the server. 'NULL' is passed for the hostname, indicating a local
	 * connection. 'server->unix_socket_path' specifies the path to the UNIX
	 * socket for the server. 'hint' provides hints about the type of socket
	 * to open. 'addrs' will hold the resolved address information.
	 */
	int			ret = pg_getaddrinfo_all(NULL, server->unixSocketPath, &hint, &addrs);

	if ((ret != STATUS_OK) || addrs == NULL)
	{
		PGDUCK_SERVER_ERROR("could not translate service \"%s\" to address: %s",
							server->unixSocketPath, gai_strerror(ret));

		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);

		return STATUS_ERROR;
	}

	server->listeningSocket = socket(addrs->ai_family, SOCK_STREAM, 0);
	if (server->listeningSocket == PGINVALID_SOCKET)
	{
		PGDUCK_SERVER_ERROR("could not create Unix socket for address \"%s\": %m",
							server->unixSocketPath);

		pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	if (strlen(server->unixSocketPath) >= UNIXSOCK_PATH_BUFLEN)
	{
		PGDUCK_SERVER_ERROR("Unix-domain socket path \"%s\" is too long (maximum %d bytes)",
							server->unixSocketPath, (int) (UNIXSOCK_PATH_BUFLEN - 1));

		pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	/*
	 * We use a lock file mechanism to avoid conflicts. We first attempt to
	 * acquire a lock on 'lock_file'. If the lock is acquired, it indicates no
	 * other process is using the socket, and the function proceeds to safely
	 * remove the existing socket file 'socket_file' using unlink().
	 *
	 * The socket is then bound using bind(). If the lock cannot be acquired,
	 * it means another process is currently using the socket, and the
	 * function will not proceed with the binding. This approach avoids issues
	 * like silent overwrites and race conditions encountered when two
	 * instances try to bind to the same socket simultaneously.
	 *
	 * We never unlink the lock file, the underlying operating system will do
	 * it for us when the process exists (even crashes).
	 */
	if (acquire_domain_socket_lock_file(server, port) != STATUS_OK)
	{
		pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	/*
	 * Once we have the interlock, we can safely delete any pre-existing
	 * socket file to avoid failure at bind() time.
	 */
	(void) unlink(server->unixSocketPath);

	if (bind(server->listeningSocket, addrs->ai_addr, addrs->ai_addrlen) != STATUS_OK)
	{
		PGDUCK_SERVER_ERROR("could not bind Unix-socket address \"%s\": %m\n " \
							"Is another pgduck_server already running on port %d?",
							server->unixSocketPath, port);

		pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	if (set_unix_socket_permissions(server->unixSocketPath,
									unixSocketOwningGroup,
									unixSocketPermissions) != STATUS_OK)
	{
		pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	const int	listenQueueSize = MaxThreads;

	if (listen(server->listeningSocket, listenQueueSize) != STATUS_OK)
	{
		PGDUCK_SERVER_ERROR("Could not listen to socket: %s", strerror(errno));

		pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	pg_freeaddrinfo_all(hint.ai_family, addrs);
	return STATUS_OK;
}

/*
 * Acquire a lockfile for the specified Unix socket file.
 */
static int
acquire_domain_socket_lock_file(PGServer * server, int port)
{
	/* no lock file for abstract sockets */
	if (server->unixSocketPath[0] == '@')
		return STATUS_OK;

	snprintf(server->lockFilePath, MAXPGPATH, "%s.lock",
			 server->unixSocketPath);

	int			lockFileDesc = open(server->lockFilePath, O_RDONLY | O_CREAT, 0600);

	if (lockFileDesc == STATUS_ERROR)
	{
		PGDUCK_SERVER_ERROR("could not open the lock file \"%s\"\n " \
							"Is another pgduck_server already running on port %d?",
							server->unixSocketPath, port);

		return STATUS_ERROR;
	}

	if (flock(lockFileDesc, LOCK_EX | LOCK_NB) != STATUS_OK)
	{
		PGDUCK_SERVER_ERROR("could not bind Unix-socket address \"%s\" " \
							"Is another pgduck_server already running on port %d?",
							server->unixSocketPath, port);

		return STATUS_ERROR;
	}

	return STATUS_OK;
}


/*
 * set_unix_socket_permissions sets the owning group and chmod permissions of
 * the unix socket path.
 *
 * Mostly copied from Setup_AF_UNIX with variable names preserved.
 */
static int
set_unix_socket_permissions(char *unixSocketPath, char *groupName, int permissionsMask)
{
	/* no file system permissions for abstract sockets */
	if (unixSocketPath[0] == '@')
		return STATUS_OK;

	if (groupName[0] != '\0')
	{
		char	   *endptr;
		unsigned long val;
		gid_t		gid;

		val = strtoul(groupName, &endptr, 10);
		if (*endptr == '\0')
		{						/* numeric group id */
			gid = val;
		}
		else
		{						/* convert group name to id */
			struct group *gr;

			gr = getgrnam(groupName);
			if (!gr)
			{
				PGDUCK_SERVER_ERROR("group \"%s\" does not exist", groupName);
				return STATUS_ERROR;
			}
			gid = gr->gr_gid;
		}
		if (chown(unixSocketPath, -1, gid) == -1)
		{
			PGDUCK_SERVER_ERROR("could not set grou of socket file \"%s\": %m\n",
								unixSocketPath);
			return STATUS_ERROR;
		}
	}

	if (chmod(unixSocketPath, permissionsMask) == -1)
	{
		PGDUCK_SERVER_ERROR("could not set Unix-socket address \"%s\" permissions: %m\n",
							unixSocketPath);

		return STATUS_ERROR;
	}

	return STATUS_OK;
}


static volatile sig_atomic_t running = 1;

/* basic sigint handler */
static void
handle_signal(int sig)
{
	running = 0;
}


/*
 * pgserver_run is the main loop for the PostgreSQL wire compatible server.
 */
int
pgserver_run(PGServer * pgServer)
{
	/* install signal handlers */
	struct sigaction sa;

	/* Use our custom handler */
	sa.sa_handler = handle_signal;

	/* Do not block any other signals during handling */
	sigemptyset(&sa.sa_mask);

	/* CRITICAL: Do NOT set the SA_RESTART flag. */
	/* This ensures that system calls like accept() are interrupted. */
	sa.sa_flags = 0;

	/* Install the handler for SIGINT and SIGTERM */
	if (sigaction(SIGINT, &sa, NULL) == -1)
	{
		perror("sigaction for SIGINT failed");
		exit(STATUS_ERROR);
	}
	if (sigaction(SIGTERM, &sa, NULL) == -1)
	{
		perror("sigaction for SIGTERM failed");
		exit(STATUS_ERROR);
	}

	while (running)
	{
		PGClient   *client = (PGClient *) pg_malloc0(sizeof(PGClient));
		socklen_t	clientAddrLen = sizeof(client->clientAddress);

		client->clientSocket =
			accept(pgServer->listeningSocket,
				   (struct sockaddr *) &client->clientAddress, &clientAddrLen);

		if (client->clientSocket < 0)
		{
			PGDUCK_SERVER_ERROR("Could not accept the client: %s",
								strerror(errno));

			/*
			 * TODO: We can probably recover from this error, but lets handle
			 * errors gracefully in the future.
			 */
			exit(STATUS_ERROR);
		}

		/*
		 * Touch Unix socket and lock files every 58 minutes, to ensure that
		 * they are not removed by overzealous /tmp-cleaning tasks.  We assume
		 * no one runs cleaners with cutoff times of less than an hour ...
		 *
		 * Note that normally you'd expect this code to run even if there are
		 * no clients, but we are not doing that. When there are no clients,
		 * we are blocked on the accept() system call. We currently rely on
		 * the fact that every 10 seconds, pg_lake_manage_cache() is called,
		 * guarantees that there is at least one new client.
		 */
		time_t		now = time(NULL);

		if (now - pgServer->last_touch_time >= 58 * SECS_PER_MINUTE)
			touch_internal_files(pgServer, now);

		/* first check if we have available threads */
		int			threadIndex = pgclient_threadpool_reserve_slot(client);

		if (threadIndex == InvalidThreadIndex)
		{
			PGDUCK_SERVER_LOG("A new client rejected as it exceeds %d clients", MaxAllowedClients);

			/* TODO: send error message to the client */
			close(client->clientSocket);
			pg_free(client);
			continue;
		}

		/* state to pass into pgclient_thread_main and pgclient_thread_cleanup */
		PgClientThreadInitState *initState =
			(PgClientThreadInitState *) pg_malloc0(sizeof(PgClientThreadInitState));

		initState->threadIndex = threadIndex;
		initState->startFunction = pgServer->startFunction;
		initState->pgClient = client;

		if (pgserver_create_client_thread(initState) != OK)
		{
			PGDUCK_SERVER_ERROR("Thread creation failed for client %d", client->clientSocket);

			close(client->clientSocket);
			pg_free(client);
			pg_free(initState);
			pgclient_threadpool_free_slot(threadIndex);
			continue;
		}
	}

	PGDUCK_SERVER_LOG("Done running");

	return STATUS_OK;
}


/*
 * pgserver_create_client_thread creates a new thread for the client.
 * We use PTHREAD_CREATE_DETACHED so that we don't have to join the threads.
 */
static int
pgserver_create_client_thread(const PgClientThreadInitState * initState)
{
	pthread_t	threadId;
	pthread_attr_t threadAttr;

	pthread_attr_init(&threadAttr);
	pthread_attr_setdetachstate(&threadAttr, PTHREAD_CREATE_DETACHED);

	int			isThreadCreated = pthread_create(&threadId,
												 &threadAttr,
												 pgclient_thread_main,
												 (void *) initState);

	if (isThreadCreated != 0)
	{
		PGDUCK_SERVER_ERROR("Thread creation failed with %d", isThreadCreated);

		/* TODO: send error message to the client */
		pthread_attr_destroy(&threadAttr);

		return STATUS_ERROR;
	}

	pthread_attr_destroy(&threadAttr);

	return STATUS_OK;
}


/*
 * pgclient_thread_main is the main entry-point for a client thread.
 *
 * This function is responsible for executing the client thread logic. It takes a
 * pointer to a PgClientThreadInitState structure as an argument, which contains the
 * necessary data for the thread to start. The function calls the startFunction
 * specified in the initState structure and runs until the client exits.
 */
static void *
pgclient_thread_main(void *arg)
{
	PgClientThreadInitState *initState = (PgClientThreadInitState *) arg;

	/* cleanup handler */
	pthread_cleanup_push(pgclient_thread_cleanup, arg);

	/* runs until the client exists */
	initState->startFunction(initState->pgClient);

	/*
	 * The '-1' argument tells pthread_cleanup_pop to execute the cleanup
	 * handler even if we exit normally, from this code path. We want all the
	 * cleanup to be centralized in the cleanup handler for both normal and
	 * abnormal exits (e.g., pthread_cancel when query cancelled).
	 */
	pthread_cleanup_pop(-1);

	return NULL;
}


/*
 * pgclient_thread_cleanup is called when a client thread is exiting. It updates the
 * thread's status in the thread pool and logs a debug message indicating the thread's
 * exit.
 */
static void
pgclient_thread_cleanup(void *arg)
{
	PgClientThreadInitState *initState = (PgClientThreadInitState *) arg;

	/* end of the thread, free the pre-thread resources */
	pgclient_threadpool_free_slot(initState->threadIndex);
	closesocket(initState->pgClient->clientSocket);
	pg_free(initState->pgClient);
	pg_free(initState);
}


/*
 * cleanup on successful exists.
 *
 * TODO: not called ever yet
 */
int
pgserver_destroy(PGServer * pgServer)
{
	closesocket(pgServer->listeningSocket);

	return STATUS_OK;
}


/*
 * touch_internal_files -- mark socket and lock files as recently accessed
 *
 * Adopted from Postgres source code, TouchSocketFiles().
 *
 * This routine should be called every so often to ensure that the socket
 * files have a recent mod date (ordinary operations on sockets usually won't
 * change the mod date).  That saves them from being removed by
 * overenthusiastic /tmp-directory-cleaner daemons.  (Another reason we should
 * never have put the socket file in /tmp...)
 */
static void
touch_internal_files(PGServer * pgServer, time_t now)
{
	/* no files for abstract sockets */
	if (pgServer->unixSocketPath[0] != '@')
	{
		/* Ignore errors; there's no point in complaining */
		(void) utime(pgServer->unixSocketPath, NULL);
		(void) utime(pgServer->lockFilePath, NULL);
	}

	pgServer->last_touch_time = now;
}
