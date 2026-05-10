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
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <common/ip.h>
#include <pthread.h>
#include <signal.h>
#include <sys/fcntl.h>
#include <sys/file.h>
#include <sys/socket.h>
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
static int	create_and_bind_tcp_sockets(PGServer * server,
										char *tcpListenAddresses,
										int port);
static int	bind_one_tcp_addr(PGServer * server, const char *address, int port);
static int	acquire_domain_socket_lock_file(PGServer * server, int port);
static int	set_unix_socket_permissions(char *unixSocketPath, char *groupName,
										int permissionsMask);
static int	pgserver_create_client_thread(const PgClientThreadInitState * initState);
static void *pgclient_thread_main(void *arg);
static void pgclient_thread_cleanup(void *arg);
static void touch_internal_files(PGServer * pgServer, time_t now);
static int	dispatch_accepted_client(PGServer * pgServer, int listeningSocket);

/*
 * pgserver_create initializes a PostgreSQL wire compatible server
 * and starts listening on the given port. Always creates the Unix
 * domain socket; additionally binds TCP sockets when
 * `tcpListenAddresses` is non-NULL and non-empty.
 */
int
pgserver_init(PGServer * pgServer,
			  char *unixSocketPath,
			  char *unixSocketOwningGroup,
			  int unixSocketPermissions,
			  char *tcpListenAddresses,
			  int port)
{
	pgServer->numTcpSockets = 0;

	if (create_and_bind_unix_socket(pgServer,
									unixSocketPath,
									unixSocketOwningGroup,
									unixSocketPermissions,
									port) != 0)
		return STATUS_ERROR;

	if (tcpListenAddresses != NULL && tcpListenAddresses[0] != '\0')
	{
		if (create_and_bind_tcp_sockets(pgServer, tcpListenAddresses, port) != STATUS_OK)
			return STATUS_ERROR;
	}

	pgServer->listeningPort = port;
	pgServer->startFunction = pgsession_handle_connection;
	pgServer->last_touch_time = time(NULL);

	PGDUCK_SERVER_LOG("pgduck_server is running with pid: %d (unix=%s, tcp_listeners=%d)",
					  getpid(), pgServer->unixSocketPath, pgServer->numTcpSockets);

	return STATUS_OK;
}

/*
 * Bind a TCP listening socket per resolved address. `tcpListenAddresses`
 * is a comma-separated list of addresses (e.g., "0.0.0.0,::") with
 * PostgreSQL-style semantics. Each address is resolved via
 * pg_getaddrinfo_all and bound on the same `port` as the Unix socket.
 *
 * On any failure, errors logged and STATUS_ERROR returned. Caller is
 * responsible for cleanup; pgserver_destroy closes whatever sockets
 * were successfully created.
 */
static int
create_and_bind_tcp_sockets(PGServer * server, char *tcpListenAddresses, int port)
{
	char	   *list_copy = strdup(tcpListenAddresses);

	if (list_copy == NULL)
	{
		PGDUCK_SERVER_ERROR("could not strdup listen_addresses: %m");
		return STATUS_ERROR;
	}

	char	   *saveptr = NULL;
	char	   *token = strtok_r(list_copy, ",", &saveptr);

	while (token != NULL)
	{
		/* trim leading whitespace */
		while (*token == ' ' || *token == '\t')
			token++;

		/* trim trailing whitespace */
		size_t		len = strlen(token);

		while (len > 0 && (token[len - 1] == ' ' || token[len - 1] == '\t'))
			token[--len] = '\0';

		if (*token != '\0')
		{
			if (bind_one_tcp_addr(server, token, port) != STATUS_OK)
			{
				free(list_copy);
				return STATUS_ERROR;
			}
		}

		token = strtok_r(NULL, ",", &saveptr);
	}

	free(list_copy);

	if (server->numTcpSockets == 0)
	{
		PGDUCK_SERVER_ERROR("listen_addresses set but no TCP sockets bound — empty value?");
		return STATUS_ERROR;
	}

	return STATUS_OK;
}

/*
 * Resolve a single address (e.g., "0.0.0.0", "::", "127.0.0.1") and bind
 * a TCP listening socket on it. May produce more than one socket if the
 * resolver returns multiple addrinfos; each is added to server->tcpSockets.
 */
static int
bind_one_tcp_addr(PGServer * server, const char *address, int port)
{
	struct addrinfo hint;

	MemSet(&hint, 0, sizeof(hint));
	hint.ai_family = AF_UNSPEC;
	hint.ai_flags = AI_PASSIVE | AI_NUMERICHOST;
	hint.ai_socktype = SOCK_STREAM;

	char		portStr[16];

	snprintf(portStr, sizeof(portStr), "%d", port);

	struct addrinfo *addrs = NULL;
	int			ret = pg_getaddrinfo_all(address, portStr, &hint, &addrs);

	if (ret != STATUS_OK || addrs == NULL)
	{
		PGDUCK_SERVER_ERROR("could not translate address \"%s\" port %d: %s",
							address, port,
							ret != STATUS_OK ? gai_strerror(ret) : "no addresses returned");
		if (addrs)
			pg_freeaddrinfo_all(hint.ai_family, addrs);
		return STATUS_ERROR;
	}

	for (struct addrinfo *a = addrs; a != NULL; a = a->ai_next)
	{
		if (server->numTcpSockets >= MAX_TCP_LISTEN_SOCKETS)
		{
			PGDUCK_SERVER_ERROR("too many TCP listen addresses (max %d)",
								MAX_TCP_LISTEN_SOCKETS);
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return STATUS_ERROR;
		}

		int			sock = socket(a->ai_family, SOCK_STREAM, 0);

		if (sock == PGINVALID_SOCKET)
		{
			PGDUCK_SERVER_ERROR("could not create TCP socket for \"%s\": %m", address);
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return STATUS_ERROR;
		}

		int			one = 1;

		/* SO_REUSEADDR avoids "address already in use" after a restart */
		if (setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0)
		{
			PGDUCK_SERVER_ERROR("setsockopt(SO_REUSEADDR) on \"%s\": %m", address);
			close(sock);
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return STATUS_ERROR;
		}

		/*
		 * For dual-stack hosts, bind IPv6 sockets to v6-only so we can
		 * separately bind 0.0.0.0 without conflict (matches PG's behavior).
		 */
		if (a->ai_family == AF_INET6 &&
			setsockopt(sock, IPPROTO_IPV6, IPV6_V6ONLY, &one, sizeof(one)) < 0)
		{
			PGDUCK_SERVER_ERROR("setsockopt(IPV6_V6ONLY) on \"%s\": %m", address);
			close(sock);
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return STATUS_ERROR;
		}

		if (bind(sock, a->ai_addr, a->ai_addrlen) < 0)
		{
			PGDUCK_SERVER_ERROR("could not bind TCP socket on \"%s\" port %d: %m",
								address, port);
			close(sock);
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return STATUS_ERROR;
		}

		const int	listenQueueSize = MaxThreads;

		if (listen(sock, listenQueueSize) < 0)
		{
			PGDUCK_SERVER_ERROR("listen() on TCP socket for \"%s\": %m", address);
			close(sock);
			pg_freeaddrinfo_all(hint.ai_family, addrs);
			return STATUS_ERROR;
		}

		server->tcpSockets[server->numTcpSockets++] = sock;
		PGDUCK_SERVER_LOG("pgduck_server listening on TCP %s:%d (fd=%d)",
						  address, port, sock);
	}

	pg_freeaddrinfo_all(hint.ai_family, addrs);
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

static void
handle_shutdown_signal(int sig)
{
	running = 0;
}


/*
 * install_shutdown_signal_handlers -- install SIGINT/SIGTERM handlers.
 *
 * The handler simply sets `running = 0`.  SA_RESTART is deliberately
 * *not* set so that accept() in the main loop returns -1/EINTR, giving
 * the loop a chance to notice the flag and exit promptly.
 */
static int
install_shutdown_signal_handlers(void)
{
	struct sigaction sa;

	sa.sa_handler = handle_shutdown_signal;
	sigemptyset(&sa.sa_mask);
	sa.sa_flags = 0;			/* no SA_RESTART — accept() must return
								 * EINTR */

	if (sigaction(SIGINT, &sa, NULL) == -1 ||
		sigaction(SIGTERM, &sa, NULL) == -1)
	{
		PGDUCK_SERVER_ERROR("sigaction failed: %s", strerror(errno));
		return STATUS_ERROR;
	}

	return STATUS_OK;
}


/*
 * disable_shutdown_signals -- block SIGINT/SIGTERM in the calling thread.
 *
 * Used before pthread_create() so the child thread inherits a blocked
 * mask and never receives these signals.
 */
static int
disable_shutdown_signals(void)
{
	sigset_t	sigs;

	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);

	if (pthread_sigmask(SIG_BLOCK, &sigs, NULL) != 0)
	{
		PGDUCK_SERVER_ERROR("pthread_sigmask failed: %s", strerror(errno));
		return STATUS_ERROR;
	}

	return STATUS_OK;
}


/*
 * enable_shutdown_signals -- unblock SIGINT/SIGTERM and re-install the
 * shutdown signal handlers so accept() can be interrupted again.
 */
static int
enable_shutdown_signals(void)
{
	sigset_t	sigs;

	sigemptyset(&sigs);
	sigaddset(&sigs, SIGINT);
	sigaddset(&sigs, SIGTERM);

	if (pthread_sigmask(SIG_UNBLOCK, &sigs, NULL) != 0)
	{
		PGDUCK_SERVER_ERROR("pthread_sigmask failed: %s", strerror(errno));
		return STATUS_ERROR;
	}

	return STATUS_OK;
}


/*
 * dispatch_accepted_client -- accept() one client on a ready listening
 * socket and hand it off to a worker thread. Returns STATUS_OK if the
 * client was accepted (success or backpressured-rejected) and the loop
 * should continue, or STATUS_ERROR for a fatal error that should cause
 * the server to exit. EINTR during accept() is treated as benign and
 * returns STATUS_OK.
 */
static int
dispatch_accepted_client(PGServer * pgServer, int listeningSocket)
{
	PGClient   *client = (PGClient *) pg_malloc0(sizeof(PGClient));
	socklen_t	clientAddrLen = sizeof(client->clientAddress);

	client->clientSocket =
		accept(listeningSocket,
			   (struct sockaddr *) &client->clientAddress, &clientAddrLen);

	if (client->clientSocket < 0)
	{
		int			save_errno = errno;

		pg_free(client);

		/*
		 * EINTR can come from our shutdown handler (running == 0) or from
		 * unrelated sources like a debugger attaching (ptrace). Either way,
		 * just continue — the while-condition handles shutdown.
		 *
		 * EAGAIN/EWOULDBLOCK shouldn't normally happen on a listening socket
		 * we just learned was readable via poll(), but be defensive.
		 */
		if (save_errno == EINTR || save_errno == EAGAIN ||
			save_errno == EWOULDBLOCK)
			return STATUS_OK;

		PGDUCK_SERVER_ERROR("Could not accept the client on fd %d: %s",
							listeningSocket, strerror(save_errno));
		return STATUS_ERROR;
	}

	/* first check if we have available threads */
	int			threadIndex = pgclient_threadpool_reserve_slot(client);

	if (threadIndex == InvalidThreadIndex)
	{
		PGDUCK_SERVER_LOG("A new client rejected as it exceeds %d clients", MaxAllowedClients);

		/* TODO: send error message to the client */
		close(client->clientSocket);
		pg_free(client);
		return STATUS_OK;
	}

	/* state to pass into pgclient_thread_main and pgclient_thread_cleanup */
	PgClientThreadInitState *initState =
		(PgClientThreadInitState *) pg_malloc0(sizeof(PgClientThreadInitState));

	initState->threadIndex = threadIndex;
	initState->startFunction = pgServer->startFunction;
	initState->pgClient = client;

	if (disable_shutdown_signals() != STATUS_OK)
		exit(STATUS_ERROR);

	if (pgserver_create_client_thread(initState) != OK)
	{
		PGDUCK_SERVER_ERROR("Thread creation failed for client %d", client->clientSocket);

		close(client->clientSocket);
		pg_free(client);
		pg_free(initState);
		pgclient_threadpool_free_slot(threadIndex);
	}

	if (enable_shutdown_signals() != STATUS_OK)
		exit(STATUS_ERROR);

	return STATUS_OK;
}

/*
 * pgserver_run is the main loop for the PostgreSQL wire compatible server.
 *
 * Polls across all listening sockets (Unix domain socket plus any TCP
 * listeners configured via --listen_addresses). When any becomes
 * readable, accepts the client there and dispatches via the thread
 * pool. Touches lock/socket files periodically so /tmp cleaners don't
 * eat them.
 */
int
pgserver_run(PGServer * pgServer)
{
	if (install_shutdown_signal_handlers() != STATUS_OK)
		return STATUS_ERROR;

	struct pollfd fds[1 + MAX_TCP_LISTEN_SOCKETS];
	int			nfds = 0;

	fds[nfds].fd = pgServer->listeningSocket;
	fds[nfds].events = POLLIN;
	nfds++;

	for (int i = 0; i < pgServer->numTcpSockets; i++)
	{
		fds[nfds].fd = pgServer->tcpSockets[i];
		fds[nfds].events = POLLIN;
		nfds++;
	}

	while (running)
	{
		/* Reset revents before each poll(). */
		for (int i = 0; i < nfds; i++)
			fds[i].revents = 0;

		/*
		 * Use a 10-second timeout so the touch-internal-files maintenance
		 * runs even when no clients connect for a long time. Previously the
		 * code relied on pg_lake_manage_cache() generating a connection
		 * every ~10s; we don't want to depend on that for the multi-socket
		 * path.
		 */
		int			r = poll(fds, nfds, 10 * 1000);

		if (r < 0)
		{
			if (errno == EINTR)
				continue;		/* shutdown signal or unrelated; let the
								 * while-condition decide */

			PGDUCK_SERVER_ERROR("poll() failed: %s", strerror(errno));
			return STATUS_ERROR;
		}

		/*
		 * Touch every 58 minutes regardless of activity. (Same logic as
		 * before, but invoked from the poll loop instead of the inline
		 * accept path.)
		 */
		time_t		now = time(NULL);

		if (now - pgServer->last_touch_time >= 58 * SECS_PER_MINUTE)
			touch_internal_files(pgServer, now);

		if (r == 0)
			continue;			/* timeout — go back to poll */

		for (int i = 0; i < nfds; i++)
		{
			if (!(fds[i].revents & POLLIN))
				continue;

			if (dispatch_accepted_client(pgServer, fds[i].fd) != STATUS_OK)
			{
				/*
				 * TODO: be more graceful — for now keep the original
				 * exit-on-fatal-accept-error behavior.
				 */
				exit(STATUS_ERROR);
			}
		}
	}

	return STATUS_OK;
}


/*
 * pgserver_destroy performs a graceful shutdown of the server.
 *
 * 1. Close the listening socket so no new connections are accepted.
 * 2. Interrupt every active DuckDB query so client threads can send a
 *    proper error to their clients instead of an abrupt TCP reset.
 * 3. Brief grace period to let interrupted threads finish their error
 *    path and close their sockets cleanly.
 *
 * We don't join the client threads (they are detached), so the grace
 * period is best-effort.  When main() returns, exit() will tear down
 * any remaining threads.
 */
void
pgserver_destroy(PGServer * pgServer)
{
	PGDUCK_SERVER_LOG("Shutting down: closing listening sockets");
	if (pgServer->listeningSocket >= 0)
	{
		closesocket(pgServer->listeningSocket);
		pgServer->listeningSocket = -1;
	}
	for (int i = 0; i < pgServer->numTcpSockets; i++)
	{
		if (pgServer->tcpSockets[i] >= 0)
		{
			closesocket(pgServer->tcpSockets[i]);
			pgServer->tcpSockets[i] = -1;
		}
	}
	pgServer->numTcpSockets = 0;

	int			interrupted = pgclient_threadpool_cancel_all();

	if (interrupted > 0)
	{
		PGDUCK_SERVER_LOG("Shutting down: interrupted %d active connection(s), "
						  "waiting briefly for them to finish", interrupted);

		/*
		 * 2 seconds is generous for the threads to send an error to their
		 * client and run through pgclient_thread_cleanup.
		 */
		pg_usleep(2 * 1000000L);
	}

	PGDUCK_SERVER_LOG("Done running");
}


/*
 * pgserver_create_client_thread creates a new thread for the client.
 * We use PTHREAD_CREATE_DETACHED so that we don't have to join the threads.
 *
 * The caller must block shutdown signals before calling this function
 * so the new thread inherits a blocked mask and never receives
 * SIGINT/SIGTERM.
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

	/*
	 * SIGINT/SIGTERM are already blocked — pgserver_create_client_thread()
	 * blocks them before pthread_create(), so this thread inherits a blocked
	 * mask.  No per-thread sigmask call needed.
	 */

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
