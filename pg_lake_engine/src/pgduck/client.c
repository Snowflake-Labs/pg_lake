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

#include "postgres.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include "access/hash.h"
#include "access/xact.h"
#include "pg_lake/pgduck/client.h"
#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

/*
 * Thin compat shim for injection points.  pg_lake_engine sits below
 * pg_lake_table in the dependency graph, so we cannot include
 * pg_lake/util/injection_points.h from there.  Inline the same three-line
 * pattern here instead.
 */
#if PG_VERSION_NUM >= 180000
#include "utils/injection_point.h"
#define PG_LAKE_ENGINE_INJECTION_POINT(name) INJECTION_POINT(name, NULL)
#elif PG_VERSION_NUM >= 170000
#include "utils/injection_point.h"
#define PG_LAKE_ENGINE_INJECTION_POINT(name) INJECTION_POINT(name)
#else
#define PG_LAKE_ENGINE_INJECTION_POINT(name) ((void) (name))
#endif


/*
 * Milliseconds to wait to cancel an in-progress query or execute a cleanup
 * query; if it takes longer than 30 seconds to do these, we assume the
 * connection is dead.
 */
#define CONNECTION_CLEANUP_TIMEOUT	30000

static void InitializePGDuckClient(void);
static void SetupPgDuckConnectionHash(void);

static void PGDuckClientTransactionCallback(XactEvent event, void *arg);
static void PGDuckClientSubtransactionCallback(SubXactEvent event,
											   SubTransactionId mySubid,
											   SubTransactionId parentSubid,
											   void *arg);
static uint32 ReleaseAllPGDuckConnections(SubTransactionId subTransactionId);
static void CancelRunningCommandOnConnection(PGconn *conn);
static bool CancelQuery(PGconn *conn);
static bool StartCancelQuery(PGconn *conn);
static bool FinishCancelQuery(PGconn *conn, TimestampTz endtime, bool consume_input);
static bool WaitForLastResultWithTimeout(PGconn *conn, TimestampTz endtime,
										 PGresult **result, bool *timed_out);

/* query engine settings */
char	   *PgduckServerConninfo = DEFAULT_PGDUCK_SERVER_CONNINFO;

/* monotonically increasing key */
static uint32 ConnectionId = 0;

/* global state */
static HTAB *PgDuckConnectionHash = NULL;
static MemoryContext PgDuckConnectionMemoryContext = NULL;
static bool IsPGDuckClientInitialized = false;

/* hash entry */
typedef struct PgDuckServerConnectionHashEntry
{
	uint32		key;
	SubTransactionId subTransactionId;
	PGDuckConnection pgDuckConnection;

}			PgDuckServerConnectionHashEntry;

/*
 * InitializePGDuckClient is called the first time we get a PGDuck connection
 * in a backend. Its primary job is to set up transaction callbacks and the
 * hash map that keeps track of the connections.
 */
static void
InitializePGDuckClient(void)
{
	if (IsPGDuckClientInitialized)
	{
		return;
	}

	RegisterXactCallback(PGDuckClientTransactionCallback, NULL);
	RegisterSubXactCallback(PGDuckClientSubtransactionCallback, NULL);

	SetupPgDuckConnectionHash();

	IsPGDuckClientInitialized = true;
}


/*
* SetupPgDuckConnectionHash sets up the hash table for the PGDuck connections.
* It creates the hash the first time we get a PGDuck connection in a backend.
* On subsequent calls, it does nothing.
*/
static void
SetupPgDuckConnectionHash(void)
{
	if (PgDuckConnectionHash != NULL)
		return;

	PgDuckConnectionMemoryContext = AllocSetContextCreateInternal(TopMemoryContext,
																  "PgDuck Connection Memory Context",
																  ALLOCSET_DEFAULT_MINSIZE,
																  ALLOCSET_DEFAULT_INITSIZE,
																  ALLOCSET_DEFAULT_MAXSIZE);


	HASHCTL		hashInfo;

	memset(&hashInfo, 0, sizeof(hashInfo));
	hashInfo.keysize = sizeof(uint32);
	hashInfo.entrysize = sizeof(PgDuckServerConnectionHashEntry);
	hashInfo.hash = uint32_hash;
	hashInfo.hcxt = PgDuckConnectionMemoryContext;
	uint32		hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	PgDuckConnectionHash = hash_create("pgduck_server connection cache hash",
									   16, &hashInfo, hashFlags);
}


/*
 * GetPGDuckConnection returns the main PGDuck connection.
 */
PGDuckConnection *
GetPGDuckConnection(void)
{
	InitializePGDuckClient();

	PGconn	   *connection = PQconnectdb(PgduckServerConninfo);

	if (PQstatus(connection) != CONNECTION_OK)
	{
		char		PG_USED_FOR_ASSERTS_ONLY *errorMessage = pstrdup(PQerrorMessage(connection));

		PQfinish(connection);

#ifdef USE_ASSERT_CHECKING
		ereport(ERROR, (errmsg("could not start query engine: %s", errorMessage)));
#else
		/* hide internals from users */
		ereport(ERROR, (errmsg("could not start query engine")));
#endif
	}

	int			connectionId = ConnectionId++;
	bool		found = false;
	PgDuckServerConnectionHashEntry *entry =
		hash_search(PgDuckConnectionHash, &connectionId, HASH_ENTER, &found);

	if (!found)
	{
		entry->subTransactionId = GetCurrentSubTransactionId();

		entry->pgDuckConnection.conn = connection;
		entry->pgDuckConnection.connectionId = connectionId;
	}

	return &entry->pgDuckConnection;
}


/*
 * ReleasePGDuckConnection closes the current connection to the
 * pgduck_server, if any.
 */
void
ReleasePGDuckConnection(PGDuckConnection * pgDuckConnection)
{
	uint32		connectionId = pgDuckConnection->connectionId;
	bool		found = false;
	PgDuckServerConnectionHashEntry *entry =
		hash_search(PgDuckConnectionHash, &connectionId, HASH_REMOVE, &found);

	if (!found)
	{
		elog(WARNING, "closing untracked connection to query engine");

		if (pgDuckConnection->conn != NULL)
			PQfinish(pgDuckConnection->conn);

		pgDuckConnection->conn = NULL;

		return;
	}

	if (entry->pgDuckConnection.conn != NULL)
		PQfinish(entry->pgDuckConnection.conn);
}


/*
 * SendQueryToPGDuck sends the given query over the connection.
 */
void
SendQueryToPGDuck(PGDuckConnection * pgDuckConnection, char *query)
{
	PGconn	   *conn = pgDuckConnection->conn;

	/* use text format */
	int			format = 0;

	/*
	 * Even though we don't have parameters, we use PQsendQueryParams because
	 * it uses extended query protocol, which activates streaming results on
	 * pgduck.
	 */
	int			sentQuery = PQsendQueryParams(conn, query,
											  0, NULL, NULL, NULL, NULL,
											  format);

	if (sentQuery == 0)
	{
		ereport(ERROR, (errmsg("lost connection to query engine")));
	}
}


/*
 * ExecuteCommandInPGDuck executes the given command in pgduck and for writes
 * it returns the number of affected rows, or -1 otherwise.
 */
int64
ExecuteCommandInPGDuck(char *command)
{
	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, command);

	/*
	 * If there's an error, make sure we release the connection.
	 *
	 * CheckPGDuckResult already clears the result in that case.
	 */
	PG_TRY();
	{
		CheckPGDuckResult(pgDuckConn, result);
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(pgDuckConn);
	}
	PG_END_TRY();

	/*
	 * we do not expect rows to be returned, but affected rows could be
	 * interesting
	 */
	int64		rowsAffected = -1;

	char	   *commandTuples = PQcmdTuples(result);

	if (*commandTuples != '\0')
		rowsAffected = atol(commandTuples);

	PQclear(result);

	return rowsAffected;
}


/*
 * ExecuteCommandsInPGDuck executes the given commands in pgduck and for writes
 * it returns list of the number of affected rows, or -1 otherwise.
 */
List *
ExecuteCommandsInPGDuck(List *commands)
{
	List	   *rowsAffected = NIL;

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();

	PG_TRY();
	{
		ListCell   *commandCell = NULL;

		foreach(commandCell, commands)
		{
			char	   *command = (char *) lfirst(commandCell);
			PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, command);

			CheckPGDuckResult(pgDuckConn, result);

			char	   *commandTuples = PQcmdTuples(result);
			int64		rowsAffectedValue = -1;

			if (*commandTuples != '\0')
				rowsAffectedValue = atol(commandTuples);

			PQclear(result);

			rowsAffected = lappend_int(rowsAffected, rowsAffectedValue);
		}
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(pgDuckConn);
	}
	PG_END_TRY();

	return rowsAffected;
}


/*
 * ExecuteOptionalCommandInPGDuck runs the command in pgduck and returns true
 * if the command was successful, false otherwise.
 */
bool
ExecuteOptionalCommandInPGDuck(char *command)
{
	MemoryContext savedContext = CurrentMemoryContext;
	volatile bool success = false;

	PG_TRY();
	{
		ExecuteCommandInPGDuck(command);
		success = true;
	}
	PG_CATCH();
	{
		MemoryContextSwitchTo(savedContext);
		ErrorData  *edata = CopyErrorData();

		FlushErrorState();

		/* continue unless it was a cancellation */
		if (edata->sqlerrcode != ERRCODE_QUERY_CANCELED)
			edata->elevel = WARNING;

		ThrowErrorData(edata);

		success = false;
	}
	PG_END_TRY();

	return success;
}



/*
 * ExecuteQueryOnPGDuckConnection executes the given query in PGDuck on the given
 * connection.
 */
PGresult *
ExecuteQueryOnPGDuckConnection(PGDuckConnection * pgDuckConnection,
							   const char *query)
{
	PGconn	   *conn = pgDuckConnection->conn;
#ifdef USE_ASSERT_CHECKING
	elog(DEBUG2, "PGDuck: %s", query);
#endif

	int			sentQuery = PQsendQuery(conn, query);

	if (sentQuery == 0)
	{
		ReleasePGDuckConnection(pgDuckConnection);

		/* may have lost connection, retry once */
		pgDuckConnection = GetPGDuckConnection();
		conn = pgDuckConnection->conn;
		sentQuery = PQsendQuery(conn, query);
		if (sentQuery == 0)
		{
			ereport(ERROR, (errmsg("lost connection to query engine")));
		}
	}

	PGresult   *result = WaitForLastResult(pgDuckConnection);

	return result;
}


/*
 * WaitForResult waits for a result from a prior asynchronous execution function call
 * while also checking for signals and postmaster death.
 */
PGresult *
WaitForResult(PGDuckConnection * pgDuckConnection)
{
	PGconn	   *conn = pgDuckConnection->conn;

	while (PQisBusy(conn))
	{
		int			wc;

		/*
		 * Injection point used by tests to pause the worker here so the test
		 * can kill pgduck_server before PQconsumeInput is called, exercising
		 * the broken-connection cleanup path.
		 */
		PG_LAKE_ENGINE_INJECTION_POINT("pgduck-wait-for-result");

		/* Sleep until there's something to do */
		wc = WaitLatchOrSocket(MyLatch,
							   WL_LATCH_SET | WL_SOCKET_READABLE |
							   WL_EXIT_ON_PM_DEATH,
							   PQsocket(conn),
							   -1L, PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		CHECK_FOR_INTERRUPTS();

		/* Data available in socket? */
		if (wc & WL_SOCKET_READABLE)
		{
			if (!PQconsumeInput(conn))
			{
				/*
				 * Do not release the connection here — WaitForResult does
				 * not own it.  Connection lifetime is the caller's
				 * responsibility: either an enclosing PG_TRY/PG_FINALLY
				 * releases it, or the transaction/subtransaction abort
				 * callback (PGDuckClientTransactionCallback /
				 * ...SubtransactionCallback) sweeps it on
				 * XACT_EVENT_ABORT.
				 *
				 * Releasing here is actively harmful:
				 * ExecuteQueryOnPGDuckConnection's retry path may have
				 * already swapped the hash slot out from under the
				 * caller's pointer, so the caller's subsequent release
				 * either double-frees the *new* connection (if dynahash
				 * reused the slab) or PQfinish()'s an already-freed
				 * PGconn (if it didn't).
				 */
				ereport(ERROR, (errmsg("lost connection to query engine")));
			}
		}
	}

	return PQgetResult(conn);
}


/*
 * WaitForLastResult waits for the result from a prior asynchronous execution function call.
 *
 * This function offers quick responsiveness by checking for any interruptions.
 *
 * This function emulates PQexec()'s behavior of returning the last result
 * when there are many.
 *
 * Caller is responsible for the error handling on the result.
 *
 * Derived from pgfdw_get_result
 */
PGresult *
WaitForLastResult(PGDuckConnection * pgDuckConnection)
{
	PGresult   *volatile last_res = NULL;

	/* In what follows, do not leak any PGresults on an error. */
	PG_TRY();
	{
		for (;;)
		{
			PGresult   *res = WaitForResult(pgDuckConnection);

			if (res == NULL)
				break;			/* query is complete */

			PQclear(last_res);
			last_res = res;
		}
	}
	PG_CATCH();
	{
		PQclear(last_res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	return last_res;
}


/*
 * CheckPGDuckResult throws the error received over a connection, if any.
 *
 * PQclear is called on the result in case of error.
 */
void
CheckPGDuckResult(PGDuckConnection * pgDuckConnection, PGresult *result)
{
	PG_TRY();
	{
		ThrowIfPGDuckResultHasError(pgDuckConnection, result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

}


/*
 * ThrowIfPGDuckResultHasError throws the error received over a connection, if any.
 */
void
ThrowIfPGDuckResultHasError(PGDuckConnection * pgDuckConnection, PGresult *result)
{
	ExecStatusType resultStatus = PQresultStatus(result);

	if (resultStatus == PGRES_TUPLES_OK || resultStatus == PGRES_COMMAND_OK ||
		resultStatus == PGRES_SINGLE_TUPLE)
	{
		return;
	}

	char	   *message = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
	char	   *messageDetail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
	char	   *messageHint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
	char	   *messageContext = PQresultErrorField(result, PG_DIAG_CONTEXT);
	char	   *sqlStateString = PQresultErrorField(result, PG_DIAG_SQLSTATE);
	int			sqlState = ERRCODE_INTERNAL_ERROR;

	if (sqlStateString != NULL)
	{
		sqlState = MAKE_SQLSTATE(sqlStateString[0],
								 sqlStateString[1],
								 sqlStateString[2],
								 sqlStateString[3],
								 sqlStateString[4]);
	}

	if (message == NULL)
	{
		message = pchomp(PQerrorMessage(pgDuckConnection->conn));
	}

	ereport(ERROR, (errcode(sqlState),
					errmsg("%s", message),
					messageDetail ? errdetail("%s", messageDetail) : 0,
					messageHint ? errhint("%s", messageHint) : 0,
					messageContext ? errcontext("%s", messageContext) : 0));
}


/*
 * PGDuckClientTransactionCallback is called at the end of a
 * transaction.
 */
static void
PGDuckClientTransactionCallback(XactEvent event, void *arg)
{
	/*
	 * Release all connections at the end of a transaction.
	 *
	 * For abort cases, we release the connections on behalf of the caller as
	 * the caller might not have the chance to release the connection. For
	 * commit cases, we normally expect all the connections to be released by
	 * the caller. However, we release the connections just in case the caller
	 * forgets to release them.
	 *
	 * This is the transaction callback, so we release all the connections.
	 */
	if (event == XACT_EVENT_ABORT || event == XACT_EVENT_PARALLEL_ABORT ||
		event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT)
	{
		int			releasedConnections = ReleaseAllPGDuckConnections(InvalidSubTransactionId);

		/* we expect all callers to release connections on successful commits */
		if ((event == XACT_EVENT_PRE_COMMIT || event == XACT_EVENT_PARALLEL_PRE_COMMIT) &&
			releasedConnections > 0)
		{
			elog(WARNING, "released %d connections on transaction commit",
				 releasedConnections);
		}
	}
}


/*
 *  Releases all PgDuck server connections.
 *
 * This function iterates over the PgDuck server connection hash table and
 * cancels any running command on each connection. It then releases the
 * PgDuck connection.
 *
 * If subTransactionId is not InvalidSubTransactionId, it releases only the
 * connections that are established within the given sub-transaction.
 */
static uint32
ReleaseAllPGDuckConnections(SubTransactionId subTransactionId)
{
	int			releasedConnections = 0;
	HASH_SEQ_STATUS status;
	PgDuckServerConnectionHashEntry *entry;

	hash_seq_init(&status, PgDuckConnectionHash);
	while ((entry = (PgDuckServerConnectionHashEntry *) hash_seq_search(&status)) != 0)
	{
		if (subTransactionId == InvalidSubTransactionId ||
			entry->subTransactionId == subTransactionId)
		{
			/* cancel any running command on the connection */
			CancelRunningCommandOnConnection(entry->pgDuckConnection.conn);

			/* release the connection */
			ReleasePGDuckConnection(&entry->pgDuckConnection);

			++releasedConnections;
		}
	}

	return releasedConnections;
}


/*
 * PGDuckClientSubtransactionCallback is called at the end of a
 * subtransaction.
 */
static void
PGDuckClientSubtransactionCallback(SubXactEvent event, SubTransactionId mySubid,
								   SubTransactionId parentSubid, void *arg)
{
	/*
	 * Release all connections that are established within this subtransaction
	 * at the end of a sub-transaction.
	 *
	 * For abort cases, we release the connections on behalf of the caller as
	 * the caller might not have the chance to release the connection. For
	 * commit cases, we normally expect all the connections to be released by
	 * the caller. However, we release the connections just in case the caller
	 * forgets to release them.
	 *
	 * This is the sub-transaction callback, so we release only the
	 * connections that are established within this sub-transaction.
	 */
	if (event == SUBXACT_EVENT_ABORT_SUB ||
		event == SUBXACT_EVENT_COMMIT_SUB)
	{
		int			releasedConnections = ReleaseAllPGDuckConnections(mySubid);

		/* we expect all callers to release connections on successful commits */
		if (event == SUBXACT_EVENT_COMMIT_SUB && releasedConnections > 0)
			elog(WARNING, "released %d connections on subtransaction commit",
				 releasedConnections);
	}
}


/*
 * CancelRunningCommandOnConnection cancels a pending query and then closes the connection.
 */
static void
CancelRunningCommandOnConnection(PGconn *conn)
{
	if (conn == NULL)
	{
		/* no active connection */
		return;
	}

	if (PQstatus(conn) != CONNECTION_OK)
	{
		/* connection is broken, nothing to do */
		return;
	}

	if (PQtransactionStatus(conn) == PQTRANS_ACTIVE)
	{
		/* a query is still running, cancel it */
		if (!CancelQuery(conn))
		{
			return;
		}
	}

	/* we currently don't have "idle in transaction" status */
	Assert(PQtransactionStatus(conn) == PQTRANS_IDLE);
}


/*
 * Cancel the currently-in-progress query (whose query text we do not have)
 * and ignore the result.  Returns true if we successfully cancel the query
 * and discard any pending result, and false if not.
 *
 * It's not a huge problem if we throw an ERROR here, but if we get into error
 * recursion trouble, we'll end up slamming the connection shut, which will
 * necessitate failing the entire toplevel transaction even if subtransactions
 * were used.  Try to use WARNING where we can.
 */
static bool
CancelQuery(PGconn *conn)
{
	TimestampTz endtime;

	/*
	 * If it takes too long to cancel the query and discard the result, assume
	 * the connection is dead.
	 */
	endtime = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
										  CONNECTION_CLEANUP_TIMEOUT);

	if (!StartCancelQuery(conn))
		return false;
	return FinishCancelQuery(conn, endtime, false);
}


/*
 * StartCancelQuery sends a cancellation over the given connection.
 *
 * Copied ad verbatim from pgfdw_cancel_query_begin.
 */
static bool
StartCancelQuery(PGconn *conn)
{
	PGcancel   *cancel;
	char		errbuf[256];

	/*
	 * Issue cancel request.  Unfortunately, there's no good way to limit the
	 * amount of time that we might block inside PQgetCancel().
	 */
	if ((cancel = PQgetCancel(conn)))
	{
		if (!PQcancel(cancel, errbuf, sizeof(errbuf)))
		{
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not send cancel request: %s",
							errbuf)));
			PQfreeCancel(cancel);
			return false;
		}
		PQfreeCancel(cancel);
	}

	return true;
}

/*
 * FinishCancelQuery finishes a cancellation started by StartCancelQuery.
 *
 * Copied ad verbatim from pgfdw_cancel_query_end
 */
static bool
FinishCancelQuery(PGconn *conn, TimestampTz endtime, bool consume_input)
{
	PGresult   *result = NULL;
	bool		timed_out;

	/*
	 * If requested, consume whatever data is available from the socket. (Note
	 * that if all data is available, this allows WaitForLastResultWithTimeout
	 * to call PQgetResult without forcing the overhead of WaitLatchOrSocket,
	 * which would be large compared to the overhead of PQconsumeInput.)
	 */
	if (consume_input && !PQconsumeInput(conn))
	{
		ereport(WARNING,
				(errcode(ERRCODE_CONNECTION_FAILURE),
				 errmsg("could not get result of cancel request: %s",
						pchomp(PQerrorMessage(conn)))));
		return false;
	}

	/* Get and discard the result of the query. */
	if (WaitForLastResultWithTimeout(conn, endtime, &result, &timed_out))
	{
		if (timed_out)
			ereport(WARNING,
					(errmsg("could not get result of cancel request due to timeout")));
		else
			ereport(WARNING,
					(errcode(ERRCODE_CONNECTION_FAILURE),
					 errmsg("could not get result of cancel request: %s",
							pchomp(PQerrorMessage(conn)))));

		return false;
	}
	PQclear(result);

	return true;
}


/*
 * WaitForLastResultWithTimeout gets the result of a query that is in progress.  This
 * might be a query that is being interrupted by transaction abort, or it might
 * be a query that was initiated as part of transaction abort to get the remote
 * side back to the appropriate state.
 *
 * endtime is the time at which we should give up and assume the remote
 * side is dead.  Returns true if the timeout expired or connection trouble
 * occurred, false otherwise.  Sets *result except in case of a timeout.
 * Sets timed_out to true only when the timeout expired.
 *
 * Copied ad verbatim from pgfdw_get_cleanup_result
 */
static bool
WaitForLastResultWithTimeout(PGconn *conn, TimestampTz endtime,
							 PGresult **result, bool *timed_out)
{
	volatile bool failed = false;
	PGresult   *volatile last_res = NULL;

	*timed_out = false;

	/* In what follows, do not leak any PGresults on an error. */
	PG_TRY();
	{
		for (;;)
		{
			PGresult   *res;

			while (PQisBusy(conn))
			{
				int			wc;
				TimestampTz now = GetCurrentTimestamp();
				long		cur_timeout;

				/* If timeout has expired, give up, else get sleep time. */
				cur_timeout = TimestampDifferenceMilliseconds(now, endtime);
				if (cur_timeout <= 0)
				{
					*timed_out = true;
					failed = true;
					goto exit;
				}

				/* Sleep until there's something to do */
				wc = WaitLatchOrSocket(MyLatch,
									   WL_LATCH_SET | WL_SOCKET_READABLE |
									   WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
									   PQsocket(conn),
									   cur_timeout, PG_WAIT_EXTENSION);
				ResetLatch(MyLatch);

				CHECK_FOR_INTERRUPTS();

				/* Data available in socket? */
				if (wc & WL_SOCKET_READABLE)
				{
					if (!PQconsumeInput(conn))
					{
						/* connection trouble */
						failed = true;
						goto exit;
					}
				}
			}

			res = PQgetResult(conn);
			if (res == NULL)
				break;			/* query is complete */

			bool		isCopyOut = PQresultStatus(res) == PGRES_COPY_OUT;

			PQclear(last_res);
			last_res = res;

			if (isCopyOut)
			{
				/*
				 * We prefer not to wait for CopyOut responses, because the
				 * logic is complex and closing the connection might be more
				 * effective at stopping the command.
				 *
				 * This is not a real failure condition, but we treat it as
				 * such since we failed to complete cancellation.
				 */
				failed = true;
				break;
			}
		}
exit:	;
	}
	PG_CATCH();
	{
		PQclear(last_res);
		PG_RE_THROW();
	}
	PG_END_TRY();

	if (failed)
		PQclear(last_res);
	else
		*result = last_res;
	return failed;
}


/*
 * GetSingleValueFromPGDuck runs a query with a single value
 * as a result and returns the value as text.
 */
char *
GetSingleValueFromPGDuck(char *query)
{
	char	   *value = NULL;

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query);

	CheckPGDuckResult(pgDuckConn, result);

	/* make sure we PQclear the result */
	PG_TRY();
	{
		int			rowCount = PQntuples(result);
		int			columnCount = PQnfields(result);

		if (columnCount < 1)
			ereport(ERROR, (errmsg("unexpected column count %d", columnCount)));

		if (rowCount < 1)
			ereport(ERROR, (errmsg("unexpected row count %d", rowCount)));

		value = pstrdup(PQgetvalue(result, 0, 0));

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleasePGDuckConnection(pgDuckConn);

	return value;
}



/*
* SendQueryWithParams sends a query with parameters to the pgduck server.
* It is a wrapper around PQsendQueryParams() followed by PQsetSingleRowMode().
*/
void
SendQueryWithParams(PGDuckConnection * pgduckConn, char *queryString,
					int numParams, const char **parameterValues)
{
	PGconn	   *conn = pgduckConn->conn;

	/*
	 * Notice that we pass NULL for paramTypes, thus forcing the remote server
	 * to infer types for all parameters.  Since we explicitly cast every
	 * parameter (see rewrite_query.c), the "inference" is trivial and will
	 * produce the desired result.  This allows us to avoid assuming that the
	 * remote server has the same OIDs we do for the parameters' types.
	 */
	if (!PQsendQueryParams(conn, queryString, numParams,
						   NULL, parameterValues, NULL, NULL, 0))
	{
		ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
						errmsg("lost connection to query engine")));
	}

	/* postgres lets to set single row mode after sending the query */
	int			singleRowMode = PQsetSingleRowMode(conn);

	if (singleRowMode == 0)
	{
		elog(ERROR, "could not set single row mode for connection");
	}
}
