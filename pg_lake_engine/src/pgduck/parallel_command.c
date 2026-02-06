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
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "storage/latch.h"
#include "utils/memutils.h"
#include "utils/wait_event.h"

#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/parallel_command.h"
#include "pg_lake/util/rel_utils.h"
#include "utils/builtins.h"

#if PG_VERSION_NUM >= 180000
#include "utils/injection_point.h"
#endif


#if PG_VERSION_NUM >= 170000
#define WaitEventSetTracker CurrentResourceOwner
#else
#define WaitEventSetTracker CurrentMemoryContext
#endif


/*
 * CommandExecutionStateMachineState represents the state of a single upload connection
 * in the state machine.
 */
typedef enum CommandExecutionStateMachineState
{
	COMMAND_EXECUTION_STATE_INITIAL,	/* not yet started */
	COMMAND_EXECUTION_STATE_FLUSHING,	/* flushing query to server */
	COMMAND_EXECUTION_STATE_WAITING,	/* query sent, waiting for result */
	COMMAND_EXECUTION_STATE_FAILING,	/* cleaning up after failure */
	COMMAND_EXECUTION_STATE_COMPLETED,	/* query completed successfully */
	COMMAND_EXECUTION_STATE_FAILED	/* query failed (terminal state) */
}			CommandExecutionStateMachineState;

/*
 * CommandExecutionState tracks the state of an in-progress upload.
 */
typedef struct CommandExecutionState
{
	char	   *command;
	PGDuckConnection *pgDuckConnection;
	CommandExecutionStateMachineState state;
	char	   *errorMessage;
}			CommandExecutionState;

/*
 * MultiCommandExecutionState tracks the state of a set of uploads.
 */
typedef struct MultiCommandExecutionState
{
	CommandExecutionState *states;
	int32		executionCount;
	int32		busyConnectionsCount;
}			MultiCommandExecutionState;


static void RunNonBlockingCommandExecutions(MultiCommandExecutionState * multiState,
											int32 maxRunningExecutions);
static void WaitForSocketEvents(MultiCommandExecutionState * multiState);
static void RunCommandExecutionStateMachine(MultiCommandExecutionState * multiState,
											CommandExecutionState * state);
static bool AdvanceCommandExecutionStateMachine(MultiCommandExecutionState * multiState,
												CommandExecutionState * state);
static int	CountActiveExecutions(MultiCommandExecutionState * multiState);
static int	CountWaitingExecutions(MultiCommandExecutionState * multiState);



/*
 * ExecuteCommandsInParallelInPGDuck runs a set of commands on pgduck_server in parallel
 * using state machines to manage each individual execution over a non-blocking
 * connection.
 */
void
ExecuteCommandsInParallelInPGDuck(List *commandList, int32 maxActiveExecutions)
{
	/* initialize execution state */
	MultiCommandExecutionState *multiState = palloc0(sizeof(MultiCommandExecutionState));

	int			executionCount = list_length(commandList);
	CommandExecutionState *execStates = palloc0(executionCount * sizeof(CommandExecutionState));

	int32		commandIndex = 0;
	ListCell   *commandCell = NULL;

	foreach(commandCell, commandList)
	{
		char	   *command = (char *) lfirst(commandCell);

		execStates[commandIndex].command = command;
		execStates[commandIndex].state = COMMAND_EXECUTION_STATE_INITIAL;

		commandIndex++;
	}

	multiState->executionCount = executionCount;
	multiState->states = execStates;

	maxActiveExecutions = Min(maxActiveExecutions, executionCount);

	/* run all the state machines (ends on error) */
	while (CountActiveExecutions(multiState) > 0)
	{
		CHECK_FOR_INTERRUPTS();

		/*
		 * Advance state machines that can make progress without waiting. This
		 * includes INITIAL->WAITING transitions.
		 */
		RunNonBlockingCommandExecutions(multiState, maxActiveExecutions);

		/* if we have waiting connections, wait for socket events */
		WaitForSocketEvents(multiState);
	}

	/* check for errors */
	for (int32 executionIndex = 0; executionIndex < multiState->executionCount; executionIndex++)
	{
		if (execStates[executionIndex].state == COMMAND_EXECUTION_STATE_FAILED)
			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("%s", execStates[executionIndex].errorMessage)));
	}
}


/*
 * RunNonBlockingCommandExecutions advances all command executions that can make
 * progress without waiting for I/O.
 */
static void
RunNonBlockingCommandExecutions(MultiCommandExecutionState * multiState,
								int32 maxActiveExecutions)
{
	CommandExecutionState *execStates = multiState->states;

	for (int32 i = 0; i < multiState->executionCount; i++)
	{
		CommandExecutionState *state = &execStates[i];

		/* skip terminal states */
		if (state->state == COMMAND_EXECUTION_STATE_COMPLETED ||
			state->state == COMMAND_EXECUTION_STATE_FAILED)
			continue;

		/*
		 * Limit the number of concurrent connections by only advancing
		 * INITIAL states when we're below the limit.
		 */
		if (state->state == COMMAND_EXECUTION_STATE_INITIAL &&
			multiState->busyConnectionsCount >= maxActiveExecutions)
			continue;

		/*
		 * Advance non-blocking states. FAILING is transitional (not
		 * terminal), so it needs to be advanced to reach FAILED.
		 */
		RunCommandExecutionStateMachine(multiState, state);
	}
}


/*
 * WaitForSocketEvents waits for upload sockets to become ready for I/O,
 * then advances those state machines.
 *
 * For FLUSHING state: waits for write-ready OR read-ready
 * For WAITING state: waits for read-ready
 */
static void
WaitForSocketEvents(MultiCommandExecutionState * multiState)
{
	int			waitingExecutionsCount = CountWaitingExecutions(multiState);

	if (waitingExecutionsCount == 0)
		return;

	WaitEventSet *waitSet = CreateWaitEventSet(WaitEventSetTracker, waitingExecutionsCount + 1);

	AddWaitEventToSet(waitSet, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);

	/* add socket events for uploads in FLUSHING or WAITING state */
	for (int32 i = 0; i < multiState->executionCount; i++)
	{
		CommandExecutionState *state = &multiState->states[i];

		if (state->state != COMMAND_EXECUTION_STATE_FLUSHING &&
			state->state != COMMAND_EXECUTION_STATE_WAITING)
			continue;

		pgsocket	sock = PQsocket(state->pgDuckConnection->conn);

		if (sock == PGINVALID_SOCKET)
		{
			state->state = COMMAND_EXECUTION_STATE_FAILING;
			continue;
		}

		/*
		 * For FLUSHING state, we need to wait for both write-ready and
		 * read-ready events. Per libpq docs, the server might send data
		 * (NOTICE messages) that we must consume before it can read our
		 * query.
		 */
		if (state->state == COMMAND_EXECUTION_STATE_FLUSHING)
		{
			AddWaitEventToSet(waitSet, WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE,
							  sock, NULL, state);
		}
		else
		{
			/* WAITING state only needs read-ready */
			AddWaitEventToSet(waitSet, WL_SOCKET_READABLE, sock, NULL, state);
		}
	}

	WaitEvent  *events = palloc(sizeof(WaitEvent) * (waitingExecutionsCount + 1));

	int32		nevents = WaitEventSetWait(waitSet, -1,
										   events, waitingExecutionsCount + 1,
										   PG_WAIT_EXTENSION);

	for (int32 eventIndex = 0; eventIndex < nevents; eventIndex++)
	{
		if (events[eventIndex].events & WL_LATCH_SET)
		{
			ResetLatch(MyLatch);
			continue;
		}

		CommandExecutionState *state = (CommandExecutionState *) events[eventIndex].user_data;

		/*
		 * Handle socket events based on current state and event type. Per
		 * libpq async documentation, during FLUSHING we must consume input
		 * when the socket becomes read-ready.
		 */
		if (state->state == COMMAND_EXECUTION_STATE_FLUSHING)
		{
			if (events[eventIndex].events & WL_SOCKET_READABLE)
			{
				/*
				 * Socket is read-ready during flush. Consume any incoming
				 * data (e.g., NOTICE messages) before retrying flush.
				 */
				if (!PQconsumeInput(state->pgDuckConnection->conn))
				{
					state->state = COMMAND_EXECUTION_STATE_FAILING;
					continue;
				}
			}

			/*
			 * Advance state machine to retry PQflush(). This handles both
			 * write-ready and read-ready cases.
			 */
			RunCommandExecutionStateMachine(multiState, state);
		}
		else if (state->state == COMMAND_EXECUTION_STATE_WAITING)
		{
			if (events[eventIndex].events & WL_SOCKET_READABLE)
			{
				/* advance this state machine now that the socket is readable */
				RunCommandExecutionStateMachine(multiState, state);
			}
		}
	}

	pfree(events);
	FreeWaitEventSet(waitSet);
}


/*
 * RunCommandExecutionStateMachine advances the command execution state machine
 * until it reaches a state that requires waiting for IO or a terminal state.
 */
static void
RunCommandExecutionStateMachine(MultiCommandExecutionState * multiState,
								CommandExecutionState * state)
{
	/* iterate until a blocking or terminal state is reached */
	while (AdvanceCommandExecutionStateMachine(multiState, state))
	{
		/* nothing extra to do, just keep iterating */
	}
}


/*
 * AdvanceCommandExecutionStateMachine advances a single command execution through
 * its state machine.
 *
 * Returns true if the state machine can be advanced further, false if we need to
 * wait for IO.
 */
static bool
AdvanceCommandExecutionStateMachine(MultiCommandExecutionState * multiState,
									CommandExecutionState * state)
{
	switch (state->state)
	{
		case COMMAND_EXECUTION_STATE_INITIAL:
			{
				/* get a connection (blocking operation) */
				PGDuckConnection *pgDuckConnection = GetPGDuckConnection();

				state->pgDuckConnection = pgDuckConnection;
				multiState->busyConnectionsCount++;

				/* set non-blocking mode for multiplexed waiting */
				if (PQsetnonblocking(pgDuckConnection->conn, 1) != 0)
				{
					state->state = COMMAND_EXECUTION_STATE_FAILING;
					return true;
				}

				/* send the query to the connection */
				int32		sentQuery = PQsendQuery(state->pgDuckConnection->conn, state->command);

				if (sentQuery == 0)
				{
					state->state = COMMAND_EXECUTION_STATE_FAILING;
					return true;
				}

				state->state = COMMAND_EXECUTION_STATE_FLUSHING;
				return true;
			}

		case COMMAND_EXECUTION_STATE_FLUSHING:
			{
				/*
				 * Flush the query to the server. Per libpq documentation, we
				 * must call PQflush() until it returns 0 before waiting for
				 * results.
				 */
#if PG_VERSION_NUM >= 180000
				INJECTION_POINT("parallel-command-flush-fail", NULL);
#endif

				int32		flushResult = PQflush(state->pgDuckConnection->conn);

				if (flushResult == -1)
				{
					/* flush error */
					state->state = COMMAND_EXECUTION_STATE_FAILING;
					return true;
				}
				else if (flushResult == 1)
				{
					/*
					 * More data to flush. We need to wait for write-ready or
					 * read-ready. Return false to indicate we can't advance
					 * further without I/O.
					 */
					return false;
				}

				/* flushResult == 0, query fully sent, wait for bytes */
				state->state = COMMAND_EXECUTION_STATE_WAITING;
				return true;
			}

		case COMMAND_EXECUTION_STATE_WAITING:
			{
				/* check if the query result is ready */
				PGconn	   *conn = state->pgDuckConnection->conn;

#if PG_VERSION_NUM >= 180000
				INJECTION_POINT("parallel-command-wait-fail", NULL);
#endif

				if (PQconsumeInput(conn) == 0)
				{
					state->state = COMMAND_EXECUTION_STATE_FAILING;
					return true;
				}

				while (!PQisBusy(conn))
				{
					PGresult   *result = PQgetResult(conn);

					if (result == NULL)
					{
#if PG_VERSION_NUM >= 180000
						/*
						 * Injection point to test partial failure scenarios
						 * where some uploads succeed but others fail.
						 */
						INJECTION_POINT("parallel-command-complete-nth", NULL);
#endif

						if (state->pgDuckConnection != NULL)
						{
							ReleasePGDuckConnection(state->pgDuckConnection);
							state->pgDuckConnection = NULL;
						}
						multiState->busyConnectionsCount--;

						state->state = COMMAND_EXECUTION_STATE_COMPLETED;
						return true;
					}

					ExecStatusType status = PQresultStatus(result);

					if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK)
					{

						char	   *primaryMsg = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);

						state->errorMessage = primaryMsg ? pstrdup(primaryMsg) : pstrdup("unknown error");
						PQclear(result);

						state->state = COMMAND_EXECUTION_STATE_FAILING;
						return true;
					}

					PQclear(result);
				}

				/* wait for more bytes */
				return false;
			}

		case COMMAND_EXECUTION_STATE_FAILING:
			{
				if (state->errorMessage == NULL)
					state->errorMessage = "lost connection to query engine";

				ReleasePGDuckConnection(state->pgDuckConnection);
				state->pgDuckConnection = NULL;
				multiState->busyConnectionsCount--;

				state->state = COMMAND_EXECUTION_STATE_FAILED;
				return true;
			}

		case COMMAND_EXECUTION_STATE_COMPLETED:
		case COMMAND_EXECUTION_STATE_FAILED:
			/* terminal states, no advancement */
			return false;
	}

	/* unreachable */
	return false;
}


/*
 * CountActiveExecutions returns the number of executions that are not yet completed or failed.
 */
static int
CountActiveExecutions(MultiCommandExecutionState * multiState)
{
	int			activeCount = 0;

	for (int commandIndex = 0; commandIndex < multiState->executionCount; commandIndex++)
	{
		if (multiState->states[commandIndex].state != COMMAND_EXECUTION_STATE_COMPLETED &&
			multiState->states[commandIndex].state != COMMAND_EXECUTION_STATE_FAILED)
			activeCount++;

		/* we currently treat any failure as the end of execution */
		if (multiState->states[commandIndex].state == COMMAND_EXECUTION_STATE_FAILED)
			return 0;
	}

	return activeCount;
}


/*
 * CountWaitingExecutions counts executions that are in a state that requires waiting
 * for IO.
 */
static int32
CountWaitingExecutions(MultiCommandExecutionState * multiState)
{
	int			waitingExecutionsCount = 0;

	for (int commandIndex = 0; commandIndex < multiState->executionCount; commandIndex++)
	{
		CommandExecutionState *state = &multiState->states[commandIndex];

		/*
		 * Count uploads that need I/O operations. Both FLUSHING and WAITING
		 * states need to wait for socket events.
		 */
		if (state->state == COMMAND_EXECUTION_STATE_FLUSHING ||
			state->state == COMMAND_EXECUTION_STATE_WAITING)
			waitingExecutionsCount++;
	}

	return waitingExecutionsCount;
}
