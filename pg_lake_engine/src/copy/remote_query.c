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
 * Routines to copy from a remote Postgres connection and put in a local
 * relation.
 *
 * This is similar to what we do for the PGDuck server, but uses a Postgres
 * source connection instead.
 */

#include "postgres.h"

#include "fmgr.h"
#include "miscadmin.h"
#include "libpq-fe.h"

#include "access/relation.h"
#include "commands/dbcommands.h"
#include "nodes/makefuncs.h"
#include "pg_lake/copy/remote_query.h"
#include "pg_lake/csv/csv_options.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_extension_base/attached_worker.h"
#include "pg_extension_base/base_workers.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

static PGconn *CurrentCopyFromConnection = NULL;
static StringInfoData CopyFromBuffer;

static int	CopyReceivedTransmitDataToBuffer(void *outbuf, int minread, int maxread);
static bool ReceiveCopyData(PGconn *conn, StringInfo buffer);

/*
 * CopyFromRemoteQuery stores the output of a query against the local pgduck
 * server and then copies it into a local relation from the temporary file.
 */
int64
CopyFromRemoteQuery(Oid relationId, char *query, bool useFreeze)
{
	volatile int64 rowsProcessed = 0L;

	if (CurrentCopyFromConnection != NULL)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("nested COPY is not supported")));

	Relation	relation = relation_open(relationId, RowExclusiveLock);
	TupleDesc	tupleDesc = RelationGetDescr(relation);

	/* TODO: only include not-nullified columns */
	List	   *copyColumns = NIL;

	for (int columnIndex = 0; columnIndex < tupleDesc->natts; columnIndex++)
	{
		Form_pg_attribute column = TupleDescAttr(tupleDesc, columnIndex);

		if (column->attisdropped)
			continue;

		/* TODO: exclude nullified? */
		char	   *columnName = NameStr(column->attname);

		copyColumns = lappend(copyColumns, makeString(columnName));
	}

	RangeTblEntry *rte = makeNode(RangeTblEntry);

	rte->rtekind = RTE_RELATION;
	rte->relid = relationId;
	rte->relkind = relation->rd_rel->relkind;
	rte->rellockmode = RowExclusiveLock;
	rte->inh = false;

	RTEPermissionInfo *permInfo = makeNode(RTEPermissionInfo);

	permInfo->relid = relationId;
	permInfo->inh = rte->inh;
	permInfo->requiredPerms = ACL_INSERT;
	permInfo->checkAsUser = GetUserId();

	/* copy asserts require a parse state */
	ParseState *pstate = make_parsestate(NULL);

	pstate->p_rtable = list_make1(rte);
	pstate->p_rteperminfos = list_make1(permInfo);

	/* use default CSV options for transmit */
	bool		includeHeader = false;
	List	   *copyOptions = InternalCSVOptions(includeHeader);

	if (useFreeze)
	{
		DefElem *freezeOption = makeDefElem("freeze", (Node *) makeBoolean(true), -1);

		copyOptions = lappend(copyOptions, freezeOption);
	}

	/* use pgduck_server TRANSMIT command to output CSV */
	char	   *transmitCommand = psprintf("TRANSMIT %s", query);

	/*
	 * Get a connection to send the query to pgduck.
	 */
	PGDuckConnection *pgDuckConn = GetPGDuckConnection();

	/* start the transmit command */
	SendQueryToPGDuck(pgDuckConn, transmitCommand);

	PGresult   *result = PQgetResult(pgDuckConn->conn);

	if (PQresultStatus(result) != PGRES_COPY_OUT)
	{
		CheckPGDuckResult(pgDuckConn, result);
	}

	PQclear(result);

	/*
	 * Initialize the buffer into which CopyReceivedTransmitDataToBuffer
	 * writes CSV-formatted lines.
	 */
	initStringInfo(&CopyFromBuffer);

	/* initialize the current connection */
	CurrentCopyFromConnection = pgDuckConn->conn;

	PG_TRY();
	{
		/* COPY into the table */
		char	   *filename = NULL;
		bool		isProgram = false;
		Node	   *whereClause = NULL;

		CopyFromState cstate = BeginCopyFrom(pstate,
											 relation,
											 whereClause,
											 filename,
											 isProgram,
											 CopyReceivedTransmitDataToBuffer,
											 copyColumns,
											 copyOptions);

		rowsProcessed = CopyFrom(cstate);

		EndCopyFrom(cstate);
	}
	PG_FINALLY();
	{
		ReleasePGDuckConnection(pgDuckConn);
		CurrentCopyFromConnection = NULL;
	}
	PG_END_TRY();

	relation_close(relation, NoLock);

	return rowsProcessed;
}


/*
 * CopyReceivedTransmitDataToBuffer receives data after a TRANSMIT command
 * and writes it to the outbuf.
 *
 * We may get data in batches that do not individually fit into the outbuf.
 * Therefore, we first append to CopyFromBuffer and then copy to outbuf
 * until CopyFromBuffer is fully consumed before fetching more data.
 *
 * We ignore minread because it is always 1 and doing so simplifies
 * the code.
 */
static int
CopyReceivedTransmitDataToBuffer(void *outbuf, int minread, int maxread)
{
	/* if there is data remaining in the buffer, copy that */
	int			bytesInBuffer = CopyFromBuffer.len - CopyFromBuffer.cursor;

	if (bytesInBuffer == 0)
	{
		/* buffer is flushed, reset the length and cursor */
		resetStringInfo(&CopyFromBuffer);

		if (!ReceiveCopyData(CurrentCopyFromConnection, &CopyFromBuffer))
		{
			/* no more tuples */
			return 0;
		}

		/* count how many bytes we have available */
		bytesInBuffer = CopyFromBuffer.len - CopyFromBuffer.cursor;
	}

	int			bytesToCopy = Min(maxread, bytesInBuffer);

	memcpy(outbuf, CopyFromBuffer.data + CopyFromBuffer.cursor, bytesToCopy);
	CopyFromBuffer.cursor += bytesToCopy;

	return bytesToCopy;
}


/*
 * ReceiveCopyData receives bytes via CopyData messages and writes
 * them to the buffer.
 */
static bool
ReceiveCopyData(PGconn *conn, StringInfo buffer)
{
	int			async = 0;
	char	   *copyBuffer = NULL;

	int			bytesReceived = PQgetCopyData(conn, &copyBuffer, async);

	if (bytesReceived < 0)
	{
		/* COPY requires one additional PQgetResult at the end */
		PGresult   *result = PQgetResult(conn);

		if (result == NULL)
		{
			return false;
		}

		/* check for errors in COPY result */
		ExecStatusType resultStatus = PQresultStatus(result);

		if (resultStatus != PGRES_COMMAND_OK)
		{
			char	   *message = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);

			if (message == NULL)
				message = pchomp(PQerrorMessage(conn));

			message = pstrdup(message);

			PQclear(result);

			ereport(ERROR, (errcode(ERRCODE_CONNECTION_FAILURE),
							errmsg("%s", message)));
		}


		PQclear(result);

		return false;
	}

	/*
	 * Slightly pedantic, but appendBinaryStringInfo can throw an OOM and OOM
	 * is a horrible time to be leaking large buffers.
	 */
	PG_TRY();
	{
		appendBinaryStringInfo(buffer, copyBuffer, bytesReceived);
	}
	PG_FINALLY();
	{
		PQfreemem(copyBuffer);
	}
	PG_END_TRY();

	/* can only get 0 return value in async mode */
	Assert(bytesReceived != 0);

	return true;
}
