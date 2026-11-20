/*
 * Copyright 2026 Snowflake Inc.
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

#include "access/htup_details.h"
#include "access/reloptions.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "nodes/parsenodes.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/query_engine.h"
#include "pg_lake/util/rel_utils.h"
#include "utils/syscache.h"


static PGDuckConnection * GetExternalEngineConnection(Oid serverId, Oid userId);
static List *GetExternalEngineUserOptions(Oid userId, Oid serverId);
static char *BuildExternalEngineConninfo(ForeignServer *server, List *userOptions);
static void AppendConninfoOption(StringInfo conninfo, const char *keyword,
								 const char *value);


/*
 * IsExternalEngineTable returns whether the given relation is a foreign table
 * of the pg_lake_remote foreign data wrapper, which is served by an external
 * query engine. Users name these servers, so unlike lake tables we identify
 * them by their foreign data wrapper.
 */
bool
IsExternalEngineTable(Oid relationId)
{
	HeapTuple	foreignTableTuple = SearchSysCache1(FOREIGNTABLEREL,
													ObjectIdGetDatum(relationId));

	if (!HeapTupleIsValid(foreignTableTuple))
		return false;

	Oid			serverId = ((Form_pg_foreign_table) GETSTRUCT(foreignTableTuple))->ftserver;

	ReleaseSysCache(foreignTableTuple);

	ForeignServer *server = GetForeignServer(serverId);
	Oid			remoteFdwId = get_foreign_data_wrapper_oid(PG_LAKE_REMOTE_FDW_NAME, true);

	return OidIsValid(remoteFdwId) && server->fdwid == remoteFdwId;
}


/*
 * GetRelationQueryEngine returns the query engine that serves the given
 * relation.
 */
Oid
GetRelationQueryEngine(Oid relationId)
{
	if (!IsAnyLakeForeignTableById(relationId))
		elog(ERROR, "relation %s is not served by a query engine",
			 GetQualifiedRelationName(relationId));

	return PGDUCK_QUERY_ENGINE;
}


/*
 * GetQueryEngineForRteList returns the query engine that serves the relations
 * in the given list of RTEs. A single query can only be sent to one engine,
 * so all relations must be served by the same engine. A query that reads no
 * relations runs on pgduck_server.
 */
Oid
GetQueryEngineForRteList(List *rteList)
{
	Oid			queryEngine = PGDUCK_QUERY_ENGINE;
	ListCell   *rteCell = NULL;

	foreach(rteCell, rteList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(rteCell);
		Oid			relationEngine = GetRelationQueryEngine(rte->relid);

		if (foreach_current_index(rteCell) > 0 && relationEngine != queryEngine)
			elog(ERROR, "cannot query relations served by different query "
				 "engines together");

		queryEngine = relationEngine;
	}

	return queryEngine;
}


/*
 * GetQueryEngineName returns the name of the given query engine: the name of
 * its foreign server, or pgduck_server.
 */
char *
GetQueryEngineName(Oid queryEngine)
{
	if (queryEngine == PGDUCK_QUERY_ENGINE)
		return "pgduck_server";

	return GetForeignServer(queryEngine)->servername;
}


/*
 * GetQueryEngineConnection returns a new connection to the given query
 * engine. An external engine is reached with the user mapping of
 * checkAsUser, the user that the scanned tables are read as, or of the
 * current user if it is InvalidOid, like ExecCheckPermissions does.
 */
PGDuckConnection *
GetQueryEngineConnection(Oid queryEngine, Oid checkAsUser)
{
	if (queryEngine == PGDUCK_QUERY_ENGINE)
		return GetPGDuckConnection();

	Oid			userId = OidIsValid(checkAsUser) ? checkAsUser : GetUserId();

	return GetExternalEngineConnection(queryEngine, userId);
}


/*
 * GetExternalEngineConnection returns a new connection to the external query
 * engine of the given foreign server, as the given user.
 *
 * Like postgres_fdw, non-superusers have to authenticate with the password
 * from their user mapping, unless a superuser set password_required to false
 * on it. Otherwise they could use the engine's trust or peer authentication,
 * or the password file of the operating system user, under a remote user
 * name of their choosing.
 */
static PGDuckConnection *
GetExternalEngineConnection(Oid serverId, Oid userId)
{
	ForeignServer *server = GetForeignServer(serverId);
	List	   *userOptions = GetExternalEngineUserOptions(userId, serverId);
	bool		passwordRequired = !superuser_arg(userId) &&
		GetBoolOption(userOptions, "password_required", true);

	if (passwordRequired && !HasOption(userOptions, "password"))
		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				 errmsg("password is required"),
				 errdetail("Non-superusers must provide a password in the user mapping.")));

	char	   *conninfo = BuildExternalEngineConninfo(server, userOptions);
	char	   *errorMessage = NULL;
	PGDuckConnection *connection =
		TryGetPGDuckConnectionWithConninfo(conninfo, server->servername,
										   &errorMessage);

	if (connection == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION),
				 errmsg("could not connect to server \"%s\"", server->servername),
				 errdetail_internal("%s", pchomp(errorMessage))));

	if (passwordRequired && !PQconnectionUsedPassword(connection->conn))
	{
		ReleasePGDuckConnection(connection);

		ereport(ERROR,
				(errcode(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
				 errmsg("password is required"),
				 errdetail("Non-superuser cannot connect if the server does not request a password."),
				 errhint("Target server's authentication method must be changed or set the password_required=false in the user mapping attributes.")));
	}

	return connection;
}


/*
 * GetExternalEngineUserOptions returns the options of the user mapping of the
 * given user for the given server, falling back to the PUBLIC user mapping.
 * Unlike GetUserMapping, it returns NIL when neither exists, since a user
 * mapping is optional for an engine that does not need credentials.
 */
static List *
GetExternalEngineUserOptions(Oid userId, Oid serverId)
{
	HeapTuple	userMappingTuple = SearchSysCache2(USERMAPPINGUSERSERVER,
												   ObjectIdGetDatum(userId),
												   ObjectIdGetDatum(serverId));

	if (!HeapTupleIsValid(userMappingTuple))
		userMappingTuple = SearchSysCache2(USERMAPPINGUSERSERVER,
										   ObjectIdGetDatum(InvalidOid),
										   ObjectIdGetDatum(serverId));

	if (!HeapTupleIsValid(userMappingTuple))
		return NIL;

	bool		isNull = false;
	Datum		optionsDatum = SysCacheGetAttr(USERMAPPINGUSERSERVER,
											   userMappingTuple,
											   Anum_pg_user_mapping_umoptions,
											   &isNull);
	List	   *options = isNull ? NIL : untransformRelOptions(optionsDatum);

	ReleaseSysCache(userMappingTuple);

	return options;
}


/*
 * BuildExternalEngineConninfo returns the conninfo for connecting to the
 * given server: the options in its conninfo, followed by the libpq options
 * of the user mapping (the user, secrets such as the password, and client
 * certificates).
 *
 * We parse the server conninfo and write it back out as keyword/value pairs,
 * since it may be a URI, to which we could not append the user mapping.
 */
static char *
BuildExternalEngineConninfo(ForeignServer *server, List *userOptions)
{
	char	   *serverConninfo = GetStringOption(server->options, "conninfo", true);
	char	   *errorMessage = NULL;
	PQconninfoOption *connOptions = PQconninfoParse(serverConninfo, &errorMessage);

	if (connOptions == NULL)
	{
		char	   *detail = errorMessage != NULL ? pchomp(errorMessage) : "out of memory";

		PQfreemem(errorMessage);
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("invalid \"conninfo\" option for server \"%s\"",
						server->servername),
				 errdetail_internal("%s", detail)));
	}

	StringInfoData conninfo;

	initStringInfo(&conninfo);

	for (PQconninfoOption *connOption = connOptions; connOption->keyword != NULL; connOption++)
	{
		if (connOption->val != NULL)
			AppendConninfoOption(&conninfo, connOption->keyword, connOption->val);
	}

	PQconninfoFree(connOptions);

	ListCell   *optionCell = NULL;

	foreach(optionCell, userOptions)
	{
		DefElem    *option = (DefElem *) lfirst(optionCell);

		if (strcmp(option->defname, "password_required") == 0)
			continue;

		AppendConninfoOption(&conninfo, option->defname, defGetString(option));
	}

	return conninfo.data;
}


/*
 * AppendConninfoOption appends keyword='value' to a conninfo string, escaping
 * backslashes and single quotes in the value.
 */
static void
AppendConninfoOption(StringInfo conninfo, const char *keyword, const char *value)
{
	if (conninfo->len > 0)
		appendStringInfoChar(conninfo, ' ');

	appendStringInfo(conninfo, "%s='", keyword);

	for (const char *valueChar = value; *valueChar != '\0'; valueChar++)
	{
		if (*valueChar == '\\' || *valueChar == '\'')
			appendStringInfoChar(conninfo, '\\');

		appendStringInfoChar(conninfo, *valueChar);
	}

	appendStringInfoChar(conninfo, '\'');
}
