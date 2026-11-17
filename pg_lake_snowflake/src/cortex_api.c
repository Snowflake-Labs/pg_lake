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

/*
 * cortex_api.c
 *
 * Requests to the Cortex REST API of a Snowflake account.
 *
 * Unlike a SQL API statement, a Cortex REST request runs no SQL and needs no
 * warehouse: the account bills the tokens the model processes. It is
 * authorized with the same credentials as a statement, but by the default
 * role of the Snowflake user, because the API takes no role. That role needs
 * the SNOWFLAKE.CORTEX_USER or SNOWFLAKE.CORTEX_REST_API_USER database role.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "catalog/pg_foreign_server.h"
#include "foreign/foreign.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"

#include "pg_lake/http/http_client.h"

#include "pg_lake_snowflake/auth.h"
#include "pg_lake_snowflake/import_schema.h"
#include "pg_lake_snowflake/options.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"

/* every Cortex REST endpoint lives below this path */
#define CORTEX_API_PATH_PREFIX "/api/v2/cortex/"

/* how many times a request is retried when the service asks us to back off */
#define CORTEX_MAX_RETRIES 5

PG_FUNCTION_INFO_V1(pg_lake_snowflake_cortex_request);

static Jsonb *SendCortexRequest(SnowflakeConnection * connection, const char *path,
								const char *body);
static bool ShouldRetryCortexStatus(long statusCode);
static void ReportCortexError(SnowflakeConnection * connection, HttpResult * result,
							  const char *path);
static char *CortexErrorMessage(HttpResult * result);
static char *JsonStringField(Jsonb *json, const char *fieldName);


/*
 * pg_lake_snowflake_cortex_request posts a JSON body to a Cortex REST endpoint
 * of the account behind a foreign server and returns the JSON answer.
 *
 * The caller needs the same privilege on the server that
 * lake_snowflake.execute needs, and the request is made with the credentials
 * of the user mapping that applies to the caller.
 */
Datum
pg_lake_snowflake_cortex_request(PG_FUNCTION_ARGS)
{
	char	   *serverName = text_to_cstring(PG_GETARG_TEXT_PP(0));
	char	   *path = text_to_cstring(PG_GETARG_TEXT_PP(1));
	Jsonb	   *body = PG_GETARG_JSONB_P(2);

	/*
	 * Only Cortex endpoints. Other REST endpoints of the account have their
	 * own privilege model, which a foreign server is not meant to grant.
	 */
	if (strncmp(path, CORTEX_API_PATH_PREFIX, strlen(CORTEX_API_PATH_PREFIX)) != 0 ||
		strstr(path, "..") != NULL || strchr(path, '?') != NULL ||
		strchr(path, '#') != NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("\"%s\" is not a Cortex REST API path", path),
				 errhint("The path has to start with \"%s\".",
						 CORTEX_API_PATH_PREFIX)));
	}

	ForeignServer *server = GetForeignServerByName(serverName, false);

	if (!IsSnowflakeForeignServer(server->serverid))
	{
		ereport(ERROR,
				(errcode(ERRCODE_WRONG_OBJECT_TYPE),
				 errmsg("foreign server \"%s\" is not a Snowflake server",
						serverName)));
	}

	AclResult	aclResult = object_aclcheck(ForeignServerRelationId, server->serverid,
											GetUserId(), ACL_USAGE);

	if (aclResult != ACLCHECK_OK)
		aclcheck_error(aclResult, OBJECT_FOREIGN_SERVER, serverName);

	SnowflakeConnection *connection = GetSnowflakeConnection(server->serverid,
															 GetUserId());
	char	   *bodyText = JsonbToCString(NULL, &body->root, VARSIZE(body));

	PG_RETURN_JSONB_P(SendCortexRequest(connection, path, bodyText));
}


/*
 * SendCortexRequest performs one Cortex REST request, retrying when the
 * service is busy, and returns the parsed answer.
 */
static Jsonb *
SendCortexRequest(SnowflakeConnection * connection, const char *path, const char *body)
{
	char	   *url = psprintf("%s%s", connection->accountUrl, path);
	MemoryContext requestContext = AllocSetContextCreate(CurrentMemoryContext,
														 "pg_lake_snowflake cortex request",
														 ALLOCSET_DEFAULT_SIZES);
	MemoryContext callerContext = CurrentMemoryContext;

	/* volatile because it is set inside PG_TRY and read after it */
	Jsonb	   *volatile response = NULL;

	if (SnowflakeLogRemoteSql)
		ereport(LOG, (errmsg("pg_lake_snowflake: POST %s", path)));

	PG_TRY();
	{
		HttpResult	result = {0};

		for (int attempt = 1; attempt <= CORTEX_MAX_RETRIES; attempt++)
		{
			MemoryContext previousContext = MemoryContextSwitchTo(requestContext);
			List	   *headers = SnowflakeRequestHeaders(connection, true);

			CHECK_FOR_INTERRUPTS();

			result = HttpPost(url, body, headers);

			MemoryContextSwitchTo(previousContext);

			if (result.status == HTTP_STATUS_UNAUTHORIZED && attempt == 1)
			{
				/*
				 * A rejected token may be one we minted and still consider
				 * valid, so drop it and try once with a fresh one.
				 */
				SnowflakeForgetCachedToken(connection);
				continue;
			}

			if (ShouldRetryCortexStatus(result.status) && attempt < CORTEX_MAX_RETRIES)
			{
				SnowflakeSleepMs(LinearBackoffSleepMs(500, attempt));
				continue;
			}

			break;
		}

		if (result.status != 200 || result.body == NULL || result.body[0] != '{')
			ReportCortexError(connection, &result, path);

		MemoryContext previousContext = MemoryContextSwitchTo(callerContext);

		response = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
													  CStringGetDatum(result.body)));

		MemoryContextSwitchTo(previousContext);
	}
	PG_FINALLY();
	{
		MemoryContextDelete(requestContext);
	}
	PG_END_TRY();

	return response;
}


/*
 * ShouldRetryCortexStatus returns whether a status means "come back later"
 * rather than "this request was wrong". Cortex answers 429 when the account is
 * over the rate limit of a model and 503 when inference timed out.
 */
static bool
ShouldRetryCortexStatus(long statusCode)
{
	return statusCode == HTTP_STATUS_TOO_MANY_REQUESTS ||
		statusCode == HTTP_STATUS_SERVICE_UNAVAILABLE ||
		statusCode == 502 ||
		statusCode == 504;
}


/*
 * ReportCortexError raises the error a failed Cortex request deserves.
 */
static void
ReportCortexError(SnowflakeConnection * connection, HttpResult * result, const char *path)
{
	if (result->body == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("could not reach the Cortex REST API of server \"%s\"",
						connection->serverName),
				 errdetail("HTTP status %ld: %s", result->status,
						   result->errorMsg != NULL ? result->errorMsg :
						   "no response body"),
				 errcontext("Cortex request: POST %s", path)));
	}

	char	   *message = CortexErrorMessage(result);

	if (result->status == HTTP_STATUS_UNAUTHORIZED)
	{
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION),
				 errmsg("Snowflake rejected the credentials of server \"%s\": %s",
						connection->serverName, message),
				 errhint("Check the user mapping. A programmatic access token "
						 "expires, and key-pair authentication needs the public "
						 "key registered on the Snowflake user.")));
	}

	ereport(ERROR,
			(errcode(ERRCODE_FDW_ERROR),
			 errmsg("Cortex request failed: %s", message),
			 errdetail("HTTP status %ld.", result->status),
			 result->status == 403 ?
			 errhint("The default role of the Snowflake user needs the "
					 "SNOWFLAKE.CORTEX_USER database role, and the account "
					 "needs access to the model.") : 0,
			 errcontext("Cortex request: POST %s", path)));
}


/*
 * CortexErrorMessage extracts the message of an error answer. Cortex sometimes
 * wraps the message of the inference service, itself a JSON object, in the
 * message of its own answer, and the inner one is the one worth reading.
 */
static char *
CortexErrorMessage(HttpResult * result)
{
	char	   *message = NULL;
	MemoryContext callerContext = CurrentMemoryContext;

	if (result->body[0] == '{')
	{
		PG_TRY();
		{
			Jsonb	   *errorJson = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
																	   CStringGetDatum(result->body)));

			message = JsonStringField(errorJson, "message");

			if (message != NULL && message[0] == '{')
			{
				Jsonb	   *innerJson = DatumGetJsonbP(DirectFunctionCall1(jsonb_in,
																		   CStringGetDatum(message)));
				char	   *innerMessage = JsonStringField(innerJson, "message");

				if (innerMessage != NULL)
					message = innerMessage;
			}
		}
		PG_CATCH();
		{
			/* a message that is not JSON after all is reported as it is */
			MemoryContextSwitchTo(callerContext);
			FlushErrorState();
		}
		PG_END_TRY();
	}

	if (message == NULL)
		message = pnstrdup(result->body, Min(result->bodyLength, 512));

	return message;
}


/*
 * JsonStringField returns a string field of a JSON object, or NULL.
 */
static char *
JsonStringField(Jsonb *json, const char *fieldName)
{
	JsonbValue	fieldKey;

	if (!JB_ROOT_IS_OBJECT(json))
		return NULL;

	fieldKey.type = jbvString;
	fieldKey.val.string.val = (char *) fieldName;
	fieldKey.val.string.len = strlen(fieldName);

	JsonbValue *value = findJsonbValueFromContainer(&json->root, JB_FOBJECT, &fieldKey);

	if (value == NULL || value->type != jbvString)
		return NULL;

	return pnstrdup(value->val.string.val, value->val.string.len);
}
