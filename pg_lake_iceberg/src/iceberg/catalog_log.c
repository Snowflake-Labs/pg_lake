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
 * Catalog activity log lines; see catalog_log.h for the line format and the
 * rules on what may go into one.
 */
#include "postgres.h"

#include <curl/curl.h>

#include "pg_extension_base/pg_compat.h"

#include "access/xact.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "nodes/miscnodes.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/memutils.h"
#include "utils/numeric.h"

#include "pg_lake/iceberg/catalog_log.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/catalog_type.h"

/* longest key=value list a line carries, prefix and event name excluded */
#define CATALOG_LOG_MAX_FIELDS 512

/* error bodies larger than this are not parsed for an error type */
#define CATALOG_LOG_MAX_ERROR_BODY 65536

/* managed via pg_lake_iceberg.log_catalog_events */
bool		LogCatalogEvents = true;

typedef enum CatalogLogLabel
{
	CATALOG_LABEL_HORIZON,
	CATALOG_LABEL_REST_BUILTIN,
	CATALOG_LABEL_REST_SERVER,
	CATALOG_LABEL_OBJECT_STORE,
	CATALOG_LABEL_POSTGRES,
	CATALOG_LABEL_COUNT
}			CatalogLogLabel;

static const char *const CatalogLabelNames[CATALOG_LABEL_COUNT] = {
	[CATALOG_LABEL_HORIZON] = "horizon",
	[CATALOG_LABEL_REST_BUILTIN] = "rest_builtin",
	[CATALOG_LABEL_REST_SERVER] = "rest_server",
	[CATALOG_LABEL_OBJECT_STORE] = "object_store",
	[CATALOG_LABEL_POSTGRES] = "postgres",
};

static const char *const RestCatalogOpNames[REST_CATALOG_OP_COUNT] = {
	[REST_CATALOG_OP_CONFIG] = "config",
	[REST_CATALOG_OP_TOKEN] = "token",
	[REST_CATALOG_OP_LIST_NAMESPACES] = "list_namespaces",
	[REST_CATALOG_OP_GET_NAMESPACE] = "get_namespace",
	[REST_CATALOG_OP_CREATE_NAMESPACE] = "create_namespace",
	[REST_CATALOG_OP_LOAD_TABLE] = "load_table",
	[REST_CATALOG_OP_CREATE_TABLE] = "create_table",
	[REST_CATALOG_OP_PUBLISH_CREATE_TABLE] = "publish_create_table",
	[REST_CATALOG_OP_PUBLISH_DROP_TABLE] = "publish_drop_table",
	[REST_CATALOG_OP_PUBLISH_COMMIT] = "publish_commit",
};

/*
 * Error types a catalog may name in a failed response: the Iceberg REST
 * exception types, and the OAuth2 error codes a token endpoint answers with.
 * Anything else is reported as "other", since the value comes from the
 * remote side.
 */
static const char *const KnownCatalogErrorTypes[] = {
	"AlreadyExistsException",
	"AuthenticationTimeoutException",
	"BadRequestException",
	"CommitFailedException",
	"CommitStateUnknownException",
	"ForbiddenException",
	"IllegalArgumentException",
	"InternalServerError",
	"NamespaceNotEmptyException",
	"NoSuchNamespaceException",
	"NoSuchTableException",
	"NoSuchViewException",
	"NotAuthorizedException",
	"NotFoundException",
	"RESTException",
	"ServiceFailureException",
	"ServiceUnavailableException",
	"UnsupportedOperationException",
	"ValidationException",
	"invalid_client",
	"invalid_grant",
	"invalid_request",
	"invalid_scope",
	"unauthorized_client",
	"unsupported_grant_type",
};

/* Iceberg types pg_lake may meet in a schema without being able to map */
static const char *const KnownIcebergTypes[] = {
	"geography",
	"geometry",
	"timestamp_ns",
	"timestamptz_ns",
	"unknown",
	"variant",
};

typedef struct PendingCatalogEvent
{
	int			nestLevel;
	char	   *event;
	char	   *fields;
}			PendingCatalogEvent;

/* lives in TopTransactionContext */
static List *PendingCatalogEvents = NIL;

static void EmitCatalogLog(const char *event, const char *fields);
static CatalogLogLabel RestCatalogLabel(const RestCatalogOptions * opts);
static CatalogLogLabel CatalogLabelForRelation(Oid relationId);
static const char *CatalogErrorType(const HttpResult * result);
static const char *CurlErrorClass(int curlCode);
static void CatalogLogXactCallback(XactEvent event, void *arg);
static void CatalogLogSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
									  SubTransactionId parentSubid, void *arg);


/*
 * InitCatalogLog registers the transaction callbacks that hold events back
 * until commit.  Called once from _PG_init.
 */
void
InitCatalogLog(void)
{
	RegisterXactCallback(CatalogLogXactCallback, NULL);
	RegisterSubXactCallback(CatalogLogSubXactCallback, NULL);
}


/*
 * EmitCatalogLog writes one line.  It goes to the server log only: these
 * lines are for whoever operates the server, and keeping them off the client
 * connection makes them safe to emit at commit.
 */
static void
EmitCatalogLog(const char *event, const char *fields)
{
	ereport(LOG_SERVER_ONLY,
			(errmsg_internal(CATALOG_LOG_PREFIX "%s %s", event, fields),
			 errhidestmt(true),
			 errhidecontext(true)));
}


/*
 * LogCatalogEvent logs event with the given key=value fields right away.
 * It allocates nothing of its own, so it is usable after commit.
 */
void
LogCatalogEvent(const char *event, const char *fieldsFormat,...)
{
	if (!LogCatalogEvents)
		return;

	char		fields[CATALOG_LOG_MAX_FIELDS];
	va_list		args;

	va_start(args, fieldsFormat);
	vsnprintf(fields, sizeof(fields), fieldsFormat, args);
	va_end(args);

	EmitCatalogLog(event, fields);
}


/*
 * LogCatalogEventAtCommit logs event when, and only if, the current
 * transaction commits, so that a statement which is rolled back reports
 * nothing.  An event recorded in a subtransaction that aborts is dropped.
 */
void
LogCatalogEventAtCommit(const char *event, const char *fieldsFormat,...)
{
	if (!LogCatalogEvents)
		return;

	char		fields[CATALOG_LOG_MAX_FIELDS];
	va_list		args;

	va_start(args, fieldsFormat);
	vsnprintf(fields, sizeof(fields), fieldsFormat, args);
	va_end(args);

	MemoryContext oldContext = MemoryContextSwitchTo(TopTransactionContext);
	PendingCatalogEvent *pending = palloc(sizeof(PendingCatalogEvent));

	pending->nestLevel = GetCurrentTransactionNestLevel();
	pending->event = pstrdup(event);
	pending->fields = pstrdup(fields);
	PendingCatalogEvents = lappend(PendingCatalogEvents, pending);

	MemoryContextSwitchTo(oldContext);
}


static void
CatalogLogXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
		case XACT_EVENT_PREPARE:
			{
				ListCell   *cell = NULL;

				foreach(cell, PendingCatalogEvents)
				{
					PendingCatalogEvent *pending = lfirst(cell);

					EmitCatalogLog(pending->event, pending->fields);
				}

				PendingCatalogEvents = NIL;
				break;
			}

		case XACT_EVENT_ABORT:
		case XACT_EVENT_PARALLEL_ABORT:
			PendingCatalogEvents = NIL;
			break;

		default:
			break;
	}
}


static void
CatalogLogSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
						  SubTransactionId parentSubid, void *arg)
{
	if (PendingCatalogEvents == NIL)
		return;

	int			nestLevel = GetCurrentTransactionNestLevel();
	ListCell   *cell = NULL;

	if (event == SUBXACT_EVENT_COMMIT_SUB)
	{
		foreach(cell, PendingCatalogEvents)
		{
			PendingCatalogEvent *pending = lfirst(cell);

			if (pending->nestLevel >= nestLevel)
				pending->nestLevel = nestLevel - 1;
		}
	}
	else if (event == SUBXACT_EVENT_ABORT_SUB)
	{
		foreach(cell, PendingCatalogEvents)
		{
			PendingCatalogEvent *pending = lfirst(cell);

			if (pending->nestLevel >= nestLevel)
				PendingCatalogEvents = foreach_delete_current(PendingCatalogEvents, cell);
		}
	}
}


/*
 * RestCatalogLabel tells catalog servers users created apart from the
 * built-in catalog, and the built-in catalog with horizon authentication
 * apart from the built-in catalog with any other.
 */
static CatalogLogLabel
RestCatalogLabel(const RestCatalogOptions * opts)
{
	if (!opts->isBuiltin)
		return CATALOG_LABEL_REST_SERVER;

	return opts->authType == REST_CATALOG_AUTH_TYPE_HORIZON ?
		CATALOG_LABEL_HORIZON : CATALOG_LABEL_REST_BUILTIN;
}


const char *
RestCatalogLogLabel(const RestCatalogOptions * opts)
{
	return CatalogLabelNames[RestCatalogLabel(opts)];
}


/*
 * CatalogLabelForRelation labels an Iceberg table by its catalog without
 * resolving the catalog's options, which validates credentials and can
 * throw for reasons that have nothing to do with the table.
 */
static CatalogLogLabel
CatalogLabelForRelation(Oid relationId)
{
	switch (GetIcebergCatalogType(relationId))
	{
		case REST_CATALOG_READ_ONLY:
		case REST_CATALOG_READ_WRITE:
			{
				ForeignTable *foreignTable = GetForeignTable(relationId);
				char	   *catalog = GetStringOption(foreignTable->options, "catalog", false);

				if (!IsBuiltinCatalogServerName(ResolveCatalogServerName(catalog)))
					return CATALOG_LABEL_REST_SERVER;

				/* the built-in catalog takes its auth type from the GUC alone */
				return RestCatalogAuthType == REST_CATALOG_AUTH_TYPE_HORIZON ?
					CATALOG_LABEL_HORIZON : CATALOG_LABEL_REST_BUILTIN;
			}

		case OBJECT_STORE_READ_ONLY:
		case OBJECT_STORE_READ_WRITE:
			return CATALOG_LABEL_OBJECT_STORE;

		default:
			return CATALOG_LABEL_POSTGRES;
	}
}


const char *
CatalogLogLabelForRelation(Oid relationId)
{
	return CatalogLabelNames[CatalogLabelForRelation(relationId)];
}


/*
 * LogCatalogRequest logs a request_failed line when a REST catalog request
 * failed after its retries.
 *
 * 404 is not logged: probing for a namespace or table that is not there is
 * a normal part of attaching and creating tables, and a missing table the
 * user asked for is reported to the user anyway.
 *
 * This runs after commit for the requests that publish changes, so it must
 * not throw; parsing the error body is done without raising errors.
 */
void
LogCatalogRequest(const RestCatalogOptions * opts, RestCatalogOp op,
				  const HttpResult * result, int attempts, bool authRefreshed,
				  long elapsedMs)
{
	if (!LogCatalogEvents)
		return;

	Assert(op >= 0 && op < REST_CATALOG_OP_COUNT);

	const char *catalog = RestCatalogLogLabel(opts);

	if (result->status == 0)
		LogCatalogEvent("request_failed",
						"catalog=%s op=%s status=0 curl_error=%s curl_code=%d "
						"attempts=%d auth_refreshed=%s ms=%ld",
						catalog, RestCatalogOpNames[op],
						CurlErrorClass(result->curlCode), result->curlCode,
						attempts, authRefreshed ? "true" : "false", elapsedMs);
	else if (result->status >= 400 && result->status != 404)
		LogCatalogEvent("request_failed",
						"catalog=%s op=%s status=%ld error_type=%s "
						"attempts=%d auth_refreshed=%s ms=%ld",
						catalog, RestCatalogOpNames[op],
						result->status, CatalogErrorType(result),
						attempts, authRefreshed ? "true" : "false", elapsedMs);
}


/*
 * LogCatalogAuthFailure logs that no credential could be obtained for a
 * REST catalog, from a credential provider or from pg_lake's own OAuth2
 * grant.  Only the SQLSTATE of the error is logged; its message may carry
 * whatever the remote side answered.
 */
void
LogCatalogAuthFailure(const RestCatalogOptions * opts, const char *source,
					  int sqlerrcode)
{
	if (sqlerrcode == ERRCODE_QUERY_CANCELED || sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		return;

	LogCatalogEvent("auth_failed", "catalog=%s source=%s sqlstate=%s",
					RestCatalogLogLabel(opts), source, unpack_sql_state(sqlerrcode));
}


/*
 * LogCatalogUnsupportedType logs a schema type pg_lake cannot map, by its
 * Iceberg type name when that is one we expect, or as "other".
 */
void
LogCatalogUnsupportedType(const char *icebergType)
{
	const char *type = "other";

	for (int i = 0; i < lengthof(KnownIcebergTypes); i++)
	{
		size_t		length = strlen(KnownIcebergTypes[i]);

		/* geometry and geography carry their parameters in parentheses */
		if (strncmp(icebergType, KnownIcebergTypes[i], length) == 0 &&
			(icebergType[length] == '\0' || icebergType[length] == '('))
		{
			type = KnownIcebergTypes[i];
			break;
		}
	}

	LogCatalogEvent("unsupported_feature", "feature=type type=%s", type);
}


/*
 * CatalogErrorType returns the error type a failed response names, from
 * the Iceberg error envelope or an OAuth2 error response, when it is one
 * we know; "other" when it is not, and "none" when there is none.
 */
static const char *
CatalogErrorType(const HttpResult * result)
{
	if (result->body == NULL || result->bodyLength == 0 ||
		result->bodyLength > CATALOG_LOG_MAX_ERROR_BODY)
		return "none";

	ErrorSaveContext escontext = {T_ErrorSaveContext};
	Datum		jsonbDatum;

	if (!DirectInputFunctionCallSafe(jsonb_in, result->body, InvalidOid, -1,
									 (Node *) &escontext, &jsonbDatum))
		return "none";

	Jsonb	   *jsonb = DatumGetJsonbP(jsonbDatum);
	char	   *type = JsonbGetOptionalString(jsonb, 2, "error", "type");

	if (type == NULL)
		type = JsonbGetOptionalString(jsonb, 1, "error");

	if (type == NULL)
		return "none";

	for (int i = 0; i < lengthof(KnownCatalogErrorTypes); i++)
	{
		if (strcmp(type, KnownCatalogErrorTypes[i]) == 0)
			return KnownCatalogErrorTypes[i];
	}

	return "other";
}


/*
 * CurlErrorClass groups the libcurl error a request failed with.  The
 * message libcurl writes alongside names the host, so it is not logged.
 */
static const char *
CurlErrorClass(int curlCode)
{
	switch (curlCode)
	{
		case CURLE_COULDNT_RESOLVE_PROXY:
		case CURLE_COULDNT_RESOLVE_HOST:
			return "dns";

		case CURLE_COULDNT_CONNECT:
			return "connect";

		case CURLE_OPERATION_TIMEDOUT:
			return "timeout";

		case CURLE_SSL_CONNECT_ERROR:
		case CURLE_PEER_FAILED_VERIFICATION:
		case CURLE_SSL_CERTPROBLEM:
		case CURLE_SSL_CIPHER:
		case CURLE_SSL_CACERT_BADFILE:
			return "tls";

		case CURLE_SEND_ERROR:
		case CURLE_RECV_ERROR:
		case CURLE_GOT_NOTHING:
		case CURLE_PARTIAL_FILE:
			return "transfer";

		case CURLE_ABORTED_BY_CALLBACK:
			return "aborted";

		case CURLE_OUT_OF_MEMORY:
			return "out_of_memory";

		default:
			return "other";
	}
}


/*
 * IcebergFormatVersionFromMetadata reads format-version from a table
 * metadata document, or returns -1 when it has none we can read.
 */
int
IcebergFormatVersionFromMetadata(Jsonb *metadata)
{
	if (metadata == NULL || !JsonContainerIsObject(&metadata->root))
		return -1;

	JsonbValue	key;

	key.type = jbvString;
	key.val.string.val = "format-version";
	key.val.string.len = strlen("format-version");

	JsonbValue *value = findJsonbValueFromContainer(&metadata->root, JB_FOBJECT, &key);

	if (value == NULL || value->type != jbvNumeric)
		return -1;

	bool		haveError = false;
	int			formatVersion = numeric_int4_opt_error(value->val.numeric, &haveError);

	return haveError ? -1 : formatVersion;
}
