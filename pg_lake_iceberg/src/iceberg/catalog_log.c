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
 * Catalog error log lines; see catalog_log.h for the line format and the
 * rules on what may go into one.  Every sentence is written here, so that
 * callers pass only values from a fixed vocabulary.
 */
#include "postgres.h"

#include <curl/curl.h>

#include "pg_extension_base/pg_compat.h"

#include "pg_lake/iceberg/catalog_log.h"

/* longest sentence a line carries, prefix excluded */
#define CATALOG_LOG_MAX_LINE 256

/* managed via pg_lake_iceberg.log_catalog_events */
bool		LogCatalogEvents = true;

static const char *const RestCatalogOpActions[REST_CATALOG_OP_COUNT] = {
	[REST_CATALOG_OP_CONFIG] = "get the configuration of",
	[REST_CATALOG_OP_TOKEN] = "get an access token from",
	[REST_CATALOG_OP_LIST_NAMESPACES] = "list namespaces in",
	[REST_CATALOG_OP_GET_NAMESPACE] = "look up a namespace in",
	[REST_CATALOG_OP_CREATE_NAMESPACE] = "create a namespace in",
	[REST_CATALOG_OP_LOAD_TABLE] = "load a table from",
	[REST_CATALOG_OP_CREATE_TABLE] = "create a table in",
	[REST_CATALOG_OP_PUBLISH_CREATE_TABLE] = "publish a new table to",
	[REST_CATALOG_OP_PUBLISH_DROP_TABLE] = "publish a table drop to",
	[REST_CATALOG_OP_PUBLISH_COMMIT] = "publish committed changes to",
};

/*
 * Error types a catalog may name in a failed response: the Iceberg REST
 * exception types, and the OAuth2 error codes a token endpoint answers with.
 * Any other value comes from the remote side and is left out.
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

static void LogCatalogLine(const char *format,...) pg_attribute_printf(1, 2);
static const char *RestCatalogLogName(const RestCatalogOptions * opts);
static const char *CatalogErrorType(const HttpResult * result);
static const char *NetworkProblem(int curlCode);


/*
 * LogCatalogLine writes one line, to the server log only: these lines are
 * for whoever operates the server, and keeping them off the client
 * connection makes them safe to write after commit.  It allocates nothing of
 * its own, for the same reason.
 */
static void
LogCatalogLine(const char *format,...)
{
	if (!LogCatalogEvents)
		return;

	char		line[CATALOG_LOG_MAX_LINE];
	va_list		args;

	va_start(args, format);
	vsnprintf(line, sizeof(line), format, args);
	va_end(args);

	ereport(LOG_SERVER_ONLY,
			(errmsg_internal(CATALOG_LOG_PREFIX "%s", line),
			 errhidestmt(true),
			 errhidecontext(true)));
}


/*
 * RestCatalogLogName tells catalog servers users created apart from the
 * built-in catalog, and the built-in catalog with horizon authentication
 * apart from the built-in catalog with any other.
 */
static const char *
RestCatalogLogName(const RestCatalogOptions * opts)
{
	if (!opts->isBuiltin)
		return "a catalog server";

	return opts->authType == REST_CATALOG_AUTH_TYPE_HORIZON ?
		"the Horizon catalog" : "the built-in REST catalog";
}


/*
 * LogCatalogRequest logs a REST catalog request that failed after its
 * retries.
 *
 * 404 is not logged for most requests: probing for a namespace or table
 * that is not there is a normal part of attaching and creating tables, a
 * missing table the user asked for is reported to the user anyway, and a
 * dropped table being gone from the catalog already is the outcome the drop
 * wanted.  Publishing a new table or committed changes is the exception:
 * there a 404 means the catalog lost the namespace or table that Postgres
 * just committed to.
 *
 * This runs after commit for the requests that publish changes, so it must
 * not throw; parsing the error body is done without raising errors.
 */
void
LogCatalogRequest(const RestCatalogOptions * opts, RestCatalogOp op,
				  const HttpResult * result)
{
	if (!LogCatalogEvents)
		return;

	Assert(op >= 0 && op < REST_CATALOG_OP_COUNT);

	const char *action = RestCatalogOpActions[op];
	const char *catalog = RestCatalogLogName(opts);

	if (result->status == 0)
	{
		const char *problem = NetworkProblem(result->curlCode);

		if (problem != NULL)
			LogCatalogLine("could not %s %s: %s", action, catalog, problem);
	}
	else if (result->status >= 400 &&
			 (result->status != 404 ||
			  op == REST_CATALOG_OP_PUBLISH_CREATE_TABLE ||
			  op == REST_CATALOG_OP_PUBLISH_COMMIT))
	{
		const char *errorType = CatalogErrorType(result);

		if (errorType != NULL)
			LogCatalogLine("could not %s %s: HTTP %ld (%s)",
						   action, catalog, result->status, errorType);
		else
			LogCatalogLine("could not %s %s: HTTP %ld",
						   action, catalog, result->status);
	}
}


/*
 * LogCatalogAuthFailure logs that no credential could be obtained for a
 * REST catalog, from a credential provider or from pg_lake's own OAuth2
 * grant.  Only the SQLSTATE of the error is logged; its message may carry
 * whatever the remote side answered.
 */
void
LogCatalogAuthFailure(const RestCatalogOptions * opts, int sqlerrcode)
{
	if (sqlerrcode == ERRCODE_QUERY_CANCELED || sqlerrcode == ERRCODE_ADMIN_SHUTDOWN)
		return;

	LogCatalogLine("could not authenticate to %s (SQLSTATE %s)",
				   RestCatalogLogName(opts), unpack_sql_state(sqlerrcode));
}


/*
 * LogCatalogNoVendedCredentials logs that a table asked its catalog for
 * vended credentials and got none.
 */
void
LogCatalogNoVendedCredentials(const RestCatalogOptions * opts)
{
	LogCatalogLine("%s returned no vended credentials for a table",
				   RestCatalogLogName(opts));
}


/*
 * LogCatalogUnusableVendedCredentials logs that a catalog vended credentials
 * pg_lake cannot use.  storageName is the fixed name of the kind of storage
 * they are for, such as "Google Cloud Storage".
 */
void
LogCatalogUnusableVendedCredentials(const RestCatalogOptions * opts,
									const char *storageName)
{
	LogCatalogLine("ignoring vended credentials for %s from %s that pg_lake cannot use",
				   storageName, RestCatalogLogName(opts));
}


/*
 * LogCatalogAzureEndpointNotAllowed logs that a catalog vended an Azure
 * endpoint pg_lake does not send credentials to.
 */
void
LogCatalogAzureEndpointNotAllowed(void)
{
	LogCatalogLine("ignoring a vended Azure endpoint that is not allowed");
}


/*
 * LogCatalogCredentialResolutionFailure logs that resolving the storage
 * credentials a table needs failed.
 */
void
LogCatalogCredentialResolutionFailure(int sqlerrcode)
{
	LogCatalogLine("could not resolve storage credentials for a table (SQLSTATE %s)",
				   unpack_sql_state(sqlerrcode));
}


/*
 * LogCatalogUnsupportedFeature logs that a table uses an Iceberg feature
 * pg_lake cannot read.
 */
void
LogCatalogUnsupportedFeature(CatalogLogFeature feature)
{
	switch (feature)
	{
		case CATALOG_LOG_FEATURE_DELETION_VECTORS:
			LogCatalogLine("a table uses Iceberg deletion vectors, "
						   "which pg_lake does not support");
			break;

		case CATALOG_LOG_FEATURE_COLUMN_DEFAULTS:
			LogCatalogLine("a table uses Iceberg column default values, "
						   "which pg_lake does not support");
			break;
	}
}


/*
 * LogCatalogUnsupportedFormatVersion logs that a table's Iceberg format
 * version is one pg_lake cannot read, or, when forWrite, cannot write.
 */
void
LogCatalogUnsupportedFormatVersion(int formatVersion, bool forWrite)
{
	LogCatalogLine("a table uses Iceberg format version %d, which pg_lake cannot %s",
				   formatVersion, forWrite ? "write" : "read");
}


/*
 * LogCatalogUnsupportedType logs a schema type pg_lake cannot map, by its
 * Iceberg type name when that is one we expect.
 */
void
LogCatalogUnsupportedType(const char *icebergType)
{
	for (int i = 0; i < lengthof(KnownIcebergTypes); i++)
	{
		size_t		length = strlen(KnownIcebergTypes[i]);

		/* geometry and geography carry their parameters in parentheses */
		if (strncmp(icebergType, KnownIcebergTypes[i], length) == 0 &&
			(icebergType[length] == '\0' || icebergType[length] == '('))
		{
			LogCatalogLine("a table uses the Iceberg type %s, "
						   "which pg_lake does not support",
						   KnownIcebergTypes[i]);
			return;
		}
	}

	LogCatalogLine("a table uses an Iceberg type pg_lake does not support");
}


/*
 * CatalogErrorType returns the error type a failed response names, from
 * the Iceberg error envelope or an OAuth2 error response, when it is one
 * we know, and NULL otherwise.
 */
static const char *
CatalogErrorType(const HttpResult * result)
{
	const char *message = NULL;
	const char *type = NULL;

	GetRestCatalogErrorFields(result->body, &message, &type);

	if (type == NULL)
		return NULL;

	for (int i = 0; i < lengthof(KnownCatalogErrorTypes); i++)
	{
		if (strcmp(type, KnownCatalogErrorTypes[i]) == 0)
			return KnownCatalogErrorTypes[i];
	}

	return NULL;
}


/*
 * NetworkProblem describes the libcurl error a request failed with.  The
 * message libcurl writes alongside names the host, so it is not logged.
 *
 * Returns NULL for a request aborted by an interrupt, since that is the
 * query being cancelled rather than a failure of the catalog.
 */
static const char *
NetworkProblem(int curlCode)
{
	switch (curlCode)
	{
		case CURLE_COULDNT_RESOLVE_PROXY:
		case CURLE_COULDNT_RESOLVE_HOST:
			return "host not found";

		case CURLE_COULDNT_CONNECT:
			return "connection failed";

		case CURLE_OPERATION_TIMEDOUT:
			return "timed out";

		case CURLE_SSL_CONNECT_ERROR:
		case CURLE_PEER_FAILED_VERIFICATION:
		case CURLE_SSL_CERTPROBLEM:
		case CURLE_SSL_CIPHER:
		case CURLE_SSL_CACERT_BADFILE:
			return "TLS error";

		case CURLE_SEND_ERROR:
		case CURLE_RECV_ERROR:
		case CURLE_GOT_NOTHING:
		case CURLE_PARTIAL_FILE:
			return "connection lost";

		case CURLE_ABORTED_BY_CALLBACK:
			return NULL;

		case CURLE_OUT_OF_MEMORY:
			return "out of memory";

		default:
			return "network error";
	}
}
