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
 * Catalog activity log lines.
 *
 * Each line is CATALOG_LOG_PREFIX, an event name, then space-separated
 * key=value fields:
 *
 *     pg_lake_catalog: request_failed catalog=rest_builtin op=load_table status=503 ...
 *
 * so a log pipeline can select the whole family by the prefix, or a single
 * event by the prefix and the event name.  Log filters match the prefix
 * literally, so its spelling and the event names are part of the interface.
 *
 * The lines are meant to be safe to collect where the rest of the server log
 * is not.  Every value is a number, an OID, a SQLSTATE, or a word from a
 * fixed vocabulary.  Never put a table, schema, namespace or catalog name, a
 * URL or path, SQL text, a remote error message, or anything else a user or
 * a remote service controls into one; map such input onto a fixed set of
 * values instead.
 */
#pragma once

#include "pg_lake/rest_catalog/rest_catalog.h"

#define CATALOG_LOG_PREFIX "pg_lake_catalog: "

extern PGDLLEXPORT bool LogCatalogEvents;

extern void InitCatalogLog(void);
extern PGDLLEXPORT void LogCatalogEvent(const char *event, const char *fieldsFormat,...)
			pg_attribute_printf(2, 3);
extern PGDLLEXPORT void LogCatalogEventAtCommit(const char *event, const char *fieldsFormat,...)
			pg_attribute_printf(2, 3);
extern PGDLLEXPORT const char *CatalogLogLabelForRelation(Oid relationId);
extern const char *RestCatalogLogLabel(const RestCatalogOptions * opts);
extern void LogCatalogRequest(const RestCatalogOptions * opts, RestCatalogOp op,
							  const HttpResult * result, int attempts,
							  bool authRefreshed, long elapsedMs);
extern void LogCatalogAuthFailure(const RestCatalogOptions * opts, const char *source,
								  int sqlerrcode);
extern void LogCatalogUnsupportedType(const char *icebergType);
extern PGDLLEXPORT int IcebergFormatVersionFromMetadata(Jsonb *metadata);
