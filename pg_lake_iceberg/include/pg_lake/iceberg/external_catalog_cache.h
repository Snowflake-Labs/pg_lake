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

#pragma once

#include "postgres.h"

/*
 * Holds what this transaction resolved from an external catalog, for as
 * long as it is entitled to that version of the catalog. See
 * external_catalog_cache.c for the rules.
 *
 * A catalog that answers for one table per request is held per relation
 * that asked. A catalog that publishes its tables as one document is held
 * as that document, under the path the document was read from.
 *
 * ForgetCachedExternalMetadataLocation drops what a relation resolved,
 * for the one command that makes it point at another table.
 */
extern PGDLLEXPORT char *GetCachedExternalMetadataLocation(Oid relationId);
extern PGDLLEXPORT void CacheExternalMetadataLocation(Oid relationId,
													  const char *metadataLocation);
extern PGDLLEXPORT void ForgetCachedExternalMetadataLocation(Oid relationId);
extern PGDLLEXPORT char *GetCachedExternalCatalogDocument(const char *documentPath);
extern PGDLLEXPORT void CacheExternalCatalogDocument(const char *documentPath, const char *documentContent);
