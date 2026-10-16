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
 * Resolves a table in an external catalog once per read, and answers from
 * what this transaction already resolved for as long as it is entitled to
 * that version of the catalog. See external_catalog_cache.c for the rules
 * and for why they are worth having.
 *
 * Each function both asks the catalog and remembers the answer, so a
 * caller cannot read one and record the other. Which answer is held, and
 * what it is held under, follows how the catalog publishes: a REST catalog
 * answers for one table per request, so its answer is held per relation
 * that asked; an object store catalog publishes its tables as one
 * document, so that document is held under the path it was read from.
 *
 * ForgetRestCatalogMetadataLocation drops what a relation resolved, for
 * the one command that makes that relation point at another table.
 */
extern PGDLLEXPORT char *GetRestCatalogMetadataLocationOncePerRead(Oid relationId);
extern PGDLLEXPORT void ForgetRestCatalogMetadataLocation(Oid relationId);
extern PGDLLEXPORT char *GetObjectStoreCatalogDocumentOncePerRead(const char *documentPath);
