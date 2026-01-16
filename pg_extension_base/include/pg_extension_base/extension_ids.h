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

#pragma once

typedef void (*ClearFunction) (void *clearContext);

/*
 * CachedExtensionIds can be used to cache extension OIDs and other metadata.
 *
 * Extensions should declare a CachedExtensionIds in a public header and C file:
 *
 *     CachedExtensionIds *MyExtension;
 *
 * Additional OIDs can be cached in a private struct with a cleanup function:
 *
 *     typedef struct MyExtensionIds
 *     {
 *         Oid func1Id;
 *         Oid func2Id;
 *     } MyExtensionIds;
 *
 *     static MyExtensionIds CachedIds;
 *
 *     static void ClearMyExtension(MyExtensionIds *cachedIds)
 *     {
 *         memset(cachedIds, '\0', sizeof(MyExtensionIds));
 *     }
 *
 * During initialization, the MyExtension field should be initialized as follows:
 *
 *     MyExtension = CreateExtensionIdsCache("my_extension", ClearFunc, &CachedIds);
 *
 * You can then efficiently check whether the extension exists through:
 *
 *     if (IsExtensionCreated(MyExtension))
 *
 * and store OIDs in MyExtensionIds that will be reset on CREATE/DROP EXTENSION.
 *
 * See pg_lake_engine.h and pg_lake_engine.c for an example.
 */
struct CachedExtensionIdsData;
typedef struct CachedExtensionIdsData CachedExtensionIds;

extern PGDLLEXPORT CachedExtensionIds * CreateExtensionIdsCache(char *extensionName,
																ClearFunction clearFn,
																void *clearContext);
extern PGDLLEXPORT bool IsExtensionCreated(CachedExtensionIds * extension);
extern PGDLLEXPORT Oid ExtensionId(CachedExtensionIds * extension);
extern PGDLLEXPORT Oid ExtensionSchemaId(CachedExtensionIds * extension);
extern PGDLLEXPORT Oid ExtensionOwnerId(CachedExtensionIds * extension);
extern PGDLLEXPORT void EnsureExtensionExists(CachedExtensionIds * extension);
