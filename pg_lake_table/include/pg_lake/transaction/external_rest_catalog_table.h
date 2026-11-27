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

#include "access/xact.h"
#include "nodes/pg_list.h"

extern PGDLLEXPORT bool IsWritableExternalRestCatalogTable(Oid relationId);
extern void PrepareExternalRestCatalogTableWrite(Oid relationId);
extern void CreateExternalRestCatalogTableInCatalog(Oid relationId, List *tableElements);
extern void RecordExternalRestCatalogTableChanges(Oid relationId, List *metadataOperations);
extern void CommitExternalRestCatalogTableChanges(void);
extern void ResetExternalRestCatalogTableChanges(void);
extern PGDLLEXPORT List *GetPendingExternalTableOperations(Oid relationId);
extern void ErrorIfPrepareWithExternalTableChanges(void);
extern void ExternalRestCatalogTableSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
													SubTransactionId parentSubid, void *arg);
extern PGDLLEXPORT void RecordTableWriteInTx(Oid relationId);
