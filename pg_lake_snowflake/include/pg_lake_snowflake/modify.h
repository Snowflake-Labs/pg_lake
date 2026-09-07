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
 * modify.h
 * Writing to a Snowflake table: INSERT, COPY, UPDATE, DELETE and TRUNCATE.
 */
#pragma once

#include "postgres.h"

#include "commands/explain.h"
#include "foreign/fdwapi.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "nodes/plannodes.h"
#include "utils/relcache.h"

extern int	SnowflakeIsForeignRelUpdatable(Relation relation);

/* INSERT, including the row-at-a-time and batched paths COPY uses */
extern List *SnowflakePlanForeignModify(PlannerInfo *root, ModifyTable *plan,
										Index resultRelation, int subplanIndex);
extern void SnowflakeBeginForeignModify(ModifyTableState *modifyTableState,
										ResultRelInfo *resultRelInfo, List *fdwPrivate,
										int subplanIndex, int executorFlags);
extern TupleTableSlot *SnowflakeExecForeignInsert(EState *executorState,
												  ResultRelInfo *resultRelInfo,
												  TupleTableSlot *slot,
												  TupleTableSlot *planSlot);
extern TupleTableSlot **SnowflakeExecForeignBatchInsert(EState *executorState,
														ResultRelInfo *resultRelInfo,
														TupleTableSlot **slots,
														TupleTableSlot **planSlots,
														int *slotCount);
extern int	SnowflakeGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo);
extern void SnowflakeEndForeignModify(EState *executorState,
									  ResultRelInfo *resultRelInfo);
extern void SnowflakeBeginForeignInsert(ModifyTableState *modifyTableState,
										ResultRelInfo *resultRelInfo);
extern void SnowflakeEndForeignInsert(EState *executorState,
									  ResultRelInfo *resultRelInfo);

/*
 * UPDATE and DELETE, which only ever run as one statement that Snowflake
 * evaluates in full. The row-at-a-time callbacks exist to fail loudly rather
 * than to be reached.
 */
extern bool SnowflakePlanDirectModify(PlannerInfo *root, ModifyTable *plan,
									  Index resultRelation, int subplanIndex);
extern void SnowflakeBeginDirectModify(ForeignScanState *scanState,
									   int executorFlags);
extern TupleTableSlot *SnowflakeIterateDirectModify(ForeignScanState *scanState);
extern void SnowflakeEndDirectModify(ForeignScanState *scanState);
extern void SnowflakeExplainDirectModify(ForeignScanState *scanState,
										 ExplainState *es);
extern TupleTableSlot *SnowflakeExecForeignUpdate(EState *executorState,
												  ResultRelInfo *resultRelInfo,
												  TupleTableSlot *slot,
												  TupleTableSlot *planSlot);
extern TupleTableSlot *SnowflakeExecForeignDelete(EState *executorState,
												  ResultRelInfo *resultRelInfo,
												  TupleTableSlot *slot,
												  TupleTableSlot *planSlot);

extern void SnowflakeExecForeignTruncate(List *relations, DropBehavior behavior,
										 bool restartSequences);
