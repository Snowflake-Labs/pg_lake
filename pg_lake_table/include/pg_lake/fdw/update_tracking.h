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

#include "access/relscan.h"
#include "executor/tuptable.h"
#include "nodes/execnodes.h"
#include "utils/palloc.h"
#include "utils/relcache.h"

/*
 * RelationUpdateTrackingState keeps state across calls to TrackPgLakeUpdate.
 */
typedef struct RelationUpdateTrackingState
{
	/* the table for which we track updates */
	Oid			relationId;

	/* metadata of the temp table in which we track updates */
	Relation	updateRel;

	/* execution state for inserts */
	EState	   *estate;
	ResultRelInfo *updateRelInfo;
	MemoryContext perTupleContext;

	/* the slot for inserting row IDs */
	TupleTableSlot *rowLocationSlot;
} RelationUpdateTrackingState;

bool		BeginRelationUpdateTracking(RelationUpdateTrackingState *state, Oid relationId);
bool		IsFirstUpdateOfTuple(RelationUpdateTrackingState *state, ItemPointer rowLocation);
void		FinishRelationUpdateTracking(RelationUpdateTrackingState *state);
