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

#include "nodes/pg_list.h"
#include "optimizer/paths.h"
#include "pg_lake/planner/extensible_nodes.h"


/*
* PlannerRelationRestriction is a custom node that is used to keep
* track of the restrictions that Postgres planner knows about a
* relation during query planning.
*/
typedef struct PlannerRelationRestriction
{
	PgLakeNode	type;

	RangeTblEntry *rte;
	List	   *baseRestrictionList;
} PlannerRelationRestriction;

/*
* Map entry for the restriction map.
*/
typedef struct RestrictionMapEntry
{
	int			uniqueRelationIdentifier;

	PlannerRelationRestriction *relationRestriction;
} RestrictionMapEntry;

/*
* Map of PlannerRelationRestrictions.
*/
typedef struct PlannerRelationRestrictionContext
{
	/* mapping uniqueRelationIdentifier to PlannerRelationRestriction */
	HTAB	   *relationRestrictionMap;
} PlannerRelationRestrictionContext;

extern set_rel_pathlist_hook_type PrevRelPathlistHook;

void		AssignUniqueRelationIdentifier(RangeTblEntry *rte);
void		SetUniqueRelationIdentifier(RangeTblEntry *rte, int uniqueRelationIdentifier);
int			GetUniqueRelationIdentifier(RangeTblEntry *rte);

void		PgLakeRecordPlannerRestrictions(PlannerInfo *root, RelOptInfo *relOptInfo,
											Index restrictionIndex, RangeTblEntry *rte);
void		PopRestrictionContext(void);
List	   *GetCurrentRelationRestrictions(void);
PlannerRelationRestrictionContext *CreateAndPushRestrictionContext(void);

void		ReplaceParamsInRestrictInfo(List *baseRestrictionList, ParamListInfo paramListInfo);
Node	   *ReplaceExternalParamsWithPgDuckConsts(Node *inputNode, ParamListInfo boundParams);
void		PrettyPrintBaseRestrictInfo(int logLevel, RangeTblEntry *rte, List *baseRestrictInfoList);
