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

#include "nodes/extensible.h"


#define PGL_NODE_TAG_START	2400

/*
 * Extensible Node Tags
 *
 * These have to be distinct from the ideas used in postgres' nodes.h.
 * We also know that Citus uses the range > 1200, so we start from 2400.
 *
 * This list should be equal to PgLakeNodeTagNames from extensible_nodes.c
 */
typedef enum PgLakeNodeTag
{
	T_PlannerRelationRestriction = PGL_NODE_TAG_START,
} PgLakeNodeTag;


typedef struct PgLakeNode
{
	ExtensibleNode extensible;
	PgLakeNodeTag pgl_tag;
} PgLakeNode;


/* filled in extensible_nodes.c */
extern const char **PgLakeNodeTagNames;


/*
* Get the tag of a PgLakeNode. Do not use directly,
* use PgLakeNodeTag macro.
*/
static inline int
PgLakeNodeTagI(Node *node)
{
	if (!IsA(node, ExtensibleNode))
	{
		return nodeTag(node);
	}

	return ((PgLakeNode *) (node))->pgl_tag;
}


/*
* Set the tag of a PgLakeNode.
*/
static inline Node *
PgLakeSetTag(Node *node, int tag)
{
	PgLakeNode *pgl_node = (PgLakeNode *) node;

	pgl_node->pgl_tag = tag;
	return node;
}


/*
 * PgLake variant of newNode(), don't use directly. Instead, use
 * PgLakeMakeNode() macro.
 */
static inline PgLakeNode *
PgLakeNewNode(size_t size, PgLakeNodeTag tag)
{
	PgLakeNode *result;

	Assert(size >= sizeof(PgLakeNode));
	result = (PgLakeNode *) palloc0(size);
	result->extensible.type = T_ExtensibleNode;
	result->extensible.extnodename = PgLakeNodeTagNames[tag - PGL_NODE_TAG_START];
	result->pgl_tag = (int) (tag);

	return result;
}


#define PgLakeNodeTag(nodeptr)		PgLakeNodeTagI((Node*) nodeptr)
#define PgLakeIsA(nodeptr,_type_)	(PgLakeNodeTag(nodeptr) == T_##_type_)
#define PgLakeMakeNode(_type_) ((_type_ *) PgLakeNewNode(sizeof(_type_),T_##_type_))

extern void RegisterPgLakeCustomNodes(void);
