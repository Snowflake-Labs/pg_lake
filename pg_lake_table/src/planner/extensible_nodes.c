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

#include "postgres.h"

#include "pg_lake/planner/extensible_nodes.h"
#include "pg_lake/planner/restriction_collector.h"

/* useful macros for defining function arguments */
#define COPYFUNC_ARGS struct ExtensibleNode *target_node, const struct \
	ExtensibleNode *source_node
#define READFUNC_ARGS struct ExtensibleNode *node
#define OUTFUNC_ARGS StringInfo str, const struct ExtensibleNode *raw_node


/* forward declarations */
static void ReadUnsupportedPgLakeNode(READFUNC_ARGS);
static bool EqualUnsupportedPgLakeNode(const struct ExtensibleNode *a,
									   const struct ExtensibleNode *b);

/* supported node functions */
static void CopyNodePlannerRelationRestriction(COPYFUNC_ARGS);
static void OutPlannerRelationRestriction(OUTFUNC_ARGS);


#define DEFINE_NODE_METHODS(type) \
	{ \
		#type, \
		sizeof(type), \
		CopyNode##type, \
		EqualUnsupportedPgLakeNode, \
		Out##type, \
		ReadUnsupportedPgLakeNode \
	}

/*
* Should be in sync with PgLakeNodeTag enum
* in extensible_nodes.h
*/
static const char *PgLakeNodeTagNamesArray[] = {
	"PlannerRelationRestriction"
};

/*
* Node methods for custom nodes.
*/
static const ExtensibleNodeMethods nodeMethods[] =
{
	DEFINE_NODE_METHODS(PlannerRelationRestriction)
};


#define DECLARE_FROM_AND_NEW_NODE(nodeTypeName) \
	nodeTypeName *newnode = \
		(nodeTypeName *) PgLakeSetTag((Node *) target_node, T_ ## nodeTypeName); \
	nodeTypeName *from = (nodeTypeName *) source_node

/* Copy a simple scalar field (int, float, bool, enum, etc) */
#define COPY_SCALAR_FIELD(fldname) \
	(newnode->fldname = from->fldname)

/* Copy a field that is a pointer to some kind of Node or Node tree */
#define COPY_NODE_FIELD(fldname) \
	(newnode->fldname = copyObject(from->fldname))

/*
* Exported in extensible_nodes.h
*/
const char **PgLakeNodeTagNames = PgLakeNodeTagNamesArray;



/*
* We currently do not support reading custom nodes.
*/
static void
ReadUnsupportedPgLakeNode(READFUNC_ARGS)
{
	ereport(ERROR, (errmsg("read not implemented")));
}

/*
 * PlannerRelationRestriction is transient planner bookkeeping. pg_lake does
 * not support reconstructing it from text, but PostgreSQL still calls nodeOut
 * when debug_print_plan is enabled or a node is printed in the debugger. Emit
 * the private fields to make that diagnostic output useful.
 */
static void
OutPlannerRelationRestriction(OUTFUNC_ARGS)
{
	const		PlannerRelationRestriction *node =
		(const PlannerRelationRestriction *) raw_node;

	appendStringInfoString(str, " :rte ");
	outNode(str, node->rte);
	appendStringInfoString(str, " :baseRestrictionList ");
	outNode(str, node->baseRestrictionList);
}


/*
* We currently do not need/support equality checks
* for custom nodes.
*/
static bool
EqualUnsupportedPgLakeNode(const struct ExtensibleNode *a,
						   const struct ExtensibleNode *b)
{
	ereport(ERROR, (errmsg("equal not implemented")));
}



/*
* Copy function for PlannerRelationRestriction node.
*/
static void
CopyNodePlannerRelationRestriction(COPYFUNC_ARGS)
{
	DECLARE_FROM_AND_NEW_NODE(PlannerRelationRestriction);

	COPY_NODE_FIELD(rte);
	COPY_NODE_FIELD(baseRestrictionList);
}


/*
* As per the extensible node API, we need to register the custom nodes
* to Postgres.
*/
void
RegisterPgLakeCustomNodes(void)
{
	StaticAssertExpr(lengthof(nodeMethods) == lengthof(PgLakeNodeTagNamesArray),
					 "number of node methods and names do not match");

	for (int off = 0; off < lengthof(nodeMethods); off++)
	{
		RegisterExtensibleNodeMethods(&nodeMethods[off]);
	}
}
