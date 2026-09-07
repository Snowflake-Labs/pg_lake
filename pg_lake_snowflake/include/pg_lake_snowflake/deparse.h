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
 * deparse.h
 * Turning a Postgres plan fragment into a Snowflake SELECT statement.
 */
#pragma once

#include "postgres.h"

#include "nodes/pathnodes.h"
#include "nodes/pg_list.h"
#include "optimizer/optimizer.h"
#include "pg_lake_snowflake/options.h"

/*
 * SnowflakeRelationInfo is the planner-private state for one relation we
 * consider scanning remotely. The same struct describes a base relation, a
 * grouped relation on top of it, and the final relation that adds LIMIT, so
 * that each stage can be built by copying the stage below and adding to it.
 */
typedef struct SnowflakeRelationInfo
{
	/* whether the relation can be evaluated remotely at all */
	bool		pushdownSafe;

	/* the base relation whose foreign table supplies the columns */
	RelOptInfo *scanRel;
	Oid			relationId;

	SnowflakeConnection *connection;
	SnowflakeTable *table;

	/* restriction clauses split by where they can be evaluated */
	List	   *remoteConds;
	List	   *localConds;

	/*
	 * columns needed by the query, offset by
	 * FirstLowInvalidHeapAttributeNumber
	 */
	Bitmapset  *attrsUsed;

	/* estimates for the relation as scanned remotely */
	double		rows;
	int			width;
	Cost		startupCost;
	Cost		totalCost;

	/*
	 * The target list of the remote statement, for a relation that does not
	 * scan a Postgres relation directly. It is NIL for a plain base relation
	 * scan, where the columns come from attrsUsed instead.
	 */
	List	   *scanTlist;

	/* set for a relation that groups and aggregates remotely */
	bool		isGrouped;
	List	   *groupExprs;
	List	   *havingQuals;
	List	   *havingLocalQuals;

	/* set for a relation that limits remotely */
	bool		hasLimit;
	int64		limitCount;
	int64		limitOffset;
}			SnowflakeRelationInfo;

/*
 * SnowflakeDeparsedQuery is a statement with holes: fragment i is followed by
 * the literal for parameter i, so that a plan can be re-executed with new
 * parameter values without re-deparsing, and without a substitution pass that
 * could confuse a marker with the contents of a string literal.
 */
typedef struct SnowflakeDeparsedQuery
{
	List	   *sqlFragments;	/* list of String, one more than paramExprs */
	List	   *paramExprs;		/* list of Expr */
	List	   *retrievedAttrs; /* list of int, one per result column */
}			SnowflakeDeparsedQuery;

extern void SnowflakeDeparseSelect(PlannerInfo *root,
								   SnowflakeRelationInfo * relationInfo,
								   SnowflakeDeparsedQuery * query);

extern bool SnowflakeIsShippableExpression(Expr *expr,
										   SnowflakeRelationInfo * relationInfo);

/* fragments joined with $1, $2, ... in the holes, for EXPLAIN and logging */
extern char *SnowflakeDeparsedQueryText(List *sqlFragments);

/* fragments joined with the given literals (list of char *) in the holes */
extern char *SnowflakeBuildStatement(List *sqlFragments, List *paramLiterals);
