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

/*-------------------------------------------------------------------------
 *
 * pg_lake_table.h
 *		  Foreign-data wrapper for lake tables
 *
 * Portions Copyright (c) 2012-2023, PostgreSQL Global Development Group
 *
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_LAKE_TABLE_H
#define PG_LAKE_TABLE_H

#include "foreign/foreign.h"
#include "lib/stringinfo.h"
#include "libpq-fe.h"
#include "nodes/execnodes.h"
#include "nodes/pathnodes.h"
#include "utils/relcache.h"

/*
 * We replace this placeholder at post hook, after table is created, with
 * the actual iceberg location. We need the relation id to add it as
 * suffix to the location. It is available only after table is created
 * at post hook.
 */
#define DEFAULT_ICEBERG_LOCATION_PLACEHOLDER "<default-location-placeholder>"

/*
 * FDW-specific planner information kept in RelOptInfo.fdw_private for a
 * pg_lake foreign table.  For a baserel, this struct is created by
 * postgresGetForeignRelSize, although some fields are not filled till later.
 * postgresGetForeignJoinPaths creates it for a joinrel, and
 * postgresGetForeignUpperPaths creates it for an upperrel.
 */
typedef struct PgLakeRelationInfo
{
	/*
	 * True means that the relation can be pushed down. Always true for simple
	 * foreign scan.
	 */
	bool		pushdown_safe;

	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 * All entries in these lists should have RestrictInfo wrappers; that
	 * improves efficiency of selectivity and cost estimation.
	 */
	List	   *remote_conds;
	List	   *local_conds;

	/* Actual remote restriction clauses for scan (sans RestrictInfos) */
	List	   *final_remote_exprs;

	/* Bitmap of attr numbers we need to fetch from the remote server. */
	Bitmapset  *attrs_used;

	/* True means that the query_pathkeys is safe to push down */
	bool		qp_is_pushdown_safe;

	/* Cost and selectivity of local_conds. */
	QualCost	local_conds_cost;
	Selectivity local_conds_sel;

	/* Selectivity of join conditions */
	Selectivity joinclause_sel;

	/* Estimated size and cost for a scan, join, or grouping/aggregation. */
	double		rows;
	int			width;
	int			disabled_nodes;
	Cost		startup_cost;
	Cost		total_cost;

	/*
	 * Estimated number of rows fetched from the foreign server, and costs
	 * excluding costs for transferring those rows from the foreign server.
	 * These are only used by estimate_path_cost_size().
	 */
	double		retrieved_rows;
	Cost		rel_startup_cost;
	Cost		rel_total_cost;

	/* Options extracted from catalogs. */
	bool		use_remote_estimate;
	Cost		fdw_startup_cost;
	Cost		fdw_tuple_cost;
	List	   *shippable_extensions;	/* OIDs of shippable extensions */

	/* Cached catalog information. */
	ForeignTable *table;
	ForeignServer *server;

	/*
	 * Name of the relation, for use while EXPLAINing ForeignScan.  It is used
	 * for join and upper relations but is set for all relations.  For a base
	 * relation, this is really just the RT index as a string; we convert that
	 * while producing EXPLAIN output.  For join and upper relations, the name
	 * indicates which base foreign tables are included and the join type or
	 * aggregation type used.
	 */
	char	   *relation_name;

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType	jointype;
	/* joinclauses contains only JOIN/ON conditions for an outer join */
	List	   *joinclauses;	/* List of RestrictInfo */

	/* Upper relation information */
	UpperRelationKind stage;

	/* Grouping information */
	List	   *grouped_tlist;

	/* Subquery information */
	bool		make_outerrel_subquery; /* do we deparse outerrel as a
										 * subquery? */
	bool		make_innerrel_subquery; /* do we deparse innerrel as a
										 * subquery? */
	Relids		lower_subquery_rels;	/* all relids appearing in lower
										 * subqueries */
	Relids		hidden_subquery_rels;	/* relids, which can't be referred to
										 * from upper relations, used
										 * internally for equivalence member
										 * search */

	/*
	 * Index of the relation.  It is used to create an alias to a subquery
	 * representing the relation.
	 */
	int			relation_index;
} PgLakeRelationInfo;

/*
 * Method used by ANALYZE to sample remote rows.
 */
typedef enum PgLakeSamplingMethod
{
	ANALYZE_SAMPLE_OFF,			/* no remote sampling */
	ANALYZE_SAMPLE_AUTO,		/* choose by server version */
	ANALYZE_SAMPLE_RANDOM,		/* remote random() */
	ANALYZE_SAMPLE_SYSTEM,		/* TABLESAMPLE system */
	ANALYZE_SAMPLE_BERNOULLI	/* TABLESAMPLE bernoulli */
} PgLakeSamplingMethod;


/* hook */
typedef void (*PgLakeModifyValidityCheckHookType) (Oid relationId);
extern PGDLLEXPORT PgLakeModifyValidityCheckHookType PgLakeModifyValidityCheckHook;

/* in postgres_fdw.c */
extern int	set_transmission_modes(void);
extern void reset_transmission_modes(int nestlevel);
extern void process_pending_request(AsyncRequest *areq);

/* in option.c */
extern int	ExtractConnectionOptions(List *defelems,
									 const char **keywords,
									 const char **values);

/* in deparse.c */
extern void classifyConditions(PlannerInfo *root,
							   RelOptInfo *baserel,
							   List *input_conds,
							   List **remote_conds,
							   List **local_conds);
extern bool is_foreign_expr(PlannerInfo *root,
							RelOptInfo *baserel,
							Expr *expr);
extern bool is_foreign_param(PlannerInfo *root,
							 RelOptInfo *baserel,
							 Expr *expr);
extern bool is_foreign_pathkey(PlannerInfo *root,
							   RelOptInfo *baserel,
							   PathKey *pathkey);
extern void deparseAnalyzeInfoSql(StringInfo buf, Relation rel);
extern void deparseAnalyzeSql(StringInfo buf, Relation rel,
							  PgLakeSamplingMethod sample_method,
							  double sample_frac,
							  List **retrieved_attrs);
extern void deparseStringLiteral(StringInfo buf, const char *val);
extern EquivalenceMember *find_em_for_rel(PlannerInfo *root,
										  EquivalenceClass *ec,
										  RelOptInfo *rel);
extern EquivalenceMember *find_em_for_rel_target(PlannerInfo *root,
												 EquivalenceClass *ec,
												 RelOptInfo *rel);
extern List *build_tlist_to_deparse(RelOptInfo *foreignrel);
extern void deparseSelectStmtForRel(StringInfo buf, PlannerInfo *root,
									RelOptInfo *rel, List *tlist,
									List *remote_conds, List *pathkeys,
									bool has_final_sort, bool has_limit,
									bool is_subquery,
									List **retrieved_attrs, List **params_list);
extern const char *get_jointype_name(JoinType jointype);


#endif							/* PG_LAKE_TABLE_H */
