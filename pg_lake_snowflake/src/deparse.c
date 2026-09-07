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
 * deparse.c
 *
 * Turning a plan fragment into a Snowflake SELECT statement, and deciding what
 * may be turned into one at all.
 *
 * What is shippable is a deliberately short allowlist rather than a search for
 * reasons to refuse. Postgres and Snowflake agree on the meaning of far less
 * than their syntax suggests: integer division truncates in one and produces a
 * decimal in the other, an ordering comparison over text follows a Postgres
 * collation that Snowflake does not implement, and AVG over an exact numeric
 * loses digits to Snowflake's scale rules. Each of those would return a wrong
 * answer rather than fail, so they stay local, and only the constructs whose
 * meaning is identical on both sides travel to the warehouse.
 *
 * A parameter cannot be written into the statement at plan time, so the result
 * of deparsing is a list of SQL fragments with the parameters belonging between
 * them. Splicing rather than substituting keeps a value that happens to look
 * like a placeholder from being mistaken for one.
 */

#include "postgres.h"

#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/optimizer.h"
#include "parser/parsetree.h"
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"

#include "pg_lake_snowflake/deparse.h"
#include "pg_lake_snowflake/options.h"
#include "pg_lake_snowflake/pg_lake_snowflake.h"
#include "pg_lake_snowflake/type_map.h"

/* DeparseContext carries the statement being built and where parameters go */
typedef struct DeparseContext
{
	PlannerInfo *root;
	SnowflakeRelationInfo *relationInfo;
	StringInfo	buffer;
	List	  **sqlFragments;
	List	  **paramExprs;
}			DeparseContext;

static void DeparseExpression(Node *node, DeparseContext * context);
static void DeparseVar(Var *var, DeparseContext * context);
static void DeparseConst(Const *constant, DeparseContext * context);
static void DeparseParam(Param *param, DeparseContext * context);
static void DeparseOpExpr(OpExpr *opExpr, DeparseContext * context);
static void DeparseScalarArrayOpExpr(ScalarArrayOpExpr *arrayOpExpr,
									 DeparseContext * context);
static void DeparseBoolExpr(BoolExpr *boolExpr, DeparseContext * context);
static void DeparseNullTest(NullTest *nullTest, DeparseContext * context);
static void DeparseAggref(Aggref *aggref, DeparseContext * context);
static void DeparseConditions(List *conditions, const char *keyword,
							  DeparseContext * context);
static void AppendParameter(Expr *paramExpr, DeparseContext * context);
static void DeparseTargetList(DeparseContext * context, List **retrievedAttrs);

static bool IsShippableNode(Node *node, SnowflakeRelationInfo * relationInfo,
							PlannerInfo *root);
static bool IsShippableOperator(Oid operatorId, Oid inputCollation,
								Oid leftType, Oid rightType,
								const char **snowflakeOperator);
static bool IsCollationSafe(Oid collation);
static bool IsTextLikeType(Oid typeId);
static bool IsNumberLikeType(Oid typeId);
static const char *SnowflakeAggregateName(Oid aggregateFunctionId);


/*
 * SnowflakeDeparseSelect builds the statement for one relation: the columns a
 * base relation scan needs, or the grouped target list of an aggregated
 * relation, plus the conditions, grouping and limit that were found shippable.
 */
void
SnowflakeDeparseSelect(PlannerInfo *root, SnowflakeRelationInfo * relationInfo,
					   SnowflakeDeparsedQuery * query)
{
	DeparseContext context = {
		.root = root,
		.relationInfo = relationInfo,
		.buffer = makeStringInfo(),
		.sqlFragments = &query->sqlFragments,
		.paramExprs = &query->paramExprs
	};

	query->sqlFragments = NIL;
	query->paramExprs = NIL;
	query->retrievedAttrs = NIL;

	appendStringInfoString(context.buffer, "SELECT ");

	if (relationInfo->scanTlist != NIL)
	{
		ListCell   *targetCell = NULL;
		int			columnNumber = 0;

		foreach(targetCell, relationInfo->scanTlist)
		{
			TargetEntry *targetEntry = (TargetEntry *) lfirst(targetCell);

			if (columnNumber > 0)
				appendStringInfoString(context.buffer, ", ");

			DeparseExpression((Node *) targetEntry->expr, &context);

			query->retrievedAttrs = lappend_int(query->retrievedAttrs, ++columnNumber);
		}
	}
	else
	{
		DeparseTargetList(&context, &query->retrievedAttrs);
	}

	appendStringInfo(context.buffer, " FROM %s",
					 SnowflakeQualifiedTableName(relationInfo->table));

	DeparseConditions(relationInfo->remoteConds, "WHERE", &context);

	if (relationInfo->isGrouped)
	{
		int			groupColumnCount = list_length(relationInfo->groupExprs);

		if (groupColumnCount > 0)
		{
			appendStringInfoString(context.buffer, " GROUP BY ");

			/*
			 * The target list starts with the grouping expressions in order,
			 * so their ordinals identify them without deparsing the same
			 * expression a second time.
			 */
			for (int columnNumber = 1; columnNumber <= groupColumnCount; columnNumber++)
			{
				if (columnNumber > 1)
					appendStringInfoString(context.buffer, ", ");

				appendStringInfo(context.buffer, "%d", columnNumber);
			}
		}

		DeparseConditions(relationInfo->havingQuals, "HAVING", &context);
	}

	if (relationInfo->hasLimit)
	{
		appendStringInfo(context.buffer, " LIMIT " INT64_FORMAT, relationInfo->limitCount);

		if (relationInfo->limitOffset > 0)
		{
			appendStringInfo(context.buffer, " OFFSET " INT64_FORMAT,
							 relationInfo->limitOffset);
		}
	}

	/* the trailing fragment closes the statement */
	query->sqlFragments = lappend(query->sqlFragments,
								  makeString(pstrdup(context.buffer->data)));
}


/*
 * DeparseTargetList writes the columns a base relation scan needs, and records
 * which attribute each result column belongs to.
 *
 * A query that needs no column at all still needs one result row per remote row,
 * which is what the constant is for.
 */
static void
DeparseTargetList(DeparseContext * context, List **retrievedAttrs)
{
	SnowflakeRelationInfo *relationInfo = context->relationInfo;
	Relation	relation = RelationIdGetRelation(relationInfo->relationId);
	TupleDesc	tupleDescriptor = RelationGetDescr(relation);
	bool		wholeRowNeeded = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
											   relationInfo->attrsUsed);
	int			columnCount = 0;

	for (AttrNumber attributeNumber = 1;
		 attributeNumber <= tupleDescriptor->natts;
		 attributeNumber++)
	{
		Form_pg_attribute attribute = TupleDescAttr(tupleDescriptor,
													attributeNumber - 1);

		if (attribute->attisdropped)
			continue;

		if (!wholeRowNeeded &&
			!bms_is_member(attributeNumber - FirstLowInvalidHeapAttributeNumber,
						   relationInfo->attrsUsed))
			continue;

		if (columnCount > 0)
			appendStringInfoString(context->buffer, ", ");

		appendStringInfoString(context->buffer,
							   SnowflakeColumnName(relationInfo->relationId,
												   attributeNumber));

		*retrievedAttrs = lappend_int(*retrievedAttrs, attributeNumber);
		columnCount++;
	}

	RelationClose(relation);

	if (columnCount == 0)
		appendStringInfoChar(context->buffer, '1');
}


/*
 * DeparseConditions writes a list of conditions joined by AND, prefixed with
 * the given keyword, or nothing when the list is empty.
 */
static void
DeparseConditions(List *conditions, const char *keyword, DeparseContext * context)
{
	ListCell   *conditionCell = NULL;
	bool		isFirst = true;

	foreach(conditionCell, conditions)
	{
		Node	   *condition = (Node *) lfirst(conditionCell);

		if (IsA(condition, RestrictInfo))
			condition = (Node *) ((RestrictInfo *) condition)->clause;

		if (isFirst)
		{
			appendStringInfo(context->buffer, " %s ", keyword);
			isFirst = false;
		}
		else
		{
			appendStringInfoString(context->buffer, " AND ");
		}

		appendStringInfoChar(context->buffer, '(');
		DeparseExpression(condition, context);
		appendStringInfoChar(context->buffer, ')');
	}
}


/*
 * AppendParameter closes the fragment before a parameter and records the
 * expression whose value belongs in the hole.
 */
static void
AppendParameter(Expr *paramExpr, DeparseContext * context)
{
	*context->sqlFragments = lappend(*context->sqlFragments,
									 makeString(pstrdup(context->buffer->data)));
	*context->paramExprs = lappend(*context->paramExprs, paramExpr);

	resetStringInfo(context->buffer);
}


/*
 * DeparseExpression writes one expression. Everything it accepts was accepted
 * by IsShippableNode first, so an unexpected node here is a coding error rather
 * than a query the user wrote.
 */
static void
DeparseExpression(Node *node, DeparseContext * context)
{
	switch (nodeTag(node))
	{
		case T_Var:
			DeparseVar((Var *) node, context);
			break;
		case T_Const:
			DeparseConst((Const *) node, context);
			break;
		case T_Param:
			DeparseParam((Param *) node, context);
			break;
		case T_OpExpr:
			DeparseOpExpr((OpExpr *) node, context);
			break;
		case T_ScalarArrayOpExpr:
			DeparseScalarArrayOpExpr((ScalarArrayOpExpr *) node, context);
			break;
		case T_BoolExpr:
			DeparseBoolExpr((BoolExpr *) node, context);
			break;
		case T_NullTest:
			DeparseNullTest((NullTest *) node, context);
			break;
		case T_RelabelType:
			DeparseExpression((Node *) ((RelabelType *) node)->arg, context);
			break;
		case T_Aggref:
			DeparseAggref((Aggref *) node, context);
			break;
		default:
			elog(ERROR, "cannot deparse node type %d for Snowflake",
				 (int) nodeTag(node));
	}
}


/*
 * DeparseVar writes the Snowflake name of the column a Var refers to.
 */
static void
DeparseVar(Var *var, DeparseContext * context)
{
	RangeTblEntry *rangeTableEntry = planner_rt_fetch(var->varno, context->root);

	appendStringInfoString(context->buffer,
						   SnowflakeColumnName(rangeTableEntry->relid, var->varattno));
}


/*
 * DeparseConst writes a constant as a Snowflake literal.
 */
static void
DeparseConst(Const *constant, DeparseContext * context)
{
	if (constant->constisnull)
	{
		appendStringInfoString(context->buffer, "NULL");
		return;
	}

	appendStringInfoString(context->buffer,
						   SnowflakeFormatLiteral(constant->constvalue,
												  constant->consttype));
}


/*
 * DeparseParam leaves a hole for a value that is only known when the plan runs.
 */
static void
DeparseParam(Param *param, DeparseContext * context)
{
	AppendParameter((Expr *) param, context);
}


/*
 * DeparseOpExpr writes a binary or unary operator expression.
 */
static void
DeparseOpExpr(OpExpr *opExpr, DeparseContext * context)
{
	Node	   *leftArgument = linitial(opExpr->args);
	Node	   *rightArgument = list_length(opExpr->args) > 1 ?
		lsecond(opExpr->args) : NULL;
	Oid			leftType = exprType(leftArgument);
	Oid			rightType = rightArgument != NULL ? exprType(rightArgument) : InvalidOid;
	const char *snowflakeOperator = NULL;

	if (!IsShippableOperator(opExpr->opno, opExpr->inputcollid, leftType, rightType,
							 &snowflakeOperator))
		elog(ERROR, "operator %u cannot be deparsed for Snowflake", opExpr->opno);

	appendStringInfoChar(context->buffer, '(');

	if (rightArgument == NULL)
	{
		/* a unary operator, which is only ever the negation of a number */
		appendStringInfo(context->buffer, "%s ", snowflakeOperator);
		DeparseExpression(leftArgument, context);
	}
	else
	{
		DeparseExpression(leftArgument, context);
		appendStringInfo(context->buffer, " %s ", snowflakeOperator);
		DeparseExpression(rightArgument, context);
	}

	appendStringInfoChar(context->buffer, ')');
}


/*
 * DeparseScalarArrayOpExpr writes an equality against a list of constants as an
 * IN or a NOT IN.
 */
static void
DeparseScalarArrayOpExpr(ScalarArrayOpExpr *arrayOpExpr, DeparseContext * context)
{
	Node	   *scalarArgument = linitial(arrayOpExpr->args);
	Node	   *arrayArgument = lsecond(arrayOpExpr->args);
	Const	   *arrayConstant = castNode(Const, arrayArgument);
	Oid			elementType = get_element_type(arrayConstant->consttype);
	int16		elementLength = 0;
	bool		elementByValue = false;
	char		elementAlignment = 0;
	Datum	   *elementValues = NULL;
	bool	   *elementNulls = NULL;
	int			elementCount = 0;

	get_typlenbyvalalign(elementType, &elementLength, &elementByValue,
						 &elementAlignment);
	deconstruct_array(DatumGetArrayTypeP(arrayConstant->constvalue), elementType,
					  elementLength, elementByValue, elementAlignment,
					  &elementValues, &elementNulls, &elementCount);

	appendStringInfoChar(context->buffer, '(');
	DeparseExpression(scalarArgument, context);
	appendStringInfo(context->buffer, " %s (",
					 arrayOpExpr->useOr ? "IN" : "NOT IN");

	bool		isFirst = true;

	for (int elementIndex = 0; elementIndex < elementCount; elementIndex++)
	{
		if (!isFirst)
			appendStringInfoString(context->buffer, ", ");

		isFirst = false;

		if (elementNulls[elementIndex])
			appendStringInfoString(context->buffer, "NULL");
		else
		{
			appendStringInfoString(context->buffer,
								   SnowflakeFormatLiteral(elementValues[elementIndex],
														  elementType));
		}
	}

	/*
	 * An empty array makes the condition false for IN and true for NOT IN,
	 * and an empty list is a syntax error, so the extreme is written out.
	 */
	if (elementCount == 0)
		appendStringInfoString(context->buffer, "NULL");

	appendStringInfoString(context->buffer, "))");
}


/*
 * DeparseBoolExpr writes AND, OR and NOT.
 */
static void
DeparseBoolExpr(BoolExpr *boolExpr, DeparseContext * context)
{
	if (boolExpr->boolop == NOT_EXPR)
	{
		appendStringInfoString(context->buffer, "(NOT ");
		DeparseExpression((Node *) linitial(boolExpr->args), context);
		appendStringInfoChar(context->buffer, ')');
		return;
	}

	const char *separator = boolExpr->boolop == AND_EXPR ? " AND " : " OR ";
	ListCell   *argumentCell = NULL;
	bool		isFirst = true;

	appendStringInfoChar(context->buffer, '(');

	foreach(argumentCell, boolExpr->args)
	{
		if (!isFirst)
			appendStringInfoString(context->buffer, separator);

		isFirst = false;

		DeparseExpression((Node *) lfirst(argumentCell), context);
	}

	appendStringInfoChar(context->buffer, ')');
}


/*
 * DeparseNullTest writes IS NULL and IS NOT NULL.
 */
static void
DeparseNullTest(NullTest *nullTest, DeparseContext * context)
{
	appendStringInfoChar(context->buffer, '(');
	DeparseExpression((Node *) nullTest->arg, context);
	appendStringInfoString(context->buffer,
						   nullTest->nulltesttype == IS_NULL ?
						   " IS NULL)" : " IS NOT NULL)");
}


/*
 * DeparseAggref writes an aggregate call.
 */
static void
DeparseAggref(Aggref *aggref, DeparseContext * context)
{
	const char *aggregateName = SnowflakeAggregateName(aggref->aggfnoid);

	appendStringInfo(context->buffer, "%s(", aggregateName);

	if (aggref->aggstar)
	{
		appendStringInfoChar(context->buffer, '*');
	}
	else
	{
		ListCell   *argumentCell = NULL;
		bool		isFirst = true;

		if (aggref->aggdistinct != NIL)
			appendStringInfoString(context->buffer, "DISTINCT ");

		foreach(argumentCell, aggref->args)
		{
			TargetEntry *argument = (TargetEntry *) lfirst(argumentCell);

			if (argument->resjunk)
				continue;

			if (!isFirst)
				appendStringInfoString(context->buffer, ", ");

			isFirst = false;

			DeparseExpression((Node *) argument->expr, context);
		}
	}

	appendStringInfoChar(context->buffer, ')');
}


/*
 * SnowflakeIsShippableExpression returns whether an expression means the same
 * thing when Snowflake evaluates it.
 */
bool
SnowflakeIsShippableExpression(Expr *expr, SnowflakeRelationInfo * relationInfo)
{
	return IsShippableNode((Node *) expr, relationInfo, NULL);
}


/*
 * IsShippableNode walks an expression and returns whether every part of it is
 * on the allowlist.
 */
static bool
IsShippableNode(Node *node, SnowflakeRelationInfo * relationInfo, PlannerInfo *root)
{
	if (node == NULL)
		return true;

	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var		   *var = (Var *) node;

				/*
				 * Only a plain column of the relation being scanned: a system
				 * column has no Snowflake counterpart, and a Var of another
				 * relation would need a join.
				 */
				if (var->varlevelsup != 0 || var->varattno <= 0)
					return false;

				if (relationInfo->scanRel != NULL &&
					var->varno != relationInfo->scanRel->relid)
					return false;

				return SnowflakeTypeIsPushdownSafe(var->vartype);
			}

		case T_Const:
			{
				Const	   *constant = (Const *) node;

				if (constant->constisnull)
					return true;

				return SnowflakeTypeIsPushdownSafe(constant->consttype);
			}

		case T_Param:
			{
				Param	   *param = (Param *) node;

				if (param->paramkind != PARAM_EXTERN &&
					param->paramkind != PARAM_EXEC)
					return false;

				return SnowflakeTypeIsPushdownSafe(param->paramtype);
			}

		case T_RelabelType:
			{
				RelabelType *relabel = (RelabelType *) node;

				if (!SnowflakeTypeIsPushdownSafe(relabel->resulttype))
					return false;

				return IsShippableNode((Node *) relabel->arg, relationInfo, root);
			}

		case T_OpExpr:
			{
				OpExpr	   *opExpr = (OpExpr *) node;
				const char *snowflakeOperator = NULL;

				if (list_length(opExpr->args) < 1 || list_length(opExpr->args) > 2)
					return false;

				Oid			leftType = exprType(linitial(opExpr->args));
				Oid			rightType = list_length(opExpr->args) > 1 ?
					exprType(lsecond(opExpr->args)) : InvalidOid;

				if (!IsShippableOperator(opExpr->opno, opExpr->inputcollid,
										 leftType, rightType, &snowflakeOperator))
					return false;

				ListCell   *argumentCell = NULL;

				foreach(argumentCell, opExpr->args)
				{
					if (!IsShippableNode((Node *) lfirst(argumentCell), relationInfo,
										 root))
						return false;
				}

				return true;
			}

		case T_ScalarArrayOpExpr:
			{
				ScalarArrayOpExpr *arrayOpExpr = (ScalarArrayOpExpr *) node;
				const char *snowflakeOperator = NULL;

				if (list_length(arrayOpExpr->args) != 2)
					return false;

				Node	   *scalarArgument = linitial(arrayOpExpr->args);
				Node	   *arrayArgument = lsecond(arrayOpExpr->args);

				/*
				 * Only a constant list becomes an IN list. A parameterised
				 * array would have to be expanded at execution time, which
				 * the fragment splicing does not do.
				 */
				if (!IsA(arrayArgument, Const) ||
					((Const *) arrayArgument)->constisnull)
					return false;

				Oid			elementType = get_element_type(exprType(arrayArgument));

				if (!OidIsValid(elementType) ||
					!SnowflakeTypeIsPushdownSafe(elementType))
					return false;

				if (!IsShippableOperator(arrayOpExpr->opno, arrayOpExpr->inputcollid,
										 exprType(scalarArgument), elementType,
										 &snowflakeOperator))
					return false;

				/* IN needs equality, NOT IN needs inequality */
				if (arrayOpExpr->useOr && strcmp(snowflakeOperator, "=") != 0)
					return false;

				if (!arrayOpExpr->useOr && strcmp(snowflakeOperator, "<>") != 0)
					return false;

				return IsShippableNode(scalarArgument, relationInfo, root);
			}

		case T_BoolExpr:
			{
				BoolExpr   *boolExpr = (BoolExpr *) node;
				ListCell   *argumentCell = NULL;

				foreach(argumentCell, boolExpr->args)
				{
					if (!IsShippableNode((Node *) lfirst(argumentCell), relationInfo,
										 root))
						return false;
				}

				return true;
			}

		case T_NullTest:
			return IsShippableNode((Node *) ((NullTest *) node)->arg, relationInfo,
								   root);

		case T_Aggref:
			{
				Aggref	   *aggref = (Aggref *) node;

				/*
				 * Only a plain aggregate over one relation: an ordered set, a
				 * FILTER clause, which Snowflake does not have, and a partial
				 * aggregate all change what the call means.
				 */
				if (aggref->aggsplit != AGGSPLIT_SIMPLE ||
					aggref->aggorder != NIL ||
					aggref->aggfilter != NULL ||
					aggref->aggvariadic ||
					aggref->aggkind != AGGKIND_NORMAL)
					return false;

				if (SnowflakeAggregateName(aggref->aggfnoid) == NULL)
					return false;

				ListCell   *argumentCell = NULL;

				foreach(argumentCell, aggref->args)
				{
					TargetEntry *argument = (TargetEntry *) lfirst(argumentCell);

					if (!IsShippableNode((Node *) argument->expr, relationInfo, root))
						return false;
				}

				return SnowflakeTypeIsPushdownSafe(aggref->aggtype);
			}

		default:
			return false;
	}
}


/*
 * IsShippableOperator returns whether an operator has the same meaning in
 * Snowflake, and what it is called there.
 *
 * Ordering comparisons over text are refused because Postgres orders by its
 * collation and Snowflake orders by code point; equality survives because a
 * deterministic collation makes it byte equality on both sides. Division and
 * modulo are refused because integer division truncates in Postgres and yields
 * a decimal in Snowflake.
 */
static bool
IsShippableOperator(Oid operatorId, Oid inputCollation, Oid leftType, Oid rightType,
					const char **snowflakeOperator)
{
	HeapTuple	operatorTuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(operatorId));

	if (!HeapTupleIsValid(operatorTuple))
		return false;

	Form_pg_operator operatorForm = (Form_pg_operator) GETSTRUCT(operatorTuple);
	char	   *operatorName = pstrdup(NameStr(operatorForm->oprname));
	Oid			operatorNamespace = operatorForm->oprnamespace;

	ReleaseSysCache(operatorTuple);

	if (operatorNamespace != PG_CATALOG_NAMESPACE)
		return false;

	bool		bothTypesSafe = SnowflakeTypeIsPushdownSafe(leftType) &&
		(!OidIsValid(rightType) || SnowflakeTypeIsPushdownSafe(rightType));

	if (!bothTypesSafe)
		return false;

	bool		anyTextLike = IsTextLikeType(leftType) || IsTextLikeType(rightType);
	bool		allNumberLike = IsNumberLikeType(leftType) &&
		(!OidIsValid(rightType) || IsNumberLikeType(rightType));

	if (strcmp(operatorName, "=") == 0 || strcmp(operatorName, "<>") == 0)
	{
		if (anyTextLike && !IsCollationSafe(inputCollation))
			return false;

		*snowflakeOperator = strcmp(operatorName, "=") == 0 ? "=" : "<>";
		return true;
	}

	if (strcmp(operatorName, "<") == 0 || strcmp(operatorName, "<=") == 0 ||
		strcmp(operatorName, ">") == 0 || strcmp(operatorName, ">=") == 0)
	{
		if (anyTextLike)
			return false;

		*snowflakeOperator = operatorName;
		return true;
	}

	if (strcmp(operatorName, "+") == 0 || strcmp(operatorName, "-") == 0 ||
		strcmp(operatorName, "*") == 0)
	{
		if (!allNumberLike)
			return false;

		*snowflakeOperator = operatorName;
		return true;
	}

	if (strcmp(operatorName, "||") == 0)
	{
		if (!anyTextLike)
			return false;

		*snowflakeOperator = "||";
		return true;
	}

	if (strcmp(operatorName, "~~") == 0 || strcmp(operatorName, "!~~") == 0)
	{
		/*
		 * LIKE compares byte by byte on both sides, so it travels, but only
		 * with a collation that does not change what equality means.
		 */
		if (!anyTextLike || !IsCollationSafe(inputCollation))
			return false;

		*snowflakeOperator = strcmp(operatorName, "~~") == 0 ? "LIKE" : "NOT LIKE";
		return true;
	}

	return false;
}


/*
 * IsCollationSafe returns whether a collation leaves equality as byte equality,
 * which is the only property the pushed-down comparisons rely on.
 */
static bool
IsCollationSafe(Oid collation)
{
	return collation == InvalidOid ||
		collation == DEFAULT_COLLATION_OID ||
		collation == C_COLLATION_OID;
}


/*
 * IsTextLikeType returns whether a type is compared as a string.
 */
static bool
IsTextLikeType(Oid typeId)
{
	return typeId == TEXTOID || typeId == VARCHAROID || typeId == BPCHAROID;
}


/*
 * IsNumberLikeType returns whether arithmetic on a type means the same thing on
 * both sides.
 */
static bool
IsNumberLikeType(Oid typeId)
{
	return typeId == INT2OID || typeId == INT4OID || typeId == INT8OID ||
		typeId == FLOAT4OID || typeId == FLOAT8OID || typeId == NUMERICOID;
}


/*
 * SnowflakeAggregateName returns the Snowflake name of an aggregate that
 * computes the same result, or NULL when there is none.
 *
 * AVG is only accepted over floating point. Over an exact numeric, Snowflake
 * decides the scale of the result by its own rules and returns fewer digits than
 * Postgres would, so a pushed-down AVG would quietly answer differently from the
 * same query without pushdown.
 */
static const char *
SnowflakeAggregateName(Oid aggregateFunctionId)
{
	HeapTuple	procedureTuple = SearchSysCache1(PROCOID,
												 ObjectIdGetDatum(aggregateFunctionId));

	if (!HeapTupleIsValid(procedureTuple))
		return NULL;

	Form_pg_proc procedureForm = (Form_pg_proc) GETSTRUCT(procedureTuple);
	char	   *procedureName = pstrdup(NameStr(procedureForm->proname));
	Oid			procedureNamespace = procedureForm->pronamespace;
	Oid			argumentType = procedureForm->pronargs > 0 ?
		procedureForm->proargtypes.values[0] : InvalidOid;

	ReleaseSysCache(procedureTuple);

	if (procedureNamespace != PG_CATALOG_NAMESPACE)
		return NULL;

	if (strcmp(procedureName, "count") == 0)
		return "COUNT";

	if (strcmp(procedureName, "sum") == 0)
	{
		if (!IsNumberLikeType(argumentType))
			return NULL;

		return "SUM";
	}

	if (strcmp(procedureName, "avg") == 0)
	{
		if (argumentType != FLOAT4OID && argumentType != FLOAT8OID)
			return NULL;

		return "AVG";
	}

	if (strcmp(procedureName, "min") == 0 || strcmp(procedureName, "max") == 0)
	{
		/*
		 * The extreme of a set of strings depends on the collation, which is
		 * the same reason ordering comparisons stay local.
		 */
		if (!SnowflakeTypeIsPushdownSafe(argumentType) || IsTextLikeType(argumentType))
			return NULL;

		return strcmp(procedureName, "min") == 0 ? "MIN" : "MAX";
	}

	return NULL;
}


/*
 * SnowflakeDeparsedQueryText renders a deparsed statement with $1, $2 and so on
 * where the parameters go, for EXPLAIN and for logging.
 */
char *
SnowflakeDeparsedQueryText(List *sqlFragments)
{
	StringInfo	statement = makeStringInfo();
	ListCell   *fragmentCell = NULL;
	int			fragmentNumber = 0;

	foreach(fragmentCell, sqlFragments)
	{
		if (fragmentNumber > 0)
			appendStringInfo(statement, "$%d", fragmentNumber);

		appendStringInfoString(statement, strVal(lfirst(fragmentCell)));
		fragmentNumber++;
	}

	return statement->data;
}


/*
 * SnowflakeBuildStatement renders a deparsed statement with the given literals
 * spliced into the holes between its fragments.
 */
char *
SnowflakeBuildStatement(List *sqlFragments, List *paramLiterals)
{
	StringInfo	statement = makeStringInfo();
	ListCell   *fragmentCell = NULL;
	int			fragmentNumber = 0;

	foreach(fragmentCell, sqlFragments)
	{
		if (fragmentNumber > 0)
		{
			appendStringInfoString(statement,
								   (char *) list_nth(paramLiterals, fragmentNumber - 1));
		}

		appendStringInfoString(statement, strVal(lfirst(fragmentCell)));
		fragmentNumber++;
	}

	return statement->data;
}
