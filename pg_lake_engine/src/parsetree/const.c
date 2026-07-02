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
#include "catalog/pg_type_d.h"

#include "pg_lake/parsetree/const.h"

#include "catalog/pg_collation_d.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "optimizer/optimizer.h"
#include "utils/builtins.h"

/*
 * IsConstCastChain returns whether the given node is a Const or
 * a Const with casts.
 */
bool
IsConstCastChain(Node *node)
{
	/* yes, we are */
	if (IsA(node, Const))
		return true;

	/* could be */
	if (IsA(node, RelabelType))
		return IsConstCastChain((Node *) ((RelabelType *) node)->arg);

	/* check if a function is a cast function */
	if (IsA(node, FuncExpr))
	{
		FuncExpr   *funcExpr = (FuncExpr *) node;

		/* casts only have a single argument to check */
		return funcExpr->funcformat != COERCE_EXPLICIT_CALL &&
			IsConstCastChain(linitial(funcExpr->args));
	}

	/* nope */
	return false;
}


/*
 * Resolve the underlying constant chain to a final datum and type.
 *
 * It is assumed you already validated that this is a valid ConstChain, and by
 * extension a valid expression to be evaluated immediately.
 */
Const *
ResolveConstChain(Node *node)
{
	Node	   *expr = eval_const_expressions(NULL, node);

	/*
	 * If we are passed a named argument we need to extract the underlying
	 * node and check that.  This can happen when parsing a function like
	 * "myfunc(arg1, arg2 := true)".
	 */
	if (IsA(expr, NamedArgExpr))
		expr = (Node *)
			((NamedArgExpr *) expr)->arg;

	if (!IsA(expr, Const))
		return NULL;

	return (Const *) expr;
}


/*
 * GetConstArg sets argConst to the nth argument of a function call
 * and evaluates constant expressions if needed.
 */
bool
GetConstArg(Node *node, int argIndex, Const **argConst)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	Node	   *argNode = list_nth(funcExpr->args, argIndex);

	argNode = eval_const_expressions(NULL, argNode);

	/*
	 * If we are passed a named argument we need to extract the underlying
	 * node and check that.  This can happen when parsing a function like
	 * "myfunc(arg1, arg2 := true)".
	 */
	if (IsA(argNode, NamedArgExpr))
		argNode = (Node *) ((NamedArgExpr *) argNode)->arg;

	if (!IsA(argNode, Const))
		return false;

	*argConst = (Const *) argNode;

	return true;
}


/*
 * MakeStringConst() creates a text-based Const * given an input cstring
 */
Const *
MakeStringConst(const char *string)
{
	bool		isnull = string == NULL;
	Datum		value = isnull ? (Datum) 0
		: CStringGetTextDatum(string);

	Const	   *newConst = makeConst(TEXTOID,
									 -1,
									 DEFAULT_COLLATION_OID,
									 -1,
									 value,
									 isnull,
									 false);

	return newConst;
}


/*
 * MakeIntConst creates a integer Const * given an input.
 */
Const *
MakeIntConst(int32 value)
{
	Const	   *newConst = makeConst(INT4OID,
									 -1,
									 InvalidOid,
									 sizeof(int32),
									 Int32GetDatum(value),
									 false,
									 true);

	return newConst;
}


/*
 * MakeInt64Const creates a int64 Const * given an input.
 */
Const *
MakeInt64Const(int64 value)
{
	Const	   *newConst = makeConst(INT8OID,
									 -1,
									 InvalidOid,
									 sizeof(int64),
									 Int64GetDatum(value),
									 false,
									 true);

	return newConst;
}
