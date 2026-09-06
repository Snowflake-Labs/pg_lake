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

#include "access/htup_details.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_type.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/parsetree/const.h"
#include "pg_lake/parsetree/expression.h"
#include "datatype/timestamp.h"
#include "utils/timestamp.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/syscache.h"


/*
 * MakeUpperCaseExpr constructs an upper(..) expression.
 */
Node *
MakeUpperCaseExpr(Node *arg)
{
	FuncExpr   *upperExpr = makeNode(FuncExpr);

	upperExpr->funcid = F_UPPER_TEXT;
	upperExpr->funcresulttype = TEXTOID;
	upperExpr->location = -1;
	upperExpr->args = list_make1(arg);

	return (Node *) upperExpr;
}


/*
 * MakeLowerCaseExpr constructs a lower(...) expression.
 */
Node *
MakeLowerCaseExpr(Node *arg)
{
	FuncExpr   *lowerExpr = makeNode(FuncExpr);

	lowerExpr->funcid = F_LOWER_TEXT;
	lowerExpr->funcresulttype = TEXTOID;
	lowerExpr->location = -1;
	lowerExpr->args = list_make1(arg);

	return (Node *) lowerExpr;
}


/*
 * MakeDatePartExpr builds a date_part('part', timestamp) expression.
 */
Node *
MakeDatePartExpr(char *part, Node *timeArg)
{
	const int	argCount = 2;
	List	   *datePartName = list_make2(makeString("pg_catalog"),
										  makeString("date_part"));
	Oid			argTypes[] = {TEXTOID, exprType(timeArg)};

	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = LookupFuncName(datePartName, argCount, argTypes, false);
	funcExpr->funcresulttype = TEXTOID;
	funcExpr->location = -1;
	funcExpr->args = list_make2(MakeStringConst(part), timeArg);

	return (Node *) funcExpr;
}


/*
 * MakeDateTruncExpr builds a date_trunc('part', timestamp) expression.
 */
Node *
MakeDateTruncExpr(char *part, Node *timeArg)
{
	const int	argCount = 2;
	List	   *datePartName = list_make2(makeString("pg_catalog"),
										  makeString("date_trunc"));
	Oid			argTypes[] = {TEXTOID, exprType(timeArg)};

	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = LookupFuncName(datePartName, argCount, argTypes, false);
	funcExpr->funcresulttype = exprType(timeArg);
	funcExpr->location = -1;
	funcExpr->args = list_make2(MakeStringConst(part), timeArg);

	return (Node *) funcExpr;
}


/*
 * MakeDivisionExpr builds a divide(arg, divisor) expression.
 */
Node *
MakeDivisionExpr(Node *arg, int divisor)
{
	Oid			argTypes[] = {INT4OID, INT4OID};
	List	   *qualifiedName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										   makeString("divide"));

	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = LookupFuncName(qualifiedName, 2, argTypes, false);
	funcExpr->funcresulttype = INT4OID;
	funcExpr->location = -1;
	funcExpr->args = list_make2(arg, MakeIntConst(divisor));

	return (Node *) funcExpr;
}


/*
 * MakeModInt32Expr builds a mod(arg, modulo) expression.
 */
Node *
MakeModInt32Expr(Node *arg, int32 modulo)
{
	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = F_MOD_INT4_INT4;
	funcExpr->funcresulttype = INT4OID;
	funcExpr->location = -1;
	funcExpr->args = list_make2(arg, MakeIntConst(modulo));

	return (Node *) funcExpr;
}


/*
 * MakeModInt64Expr builds a mod(arg, modulo) expression.
 */
Node *
MakeModInt64Expr(Node *arg, int64 modulo)
{
	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = F_MOD_INT8_INT8;
	funcExpr->funcresulttype = INT8OID;
	funcExpr->location = -1;
	funcExpr->args = list_make2(arg, MakeInt64Const(modulo));

	return (Node *) funcExpr;
}


/*
 * MakeLpadExpr builds a lpad(arg, length, string) expression.
 */
Node *
MakeLpadExpr(Node *arg, int length, char *string)
{
	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = F_LPAD_TEXT_INT4_TEXT;
	funcExpr->funcresulttype = TEXTOID;
	funcExpr->location = -1;
	funcExpr->args = list_make3(arg, MakeIntConst(length), MakeStringConst(string));

	return (Node *) funcExpr;
}


/*
 * MakeRpadExpr builds a rpad(arg, length, string) expression.
 */
Node *
MakeRpadExpr(Node *arg, int length, char *string)
{
	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = F_RPAD_TEXT_INT4_TEXT;
	funcExpr->funcresulttype = TEXTOID;
	funcExpr->location = -1;
	funcExpr->args = list_make3(arg, MakeIntConst(length), MakeStringConst(string));

	return (Node *) funcExpr;
}


/*
 * MakeCastExpr builds a arg::type expression.
 */
Node *
MakeCastExpr(Node *arg, Oid targetType)
{
	CoerceViaIO *castExpr = makeNode(CoerceViaIO);

	castExpr->resulttype = targetType;
	castExpr->arg = (Expr *) arg;
	castExpr->coerceformat = COERCE_EXPLICIT_CAST;
	castExpr->location = -1;

	return (Node *) castExpr;
}


/*
 * MakeCaseExpr builds a CASE WHEN testArg THEN ifTrueArg ELSE elseArg END
 * expression.
 */
Node *
MakeCaseExpr(Node *testArg, Node *ifTrueArg, Node *elseArg)
{
	/* CASE WHEN testArg THEN ifTrueArg */
	CaseWhen   *caseWhenExpr = makeNode(CaseWhen);

	caseWhenExpr->expr = (Expr *) testArg;
	caseWhenExpr->result = (Expr *) ifTrueArg;
	caseWhenExpr->location = -1;

	/* CASE WHEN testArg THEN ifTrueArg ELSE elseArg END */
	CaseExpr   *caseExpr = makeNode(CaseExpr);

	caseExpr->args = list_make1(caseWhenExpr);
	caseExpr->defresult = (Expr *) elseArg;
	caseExpr->casetype = TEXTOID;

	return (Node *) caseExpr;
}


/*
 * MakeAddIntervalExpr constructs a <node> + interval '...' expression.
 */
Node *
MakeOpExpr(Node *left, char *schemaName, char *operatorName, Node *right)
{
	List	   *nameList = list_make2(makeString(schemaName), makeString(operatorName));
	Oid			leftTypeId = exprType(left);
	Oid			rightTypeId = exprType(right);

	Operator	operatorTuple = oper(NULL, nameList, leftTypeId, rightTypeId, false, -1);
	Form_pg_operator operator = (Form_pg_operator) GETSTRUCT(operatorTuple);

	OpExpr	   *opExpr = makeNode(OpExpr);

	opExpr->opno = operator->oid;
	opExpr->opfuncid = operator->oprcode;
	opExpr->opresulttype = operator->oprresult;
	opExpr->args = list_make2(left, right);

	ReleaseSysCache(operatorTuple);

	return (Node *) opExpr;
}


/*
 * MakeAddIntervalExpr constructs a <node> + interval '...' expression.
 */
Node *
MakeAddIntervalExpr(Node *node, Interval *interval)
{
	Const	   *intervalConst = makeConst(INTERVALOID, -1, InvalidOid,
										  sizeof(Interval),
										  IntervalPGetDatum(interval),
										  false, true);

	return MakeOpExpr(node, "pg_catalog", "+", (Node *) intervalConst);
}
