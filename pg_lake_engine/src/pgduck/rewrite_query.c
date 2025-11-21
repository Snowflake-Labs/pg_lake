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

#include <float.h>
#include <math.h>

#include "catalog/pg_operator.h"
#include "catalog/pg_operator_d.h"
#include "catalog/pg_proc.h"
#include "common/hashfn.h"
#include "pg_lake/extensions/pg_lake_spatial.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/parsetree/const.h"
#include "pg_lake/parsetree/columns.h"
#include "pg_lake/parsetree/expression.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/rewrite_query.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/to_char.h"
#include "pg_lake/util/operator_utils.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parse_func.h"
#include "parser/parse_oper.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/syscache.h"

#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

typedef Node *(*ExpressionRewriter) (Node *expression, void *context);

/*
 * RewriteQueryTreeContext keeps context for query tree rewrite rules.
 */
typedef struct RewriteQueryTreeContext
{
	/* current level of the query (0 is top level) */
	int			level;
	/* our extra arg from rewrite rule */
	int			funcIntArg;
}			RewriteQueryTreeContext;


/*
 * RewriteRule represents a rule to rewrite some form of expression
 */
typedef struct RewriteRule
{
	Oid			objId;			/* we assume no OID conflicts between types so
								 * we can use oid_hash as key func */
	ExpressionRewriter rewriteFunction;
	union
	{
		char	   *stringArg;
		int			intArg;
	}			extra;
} RewriteRule;


/*
 * FunctionCallRewriteRuleByName represents a rule to rewrite a FuncExpr
 * for use in PGDuck.
 */
typedef struct FunctionCallRewriteRuleByName
{
	/*
	 * TODO: We might need to require nArgs and argTypes to be able to
	 * precisely decide on when to rewrite the function call. Currently, we
	 * are only using functionName and schemaName to decide on the rewrite as
	 * we either re-write all the functions with the same name and nArgs, or
	 * we don't rewrite them at all.
	 */
	const char *schemaName;
	const char *functionName;
	ExpressionRewriter rewriteFunction;
	int			limitArgs;
}			FunctionCallRewriteRuleByName;


/*
 * AggregateRewriteRuleByName represents a rule to rewrite an Aggref
 * for use in PGDuck.
 */

typedef struct AggregateRewriteRuleByName
{
	const char *schemaName;
	const char *aggregateName;
	ExpressionRewriter rewriteAggregate;
}			AggregateRewriteRuleByName;


/*
 * OperatorRewriteRuleByName represents a rule to rewrite an OpExpr
 * for use in PGDuck.
 */
typedef struct OperatorRewriteRuleByName
{
	const char *schemaName;
	const char *operatorName;
	char	   *replacementFunctionName;
	ExpressionRewriter rewriteOperator;
}			OperatorRewriteRuleByName;


static void InitRewriteRules(void);
static Node *RewriteQueryTreeForPGDuckMutator(Node *node, RewriteQueryTreeContext * context);
static Node *ReplaceJsonbWithPgLakeInternalJsonbFunction(Node *inputNode, void *context);
static bool IsJsonbCast(Node *node);
static Node *WrapPgLakeInternalJsonbFunction(Node *data);
static Node *FindArgForNode(Node *inputNode);
static Node *RewriteParam(Param *paramExpr);

static RewriteRule *GetRewriteRuleForFunctionId(Oid functionId);
static ExpressionRewriter GetFunctionCallRewriter(Oid functionId, int *intArg);

static RewriteRule *GetRewriteRuleForAggregateId(Oid functionId);
static ExpressionRewriter GetAggregateRewriter(Oid functionId);

static RewriteRule *GetRewriteRuleForOperatorId(Oid functionId);
static ExpressionRewriter GetOperatorRewriter(Oid functionId);
static char *GetOperatorRewriteFunctionName(Oid functionId);

static RewriteRule *GetRewriteRuleForOperatorId(Oid functionId);
static ExpressionRewriter GetOperatorRewriter(Oid functionId);
static char *GetOperatorRewriteFunctionName(Oid functionId);

static Node *RewriteFuncExprBtrim(Node *node, void *context);
static Node *RewriteFunctionCallExpression(Oid functionId, Node *node, void *context);
static Node *RewriteFuncExprCast(Node *node, void *context);
static Node *RewriteFuncExprExtract(Node *node, void *context);
static Node *RewriteFuncExprDateBin(Node *node, void *context);
static Node *RewriteFuncExprDateTrunc(Node *node, void *context);
static Node *RewriteFuncExprDiv(Node *node, void *context);
static Node *RewriteFuncExprMod(Node *node, void *context);
static Node *RewriteFuncExprSubstring(Node *node, void *context);
static Node *RewriteFuncExprGenerateSeries(Node *node, void *context);
static Node *RewriteFuncExprToChar(Node *node, void *context);
static Node *RewriteFuncExprNow(Node *node, void *context);
static Node *RewriteFuncExprMapExtract(Node *node, void *context);
static Node *RewriteFuncExprPad(Node *node, void *context);
static Node *RewriteFuncExprCardinality(Node *node, void *context);
static Node *RewriteFuncExprArrayLength(Node *node, void *context);
static Node *RewriteFuncExprPostgisBytea(Node *node, void *context);
static Node *RewriteFuncExprTrigonometry(Node *node, void *context);
static Node *RewriteFuncExprInverseTrigonometry(Node *node, void *context);
static Node *RewriteFuncExprHyperbolic(Node *node, void *context);
static Node *RewriteFuncExprJsonbArrayLength(Node *node, void *context);
static Node *RewriteFuncExprEncode(Node *node, void *context);
static Node *RewriteFuncExprDecode(Node *node, void *context);
static Node *RewriteFuncExprToZeroOrNullConst(Node *node, void *context);
static Node *RewriteFuncExprPostgisTransform(Node *node, void *context);
static Node *RewriteFuncExprPostgisTransformGeometry(Node *node, void *context);
static Node *RewriteFuncExprPostgisGeometryTypeFunction(Node *node, void *context);
static Node *RewriteFuncExprPostgisUnionAgg(Node *node, void *context);
static Node *RewriteFuncExprPostgisGeometryFromText(Node *node, void *context);
static Node *RewriteFuncExprPostgisGeographyCast(Node *node, void *context);
static Node *RewriteFuncExprPostgisArea(Node *node, void *context);
static Node *RewriteFuncExprPostgisLength(Node *node, void *context);
static Node *RewriteFuncExprPostgisPerimeter(Node *node, void *context);
static Node *RewriteFuncExprPostgisDistance(Node *node, void *context);
static Node *RewriteFuncExprPostgisSpheroid(Node *node, char *functionName,
											int argCount, Oid *argTypes);
static Node *RewriteOpExprPostgisBoxIntersects(Node *node, void *context);
static Node *RewriteOpExprLikeNotLike(Node *node, void *context);
static Node *RewriteOpExprXor(Node *node, void *context);
static Node *RewriteOpExprSqrt(Node *node, void *context);
static Node *RewriteOpExprIntegerDivision(Node *node, void *context);
static Node *RewriteOpExprArrayAppend(Node *node, void *context);
static Node *RewriteFuncExprRemoveArgs(Node *node, void *context);

static Node *RewriteAggregateExpression(Aggref *node, void *context);

static Node *RewriteOperatorExpression(OpExpr *node, void *context);
static Node *FindCompatibleFunctionWithName(OpExpr *opExpr, char *funcname);

static Node *RewriteSQLValueFunction(SQLValueFunction *node, void *context);

static Node *AddOneYear(Node *node);
static Node *DereferenceIndex(Node *node, int index);
static FuncExpr *ConvertOpExprToFuncExpr(OpExpr *opExpr);

static OpExpr *CommutateOpExpr(OpExpr *opExpr);
static Node *DowncastFloatConstInOpExpr(Node *node);

PG_FUNCTION_INFO_V1(pg_lake_internal_dummy_function);

/* function or operator rewrite rules */
static FunctionCallRewriteRuleByName BuiltinFunctionCallRewriteRulesByName[] =
{
	{
		"pg_catalog", "btrim", RewriteFuncExprBtrim, 0
	},
	{
		"pg_catalog", "extract", RewriteFuncExprExtract, 0
	},
	{
		"pg_catalog", "date_bin", RewriteFuncExprDateBin, 0
	},
	{
		"pg_catalog", "date_trunc", RewriteFuncExprDateTrunc, 0
	},
	{
		"pg_catalog", "div", RewriteFuncExprDiv, 0
	},
	{
		"pg_catalog", "mod", RewriteFuncExprMod, 0
	},
	{
		"pg_catalog", "rpad", RewriteFuncExprPad, 0
	},
	{
		"pg_catalog", "lpad", RewriteFuncExprPad, 0
	},
	{
		"pg_catalog", "substr", RewriteFuncExprSubstring, 0
	},
	{
		"pg_catalog", "substring", RewriteFuncExprSubstring, 0
	},
	{
		"pg_catalog", "generate_series", RewriteFuncExprGenerateSeries, 0
	},
	{
		"pg_catalog", "to_char", RewriteFuncExprToChar, 0
	},
	{
		"pg_catalog", "now", RewriteFuncExprNow, 0
	},
	{
		"map_type", "extract", RewriteFuncExprMapExtract
	},

	/* array functions */
	{
		"pg_catalog", "array_length", RewriteFuncExprArrayLength, 0
	},
	{
		"pg_catalog", "cardinality", RewriteFuncExprCardinality, 0
	},

	/* degree variants of trigonometry functions */
	{
		"pg_catalog", "acosd", RewriteFuncExprInverseTrigonometry, 0
	},
	{
		"pg_catalog", "asind", RewriteFuncExprInverseTrigonometry, 0
	},
	{
		"pg_catalog", "atand", RewriteFuncExprInverseTrigonometry, 0
	},
	{
		"pg_catalog", "atan2d", RewriteFuncExprInverseTrigonometry, 0
	},
	{
		"pg_catalog", "cosd", RewriteFuncExprTrigonometry, 0
	},
	{
		"pg_catalog", "cotd", RewriteFuncExprTrigonometry, 0
	},
	{
		"pg_catalog", "sind", RewriteFuncExprTrigonometry, 0
	},
	{
		"pg_catalog", "tand", RewriteFuncExprTrigonometry, 0
	},

	/* hyperbolic functions */
	{
		"pg_catalog", "acosh", RewriteFuncExprHyperbolic, 0
	},
	{
		"pg_catalog", "atanh", RewriteFuncExprHyperbolic, 0
	},

	/* explicit calls to cast functions */
	{
		"pg_catalog", "bool", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "float4", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "float8", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "int2", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "int4", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "int8", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "date", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "text", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "timestamp", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "timestamptz", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "time", RewriteFuncExprCast, 0
	},
	{
		"pg_catalog", "timetz", RewriteFuncExprCast, 0
	},

	/* jsonb functions */
	{
		"pg_catalog", "jsonb_array_length", RewriteFuncExprJsonbArrayLength, 0
	},

	/* encode/decode functions */
	{
		"pg_catalog", "encode", RewriteFuncExprEncode, 0
	},
	{
		"pg_catalog", "decode", RewriteFuncExprDecode, 0
	},

};

static OperatorRewriteRuleByName BuiltinOperatorRewriteRulesByName[] =
{
	{
		"pg_catalog", "->", NULL, RewriteFuncExprMapExtract
	},
	{
		"pg_catalog", "~", NULL, RewriteOpExprLikeNotLike
	},
	{
		"pg_catalog", "!~", NULL, RewriteOpExprLikeNotLike
	},
	{
		"pg_catalog", "~*", NULL, RewriteOpExprLikeNotLike
	},
	{
		"pg_catalog", "!~*", NULL, RewriteOpExprLikeNotLike
	},
	{
		"pg_catalog", "#", NULL, RewriteOpExprXor
	},
	{
		"pg_catalog", "|/", NULL, RewriteOpExprSqrt
	},
	{
		"pg_catalog", "||/", NULL, RewriteOpExprSqrt
	},
	{
		"pg_catalog", "/", NULL, RewriteOpExprIntegerDivision
	},
	{
		"pg_catalog", "||", NULL, RewriteOpExprArrayAppend
	},
};

static FunctionCallRewriteRuleByName PostgisFunctionCallRewriteRulesByName[] =
{
	{
		POSTGIS_SCHEMA, "bytea", RewriteFuncExprPostgisBytea, 0
	},
	/* st_asbinary(geometry) and bytea(geometry) are interchangeable */
	{
		POSTGIS_SCHEMA, "st_asbinary", RewriteFuncExprPostgisBytea, 0
	},
	{
		POSTGIS_SCHEMA, "st_srid", RewriteFuncExprToZeroOrNullConst, 0
	},
	{
		POSTGIS_SCHEMA, "st_zmflag", RewriteFuncExprToZeroOrNullConst, 0
	},
	{
		POSTGIS_SCHEMA, "st_transform", RewriteFuncExprPostgisTransform, 0
	},
	{
		POSTGIS_SCHEMA, "st_geometryfromtext", RewriteFuncExprPostgisGeometryFromText, 0
	},
	{
		POSTGIS_SCHEMA, "st_difference", RewriteFuncExprRemoveArgs, 2
	},
	{
		POSTGIS_SCHEMA, "st_intersection", RewriteFuncExprRemoveArgs, 2
	},
	{
		POSTGIS_SCHEMA, "st_makeenvelope", RewriteFuncExprRemoveArgs, 4
	},
	{
		POSTGIS_SCHEMA, "st_asgeojson", RewriteFuncExprRemoveArgs, 1
	},
	{
		POSTGIS_SCHEMA, "postgis_transform_geometry", RewriteFuncExprPostgisTransformGeometry, 0
	},
	{
		POSTGIS_SCHEMA, "geography", RewriteFuncExprPostgisGeographyCast, 0
	},
	{
		POSTGIS_SCHEMA, "st_area", RewriteFuncExprPostgisArea, 0
	},
	{
		POSTGIS_SCHEMA, "st_distance", RewriteFuncExprPostgisDistance, 0
	},
	{
		POSTGIS_SCHEMA, "st_length", RewriteFuncExprPostgisLength, 0
	},
	{
		POSTGIS_SCHEMA, "st_perimeter", RewriteFuncExprPostgisPerimeter, 0
	},
	{
		POSTGIS_SCHEMA, "geometrytype", RewriteFuncExprPostgisGeometryTypeFunction, 0
	},
};

static AggregateRewriteRuleByName PostgisAggregateRewriteRulesByName[] =
{
	{
		POSTGIS_SCHEMA, "st_union", RewriteFuncExprPostgisUnionAgg
	},
};


static AggregateRewriteRuleByName BuiltinAggregateRewriteRulesByName[] =
{
};


static OperatorRewriteRuleByName PostgisOperatorRewriteRulesByName[] =
{
	{
		POSTGIS_SCHEMA, "<->", "st_distance", NULL
	},
	{
		POSTGIS_SCHEMA, "&&", NULL, RewriteOpExprPostgisBoxIntersects
	},
};


static MemoryContext RewriteRulesMemoryContext = NULL;
static HTAB *RewriteRulesHash = NULL;


/*
 * InitRewriteRules sets up the per-backend hash that maps function OIDs and
 * operator OIDs to rewrite rules.
 */
static void
InitRewriteRules(void)
{
	if (RewriteRulesHash != NULL)
		return;

	RewriteRulesMemoryContext = AllocSetContextCreateInternal(TopMemoryContext,
															  "Rewrite Rules Memory Context",
															  ALLOCSET_DEFAULT_MINSIZE,
															  ALLOCSET_DEFAULT_INITSIZE,
															  ALLOCSET_DEFAULT_MAXSIZE);

	HASHCTL		hashInfo = {0};

	hashInfo.keysize = sizeof(Oid);
	hashInfo.entrysize = sizeof(RewriteRule);
	hashInfo.hash = oid_hash;
	hashInfo.hcxt = RewriteRulesMemoryContext;

	uint32		hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);

	RewriteRulesHash = hash_create("rewrite rules hash", 16, &hashInfo,
								   hashFlags);
}


/*
 * RewriteQueryTreeForPGDuckMutator rewrites a PostgreSQL query tree for use in pgduck
 * by rewriting expressions.
 */
Query *
RewriteQueryTreeForPGDuck(Query *query)
{
	RewriteQueryTreeContext context = {
		/* start at -1 such that expression in top-level query see level 0 */
		.level = -1
	};

	return (Query *) RewriteQueryTreeForPGDuckMutator((Node *) query, &context);
}


/*
 * RewriteQueryTreeForPGDuck rewrites a PostgreSQL expression and child expressions
 * for use in pgduck.
 */
static Node *
RewriteQueryTreeForPGDuckMutator(Node *node, RewriteQueryTreeContext * context)
{
	if (node == NULL)
	{
		return NULL;
	}

	if (IsA(node, Query))
	{
		Query	   *query = (Query *) node;

		/* we handle FOR UPDATE separately */
		query->hasForUpdate = false;

		context->level++;

		node = (Node *) query_tree_mutator(query,
										   RewriteQueryTreeForPGDuckMutator,
										   context, 0);

		context->level--;

		return node;
	}

	/*
	 * We do depth-first rewrites to make the code flow a bit more nicely,
	 * because we don't have to avoid recursing back into our rewritten
	 * result. It also lets us see rewritten arguments.
	 */
	node = expression_tree_mutator(node, RewriteQueryTreeForPGDuckMutator, context);

	/*
	 * DuckDB does not support JSONB, so we need to rewrite it to JSON.
	 */
	if (IsJsonbCast(node))
	{
		node = ReplaceJsonbWithPgLakeInternalJsonbFunction(node, context);
	}

	if (IsA(node, Const))
	{
		Const	   *constExpr = (Const *) node;

		node = RewriteConst(constExpr);
	}
	else if (IsA(node, Param))
	{
		Param	   *paramExpr = (Param *) node;

		node = RewriteParam(paramExpr);
	}
	else if (IsA(node, FuncExpr))
	{
		FuncExpr   *funcExpr = (FuncExpr *) node;
		Oid			functionId = funcExpr->funcid;

		node = RewriteFunctionCallExpression(functionId, node, context);
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr	   *opExpr = (OpExpr *) node;

		node = RewriteOperatorExpression(opExpr, context);
	}
	else if (IsA(node, Aggref))
	{
		Aggref	   *aggExpr = (Aggref *) node;

		node = RewriteAggregateExpression(aggExpr, context);
	}
	else if (IsA(node, SQLValueFunction))
	{
		SQLValueFunction *sqlValueFunctionExpr = (SQLValueFunction *) node;

		node = RewriteSQLValueFunction(sqlValueFunctionExpr, context);
	}

	return node;
}


/*
* IsJsonbCast checks whether the given node is a jsonb cast.
* We check for the following node types as they can have jsonb casts:
* - Const
* - CoerceViaIO
* - RelabelType
* - CoerceToDomain
* - Param
*
* For other nodes (e.g., CoalesceExpr, FuncExpr, etc.) we traverse the expression
* tree, so we'll eventually find the jsonb cast if it exists.
*/
static bool
IsJsonbCast(Node *node)
{
	if (node == NULL)
		return false;

	if (IsA(node, Const) || IsA(node, CoerceViaIO) || IsA(node, RelabelType) ||
		IsA(node, CoerceToDomain) || IsA(node, Param))
	{
		return exprType(node) == JSONBOID;
	}

	return false;
}

/*
 * ReplaceJsonbWithPgLakeInternalJsonbFunction replaces any jsonb in with
 * a function call to the __lake__internal__nsp__.jsonb, which is a
 * function that also exists in DuckDB.
 *
 * We need this function because DuckDB does not have JSONB type, but only
 * JSON. We wrap any JSONB type in a function call which is then sent to duckdb
 * where it is converted to JSON. With that, we make sure we never have any bogus
 * JSONB.
 */
static Node *
ReplaceJsonbWithPgLakeInternalJsonbFunction(Node *inputNode, void *context)
{
	/* this function only intended for exprType = JSONBOID */
	Assert(IsJsonbCast(inputNode));

	if (IsA(inputNode, Const))
	{
		Const	   *constExpr = (Const *) inputNode;

		char	   *jsonb_cstr =
			constExpr->constisnull ? NULL :
			DatumGetCString(DirectFunctionCall1(jsonb_out, constExpr->constvalue));

		return (Node *) WrapPgLakeInternalJsonbFunction((Node *) MakeStringConst(jsonb_cstr));
	}
	else if (IsA(inputNode, Param))
	{
		Param	   *paramExpr = (Param *) inputNode;

		return (Node *) WrapPgLakeInternalJsonbFunction((Node *) paramExpr);
	}
	else if (IsA(inputNode, CoerceViaIO) || IsA(inputNode, RelabelType) ||
			 IsA(inputNode, CoerceToDomain))
	{

		Node	   *arg = FindArgForNode(inputNode);
		Oid			argType = exprType(arg);

		if (argType == TEXTOID || argType == CSTRINGOID ||
			argType == JSONOID || argType == VARCHAROID ||
			argType == BPCHAROID)
		{
			return WrapPgLakeInternalJsonbFunction(arg);
		}
	}

	return (Node *) inputNode;
}


/*
* FindArgForNode is a helper function for fetching the argument of a
* CoerceViaIO, RelabelType or CoerceToDomain node.
*/
static Node *
FindArgForNode(Node *inputNode)
{
	if (IsA(inputNode, CoerceViaIO))
		return (Node *) ((CoerceViaIO *) inputNode)->arg;
	else if (IsA(inputNode, RelabelType))
		return (Node *) ((RelabelType *) inputNode)->arg;
	else if (IsA(inputNode, CoerceToDomain))
		return (Node *) ((CoerceToDomain *) inputNode)->arg;

	elog(ERROR, "unexpected node type %d while fetching arguments",
		 nodeTag(inputNode));
}

/*
 * WrapPgLakeInternalJsonbFunction is a helper function that
 * wraps the given node in a function call to the
 * __lake__internal__nsp__.jsonb function.
 */
static Node *
WrapPgLakeInternalJsonbFunction(Node *node)
{
	Oid			argTypes[] = {exprType(node)};
	List	   *qualifiedName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										   makeString("jsonb"));

	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = LookupFuncName(qualifiedName, 1, argTypes, false);
	funcExpr->funcresulttype = JSONBOID;
	funcExpr->location = -1;
	funcExpr->args = list_make1(node);

	return (Node *) funcExpr;
}


/*
 * RewriteConst rewrites a constant to use the DuckDB serialization format.
 */
Node *
RewriteConst(Const *constExpr)
{
	Oid			constTypeId = constExpr->consttype;

	if (constExpr->constisnull || !IsPGDuckSerializeRequired(MakePGType(constTypeId, constExpr->consttypmod)))
	{
		/* NULL or regular type */
		return (Node *) constExpr;
	}

	/* look up the regular output function for the type */
	FmgrInfo	outFunc;
	Oid			outFuncId = InvalidOid;
	bool		isvarlena = false;

	getTypeOutputInfo(constTypeId, &outFuncId, &isvarlena);
	fmgr_info(outFuncId, &outFunc);

	/* serialize Const in the DuckDB format */
	char	   *pgduckText = PGDuckSerialize(&outFunc, constTypeId, constExpr->constvalue);

	/* construct a text constant with the rewritten text */
	Const	   *textConst = makeNode(Const);

	textConst->consttype = TEXTOID;
	textConst->consttypmod = -1;
	textConst->constlen = -1;
	textConst->constvalue = CStringGetTextDatum(pgduckText);
	textConst->constbyval = false;
	textConst->constisnull = false;
	textConst->location = -1;

	/* add explicit cast to the original type */
	RelabelType *coerceExpr = makeNode(RelabelType);

	coerceExpr->arg = (Expr *) textConst;
	coerceExpr->resulttype = constTypeId;
	coerceExpr->resulttypmod = constExpr->consttypmod;
	coerceExpr->resultcollid = constExpr->constcollid;
	coerceExpr->relabelformat = COERCE_EXPLICIT_CAST;
	coerceExpr->location = -1;

	return (Node *) coerceExpr;
}

/*
 * RewriteParam adds an explicit cast to Param nodes in order to
 * make sure DuckDB knows the type (which we don't pass via bind
 * in pgduck_server at the moment).
 */
static Node *
RewriteParam(Param *paramExpr)
{
	RelabelType *coerceExpr = makeNode(RelabelType);

	coerceExpr->arg = (Expr *) paramExpr;
	coerceExpr->resulttype = paramExpr->paramtype;
	coerceExpr->resulttypmod = paramExpr->paramtypmod;
	coerceExpr->resultcollid = paramExpr->paramcollid;
	coerceExpr->relabelformat = COERCE_EXPLICIT_CAST;
	coerceExpr->location = -1;

	return (Node *) coerceExpr;
}


/*
 * Rewrite a SQLValueFunction
 */
static Node *
RewriteSQLValueFunction(SQLValueFunction *node, void *context)
{
	/*
	 * Rewrite to the same __lake_now() function which will be replaced on the
	 * deparser with the actual value, but casted to the appropriate type..
	 */

	Assert(IsShippableSQLValueFunction(node->op));

	List	   *currentDateName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											 makeString(PG_LAKE_NOW_TEMPLATE));

	int			argCount = 0;

	/* set the function OID to the __lake_now OID */
	FuncExpr   *funcExpr = makeFuncExpr(
										LookupFuncName(currentDateName, argCount, NULL, false),
										TIMESTAMPTZOID,
										NULL,
										InvalidOid,
										InvalidOid,
										COERCE_EXPLICIT_CALL
		);

	/* now the return values cast to date */
	RelabelType *coerceExpr = makeNode(RelabelType);

	coerceExpr->arg = (Expr *) funcExpr;
	coerceExpr->resulttype = exprType((Node *) node);
	coerceExpr->resulttypmod = -1;
	coerceExpr->resultcollid = InvalidOid;
	coerceExpr->relabelformat = COERCE_EXPLICIT_CAST;
	coerceExpr->location = -1;

	return (Node *) coerceExpr;

}


/*
 * GetRewriteRuleForAggregateId returns the rewrite rule for an aggregate ID.
 * If the functionName is set, it should be called to rewrite the
 * aggregate expression.
 */
static RewriteRule *
GetRewriteRuleForAggregateId(Oid functionId)
{
	InitRewriteRules();

	bool		found = false;
	RewriteRule *rule =
		hash_search(RewriteRulesHash, &functionId, HASH_ENTER, &found);

	if (!found)
	{
		rule->rewriteFunction = GetAggregateRewriter(functionId);
	}

	return rule;
}


/*
 * GetAggregateRewriter gets the function for rewriting an aggregate expression.
 */
static ExpressionRewriter
GetAggregateRewriter(Oid aggregateId)
{
	Oid			schemaId = get_func_namespace(aggregateId);
	char	   *schemaName = get_namespace_name(schemaId);

	char	   *aggregateName = get_func_name(aggregateId);
	size_t		ruleCount = ARRAY_SIZE(BuiltinAggregateRewriteRulesByName);

	for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
	{
		AggregateRewriteRuleByName *rule =
			&BuiltinAggregateRewriteRulesByName[ruleIndex];

		if (strcmp(schemaName, rule->schemaName) == 0 &&
			strcmp(aggregateName, rule->aggregateName) == 0)
		{
			return rule->rewriteAggregate;
		}
	}

	if (IsExtensionCreated(Postgis) && schemaId == ExtensionSchemaId(Postgis))
	{
		ruleCount = ARRAY_SIZE(PostgisAggregateRewriteRulesByName);

		for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
		{
			AggregateRewriteRuleByName *rule =
				&PostgisAggregateRewriteRulesByName[ruleIndex];

			if (strcmp(aggregateName, rule->aggregateName) == 0)
			{
				return rule->rewriteAggregate;
			}
		}
	}

	return NULL;
}


/*
 * GetRewriteRuleForOperatorId returns the rewrite rule for an operator ID.
 * If the functionName is set, it should be called to rewrite the
 * operator expression.
 */
static RewriteRule *
GetRewriteRuleForOperatorId(Oid functionId)
{
	InitRewriteRules();

	bool		found = false;
	RewriteRule *rule =
		hash_search(RewriteRulesHash, &functionId, HASH_ENTER, &found);

	if (!found)
	{
		rule->rewriteFunction = GetOperatorRewriter(functionId);
		rule->extra.stringArg = GetOperatorRewriteFunctionName(functionId);
	}

	return rule;
}


/*
 * GetOperatorRewriter gets the function for rewriting an operator expression.
 */
static ExpressionRewriter
GetOperatorRewriter(Oid operatorId)
{
	Oid			schemaId = get_oper_namespace(operatorId);
	char	   *schemaName = get_namespace_name(schemaId);

	char	   *operatorName = get_oper_name(operatorId);
	size_t		ruleCount = ARRAY_SIZE(BuiltinOperatorRewriteRulesByName);

	for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
	{
		OperatorRewriteRuleByName *rule =
			&BuiltinOperatorRewriteRulesByName[ruleIndex];

		if (strcmp(schemaName, rule->schemaName) == 0 &&
			strcmp(operatorName, rule->operatorName) == 0)
		{
			return rule->rewriteOperator;
		}
	}

	if (IsExtensionCreated(Postgis) && schemaId == ExtensionSchemaId(Postgis))
	{
		ruleCount = ARRAY_SIZE(PostgisOperatorRewriteRulesByName);

		for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
		{
			OperatorRewriteRuleByName *rule =
				&PostgisOperatorRewriteRulesByName[ruleIndex];

			if (strcmp(operatorName, rule->operatorName) == 0)
			{
				return rule->rewriteOperator;
			}
		}
	}

	return NULL;
}

/*
 * GetOperatorRewriter gets the name of a function to be rewritten to.
 */
static char *
GetOperatorRewriteFunctionName(Oid operatorId)
{
	Oid			schemaId = get_oper_namespace(operatorId);
	char	   *schemaName = get_namespace_name(schemaId);

	char	   *operatorName = get_oper_name(operatorId);
	size_t		ruleCount = ARRAY_SIZE(BuiltinOperatorRewriteRulesByName);

	for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
	{
		OperatorRewriteRuleByName *rule =
			&BuiltinOperatorRewriteRulesByName[ruleIndex];

		if (strcmp(schemaName, rule->schemaName) == 0 &&
			strcmp(operatorName, rule->operatorName) == 0)
		{
			return rule->replacementFunctionName;
		}
	}

	if (IsExtensionCreated(Postgis) && schemaId == ExtensionSchemaId(Postgis))
	{
		ruleCount = ARRAY_SIZE(PostgisOperatorRewriteRulesByName);

		for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
		{
			OperatorRewriteRuleByName *rule =
				&PostgisOperatorRewriteRulesByName[ruleIndex];

			if (strcmp(operatorName, rule->operatorName) == 0)
			{
				return rule->replacementFunctionName;
			}
		}
	}

	return NULL;
}

/*
 * GetRewriteRuleForFunctionId returns the rewrite rule for a function ID.
 * IF the rewriteFunction is set, it should be called to rewrite the
 * function expression
 */
static RewriteRule *
GetRewriteRuleForFunctionId(Oid functionId)
{
	InitRewriteRules();

	bool		found = false;
	RewriteRule *rule =
		hash_search(RewriteRulesHash, &functionId, HASH_ENTER, &found);

	if (!found)
	{
		int			limitArgs = 0;

		rule->rewriteFunction = GetFunctionCallRewriter(functionId, &limitArgs);
		rule->extra.intArg = limitArgs;
	}

	return rule;
}


/*
 * GetFunctionCallRewriter gets the function for rewriting a function
 * call expression.
 */
static ExpressionRewriter
GetFunctionCallRewriter(Oid functionId, int *limitArgs)
{
	Oid			schemaId = get_func_namespace(functionId);
	char	   *schemaName = get_namespace_name(schemaId);

	char	   *functionName = get_func_name(functionId);
	size_t		ruleCount = ARRAY_SIZE(BuiltinFunctionCallRewriteRulesByName);

	for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
	{
		FunctionCallRewriteRuleByName *rule =
			&BuiltinFunctionCallRewriteRulesByName[ruleIndex];

		if (strcmp(schemaName, rule->schemaName) == 0 &&
			strcmp(functionName, rule->functionName) == 0)
		{
			if (limitArgs)
				*limitArgs = rule->limitArgs;

			return rule->rewriteFunction;
		}
	}

	if (IsExtensionCreated(Postgis) && schemaId == ExtensionSchemaId(Postgis))
	{
		ruleCount = ARRAY_SIZE(PostgisFunctionCallRewriteRulesByName);

		for (int ruleIndex = 0; ruleIndex < ruleCount; ruleIndex++)
		{
			FunctionCallRewriteRuleByName *rule =
				&PostgisFunctionCallRewriteRulesByName[ruleIndex];

			if (strcmp(functionName, rule->functionName) == 0)
			{
				if (limitArgs)
					*limitArgs = rule->limitArgs;

				return rule->rewriteFunction;
			}
		}
	}

	return NULL;
}


/*
 * RewriteFuncExprBtrim rewrites btrim to trim, which is an expression in Postgres,
 * but not an actual function.
 */
static Node *
RewriteFuncExprBtrim(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	int			argCount = list_length(funcExpr->args);

	if (argCount < 1 || argCount > 2)
		return node;

	Oid			argTypes[] = {TEXTOID, TEXTOID};
	List	   *trimName = list_make2(makeString(PG_LAKE_INTERNAL_NSP), makeString("trim"));

	funcExpr->funcid = LookupFuncName(trimName, argCount, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFunctionCallExpression rewrites FuncExpr and OpExpr expressions
 * if a rewrite rule is defined.
 */
static Node *
RewriteFunctionCallExpression(Oid functionId, Node *node, void *context)
{
	RewriteRule *rule = GetRewriteRuleForFunctionId(functionId);

	if (rule->rewriteFunction != NULL)
	{
		/* rewrite the function expression */
		((RewriteQueryTreeContext *) context)->funcIntArg = rule->extra.intArg;
		node = rule->rewriteFunction(node, context);
	}

	return node;
}


/*
 * RewriteFuncExprCast rewrites explicit calls to cast functions like date(..)
 * to actual casts, since DuckDB only supports the latter.
 */
static Node *
RewriteFuncExprCast(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* we only expect single argument cast functions here for now */
	if (list_length(funcExpr->args) != 1)
		elog(ERROR, "unexpected number of arguments for function %d", funcExpr->funcid);

	/* change into an explicit cast */
	funcExpr->funcformat = COERCE_EXPLICIT_CAST;

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprExtract rewrites extract(..) function calls into date_part(..)
 * function calls because during deparsing we emit an extract('field', time) function
 * call, but DuckDB only accepts the extract('field', time) syntax. Luckily, date_part
 * does the same thing as extract, so we only need to rewrite the function OID.
 */
static Node *
RewriteFuncExprExtract(Node *node, void *context)
{
	const int	argCount = 2;

	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* be defensive about signature (mostly for future PG versions) */
	if (list_length(funcExpr->args) != argCount)
		elog(ERROR, "unexpected number of arguments for extract");

	Node	   *timeArg = lsecond(funcExpr->args);

	/* there are multiple definitions of date_part for different time types */
	List	   *datePartName = list_make2(makeString("pg_catalog"),
										  makeString("date_part"));
	Oid			argTypes[] = {TEXTOID, exprType(timeArg)};

	/* set the function OID to the equivalent date_part OID */
	funcExpr->funcid =
		LookupFuncName(datePartName, argCount, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprDateBin rewrites date_bin(..) function calls into time_bucket(..)
 * function calls.
 */
static Node *
RewriteFuncExprDateBin(Node *node, void *context)
{
	const int	argCount = 3;

	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* be defensive about signature (mostly for future PG versions) */
	if (list_length(funcExpr->args) != argCount)
		elog(ERROR, "unexpected number of arguments for date_bin");

	Node	   *timeArg = lsecond(funcExpr->args);
	Node	   *originArg = lthird(funcExpr->args);

	/* there are multiple definitions of date_part for different time types */
	List	   *datePartName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										  makeString("time_bucket"));
	Oid			argTypes[] = {INTERVALOID, exprType(timeArg), exprType(originArg)};

	/* set the function OID to the equivalent date_bin OID */
	funcExpr->funcid =
		LookupFuncName(datePartName, argCount, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprDateTrunc rewrites date_trunc(..) function calls to add
 * an explicit cast to the return value, because DuckDB may use a different
 * return value depending on the value of the first argument.
 */
static Node *
RewriteFuncExprDateTrunc(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* be defensive about signature (mostly for future PG versions) */
	if (list_length(funcExpr->args) < 2)
		elog(ERROR, "unexpected number of arguments for date_bin");

	Node	   *fieldArg = linitial(funcExpr->args);
	Node	   *timeArg = lsecond(funcExpr->args);

	/*
	 * Some fun weirdness. DuckDB starts the century and millennium at
	 * 2000-01-01 while PostgreSQL starts at 2001-01-01. We assume that in the
	 * vast majority of cases date_trunc is used with a constant text field,
	 * such that we can rewrite it to: date_trunc('century', ...) + interval
	 * '1 year'
	 *
	 * Longer term, we should perhaps provide our own implementation in the
	 * DuckDB extension, although DuckDB is not self-consistent.
	 * extract(millennium from '2000-02-01') returns 2 instead of 3, so
	 * extract does start the millennium at 2001. This may be a bug that will
	 * be fixed in a future release (in either direction. We will be covered
	 * by tests in that case.
	 */
	if (IsA(fieldArg, Const))
	{
		Const	   *fieldConst = (Const *) fieldArg;

		/* we only try to deal with text */
		if (fieldConst->consttype == TEXTOID && !fieldConst->constisnull)
		{
			char	   *fieldText = TextDatumGetCString(fieldConst->constvalue);

			if (strcasecmp(fieldText, "millennium") == 0 ||
				strcasecmp(fieldText, "century") == 0)
			{
				node = AddOneYear(node);
			}
		}
	}

	/*
	 * DuckDB has the ability to adapt its return type to the value of the
	 * arguments, and will return date for certain fields, which can cause
	 * issues in comparison. We make the behaviour similar to PostgreSQL by
	 * explicitly casting to the type of the argument (e.g. timestamp,
	 * timestamptz).
	 */
	RelabelType *coerceExpr = makeNode(RelabelType);

	coerceExpr->arg = (Expr *) node;
	coerceExpr->resulttype = exprType(timeArg);
	coerceExpr->resulttypmod = exprTypmod(node);
	coerceExpr->resultcollid = exprCollation(node);
	coerceExpr->relabelformat = COERCE_EXPLICIT_CAST;
	coerceExpr->location = -1;

	return (Node *) coerceExpr;
}


/*
 * RewriteFuncExprDiv rewrites div(..) function calls into fdiv(..) function calls.
 */
static Node *
RewriteFuncExprDiv(Node *node, void *context)
{
	const int	argCount = 2;

	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* be defensive about signature (mostly for future PG versions) */
	if (list_length(funcExpr->args) != argCount)
		elog(ERROR, "unexpected number of arguments for div");

	List	   *fdivName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
									  makeString("fdiv"));
	Oid			argTypes[] = {NUMERICOID, NUMERICOID};

	/* set the function OID to the fdiv OID */
	funcExpr->funcid =
		LookupFuncName(fdivName, argCount, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprMod rewrites mod(..) function calls into fmod(..) function calls.
 */
static Node *
RewriteFuncExprMod(Node *node, void *context)
{
	const int	argCount = 2;

	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* be defensive about signature (mostly for future PG versions) */
	if (list_length(funcExpr->args) != argCount)
		elog(ERROR, "unexpected number of arguments for mod");

	List	   *fmodName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
									  makeString("fmod"));
	Oid			argTypes[] = {NUMERICOID, NUMERICOID};

	/* set the function OID to the fmod OID */
	funcExpr->funcid =
		LookupFuncName(fmodName, argCount, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprNow rewrites now(..) function calls into __lake_now()
 * for later search & replace.
 */
static Node *
RewriteFuncExprNow(Node *node, void *context)
{
	const int	argCount = 0;
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	if (list_length(funcExpr->args) != argCount)
		return node;

	List	   *nowName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
									 makeString(PG_LAKE_NOW_TEMPLATE));

	/* set the function OID to the __lake_now OID */
	funcExpr->funcid =
		LookupFuncName(nowName, argCount, NULL, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprSubstring rewrites substring(..) and substr(...) function calls
 * into substring_pg(..) function calls.
 */
static Node *
RewriteFuncExprSubstring(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	const int	argCount = list_length(funcExpr->args);

	/* be defensive about signature (mostly for future PG versions) */
	if (!(argCount == 2 || argCount == 3))
		elog(ERROR, "unexpected number of arguments for substring");

	/*
	 * there are multiple definitions of substring for different parameter
	 * types
	 */
	List	   *substringPg = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										 makeString("substring_pg"));

	if (argCount == 2)
	{
		Oid			argTypesWithOffset[] = {TEXTOID, INT4OID};

		funcExpr->funcid =
			LookupFuncName(substringPg, argCount, argTypesWithOffset, false);
	}
	else
	{
		Oid			argTypesWithOffsetAndLength[] = {TEXTOID, INT4OID, INT4OID};

		funcExpr->funcid =
			LookupFuncName(substringPg, argCount, argTypesWithOffsetAndLength, false);
	}

	return (Node *) funcExpr;
}


static Node *
RewriteFuncExprGenerateSeries(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	const int	argCount = list_length(funcExpr->args);

	if (argCount != 2 && argCount != 3)
	{
		/* we are not interested in others */
		return node;
	}

	/*
	 * We are only interested in generate_series(int, int) and
	 * generate_series(int, int, int). DuckDB has the equivalent all of other
	 * generate_series() functions that we pushdown (see
	 * shippable_builtin_functions.c).
	 *
	 * For the int versions, we rewrite to our custom generate_series_int and
	 * generate_series_int_step functions, otherwise we'd implicitly produce
	 * bigints instead of ints., which would silently cause divergent data
	 * types between the data files and the tables.
	 */
	Node	   *firstArg = linitial(funcExpr->args);

	if (argCount == 2 && exprType(firstArg) == INT4OID)
	{
		List	   *generateSeriesName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
													makeString("generate_series_int"));

		Oid			argTypes[] = {INT4OID, INT4OID};

		funcExpr->funcid =
			LookupFuncName(generateSeriesName, argCount, argTypes, false);

		return (Node *) funcExpr;
	}
	else if (argCount == 3 && exprType(firstArg) == INT4OID)
	{
		List	   *generateSeriesName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
													makeString("generate_series_int_step"));

		Oid			argTypes[] = {INT4OID, INT4OID, INT4OID};

		funcExpr->funcid =
			LookupFuncName(generateSeriesName, argCount, argTypes, false);

		return (Node *) funcExpr;
	}

	return node;
}



/*
 * RewriteFuncExprToChar rewrites to_char(timestamp,text) function calls
 * to strftime chains.
 */
static Node *
RewriteFuncExprToChar(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	const int	argCount = list_length(funcExpr->args);

	if (argCount != 2)
		elog(ERROR, "unexpected number of arguments for to_char");

	bool		checkOnly = false;
	List	   *chain = NIL;

	/* create a chain of expressions to be concatenated */
	if (!BuildStrftimeChain(funcExpr, checkOnly, &chain))
		elog(ERROR, "unsupported format specifier");

	List	   *nameList = list_make2(makeString("pg_catalog"), makeString("||"));
	Operator	operatorTuple = oper(NULL, nameList, TEXTOID, TEXTOID, false, -1);
	Form_pg_operator operator = (Form_pg_operator) GETSTRUCT(operatorTuple);

	ListCell   *toCharPartCell = NULL;
	Node	   *concatExpr = NULL;

	foreach(toCharPartCell, chain)
	{
		Node	   *toCharPart = lfirst(toCharPartCell);

		if (concatExpr == NULL)
		{
			concatExpr = toCharPart;
		}
		else
		{
			OpExpr	   *opExpr = makeNode(OpExpr);

			opExpr->opno = operator->oid;
			opExpr->opfuncid = operator->oprcode;
			opExpr->opresulttype = operator->oprresult;
			opExpr->args = list_make2(concatExpr, toCharPart);

			concatExpr = (Node *) opExpr;
		}
	}

	ReleaseSysCache(operatorTuple);

	return (Node *) concatExpr;
}


/*
 * RewriteFuncExprJsonbArrayLength rewrites jsonb_array_length to json_array_length.
 */
static Node *
RewriteFuncExprJsonbArrayLength(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	const int	argCount = list_length(funcExpr->args);

	if (argCount != 1)
		return node;

	Oid			argTypes[] = {JSONBOID};
	List	   *funcName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
									  makeString("json_array_length"));

	funcExpr->funcid = LookupFuncName(funcName, argCount, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprEncode rewrites encode(bytea, 'base64|hex') function calls into
 * to_base64(bytea) or to_hex(bytea) function calls.
 */
static Node *
RewriteFuncExprEncode(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	const int	argCount = list_length(funcExpr->args);

	if (argCount != 2)
		return node;

	Node	   *formatArg = lsecond(funcExpr->args);

	/* we only handle base64 encoding for now */
	if (!IsA(formatArg, Const))
		return node;

	Const	   *formatConst = (Const *) formatArg;

	if (formatConst->constisnull ||
		formatConst->consttype != TEXTOID)
	{
		return node;
	}

	char	   *formatText = TextDatumGetCString(formatConst->constvalue);

	if (strcasecmp(formatText, "base64") != 0 && strcasecmp(formatText, "hex") != 0)
		return node;

	/* rewrite to to_base64(bytea) */
	List	   *funcName;

	if (strcasecmp(formatText, "base64") == 0)
	{
		funcName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
							  makeString("to_base64"));
	}
	else
	{
		funcName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
							  makeString("to_hex"));
	}

	Oid			argTypes[] = {BYTEAOID};

	funcExpr->funcid =
		LookupFuncName(funcName, 1, argTypes, false);

	/* remove the second argument */
	funcExpr->args = list_make1(linitial(funcExpr->args));

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprDecode rewrites decode(text, 'base64|hex') function calls into
 * from_base64(text) or from_hex(text) function calls.
 */
static Node *
RewriteFuncExprDecode(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	const int	argCount = list_length(funcExpr->args);

	if (argCount != 2)
		return node;

	Node	   *formatArg = lsecond(funcExpr->args);

	/* we only handle base64 decoding for now */
	if (!IsA(formatArg, Const))
		return node;

	Const	   *formatConst = (Const *) formatArg;

	if (formatConst->constisnull ||
		formatConst->consttype != TEXTOID)
	{
		return node;
	}

	char	   *formatText = TextDatumGetCString(formatConst->constvalue);

	if (strcasecmp(formatText, "base64") != 0 && strcasecmp(formatText, "hex") != 0)
		return node;

	/* rewrite to from_base64(text) */
	List	   *funcName;

	if (strcasecmp(formatText, "base64") == 0)
	{
		funcName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
							  makeString("from_base64"));
	}
	else
	{
		funcName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
							  makeString("from_hex"));
	}

	Oid			argTypes[] = {TEXTOID};

	funcExpr->funcid =
		LookupFuncName(funcName, 1, argTypes, false);

	/* remove the second argument */
	funcExpr->args = list_make1(linitial(funcExpr->args));

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprMapExtract rewrites map_type.extract(..) function calls
 * into remote map_extract(map,key)[1] expressions
 */
static Node *
RewriteFuncExprMapExtract(Node *node, void *context)
{
	const int	argCount = 2;
	FuncExpr   *funcExpr;

	/* check/validate if this is a differently-named operator or a map type */
	if (IsA(node, OpExpr))
	{
		OpExpr	   *operExpr = castNode(OpExpr, node);

		if (list_length(operExpr->args) != argCount)
			return node;

		/*
		 * The first argument should be the map type; validate the type is a
		 * map type otherwise skip this rewrite.  Since '->' is a valid
		 * operator for other types, we need to confirm we're not dealing with
		 * JSON or something else.
		 */

		Node	   *mapArg = linitial(operExpr->args);

		if (!IsMapTypeOid(exprType(mapArg)))
			return node;

		/*
		 * Now let's generate the same thing, but as a function for later
		 * handling.
		 */
		funcExpr = ConvertOpExprToFuncExpr(operExpr);
	}
	else
	{
		/* We were originally a function, so just verify arg count. */
		funcExpr = castNode(FuncExpr, node);

		if (list_length(funcExpr->args) != argCount)
			return node;
	}

	/*
	 * Whether or not we were called as an operator or a function, we are
	 * going to create a remote function node and dereference appropriately.
	 */

	List	   *mapExtractName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											makeString("map_extract"));

	/* polymorphic function signature */
	Oid			funcTypes[2] = {ANYOID, ANYOID};

	/* set the function OID to the "map_extract" function OID */
	funcExpr->funcid =
		LookupFuncName(mapExtractName, argCount, funcTypes, false);

	/*
	 * The upstream "map_extract" function signature changed; it had
	 * previously returned the looked-up value, or NULL if it was not found.
	 * Prior to the DuckDB 1.0 release, it changed to returning a list of 0 or
	 * 1 elements. (I assume this was so the user could distinguish between a
	 * "not found" or a literal NULL value.)
	 *
	 * From a pushdown-perspective, it's easier to keep the old interpretation
	 * so we could push down expressions like:
	 *
	 * SELECT * FROM mytable WHERE mymap->'key' = 'a';
	 *
	 * ...which would get transformed into the entirely pushdownable query:
	 *
	 * SELECT * FROM mytable WHERE map_extract(mymap, 'key')[1] = 'a';
	 *
	 * This results in the user not needing to know the details of how this
	 * works, it just gets the results as expected.  (DuckDB has some odd
	 * handling of NULL, where all NULLs are considered equal, not NULL, so
	 * this differs from Postgres.)
	 */

	return DereferenceIndex((Node *) funcExpr, 1);
}


/*
 * RewriteOperatorExpression() - replace an operator with a remote function
 * call or perform other semantic substitutions.
 *
 * In the simple case, there is a direct textual replacement of the
 * function name, which gets called with the same parameters as the operator
 * itself.
 *
 * In more complex cases, an ExpressionRewriter hook can be provided for
 * additional control over how this rewrite happens.
 */
static Node *
RewriteOperatorExpression(OpExpr *opExpr, void *context)
{
	RewriteRule *rule = GetRewriteRuleForOperatorId(opExpr->opno);
	Node	   *node = (Node *) opExpr;

	/*
	 * We rewrite OpExpr with FieldSelect as LHS to its commutator due to
	 * performance issues when pushing down to (eventually) read_parquet().
	 *
	 * The short explanation here (#445) is that FieldSelect on the LHS will
	 * not push down to the read_parquet() call if there is a cast on the RHS,
	 * but commuting the operator **does** push down to read_parquet() which
	 * is more efficient.  (Wut?)  This is a DuckDB quirk for sure, but
	 * important enough for spatial workloads that we want to keep this
	 * working.
	 *
	 * We could potentially loosen the restriction here for a boolean return
	 * value, but the major use case is for '<' and '>' operators where we
	 * know that the commutator has the same meaning.  If we find that there
	 * are other operators without bool return values that are also compared,
	 * then we can be less restrictive, but let's start by being cautious
	 * about pushdown/transformation.
	 */
	if (list_length(opExpr->args) == 2 &&
		nodeTag(linitial(opExpr->args)) == T_FieldSelect &&
		opExpr->opresulttype == BOOLOID)
	{
		node = (Node *) CommutateOpExpr(opExpr);
	}
	else if (rule->rewriteFunction != NULL)
	{
		/* rewrite the function expression */
		node = rule->rewriteFunction(node, context);
	}
	else if (rule->extra.stringArg != NULL)
	{
		node = FindCompatibleFunctionWithName(opExpr, rule->extra.stringArg);
	}

	/*
	 * Another DuckDB quirk is that <Var float4> <op> <Const float8>
	 * comparisons do not push down into read_parquet(), so we try to downcast
	 * the Const when possible.
	 *
	 * Most of the time this function just returns the node immediately.
	 */
	node = DowncastFloatConstInOpExpr(node);

	return node;
}


/*
 * RewriteAggregateExpression() - replace an aggregate with a different
 * aggregate.
 *
 * Note that if the aggregate rewriter validates the number of arguments you
 * will need to check for non-resjunk columns using the CountNonJunkColumns()
 * routine instead of just looking at the list_length(aggref->args).
 *
 * This is because aggregates with internal ORDER BY or other constructs can
 * add resjunk columns to the args list, as in this query:
 *
 *     SELECT ST_Union(mygeom ORDER BY othercol);
 *
 * This results in two args in Aggref->args instead of just the TargetEntry
 * for the field.  See RewriteFuncExprPostgisUnionAgg() for an example.
 */
static Node *
RewriteAggregateExpression(Aggref *aggExpr, void *context)
{
	RewriteRule *rule = GetRewriteRuleForAggregateId(aggExpr->aggfnoid);
	Node	   *node = (Node *) aggExpr;

	if (rule->rewriteFunction != NULL)
	{
		/* rewrite the function expression */
		node = rule->rewriteFunction(node, context);
	}
	return node;
}


/*
 * This function searches for a function of the given name that matches the
 * argument types for the passed-in operator.  The point here is to be able to
 * transform an operator on the Postgres side into a corresponding function call
 * at the remote side, basically mapping the operator to a different name.
 *
 * Since the AST needs to be valid on both Pg and DuckDB sides, this function
 * must exist on the Postgres side of things, even if it ends up being a
 * compatibility function in the __lake__internal__nsp__ schema.
 *
 * If we cannot find a candidate, then we return InvalidOid.  The rewrite of the
 * tree will then be skipped, which means that the remote query will presumably
 * fail since we did not do the mapping, so perhaps we should error on this
 * side--really the right answer here is to catch this in the shippability
 * check, since we shouldn't be pushing down something that we expect to map
 * successfully.
 */
static Node *
FindCompatibleFunctionWithName(OpExpr *opExpr, char *funcname)
{
	Assert(opExpr && funcname);

	FuncExpr   *funcExpr = ConvertOpExprToFuncExpr(opExpr);

	/* we need the args of the operator */
	int			numArgs = 0;	/* 1 or 2 */
	Oid			listArgs[2] = {InvalidOid, InvalidOid};

	ListCell   *cell;

	foreach(cell, opExpr->args)
	{
		Var		   *argument = (Var *) lfirst(cell);

		/* prevent stack overwrite */
		if (numArgs >= 2)
			ereport(ERROR, (errmsg("unexpected number of arguments to operator (expect 1 or 2)"),
							errcode(ERRCODE_INTERNAL_ERROR)));
		listArgs[numArgs] = exprType((Node *) argument);
		numArgs++;
	}

	/* need this check as well */
	if (numArgs < 1 || numArgs > 2)
		ereport(ERROR, (errmsg("unexpected number of arguments to operator (expect 1 or 2)"),
						errcode(ERRCODE_INTERNAL_ERROR)));

	/* Use the search path as the operator to find */
	List	   *funcNameList = list_make2(
										  makeString(get_namespace_name(get_oper_namespace(opExpr->opno))),
										  makeString(funcname));

	/* TODO: search path or explicit qualification?  fallback to same schema? */
	Oid			foundFunction = LookupFuncName(
											   funcNameList,
											   numArgs,
											   listArgs,
											   true);

	/*
	 * if we didn't find anything, we'll just fallthrough to no rewrite
	 */

	if (foundFunction != InvalidOid)
	{
		/*
		 * The only thing that needs to change is our underlying function;
		 * args, return, etc, are all the same.
		 */

		funcExpr->funcid = foundFunction;
		return (Node *) funcExpr;
	}

	ereport(DEBUG1,
			(errmsg("skipping unmappable operator pushdown func name: "
					"%s", funcname)));

	return (Node *) opExpr;
}

/*
 * AddOneYear constructs a <node> + interval '1 year' expression.
 */
static Node *
AddOneYear(Node *node)
{
	Interval   *yearInterval = (Interval *) palloc0(sizeof(Interval));

	yearInterval->month = 12;

	return MakeAddIntervalExpr(node, yearInterval);
}


/*
 * DereferenceIndex() constructs a <node>[<index>] array dereference for use in
 * pushing down a transparent subscript when transforming a DuckDB map lookup
 * result.
 *
 * Since this is used only for our DuckDB AST and not for use on the Postgres
 * side, we can get away with sort of misusing this node by not setting the
 * refelemtype or refcontainertype fields, since it is not (necessarily) an
 * array.  (Which, we should validate what happens if we *do* get an array type as
 * exprType(node).)
 *
 * For the purposes of handling this, we want to treat the type of the
 * dereference as the same type as the passed-in Node.  This ensures that
 * exprType(node) == exprType(<return>) so ORDER BY and the like will work as
 * expected with the returned values here.
 */
static Node *
DereferenceIndex(Node *node, int index)
{
	SubscriptingRef *sbsref;
	Const	   *indexConst;
	List	   *indexList;

	/* Create a constant node for the index */
	indexConst = makeConst(INT4OID, /* type of the constant */
						   -1,	/* type modifier (none) */
						   InvalidOid,	/* collation (none) */
						   sizeof(int32),	/* size of the constant */
						   Int32GetDatum(index),	/* value of the constant */
						   false,	/* isnull */
						   true);	/* byval */

	/* Create a list with the index constant */
	indexList = list_make1(indexConst);

	/* Create a SubscriptingRef node */
	sbsref = makeNode(SubscriptingRef);

	sbsref->refrestype = exprType(node);
	sbsref->reftypmod = -1;		/* typmod (none) */
	sbsref->refcollid = exprCollation(node);	/* collation */
	sbsref->refupperindexpr = indexList;	/* index expression */
	sbsref->reflowerindexpr = NIL;	/* lower index (none) */
	sbsref->refexpr = (Expr *) node;	/* the array expression */
	sbsref->refassgnexpr = NULL;	/* assignment expression (none) */

	return (Node *) sbsref;
}


/*
 * Utility function to commute an OpExpr if a corresponding commutator operator exists.
 */
static OpExpr *
CommutateOpExpr(OpExpr *opExpr)
{
	Oid			commutator;

	/* This is checked by caller, so assert here */
	Assert(list_length(opExpr->args) == 2);

	commutator = get_commutator(opExpr->opno);

	/* Skip missing or self-commutators */
	if (!OidIsValid(commutator) || commutator == opExpr->opno)
		return opExpr;

	/*
	 * The whole context of this is a mutable tree, so just change in-place.
	 * Same result type, collation, etc, so we don't care about anything but
	 * opno, funcid and arg order.
	 */
	opExpr->opno = commutator;
	opExpr->opfuncid = InvalidOid;	/* deparse only, no needed implementation */

	/* swap the argument order as well */
	opExpr->args = list_make2(list_nth(opExpr->args, 1),
							  list_nth(opExpr->args, 0));
	return opExpr;
}


/*
 * Utility function to convert an OpExpr into the corresponding FuncExpr.
 */
static FuncExpr *
ConvertOpExprToFuncExpr(OpExpr *opExpr)
{
	/* Find the function OID for the operator */
	Oid			funcOid;
	HeapTuple	tuple;

	/* This lookup is required because opExpr might be a shell operator */

	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(opExpr->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", opExpr->opno);

	funcOid = ((Form_pg_operator) GETSTRUCT(tuple))->oprcode;
	ReleaseSysCache(tuple);

	/* Create a new FuncExpr node */
	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = funcOid;
	funcExpr->funcresulttype = opExpr->opresulttype;
	funcExpr->funcretset = false;
	funcExpr->funcvariadic = false;
	funcExpr->funcformat = COERCE_EXPLICIT_CALL;
	funcExpr->location = opExpr->location;

	/* Copy the arguments from the OpExpr to the FuncExpr */
	funcExpr->args = opExpr->args;

	return funcExpr;
}


/*
 * RewriteFuncExprPad() takes a 2-arg lpad() or rpad() function and rewrites to
 * a 3-arg form with an implied ' ' third argument.  This is because DuckDB does
 * not have the 2-arg forms.
 */
static Node *
RewriteFuncExprPad(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	if (list_length(funcExpr->args) != 2)
		return node;

	/* only for these specific invocations */
	if (funcExpr->funcid != F_LPAD_TEXT_INT4 && funcExpr->funcid != F_RPAD_TEXT_INT4)
		return node;

	/* replace the 2-arg form oid with the 3-arg form oid */
	funcExpr->funcid = (funcExpr->funcid == F_LPAD_TEXT_INT4
						? F_LPAD_TEXT_INT4_TEXT
						: F_RPAD_TEXT_INT4_TEXT);

	Const	   *newConst = MakeStringConst(" ");

	funcExpr->args = lappend(funcExpr->args, newConst);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprArrayLength rewrites array_length(anyarray,int) to
 * nullif(array_length(anyarray, 1), 0) to match PostgreSQL's NULL behaviour
 * for empty arrays.
 */
static Node *
RewriteFuncExprArrayLength(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	if (list_length(funcExpr->args) != 2 || funcExpr->funcid != F_ARRAY_LENGTH)
		return node;

	/* wrap in nullif(..., 0) to cancel out 0 value */
	NullIfExpr *nullifExpr = makeNode(NullIfExpr);

	nullifExpr->args = list_make2(funcExpr, MakeIntConst(0));

	return (Node *) nullifExpr;
}


/*
 * RewriteFuncExprCardinality rewrites cardinality(anyarray) to array_length.
 */
static Node *
RewriteFuncExprCardinality(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	if (list_length(funcExpr->args) != 1 || funcExpr->funcid != F_CARDINALITY)
		return node;

	/* replace with array_length(anyarray, 1) */
	funcExpr->funcid = F_ARRAY_LENGTH;
	funcExpr->args = lappend(funcExpr->args, MakeIntConst(1));

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprTrigonometry rewrites several trigonometry
 * function calls by adding a radians(..) call.
 */
static Node *
RewriteFuncExprTrigonometry(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* find the no degrees variant */
	switch (funcExpr->funcid)
	{
		case F_COSD:
			funcExpr->funcid = F_COS;
			break;

		case F_COTD:
			funcExpr->funcid = F_COT;
			break;

		case F_SIND:
			funcExpr->funcid = F_SIN;
			break;

		case F_TAND:
			funcExpr->funcid = F_TAN;
			break;

		default:
			elog(ERROR, "unexpected function ID in rewrite %d", funcExpr->funcid);
	}

	FuncExpr   *radiansExpr = makeNode(FuncExpr);

	radiansExpr->funcid = F_RADIANS;
	radiansExpr->funcresulttype = funcExpr->funcresulttype;
	radiansExpr->funcretset = false;
	radiansExpr->funcvariadic = false;
	radiansExpr->funcformat = COERCE_EXPLICIT_CALL;
	radiansExpr->location = -1;
	radiansExpr->args = funcExpr->args;

	funcExpr->args = list_make1((Node *) radiansExpr);
	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprInverseTrigonometry rewrites several inverse
 * trigonometry function calls by adding a degrees(..) call.
 */
static Node *
RewriteFuncExprInverseTrigonometry(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* find the no degrees variant */
	switch (funcExpr->funcid)
	{
		case F_ACOSD:
			funcExpr->funcid = F_ACOS;
			break;

		case F_ASIND:
			funcExpr->funcid = F_ASIN;
			break;

		case F_ATAND:
			funcExpr->funcid = F_ATAN;
			break;

		case F_ATAN2D:
			funcExpr->funcid = F_ATAN2;
			break;

		default:
			elog(ERROR, "unexpected function ID in rewrite %d", funcExpr->funcid);
	}

	FuncExpr   *degreesExpr = makeNode(FuncExpr);

	degreesExpr->funcid = F_DEGREES;
	degreesExpr->funcresulttype = funcExpr->funcresulttype;
	degreesExpr->funcretset = false;
	degreesExpr->funcvariadic = false;
	degreesExpr->funcformat = COERCE_EXPLICIT_CALL;
	degreesExpr->location = -1;
	degreesExpr->args = list_make1(node);

	return (Node *) degreesExpr;
}


/*
 * RewriteFuncExprHyperbolic rewrites acosh(..) and atanh(...) function calls
 * into acosh_pg(..) and atanh_pg(..) function calls.
 */
static Node *
RewriteFuncExprHyperbolic(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	List *funcName;

	switch (funcExpr->funcid)
	{
		case F_ACOSH:
			funcName = list_make2(makeString(PG_LAKE_INTERNAL_NSP), makeString("acosh_pg"));
			break;
		
		case F_ATANH:
			funcName = list_make2(makeString(PG_LAKE_INTERNAL_NSP), makeString("atanh_pg"));
			break;

		default:
			elog(ERROR, "unexpected function ID in rewrite %d", funcExpr->funcid);
	}

	Oid argTypes[] = {FLOAT8OID};
	int argCount = 1;
	funcExpr->funcid = LookupFuncName(funcName, argCount, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprPostgisBytea rewrites bytea(geometry) function calls into
 * ST_AsWKB(..) function calls to push down (implicit) casts.
 */
static Node *
RewriteFuncExprPostgisBytea(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	List	   *args = funcExpr->args;

	/* be defensive about signature */
	if (list_length(args) > 2 || list_length(args) == 0)
	{
		return node;
	}
	else if (list_length(args) == 2)
	{
		/*
		 * Truncate second argument, we should only get here if the constant
		 * matches our endianness.
		 */
		args = list_make1(linitial(args));
	}

	Node	   *geomArg = linitial(funcExpr->args);

	/* check whether the function has a geometry argument */
	if (exprType(geomArg) != GeometryTypeId())
		return node;

	/* replace the function ID with one that deparses to st_aswkb */
	funcExpr->funcid = InternalAsWKBFunctionId();

	/* treat it as a regular function call rather than a cast */
	funcExpr->funcformat = COERCE_EXPLICIT_CALL;

	/* truncate the arguments of ST_AsBinary(geometry,text) if needed */
	funcExpr->args = args;

	/*
	 * Add an explicit cast to our expected return type of bytea, since
	 * ST_AsWKB in duckdb_spatial returns wkb_blob which can be cast to bytea.
	 */
	RelabelType *coerceExpr = makeNode(RelabelType);

	coerceExpr->arg = (Expr *) funcExpr;
	coerceExpr->resulttype = exprType(node);
	coerceExpr->resulttypmod = exprTypmod(node);
	coerceExpr->resultcollid = exprCollation(node);
	coerceExpr->relabelformat = COERCE_EXPLICIT_CAST;
	coerceExpr->location = -1;

	return (Node *) coerceExpr;
}


/*
 * RewriteFuncExprToZeroOrNullConst rewrites any function call into a 0 constant,
 * though it preserves NULL.
 *
 * This is mainly used for PostGIS functions that do not exist in DuckDB, but if
 * they did could only return 0 (st_srid, st_zmflag).
 *
 * Currently this only works for single argument functions that return int2 or int4.
 */
static Node *
RewriteFuncExprToZeroOrNullConst(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	Assert(list_length(funcExpr->args) == 1);

	Oid			resultType = funcExpr->funcresulttype;
	int			typeLength = resultType == INT4OID ? sizeof(int32) : sizeof(int16);
	Datum		zeroDatum = resultType == INT4OID ? Int32GetDatum(0) : Int16GetDatum(0);

	/* node IS NULL check */
	NullTest   *isNullCheck = makeNode(NullTest);

	isNullCheck->arg = (Expr *) linitial(funcExpr->args);
	isNullCheck->nulltesttype = IS_NULL;
	isNullCheck->argisrow = false;
	isNullCheck->location = -1;

	/* NULL return */
	Const	   *nullConst = makeNode(Const);

	nullConst->consttype = resultType;
	nullConst->consttypmod = -1;
	nullConst->constlen = typeLength;
	nullConst->constvalue = zeroDatum;
	nullConst->constbyval = true;
	nullConst->constisnull = true;
	nullConst->location = -1;

	/* CASE WHEN node IS NULL THEN NULL */
	CaseWhen   *caseWhenExpr = makeNode(CaseWhen);

	caseWhenExpr->expr = (Expr *) isNullCheck;
	caseWhenExpr->result = (Expr *) nullConst;
	caseWhenExpr->location = -1;

	/* 0 return */
	Const	   *zeroConst = makeNode(Const);

	zeroConst->consttype = funcExpr->funcresulttype;
	zeroConst->consttypmod = -1;
	zeroConst->constlen = typeLength;
	zeroConst->constvalue = zeroDatum;
	zeroConst->constbyval = true;
	zeroConst->constisnull = false;
	zeroConst->location = -1;

	/* CASE WHEN node IS NULL THEN NULL ELSE 0 */
	CaseExpr   *caseExpr = makeNode(CaseExpr);

	caseExpr->args = list_make1(caseWhenExpr);
	caseExpr->defresult = (Expr *) zeroConst;
	caseExpr->casetype = resultType;
	caseExpr->casecollid = funcExpr->funccollid;

	return (Node *) caseExpr;
}


/*
 * RewriteFuncExprPostgisTransform rewrites ST_Transform(geometry,text,text) function
 * calls into ST_Transform(geometry,text,text,bool) function calls with the always_xy
 * argument set to true to align with PostGIS behaviour.
 */
static Node *
RewriteFuncExprPostgisTransform(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/*
	 * only the 3 argument version is shippable, so we should not get here
	 * otherwise
	 */
	Assert(list_length(funcExpr->args) == 3);

	/* replace the function ID with the 4-argument skeleton */
	funcExpr->funcid = InternalTransformFunctionId();

	/* add always_xy argument with value true */
	bool		isNull = false;
	Node	   *trueConst = makeBoolConst(true, isNull);

	funcExpr->args = lappend(funcExpr->args, trueConst);

	return node;
}


/*
 * RewriteFuncExprPostgisTransformGeometry rewrites postgis_transform_geometry function
 * calls into ST_Transform(geometry,text,text,bool) function calls with the always_xy
 * argument set to true to align with PostGIS behaviour.
 *
 * We have this extra rule because ST_Transform is defined as a SQL function that
 * calls postgis_transform_geometry and may get inlined.
 */
static Node *
RewriteFuncExprPostgisTransformGeometry(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/*
	 * only the 4 argument version is shippable, so we should not get here
	 * otherwise
	 */
	Assert(list_length(funcExpr->args) == 4);

	/* replace the function ID with the 4-argument skeleton */
	funcExpr->funcid = InternalTransformFunctionId();

	/*
	 * We ignore the last argument, because the only version that can get here
	 * passes in 0.
	 */
	funcExpr->args = list_delete_last(funcExpr->args);

	/* add always_xy argument with value true */
	bool		isNull = false;
	Node	   *trueConst = makeBoolConst(true, isNull);

	funcExpr->args = lappend(funcExpr->args, trueConst);

	return node;
}


/*
 * RewriteFuncExprPostgisGeometryTypeFunction rewrites geometrytype(geometry) function
 * calls into st_geometrytype(geometry) function calls.
 */
static Node *
RewriteFuncExprPostgisGeometryTypeFunction(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* replace the function ID */
	funcExpr->funcid = InternalGeometryTypeFunctionId();

	/* add explicit cast to text because underlying function returns an enum */
	RelabelType *coerceExpr = makeNode(RelabelType);

	coerceExpr->arg = (Expr *) funcExpr;
	coerceExpr->resulttype = TEXTOID;
	coerceExpr->resulttypmod = -1;
	coerceExpr->resultcollid = funcExpr->funccollid;
	coerceExpr->relabelformat = COERCE_EXPLICIT_CAST;
	coerceExpr->location = -1;

	return (Node *) coerceExpr;
}


/*
 * RewriteFuncExprPostgisGeometryFromText rewrites ST_GeometryFromText(text) function
 * calls into ST_GeomFromText(..) function calls.
 */
static Node *
RewriteFuncExprPostgisGeometryFromText(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/*
	 * only the 1 argument version is shippable, so we should not get here
	 * otherwise
	 */
	Assert(list_length(funcExpr->args) == 1);

	Node	   *geomArg PG_USED_FOR_ASSERTS_ONLY = linitial(funcExpr->args);

	/* only the text version is shippable, so we should not get here otherwise */
	Assert(exprType(geomArg) == TEXTOID);

	/* replace the function ID with st_geomfromtext */
	funcExpr->funcid = ST_GeomFromTextFunctionId();

	return node;
}


/*
 * RewriteOpExprPostgisBoxIntersects rewrites the && operator to a
 * ST_Intersects_Extent(..) function call.
 */
static Node *
RewriteOpExprPostgisBoxIntersects(Node *node, void *context)
{
	OpExpr	   *opExpr = castNode(OpExpr, node);

	Assert(list_length(opExpr->args) == 2);

	FuncExpr   *funcExpr = ConvertOpExprToFuncExpr(opExpr);

	/* replace the function ID with st_intersects_extent */
	funcExpr->funcid = InternalIntersectsExtentFunctionId();

	return (Node *) funcExpr;
}


/*
 * RewriteOpExprLikeNotLike rewrites the ~ and !~ operator to the appropriate
 * lake_regexp_matches() function call.  The name disambiguation is still
 * required due to conflicting names/signatures for `regexp_matches` on the
 * Postgres and DuckDB sides.
 */
static Node *
RewriteOpExprLikeNotLike(Node *node, void *context)
{
	OpExpr	   *opExpr = castNode(OpExpr, node);

	if (list_length(opExpr->args) != 2)
		return node;

	/* these have no equivalent in pg_operator_d.h, but might in the future */
#define OID_TEXT_REGEXNE_OP 642 /* !~(text,text) */
#define OID_TEXT_ICREGEXNE_OP 1229	/* !~*(text,text) */

	/* specific opno for case-insensitive operators */
	bool		caseInsensitive =
		opExpr->opno == OID_TEXT_ICREGEXEQ_OP ||	/* ~*(text,text) */
		opExpr->opno == OID_TEXT_ICREGEXNE_OP;	/* !~*(text,text) */

	/* only affect text operators */
	if (exprType(linitial(opExpr->args)) != TEXTOID ||
		exprType(lsecond(opExpr->args)) != TEXTOID)
	{
		return node;
	}

	FuncExpr   *funcExpr = ConvertOpExprToFuncExpr(opExpr);

	List	   *regexpMatchesName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
											   makeString("lake_regexp_matches"));
	Oid			argTypes[] = {TEXTOID, TEXTOID, BOOLOID};

	/* get our appropriate function id here */
	funcExpr->funcid =
		LookupFuncName(regexpMatchesName, 3, argTypes, false);

	/* add our case insensitivity flag */
	funcExpr->args = lappend(funcExpr->args,
							 makeConst(BOOLOID,
									   -1,	/* typmod */
									   InvalidOid,	/* collation */
									   sizeof(bool),
									   BoolGetDatum(caseInsensitive),
									   false,	/* is null */
									   true /* by value */
									   ));

	/* negate if needed */
	if (opExpr->opno == OID_TEXT_REGEXNE_OP ||	/* !~(text,text) */
		opExpr->opno == OID_TEXT_ICREGEXNE_OP)	/* !~*(text,text) */
	{
		return (Node *) make_notclause((Expr *) funcExpr);
	}

	return (Node *) funcExpr;
}


/*
 * RewriteOpExprXor rewrites the # operator to a xor function call.
 */
static Node *
RewriteOpExprXor(Node *node, void *context)
{
	OpExpr	   *opExpr = castNode(OpExpr, node);

	/* only know how to handle 2-argument operators */
	if (list_length(opExpr->args) != 2)
		return node;

	Oid			leftOid = exprType(linitial(opExpr->args));
	Oid			rightOid = exprType(lsecond(opExpr->args));

	/* only know how to handle symmetric xor operators */
	if (leftOid != rightOid)
		return node;

	/* only know how to handle integral xor */
	if (leftOid != INT2OID && leftOid != INT4OID && leftOid != INT8OID)
		return node;

	FuncExpr   *funcExpr = ConvertOpExprToFuncExpr(opExpr);

	List	   *xorName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
									 makeString("xor"));
	Oid			argTypes[] = {leftOid, rightOid};

	/* get our appropriate function id here */
	funcExpr->funcid =
		LookupFuncName(xorName, 2, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteOpExprSqrt rewrites the |/ operator to a sqrt function call
 * and the ||/ operator to a cbrt function call.
 */
static Node *
RewriteOpExprSqrt(Node *node, void *context)
{
	OpExpr	   *opExpr = castNode(OpExpr, node);

	/* only know how to handle 1-argument operator */
	if (list_length(opExpr->args) != 1)
		return node;

	/* only know how to handle float8 operator */
	if (exprType(linitial(opExpr->args)) != FLOAT8OID)
		return node;

	FuncExpr   *funcExpr = ConvertOpExprToFuncExpr(opExpr);

	/*
	 * For some reason the |/ operator uses the "dsqrt" function, which is
	 * equivalent to the sqrt function. DuckDB only has sqrt.
	 */
	funcExpr->funcid =
		funcExpr->funcid == F_DSQRT ? F_SQRT_FLOAT8 : F_CBRT;

	return (Node *) funcExpr;
}


/*
 * RewriteOpExprIntegerDivision rewrites the '/' operator to '//' on the DuckDB
 * side when the arguments are both integer types.  DuckDB always returns a
 * float for integer division when using the '/' operator
 */
static Node *
RewriteOpExprIntegerDivision(Node *node, void *context)
{
	OpExpr	   *opExpr = castNode(OpExpr, node);

	/* only know how to handle 2-argument operator */
	if (list_length(opExpr->args) != 2)
		return node;

	/* don't rewrite unless we're using INT types on both sides */
	Oid			leftType = exprType(linitial(opExpr->args));
	Oid			rightType = exprType(lsecond(opExpr->args));

	/* check our oids */
	if ((leftType != INT8OID && leftType != INT4OID && leftType != INT2OID) ||
		(rightType != INT8OID && rightType != INT4OID && rightType != INT2OID))
		return node;

	/* convert to underlying divide() function */
	Oid			argTypes[] = {leftType, rightType};
	List	   *qualifiedName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										   makeString("divide"));

	Oid			foundFunction = LookupFuncName(qualifiedName, 2, argTypes, true);

	if (foundFunction == InvalidOid)
	{
		/* can't find the function, likely related to version issues */
		ereport(ERROR,
				(errmsg("cannot find integer division function for pushdown"),
				 errhint("run: ALTER EXTENSION pg_lake UPDATE;")));
	}

	FuncExpr   *funcExpr = ConvertOpExprToFuncExpr(opExpr);

	funcExpr->funcid = foundFunction;

	return (Node *) funcExpr;
}


/*
 * RewriteOpExprArrayAppend rewrites the array anycompatiblearray || anycompatible
 * operator.
 */
static Node *
RewriteOpExprArrayAppend(Node *node, void *context)
{
	OpExpr	   *opExpr = castNode(OpExpr, node);

	if (list_length(opExpr->args) != 2)
		return node;

	Oid			leftType = exprType(linitial(opExpr->args));
	Oid			rightType = exprType(lsecond(opExpr->args));

	/* we only handle concat of array and non-array here */
	if (!type_is_array(leftType) || type_is_array(rightType))
		return node;

	FuncExpr   *funcExpr = ConvertOpExprToFuncExpr(opExpr);
	List	   *funcName = list_make2(makeString("pg_catalog"),
									  makeString("array_append"));

	Oid			argTypes[] = {ANYCOMPATIBLEARRAYOID, ANYCOMPATIBLEOID};

	funcExpr->funcid = LookupFuncName(funcName, 2, argTypes, false);

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprRemoveArgs rewrites the given function to limit it to the
 * number of args in our lookup table.
 *
 * This function is used to allow us to push a function with default arguments
 * in Postgis to DuckDB which does not have those arguments, so effectively
 * turning a function (geometry, geometry, int DEFAULT) to (geometry, geometry).
 *
 * This currently just eliminates any arguments than the first funcIntArg.  Due
 * to how shippability is computed prior to the rewrite phase, we are unable to
 * prevent the pushdown of this argument at this juncture; we just rewrite
 * things to be syntactically valid on the DuckDB side.
 *
 * If we want to avoid syntax errors for non-default forms of the functions that
 * use this, we will need to examine the explicit arguments in the shippability
 * computation and only mark as shippable if the extra arguments we remove here
 * match the default values.  (It would appear we cannot determine post-parse
 * analysis whether the arguments were provided by the user, just whether they
 * have the same values as the defaults for the function itself.)  Consider this
 * a future enhancement.
 */
static Node *
RewriteFuncExprRemoveArgs(Node *node, void *context)
{
	/* if no argument limit, we don't do anything */
	RewriteQueryTreeContext *rewriteContext = (RewriteQueryTreeContext *) context;

	if (!rewriteContext->funcIntArg)
		return node;

	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/*
	 * Do we need to re-lookup the new arguments?  Let's try just truncating
	 * the list.
	 */

	List	   *result = NIL;
	ListCell   *cell;
	int			count = 0;

	foreach(cell, funcExpr->args)
	{
		if (count >= rewriteContext->funcIntArg)
		{
			break;
		}
		result = lappend(result, lfirst(cell));
		count++;
	}

	/* replace the function args with our (potentially) shorter list */
	funcExpr->args = result;

	return (Node *) funcExpr;
}


/*
 * RewriteFuncExprPostgisUnionAgg rewrites the aggregate ST_Union(geometry) to
 * ST_Union_Agg(geometry) on the DuckDB side; the only thing this does is
 * effectively change the function name.
 *
 * Note the subtle id function name differences:
 *
 * ST_UnionAggregateId() for Postgis' built-in ST_Union aggregate
 * ST_Union_AggAggregateId() for our internal ST_Union_Agg aggregate
 */
static Node *
RewriteFuncExprPostgisUnionAgg(Node *node, void *context)
{
	const int	argCount = 1;

	Aggref	   *aggExpr = castNode(Aggref, node);

	if (aggExpr->aggfnoid != ST_UnionAggregateId())
		return node;

	/*
	 * Aggrefs can have junk columns when there are sort columns involved, so
	 * we need to count only non-resjunks here.
	 */

	/* be defensive about signature */
	if (CountNonJunkColumns(aggExpr->args) != argCount)
		return node;

	Node	   *geomTargetEntry = linitial(aggExpr->args);
	Node	   *geomArg = NULL;

	if (!IsA(geomTargetEntry, TargetEntry))
		return node;

	/* get the argument from the target entry */
	geomArg = (Node *) ((TargetEntry *) geomTargetEntry)->expr;

	/* check whether the function has a geometry argument */
	if (exprType(geomArg) != GeometryTypeId())
		return node;

	/* replace the aggregate ID with one the internal one with the proper name */
	aggExpr->aggfnoid = ST_Union_AggAggregateId();

	return (Node *) aggExpr;
}


/*
 * RewriteFuncExprPostgisGeographyCast removes geography(geometry) casts
 * since the geography type is not available.
 */
static Node *
RewriteFuncExprPostgisGeographyCast(Node *node, void *context)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	Assert(list_length(funcExpr->args) == 1);

	return (Node *) linitial(funcExpr->args);
}


/*
 * CreateFlipCoordinatesExpression returns an st_flipcoordinates(node)
 * expression.
 */
static Node *
CreateFlipCoordinatesExpression(Node *node)
{
	Assert(exprType(node) == GeometryTypeId());

	/* wrap the geometry in an st_flipcoordinates */
	FuncExpr   *flipExpr = makeNode(FuncExpr);

	flipExpr->funcid = ST_FlipCoordinatesFunctionId();
	flipExpr->funcresulttype = GeometryTypeId();
	flipExpr->funcretset = false;
	flipExpr->funcvariadic = false;
	flipExpr->funcformat = COERCE_EXPLICIT_CALL;
	flipExpr->location = -1;
	flipExpr->args = list_make1(node);

	return (Node *) flipExpr;
}


/*
 * RewriteFuncExprPostgisArea rewrites st_area(geography,bool) to
 * st_area_spheroid(geometry)
 */
static Node *
RewriteFuncExprPostgisArea(Node *node, void *context)
{
	Oid			argTypes[] = {GeometryTypeId()};

	return RewriteFuncExprPostgisSpheroid(node, "st_area", 1, argTypes);
}

/*
 * RewriteFuncExprPostgisLength rewrites st_length(geography,bool) to
 * st_length_spheroid(geometry)
 */
static Node *
RewriteFuncExprPostgisLength(Node *node, void *context)
{
	Oid			argTypes[] = {GeometryTypeId()};

	return RewriteFuncExprPostgisSpheroid(node, "st_length", 1, argTypes);
}

/*
 * RewriteFuncExprPostgisPerimeter rewrites st_perimeter(geography,bool) to
 * st_perimeter_spheroid(geometry)
 */
static Node *
RewriteFuncExprPostgisPerimeter(Node *node, void *context)
{
	Oid			argTypes[] = {GeometryTypeId()};

	return RewriteFuncExprPostgisSpheroid(node, "st_perimeter", 1, argTypes);
}

/*
 * RewriteFuncExprPostgisDistance rewrites st_perimeter(geography,geography,bool) to
 * st_perimeter_spheroid(geometry)
 */
static Node *
RewriteFuncExprPostgisDistance(Node *node, void *context)
{
	Oid			argTypes[] = {GeometryTypeId(), GeometryTypeId()};

	return RewriteFuncExprPostgisSpheroid(node, "st_distance", 2, argTypes);
}

/*
 * RewriteFuncExprPostgisSpheroid is a helper function for all spheroid
 * function rewrites.
 *
 * We assume a pattern where the PostGIS function st_xxx has a variant with
 * use_spheroid bool as the last argument, whereas DuckDB has a function
 * named st_xxx and st_xxx_spheroid.
 *
 * The case where use_spheroid is false is mapped to the regular st_xxx.
 * The case where use_spheroid is true is mapped to st_xxx_spheroid.
 * The case where use_spheroid is not a Const or NULL is not pushed down here.
 */
static Node *
RewriteFuncExprPostgisSpheroid(Node *node, char *functionName, int argCount, Oid *argTypes)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	/* only variant with extra use_spheroid argument is rewritten */
	if (list_length(funcExpr->args) != argCount + 1)
		return node;

	Node	   *useSpheroidArg = (Node *) llast(funcExpr->args);

	Assert(exprType(useSpheroidArg) == BOOLOID);

	bool		useSpheroid = true;

	/*
	 * Currently only the st_distance function has a _sphere variant that
	 * corresponds to the use_spheroid := false behaviour of PostGIS. For
	 * other functions we use the "slow" _spheroid variant, which does give
	 * similar output.
	 *
	 * We use argCount 2 as a proxy for st_distance.
	 */
	bool		hasSphereFunction = argCount == 2;

	if (hasSphereFunction)
	{
		/*
		 * We only switch to sphere if the last argument is false.
		 *
		 * Otherwise we keep using the slower spheroid variant.
		 */
		Const	   *useSpheroidConst = ResolveConstChain(useSpheroidArg);

		if (useSpheroidConst != NULL &&
			!useSpheroidConst->constisnull &&
			!DatumGetBool(useSpheroidConst->constvalue))
		{
			useSpheroid = false;
		}
	}

	List	   *newArgList = NIL;
	ListCell   *argCell = NULL;

	foreach(argCell, funcExpr->args)
	{
		Node	   *arg = (Node *) lfirst(argCell);

		/* wrap geometries in st_flipcoordinates to fix DuckDB issue */
		if (exprType(arg) == GeometryTypeId())
			newArgList = lappend(newArgList, CreateFlipCoordinatesExpression(arg));
		else
			newArgList = lappend(newArgList, arg);

		/* do not include use_spheroid */
		if (list_length(newArgList) == argCount)
			break;
	}

	/*
	 * when use_spheroid is true, replace the function ID with
	 * st_xxx_spheroid. Otherwise, replace with st_xxx_sphere.
	 */

	char	   *schemaName = PG_LAKE_INTERNAL_NSP;

	if (useSpheroid || !hasSphereFunction)
	{
		functionName = psprintf("%s_spheroid", functionName);
	}
	else
	{
		functionName = psprintf("%s_sphere", functionName);
	}

	List	   *qualifiedName = list_make2(makeString(schemaName),
										   makeString(functionName));

	funcExpr->funcid = LookupFuncName(qualifiedName, argCount, argTypes, false);
	funcExpr->args = newArgList;

	return node;
}


/*
 * DowncastFloatConstInOpExpr downcasts float8 Const in a
 * <float8 Const> <operator> <float4> expression.
 */
static Node *
DowncastFloatConstInOpExpr(Node *node)
{
	if (!IsA(node, OpExpr))
		return node;

	OpExpr	   *opExpr = castNode(OpExpr, node);

	if (list_length(opExpr->args) != 2)
		return node;

	Node	   *leftArg = (Node *) linitial(opExpr->args);
	Node	   *rightArg = (Node *) lsecond(opExpr->args);

	Oid			leftTypeId = exprType(leftArg);
	Oid			rightTypeId = exprType(rightArg);

	bool		leftNeedsDownCast =
		leftTypeId == FLOAT8OID && rightTypeId == FLOAT4OID;

	bool		rightNeedsDownCast =
		leftTypeId == FLOAT4OID && rightTypeId == FLOAT8OID;

	if (!leftNeedsDownCast && !rightNeedsDownCast)
		return node;

	if (leftNeedsDownCast && !IsConstCastChain(leftArg))
		return node;

	if (rightNeedsDownCast && !IsConstCastChain(rightArg))
		return node;

	Node	   *constArg = leftNeedsDownCast ? leftArg : rightArg;

	bool		isNull = false;
	Datum		constValue = 0;

	Const	   *resolvedConst = ResolveConstChain(constArg);

	/* could not resolve to Const (should not happen) */
	if (!resolvedConst)
		return node;

	constValue = resolvedConst->constvalue;
	isNull = resolvedConst->constisnull;

	if (isNull)
		/* value is NULL, do not cast */
		return node;

	float8		float8Value = DatumGetFloat8(constValue);
	float4		float4Value = (float4) float8Value;

	if (isinf(float4Value))
		/* can only downcast if the value is within float4 range */
		return node;

	/*
	 * Create a new Const for the down-casted value.
	 */
	Const	   *newConst = makeNode(Const);

	newConst->consttype = FLOAT4OID;
	newConst->consttypmod = -1;
	newConst->constlen = sizeof(float4);
	newConst->constvalue = Float4GetDatum(float4Value);
	newConst->constbyval = true;
	newConst->constisnull = false;
	newConst->location = -1;

	/*
	 * The type is now correct/reduced, but we won't generate a cast without
	 * an explicit conversion, so do that.
	 */

	RelabelType *coerceExpr = makeNode(RelabelType);

	coerceExpr->arg = (Expr *) newConst;
	coerceExpr->resulttype = FLOAT4OID;
	coerceExpr->resulttypmod = -1;
	coerceExpr->resultcollid = InvalidOid;
	coerceExpr->relabelformat = COERCE_EXPLICIT_CAST;
	coerceExpr->location = -1;

	/*
	 * We must also lookup an equivalent operator based on the new types as to
	 * not confuse the deparser.
	 */
	char	   *opname = get_opname(opExpr->opno);
	Oid			newOpNo = OpernameGetOprid(list_make1(makeString(opname)),
										   FLOAT4OID, FLOAT4OID);

	if (!OidIsValid(newOpNo))
		/* shouldn't happen, but be on the safe side */
		return node;

	/*
	 * Now create a new OpExpr for the downcasted values.
	 */

	/* work on a copy */
	opExpr = copyObject(opExpr);

	/* replace our opno */
	opExpr->opno = newOpNo;
	/* only for deparse, not needed */
	opExpr->opfuncid = InvalidOid;

	/* replace the old const with the casted node */
	if (leftNeedsDownCast)
		opExpr->args = list_make2(coerceExpr, rightArg);
	else
		opExpr->args = list_make2(leftArg, coerceExpr);

	return (Node *) opExpr;
}


/*
 * pg_lake_internal_dummy_function is used to define PGDuck UDFs in
 * the internal schema for deparsing purposes.
 */
Datum
pg_lake_internal_dummy_function(PG_FUNCTION_ARGS)
{
	PG_RETURN_VOID();
}
