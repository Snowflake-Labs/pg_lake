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
 * shippable_builtin_functions.c
 *	  Determine which built-in functions are shippable to pgduck_server
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "pg_lake/parsetree/const.h"
#include "pg_lake/pgduck/to_char.h"
#include "catalog/pg_type.h"
#include "optimizer/optimizer.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/regproc.h"
#include "utils/syscache.h"

#include "pg_lake/pgduck/shippable_builtin_functions.h"

static bool IsConcatShippable(Node *node);
static bool IsEncodeShippable(Node *node);
static bool IsDecodeShippable(Node *node);
static bool IsArrayLengthShippable(Node *node);
static bool IsCast(Node *node);
static bool IsConvertibleToChar(Node *node);


static const PGDuckShippableFunction ShippableBuiltinProcs[] =
{
	{"count", 'a', 0, {NULL}, NULL},
	{"count", 'a', 1, {"any"}, NULL},

	{"any_value", 'a', 1, {"anyelement"}, NULL},

	/*
	 * {"avg", 'a', 1, {"interval"}, NULL},  DuckDB does not support
	 * avg(interval)
	 */
	{"avg", 'a', 1, {"int2"}, NULL},
	{"avg", 'a', 1, {"int4"}, NULL},
	{"avg", 'a', 1, {"int8"}, NULL},
	{"avg", 'a', 1, {"float4"}, NULL},
	{"avg", 'a', 1, {"float8"}, NULL},
	{"avg", 'a', 1, {"numeric"}, NULL},

	/*
	 * {"sum", 'a', 1, {"interval"}, NULL},  DuckDB does not support
	 * sum(interval)
	 */
	{"sum", 'a', 1, {"int2"}, NULL},
	{"sum", 'a', 1, {"int4"}, NULL},
	{"sum", 'a', 1, {"int8"}, NULL},
	{"sum", 'a', 1, {"float4"}, NULL},
	{"sum", 'a', 1, {"float8"}, NULL},
	{"sum", 'a', 1, {"numeric"}, NULL},

	{"min", 'a', 1, {"int2"}, NULL},
	{"min", 'a', 1, {"int4"}, NULL},
	{"min", 'a', 1, {"int8"}, NULL},
	{"min", 'a', 1, {"float4"}, NULL},
	{"min", 'a', 1, {"float8"}, NULL},
	{"min", 'a', 1, {"numeric"}, NULL},
	{"min", 'a', 1, {"text"}, NULL},
	{"min", 'a', 1, {"bpchar"}, NULL},
	{"min", 'a', 1, {"date"}, NULL},
	{"min", 'a', 1, {"timestamp"}, NULL},
	{"min", 'a', 1, {"timestamptz"}, NULL},
	{"min", 'a', 1, {"time"}, NULL},
	{"min", 'a', 1, {"timetz"}, NULL},
	{"min", 'a', 1, {"anyenum"}, NULL},
	{"min", 'a', 1, {"anyarray"}, NULL},

	{"max", 'a', 1, {"int2"}, NULL},
	{"max", 'a', 1, {"int4"}, NULL},
	{"max", 'a', 1, {"int8"}, NULL},
	{"max", 'a', 1, {"float4"}, NULL},
	{"max", 'a', 1, {"float8"}, NULL},
	{"max", 'a', 1, {"numeric"}, NULL},
	{"max", 'a', 1, {"text"}, NULL},
	{"max", 'a', 1, {"bpchar"}, NULL},
	{"max", 'a', 1, {"date"}, NULL},
	{"max", 'a', 1, {"timestamp"}, NULL},
	{"max", 'a', 1, {"timestamptz"}, NULL},
	{"max", 'a', 1, {"time"}, NULL},
	{"max", 'a', 1, {"timetz"}, NULL},
	{"max", 'a', 1, {"anyenum"}, NULL},
	{"max", 'a', 1, {"anyarray"}, NULL},

	{"stddev", 'a', 1, {"int2"}, NULL},
	{"stddev", 'a', 1, {"int4"}, NULL},
	{"stddev", 'a', 1, {"int8"}, NULL},
	{"stddev", 'a', 1, {"float4"}, NULL},
	{"stddev", 'a', 1, {"float8"}, NULL},
	{"stddev", 'a', 1, {"numeric"}, NULL},

	{"stddev_pop", 'a', 1, {"int2"}, NULL},
	{"stddev_pop", 'a', 1, {"int4"}, NULL},
	{"stddev_pop", 'a', 1, {"int8"}, NULL},
	{"stddev_pop", 'a', 1, {"float4"}, NULL},
	{"stddev_pop", 'a', 1, {"float8"}, NULL},
	{"stddev_pop", 'a', 1, {"numeric"}, NULL},

	{"stddev_samp", 'a', 1, {"int2"}, NULL},
	{"stddev_samp", 'a', 1, {"int4"}, NULL},
	{"stddev_samp", 'a', 1, {"int8"}, NULL},
	{"stddev_samp", 'a', 1, {"float4"}, NULL},
	{"stddev_samp", 'a', 1, {"float8"}, NULL},
	{"stddev_samp", 'a', 1, {"numeric"}, NULL},

	{"variance", 'a', 1, {"int2"}, NULL},
	{"variance", 'a', 1, {"int4"}, NULL},
	{"variance", 'a', 1, {"int8"}, NULL},
	{"variance", 'a', 1, {"float4"}, NULL},
	{"variance", 'a', 1, {"float8"}, NULL},
	{"variance", 'a', 1, {"numeric"}, NULL},

	{"var_samp", 'a', 1, {"int2"}, NULL},
	{"var_samp", 'a', 1, {"int4"}, NULL},
	{"var_samp", 'a', 1, {"int8"}, NULL},
	{"var_samp", 'a', 1, {"float4"}, NULL},
	{"var_samp", 'a', 1, {"float8"}, NULL},
	{"var_samp", 'a', 1, {"numeric"}, NULL},

	{"var_pop", 'a', 1, {"int2"}, NULL},
	{"var_pop", 'a', 1, {"int4"}, NULL},
	{"var_pop", 'a', 1, {"int8"}, NULL},
	{"var_pop", 'a', 1, {"float4"}, NULL},
	{"var_pop", 'a', 1, {"float8"}, NULL},
	{"var_pop", 'a', 1, {"numeric"}, NULL},

	{"corr", 'a', 2, {"float8", "float8"}, NULL},
	{"covar_pop", 'a', 2, {"float8", "float8"}, NULL},
	{"covar_samp", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_avgx", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_avgy", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_count", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_intercept", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_r2", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_slope", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_sxx", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_sxy", 'a', 2, {"float8", "float8"}, NULL},
	{"regr_syy", 'a', 2, {"float8", "float8"}, NULL},

	{"mode", 'a', 1, {"anyelement"}, NULL},
	{"percentile_cont", 'a', 2, {"float8", "float8"}, NULL},
	{"percentile_disc", 'a', 2, {"float8", "anyelement"}, NULL},
	{"percentile_cont", 'a', 2, {"_float8", "float8"}, NULL},
	{"percentile_disc", 'a', 2, {"_float8", "anyelement"}, NULL},

	{"numeric", 'f', 1, {"int2"}, NULL},
	{"numeric", 'f', 1, {"int4"}, NULL},
	{"numeric", 'f', 1, {"int8"}, NULL},
	{"numeric", 'f', 1, {"float4"}, NULL},
	{"numeric", 'f', 1, {"float8"}, NULL},
	{"numeric", 'f', 2, {"numeric", "int4"}, IsCast},

	{"float8", 'f', 1, {"int2"}, NULL},
	{"float8", 'f', 1, {"int4"}, NULL},
	{"float8", 'f', 1, {"int8"}, NULL},
	{"float8", 'f', 1, {"float4"}, NULL},
	{"float8", 'f', 1, {"numeric"}, NULL},

	{"float4", 'f', 1, {"int2"}, NULL},
	{"float4", 'f', 1, {"int4"}, NULL},
	{"float4", 'f', 1, {"int8"}, NULL},
	{"float4", 'f', 1, {"float8"}, NULL},
	{"float4", 'f', 1, {"numeric"}, NULL},

	{"int2", 'f', 1, {"int4"}, NULL},
	{"int2", 'f', 1, {"int8"}, NULL},
	{"int2", 'f', 1, {"float4"}, NULL},
	{"int2", 'f', 1, {"float8"}, NULL},
	{"int2", 'f', 1, {"numeric"}, NULL},

	{"int4", 'f', 1, {"bit"}, NULL},
	{"int4", 'f', 1, {"int2"}, NULL},
	{"int4", 'f', 1, {"int8"}, NULL},
	{"int4", 'f', 1, {"float4"}, NULL},
	{"int4", 'f', 1, {"float8"}, NULL},
	{"int4", 'f', 1, {"numeric"}, NULL},

	{"int8", 'f', 1, {"bit"}, NULL},
	{"int8", 'f', 1, {"int2"}, NULL},
	{"int8", 'f', 1, {"int4"}, NULL},
	{"int8", 'f', 1, {"float4"}, NULL},
	{"int8", 'f', 1, {"float8"}, NULL},
	{"int8", 'f', 1, {"numeric"}, NULL},

	/* text casts */
	{"text", 'f', 1, {"bpchar"}, NULL},
	{"text", 'f', 1, {"char"}, NULL},
	{"text", 'f', 1, {"bool"}, NULL},
	{"char", 'f', 1, {"text"}, IsCast},

	/*
	 * Bit of a weirdo, but happens for 'abc' || 'd'::char, and is then
	 * deparsed as ::character(1).
	 */
	{"bpchar", 'f', 3, {"bpchar", "int4", "bool"}, IsCast},

	/* date <-> timestamp <-> timestamptz <-> time <-> timetz casts */
	{"date", 'f', 1, {"timestamp"}, NULL},
	{"date", 'f', 1, {"timestamptz"}, NULL},
	{"timestamp", 'f', 1, {"date"}, NULL},
	{"timestamp", 'f', 1, {"timestamptz"}, NULL},
	{"timestamptz", 'f', 1, {"date"}, NULL},
	{"timestamptz", 'f', 1, {"timestamp"}, NULL},
	{"time", 'f', 1, {"timetz"}, NULL},
	{"time", 'f', 1, {"timestamp"}, NULL},
	{"timetz", 'f', 1, {"time"}, NULL},
	{"timetz", 'f', 1, {"timestamptz"}, NULL},

	{"length", 'f', 1, {"text"}, NULL},

	{"to_date", 'f', 1, {"float8"}, NULL},
	{"to_timestamp", 'f', 1, {"float8"}, NULL},

	{"extract", 'f', 2, {"text", "date"}, NULL},
	{"extract", 'f', 2, {"text", "interval"}, NULL},
	{"extract", 'f', 2, {"text", "timetz"}, NULL},
	{"extract", 'f', 2, {"text", "time"}, NULL},
	{"extract", 'f', 2, {"text", "timestamptz"}, NULL},
	{"extract", 'f', 2, {"text", "timestamp"}, NULL},

	{"date_part", 'f', 2, {"text", "date"}, NULL},
	{"date_part", 'f', 2, {"text", "interval"}, NULL},
	{"date_part", 'f', 2, {"text", "time"}, NULL},
	{"date_part", 'f', 2, {"text", "timetz"}, NULL},
	{"date_part", 'f', 2, {"text", "timestamptz"}, NULL},
	{"date_part", 'f', 2, {"text", "timestamp"}, NULL},

	{"date_bin", 'f', 3, {"interval", "timestamptz", "timestamptz"}, NULL},
	{"date_bin", 'f', 3, {"interval", "timestamp", "timestamp"}, NULL},

	{"date_trunc", 'f', 2, {"text", "interval"}, NULL},
	{"date_trunc", 'f', 2, {"text", "timestamp"}, NULL},
	{"date_trunc", 'f', 2, {"text", "timestamptz"}, NULL},

	{"now", 'f', 0, {}, NULL},

	{"to_char", 'f', 2, {"timestamp", "text"}, IsConvertibleToChar},
	{"to_char", 'f', 2, {"timestamptz", "text"}, IsConvertibleToChar},

	/* 3-argument version is not available */
	/* {"date_trunc", 'f', 3, {"text", "timestamptz", "text"}, NULL}, */

	{"regexp_replace", 'f', 3, {"text", "text", "text"}, NULL},
	{"regexp_replace", 'f', 4, {"text", "text", "text", "text"}, NULL},

	/* these regexp_replace variants are not available: */
	/* {"regexp_replace", 'f', 4, {"text", "text", "text", "int4"}, NULL}, */

	/*
	 * {"regexp_replace", 'f', 5, {"text", "text", "text", "int4", "int4"},
	 * NULL},
	 */

	/*
	 * {"regexp_replace", 'f', 6, {"text", "text", "text", "int4", "int4",
	 * "text"}, NULL},
	 */

	{"concat", 'f', 1, {"any"}, IsConcatShippable},

	{"bool", 'f', 1, {"int4"}, NULL},
	{"bool_and", 'a', 1, {"bool"}, NULL},
	{"bool_or", 'a', 1, {"bool"}, NULL},

	/* Mathematical functions */
	{"abs", 'f', 1, {"int2"}, NULL},
	{"abs", 'f', 1, {"int4"}, NULL},
	{"abs", 'f', 1, {"int8"}, NULL},
	{"abs", 'f', 1, {"float4"}, NULL},
	{"abs", 'f', 1, {"float8"}, NULL},
	{"abs", 'f', 1, {"numeric"}, NULL},
	{"cbrt", 'f', 1, {"float8"}, NULL},
	{"ceil", 'f', 1, {"float8"}, NULL},
	{"ceil", 'f', 1, {"numeric"}, NULL},
	{"ceiling", 'f', 1, {"float8"}, NULL},
	{"ceiling", 'f', 1, {"numeric"}, NULL},
	{"degrees", 'f', 1, {"float8"}, NULL},
	/* div rewrites to fdiv */
	{"div", 'f', 2, {"numeric", "numeric"}, NULL},
	{"exp", 'f', 1, {"float8"}, NULL},
	{"exp", 'f', 1, {"numeric"}, NULL},
	{"floor", 'f', 1, {"float8"}, NULL},
	{"floor", 'f', 1, {"numeric"}, NULL},
	{"ln", 'f', 1, {"float8"}, NULL},
	{"ln", 'f', 1, {"numeric"}, NULL},
	{"log", 'f', 1, {"float8"}, NULL},
	{"log", 'f', 1, {"numeric"}, NULL},
	{"log10", 'f', 1, {"float8"}, NULL},
	{"log10", 'f', 1, {"numeric"}, NULL},
	/* mod rewrites to fmod */
	{"mod", 'f', 2, {"numeric", "numeric"}, NULL},
	{"pi", 'f', 0, {}, NULL},
	{"power", 'f', 2, {"float8", "float8"}, NULL},
	{"power", 'f', 2, {"numeric", "numeric"}, NULL},
	{"radians", 'f', 1, {"float8"}, NULL},
	{"round", 'f', 1, {"float8"}, NULL},
	{"round", 'f', 1, {"numeric"}, NULL},
	{"round", 'f', 2, {"numeric", "int4"}, NULL},
	{"sqrt", 'f', 1, {"float8"}, NULL},
	{"sqrt", 'f', 1, {"numeric"}, NULL},
	{"trunc", 'f', 1, {"float8"}, NULL},
	{"trunc", 'f', 1, {"numeric"}, NULL},

	/* Random functions */
	{"random", 'f', 0, {}, NULL},

	/* Trigonometric functions */
	{"acos", 'f', 1, {"float8"}, NULL},
	{"acosd", 'f', 1, {"float8"}, NULL},
	{"asin", 'f', 1, {"float8"}, NULL},
	{"asind", 'f', 1, {"float8"}, NULL},
	{"atan", 'f', 1, {"float8"}, NULL},
	{"atand", 'f', 1, {"float8"}, NULL},
	{"atan2", 'f', 2, {"float8", "float8"}, NULL},
	{"atan2d", 'f', 2, {"float8", "float8"}, NULL},
	{"cos", 'f', 1, {"float8"}, NULL},
	{"cosd", 'f', 1, {"float8"}, NULL},
	{"cot", 'f', 1, {"float8"}, NULL},
	{"cotd", 'f', 1, {"float8"}, NULL},
	{"sin", 'f', 1, {"float8"}, NULL},
	{"sind", 'f', 1, {"float8"}, NULL},
	{"tan", 'f', 1, {"float8"}, NULL},
	{"tand", 'f', 1, {"float8"}, NULL},

	/* array functions */
	{"array_append", 'f', 2, {"anycompatiblearray", "anycompatible"}, NULL},
	{"array_cat", 'f', 2, {"anycompatiblearray", "anycompatiblearray"}, NULL},
	/* not supported: array_dims */
	/* not supported: array_fill */
	{"array_length", 'f', 2, {"anyarray", "int4"}, IsArrayLengthShippable},
	/* not supported: array_lower */
	/* not supported: array_ndims */
	/* array_position returns different results for NULL */
	/* not supported: array_positions */
	{"array_prepend", 'f', 2, {"anycompatible", "anycompatiblearray"}, NULL},
	/* not supported: array_remove */
	/* not supported: array_replace */
	/* not supported: array_sample */
	/* not supported: array_shuffle */
	/* array_to_string behaves differently for NULL */
	/* not supported: array_to_string with 3 args */
	/* not supported: array_upper */
	{"cardinality", 'f', 1, {"anyarray"}, NULL},
	/* not supported: trim_array */
	{"unnest", 'f', 1, {"anyarray"}, NULL},

	/* array aggregates */
	{"array_agg", 'a', 1, {"anyarray"}, NULL},
	{"array_agg", 'a', 1, {"anynonarray"}, NULL},

	/* window function aggregates */
	{"rank", 'w', 0, {}, NULL},
	{"row_number", 'w', 0, {}, NULL},
	{"dense_rank", 'w', 0, {}, NULL},
	{"percent_rank", 'w', 0, {}, NULL},
	{"cume_dist", 'w', 0, {}, NULL},
	{"ntile", 'w', 1, {"int4"}, NULL},
	{"lag", 'w', 1, {"anyelement"}, NULL},
	{"lag", 'w', 2, {"anyelement", "int4"}, NULL},
	{"lag", 'w', 3, {"anycompatible", "int4", "anycompatible"}, NULL},
	{"lead", 'w', 1, {"anyelement"}, NULL},
	{"lead", 'w', 2, {"anyelement", "int4"}, NULL},
	{"lead", 'w', 3, {"anycompatible", "int4", "anycompatible"}, NULL},
	{"first_value", 'w', 1, {"anyelement"}, NULL},
	{"last_value", 'w', 1, {"anyelement"}, NULL},
	{"nth_value", 'w', 2, {"anyelement", "int4"}, NULL},

	{"generate_series", 'f', 2, {"int4", "int4"}, NULL},
	{"generate_series", 'f', 3, {"int4", "int4", "int4"}, NULL},
	{"generate_series", 'f', 2, {"int8", "int8"}, NULL},
	{"generate_series", 'f', 3, {"int8", "int8", "int8"}, NULL},
	{"generate_series", 'f', 3, {"timestamp", "timestamp", "interval"}, NULL},
	{"generate_series", 'f', 3, {"timestamptz", "timestamptz", "interval"}, NULL},

	/* more text functions */
	{"ascii", 'f', 1, {"text"}, NULL},
	{"bit_length", 'f', 1, {"text"}, NULL},
	{"btrim", 'f', 1, {"text"}, NULL},
	{"btrim", 'f', 2, {"text", "text"}, NULL},
	{"chr", 'f', 1, {"int4"}, NULL},
	{"concat_ws", 'f', 2, {"text", "any"}, IsConcatShippable},
	{"left", 'f', 2, {"text", "int4"}, NULL},
	{"lower", 'f', 1, {"text"}, NULL},
	{"lpad", 'f', 2, {"text", "int4"}, NULL},
	{"lpad", 'f', 3, {"text", "int4", "text"}, NULL},
	{"ltrim", 'f', 1, {"text"}, NULL},
	{"ltrim", 'f', 2, {"text", "text"}, NULL},
	{"md5", 'f', 1, {"text"}, NULL},
	{"position", 'f', 2, {"text", "text"}, NULL},
	{"regexp_like", 'f', 2, {"text", "text"}, NULL},
	{"regexp_like", 'f', 3, {"text", "text", "text"}, NULL},
	{"repeat", 'f', 2, {"text", "int4"}, NULL},
	{"replace", 'f', 3, {"text", "text", "text"}, NULL},
	{"reverse", 'f', 1, {"text"}, NULL},
	{"right", 'f', 2, {"text", "int4"}, NULL},
	{"rpad", 'f', 2, {"text", "int4"}, NULL},
	{"rpad", 'f', 3, {"text", "int4", "text"}, NULL},
	{"rtrim", 'f', 1, {"text"}, NULL},
	{"rtrim", 'f', 2, {"text", "text"}, NULL},
	{"split_part", 'f', 3, {"text", "text", "int4"}, NULL},
	{"starts_with", 'f', 2, {"text", "text"}, NULL},
	{"strpos", 'f', 2, {"text", "text"}, NULL},
	{"substr", 'f', 2, {"text", "int4"}, NULL},
	{"substr", 'f', 3, {"text", "int4", "int4"}, NULL},
	{"substring", 'f', 2, {"text", "int4"}, NULL},
	{"substring", 'f', 3, {"text", "int4", "int4"}, NULL},
	{"upper", 'f', 1, {"text"}, NULL},

	/* json functions */
	{"json_array_length", 'f', 1, {"json"}, NULL},
	{"jsonb_array_length", 'f', 1, {"jsonb"}, NULL},

	/* encode/decode functions */
	{"encode", 'f', 2, {"bytea", "text"}, IsEncodeShippable},
	{"decode", 'f', 2, {"text", "text"}, IsDecodeShippable},

	/* trim() */
};


/*
 * GetShippableBuiltinFunctionCount
 *		Returns the number of shippable functions
 */
const		PGDuckShippableFunction *
GetShippableBuiltinFunctions(int *sizePointer)
{
	*sizePointer = ARRAY_SIZE(ShippableBuiltinProcs);

	return ShippableBuiltinProcs;
}


/*
 * This is a special case for "concat" function. "concat" is very flexible
 * in both PostgreSQL and DuckDB. In both databases, it can take any
 * number of arguments with any type, and return a string.
 *
 * We have this special case because "concat" is used in few analytical
 * benchmark queries, such as tpc-ds, etc.
 *
 * The problem here is that, some types have different representations in
 * PostgreSQL and DuckDB. For example, in PostgreSQL, a boolean value is
 * represented as 't' or 'f', while in DuckDB, it is represented as 'true'
 * or 'false'. So, if a boolean value is passed to "concat" function, the
 * result will be different in PostgreSQL and DuckDB. Similarly for arrays
 * and other types.
 *
 * To avoid this problem, we check whether the arguments of "concat"
 * function are simple types that we can safely pass to DuckDB.
 *
 * In practice, this function is used extensively for text concatenation.
 * But we still allow some others like integer, float, etc.
 *
 * concat_ws() also uses this, but since the first argument should be text it
 * requires no further changes.
 */
static bool
IsConcatShippable(Node *node)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);

	ListCell   *concatArgCell = NULL;

	foreach(concatArgCell, funcExpr->args)
	{
		Node	   *concatArg = (Node *) lfirst(concatArgCell);
		Oid			argType = exprType(concatArg);

		if (!(argType == UNKNOWNOID || argType == TEXTOID ||
			  argType == VARCHAROID ||
			  argType == BPCHAROID || argType == CHAROID ||
			  argType == INT2OID || argType == INT4OID ||
			  argType == INT8OID || argType == FLOAT4OID ||
			  argType == FLOAT8OID || argType == NUMERICOID ||
			  argType == UUIDOID))
		{
			/* not shippable parameters for concat */
			return false;
		}
	}

	/* shippable parameters for concat */
	return true;
}


/*
 * IsEncodeShippable returns whether encode is used with 'base64' or 'hex' format.
 */
static bool
IsEncodeShippable(Node *node)
{
	FuncExpr   *funcExpr PG_USED_FOR_ASSERTS_ONLY = castNode(FuncExpr, node);

	Assert(list_length(funcExpr->args) == 2);

	Const	   *formatConst;

	/* format must be a non-null Const */
	if (!GetConstArg(node, 1, &formatConst))
		return false;

	if (formatConst->constisnull || formatConst->consttype != TEXTOID)
		return false;

	text	   *formatText = DatumGetTextP(formatConst->constvalue);
	char	   *formatCStr = text_to_cstring(formatText);

	bool		result = (pg_strcasecmp(formatCStr, "base64") == 0) ||
		(pg_strcasecmp(formatCStr, "hex") == 0);

	pfree(formatCStr);

	return result;
}


/*
 * IsDecodeShippable returns whether decode is used with 'base64' or 'hex' format.
 */
static bool
IsDecodeShippable(Node *node)
{
	FuncExpr   *funcExpr PG_USED_FOR_ASSERTS_ONLY = castNode(FuncExpr, node);

	Assert(list_length(funcExpr->args) == 2);

	Const	   *formatConst;

	/* format must be a non-null Const */
	if (!GetConstArg(node, 1, &formatConst))
		return false;

	if (formatConst->constisnull || formatConst->consttype != TEXTOID)
		return false;

	text	   *formatText = DatumGetTextP(formatConst->constvalue);
	char	   *formatCStr = text_to_cstring(formatText);

	bool		result = (pg_strcasecmp(formatCStr, "base64") == 0) ||
		(pg_strcasecmp(formatCStr, "hex") == 0);

	pfree(formatCStr);

	return result;
}


/*
 * IsArrayLengthShippable returns whether array_length is used for
 * a 1-dimensional array.
 */
static bool
IsArrayLengthShippable(Node *node)
{
	FuncExpr   *funcExpr PG_USED_FOR_ASSERTS_ONLY = castNode(FuncExpr, node);

	Assert(list_length(funcExpr->args) == 2);

	Const	   *dimConst;

	if (!GetConstArg(node, 1, &dimConst))
		return false;

	if (dimConst->constisnull || dimConst->consttype != INT4OID)
		return false;

	if (DatumGetInt32(dimConst->constvalue) != 1)
		return false;

	return true;
}


/*
 * IsCast checks whether the given FuncExpr is a cast, since
 * explicit calls are likely not pushdownable.
 */
static bool
IsCast(Node *node)
{
	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	CoercionForm coercionForm = funcExpr->funcformat;

	return coercionForm == COERCE_EXPLICIT_CAST || coercionForm == COERCE_IMPLICIT_CAST;
}


/*
 * IsConvertibleToChar checks whether a to_char call can be rewritten to strftime.
 */
static bool
IsConvertibleToChar(Node *node)
{
	/*
	 * our current implementation clones the argument, so it must not be
	 * volatile
	 */
	if (contain_volatile_functions(node))
		return false;

	Const	   *formatConst;

	/* format must be a non-null Const */
	if (!GetConstArg(node, 1, &formatConst))
		return false;

	if (formatConst->constisnull || formatConst->consttype != TEXTOID)
		return false;

	FuncExpr   *funcExpr = castNode(FuncExpr, node);
	bool		checkOnly = true;

	return BuildStrftimeChain(funcExpr, checkOnly, NULL);
}
