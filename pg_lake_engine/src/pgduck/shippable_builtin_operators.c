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
 * shippable_builtin_operators.c
 *	  Set of built-in operators that can be shipped to pgduck.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/pg_type.h"
#include "pg_lake/pgduck/shippable_builtin_operators.h"
#include "nodes/nodeFuncs.h"

#define SHIPPABLE_OPERATORS_BY_TYPE(typename, oprs) \
	{typename, ARRAY_SIZE(oprs), oprs}

static bool IsTextConcatAnyNonArrayShippable(Node *node);

/* Bytea operators */
static const PGDuckShippableOperator PGDuckShippableByteaOperators[] = {
	{"=", "pg_catalog", "byteaeq", 2, {"bytea", "bytea"}, NULL},
	{"<>", "pg_catalog", "byteane", 2, {"bytea", "bytea"}, NULL},
	{"<", "pg_catalog", "bytealt", 2, {"bytea", "bytea"}, NULL},
	{">", "pg_catalog", "byteagt", 2, {"bytea", "bytea"}, NULL},
	{"<=", "pg_catalog", "byteale", 2, {"bytea", "bytea"}, NULL},
	{">=", "pg_catalog", "byteage", 2, {"bytea", "bytea"}, NULL},
	{"||", "pg_catalog", "byteacat", 2, {"bytea", "bytea"}, NULL},
	/* missing from DuckDB: ~~ (LIKE) !~~ (NOT LIKE) */
};

/* Text operators */
static const PGDuckShippableOperator PGDuckShippableTextOperators[] = {
	{"=", "pg_catalog", "texteq", 2, {"text", "text"}, NULL},
	{"<>", "pg_catalog", "textne", 2, {"text", "text"}, NULL},
	{"<", "pg_catalog", "textlt", 2, {"text", "text"}, NULL},
	{">", "pg_catalog", "textgt", 2, {"text", "text"}, NULL},
	{"<=", "pg_catalog", "textle", 2, {"text", "text"}, NULL},
	{">=", "pg_catalog", "textge", 2, {"text", "text"}, NULL},
	{"~~", "pg_catalog", "textlike", 2, {"text", "text"}, NULL},
	{"!~~", "pg_catalog", "textnlike", 2, {"text", "text"}, NULL},
	{"~~*", "pg_catalog", "texticlike", 2, {"text", "text"}, NULL},
	{"!~~*", "pg_catalog", "texticnlike", 2, {"text", "text"}, NULL},
	{"||", "pg_catalog", "textcat", 2, {"text", "text"}, NULL},
	{"||", "pg_catalog", "textanycat", 2, {"text", "anynonarray"}, IsTextConcatAnyNonArrayShippable},
	{"||", "pg_catalog", "anytextcat", 2, {"anynonarray", "text"}, IsTextConcatAnyNonArrayShippable},
	{"^@", "pg_catalog", "starts_with", 2, {"text", "text"}, NULL},
	{"~", "pg_catalog", "textregex", 2, {"text", "text"}, NULL},
	{"!~", "pg_catalog", "textregexne", 2, {"text", "text"}, NULL},
	{"~*", "pg_catalog", "textciregex", 2, {"text", "text"}, NULL},
	{"!~*", "pg_catalog", "textciregexne", 2, {"text", "text"}, NULL},
};

/* Char operators */
static const PGDuckShippableOperator PGDuckShippableCharOperators[] = {
	{"=", "pg_catalog", "chareq", 2, {"char", "char"}, NULL},
	{"<>", "pg_catalog", "charne", 2, {"char", "char"}, NULL},
	{"<", "pg_catalog", "charlt", 2, {"char", "char"}, NULL},
	{">", "pg_catalog", "chargt", 2, {"char", "char"}, NULL},
	{"<=", "pg_catalog", "charle", 2, {"char", "char"}, NULL},
	{">=", "pg_catalog", "charge", 2, {"char", "char"}, NULL},
};

/* BpChar operators */
static const PGDuckShippableOperator PGDuckShippableBpCharOperators[] = {
	{"=", "pg_catalog", "bpchareq", 2, {"bpchar", "bpchar"}, NULL},
	{"<>", "pg_catalog", "bpcharne", 2, {"bpchar", "bpchar"}, NULL},
	{"<", "pg_catalog", "bpcharlt", 2, {"bpchar", "bpchar"}, NULL},
	{">", "pg_catalog", "bpchargt", 2, {"bpchar", "bpchar"}, NULL},
	{"<=", "pg_catalog", "bpcharle", 2, {"bpchar", "bpchar"}, NULL},
	{">=", "pg_catalog", "bpcharge", 2, {"bpchar", "bpchar"}, NULL},
	{"~~", "pg_catalog", "bpcharlike", 2, {"bpchar", "text"}, NULL},
	{"!~~", "pg_catalog", "bpcharnlike", 2, {"bpchar", "text"}, NULL},
	{"~~*", "pg_catalog", "bpchariclike", 2, {"bpchar", "text"}, NULL},
	{"!~~*", "pg_catalog", "bpcharicnlike", 2, {"bpchar", "text"}, NULL},
};

/* Bool operators */
static const PGDuckShippableOperator PGDuckShippableBoolOperators[] = {
	{"=", "pg_catalog", "booleq", 2, {"bool", "bool"}, NULL},
	{">=", "pg_catalog", "boolge", 2, {"bool", "bool"}, NULL},
	{">", "pg_catalog", "boolgt", 2, {"bool", "bool"}, NULL},
	{"<=", "pg_catalog", "boolle", 2, {"bool", "bool"}, NULL},
	{"<", "pg_catalog", "boollt", 2, {"bool", "bool"}, NULL},
	{"<>", "pg_catalog", "boolne", 2, {"bool", "bool"}, NULL},
};

/* Float4 operators */
static const PGDuckShippableOperator PGDuckShippableFloat4Operators[] = {
	{"@", "pg_catalog", "float4abs", 1, {"float4"}, NULL},
	{"-", "pg_catalog", "float4um", 1, {"float4"}, NULL},
	{"+", "pg_catalog", "float4up", 1, {"float4"}, NULL},

	{"=", "pg_catalog", "float4eq", 2, {"float4", "float4"}, NULL},
	{"<>", "pg_catalog", "float4ne", 2, {"float4", "float4"}, NULL},
	{"<", "pg_catalog", "float4lt", 2, {"float4", "float4"}, NULL},
	{">", "pg_catalog", "float4gt", 2, {"float4", "float4"}, NULL},
	{"<=", "pg_catalog", "float4le", 2, {"float4", "float4"}, NULL},
	{">=", "pg_catalog", "float4ge", 2, {"float4", "float4"}, NULL},
	{"+", "pg_catalog", "float4pl", 2, {"float4", "float4"}, NULL},
	{"-", "pg_catalog", "float4mi", 2, {"float4", "float4"}, NULL},
	{"*", "pg_catalog", "float4mul", 2, {"float4", "float4"}, NULL},
	{"/", "pg_catalog", "float4div", 2, {"float4", "float4"}, NULL},

	{"=", "pg_catalog", "float48eq", 2, {"float4", "float8"}, NULL},
	{"<>", "pg_catalog", "float48ne", 2, {"float4", "float8"}, NULL},
	{"<", "pg_catalog", "float48lt", 2, {"float4", "float8"}, NULL},
	{">", "pg_catalog", "float48gt", 2, {"float4", "float8"}, NULL},
	{"<=", "pg_catalog", "float48le", 2, {"float4", "float8"}, NULL},
	{">=", "pg_catalog", "float48ge", 2, {"float4", "float8"}, NULL},
	{"+", "pg_catalog", "float48pl", 2, {"float4", "float8"}, NULL},
	{"-", "pg_catalog", "float48mi", 2, {"float4", "float8"}, NULL},
	{"*", "pg_catalog", "float48mul", 2, {"float4", "float8"}, NULL},
	{"/", "pg_catalog", "float48div", 2, {"float4", "float8"}, NULL},
};

/* Float8 operators */
static const PGDuckShippableOperator PGDuckShippableFloat8Operators[] = {
	{"@", "pg_catalog", "float8abs", 1, {"float8"}, NULL},
	{"-", "pg_catalog", "float8um", 1, {"float8"}, NULL},
	{"+", "pg_catalog", "float8up", 1, {"float8"}, NULL},
	{"|/", "pg_catalog", "dsqrt", 1, {"float8"}, NULL},
	{"||/", "pg_catalog", "dcbrt", 1, {"float8"}, NULL},

	{"=", "pg_catalog", "float84eq", 2, {"float8", "float4"}, NULL},
	{"<>", "pg_catalog", "float84ne", 2, {"float8", "float4"}, NULL},
	{"<", "pg_catalog", "float84lt", 2, {"float8", "float4"}, NULL},
	{">", "pg_catalog", "float84gt", 2, {"float8", "float4"}, NULL},
	{"<=", "pg_catalog", "float84le", 2, {"float8", "float4"}, NULL},
	{">=", "pg_catalog", "float84ge", 2, {"float8", "float4"}, NULL},
	{"+", "pg_catalog", "float84pl", 2, {"float8", "float4"}, NULL},
	{"-", "pg_catalog", "float84mi", 2, {"float8", "float4"}, NULL},
	{"*", "pg_catalog", "float84mul", 2, {"float8", "float4"}, NULL},
	{"/", "pg_catalog", "float84div", 2, {"float8", "float4"}, NULL},

	{"=", "pg_catalog", "float8eq", 2, {"float8", "float8"}, NULL},
	{"<>", "pg_catalog", "float8ne", 2, {"float8", "float8"}, NULL},
	{"<", "pg_catalog", "float8lt", 2, {"float8", "float8"}, NULL},
	{">", "pg_catalog", "float8gt", 2, {"float8", "float8"}, NULL},
	{"<=", "pg_catalog", "float8le", 2, {"float8", "float8"}, NULL},
	{">=", "pg_catalog", "float8ge", 2, {"float8", "float8"}, NULL},
	{"+", "pg_catalog", "float8pl", 2, {"float8", "float8"}, NULL},
	{"-", "pg_catalog", "float8mi", 2, {"float8", "float8"}, NULL},
	{"*", "pg_catalog", "float8mul", 2, {"float8", "float8"}, NULL},
	{"/", "pg_catalog", "float8div", 2, {"float8", "float8"}, NULL},
	{"^", "pg_catalog", "dpow", 2, {"float8", "float8"}, NULL},
};

/* Int2 operators */
static const PGDuckShippableOperator PGDuckShippableInt2Operators[] = {
	{"@", "pg_catalog", "int2abs", 1, {"int2"}, NULL},
	{"-", "pg_catalog", "int2um", 1, {"int2"}, NULL},
	{"+", "pg_catalog", "int2up", 1, {"int2"}, NULL},
	{"~", "pg_catalog", "int2not", 1, {"int2"}, NULL},

	{"=", "pg_catalog", "int2eq", 2, {"int2", "int2"}, NULL},
	{"<>", "pg_catalog", "int2ne", 2, {"int2", "int2"}, NULL},
	{"<", "pg_catalog", "int2lt", 2, {"int2", "int2"}, NULL},
	{">", "pg_catalog", "int2gt", 2, {"int2", "int2"}, NULL},
	{"<=", "pg_catalog", "int2le", 2, {"int2", "int2"}, NULL},
	{">=", "pg_catalog", "int2ge", 2, {"int2", "int2"}, NULL},
	{"+", "pg_catalog", "int2pl", 2, {"int2", "int2"}, NULL},
	{"-", "pg_catalog", "int2mi", 2, {"int2", "int2"}, NULL},
	{"*", "pg_catalog", "int2mul", 2, {"int2", "int2"}, NULL},
	{"/", "pg_catalog", "int2div", 2, {"int2", "int2"}, NULL},
	{"%", "pg_catalog", "int2mod", 2, {"int2", "int2"}, NULL},

	{"&", "pg_catalog", "int2and", 2, {"int2", "int2"}, NULL},
	{"|", "pg_catalog", "int2or", 2, {"int2", "int2"}, NULL},
	{"#", "pg_catalog", "int2xor", 2, {"int2", "int2"}, NULL},
	{"<<", "pg_catalog", "int2shl", 2, {"int2", "int4"}, NULL},
	{">>", "pg_catalog", "int2shr", 2, {"int2", "int4"}, NULL},

	{"=", "pg_catalog", "int24eq", 2, {"int2", "int4"}, NULL},
	{"<>", "pg_catalog", "int24ne", 2, {"int2", "int4"}, NULL},
	{"<", "pg_catalog", "int24lt", 2, {"int2", "int4"}, NULL},
	{">", "pg_catalog", "int24gt", 2, {"int2", "int4"}, NULL},
	{"<=", "pg_catalog", "int24le", 2, {"int2", "int4"}, NULL},
	{">=", "pg_catalog", "int24ge", 2, {"int2", "int4"}, NULL},
	{"+", "pg_catalog", "int24pl", 2, {"int2", "int4"}, NULL},
	{"-", "pg_catalog", "int24mi", 2, {"int2", "int4"}, NULL},
	{"*", "pg_catalog", "int24mul", 2, {"int2", "int4"}, NULL},
	{"/", "pg_catalog", "int24div", 2, {"int2", "int4"}, NULL},

	{"=", "pg_catalog", "int28eq", 2, {"int2", "int8"}, NULL},
	{"<>", "pg_catalog", "int28ne", 2, {"int2", "int8"}, NULL},
	{"<", "pg_catalog", "int28lt", 2, {"int2", "int8"}, NULL},
	{">", "pg_catalog", "int28gt", 2, {"int2", "int8"}, NULL},
	{"<=", "pg_catalog", "int28le", 2, {"int2", "int8"}, NULL},
	{">=", "pg_catalog", "int28ge", 2, {"int2", "int8"}, NULL},
	{"+", "pg_catalog", "int28pl", 2, {"int2", "int8"}, NULL},
	{"-", "pg_catalog", "int28mi", 2, {"int2", "int8"}, NULL},
	{"*", "pg_catalog", "int28mul", 2, {"int2", "int8"}, NULL},
	{"/", "pg_catalog", "int28div", 2, {"int2", "int8"}, NULL},
};

/* Int4 operators */
static const PGDuckShippableOperator PGDuckShippableInt4Operators[] = {
	{"@", "pg_catalog", "int4abs", 1, {"int4"}, NULL},
	{"-", "pg_catalog", "int4um", 1, {"int4"}, NULL},
	{"+", "pg_catalog", "int4up", 1, {"int4"}, NULL},
	{"~", "pg_catalog", "int4not", 1, {"int4"}, NULL},

	{"=", "pg_catalog", "int4eq", 2, {"int4", "int4"}, NULL},
	{"<>", "pg_catalog", "int4ne", 2, {"int4", "int4"}, NULL},
	{"<", "pg_catalog", "int4lt", 2, {"int4", "int4"}, NULL},
	{">", "pg_catalog", "int4gt", 2, {"int4", "int4"}, NULL},
	{"<=", "pg_catalog", "int4le", 2, {"int4", "int4"}, NULL},
	{">=", "pg_catalog", "int4ge", 2, {"int4", "int4"}, NULL},
	{"+", "pg_catalog", "int4pl", 2, {"int4", "int4"}, NULL},
	{"-", "pg_catalog", "int4mi", 2, {"int4", "int4"}, NULL},
	{"*", "pg_catalog", "int4mul", 2, {"int4", "int4"}, NULL},
	{"/", "pg_catalog", "int4div", 2, {"int4", "int4"}, NULL},
	{"%", "pg_catalog", "int4mod", 2, {"int4", "int4"}, NULL},

	{"&", "pg_catalog", "int4and", 2, {"int4", "int4"}, NULL},
	{"|", "pg_catalog", "int4or", 2, {"int4", "int4"}, NULL},
	{"#", "pg_catalog", "int4xor", 2, {"int4", "int4"}, NULL},
	{"<<", "pg_catalog", "int4shl", 2, {"int4", "int4"}, NULL},
	{">>", "pg_catalog", "int4shr", 2, {"int4", "int4"}, NULL},

	{"=", "pg_catalog", "int42eq", 2, {"int4", "int2"}, NULL},
	{"<>", "pg_catalog", "int42ne", 2, {"int4", "int2"}, NULL},
	{"<", "pg_catalog", "int42lt", 2, {"int4", "int2"}, NULL},
	{">", "pg_catalog", "int42gt", 2, {"int4", "int2"}, NULL},
	{"<=", "pg_catalog", "int42le", 2, {"int4", "int2"}, NULL},
	{">=", "pg_catalog", "int42ge", 2, {"int4", "int2"}, NULL},
	{"+", "pg_catalog", "int42pl", 2, {"int4", "int2"}, NULL},
	{"-", "pg_catalog", "int42mi", 2, {"int4", "int2"}, NULL},
	{"*", "pg_catalog", "int42mul", 2, {"int4", "int2"}, NULL},
	{"/", "pg_catalog", "int42div", 2, {"int4", "int2"}, NULL},

	{"=", "pg_catalog", "int48eq", 2, {"int4", "int8"}, NULL},
	{"<>", "pg_catalog", "int48ne", 2, {"int4", "int8"}, NULL},
	{"<", "pg_catalog", "int48lt", 2, {"int4", "int8"}, NULL},
	{">", "pg_catalog", "int48gt", 2, {"int4", "int8"}, NULL},
	{"<=", "pg_catalog", "int48le", 2, {"int4", "int8"}, NULL},
	{">=", "pg_catalog", "int48ge", 2, {"int4", "int8"}, NULL},
	{"+", "pg_catalog", "int48pl", 2, {"int4", "int8"}, NULL},
	{"-", "pg_catalog", "int48mi", 2, {"int4", "int8"}, NULL},
	{"*", "pg_catalog", "int48mul", 2, {"int4", "int8"}, NULL},
	{"/", "pg_catalog", "int48div", 2, {"int4", "int8"}, NULL},
};

/* Int8 operators */
static const PGDuckShippableOperator PGDuckShippableInt8Operators[] = {
	{"@", "pg_catalog", "int8abs", 1, {"int8"}, NULL},
	{"-", "pg_catalog", "int8um", 1, {"int8"}, NULL},
	{"+", "pg_catalog", "int8up", 1, {"int8"}, NULL},
	{"~", "pg_catalog", "int8not", 1, {"int8"}, NULL},

	{"=", "pg_catalog", "int82eq", 2, {"int8", "int2"}, NULL},
	{"<>", "pg_catalog", "int82ne", 2, {"int8", "int2"}, NULL},
	{"<", "pg_catalog", "int82lt", 2, {"int8", "int2"}, NULL},
	{">", "pg_catalog", "int82gt", 2, {"int8", "int2"}, NULL},
	{"<=", "pg_catalog", "int82le", 2, {"int8", "int2"}, NULL},
	{">=", "pg_catalog", "int82ge", 2, {"int8", "int2"}, NULL},
	{"+", "pg_catalog", "int82pl", 2, {"int8", "int2"}, NULL},
	{"-", "pg_catalog", "int82mi", 2, {"int8", "int2"}, NULL},
	{"*", "pg_catalog", "int82mul", 2, {"int8", "int2"}, NULL},
	{"/", "pg_catalog", "int82div", 2, {"int8", "int2"}, NULL},

	{"=", "pg_catalog", "int84eq", 2, {"int8", "int4"}, NULL},
	{"<>", "pg_catalog", "int84ne", 2, {"int8", "int4"}, NULL},
	{"<", "pg_catalog", "int84lt", 2, {"int8", "int4"}, NULL},
	{">", "pg_catalog", "int84gt", 2, {"int8", "int4"}, NULL},
	{"<=", "pg_catalog", "int84le", 2, {"int8", "int4"}, NULL},
	{">=", "pg_catalog", "int84ge", 2, {"int8", "int4"}, NULL},
	{"+", "pg_catalog", "int84pl", 2, {"int8", "int4"}, NULL},
	{"-", "pg_catalog", "int84mi", 2, {"int8", "int4"}, NULL},
	{"*", "pg_catalog", "int84mul", 2, {"int8", "int4"}, NULL},
	{"/", "pg_catalog", "int84div", 2, {"int8", "int4"}, NULL},

	{"=", "pg_catalog", "int8eq", 2, {"int8", "int8"}, NULL},
	{"<>", "pg_catalog", "int8ne", 2, {"int8", "int8"}, NULL},
	{"<", "pg_catalog", "int8lt", 2, {"int8", "int8"}, NULL},
	{">", "pg_catalog", "int8gt", 2, {"int8", "int8"}, NULL},
	{"<=", "pg_catalog", "int8le", 2, {"int8", "int8"}, NULL},
	{">=", "pg_catalog", "int8ge", 2, {"int8", "int8"}, NULL},
	{"+", "pg_catalog", "int8pl", 2, {"int8", "int8"}, NULL},
	{"-", "pg_catalog", "int8mi", 2, {"int8", "int8"}, NULL},
	{"*", "pg_catalog", "int8mul", 2, {"int8", "int8"}, NULL},
	{"/", "pg_catalog", "int8div", 2, {"int8", "int8"}, NULL},
	{"%", "pg_catalog", "int8mod", 2, {"int8", "int8"}, NULL},

	{"&", "pg_catalog", "int8and", 2, {"int8", "int8"}, NULL},
	{"|", "pg_catalog", "int8or", 2, {"int8", "int8"}, NULL},
	{"<<", "pg_catalog", "int8shl", 2, {"int8", "int4"}, NULL},
	{">>", "pg_catalog", "int8shr", 2, {"int8", "int4"}, NULL},
	{"#", "pg_catalog", "int8xor", 2, {"int8", "int8"}, NULL},

};

/* Numeric operators */
static const PGDuckShippableOperator PGDuckShippableNumericOperators[] = {
	{"@", "pg_catalog", "numeric_abs", 1, {"numeric"}, NULL},
	{"-", "pg_catalog", "numeric_uminus", 1, {"numeric"}, NULL},
	{"+", "pg_catalog", "numeric_uplus", 1, {"numeric"}, NULL},

	{"=", "pg_catalog", "numeric_eq", 2, {"numeric", "numeric"}, NULL},
	{"<>", "pg_catalog", "numeric_ne", 2, {"numeric", "numeric"}, NULL},
	{"<", "pg_catalog", "numeric_lt", 2, {"numeric", "numeric"}, NULL},
	{">", "pg_catalog", "numeric_gt", 2, {"numeric", "numeric"}, NULL},
	{"<=", "pg_catalog", "numeric_le", 2, {"numeric", "numeric"}, NULL},
	{">=", "pg_catalog", "numeric_ge", 2, {"numeric", "numeric"}, NULL},
	{"+", "pg_catalog", "numeric_pl", 2, {"numeric", "numeric"}, NULL},
	{"-", "pg_catalog", "numeric_mi", 2, {"numeric", "numeric"}, NULL},
	{"*", "pg_catalog", "numeric_mul", 2, {"numeric", "numeric"}, NULL},
	{"/", "pg_catalog", "numeric_div", 2, {"numeric", "numeric"}, NULL},
	{"%", "pg_catalog", "numeric_mod", 2, {"numeric", "numeric"}, NULL},
	{"^", "pg_catalog", "numeric_pow", 2, {"numeric", "numeric"}, NULL},
};

/* Date operators */
static const PGDuckShippableOperator PGDuckShippableDateOperators[] = {
	{"=", "pg_catalog", "date_eq", 2, {"date", "date"}, NULL},
	{"<>", "pg_catalog", "date_ne", 2, {"date", "date"}, NULL},
	{"<", "pg_catalog", "date_lt", 2, {"date", "date"}, NULL},
	{"<=", "pg_catalog", "date_le", 2, {"date", "date"}, NULL},
	{">", "pg_catalog", "date_gt", 2, {"date", "date"}, NULL},
	{">=", "pg_catalog", "date_ge", 2, {"date", "date"}, NULL},
	{"-", "pg_catalog", "date_mi", 2, {"date", "date"}, NULL},

	{"=", "pg_catalog", "date_eq_timestamp", 2, {"date", "timestamp"}, NULL},
	{">=", "pg_catalog", "date_ge_timestamp", 2, {"date", "timestamp"}, NULL},
	{">", "pg_catalog", "date_gt_timestamp", 2, {"date", "timestamp"}, NULL},
	{"<=", "pg_catalog", "date_le_timestamp", 2, {"date", "timestamp"}, NULL},
	{"<", "pg_catalog", "date_lt_timestamp", 2, {"date", "timestamp"}, NULL},
	{"<>", "pg_catalog", "date_ne_timestamp", 2, {"date", "timestamp"}, NULL},

	{"=", "pg_catalog", "date_eq_timestamptz", 2, {"date", "timestamptz"}, NULL},
	{">=", "pg_catalog", "date_ge_timestamptz", 2, {"date", "timestamptz"}, NULL},
	{">", "pg_catalog", "date_gt_timestamptz", 2, {"date", "timestamptz"}, NULL},
	{"<=", "pg_catalog", "date_le_timestamptz", 2, {"date", "timestamptz"}, NULL},
	{"<", "pg_catalog", "date_lt_timestamptz", 2, {"date", "timestamptz"}, NULL},
	{"<>", "pg_catalog", "date_ne_timestamptz", 2, {"date", "timestamptz"}, NULL},

	{"+", "pg_catalog", "date_pli", 2, {"date", "int4"}, NULL},
	{"-", "pg_catalog", "date_mii", 2, {"date", "int4"}, NULL},

	{"+", "pg_catalog", "date_pl_interval", 2, {"date", "interval"}, NULL},
	{"-", "pg_catalog", "date_mi_interval", 2, {"date", "interval"}, NULL},

	{"+", "pg_catalog", "datetime_pl", 2, {"date", "time"}, NULL},
	{"+", "pg_catalog", "datetimetz_pl", 2, {"date", "timetz"}, NULL},
};

/* Time operators */
static const PGDuckShippableOperator PGDuckShippableTimeOperators[] = {
	{"=", "pg_catalog", "time_eq", 2, {"time", "time"}, NULL},
	{"<>", "pg_catalog", "time_ne", 2, {"time", "time"}, NULL},
	{"<", "pg_catalog", "time_lt", 2, {"time", "time"}, NULL},
	{"<=", "pg_catalog", "time_le", 2, {"time", "time"}, NULL},
	{">", "pg_catalog", "time_gt", 2, {"time", "time"}, NULL},
	{">=", "pg_catalog", "time_ge", 2, {"time", "time"}, NULL},
};

/* Timetz operators */
static const PGDuckShippableOperator PGDuckShippableTimeTzOperators[] = {
	{"=", "pg_catalog", "timetz_eq", 2, {"timetz", "timetz"}, NULL},
	{"<>", "pg_catalog", "timetz_ne", 2, {"timetz", "timetz"}, NULL},
	{"<", "pg_catalog", "timetz_lt", 2, {"timetz", "timetz"}, NULL},
	{"<=", "pg_catalog", "timetz_le", 2, {"timetz", "timetz"}, NULL},
	{">", "pg_catalog", "timetz_gt", 2, {"timetz", "timetz"}, NULL},
	{">=", "pg_catalog", "timetz_ge", 2, {"timetz", "timetz"}, NULL},
};

/* Timestamp operators */
static const PGDuckShippableOperator PGDuckShippableTimestampOperators[] = {
	{"=", "pg_catalog", "timestamp_eq", 2, {"timestamp", "timestamp"}, NULL},
	{"<>", "pg_catalog", "timestamp_ne", 2, {"timestamp", "timestamp"}, NULL},
	{"<", "pg_catalog", "timestamp_lt", 2, {"timestamp", "timestamp"}, NULL},
	{"<=", "pg_catalog", "timestamp_le", 2, {"timestamp", "timestamp"}, NULL},
	{">", "pg_catalog", "timestamp_gt", 2, {"timestamp", "timestamp"}, NULL},
	{">=", "pg_catalog", "timestamp_ge", 2, {"timestamp", "timestamp"}, NULL},
	{"-", "pg_catalog", "timestamp_mi", 2, {"timestamp", "timestamp"}, NULL},

	{"+", "pg_catalog", "timestamp_pl_interval", 2, {"timestamp", "interval"}, NULL},
	{"-", "pg_catalog", "timestamp_mi_interval", 2, {"timestamp", "interval"}, NULL},

	{"=", "pg_catalog", "timestamp_eq_date", 2, {"timestamp", "date"}, NULL},
	{">=", "pg_catalog", "timestamp_ge_date", 2, {"timestamp", "date"}, NULL},
	{">", "pg_catalog", "timestamp_gt_date", 2, {"timestamp", "date"}, NULL},
	{"<=", "pg_catalog", "timestamp_le_date", 2, {"timestamp", "date"}, NULL},
	{"<", "pg_catalog", "timestamp_lt_date", 2, {"timestamp", "date"}, NULL},
	{"<>", "pg_catalog", "timestamp_ne_date", 2, {"timestamp", "date"}, NULL},

	{"=", "pg_catalog", "timestamp_eq_timestamptz", 2, {"timestamp", "timestamptz"}, NULL},
	{">=", "pg_catalog", "timestamp_ge_timestamptz", 2, {"timestamp", "timestamptz"}, NULL},
	{">", "pg_catalog", "timestamp_gt_timestamptz", 2, {"timestamp", "timestamptz"}, NULL},
	{"<=", "pg_catalog", "timestamp_le_timestamptz", 2, {"timestamp", "timestamptz"}, NULL},
	{"<", "pg_catalog", "timestamp_lt_timestamptz", 2, {"timestamp", "timestamptz"}, NULL},
	{"<>", "pg_catalog", "timestamp_ne_timestamptz", 2, {"timestamp", "timestamptz"}, NULL},
};

/* Timestamptz operators */
static const PGDuckShippableOperator PGDuckShippableTimestampTzOperators[] = {
	{"=", "pg_catalog", "timestamptz_eq", 2, {"timestamptz", "timestamptz"}, NULL},
	{"<>", "pg_catalog", "timestamptz_ne", 2, {"timestamptz", "timestamptz"}, NULL},
	{"<", "pg_catalog", "timestamptz_lt", 2, {"timestamptz", "timestamptz"}, NULL},
	{"<=", "pg_catalog", "timestamptz_le", 2, {"timestamptz", "timestamptz"}, NULL},
	{">", "pg_catalog", "timestamptz_gt", 2, {"timestamptz", "timestamptz"}, NULL},
	{">=", "pg_catalog", "timestamptz_ge", 2, {"timestamptz", "timestamptz"}, NULL},
	{"-", "pg_catalog", "timestamptz_mi", 2, {"timestamptz", "timestamptz"}, NULL},

	{"+", "pg_catalog", "timestamptz_pl_interval", 2, {"timestamptz", "interval"}, NULL},
	{"-", "pg_catalog", "timestamptz_mi_interval", 2, {"timestamptz", "interval"}, NULL},

	{"=", "pg_catalog", "timestamptz_eq_date", 2, {"timestamptz", "date"}, NULL},
	{">=", "pg_catalog", "timestamptz_ge_date", 2, {"timestamptz", "date"}, NULL},
	{">", "pg_catalog", "timestamptz_gt_date", 2, {"timestamptz", "date"}, NULL},
	{"<=", "pg_catalog", "timestamptz_le_date", 2, {"timestamptz", "date"}, NULL},
	{"<", "pg_catalog", "timestamptz_lt_date", 2, {"timestamptz", "date"}, NULL},
	{"<>", "pg_catalog", "timestamptz_ne_date", 2, {"timestamptz", "date"}, NULL},

	{"=", "pg_catalog", "timestamptz_eq_timestamp", 2, {"timestamptz", "timestamp"}, NULL},
	{">=", "pg_catalog", "timestamptz_ge_timestamp", 2, {"timestamptz", "timestamp"}, NULL},
	{">", "pg_catalog", "timestamptz_gt_timestamp", 2, {"timestamptz", "timestamp"}, NULL},
	{"<=", "pg_catalog", "timestamptz_le_timestamp", 2, {"timestamptz", "timestamp"}, NULL},
	{"<", "pg_catalog", "timestamptz_lt_timestamp", 2, {"timestamptz", "timestamp"}, NULL},
	{"<>", "pg_catalog", "timestamptz_ne_timestamp", 2, {"timestamptz", "timestamp"}, NULL},
};

/* Interval operators */
static const PGDuckShippableOperator PGDuckShippableIntervalOperators[] = {
	{"=", "pg_catalog", "interval_eq", 2, {"interval", "interval"}, NULL},
	{"<>", "pg_catalog", "interval_ne", 2, {"interval", "interval"}, NULL},
	{"<", "pg_catalog", "interval_lt", 2, {"interval", "interval"}, NULL},
	{"<=", "pg_catalog", "interval_le", 2, {"interval", "interval"}, NULL},
	{">", "pg_catalog", "interval_gt", 2, {"interval", "interval"}, NULL},
	{">=", "pg_catalog", "interval_ge", 2, {"interval", "interval"}, NULL},
	{"+", "pg_catalog", "interval_pl", 2, {"interval", "interval"}, NULL},
	{"-", "pg_catalog", "interval_mi", 2, {"interval", "interval"}, NULL},
};

/* UUID operators */
static const PGDuckShippableOperator PGDuckShippableUUIDOperators[] = {
	{"=", "pg_catalog", "uuid_eq", 2, {"uuid", "uuid"}, NULL},
	{"<>", "pg_catalog", "uuid_ne", 2, {"uuid", "uuid"}, NULL},
	{"<", "pg_catalog", "uuid_lt", 2, {"uuid", "uuid"}, NULL},
	{"<=", "pg_catalog", "uuid_le", 2, {"uuid", "uuid"}, NULL},
	{">", "pg_catalog", "uuid_gt", 2, {"uuid", "uuid"}, NULL},
	{">=", "pg_catalog", "uuid_ge", 2, {"uuid", "uuid"}, NULL},
};

/* anyarray operators */
static const PGDuckShippableOperator PGDuckShippableArrayOperators[] = {
	{"=", "pg_catalog", "array_eq", 2, {"anyarray", "anyarray"}, NULL},
	{"<>", "pg_catalog", "array_ne", 2, {"anyarray", "anyarray"}, NULL},
	{"<", "pg_catalog", "array_lt", 2, {"anyarray", "anyarray"}, NULL},
	{"<=", "pg_catalog", "array_le", 2, {"anyarray", "anyarray"}, NULL},
	{">", "pg_catalog", "array_gt", 2, {"anyarray", "anyarray"}, NULL},
	{">=", "pg_catalog", "array_ge", 2, {"anyarray", "anyarray"}, NULL},
	{"&&", "pg_catalog", "arrayoverlap", 2, {"anyarray", "anyarray"}, NULL},
	/* @> and <@ behave differently for NULL */
	{"||", "pg_catalog", "array_append", 2, {"anycompatiblearray", "anycompatible"}, NULL},
	{"||", "pg_catalog", "array_cat", 2, {"anycompatiblearray", "anycompatiblearray"}, NULL},
};

/* json operators */
static const PGDuckShippableOperator PGDuckShippableJsonOperators[] = {
	{"->", "pg_catalog", "json_object_field_text", 2, {"json", "text"}, NULL},
	{"->", "pg_catalog", "json_array_element_text", 2, {"json", "int4"}, NULL},
	{"->>", "pg_catalog", "json_object_field", 2, {"json", "text"}, NULL},
	{"->>", "pg_catalog", "json_array_element", 2, {"json", "int4"}, NULL},
};

/* jsonb operators */
static const PGDuckShippableOperator PGDuckShippableJsonbOperators[] = {
	{"=", "pg_catalog", "jsonb_eq", 2, {"jsonb", "jsonb"}, NULL},
	{"->", "pg_catalog", "jsonb_object_field_text", 2, {"jsonb", "text"}, NULL},
	{"->", "pg_catalog", "jsonb_array_element_text", 2, {"jsonb", "int4"}, NULL},
	{"->>", "pg_catalog", "jsonb_object_field", 2, {"jsonb", "text"}, NULL},
	{"->>", "pg_catalog", "jsonb_array_element", 2, {"jsonb", "int4"}, NULL},
};

static const PGDuckShippableOperatorsByType PGDuckShippableOperatorsByTypes[] = {
	SHIPPABLE_OPERATORS_BY_TYPE("bytea", PGDuckShippableByteaOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("bpchar", PGDuckShippableBpCharOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("char", PGDuckShippableCharOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("text", PGDuckShippableTextOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("bool", PGDuckShippableBoolOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("float4", PGDuckShippableFloat4Operators),
	SHIPPABLE_OPERATORS_BY_TYPE("float8", PGDuckShippableFloat8Operators),
	SHIPPABLE_OPERATORS_BY_TYPE("int2", PGDuckShippableInt2Operators),
	SHIPPABLE_OPERATORS_BY_TYPE("int4", PGDuckShippableInt4Operators),
	SHIPPABLE_OPERATORS_BY_TYPE("int8", PGDuckShippableInt8Operators),
	SHIPPABLE_OPERATORS_BY_TYPE("numeric", PGDuckShippableNumericOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("date", PGDuckShippableDateOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("time", PGDuckShippableTimeOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("timetz", PGDuckShippableTimeTzOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("timestamp", PGDuckShippableTimestampOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("timestamptz", PGDuckShippableTimestampTzOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("interval", PGDuckShippableIntervalOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("uuid", PGDuckShippableUUIDOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("array", PGDuckShippableArrayOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("json", PGDuckShippableJsonOperators),
	SHIPPABLE_OPERATORS_BY_TYPE("jsonb", PGDuckShippableJsonbOperators),
};

/*
 * GetPGDuckShippableOperatorsByType returns all shippable built-in operators,
 * group by type.
 */
const PGDuckShippableOperatorsByType *
GetPGDuckShippableOperatorsByType(int *sizePointer)
{
	*sizePointer = ARRAY_SIZE(PGDuckShippableOperatorsByTypes);

	return PGDuckShippableOperatorsByTypes;
}


/*
 * IsTextConcatAnyNonArrayShippable termines whether the text || anynonarray
 * operator is shippable.
 */
static bool
IsTextConcatAnyNonArrayShippable(Node *node)
{
	OpExpr	   *opExpr = castNode(OpExpr, node);

	ListCell   *concatArgCell = NULL;

	foreach(concatArgCell, opExpr->args)
	{
		Node	   *concatArg = (Node *) lfirst(concatArgCell);
		Oid			argType = exprType(concatArg);

		/*
		 * These are mostly our base types except bytea, since it serializes
		 * differently. Note that a similar check for concat() excludes
		 * boolean because it serializes differently in DuckDB, but || behaves
		 * a bit differently and is consistent.
		 */
		if (!(argType == UNKNOWNOID || argType == BOOLOID ||
			  argType == TEXTOID || argType == VARCHAROID ||
			  argType == BPCHAROID || argType == CHAROID ||
			  argType == INT2OID || argType == INT4OID || argType == INT8OID ||
			  argType == FLOAT4OID || argType == FLOAT8OID || argType == NUMERICOID ||
			  argType == DATEOID || argType == TIMEOID || argType == TIMETZOID ||
			  argType == TIMESTAMPOID || argType == TIMESTAMPTZOID ||
			  argType == INTERVALOID ||
			  argType == JSONOID || argType == JSONBOID ||
			  argType == UUIDOID))
		{
			return false;
		}
	}

	return true;
}
