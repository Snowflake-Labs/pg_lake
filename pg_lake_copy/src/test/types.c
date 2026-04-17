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

/*
 * User-defined functions for type handling and struct parsing.
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "pg_lake/pgduck/type.h"
#include "pg_lake/pgduck/parse_struct.h"
#include "utils/builtins.h"


PG_FUNCTION_INFO_V1(duckdb_type_by_name);
PG_FUNCTION_INFO_V1(duckdb_name_by_type);
PG_FUNCTION_INFO_V1(duckdb_parse_struct);

static void output_composite_type_rows(ReturnSetInfo *rsinfo, CompositeType *parsedType, int level);

/*
 * lookup pgduck type by pgduck name
 */
Datum
duckdb_type_by_name(PG_FUNCTION_ARGS)
{
	char	   *queryString = text_to_cstring(PG_GETARG_TEXT_P(0));

	DuckDBType	duckdbType = GetDuckDBTypeByName((const char *) queryString);

	if (duckdbType == DUCKDB_TYPE_INVALID)
		PG_RETURN_NULL();

	PG_RETURN_INT32(duckdbType);
}

/*
 * lookup pgduck name by pgduck type
 */
Datum
duckdb_name_by_type(PG_FUNCTION_ARGS)
{
	DuckDBType	duckdbType = PG_GETARG_INT32(0);

	PG_RETURN_TEXT_P(cstring_to_text(GetDuckDBTypeName(duckdbType)));
}


/* parse struct type returning a set of name and types found */
Datum
duckdb_parse_struct(PG_FUNCTION_ARGS)
{
	char	   *structString = text_to_cstring(PG_GETARG_TEXT_P(0));
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	TupleDesc	tupdesc;

	InitMaterializedSRF(fcinfo, 0);

	tupdesc = CreateTemplateTupleDesc(5);

	TupleDescInitEntry(tupdesc, (AttrNumber) 1, "name", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 2, "type", TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 3, "typeid", INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 4, "isarray", BOOLOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) 5, "level", INT4OID, -1, 0);

	tupdesc = BlessTupleDesc(tupdesc);

	CompositeType *parsedType = ParseStructString(structString);

	if (parsedType)
		output_composite_type_rows(rsinfo, parsedType, 0);

	PG_RETURN_VOID();
}

/* recursive helper for above */
static void
output_composite_type_rows(ReturnSetInfo *rsinfo, CompositeType *parsedType, int level)
{
	if (parsedType->cols)
	{
		ListCell   *lc;

		foreach(lc, parsedType->cols)
		{
			CompositeCol *column = (CompositeCol *) lfirst(lc);
			Datum		values[5];
			bool		nulls[5] = {0};

			/* gather info about current column */
			values[0] = CStringGetTextDatum(column->colName);
			values[1] = CStringGetTextDatum(column->colTypeName);
			values[2] = column->colType;
			values[3] = BoolGetDatum(column->isArray);
			values[4] = level;

			tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);

			if (column->subStruct)
				output_composite_type_rows(rsinfo, column->subStruct, level + 1);
		}
	}
}
