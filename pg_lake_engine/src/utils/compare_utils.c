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
#include "catalog/pg_collation.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"

#include "pg_lake/util/compare_utils.h"

/*
 * IsDatumGreaterThan checks if the first datum is greater than the second datum
 * based on the provided PGType.
 *
 * All '<' on all iceberg types are already shippable to duckdb. We need to be
 * careful about that.
 */
bool
IsDatumGreaterThan(Datum v1, Datum v2, PGType pgType)
{
	switch (pgType.postgresTypeOid)
	{
		case BOOLOID:
			return DatumGetBool(DirectFunctionCall2(boolgt, v1, v2));
		case INT2OID:
			return DatumGetBool(DirectFunctionCall2(int2gt, v1, v2));
		case INT4OID:
			return DatumGetBool(DirectFunctionCall2(int4gt, v1, v2));
		case INT8OID:
			return DatumGetBool(DirectFunctionCall2(int8gt, v1, v2));
		case FLOAT4OID:
			return DatumGetBool(DirectFunctionCall2(float4gt, v1, v2));
		case FLOAT8OID:
			return DatumGetBool(DirectFunctionCall2(float8gt, v1, v2));
		case NUMERICOID:
			return DatumGetBool(DirectFunctionCall2(numeric_gt, v1, v2));
		case DATEOID:
			return DatumGetBool(DirectFunctionCall2(date_gt, v1, v2));
		case TIMESTAMPOID:
			return DatumGetBool(DirectFunctionCall2(timestamp_gt, v1, v2));
		case TIMESTAMPTZOID:
			return DatumGetBool(DirectFunctionCall2(timestamp_gt, v1, v2));
		case TIMEOID:
			return DatumGetBool(DirectFunctionCall2(time_gt, v1, v2));
		case TIMETZOID:
			return DatumGetBool(DirectFunctionCall2(timetz_gt, v1, v2));
		case TEXTOID:
		case VARCHAROID:
			return DatumGetBool(DirectFunctionCall2Coll(text_gt, DEFAULT_COLLATION_OID, v1, v2));
		case BPCHAROID:
			return DatumGetBool(DirectFunctionCall2Coll(bpchargt, DEFAULT_COLLATION_OID, v1, v2));
		case BYTEAOID:
			return DatumGetBool(DirectFunctionCall2(byteagt, v1, v2));
		case UUIDOID:
			return DatumGetBool(DirectFunctionCall2(uuid_gt, v1, v2));
		default:
			ereport(ERROR, (errmsg("Unsupported type for > comparison: %s",
								   format_type_be(pgType.postgresTypeOid))));
			return false;		/* unreachable */
	}
}


/*
 * IsDatumLessThan checks if the first datum is less than the second datum
 * based on the provided PGType.
 *
 * All '>' on all iceberg types are already shippable to duckdb. We need to be
 * careful about that.
 */
bool
IsDatumLessThan(Datum v1, Datum v2, PGType pgType)
{
	switch (pgType.postgresTypeOid)
	{
		case BOOLOID:
			return DatumGetBool(DirectFunctionCall2(boollt, v1, v2));
		case INT2OID:
			return DatumGetBool(DirectFunctionCall2(int2lt, v1, v2));
		case INT4OID:
			return DatumGetBool(DirectFunctionCall2(int4lt, v1, v2));
		case INT8OID:
			return DatumGetBool(DirectFunctionCall2(int8lt, v1, v2));
		case FLOAT4OID:
			return DatumGetBool(DirectFunctionCall2(float4lt, v1, v2));
		case FLOAT8OID:
			return DatumGetBool(DirectFunctionCall2(float8lt, v1, v2));
		case NUMERICOID:
			return DatumGetBool(DirectFunctionCall2(numeric_lt, v1, v2));
		case DATEOID:
			return DatumGetBool(DirectFunctionCall2(date_lt, v1, v2));
		case TIMESTAMPOID:
			return DatumGetBool(DirectFunctionCall2(timestamp_lt, v1, v2));
		case TIMESTAMPTZOID:
			return DatumGetBool(DirectFunctionCall2(timestamp_lt, v1, v2));
		case TIMEOID:
			return DatumGetBool(DirectFunctionCall2(time_lt, v1, v2));
		case TIMETZOID:
			return DatumGetBool(DirectFunctionCall2(timetz_lt, v1, v2));
		case TEXTOID:
		case VARCHAROID:
			return DatumGetBool(DirectFunctionCall2Coll(text_lt, DEFAULT_COLLATION_OID, v1, v2));
		case BPCHAROID:
			return DatumGetBool(DirectFunctionCall2Coll(bpcharlt, DEFAULT_COLLATION_OID, v1, v2));
		case BYTEAOID:
			return DatumGetBool(DirectFunctionCall2(bytealt, v1, v2));
		case UUIDOID:
			return DatumGetBool(DirectFunctionCall2(uuid_lt, v1, v2));
		default:
			ereport(ERROR, (errmsg("Unsupported type for < comparison: %s",
								   format_type_be(pgType.postgresTypeOid))));
			return false;		/* unreachable */
	}
}
