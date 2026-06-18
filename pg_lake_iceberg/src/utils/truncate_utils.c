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
#include "pg_extension_base/pg_compat.h"
#include "mb/pg_wchar.h"
#include "utils/builtins.h"
#include "utils/fmgrprotos.h"
#include "utils/numeric.h"
#include "varatt.h"

#include "pg_lake/iceberg/truncate_utils.h"
#include "pg_lake/util/numeric.h"


/*
 * IcebergTruncateTransformInt32 truncates an int32 datum to the specified width
 * according to the Iceberg spec.
 *
 * Formula: v - (((v % W) + W) % W)
 */
Datum
IcebergTruncateTransformInt32(Datum value, int64_t width)
{
	int32_t		val = DatumGetInt32(value);

	int32_t		truncatedVal = val - (((val % width) + width) % width);

	return Int32GetDatum(truncatedVal);
}


/*
 * IcebergTruncateTransformInt64 truncates an int64 datum to the specified width
 * according to the Iceberg spec.
 *
 * Formula: v - (((v % W) + W) % W)
 */
Datum
IcebergTruncateTransformInt64(Datum value, int64_t width)
{
	int64_t		val = DatumGetInt64(value);

	int64_t		truncatedVal = val - (((val % width) + width) % width);

	return Int64GetDatum(truncatedVal);
}


/*
 * IcebergTruncateTransformNumeric truncates a numeric datum to the specified width
 * according to the Iceberg spec.
 *
 * Formula: v - (((unscaledValue % unscaledWidth) + unscaledWidth) % unscaledWidth)
 *
 * unscaledValue = v * 10^scale e.g. 123.45 -> 12345
 * We apply the formula on the unscaled value and finally rescale the result.
 *
 * WARNING: This function is currently not safe to use.
 * See issue https://github.com/apache/iceberg/issues/12915
 */
Datum
IcebergTruncateTransformNumeric(Datum value, int32_t typmod, int64_t width)
{
	/* remove typmod to not overflow in following operations */
	Numeric		val = DatumGetNumeric(DirectFunctionCall3(numeric_in,
														  DirectFunctionCall1(numeric_out, value),
														  ObjectIdGetDatum(InvalidOid),
														  Int32GetDatum(-1)));

	/* unscaled value */
	int32_t		scale = numeric_typmod_scale(typmod);

	Numeric		scaleMultiplier;
	Numeric		unscaledValue;

	if (scale > 0)
	{
		/* we multiply the numeric value by 10^scale to get the unscaled value */
		const char *scaleMultiplierText = psprintf("1%0*d", scale, 0);

		scaleMultiplier = DatumGetNumeric(DirectFunctionCall3(numeric_in,
															  CStringGetDatum(scaleMultiplierText),
															  ObjectIdGetDatum(InvalidOid),
															  Int32GetDatum(-1)));
		unscaledValue = numeric_mul_opt_error(val, scaleMultiplier, NULL);
	}
	else
	{
		unscaledValue = val;
	}

	/* unscaled width */
	Numeric		unscaledWidth = int64_to_numeric(width);

	/* remainder = (unscaledValue % unscaledWidth) */
	Numeric		remainder = numeric_mod_opt_error(unscaledValue, unscaledWidth, NULL);

	/* remainder = (remainder + unscaledWidth) */
	remainder = numeric_add_opt_error(remainder, unscaledWidth, NULL);

	/* remainder = (remainder + unscaledWidth) % unscaledWidth */
	remainder = numeric_mod_opt_error(remainder, unscaledWidth, NULL);

	/* divide by scale */
	if (scale > 0)
		remainder = numeric_div_opt_error(remainder, scaleMultiplier, NULL);

	/* result = value - remainder */
	Numeric		result = numeric_sub_opt_error(val, remainder, NULL);

	/* back to original typmod */
	return DirectFunctionCall3(numeric_in,
							   DirectFunctionCall1(numeric_out, NumericGetDatum(result)),
							   ObjectIdGetDatum(InvalidOid),
							   Int32GetDatum(typmod));
}


/*
 * IcebergTruncateTransformText truncates a text datum to the specified length
 * according to the Iceberg spec. Strings are truncated to a valid UTF-8 string
 * with no more than L code points.
 *
 * Formula: v.substring(0, L)
 */
Datum
IcebergTruncateTransformText(Datum value, int64_t truncateLength)
{
	const char *val = TextDatumGetCString(value);

	size_t		valLength = strlen(val);

	if (valLength > truncateLength)
	{
		char	   *utf8Text = pg_server_to_any(val, valLength, PG_UTF8);

		/*
		 * char length that holds ≤ limit_cp code points and ends on a
		 * character boundary (no half‑cut multibyte sequence)
		 */
		int			utf8Length = pg_mbcharcliplen(utf8Text, valLength, truncateLength);

		const char *truncatedValue = pnstrdup(utf8Text, utf8Length);

		return CStringGetTextDatum(truncatedValue);
	}
	else
	{
		return value;
	}
}


/*
 * IcebergTruncateTransformBytea truncates a bytea datum to the specified length
 * according to the Iceberg spec.
 *
 * Formula: v.subarray(0, L)
 */
Datum
IcebergTruncateTransformBytea(Datum value, int64_t length)
{
	bytea	   *val = DatumGetByteaP(value);

	size_t		totalDataBytes = VARSIZE(val) - VARHDRSZ;

	if (totalDataBytes > length)
	{
		bytea	   *truncatedVal = palloc(length + VARHDRSZ);

		SET_VARSIZE(truncatedVal, length + VARHDRSZ);
		memcpy(VARDATA(truncatedVal), VARDATA(val), length);

		return PointerGetDatum(truncatedVal);
	}
	else
	{
		return value;
	}
}
