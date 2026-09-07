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
 * type_map.c
 *
 * Snowflake types and their Postgres equivalents, in both directions: which
 * Postgres type a column is given when a table is attached, how a value the SQL
 * API sent is turned into a Datum, and how a Datum is written as a Snowflake
 * literal when a comparison is pushed down.
 *
 * The wire forms are what the "jsonv2" result format sends, where every value
 * is a JSON string:
 *
 *   fixed          the decimal number, already scaled ("1.25")
 *   real           the decimal number ("3.3999999999999999")
 *   text           the string itself
 *   boolean        "true" or "false"
 *   date           whole days since 1970-01-01 ("18705")
 *   time           seconds since midnight, 9 decimals ("45296.123000000")
 *   timestamp_ntz  seconds since the epoch of the wall clock reading
 *   timestamp_ltz  seconds since the epoch
 *   timestamp_tz   seconds since the epoch, a space, the offset in minutes + 1440
 *   binary         hexadecimal, because BINARY_OUTPUT_FORMAT is pinned to HEX
 *   variant etc.   the JSON text
 *
 * Most of those are already valid Postgres input syntax, so they go straight
 * into the input function of the target type. The date and time types are the
 * exception and are computed, never parsed from a formatted string, so that a
 * fractional second cannot be lost to a double.
 */

#include "postgres.h"
#include "fmgr.h"

#include <math.h>

#include "catalog/pg_type.h"
#include "lib/stringinfo.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/lsyscache.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"

#include "pg_lake_snowflake/type_map.h"

/* days between the Postgres epoch (2000-01-01) and the Unix epoch */
#define SNOWFLAKE_EPOCH_DAY_OFFSET (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE)

/* seconds between the two epochs */
#define SNOWFLAKE_EPOCH_SECOND_OFFSET \
	((int64) SNOWFLAKE_EPOCH_DAY_OFFSET * SECS_PER_DAY)

/*
 * The offset Snowflake adds to the time zone offset of a timestamp_tz value so
 * that it never has to send a sign.
 */
#define SNOWFLAKE_TZ_OFFSET_BIAS_MINUTES 1440

typedef struct SnowflakeTypeName
{
	const char *typeName;
	SnowflakeTypeCode typeCode;
}			SnowflakeTypeName;

static SnowflakeTypeName SnowflakeTypeNames[] =
{
	{
		"fixed", SNOWFLAKE_TYPE_FIXED
	},
	{
		"real", SNOWFLAKE_TYPE_REAL
	},
	{
		"text", SNOWFLAKE_TYPE_TEXT
	},
	{
		"boolean", SNOWFLAKE_TYPE_BOOLEAN
	},
	{
		"date", SNOWFLAKE_TYPE_DATE
	},
	{
		"time", SNOWFLAKE_TYPE_TIME
	},
	{
		"timestamp_ntz", SNOWFLAKE_TYPE_TIMESTAMP_NTZ
	},
	{
		"timestamp_ltz", SNOWFLAKE_TYPE_TIMESTAMP_LTZ
	},
	{
		"timestamp_tz", SNOWFLAKE_TYPE_TIMESTAMP_TZ
	},
	{
		"binary", SNOWFLAKE_TYPE_BINARY
	},
	{
		"variant", SNOWFLAKE_TYPE_VARIANT
	},
	{
		"object", SNOWFLAKE_TYPE_OBJECT
	},
	{
		"array", SNOWFLAKE_TYPE_ARRAY
	},
	{
		"map", SNOWFLAKE_TYPE_MAP
	},
	{
		"geography", SNOWFLAKE_TYPE_GEOGRAPHY
	},
	{
		"geometry", SNOWFLAKE_TYPE_GEOMETRY
	},
	{
		"vector", SNOWFLAKE_TYPE_VECTOR
	},
	{
		NULL, SNOWFLAKE_TYPE_UNKNOWN
	}
};

static void SnowflakeNaturalDatum(const char *value, SnowflakeResultColumn * column,
								  Datum *naturalValue, Oid *naturalTypeId);
static char *OutputDatum(Datum value, Oid typeId);
static int64 ParseFractionalSeconds(const char *value, int64 *wholeSeconds);
static char *FormatSnowflakeStringLiteral(const char *value);
static char *FormatLiteral(Datum value, Oid typeId, bool forValuesClause);


/*
 * SnowflakeTypeCodeFromName maps the type of a result column to its code.
 */
SnowflakeTypeCode
SnowflakeTypeCodeFromName(const char *typeName)
{
	if (typeName == NULL)
		return SNOWFLAKE_TYPE_UNKNOWN;

	for (const SnowflakeTypeName * entry = SnowflakeTypeNames; entry->typeName; entry++)
	{
		if (pg_strcasecmp(entry->typeName, typeName) == 0)
			return entry->typeCode;
	}

	return SNOWFLAKE_TYPE_UNKNOWN;
}


/*
 * SnowflakeColumnPostgresType returns the Postgres type a Snowflake column is
 * attached as.
 *
 * A NUMBER without a scale becomes an integer type when its precision fits one,
 * because that is what a Postgres query expects to join and aggregate. Anything
 * character-like becomes text rather than a length-limited varchar: the API
 * reports the length of the expression rather than of the column, and a
 * truncation error on the Postgres side of a read-only table would be nothing
 * but an obstacle.
 */
void
SnowflakeColumnPostgresType(SnowflakeResultColumn * column, Oid *typeId, int32 *typeMod)
{
	*typeMod = -1;

	switch (column->typeCode)
	{
		case SNOWFLAKE_TYPE_FIXED:
			if (column->scale == 0 && column->precision >= 1 && column->precision <= 9)
				*typeId = INT4OID;
			else if (column->scale == 0 && column->precision >= 1 &&
					 column->precision <= 18)
				*typeId = INT8OID;
			else
			{
				*typeId = NUMERICOID;

				if (column->precision >= 1 && column->precision <= 1000 &&
					column->scale >= 0 && column->scale <= column->precision)
				{
					*typeMod = ((column->precision << 16) | column->scale) + VARHDRSZ;
				}
			}
			break;

		case SNOWFLAKE_TYPE_REAL:
			*typeId = FLOAT8OID;
			break;

		case SNOWFLAKE_TYPE_BOOLEAN:
			*typeId = BOOLOID;
			break;

		case SNOWFLAKE_TYPE_DATE:
			*typeId = DATEOID;
			break;

		case SNOWFLAKE_TYPE_TIME:
			*typeId = TIMEOID;
			break;

		case SNOWFLAKE_TYPE_TIMESTAMP_NTZ:
			*typeId = TIMESTAMPOID;
			break;

		case SNOWFLAKE_TYPE_TIMESTAMP_LTZ:
		case SNOWFLAKE_TYPE_TIMESTAMP_TZ:
			*typeId = TIMESTAMPTZOID;
			break;

		case SNOWFLAKE_TYPE_BINARY:
			*typeId = BYTEAOID;
			break;

		case SNOWFLAKE_TYPE_VARIANT:
		case SNOWFLAKE_TYPE_OBJECT:
		case SNOWFLAKE_TYPE_ARRAY:
		case SNOWFLAKE_TYPE_MAP:
		case SNOWFLAKE_TYPE_VECTOR:
			*typeId = JSONBOID;
			break;

		case SNOWFLAKE_TYPE_TEXT:
		case SNOWFLAKE_TYPE_GEOGRAPHY:
		case SNOWFLAKE_TYPE_GEOMETRY:
		case SNOWFLAKE_TYPE_UNKNOWN:
		default:
			*typeId = TEXTOID;
			break;
	}
}


/*
 * SnowflakeInitTypeConversion looks up the input function of a target type once
 * per scan rather than once per value.
 */
void
SnowflakeInitTypeConversion(SnowflakeTypeConversion * conversion, Oid typeId,
							int32 typeMod)
{
	Oid			inputFunctionId = InvalidOid;

	conversion->typeId = typeId;
	conversion->typeMod = typeMod;

	getTypeInputInfo(typeId, &inputFunctionId, &conversion->inputParameterType);
	fmgr_info(inputFunctionId, &conversion->inputFunction);
}


/*
 * SnowflakeValueToDatum converts the text of one result value into a Datum of
 * the type the Postgres column is declared as.
 *
 * The date and time types are computed into their natural Postgres type. When
 * the column was declared as something else, the natural value is rendered and
 * re-parsed, which is slow but only happens for a column whose declared type
 * disagrees with the Snowflake one.
 */
Datum
SnowflakeValueToDatum(const char *value, SnowflakeResultColumn * column,
					  SnowflakeTypeConversion * conversion)
{
	const char *inputText = value;

	switch (column->typeCode)
	{
		case SNOWFLAKE_TYPE_DATE:
		case SNOWFLAKE_TYPE_TIME:
		case SNOWFLAKE_TYPE_TIMESTAMP_NTZ:
		case SNOWFLAKE_TYPE_TIMESTAMP_LTZ:
		case SNOWFLAKE_TYPE_TIMESTAMP_TZ:
			{
				Datum		naturalValue = 0;
				Oid			naturalTypeId = InvalidOid;

				SnowflakeNaturalDatum(value, column, &naturalValue, &naturalTypeId);

				if (naturalTypeId == conversion->typeId)
					return naturalValue;

				inputText = OutputDatum(naturalValue, naturalTypeId);
				break;
			}

		case SNOWFLAKE_TYPE_BINARY:

			/*
			 * Hexadecimal, which is what BINARY_OUTPUT_FORMAT is pinned to,
			 * is the input syntax of bytea once it is marked as such.
			 */
			inputText = psprintf("\\x%s", value);
			break;

		default:
			/* the wire form is already valid input syntax */
			break;
	}

	return InputFunctionCall(&conversion->inputFunction, (char *) inputText,
							 conversion->inputParameterType, conversion->typeMod);
}


/*
 * SnowflakeNaturalDatum computes the Postgres value of a date or time type from
 * the number of days or seconds the SQL API sent.
 */
static void
SnowflakeNaturalDatum(const char *value, SnowflakeResultColumn * column,
					  Datum *naturalValue, Oid *naturalTypeId)
{
	switch (column->typeCode)
	{
		case SNOWFLAKE_TYPE_DATE:
			{
				int64		unixDays = strtoll(value, NULL, 10);

				*naturalTypeId = DATEOID;
				*naturalValue = DateADTGetDatum((DateADT)
												(unixDays - SNOWFLAKE_EPOCH_DAY_OFFSET));
				break;
			}

		case SNOWFLAKE_TYPE_TIME:
			{
				int64		wholeSeconds = 0;
				int64		microseconds = ParseFractionalSeconds(value, &wholeSeconds);

				*naturalTypeId = TIMEOID;
				*naturalValue = TimeADTGetDatum((TimeADT)
												(wholeSeconds * USECS_PER_SEC +
												 microseconds));
				break;
			}

		case SNOWFLAKE_TYPE_TIMESTAMP_NTZ:
		case SNOWFLAKE_TYPE_TIMESTAMP_LTZ:
		case SNOWFLAKE_TYPE_TIMESTAMP_TZ:
			{
				int64		wholeSeconds = 0;
				int64		microseconds = ParseFractionalSeconds(value, &wholeSeconds);
				int64		timestamp =
					(wholeSeconds - SNOWFLAKE_EPOCH_SECOND_OFFSET) * USECS_PER_SEC +
					microseconds;

				/*
				 * A timestamp_tz also carries its offset, which Postgres does
				 * not store: the instant is the same value either way.
				 */
				if (column->typeCode == SNOWFLAKE_TYPE_TIMESTAMP_NTZ)
				{
					*naturalTypeId = TIMESTAMPOID;
					*naturalValue = TimestampGetDatum((Timestamp) timestamp);
				}
				else
				{
					*naturalTypeId = TIMESTAMPTZOID;
					*naturalValue = TimestampTzGetDatum((TimestampTz) timestamp);
				}
				break;
			}

		default:
			elog(ERROR, "Snowflake type %d has no computed Postgres value",
				 (int) column->typeCode);
	}
}


/*
 * ParseFractionalSeconds splits a "<seconds>.<fraction>" value into whole
 * seconds and microseconds, rounding the fraction rather than truncating it.
 *
 * The fraction has nine digits and a negative value is a whole-second count
 * below the epoch with a positive fraction added to it, which is why the
 * fraction is applied with the sign of the whole part.
 */
static int64
ParseFractionalSeconds(const char *value, int64 *wholeSeconds)
{
	char	   *fractionStart = NULL;
	int64		seconds = strtoll(value, &fractionStart, 10);
	int64		microseconds = 0;

	*wholeSeconds = seconds;

	if (fractionStart == NULL || *fractionStart != '.')
		return 0;

	const char *digit = fractionStart + 1;
	int64		nanoseconds = 0;
	int			digitCount = 0;

	for (; digitCount < 9; digitCount++)
	{
		int			digitValue = 0;

		if (digit[digitCount] >= '0' && digit[digitCount] <= '9')
			digitValue = digit[digitCount] - '0';
		else
			break;

		nanoseconds = nanoseconds * 10 + digitValue;
	}

	/* pad a short fraction out to nanoseconds */
	for (int missingDigit = digitCount; missingDigit < 9; missingDigit++)
		nanoseconds *= 10;

	microseconds = (nanoseconds + 500) / 1000;

	return value[0] == '-' ? -microseconds : microseconds;
}


/*
 * SnowflakeTypeIsPushdownSafe returns whether a value of this Postgres type can
 * be written as a Snowflake literal with the same meaning, which is what decides
 * whether an expression over it can be evaluated remotely.
 */
bool
SnowflakeTypeIsPushdownSafe(Oid typeId)
{
	switch (typeId)
	{
		case BOOLOID:
		case INT2OID:
		case INT4OID:
		case INT8OID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case DATEOID:
		case TIMEOID:
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		case BYTEAOID:
			return true;
		default:
			return false;
	}
}


/*
 * SnowflakeTypeIsWritable returns whether a value of this Postgres type can be
 * written into a Snowflake column as a literal.
 */
bool
SnowflakeTypeIsWritable(Oid typeId)
{
	return SnowflakeTypeIsPushdownSafe(typeId) ||
		typeId == JSONBOID || typeId == JSONOID;
}


/*
 * SnowflakeFormatLiteral writes a Datum as a Snowflake literal.
 *
 * Every literal that is not a plain number is written with an explicit cast, so
 * that the comparison Snowflake evaluates is the one Postgres would have
 * evaluated rather than one that depends on a session format setting.
 */
char *
SnowflakeFormatLiteral(Datum value, Oid typeId)
{
	return FormatLiteral(value, typeId, false);
}


/*
 * SnowflakeFormatValuesLiteral writes a Datum as a constant that is accepted in
 * a VALUES clause, leaving any conversion to
 * SnowflakeValuesColumnExpression.
 */
char *
SnowflakeFormatValuesLiteral(Datum value, Oid typeId)
{
	return FormatLiteral(value, typeId, true);
}


/*
 * SnowflakeTypeNeedsValuesExpression returns whether a value of this type has to
 * be converted outside the VALUES clause.
 */
bool
SnowflakeTypeNeedsValuesExpression(Oid typeId)
{
	return typeId == JSONBOID || typeId == JSONOID || typeId == BYTEAOID;
}


/*
 * SnowflakeValuesColumnExpression returns the SELECT list expression that turns
 * one column of a VALUES clause into the value to insert.
 */
char *
SnowflakeValuesColumnExpression(Oid typeId, int columnNumber)
{
	switch (typeId)
	{
		case JSONBOID:
		case JSONOID:
			return psprintf("PARSE_JSON(column%d)", columnNumber);
		case BYTEAOID:
			return psprintf("TO_BINARY(column%d, 'HEX')", columnNumber);
		default:
			return psprintf("column%d", columnNumber);
	}
}


/*
 * FormatLiteral writes a Datum as a Snowflake expression, or as a plain constant
 * when it has to stand in a VALUES clause.
 */
static char *
FormatLiteral(Datum value, Oid typeId, bool forValuesClause)
{
	switch (typeId)
	{
		case BOOLOID:
			return DatumGetBool(value) ? pstrdup("TRUE") : pstrdup("FALSE");

		case INT2OID:
		case INT4OID:
		case INT8OID:
		case NUMERICOID:
			return OutputDatum(value, typeId);

		case FLOAT4OID:
		case FLOAT8OID:
			{
				double		doubleValue = typeId == FLOAT4OID ?
					(double) DatumGetFloat4(value) : DatumGetFloat8(value);

				if (isnan(doubleValue))
					return pstrdup("'NaN'::FLOAT");

				if (isinf(doubleValue))
					return psprintf("'%sinf'::FLOAT", doubleValue < 0 ? "-" : "");

				return OutputDatum(value, typeId);
			}

		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			return FormatSnowflakeStringLiteral(OutputDatum(value, typeId));

		case DATEOID:
			{
				DateADT		dateValue = DatumGetDateADT(value);

				if (DATE_NOT_FINITE(dateValue))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot send an infinite date to Snowflake")));
				}

				return psprintf("%s::DATE",
								FormatSnowflakeStringLiteral(OutputDatum(value, DATEOID)));
			}

		case TIMEOID:
			return psprintf("%s::TIME",
							FormatSnowflakeStringLiteral(OutputDatum(value, TIMEOID)));

		case TIMESTAMPOID:
			{
				Timestamp	timestamp = DatumGetTimestamp(value);

				if (TIMESTAMP_NOT_FINITE(timestamp))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot send an infinite timestamp to Snowflake")));
				}

				return psprintf("%s::TIMESTAMP_NTZ",
								FormatSnowflakeStringLiteral(OutputDatum(value, TIMESTAMPOID)));
			}

		case TIMESTAMPTZOID:
			{
				TimestampTz timestamp = DatumGetTimestampTz(value);

				if (TIMESTAMP_NOT_FINITE(timestamp))
				{
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot send an infinite timestamp to Snowflake")));
				}

				/*
				 * A timestamptz counts microseconds from the Postgres epoch
				 * in UTC, which is exactly what the timestamp output function
				 * renders without applying a time zone, so writing the
				 * literal in UTC costs nothing but the cast.
				 */
				char	   *utcText = OutputDatum(TimestampGetDatum((Timestamp) timestamp),
												  TIMESTAMPOID);

				return psprintf("%s::TIMESTAMP_TZ",
								FormatSnowflakeStringLiteral(psprintf("%s+00:00", utcText)));
			}

		case JSONBOID:
		case JSONOID:
			{
				char	   *documentText =
					FormatSnowflakeStringLiteral(OutputDatum(value, typeId));

				if (forValuesClause)
					return documentText;

				/*
				 * PARSE_JSON produces a VARIANT, which Snowflake accepts for
				 * an OBJECT or an ARRAY column as well.
				 */
				return psprintf("PARSE_JSON(%s)", documentText);
			}

		case BYTEAOID:
			{
				char	   *hexText = OutputDatum(value, BYTEAOID);

				/* bytea_out produces the "\x<hex>" form */
				if (hexText[0] == '\\' && hexText[1] == 'x')
					hexText += 2;

				if (forValuesClause)
					return FormatSnowflakeStringLiteral(hexText);

				return psprintf("TO_BINARY(%s, 'HEX')",
								FormatSnowflakeStringLiteral(hexText));
			}

		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot send a value of type %s to Snowflake",
							format_type_be(typeId))));
			return NULL;		/* keep the compiler quiet */
	}
}


/*
 * FormatSnowflakeStringLiteral quotes a string for Snowflake, which treats a
 * backslash inside a single-quoted literal as the start of an escape sequence.
 */
static char *
FormatSnowflakeStringLiteral(const char *value)
{
	StringInfo	literal = makeStringInfo();

	appendStringInfoChar(literal, '\'');

	for (const char *cursor = value; *cursor != '\0'; cursor++)
	{
		if (*cursor == '\'' || *cursor == '\\')
			appendStringInfoChar(literal, *cursor == '\'' ? '\'' : '\\');

		appendStringInfoChar(literal, *cursor);
	}

	appendStringInfoChar(literal, '\'');

	return literal->data;
}


/*
 * OutputDatum renders a Datum with the output function of its type.
 */
static char *
OutputDatum(Datum value, Oid typeId)
{
	Oid			outputFunctionId = InvalidOid;
	bool		typeIsVarlena = false;

	getTypeOutputInfo(typeId, &outputFunctionId, &typeIsVarlena);

	return OidOutputFunctionCall(outputFunctionId, value);
}
