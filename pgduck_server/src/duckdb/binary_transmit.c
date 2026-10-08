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
 * binary_transmit.c serializes DuckDB values in the PostgreSQL binary COPY
 * format for TRANSMIT BINARY queries.
 *
 * The client names a PostgreSQL target type for every result column. We only
 * agree to send binary when we can produce exactly the value the client
 * would have obtained by running the type's input function on our text
 * output, so for each (DuckDB type, target type) pair below the binary
 * value and the text round trip are equivalent. Any other pair makes the
 * whole query fall back to the CSV transmit format.
 */
#include "c.h"

#include <string.h>

#include "catalog/pg_type_d.h"
#include "lib/stringinfo.h"
#include "port/pg_bswap.h"

#include "duckdb.h"
#include "duckdb/binary_transmit.h"
#include "pgsession/pqformat.h"

/* PostgreSQL dates and timestamps count from 2000-01-01, DuckDB from 1970 */
#define POSTGRES_EPOCH_DAYS_SINCE_UNIX_EPOCH (10957)
#define POSTGRES_EPOCH_USECS_SINCE_UNIX_EPOCH (INT64CONST(946684800000000))

/* numeric binary format constants, see src/backend/utils/adt/numeric.c */
#define NUMERIC_NBASE (10000)
#define NUMERIC_DEC_DIGITS (4)
#define NUMERIC_SIGN_POS (0x0000)
#define NUMERIC_SIGN_NEG (0x4000)

/* a 128-bit integer has at most 39 decimal digits */
#define MAX_INT128_DECIMAL_DIGITS (39)
#define MAX_NUMERIC_GROUPS ((MAX_INT128_DECIMAL_DIGITS + 2 * NUMERIC_DEC_DIGITS) / NUMERIC_DEC_DIGITS + 1)

#define PG_COPY_BINARY_SIGNATURE "PGCOPY\n\377\r\n\0"
#define PG_COPY_BINARY_SIGNATURE_LENGTH (11)

static bool is_integer_type(duckdb_type type);
static int	integer_type_size(duckdb_type type);
static bool integer_type_is_unsigned(duckdb_type type);
static int128 read_integer(duckdb_type type, void *data, idx_t row);
static void append_int16(StringInfo buf, int16 value);
static void append_int32(StringInfo buf, int32 value);
static void append_int64(StringInfo buf, int64 value);
static void append_numeric(StringInfo buf, int128 value, int scale);
static int	numeric_group_of_power(int power);
static void append_string(StringInfo buf, duckdb_string_t * value,
						  bool stopAtNul, bool jsonbVersion);


/*
 * binary_transmit_choose_writer determines whether values of the given DuckDB
 * type can be sent in the binary format of targetTypeId and, if so, fills in
 * the writer.
 */
bool
binary_transmit_choose_writer(duckdb_logical_type logicalType, Oid targetTypeId,
							  BinaryColumnWriter * writer)
{
	duckdb_type sourceType = duckdb_get_type_id(logicalType);

	memset(writer, 0, sizeof(BinaryColumnWriter));
	writer->sourceType = sourceType;

	switch (targetTypeId)
	{
		case BOOLOID:
			if (sourceType == DUCKDB_TYPE_BOOLEAN)
				writer->kind = BINARY_WRITER_BOOL;
			break;

		case INT2OID:
		case INT4OID:
		case INT8OID:
			{
				int			targetSize = targetTypeId == INT2OID ? 2 :
					targetTypeId == INT4OID ? 4 : 8;

				/*
				 * Only widening conversions, so we never have to raise an
				 * out-of-range error ourselves. An unsigned type needs a
				 * target that is strictly larger.
				 */
				if (is_integer_type(sourceType) && sourceType != DUCKDB_TYPE_HUGEINT &&
					(integer_type_is_unsigned(sourceType) ?
					 integer_type_size(sourceType) < targetSize :
					 integer_type_size(sourceType) <= targetSize))
				{
					writer->kind = BINARY_WRITER_INT;
					writer->targetSize = targetSize;
				}
				break;
			}

		case FLOAT4OID:
			if (sourceType == DUCKDB_TYPE_FLOAT)
				writer->kind = BINARY_WRITER_FLOAT4;
			break;

		case FLOAT8OID:

			/*
			 * FLOAT to float8 is not equivalent: the text path goes through
			 * the shortest float4 representation, which parses to a different
			 * double than the widened float.
			 */
			if (sourceType == DUCKDB_TYPE_DOUBLE)
				writer->kind = BINARY_WRITER_FLOAT8;
			break;

		case NUMERICOID:
			if (is_integer_type(sourceType))
			{
				writer->kind = BINARY_WRITER_NUMERIC;
				writer->scale = 0;
			}
			else if (sourceType == DUCKDB_TYPE_DECIMAL)
			{
				writer->kind = BINARY_WRITER_NUMERIC;
				writer->sourceType = duckdb_decimal_internal_type(logicalType);
				writer->scale = duckdb_decimal_scale(logicalType);
			}
			break;

		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
		case JSONOID:
			if (sourceType == DUCKDB_TYPE_VARCHAR)
				writer->kind = BINARY_WRITER_TEXT;
			break;

		case JSONBOID:
			if (sourceType == DUCKDB_TYPE_VARCHAR)
				writer->kind = BINARY_WRITER_JSONB;
			break;

		case BYTEAOID:
			if (sourceType == DUCKDB_TYPE_BLOB)
			{
				/*
				 * Aliased blobs (e.g. WKB_BLOB) have a different text
				 * representation, so only plain blobs are equivalent to the
				 * raw bytes.
				 */
				char	   *alias = duckdb_logical_type_get_alias(logicalType);

				if (alias == NULL)
					writer->kind = BINARY_WRITER_BYTEA;

				duckdb_free(alias);
			}
			break;

		case DATEOID:
			if (sourceType == DUCKDB_TYPE_DATE)
				writer->kind = BINARY_WRITER_DATE;
			break;

		case TIMESTAMPOID:
			if (sourceType == DUCKDB_TYPE_TIMESTAMP)
				writer->kind = BINARY_WRITER_TIMESTAMP;
			break;

		case TIMESTAMPTZOID:
			if (sourceType == DUCKDB_TYPE_TIMESTAMP_TZ)
				writer->kind = BINARY_WRITER_TIMESTAMP;
			break;

		case TIMEOID:
			if (sourceType == DUCKDB_TYPE_TIME)
				writer->kind = BINARY_WRITER_TIME;
			break;

		case UUIDOID:
			if (sourceType == DUCKDB_TYPE_UUID)
				writer->kind = BINARY_WRITER_UUID;
			break;

		default:
			break;
	}

	return writer->kind != BINARY_WRITER_NONE;
}


/*
 * binary_transmit_append_header appends the binary COPY file header.
 */
void
binary_transmit_append_header(StringInfo buf)
{
	appendBinaryStringInfo(buf, PG_COPY_BINARY_SIGNATURE,
						   PG_COPY_BINARY_SIGNATURE_LENGTH);

	/* flags field */
	append_int32(buf, 0);

	/* header extension area length */
	append_int32(buf, 0);
}


/*
 * binary_transmit_append_trailer appends the binary COPY file trailer.
 */
void
binary_transmit_append_trailer(StringInfo buf)
{
	append_int16(buf, -1);
}


/*
 * binary_transmit_append_value appends a non-NULL value as a length-prefixed
 * field in the binary send format of the writer's target type.
 */
void
binary_transmit_append_value(StringInfo buf, BinaryColumnWriter * writer,
							 void *data, idx_t row)
{
	switch (writer->kind)
	{
		case BINARY_WRITER_BOOL:
			append_int32(buf, 1);
			pq_sendbyte(buf, ((bool *) data)[row] ? 1 : 0);
			break;

		case BINARY_WRITER_INT:
			{
				int64		value = (int64) read_integer(writer->sourceType, data, row);

				append_int32(buf, writer->targetSize);

				if (writer->targetSize == 2)
					append_int16(buf, (int16) value);
				else if (writer->targetSize == 4)
					append_int32(buf, (int32) value);
				else
					append_int64(buf, value);
				break;
			}

		case BINARY_WRITER_FLOAT4:
			{
				uint32		bits;

				memcpy(&bits, &((float *) data)[row], sizeof(bits));
				append_int32(buf, 4);
				append_int32(buf, (int32) bits);
				break;
			}

		case BINARY_WRITER_FLOAT8:
			{
				uint64		bits;

				memcpy(&bits, &((double *) data)[row], sizeof(bits));
				append_int32(buf, 8);
				append_int64(buf, (int64) bits);
				break;
			}

		case BINARY_WRITER_NUMERIC:
			append_numeric(buf, read_integer(writer->sourceType, data, row),
						   writer->scale);
			break;

		case BINARY_WRITER_TEXT:

			/*
			 * The text path hands the value over as a C string, so it ends at
			 * the first NUL byte. Do the same to produce identical values.
			 */
			append_string(buf, &((duckdb_string_t *) data)[row], true, false);
			break;

		case BINARY_WRITER_JSONB:
			append_string(buf, &((duckdb_string_t *) data)[row], true, true);
			break;

		case BINARY_WRITER_BYTEA:
			append_string(buf, &((duckdb_string_t *) data)[row], false, false);
			break;

		case BINARY_WRITER_DATE:
			{
				int32		days = ((duckdb_date *) data)[row].days;
				int32		result;

				if (days == PG_INT32_MAX)
					result = PG_INT32_MAX;	/* infinity */
				else if (days == -PG_INT32_MAX)
					result = PG_INT32_MIN;	/* -infinity */
				else
				{
					int64		shifted = (int64) days - POSTGRES_EPOCH_DAYS_SINCE_UNIX_EPOCH;

					/*
					 * Values beyond the PostgreSQL range are rejected by
					 * date_recv, just like date_in rejects them on the text
					 * path; we only need to avoid wrapping into a valid (or
					 * infinite) value.
					 */
					result = shifted < PG_INT32_MIN + 1 ? PG_INT32_MIN + 1 : (int32) shifted;
				}

				append_int32(buf, 4);
				append_int32(buf, result);
				break;
			}

		case BINARY_WRITER_TIMESTAMP:
			{
				int64		micros = ((duckdb_timestamp *) data)[row].micros;
				int64		result;

				if (micros == PG_INT64_MAX)
					result = PG_INT64_MAX;	/* infinity */
				else if (micros == -PG_INT64_MAX)
					result = PG_INT64_MIN;	/* -infinity */
				else if (__builtin_sub_overflow(micros,
												POSTGRES_EPOCH_USECS_SINCE_UNIX_EPOCH,
												&result) ||
						 result == PG_INT64_MIN)
				{
					/* out of range, rejected by timestamp_recv */
					result = PG_INT64_MIN + 1;
				}

				append_int32(buf, 8);
				append_int64(buf, result);
				break;
			}

		case BINARY_WRITER_TIME:
			append_int32(buf, 8);
			append_int64(buf, ((duckdb_time *) data)[row].micros);
			break;

		case BINARY_WRITER_UUID:
			{
				duckdb_hugeint value = ((duckdb_hugeint *) data)[row];

				/*
				 * DuckDB flips the top bit to make UUIDs sort as signed
				 * integers
				 */
				uint64		upper = ((uint64) value.upper) ^ (UINT64CONST(1) << 63);

				append_int32(buf, 16);
				append_int64(buf, (int64) upper);
				append_int64(buf, (int64) value.lower);
				break;
			}

		case BINARY_WRITER_NONE:
			/* not reachable, writers are only used if all columns have one */
			break;
	}
}


static bool
is_integer_type(duckdb_type type)
{
	return integer_type_size(type) > 0;
}


/*
 * integer_type_size returns the size of an integer type in bytes, or 0 if
 * the type is not an integer type.
 */
static int
integer_type_size(duckdb_type type)
{
	switch (type)
	{
		case DUCKDB_TYPE_TINYINT:
		case DUCKDB_TYPE_UTINYINT:
			return 1;
		case DUCKDB_TYPE_SMALLINT:
		case DUCKDB_TYPE_USMALLINT:
			return 2;
		case DUCKDB_TYPE_INTEGER:
		case DUCKDB_TYPE_UINTEGER:
			return 4;
		case DUCKDB_TYPE_BIGINT:
		case DUCKDB_TYPE_UBIGINT:
			return 8;
		case DUCKDB_TYPE_HUGEINT:
			return 16;
		default:
			return 0;
	}
}


static bool
integer_type_is_unsigned(duckdb_type type)
{
	return type == DUCKDB_TYPE_UTINYINT || type == DUCKDB_TYPE_USMALLINT ||
		type == DUCKDB_TYPE_UINTEGER || type == DUCKDB_TYPE_UBIGINT;
}


/*
 * read_integer reads an integer value of the given type from a vector.
 */
static int128
read_integer(duckdb_type type, void *data, idx_t row)
{
	switch (type)
	{
		case DUCKDB_TYPE_TINYINT:
			return ((int8 *) data)[row];
		case DUCKDB_TYPE_UTINYINT:
			return ((uint8 *) data)[row];
		case DUCKDB_TYPE_SMALLINT:
			return ((int16 *) data)[row];
		case DUCKDB_TYPE_USMALLINT:
			return ((uint16 *) data)[row];
		case DUCKDB_TYPE_INTEGER:
			return ((int32 *) data)[row];
		case DUCKDB_TYPE_UINTEGER:
			return ((uint32 *) data)[row];
		case DUCKDB_TYPE_BIGINT:
			return ((int64 *) data)[row];
		case DUCKDB_TYPE_UBIGINT:
			return ((uint64 *) data)[row];
		case DUCKDB_TYPE_HUGEINT:
			{
				duckdb_hugeint value = ((duckdb_hugeint *) data)[row];

				return (int128) (((uint128) (uint64) value.upper << 64) | value.lower);
			}
		default:
			/* not reachable, writers are only chosen for integer types */
			return 0;
	}
}


static void
append_int16(StringInfo buf, int16 value)
{
	uint16		netValue = pg_hton16((uint16) value);

	appendBinaryStringInfo(buf, &netValue, sizeof(netValue));
}


static void
append_int32(StringInfo buf, int32 value)
{
	uint32		netValue = pg_hton32((uint32) value);

	appendBinaryStringInfo(buf, &netValue, sizeof(netValue));
}


static void
append_int64(StringInfo buf, int64 value)
{
	uint64		netValue = pg_hton64((uint64) value);

	appendBinaryStringInfo(buf, &netValue, sizeof(netValue));
}


/*
 * append_numeric appends value * 10^-scale in the numeric binary format,
 * which consists of ndigits, weight, sign and dscale followed by ndigits
 * base-10000 digits, most significant first. The digit groups are aligned
 * on the decimal point and weight is the power of 10000 of the first one.
 */
static void
append_numeric(StringInfo buf, int128 value, int scale)
{
	bool		isNegative = value < 0;
	uint128		absValue = isNegative ? -(uint128) value : (uint128) value;

	/* decimal digits, least significant first */
	uint8		decimalDigits[MAX_INT128_DECIMAL_DIGITS];
	int			decimalDigitCount = 0;

	while (absValue > 0)
	{
		/* peel off 19 digits at a time to keep most divisions 64-bit */
		uint64		chunk;
		int			chunkDigits = 0;
		bool		isLastChunk = absValue <= PG_UINT64_MAX;

		if (isLastChunk)
		{
			chunk = (uint64) absValue;
			absValue = 0;
		}
		else
		{
			uint64		divisor = UINT64CONST(10000000000000000000);

			chunk = (uint64) (absValue % divisor);
			absValue /= divisor;
		}

		while (chunk > 0 || (!isLastChunk && chunkDigits < 19))
		{
			decimalDigits[decimalDigitCount++] = chunk % 10;
			chunk /= 10;
			chunkDigits++;
		}
	}

	/*
	 * Place every decimal digit into its base-10000 group. Digit i has power
	 * of ten i - scale, which lives in group floor((i - scale) / 4).
	 */
	static const int16 powersOfTen[NUMERIC_DEC_DIGITS] = {1, 10, 100, 1000};
	int16		groups[MAX_NUMERIC_GROUPS] = {0};
	int			lowestGroup = numeric_group_of_power(-scale);

	for (int digitIndex = 0; digitIndex < decimalDigitCount; digitIndex++)
	{
		int			power = digitIndex - scale;
		int			group = numeric_group_of_power(power);

		groups[group - lowestGroup] +=
			decimalDigits[digitIndex] * powersOfTen[power - group * NUMERIC_DEC_DIGITS];
	}

	/* strip zero groups on both ends */
	int			firstIndex = 0;
	int			lastIndex = decimalDigitCount > 0 ?
		numeric_group_of_power(decimalDigitCount - 1 - scale) - lowestGroup : -1;

	while (lastIndex >= firstIndex && groups[lastIndex] == 0)
		lastIndex--;
	while (firstIndex <= lastIndex && groups[firstIndex] == 0)
		firstIndex++;

	int			ndigits = lastIndex >= firstIndex ? lastIndex - firstIndex + 1 : 0;
	int			weight = ndigits > 0 ? lastIndex + lowestGroup : 0;

	append_int32(buf, (int32) (sizeof(int16) * (4 + ndigits)));
	append_int16(buf, (int16) ndigits);
	append_int16(buf, (int16) weight);
	append_int16(buf, (int16) (ndigits > 0 && isNegative ? NUMERIC_SIGN_NEG : NUMERIC_SIGN_POS));
	append_int16(buf, (int16) scale);

	for (int groupIndex = lastIndex; groupIndex >= firstIndex; groupIndex--)
		append_int16(buf, groups[groupIndex]);
}


/*
 * numeric_group_of_power returns the base-10000 group that holds the decimal
 * digit with the given power of ten, i.e. floor(power / 4).
 */
static int
numeric_group_of_power(int power)
{
	if (power >= 0)
		return power / NUMERIC_DEC_DIGITS;

	return -((-power + NUMERIC_DEC_DIGITS - 1) / NUMERIC_DEC_DIGITS);
}


/*
 * append_string appends a string or blob value as a length-prefixed field,
 * optionally ending at the first NUL byte and optionally prefixed with the
 * jsonb binary format version.
 */
static void
append_string(StringInfo buf, duckdb_string_t * value, bool stopAtNul,
			  bool jsonbVersion)
{
	uint32		length = duckdb_string_t_length(*value);
	const char *bytes = duckdb_string_t_data(value);

	if (stopAtNul)
	{
		const char *nul = memchr(bytes, '\0', length);

		if (nul != NULL)
			length = nul - bytes;
	}

	append_int32(buf, (int32) (length + (jsonbVersion ? 1 : 0)));

	if (jsonbVersion)
		pq_sendbyte(buf, 1);

	appendBinaryStringInfo(buf, bytes, length);
}
