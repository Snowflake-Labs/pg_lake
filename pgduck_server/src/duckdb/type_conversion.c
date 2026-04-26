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
 * The functions in this file deals with converting DuckDB data types to
 * PostgreSQL wire protocol compatible format.
 *
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#include "c.h"

#include "postgres_fe.h"

#include <duckdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "duckdb/duckdb.h"
#include "duckdb/type_conversion.h"
#include "duckdb/duckdb_pglake.h"
#include "utils/pgduck_log_utils.h"
#include "utils/hex.h"
#include "utils/numutils.h"
#include "utils/pgduck_log_utils.h"

/*
* We are propagating this information to caller, and keeping the backend running
*/
#define CHECK_OOM(pointer) \
	(((pointer) == NULL) ? DUCKDB_OUT_OF_MEMORY_ERROR : DUCKDB_SUCCESS)

#define VECTOR_VALUE(Data,Type,RowIndex) ((Type *) Data)[RowIndex]

#define DEFINE_TO_TEXT_FUNCTION(func_name, data_type, format_func) \
static DuckDBStatus func_name(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer *toTextBuffer) \
{                                                                  \
    void *data = duckdb_vector_get_data(vector);                   \
    data_type val = VECTOR_VALUE(data, data_type, row);            \
    format_func((data_type)val, toTextBuffer->buffer);			   \
    return CHECK_OOM(toTextBuffer->buffer);						   \
}

#define DEFINE_TO_TEXT_FUNCTION_ALLOC(func_name, data_type, format_func) \
static DuckDBStatus func_name(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer *toTextBuffer) \
{																		\
    void *data = duckdb_vector_get_data(vector);						\
    data_type val = VECTOR_VALUE(data, data_type, row);					\
    format_func((data_type)val, toTextBuffer);							\
    return CHECK_OOM(toTextBuffer->buffer);								\
}

static DuckDBStatus pg_varchar_to_text(duckdb_string_t val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_uuid_to_text(duckdb_hugeint val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_hugeint_to_text(duckdb_hugeint val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_uhugeint_to_text(duckdb_uhugeint val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_timestamp_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_timestamp_ns_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_timestamp_ms_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_timestamp_sec_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_timestamp_tz_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_interval_to_text(duckdb_interval val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_time_to_text(duckdb_time val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_time_tz_to_text(duckdb_time_tz val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus pg_date_to_text(duckdb_date val, TextOutputBuffer * toTextBuffer);
static DuckDBStatus unsupported_type(duckdb_vector vector, int row, TextOutputBuffer * toTextBuffer);

DEFINE_TO_TEXT_FUNCTION(boolean_to_text, bool, pg_bool_to_text)
DEFINE_TO_TEXT_FUNCTION(tinyint_to_text, int8_t, pg_itoa)
DEFINE_TO_TEXT_FUNCTION(smallint_to_text, int16_t, pg_itoa)
DEFINE_TO_TEXT_FUNCTION(int_to_text, int32_t, pg_ltoa)
DEFINE_TO_TEXT_FUNCTION(bigint_to_text, int64_t, pg_lltoa)
DEFINE_TO_TEXT_FUNCTION(utinyint_to_text, uint8_t, pg_itoa)
DEFINE_TO_TEXT_FUNCTION(usmallint_to_text, uint16_t, pg_ltoa)
DEFINE_TO_TEXT_FUNCTION(uint_to_text, uint32_t, pg_lltoa)
DEFINE_TO_TEXT_FUNCTION(uint64_to_text, uint64_t, pg_ulltoa)
DEFINE_TO_TEXT_FUNCTION(float4_to_text, float4, pg_float4_to_text)
DEFINE_TO_TEXT_FUNCTION(float8_to_text, float8, pg_float8_to_text)

DEFINE_TO_TEXT_FUNCTION_ALLOC(varchar_to_text, duckdb_string_t, pg_varchar_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(uuid_to_text, duckdb_hugeint, pg_uuid_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(hugeint_to_text, duckdb_hugeint, pg_hugeint_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(uhugeint_to_text, duckdb_uhugeint, pg_uhugeint_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(timestamp_to_text, duckdb_timestamp, pg_timestamp_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(timestampns_to_text, duckdb_timestamp, pg_timestamp_ns_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(timestampms_to_text, duckdb_timestamp, pg_timestamp_ms_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(timestampsec_to_text, duckdb_timestamp, pg_timestamp_sec_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(timestamp_tz_to_text, duckdb_timestamp, pg_timestamp_tz_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(interval_to_text, duckdb_interval, pg_interval_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(time_to_text, duckdb_time, pg_time_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(time_tz_to_text, duckdb_time_tz, pg_time_tz_to_text)
DEFINE_TO_TEXT_FUNCTION_ALLOC(date_to_text, duckdb_date, pg_date_to_text)

/* logical types cannot use the above macros */

static DuckDBStatus array_to_text(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer * toTextBuffer);
static DuckDBStatus blob_to_text(duckdb_vector vector, duckdb_logical_type blobType, int row, TextOutputBuffer * toTextBuffer);
static DuckDBStatus decimal_to_text(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer * toTextBuffer);
static DuckDBStatus enum_to_text(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer * toTextBuffer);
static DuckDBStatus list_to_text(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer * toTextBuffer);
static DuckDBStatus map_to_text(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer * toTextBuffer);
static DuckDBStatus struct_to_text(duckdb_vector vector, duckdb_logical_type logicalType, int row, TextOutputBuffer * toTextBuffer);

static void AppendFieldValue(const char *value, bool useQuote, StringInfo output);

/*
 * This comment explains how DuckDB types are converted to PostgreSQL types
 * over the wire.
 *
 * Use the find_duck_type_info() function to access this information.
 *
 * Converting types from DuckDB to PostgreSQL can be done in different ways.
 * Our chosen method involves sending InvalidOid to PostgreSQL clients for all
 * types and ensuring data is correctly formatted as text. In other words, we
 * currently do not keep 1-1 mapping between Postgres and DuckDB types.
 *
 * This strategy works because most PostgreSQL clients focus on the text
 * format of data rather than its type. However, for clients that do require
 * specific type information,  they rely on the PQftype() function. Thus, it's
 * important to watch out for conversion errors with these clients. In our
 * situation, since we mainly deal with our own internal clients, relying on
 * text format is sufficient.
 *
 * Another possible approach is to select the smallest possible PostgreSQL type that
 * can accurately represent a DuckDB type. This method might be easier in some scenarios
 * but isn't always feasible. For instance, DuckDB's integer type (DUCKDB_TYPE_INTEGER),
 * which is 4 bytes, matches PostgreSQL's INT4OID. Similarly, DuckDB's unsigned integer
 * (DUCKDB_TYPE_UINTEGER), also 4 bytes, can be represented by PostgreSQL's 8-byte bigint
 * (INT8OID) to include sign information. However, DuckDB's unsigned bigint
 * (DUCKDB_TYPE_UBIGINT) is 8 bytes, and PostgreSQL lacks a larger corresponding type.
 * If adopting this smaller type approach, consistency is key, especially when converting
 * types for tables created from parquet files. To fully implement this, considering
 * PostgreSQL extensions for any missing types might be necessary.
 */
static DuckDBTypeInfo TypeInfo[] =
{

	/*
	 * This is not accessible via find_duck_type_info() function. This is
	 * required because the index of the array is the same as the duckdb_type
	 * enum.
	 */
	{
		 /* 0 */ DUCKDB_TYPE_INVALID, NULL
	},

	/*
	 * DuckDB bool (DUCKDB_TYPE_BOOLEAN) is 1 byte PG bool (BOOLOID) is 1
	 * byte.
	 */
	{
		 /* 1 */ DUCKDB_TYPE_BOOLEAN, boolean_to_text
	},

	/*
	 * DuckDB tinyint (DUCKDB_TYPE_TINYINT) is 1 byte PG smallint (INT2OID) is
	 * 2 bytes. Given there there is no 1 byte integer type in PG, we use
	 * smallint.
	 *
	 * It is safe to use a larger type in PG, so we use the smallest type that
	 * can hold the value.
	 */
	{
		 /* 2 */ DUCKDB_TYPE_TINYINT, tinyint_to_text
	},

	/*
	 * DuckDB smallint (DUCKDB_TYPE_SMALLINT) is 2 bytes PG smallint (INT2OID)
	 * is 2 bytes.
	 */
	{
		 /* 3 */ DUCKDB_TYPE_SMALLINT, smallint_to_text
	},

	/*
	 * DuckDB int (DUCKDB_TYPE_INTEGER) is 4 bytes PG int (INT4OID) is 4
	 * bytes.
	 */
	{
		 /* 4 */ DUCKDB_TYPE_INTEGER, int_to_text
	},

	/*
	 * DuckDB bigint (DUCKDB_TYPE_BIGINT) is 8 bytes PG bigint (INT8OID) is 8
	 * bytes.
	 */
	{
		 /* 5 */ DUCKDB_TYPE_BIGINT, bigint_to_text
	},

	/*
	 * DuckDB utinyint (DUCKDB_TYPE_UTINYINT) is 1 byte PG smallint (INT2OID)
	 * is 2 bytes. Given there there is no 1 byte integer type in PG, we use
	 * smallint.
	 *
	 * It is safe to use a larger type in PG, so we use the smallest type that
	 * can hold the value.
	 */
	{
		 /* 6 */ DUCKDB_TYPE_UTINYINT, utinyint_to_text
	},

	/*
	 * DuckDB usmallint (DUCKDB_TYPE_USMALLINT) is 2 bytes PG integer
	 * (INT2OID) is 2 bytes.
	 */
	{
		 /* 7 */ DUCKDB_TYPE_USMALLINT, usmallint_to_text
	},

	/*
	 * DuckDB uinteger (DUCKDB_TYPE_UINTEGER) is 4 bytes PG bigint (INT4OID)
	 * is 4 bytes.
	 */
	{
		 /* 8 */ DUCKDB_TYPE_UINTEGER, uint_to_text
	},

	/*
	 * DuckDB float (DUCKDB_TYPE_UBIGINT) is 8 bytes PG bigint (INT8OID) is 8
	 * bytes.
	 */
	{
		 /* 9 */ DUCKDB_TYPE_UBIGINT, uint64_to_text
	},

	/*
	 * DuckDB float (DUCKDB_TYPE_FLOAT) is 4 bytes PG float (FLOAT4OID) is 4
	 * bytes.
	 */
	{
		 /* 10 */ DUCKDB_TYPE_FLOAT, float4_to_text
	},

	/*
	 * DuckDB double (DUCKDB_TYPE_DOUBLE) is 8 bytes PG double (FLOAT8OID) is
	 * 8 bytes.
	 */
	{
		 /* 11 */ DUCKDB_TYPE_DOUBLE, float8_to_text
	},

	{
		 /* 12 */ DUCKDB_TYPE_TIMESTAMP, timestamp_to_text
	},

	{
		 /* 13 */ DUCKDB_TYPE_DATE, date_to_text
	},

	{
		 /* 14 */ DUCKDB_TYPE_TIME, time_to_text
	},

	{
		 /* 15 */ DUCKDB_TYPE_INTERVAL, interval_to_text
	},

	{
		 /* 16 */ DUCKDB_TYPE_HUGEINT, hugeint_to_text
	},

	{
		 /* 17 */ DUCKDB_TYPE_VARCHAR, varchar_to_text
	},

	{
		 /* 18 */ DUCKDB_TYPE_BLOB, blob_to_text
	},
	{
		 /* 19 */ DUCKDB_TYPE_DECIMAL, decimal_to_text
	},
	{
		 /* 20 */ DUCKDB_TYPE_TIMESTAMP_S, timestampsec_to_text
	},
	{
		 /* 21 */ DUCKDB_TYPE_TIMESTAMP_MS, timestampms_to_text
	},
	{
		 /* 22 */ DUCKDB_TYPE_TIMESTAMP_NS, timestampns_to_text
	},
	{
		 /* 23 */ DUCKDB_TYPE_ENUM, enum_to_text
	},
	{
		 /* 24 */ DUCKDB_TYPE_LIST, list_to_text
	},
	{
		 /* 25 */ DUCKDB_TYPE_STRUCT, struct_to_text
	},
	{
		 /* 26 */ DUCKDB_TYPE_MAP, map_to_text
	},
	{
		 /* 27 */ DUCKDB_TYPE_UUID, uuid_to_text
	},
	{
		 /* 28 */ DUCKDB_TYPE_UNION, NULL
	},
	{
		 /* 29 */ DUCKDB_TYPE_BIT, NULL
	},
	{
		 /* 30 */ DUCKDB_TYPE_TIME_TZ, time_tz_to_text
	},
	{
		 /* 31 */ DUCKDB_TYPE_TIMESTAMP_TZ, timestamp_tz_to_text
	},
	{
		 /* 32 */ DUCKDB_TYPE_UHUGEINT, uhugeint_to_text
	},
	{
		 /* 33 */ DUCKDB_TYPE_ARRAY, array_to_text
	},
};


/*
 * Lookup in O(1), return NULL if not found.
 */
DuckDBTypeInfo *
find_duck_type_info(duckdb_type duckType)
{
	if (duckType > DUCKDB_TYPE_INVALID && duckType <= DUCKDB_TYPE_ARRAY)
		return &TypeInfo[duckType];

	return NULL;
}

/*
 * duckdb_type_to_pg_oid: maps a DuckDB type to its closest PostgreSQL OID.
 *
 * For types with no direct PG equivalent (e.g. UBIGINT, HUGEINT, UHUGEINT,
 * complex logical types), we fall back to TEXTOID so the client can still
 * consume the text-serialised value we always send.
 * 
 * Note: consider falling back to InvalidOid (0).
 *
 * For DECIMAL we always return NUMERICOID regardless of width/scale;
 * callers that need the typmod should handle that separately.
 */
Oid duckdb_type_to_pg_oid(duckdb_type duckType)
{
	switch (duckType)
	{
	/* ---- booleans ---- */
	case DUCKDB_TYPE_BOOLEAN:
		return BOOLOID;

	/* ---- integers ---- */
	case DUCKDB_TYPE_TINYINT:
		return INT2OID; /* no INT1 in PG */
	case DUCKDB_TYPE_SMALLINT:
		return INT2OID;
	case DUCKDB_TYPE_INTEGER:
		return INT4OID;
	case DUCKDB_TYPE_BIGINT:
		return INT8OID;

	/* unsigned — promote to next signed size that fits */
	case DUCKDB_TYPE_UTINYINT:
		return INT2OID;
	case DUCKDB_TYPE_USMALLINT:
		return INT4OID;
	case DUCKDB_TYPE_UINTEGER:
		return INT8OID;
	case DUCKDB_TYPE_UBIGINT:
		return NUMERICOID; /* no u64 in PG */
	case DUCKDB_TYPE_HUGEINT:
		return NUMERICOID;
	case DUCKDB_TYPE_UHUGEINT:
		return NUMERICOID;

	/* ---- floats ---- */
	case DUCKDB_TYPE_FLOAT:
		return FLOAT4OID;
	case DUCKDB_TYPE_DOUBLE:
		return FLOAT8OID;

	/* ---- arbitrary precision ---- */
	case DUCKDB_TYPE_DECIMAL:
		return NUMERICOID;

	/* ---- text / bytes ---- */
	case DUCKDB_TYPE_VARCHAR:
		return TEXTOID;
	case DUCKDB_TYPE_BLOB:
		return BYTEAOID;
	case DUCKDB_TYPE_BIT:
		return BITOID;
	case DUCKDB_TYPE_ENUM:
		return TEXTOID; /* sent as label text */

	/* ---- date / time ---- */
	case DUCKDB_TYPE_DATE:
		return DATEOID;
	case DUCKDB_TYPE_TIME:
		return TIMEOID;
	case DUCKDB_TYPE_TIME_TZ:
		return TIMETZOID;
	case DUCKDB_TYPE_TIMESTAMP:
		return TIMESTAMPOID;
	case DUCKDB_TYPE_TIMESTAMP_S:
		return TIMESTAMPOID;
	case DUCKDB_TYPE_TIMESTAMP_MS:
		return TIMESTAMPOID;
	case DUCKDB_TYPE_TIMESTAMP_NS:
		return TIMESTAMPOID;
	case DUCKDB_TYPE_TIMESTAMP_TZ:
		return TIMESTAMPTZOID;
	case DUCKDB_TYPE_INTERVAL:
		return INTERVALOID;

	/* ---- uuid ---- */
	case DUCKDB_TYPE_UUID:
		return UUIDOID;

	/* ---- complex / nested — serialised as text ---- */
	/* TODO: Handle this case */
	case DUCKDB_TYPE_LIST:
		return TEXTOID;
	case DUCKDB_TYPE_ARRAY:
		return TEXTOID;
	case DUCKDB_TYPE_STRUCT:
		return TEXTOID;
	case DUCKDB_TYPE_MAP:
		return TEXTOID;
	case DUCKDB_TYPE_UNION:
		return TEXTOID;

	default:
		return TEXTOID;
	}
}

/*
 * This function performs necessary cleanup of a TextOutputBuffer for handling
 * error conditions.
 */
static void
CleanupOutputBuffer(TextOutputBuffer * outputBuffer)
{
	if (outputBuffer->needsFree && outputBuffer->buffer != NULL)
		pg_free(outputBuffer->buffer);

	outputBuffer->buffer = NULL;
	outputBuffer->needsFree = false;
}

/*
 * This function is used to handle unsupported types in the type_conversion module.
 * It ignores the type and sets the buffer to an empty string.
 */
static DuckDBStatus
unsupported_type(duckdb_vector vector, int row, TextOutputBuffer * toTextBuffer)
{
	if (toTextBuffer != NULL && toTextBuffer->buffer != NULL)
		strcpy(toTextBuffer->buffer, "\0");
	return DUCKDB_TYPE_CONVERSION_ERROR;
}

/*
 * pg_varchar_to_text: converts a varchar to its string representation.
 *
 * Caller must ensure that 'buffer' points to enough memory to hold the result.
 */
static DuckDBStatus
pg_varchar_to_text(duckdb_string_t val, TextOutputBuffer * toTextBuffer)
{
	uint32_t	length = 0;

	/*
	 * INLINE_BYTES = 12, and our buffer is
	 * STACK_ALLOCATED_OUTPUT_BUFFER_SIZE(42), so we can hold the data in the
	 * pre-allocated buffer.
	 */
	if (duckdb_string_is_inlined(val))
	{
		length = val.value.inlined.length;
		memcpy(toTextBuffer->buffer, val.value.inlined.inlined, length);
	}
	else
	{
		length = val.value.pointer.length;
		if (length >= STACK_ALLOCATED_OUTPUT_BUFFER_SIZE)
		{
			/*
			 * The data is too large to fit in the pre-allocated buffer, so we
			 * need to allocate a new buffer on the heap.
			 */
			toTextBuffer->buffer = pg_malloc(length + 1);

			/* before accessing to the buffer, make sure no allocation errors */
			if (toTextBuffer->buffer == NULL)
				return DUCKDB_OUT_OF_MEMORY_ERROR;

			toTextBuffer->needsFree = true;
		}

		memcpy(toTextBuffer->buffer, val.value.pointer.ptr, length);
	}

	(toTextBuffer->buffer)[length] = '\0';

	return CHECK_OOM(toTextBuffer->buffer);
}


/*
 * pg_blob_to_text: converts a blob to its string representation
 *
 * Note that blob uses duckdb_string_t internally.
 */
static DuckDBStatus
blob_to_text(duckdb_vector vector, duckdb_logical_type blobType, int row, TextOutputBuffer * toTextBuffer)
{
	void	   *data = duckdb_vector_get_data(vector);
	duckdb_string_t val = VECTOR_VALUE(data, duckdb_string_t, row);

	char	   *typeAlias = duckdb_logical_type_get_alias(blobType);
	bool		emitEscapeSequence = true;

	if (typeAlias != NULL)
	{
		if (strcmp(typeAlias, "GEOMETRY") == 0)
		{
			/* output geometry via st_ashexwkb */
			toTextBuffer->buffer = (char *) duckdb_pglake_geometry_to_string(DuckDB, val);
			toTextBuffer->needsFree = true;

			return CHECK_OOM(toTextBuffer->buffer);
		}
		else if (strcmp(typeAlias, "WKB_BLOB") == 0)
		{
			/* output WKB_BLOB as pure hex to be parseable as geometry */
			emitEscapeSequence = false;
		}
	}

	uint32_t	blobLength;
	char	   *blobBuffer;

	if (duckdb_string_is_inlined(val))
	{
		/* use the small inlined buffer */
		blobBuffer = val.value.inlined.inlined;
		blobLength = val.value.inlined.length;
	}
	else
	{
		/* use a buffer from a child vector */
		blobBuffer = val.value.pointer.ptr;
		blobLength = val.value.pointer.length;
	}

	/* 2 characters per byte + terminating byte */
	size_t		textBufferLength = blobLength * 2 + 1;

	/* prefix with \x, which adds 2 bytes */
	if (emitEscapeSequence)
		textBufferLength += 2;

	if (textBufferLength > STACK_ALLOCATED_OUTPUT_BUFFER_SIZE)
	{
		/*
		 * The data is too large to fit in the pre-allocated buffer, so we
		 * need to allocate a new buffer on the heap.
		 */
		toTextBuffer->buffer = pg_malloc(textBufferLength);

		if (toTextBuffer->buffer == NULL)
			return DUCKDB_OUT_OF_MEMORY_ERROR;

		toTextBuffer->needsFree = true;
	}

	char	   *bufferStart = toTextBuffer->buffer;

	/* prefix with \x */
	if (emitEscapeSequence)
	{
		toTextBuffer->buffer[0] = '\\';
		toTextBuffer->buffer[1] = 'x';

		/* start the hex encoded string at offset 2 */
		bufferStart += 2;
	}

	/* add hexadecimal string to end of text buffer */
	hex_encode(blobBuffer, blobLength, bufferStart);

	/* make sure we terminate the string */
	(toTextBuffer->buffer)[textBufferLength - 1] = '\0';

	return DUCKDB_SUCCESS;
}

/*
 * pg_uuid_to_text: converts an UUID to its string representation.
 *
 * UUID is represented with a hugeint in DuckDB. The function relies on
 * duckdb_pglake_uuid_to_string() to convert the UUID to a string. The function
 * is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_uuid_to_text(duckdb_hugeint val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_uuid_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_hugeint_to_text: converts a hugeint to its string representation.
 *
 * The function relies on duckdb_pglake_hugeint_to_string() to convert
 * the huge int to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_hugeint_to_text(duckdb_hugeint val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_hugeint_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}


/*
 * pg_uhugeint_to_text: converts a uhugeint to its string representation.
 *
 * The function relies on duckdb_pglake_uhugeint_to_string() to convert
 * the huge int to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_uhugeint_to_text(duckdb_uhugeint val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_uhugeint_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}


/*
 * pg_timestamp_to_text: converts a timestamp to its string representation.
 *
 * The function relies on duckdb_pglake_timestamp_to_string() to convert
 * the timestamp to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_timestamp_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_timestamp_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_timestamp_ns_to_text: converts a timestamp to its string representation.
 *
 * The function relies on duckdb_pglake_timestamp_ns_to_string() to convert
 * the timestamp to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_timestamp_ns_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_timestamp_ns_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_timestamp_ms_to_text: converts a timestamp to its string representation.
 *
 * The function relies on duckdb_pglake_timestamp_ms_to_string() to convert
 * the timestamp to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_timestamp_ms_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_timestamp_ms_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_timestamp_sec_to_text: converts a timestamp to its string representation.
 *
 * The function relies on duckdb_pglake_timestamp_sec_to_string() to convert
 * the timestamp to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_timestamp_sec_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_timestamp_sec_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_timestamp_tz_to_text: converts a timestamp to its string representation.
 *
 * The function relies on duckdb_pglake_timestamp_tz_to_string() to convert
 * the timestamp to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_timestamp_tz_to_text(duckdb_timestamp val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_timestamp_tz_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_interval_to_text: converts an interval to its string representation.
 *
 * The function relies on duckdb_pglake_interval_to_string() to convert
 * the interval to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_interval_to_text(duckdb_interval val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_interval_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}


/*
 * pg_time_to_text: converts a time to its string representation.
 *
 * The function relies on duckdb_pglake_time_to_string() to convert
 * the interval to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_time_to_text(duckdb_time val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_time_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_time_tz_to_text: converts a time to its string representation.
 *
 * The function relies on duckdb_pglake_time_tz_to_string() to convert
 * the interval to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_time_tz_to_text(duckdb_time_tz val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_time_tz_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}

/*
 * pg_date_to_text: converts a date to its string representation.
 *
 * The function relies on duckdb_pglake_date_to_string() to convert
 * the date to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
pg_date_to_text(duckdb_date val, TextOutputBuffer * toTextBuffer)
{
	toTextBuffer->buffer = (char *) duckdb_pglake_date_to_string(val);
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}


/*
 * decimal_to_text: converts a decimal to its string representation.
 * The function relies on duckdb_pglake_decimal_to_string() to convert the
 * decimal to a string. The function is defined in duckdb_pglake extension.
 */
static DuckDBStatus
decimal_to_text(duckdb_vector vector, duckdb_logical_type decimalType, int row, TextOutputBuffer * toTextBuffer)
{
	void	   *data = duckdb_vector_get_data(vector);

	duckdb_type valueType = duckdb_decimal_internal_type(decimalType);
	duckdb_decimal duckdbDecimal;

	duckdbDecimal.width = duckdb_decimal_width(decimalType);
	duckdbDecimal.scale = duckdb_decimal_scale(decimalType);

	switch (valueType)
	{
		case DUCKDB_TYPE_SMALLINT:
			{
				int16_t		val = VECTOR_VALUE(data, int16_t, row);

				duckdbDecimal.value.lower = (uint64_t) val;
				duckdbDecimal.value.upper = (val < 0) * -1;

				break;
			}

		case DUCKDB_TYPE_INTEGER:
			{
				int32_t		val = VECTOR_VALUE(data, int32_t, row);

				duckdbDecimal.value.lower = (uint64_t) val;
				duckdbDecimal.value.upper = (val < 0) * -1;

				break;
			}

		case DUCKDB_TYPE_BIGINT:
			{
				int64_t		val = VECTOR_VALUE(data, int64_t, row);

				duckdbDecimal.value.lower = (uint64_t) val;
				duckdbDecimal.value.upper = (val < 0) * -1;

				break;
			}

		case DUCKDB_TYPE_HUGEINT:
			{
				duckdbDecimal.value = VECTOR_VALUE(data, duckdb_hugeint, row);

				break;
			}

		default:
			{
				unsupported_type(vector, row, toTextBuffer);

				return DUCKDB_TYPE_CONVERSION_ERROR;
			}
	}

	toTextBuffer->buffer = (char *) duckdb_pglake_decimal_to_string(duckdbDecimal);

	/* duckdb_pglake_decimal_to_string does strdup, so we should free */
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}


/*
 * enum_to_text: converts an enum to its string representation.
 */
static DuckDBStatus
enum_to_text(duckdb_vector vector, duckdb_logical_type enumType, int row, TextOutputBuffer * toTextBuffer)
{
	void	   *data = duckdb_vector_get_data(vector);
	idx_t		valueIndex = 0;
	duckdb_type indexType = duckdb_enum_internal_type(enumType);

	switch (indexType)
	{
		case DUCKDB_TYPE_UTINYINT:
			valueIndex = VECTOR_VALUE(data, uint8_t, row);
			break;

		case DUCKDB_TYPE_USMALLINT:
			valueIndex = VECTOR_VALUE(data, uint16_t, row);
			break;

		case DUCKDB_TYPE_UINTEGER:
			valueIndex = VECTOR_VALUE(data, uint32_t, row);
			break;

		default:
			unsupported_type(vector, row, toTextBuffer);

			return DUCKDB_TYPE_CONVERSION_ERROR;
	}

	toTextBuffer->buffer = (char *) duckdb_enum_dictionary_value(enumType, valueIndex);

	/* duckdb_pglake_decimal_to_string does strdup, so we should free */
	toTextBuffer->needsFree = true;

	return CHECK_OOM(toTextBuffer->buffer);
}



/*
 * array_to_text(): transform an ARRAY object to its string representation.
 * This uses the base array type's to_text() formatter to walk the array and
 * join together the individual representations with commas.
 */
static DuckDBStatus
array_to_text(duckdb_vector vector, duckdb_logical_type arrayLogicalType, int row, TextOutputBuffer * toTextBuffer)
{
	DuckDBStatus status = DUCKDB_SUCCESS;

	/*
	 * Arrays are stored as fixed-size slices of the child vectors.  Nested
	 * arrays use their own array size to know how large the multiplier should
	 * be.
	 */

	/* prepare a bunch of internal structs to lookup array info */
	duckdb_logical_type arrayElemType = duckdb_array_type_child_type(arrayLogicalType);
	duckdb_type arrayElemTypeId = duckdb_get_type_id(arrayElemType);
	duckdb_vector arrayElements = duckdb_array_vector_get_child(vector);
	uint64_t   *validity = duckdb_vector_get_validity(arrayElements);

	/*
	 * Lookup the type of the contained info; we will use our normal lookup
	 * tables to dispatch to the proper textifier for the array elements.
	 */
	DuckDBTypeInfo *typeMap = find_duck_type_info(arrayElemTypeId);

	if (typeMap == NULL || typeMap->to_text == NULL)
	{
		PGDUCK_SERVER_ERROR("could not find type mapping for DuckDB type: %d", arrayElemTypeId);
		status = DUCKDB_TYPE_CONVERSION_ERROR;
		goto cleanup;
	}


	/* prepare our output structures */

	bool		firstField = true;
	StringInfoData topLevelBuffer;

	initStringInfo(&topLevelBuffer);

	appendStringInfoCharMacro(&topLevelBuffer, '{');

	/*
	 * Here we calculate the length of the specific array segment as mentioned
	 * in the leading comment, which is both the size of the for loop as well
	 * as the multiplier used with our row field to determine the index inside
	 * the array child vector.
	 */

	uint64		numElems = duckdb_array_type_array_size(arrayLogicalType);

	for (uint64 currElem = 0; currElem < numElems; currElem++)
	{
		/*
		 * The guts here are similar to duckdb_vector_to_pg_wire(), but the
		 * attempted refactor was way more expansive than we need to just
		 * accommodate a few duplicated lines here. Just leave for now.
		 */

		char		buffer[STACK_ALLOCATED_OUTPUT_BUFFER_SIZE];
		TextOutputBuffer elementOutputBuffer = {
			.buffer = buffer,
			.needsFree = false
		};

		/*
		 * The child vector for a multi-level array is scaled by the row size,
		 * so we need to use the input row to know where the specific array
		 * level's offset starts.
		 */

		int			currentArrayStart = row * numElems;

		/*
		 * We need to do a NULL check and set the buffer appropriately.  If
		 * NULL, we just clear the pointer, since AppendFieldValue will do the
		 * right thing here.
		 *
		 * Otherwise, convert the sub-element to text.
		 */

		uint64		elementVectorOffset = currentArrayStart + currElem;

		if (validity && !duckdb_validity_row_is_valid(validity, elementVectorOffset))
			elementOutputBuffer.buffer = NULL;
		else
		{
			status = typeMap->to_text(arrayElements,
									  arrayElemType,
									  elementVectorOffset,
									  &elementOutputBuffer);

			if (status != DUCKDB_SUCCESS)
			{
				CleanupOutputBuffer(&elementOutputBuffer);
				goto cleanup;
			}
		}

		if (firstField)
			firstField = false;
		else
			appendStringInfoCharMacro(&topLevelBuffer, ',');

		if (arrayElemTypeId == DUCKDB_TYPE_ARRAY || arrayElemTypeId == DUCKDB_TYPE_LIST)
			/* nested types should not be quoted again */
			appendStringInfoString(&topLevelBuffer, elementOutputBuffer.buffer);
		else
			AppendFieldValue(elementOutputBuffer.buffer, false, &topLevelBuffer);

		/* per-loop cleanup */
		if (elementOutputBuffer.needsFree)
		{
			pg_free(elementOutputBuffer.buffer);
			elementOutputBuffer.buffer = NULL;
		}
	}

	/*
	 * All fields are done, let's now output the closing chars and
	 * cleanup/handle the rest
	 */

	appendStringInfoCharMacro(&topLevelBuffer, '}');

	toTextBuffer->buffer = topLevelBuffer.data;
	toTextBuffer->needsFree = true;

cleanup:
	if (arrayElemType)
		duckdb_destroy_logical_type(&arrayElemType);

	return status;
}


/*
 * list_to_text(): transformat a LIST object to its string representation.  This
 * uses the List's element type to look up the proper to_text() formatter and
 * maps over the contained elements.
 */
static DuckDBStatus
list_to_text(duckdb_vector vector, duckdb_logical_type listLogicalType, int row, TextOutputBuffer * toTextBuffer)
{
	DuckDBStatus status = DUCKDB_SUCCESS;

	/*
	 * Lists of a simple type are fairly straightforward; they are stored as a
	 * vector, where duckdb_list_vector_get_size directly corresponds to the
	 * number of elements in the list.  However, when using nested lists,
	 * duckdb_list_vector_get_size() no longer works as expected, and we need
	 * to break into more of the internal details.
	 *
	 * In DuckDB, all of the elements in a single level of the list are
	 * included in the same vector, so for a nested structure such as
	 * `[[1,2],[3,4]` this is stored as a List of Lists with a grandchild
	 * vector that just contains `1,2,3,4`.  Interpreting how the nested
	 * structure works is not well-documented in the DuckDB code and docs, so
	 * it took a while to figure it out.
	 *
	 * The vector data for the top-level list is a sequence of
	 * duckdb_list_entry structs, themselves just a simple offset/length.  For
	 * top-level lists, the simple length is the same as the length of the
	 * arrays.  Where this comes into play is for nested lists.  The second
	 * level of a list of lists has N duckdb_list_entry structs (one for each
	 * base element), which track the offset and length into the child vector
	 * (which, again, has all of the elements in the same level represented.)
	 *
	 * What this means in practice is when we iterate over the children we
	 * just need to make sure to iterate over the correct offsets/values for
	 * the specific subchild.  Since this is effectively recursive, we use the
	 * input row argument and the child vector to locate the correct list
	 * struct, and this ends up working fine in both cases.
	 */

	/* prepare a bunch of internal structs to lookup list info */
	duckdb_logical_type listElemType = duckdb_list_type_child_type(listLogicalType);
	duckdb_list_entry *listElemStruct =
		(duckdb_list_entry *) duckdb_vector_get_data(vector);
	duckdb_type listElemTypeId = duckdb_get_type_id(listElemType);
	duckdb_vector listElements = duckdb_list_vector_get_child(vector);
	uint64_t   *validity = duckdb_vector_get_validity(listElements);

	/*
	 * Lookup the type of the contained info; we will use our normal lookup
	 * tables to dispatch to the proper textifier for the list elements.
	 */
	DuckDBTypeInfo *typeMap = find_duck_type_info(listElemTypeId);

	if (typeMap == NULL || typeMap->to_text == NULL)
	{
		PGDUCK_SERVER_ERROR("could not find type mapping for DuckDB type: %d", listElemTypeId);
		status = DUCKDB_TYPE_CONVERSION_ERROR;
		goto cleanup;
	}

	/* prepare our output structures */

	bool		firstField = true;
	StringInfoData topLevelBuffer;

	initStringInfo(&topLevelBuffer);

	appendStringInfoCharMacro(&topLevelBuffer, '{');

	/*
	 * Here we look up the length of the specific list segment as mentioned in
	 * the leading comment.  We then loop through the range of sub elements,
	 * adjusting by the offset of this same segment descriptor so any
	 * recursive bits point to the right range of the child data vector.
	 */

	uint64		numElems = listElemStruct[row].length;

	for (uint64 currElem = 0; currElem < numElems; currElem++)
	{
		/*
		 * The guts here are similar to duckdb_vector_to_pg_wire(), but the
		 * attempted refactor was way more expansive than we need to just
		 * accommodate a few duplicated lines here. Just leave for now.
		 */

		char		buffer[STACK_ALLOCATED_OUTPUT_BUFFER_SIZE];
		TextOutputBuffer elementOutputBuffer = {
			.buffer = buffer,
			.needsFree = false
		};

		/*
		 * We need to do a NULL check and set the buffer appropriately.  If
		 * NULL, we just clear the pointer, since AppendFieldValue will do the
		 * right thing here.
		 *
		 * Otherwise, convert the sub-element to text.  Note that we are
		 * offsetting our index by the listElemStruct's offset so we access
		 * the correct element range.
		 */

		uint64		elementVectorOffset = currElem + listElemStruct[row].offset;

		if (validity && !duckdb_validity_row_is_valid(validity, elementVectorOffset))
			elementOutputBuffer.buffer = NULL;
		else
		{
			status = typeMap->to_text(listElements,
									  listElemType,
									  elementVectorOffset,
									  &elementOutputBuffer);

			if (status != DUCKDB_SUCCESS)
			{
				CleanupOutputBuffer(&elementOutputBuffer);
				goto cleanup;
			}
		}

		if (firstField)
			firstField = false;
		else
			appendStringInfoCharMacro(&topLevelBuffer, ',');

		if (listElemTypeId == DUCKDB_TYPE_ARRAY || listElemTypeId == DUCKDB_TYPE_LIST)
			/* nested types should not be quoted again */
			appendStringInfoString(&topLevelBuffer, elementOutputBuffer.buffer);
		else
			AppendFieldValue(elementOutputBuffer.buffer, false, &topLevelBuffer);

		/* per-loop cleanup */
		if (elementOutputBuffer.needsFree)
		{
			pg_free(elementOutputBuffer.buffer);
			elementOutputBuffer.buffer = NULL;
		}
	}

	/*
	 * All fields are done, let's now output the closing chars and
	 * cleanup/handle the rest
	 */

	appendStringInfoCharMacro(&topLevelBuffer, '}');

	toTextBuffer->buffer = topLevelBuffer.data;
	toTextBuffer->needsFree = true;

cleanup:
	if (listElemType)
		duckdb_destroy_logical_type(&listElemType);

	return status;
}


/*
 * map_to_text(): transform a MAP object to its string representation.  Maps are
 * effectively a STRUCT(key, value)[] where keys are in sorted order.
 */
static DuckDBStatus
map_to_text(duckdb_vector vector, duckdb_logical_type mapLogicalType, int row, TextOutputBuffer * toTextBuffer)
{
	return list_to_text(vector, mapLogicalType, row, toTextBuffer);
}


/*
 * struct_to_text(): transform a STRUCT object to its string representation.
 * Structs have string keys and any-valued types, so the representation requires
 * checking each column for its type, rather than being able to cache the type
 * info like the list and array type do.
 *
 * Structs in Postgres map to known-created composite types, so we do not
 * provide anything about the names of the fields, just use paren-delimited
 * values.
 */
static DuckDBStatus
struct_to_text(duckdb_vector vector, duckdb_logical_type structLogicalType, int row, TextOutputBuffer * toTextBuffer)
{
	DuckDBStatus status = DUCKDB_SUCCESS;

	/* prepare our output structures */

	bool		firstField = true;
	StringInfoData topLevelBuffer;

	initStringInfo(&topLevelBuffer);

	appendStringInfoCharMacro(&topLevelBuffer, '(');

	/*
	 * Here we iterate over the child columns, dispatching each by column
	 * type.
	 */

	uint64		numElems = duckdb_struct_type_child_count(structLogicalType);
	duckdb_logical_type structElemType = NULL;
	duckdb_type structElemTypeId = DUCKDB_TYPE_INVALID;

	for (uint64 currElem = 0; currElem < numElems; currElem++)
	{
		/* cleanup previous logical type if we need to */
		if (structElemType)
			duckdb_destroy_logical_type(&structElemType);

		structElemType = duckdb_struct_type_child_type(structLogicalType, currElem);
		structElemTypeId = duckdb_get_type_id(structElemType);
		duckdb_vector structElements = duckdb_struct_vector_get_child(vector, currElem);
		uint64_t   *validity = duckdb_vector_get_validity(structElements);

		/*
		 * Lookup the type of the contained info; we will use our normal
		 * lookup tables to dispatch to the proper textifier for the struct
		 * elements.
		 */
		DuckDBTypeInfo *typeMap = find_duck_type_info(structElemTypeId);

		if (typeMap == NULL || typeMap->to_text == NULL)
		{
			PGDUCK_SERVER_ERROR("could not find type mapping for DuckDB type: %d", structElemTypeId);
			status = DUCKDB_TYPE_CONVERSION_ERROR;
			goto cleanup;
		}

		/*
		 * The guts here are similar to duckdb_vector_to_pg_wire(), but the
		 * attempted refactor was way more expansive than we need to just
		 * accommodate a few duplicated lines here. Just leave for now.
		 */

		char		buffer[STACK_ALLOCATED_OUTPUT_BUFFER_SIZE];
		TextOutputBuffer elementOutputBuffer = {
			.buffer = buffer,
			.needsFree = false
		};

		/*
		 * We need to do a NULL check and set the buffer appropriately.  If
		 * NULL, we just clear the pointer, since AppendFieldValue will do the
		 * right thing here.
		 *
		 * Otherwise, convert the sub-element to text.
		 *
		 * Since each individual struct field has its own child vector, we
		 * always use the row offset for this.
		 */
		if (validity && !duckdb_validity_row_is_valid(validity, row))
			elementOutputBuffer.buffer = NULL;
		else
		{
			status = typeMap->to_text(structElements, structElemType, row, &elementOutputBuffer);

			if (status != DUCKDB_SUCCESS)
			{
				CleanupOutputBuffer(&elementOutputBuffer);
				goto cleanup;
			}
		}

		if (firstField)
			firstField = false;
		else
			appendStringInfoCharMacro(&topLevelBuffer, ',');

		AppendFieldValue(elementOutputBuffer.buffer, true, &topLevelBuffer);

		if (elementOutputBuffer.needsFree)
		{
			pg_free(elementOutputBuffer.buffer);
			elementOutputBuffer.buffer = NULL;
		}
	}

	/*
	 * All fields are done, let's now output the closing chars and
	 * cleanup/handle the rest
	 */

	appendStringInfoCharMacro(&topLevelBuffer, ')');

	toTextBuffer->buffer = topLevelBuffer.data;
	toTextBuffer->needsFree = true;

cleanup:
	if (structElemType)
		duckdb_destroy_logical_type(&structElemType);

	return status;
}


/*
 * Utility function to output a potentially quoted field value to a StringInfo.
 * This handles quoting rules for both arrays and records, and is not a
 * general-purpose quoting routine.  (See parsing details in array_out() and
 * record_out() for inspiration.
 */
static void
AppendFieldValue(const char *value, bool useRecordQuoting, StringInfo output)
{
	const char *tmp;

	/* NULL handling as fast-path */
	if (!value)
	{
		/*
		 * records handle NULL fields by omitting the field, arrays include
		 * literal NULL string
		 */

		if (!useRecordQuoting)
			appendStringInfoString(output, "NULL");
		return;
	}

	if (useRecordQuoting)
	{
		/* swiped from rowtypes.c record_out() */
		bool		nq = value[0] == '\0';

		for (tmp = value; *tmp; tmp++)
		{
			char		ch = *tmp;

			if (ch == '"' || ch == '\\' ||
				ch == '(' || ch == ')' || ch == ',' ||
				isspace((unsigned char) ch))
			{
				nq = true;
				break;
			}
		}

		/* And emit the string */
		if (nq)
			appendStringInfoCharMacro(output, '"');
		for (tmp = value; *tmp; tmp++)
		{
			char		ch = *tmp;

			if (ch == '"' || ch == '\\')
				appendStringInfoCharMacro(output, ch);
			appendStringInfoCharMacro(output, ch);
		}
		if (nq)
			appendStringInfoCharMacro(output, '"');
	}
	else
	{
		/* swiped and lightly modified from array_out */
		bool		needquote = true;
		char		typdelim = ','; /* technically this can be set per type,
									 * but only 'box' uses, and we don't
									 * really care about supporting that one
									 * natively */

		/* count data plus backslashes; detect chars needing quotes */
		if (value[0] == '\0')
			needquote = true;	/* force quotes for empty string */
		else if (pg_strcasecmp(value, "NULL") == 0)
			needquote = true;	/* force quotes for literal NULL */
		else
			needquote = false;

		for (tmp = value; *tmp != '\0'; tmp++)
		{
			char		ch = *tmp;

			if (ch == '"' || ch == '\\')
			{
				needquote = true;
			}
			else if (ch == '{' || ch == '}' || ch == typdelim ||
					 isspace(ch))
				needquote = true;
		}

#define APPENDSTR(str)	appendStringInfoString(output, str)
#define APPENDCHAR(ch)	appendStringInfoCharMacro(output, ch)

		if (needquote)
		{
			APPENDCHAR('"');
			for (tmp = value; *tmp; tmp++)
			{
				char		ch = *tmp;

				if (ch == '"' || ch == '\\')
					APPENDCHAR('\\');
				APPENDCHAR(ch);
			}
			APPENDCHAR('"');
		}
		else
			APPENDSTR(value);

#undef APPENDSTR
#undef APPENDCHAR
	}
}
