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

#include <math.h>

#include "catalog/pg_type_d.h"
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/iceberg/iceberg_type_numeric_binary_serde.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/util/timetz.h"

#include "port/pg_bswap.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

#ifdef WORDS_BIGENDIAN
#define ToLittleEndian32(val) pg_bswap32(val)
#define ToLittleEndian64(val) pg_bswap64(val)
#else
#define ToLittleEndian32(val) (val)
#define ToLittleEndian64(val) (val)
#endif

#ifdef WORDS_BIGENDIAN
#define FromLittleEndian32(val) pg_bswap32(val)
#define FromLittleEndian64(val) pg_bswap64(val)
#else
#define FromLittleEndian32(val) (val)
#define FromLittleEndian64(val) (val)
#endif


/*
 * FixedWidthBinaryLength is the serialized width of one Iceberg primitive.
 *
 * promotedFromLen is the narrower width the spec still allows, because column
 * metrics keep the type the data file was written with: after an int -> long or
 * float -> double promotion, older files carry 4-byte values for a type that is
 * now 8 bytes wide. Zero means no narrower form is accepted.
 *
 * Types with no fixed width (text, bytea, numeric, and anything Iceberg encodes
 * as a string) are deliberately absent and bounded by their own branch instead.
 */
typedef struct FixedWidthBinaryLength
{
	Oid			postgresTypeOid;
	size_t		expectedLen;
	size_t		promotedFromLen;
}			FixedWidthBinaryLength;

static const FixedWidthBinaryLength FixedWidthBinaryLengths[] = {
	{BOOLOID, 1, 0},
	/* iceberg does not have int16 type. we store them as int32. */
	{INT2OID, sizeof(int32), 0},
	{INT4OID, sizeof(int32), 0},
	{INT8OID, sizeof(int64), sizeof(int32)},
	{FLOAT4OID, sizeof(float4), 0},
	{FLOAT8OID, sizeof(float8), sizeof(float4)},
	{DATEOID, sizeof(DateADT), 0},
	{TIMEOID, sizeof(TimeADT), 0},
	/* timetz is stored as iceberg time, i.e. UTC microseconds */
	{TIMETZOID, sizeof(int64), 0},
	{TIMESTAMPOID, sizeof(Timestamp), 0},
	{TIMESTAMPTZOID, sizeof(TimestampTz), 0},
	{UUIDOID, UUID_LEN, 0},
};


static unsigned char *PGIcebergBinarySerialize(Datum datum, Field * field, PGType pgType, bool addNullTerminator, size_t *binaryLen);
static void ErrorIfUnexpectedBinaryLength(size_t binaryLen, Field * field, PGType pgType);
static int32 ReadLittleEndianInt32(const unsigned char *binaryValue);
static int64 ReadLittleEndianInt64(const unsigned char *binaryValue);
static float4 ReadLittleEndianFloat4(const unsigned char *binaryValue);
static float8 ReadLittleEndianFloat8(const unsigned char *binaryValue);
static unsigned char *WriteLittleEndianInt32(int32 value);
static unsigned char *WriteLittleEndianInt64(int64 value);
static unsigned char *WriteLittleEndianFloat4(float4 value);
static unsigned char *WriteLittleEndianFloat8(float8 value);


/*
 * PGIcebergBinarySerializeBoundValue binary serializes upper or lower bound value.
 * It uses PGIcebergBinarySerialize by setting addNullTerminator to false.
 * lower and upper bounds for data file stats and partition summary are binary serialized.
 * They both are of avro "bytes" type. For those, addNullTerminator should be set to false.
 */
unsigned char *
PGIcebergBinarySerializeBoundValue(Datum datum, Field * field, PGType pgType, size_t *binaryLen)
{
	bool		addNullTerminator = false;

	return PGIcebergBinarySerialize(datum, field, pgType, addNullTerminator, binaryLen);
}


/*
 * PGIcebergBinarySerializePartitionFieldValue binary serializes partition field value.
 * It uses PGIcebergBinarySerialize by setting addNullTerminator to true.
 * Partition field values are of avro types of the values itself. For text values,
 * addNullTerminator should be set to true.
 */
unsigned char *
PGIcebergBinarySerializePartitionFieldValue(Datum datum, Field * field, PGType pgType, size_t *binaryLen)
{
	bool		addNullTerminator = true;

	return PGIcebergBinarySerialize(datum, field, pgType, addNullTerminator, binaryLen);
}


/*
 * PGIcebergBinarySerialize converts PG datum to binary serialized value according to
 * Iceberg spec. https://iceberg.apache.org/spec/#binary-single-value-serialization
 * Only scalar types are supported for single value binary serde.
 */
static unsigned char *
PGIcebergBinarySerialize(Datum datum, Field * field, PGType pgType, bool addNullTerminator, size_t *binaryLen)
{
	Assert(field->type == FIELD_TYPE_SCALAR);

	unsigned char *binaryValue = NULL;

	if (PGTypeRequiresConversionToIcebergString(field, pgType))
	{
		/*
		 * Some types are not represented natively in iceberg spec, e.g.
		 * geometry. Convert to text before binary serializing them.
		 */
		Oid			typoutput;
		bool		typIsVarlena;

		getTypeOutputInfo(pgType.postgresTypeOid, &typoutput, &typIsVarlena);
		const char *datumAsString = OidOutputFunctionCall(typoutput, datum);

		*binaryLen = strlen(datumAsString);

		if (addNullTerminator)
			*binaryLen = *binaryLen + 1;

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, datumAsString, *binaryLen);
	}
	else if (pgType.postgresTypeOid == BOOLOID)
	{
		bool		boolValue = DatumGetBool(datum);

		*binaryLen = 1;

		binaryValue = (boolValue) ? (unsigned char *) "\x01" : (unsigned char *) "\x00";
	}
	else if (pgType.postgresTypeOid == INT2OID)
	{
		/* iceberg does not have int16 type. we store them as int32. */
		int32		shortValue = DatumGetInt16(datum);

		*binaryLen = sizeof(int32);
		binaryValue = WriteLittleEndianInt32(shortValue);
	}
	else if (pgType.postgresTypeOid == INT4OID)
	{
		int32		intValue = DatumGetInt32(datum);

		*binaryLen = sizeof(int32);
		binaryValue = WriteLittleEndianInt32(intValue);
	}
	else if (pgType.postgresTypeOid == INT8OID)
	{
		int64		longValue = DatumGetInt64(datum);

		*binaryLen = sizeof(int64);
		binaryValue = WriteLittleEndianInt64(longValue);
	}
	else if (pgType.postgresTypeOid == FLOAT4OID)
	{
		float4		floatValue = DatumGetFloat4(datum);

		/*
		 * NaN is not allowed by icebeg spec. Instead of hard error, lets
		 * return NULL.
		 */
		if (isnan(floatValue))
		{
			*binaryLen = 0;
			return NULL;
		}

		/*
		 * TODO: normally should allow. Fix Old repo: issues/935
		 */
		if (isinf(floatValue))
		{
			*binaryLen = 0;
			return NULL;
		}

		*binaryLen = sizeof(float4);
		binaryValue = WriteLittleEndianFloat4(floatValue);
	}
	else if (pgType.postgresTypeOid == FLOAT8OID)
	{
		float8		doubleValue = DatumGetFloat8(datum);

		/*
		 * NaN is not allowed by icebeg spec. Instead of hard error, lets
		 * return NULL.
		 */
		if (isnan(doubleValue))
		{
			*binaryLen = 0;
			return NULL;
		}

		/*
		 * TODO: normally should allow. Fix Old repo: issues/935
		 */
		if (isinf(doubleValue))
		{
			*binaryLen = 0;
			return NULL;
		}

		*binaryLen = sizeof(float8);
		binaryValue = WriteLittleEndianFloat8(doubleValue);
	}
	else if (pgType.postgresTypeOid == DATEOID)
	{
		DateADT		dateValue = DatumGetDateADT(datum);

		dateValue = AdjustDateFromPostgresToUnix(dateValue);

		*binaryLen = sizeof(DateADT);
		binaryValue = WriteLittleEndianInt32(dateValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	timestampValue = DatumGetTimestamp(datum);

		timestampValue = AdjustTimestampFromPostgresToUnix(timestampValue);

		*binaryLen = sizeof(Timestamp);
		binaryValue = WriteLittleEndianInt64(timestampValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz timestampTzValue = DatumGetTimestampTz(datum);

		timestampTzValue = AdjustTimestampFromPostgresToUnix(timestampTzValue);

		*binaryLen = sizeof(TimestampTz);
		binaryValue = WriteLittleEndianInt64(timestampTzValue);
	}
	else if (pgType.postgresTypeOid == TIMEOID)
	{
		TimeADT		timeValue = DatumGetTimeADT(datum);

		*binaryLen = sizeof(TimeADT);
		binaryValue = WriteLittleEndianInt64(timeValue);
	}
	else if (pgType.postgresTypeOid == TIMETZOID)
	{
		/*
		 * Iceberg stores time as microseconds since midnight (no timezone).
		 * Convert timetz to UTC microseconds.
		 */
		TimeTzADT  *timetz = DatumGetTimeTzADTP(datum);
		TimeADT		utcMicros = TimeTzGetUTCMicros(timetz);

		*binaryLen = sizeof(int64);
		binaryValue = WriteLittleEndianInt64(utcMicros);
	}
	else if (pgType.postgresTypeOid == TEXTOID)
	{
		const char *textValue = TextDatumGetCString(datum);

		*binaryLen = strlen(textValue);

		if (addNullTerminator)
			*binaryLen = *binaryLen + 1;

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, textValue, *binaryLen);
	}
	else if (pgType.postgresTypeOid == BYTEAOID)
	{
		bytea	   *byteaValue = DatumGetByteaP(datum);

		*binaryLen = VARSIZE(byteaValue) - VARHDRSZ;

		char	   *byteaData = palloc(*binaryLen);

		memcpy(byteaData, VARDATA(byteaValue), *binaryLen);

		binaryValue = (unsigned char *) byteaData;
	}
	else if (pgType.postgresTypeOid == NUMERICOID)
	{
		/*
		 * Generate numeric binary in big-endian.  The Iceberg spec requires
		 * the unscaled integer at the schema's scale, so we must pass the
		 * target scale.  For unbounded numeric (typmod == -1) this falls back
		 * to the default Iceberg scale via
		 * GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod.
		 */
		int			precision;
		int			scale;

		GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(
															 pgType.postgresTypeMod,
															 &precision, &scale);

		binaryValue = PGNumericIcebergBinarySerialize(datum, scale,
													  binaryLen);
	}
	else if (pgType.postgresTypeOid == UUIDOID)
	{
		pg_uuid_t  *uuidValue = DatumGetUUIDP(datum);

		*binaryLen = sizeof(uuidValue->data);

		/* uuid is stored in big endian by Postgres and Iceberg */
		binaryValue = uuidValue->data;
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("unsupported type for binary serialization: %u", pgType.postgresTypeOid)));
	}

	return binaryValue;
}


/*
 * PGIcebergBinaryDeserialize converts binary serialized Iceberg type to datum according to
 * Iceberg spec. https://iceberg.apache.org/spec/#binary-single-value-serialization
 * Only scalar types are supported for single value binary serde.
 */
Datum
PGIcebergBinaryDeserialize(unsigned char *binaryValue, size_t binaryLen, Field * field, PGType pgType)
{
	Assert(field && field->type == FIELD_TYPE_SCALAR);
	Assert(binaryValue);

	ErrorIfUnexpectedBinaryLength(binaryLen, field, pgType);

	Datum		datum = 0;

	if (PGTypeRequiresConversionToIcebergString(field, pgType))
	{
		/*
		 * Some types are not represented natively in iceberg spec, e.g.
		 * geometry. Deserialize to text datum before converting to native
		 * datum.
		 */
		char	   *stringRepr = palloc0(binaryLen + 1);

		memcpy(stringRepr, (char *) binaryValue, binaryLen);

		/* convert to native datum from string repr */
		Oid			typoinput;
		Oid			typioparam;

		getTypeInputInfo(pgType.postgresTypeOid, &typoinput, &typioparam);

		datum = OidInputFunctionCall(typoinput, stringRepr, typioparam, pgType.postgresTypeMod);
	}
	else if (pgType.postgresTypeOid == BOOLOID)
	{
		bool		boolValue = binaryValue[0] == 0x01;

		datum = BoolGetDatum(boolValue);
	}
	else if (pgType.postgresTypeOid == INT2OID)
	{
		/* iceberg does not have int16 type. we store them as int32. */
		int16		shortValue = (int16) ReadLittleEndianInt32(binaryValue);

		datum = Int16GetDatum(shortValue);
	}
	else if (pgType.postgresTypeOid == INT4OID)
	{
		int32		intValue = ReadLittleEndianInt32(binaryValue);

		datum = Int32GetDatum(intValue);
	}
	else if (pgType.postgresTypeOid == INT8OID)
	{
		/*
		 * Per the Iceberg spec, column metrics are serialized using the type
		 * at the time the data file was written. After an int -> long type
		 * promotion, existing data files retain 4-byte int bounds while the
		 * current schema expects 8-byte long. Widen on read.
		 *
		 * See:
		 * https://iceberg.apache.org/spec/#binary-single-value-serialization
		 */
		if (binaryLen == sizeof(int32))
		{
			int32		intValue = ReadLittleEndianInt32(binaryValue);

			datum = Int64GetDatum((int64) intValue);
		}
		else
		{
			int64		longValue = ReadLittleEndianInt64(binaryValue);

			datum = Int64GetDatum(longValue);
		}
	}
	else if (pgType.postgresTypeOid == FLOAT4OID)
	{
		float4		floatValue = ReadLittleEndianFloat4(binaryValue);

		datum = Float4GetDatum(floatValue);
	}
	else if (pgType.postgresTypeOid == FLOAT8OID)
	{
		/*
		 * Same as int -> long above: after a float -> double type promotion,
		 * existing data files retain 4-byte float bounds while the current
		 * schema expects 8-byte double. Widen on read.
		 */
		if (binaryLen == sizeof(float4))
		{
			float4		floatValue = ReadLittleEndianFloat4(binaryValue);

			datum = Float8GetDatum((float8) floatValue);
		}
		else
		{
			float8		doubleValue = ReadLittleEndianFloat8(binaryValue);

			datum = Float8GetDatum(doubleValue);
		}
	}
	else if (pgType.postgresTypeOid == DATEOID)
	{
		DateADT		dateValue = ReadLittleEndianInt32(binaryValue);

		dateValue = AdjustDateFromUnixToPostgres(dateValue);

		datum = DateADTGetDatum(dateValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	timestampValue = ReadLittleEndianInt64(binaryValue);

		timestampValue = AdjustTimestampFromUnixToPostgres(timestampValue);

		datum = TimestampGetDatum(timestampValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz timestampTzValue = ReadLittleEndianInt64(binaryValue);

		timestampTzValue = AdjustTimestampFromUnixToPostgres(timestampTzValue);

		datum = TimestampTzGetDatum(timestampTzValue);
	}
	else if (pgType.postgresTypeOid == TIMEOID)
	{
		TimeADT		timeValue = ReadLittleEndianInt64(binaryValue);

		datum = TimeADTGetDatum(timeValue);
	}
	else if (pgType.postgresTypeOid == TIMETZOID)
	{
		/*
		 * Iceberg stores time as UTC microseconds since midnight. Deserialize
		 * into a TimeTzADT at UTC (zone = 0).
		 */
		int64		utcMicros = ReadLittleEndianInt64(binaryValue);

		TimeTzADT  *timetz = palloc(sizeof(TimeTzADT));

		timetz->time = utcMicros;
		timetz->zone = 0;

		datum = TimeTzADTPGetDatum(timetz);
	}
	else if (pgType.postgresTypeOid == TEXTOID)
	{
		char	   *strValue = palloc0(binaryLen + 1);

		memcpy(strValue, (char *) binaryValue, binaryLen);

		datum = CStringGetTextDatum(strValue);
	}
	else if (pgType.postgresTypeOid == BYTEAOID)
	{
		bytea	   *byteaValue = palloc(binaryLen + VARHDRSZ);

		SET_VARSIZE(byteaValue, binaryLen + VARHDRSZ);
		memcpy(VARDATA(byteaValue), (char *) binaryValue, binaryLen);

		datum = PointerGetDatum(byteaValue);
	}
	else if (pgType.postgresTypeOid == NUMERICOID)
	{
		/* deserialize numeric datum from numeric binary in big endian */
		datum = PGNumericIcebergBinaryDeserialize(binaryValue, binaryLen, pgType);
	}
	else if (pgType.postgresTypeOid == UUIDOID)
	{
		pg_uuid_t  *uuidValue = palloc(sizeof(pg_uuid_t));

		/* uuid is stored in big endian by Postgres and Iceberg spec */
		memcpy(&uuidValue->data, binaryValue, sizeof(uuidValue->data));

		datum = UUIDPGetDatum(uuidValue);
	}
	else
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("unsupported type for binary deserialization: %u", pgType.postgresTypeOid)));
	}

	return datum;
}


/*
 * ErrorIfUnexpectedBinaryLength rejects a fixed-width value that is not exactly
 * as wide as its type. The bytes are Avro "bytes" of arbitrary length taken from
 * a manifest we did not necessarily write, so without this every fixed-width
 * read in PGIcebergBinaryDeserialize is an out-of-bounds read waiting for a
 * short value.
 */
static void
ErrorIfUnexpectedBinaryLength(size_t binaryLen, Field * field, PGType pgType)
{
	/* string-encoded types carry a value of any length, e.g. geometry */
	if (PGTypeRequiresConversionToIcebergString(field, pgType))
		return;

	for (int i = 0; i < lengthof(FixedWidthBinaryLengths); i++)
	{
		if (FixedWidthBinaryLengths[i].postgresTypeOid != pgType.postgresTypeOid)
			continue;

		size_t		expectedLen = FixedWidthBinaryLengths[i].expectedLen;
		size_t		promotedFromLen = FixedWidthBinaryLengths[i].promotedFromLen;

		if (binaryLen == expectedLen ||
			(promotedFromLen > 0 && binaryLen == promotedFromLen))
			return;

		char	   *expectedLengths = promotedFromLen > 0 ?
			psprintf("%zu or %zu", promotedFromLen, expectedLen) :
			psprintf("%zu", expectedLen);

		ereport(ERROR,
				(errcode(ERRCODE_DATA_CORRUPTED),
				 errmsg("Iceberg value of type %s has invalid serialized length %zu",
						format_type_be(pgType.postgresTypeOid), binaryLen),
				 errdetail("Expected %s bytes.", expectedLengths)));
	}
}


/*
 * Iceberg serializes fixed-width primitives little-endian. Go through a memcpy
 * in both directions rather than a cast: the byte swap has to apply to the value
 * and not to the pointer, and a value read out of an Avro buffer has no
 * alignment guarantee.
 */
static int32
ReadLittleEndianInt32(const unsigned char *binaryValue)
{
	uint32		rawValue;

	memcpy(&rawValue, binaryValue, sizeof(rawValue));

	return (int32) FromLittleEndian32(rawValue);
}


static int64
ReadLittleEndianInt64(const unsigned char *binaryValue)
{
	uint64		rawValue;

	memcpy(&rawValue, binaryValue, sizeof(rawValue));

	return (int64) FromLittleEndian64(rawValue);
}


static float4
ReadLittleEndianFloat4(const unsigned char *binaryValue)
{
	int32		rawValue = ReadLittleEndianInt32(binaryValue);
	float4		floatValue;

	memcpy(&floatValue, &rawValue, sizeof(floatValue));

	return floatValue;
}


static float8
ReadLittleEndianFloat8(const unsigned char *binaryValue)
{
	int64		rawValue = ReadLittleEndianInt64(binaryValue);
	float8		doubleValue;

	memcpy(&doubleValue, &rawValue, sizeof(doubleValue));

	return doubleValue;
}


static unsigned char *
WriteLittleEndianInt32(int32 value)
{
	uint32		rawValue = ToLittleEndian32((uint32) value);
	unsigned char *binaryValue = palloc(sizeof(rawValue));

	memcpy(binaryValue, &rawValue, sizeof(rawValue));

	return binaryValue;
}


static unsigned char *
WriteLittleEndianInt64(int64 value)
{
	uint64		rawValue = ToLittleEndian64((uint64) value);
	unsigned char *binaryValue = palloc(sizeof(rawValue));

	memcpy(binaryValue, &rawValue, sizeof(rawValue));

	return binaryValue;
}


static unsigned char *
WriteLittleEndianFloat4(float4 value)
{
	int32		rawValue;

	memcpy(&rawValue, &value, sizeof(rawValue));

	return WriteLittleEndianInt32(rawValue);
}


static unsigned char *
WriteLittleEndianFloat8(float8 value)
{
	int64		rawValue;

	memcpy(&rawValue, &value, sizeof(rawValue));

	return WriteLittleEndianInt64(rawValue);
}
