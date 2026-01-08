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
#include "fmgr.h"
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/iceberg/iceberg_type_numeric_binary_serde.h"
#include "pg_lake/iceberg/utils.h"

#include "port/pg_bswap.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"
#include "utils/timestamp.h"

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


static unsigned char *PGIcebergBinarySerialize(Datum datum, Field * field, PGType pgType, bool addNullTerminator, size_t *binaryLen);


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

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &shortValue, *binaryLen);

		binaryValue = ToLittleEndian32(binaryValue);
	}
	else if (pgType.postgresTypeOid == INT4OID)
	{
		int32		intValue = DatumGetInt32(datum);

		*binaryLen = sizeof(int32);

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &intValue, *binaryLen);

		binaryValue = ToLittleEndian32(binaryValue);
	}
	else if (pgType.postgresTypeOid == INT8OID)
	{
		int64		longValue = DatumGetInt64(datum);

		*binaryLen = sizeof(int64);

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &longValue, *binaryLen);

		binaryValue = ToLittleEndian64(binaryValue);
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

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &floatValue, *binaryLen);

		binaryValue = ToLittleEndian32(binaryValue);
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

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &doubleValue, *binaryLen);

		binaryValue = ToLittleEndian64(binaryValue);
	}
	else if (pgType.postgresTypeOid == DATEOID)
	{
		DateADT		dateValue = DatumGetDateADT(datum);

		dateValue = AdjustDateFromPostgresToUnix(dateValue);

		*binaryLen = sizeof(DateADT);

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &dateValue, *binaryLen);

		binaryValue = ToLittleEndian32(binaryValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	timestampValue = DatumGetTimestamp(datum);

		timestampValue = AdjustTimestampFromPostgresToUnix(timestampValue);

		*binaryLen = sizeof(Timestamp);

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &timestampValue, *binaryLen);

		binaryValue = ToLittleEndian64(binaryValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz timestampTzValue = DatumGetTimestampTz(datum);

		timestampTzValue = AdjustTimestampFromPostgresToUnix(timestampTzValue);

		*binaryLen = sizeof(TimestampTz);

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &timestampTzValue, *binaryLen);

		binaryValue = ToLittleEndian64(binaryValue);
	}
	else if (pgType.postgresTypeOid == TIMEOID)
	{
		TimeADT		timeValue = DatumGetTimeADT(datum);

		*binaryLen = sizeof(TimeADT);

		binaryValue = palloc0(*binaryLen);
		memcpy(binaryValue, (unsigned char *) &timeValue, *binaryLen);

		binaryValue = ToLittleEndian64(binaryValue);
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
		/* generate numeric binary in bigendian */
		binaryValue = PGNumericIcebergBinarySerialize(datum, binaryLen);
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
		binaryValue = FromLittleEndian32(binaryValue);

		/* iceberg does not have int16 type. we store them as int32. */
		int16		shortValue = (int16) *((int32 *) binaryValue);

		datum = Int16GetDatum(shortValue);
	}
	else if (pgType.postgresTypeOid == INT4OID)
	{
		binaryValue = FromLittleEndian32(binaryValue);
		int32		intValue = *((int32 *) binaryValue);

		datum = Int32GetDatum(intValue);
	}
	else if (pgType.postgresTypeOid == INT8OID)
	{
		binaryValue = FromLittleEndian64(binaryValue);
		int64		longValue = *((int64 *) binaryValue);

		datum = Int64GetDatum(longValue);
	}
	else if (pgType.postgresTypeOid == FLOAT4OID)
	{
		binaryValue = FromLittleEndian32(binaryValue);
		float4		floatValue = *((float4 *) binaryValue);

		datum = Float4GetDatum(floatValue);
	}
	else if (pgType.postgresTypeOid == FLOAT8OID)
	{
		binaryValue = FromLittleEndian64(binaryValue);
		float8		doubleValue = *((float8 *) binaryValue);

		datum = Float8GetDatum(doubleValue);
	}
	else if (pgType.postgresTypeOid == DATEOID)
	{
		binaryValue = FromLittleEndian32(binaryValue);

		DateADT		dateValue = *((DateADT *) binaryValue);

		dateValue = AdjustDateFromUnixToPostgres(dateValue);

		datum = DateADTGetDatum(dateValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPOID)
	{
		binaryValue = FromLittleEndian64(binaryValue);

		Timestamp	timestampValue = *((Timestamp *) binaryValue);

		timestampValue = AdjustTimestampFromUnixToPostgres(timestampValue);

		datum = TimestampGetDatum(timestampValue);
	}
	else if (pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		binaryValue = FromLittleEndian64(binaryValue);

		TimestampTz timestampTzValue = *((TimestampTz *) binaryValue);

		timestampTzValue = AdjustTimestampFromUnixToPostgres(timestampTzValue);

		datum = TimestampTzGetDatum(timestampTzValue);
	}
	else if (pgType.postgresTypeOid == TIMEOID)
	{
		binaryValue = FromLittleEndian64(binaryValue);

		TimeADT		timeValue = *((TimeADT *) binaryValue);

		datum = TimeADTGetDatum(timeValue);
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
