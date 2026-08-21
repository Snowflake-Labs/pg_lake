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
#include "catalog/pg_type_d.h"
#include "common/string.h"
#include "pg_lake/extensions/postgis.h"
#include "pg_lake/pgduck/array_conversion.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/pgduck/map_conversion.h"
#include "pg_lake/pgduck/serialize.h"
#include "pg_lake/pgduck/struct_conversion.h"
#include "pg_lake/util/timetz.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"


static char *ByteAOutForPGDuck(Datum value);
static char *TimeTzOutForPGDuck(Datum value);


/*
 * ConvertBCToISOYearIfNeeded converts PostgreSQL's BC-era date/timestamp
 * string to ISO 8601 year numbering.
 *
 * If the input ends with " BC", the 1-based BC year is converted:
 *   1 BC → 0000, 2 BC → -0001, 4712 BC → -4711
 *
 * The rest of the string (month-day, time, timezone) is preserved:
 *   "4712-01-01 BC"            → "-4711-01-01"
 *   "4712-01-01 00:00:00 BC"   → "-4711-01-01 00:00:00"
 *   "0001-01-01 00:00:00+00 BC" → "0000-01-01 00:00:00+00"
 *
 * AD strings (no " BC" suffix) are returned unchanged.
 */
const char *
ConvertBCToISOYearIfNeeded(const char *dateTimestampString)
{
	if (!pg_str_endswith(dateTimestampString, " BC"))
		return dateTimestampString;

	char	   *endptr;
	int			pgBcYear = strtoint(dateTimestampString, &endptr, 10);

	if (endptr == dateTimestampString || pgBcYear <= 0 || *endptr != '-')
		return dateTimestampString;

	/*
	 * Convert: 1 BC (pgBcYear=1) → ISO year 0, 2 BC → -1, 4712 BC →
	 * -4711
	 */
	int			isoYear = -(pgBcYear - 1);

	/*
	 * copy the part after the year. endptr points to "-MM-DD...", strip
	 * trailing " BC"
	 */
	char	   *dateTimeRemainder = pstrdup(endptr);

	/* strip the " BC" */
	dateTimeRemainder[strlen(dateTimeRemainder) - 3] = '\0';

	const char *result;

	if (isoYear == 0)
		result = psprintf("%04d%s", isoYear, dateTimeRemainder);
	else
		result = psprintf("-%04d%s", -isoYear, dateTimeRemainder);

	pfree(dateTimeRemainder);
	return result;
}


/*
 * ConvertISOYearToBCIfNeeded converts an ISO 8601 zero/negative year
 * date/timestamp string to PostgreSQL's "YYYY BC" format.
 *
 *   "-4711-01-01"            → "4712-01-01 BC"
 *   "0000-01-01T00:00:00"    → "0001-01-01T00:00:00 BC"
 *   "0000-01-01 00:00:00+00" → "0001-01-01 00:00:00+00 BC"
 *
 * Positive-year strings pass through unchanged.
 */
const char *
ConvertISOYearToBCIfNeeded(const char *dateTimestampString)
{
	char	   *endptr;
	int			isoYear = strtoint(dateTimestampString, &endptr, 10);

	if (endptr == dateTimestampString || isoYear > 0)
		return dateTimestampString;

	/* ISO year 0 = 1 BC, -1 = 2 BC, etc. */
	int			bcYear = 1 - isoYear;

	return psprintf("%04d%s BC", bcYear, endptr);
}


/*
 * GetPGDuckSerializeKind determines how a value of the given type is
 * serialized for PGDuck, based on the type, its output function and the target
 * format.
 *
 * The answer is the same for every value of a column, but deriving it costs
 * several syscache lookups (domain resolution, and the map check on top of
 * that for arrays), so callers that serialize many values of the same type
 * should resolve the kind once and pass it to PGDuckSerializeWithKind.
 */
PGDuckSerializeKind
GetPGDuckSerializeKind(FmgrInfo *flinfo, Oid columnType, CopyDataFormat format)
{
	/*
	 * Unwrap domain types so that e.g. a domain-over-bytea is serialized with
	 * the bytea-specific path rather than the generic text output path.  The
	 * serialization here does not depend on the type modifier, so it is
	 * discarded.
	 */
	int32		columnTypeMod = -1;

	columnType = ResolveDomainBaseTypeAndTypmod(columnType, &columnTypeMod);

	if (flinfo->fn_oid == ARRAY_OUT_OID)
	{
		/* maps are a type of array */
		if (IsMapTypeOid(columnType))
			return PGDUCK_SERIALIZE_MAP;

		return PGDUCK_SERIALIZE_ARRAY;
	}

	if (flinfo->fn_oid == RECORD_OUT_OID)
		return PGDUCK_SERIALIZE_STRUCT;

	/*
	 * For Iceberg, intervals are serialized as struct(months, days,
	 * microseconds). We need to to this during writing to csv because duckdb
	 * interval formatting is different from postgres interval formatting.
	 */
	if (columnType == INTERVALOID && format == DATA_FORMAT_ICEBERG)
		return PGDUCK_SERIALIZE_INTERVAL;

	if (columnType == BYTEAOID)
		return PGDUCK_SERIALIZE_BYTEA;

	/*
	 * TimeTZ is stored as TIME (UTC-normalized) in Iceberg. We convert to UTC
	 * in PGDuckSerialize, so DuckDB should parse as TIME.
	 */
	if (columnType == TIMETZOID && format == DATA_FORMAT_ICEBERG)
		return PGDUCK_SERIALIZE_TIMETZ;

	if (IsGeometryOutFunctionId(flinfo->fn_oid))
		return PGDUCK_SERIALIZE_GEOMETRY;

	if (columnType == DATEOID || columnType == TIMESTAMPOID ||
		columnType == TIMESTAMPTZOID)
		return PGDUCK_SERIALIZE_DATE_TIMESTAMP;

	return PGDUCK_SERIALIZE_OUTPUT_FUNCTION;
}


/*
 * PGDuckSerializeWithKind serializes a Datum in a PGDuck-compatible way using
 * a serialization kind that the caller already resolved via
 * GetPGDuckSerializeKind.
 *
 * The kind must have been resolved for the type of the value being passed in,
 * with the same target format.
 */
char *
PGDuckSerializeWithKind(FmgrInfo *flinfo, PGDuckSerializeKind serializeKind,
						Datum value, CopyDataFormat format)
{
	switch (serializeKind)
	{
		case PGDUCK_SERIALIZE_MAP:
			return MapOutForPGDuck(value, format);

		case PGDUCK_SERIALIZE_ARRAY:
			return ArrayOutForPGDuck(DatumGetArrayTypeP(value), format);

		case PGDUCK_SERIALIZE_STRUCT:
			return StructOutForPGDuck(value, format);

		case PGDUCK_SERIALIZE_INTERVAL:
			return IntervalOutForPGDuck(value);

		case PGDUCK_SERIALIZE_BYTEA:
			return ByteAOutForPGDuck(value);

		case PGDUCK_SERIALIZE_TIMETZ:
			return TimeTzOutForPGDuck(value);

		case PGDUCK_SERIALIZE_GEOMETRY:
			{
				/*
				 * Postgis emits HEX WKB by default, which DuckDB does not
				 * accept. Hence, we emit as WKT.
				 */
				Datum		geomAsText = OidFunctionCall1(ST_AsTextFunctionId(), value);

				return TextDatumGetCString(geomAsText);
			}

		case PGDUCK_SERIALIZE_DATE_TIMESTAMP:
			{
				/*
				 * PostgreSQL outputs BC dates/timestamps with a " BC" suffix
				 * (e.g. "4712-01-01 BC"), but DuckDB expects ISO 8601
				 * negative-year format (e.g. "-4711-01-01").  Without this
				 * conversion, DuckDB silently drops the BC era indicator and
				 * treats the value as AD.
				 */
				char	   *result = OutputFunctionCall(flinfo, value);

				return (char *) ConvertBCToISOYearIfNeeded(result);
			}

		case PGDUCK_SERIALIZE_OUTPUT_FUNCTION:
			return OutputFunctionCall(flinfo, value);
	}

	/* keep the compiler happy; all kinds are handled above */
	return OutputFunctionCall(flinfo, value);
}


/*
 * Serialize a Datum in a PGDuck-compatible way; a central hook for
 * special-purpose conversion of PG datatypes in preparation for PGDuck.
 *
 * By default, we call the supplied OutputFunction as provided, however we want
 * the ability to override this serialization on a type-by-type basis, in
 * particular for arrays and records, which get special handling to convert to
 * DuckDB-compatible text format.
 *
 * Callers in a per-value loop should hoist the GetPGDuckSerializeKind call out
 * of the loop and call PGDuckSerializeWithKind directly.
 */
char *
PGDuckSerialize(FmgrInfo *flinfo, Oid columnType, Datum value,
				CopyDataFormat format)
{
	PGDuckSerializeKind serializeKind =
		GetPGDuckSerializeKind(flinfo, columnType, format);

	return PGDuckSerializeWithKind(flinfo, serializeKind, value, format);
}


/*
 * IsPGDuckSerializeRequired returns whether the given type requires PGDuckSerialize
 * if passed to DuckDB.
 */
bool
IsPGDuckSerializeRequired(PGType postgresType)
{
	Oid			typeId = postgresType.postgresTypeOid;

	if (typeId == BYTEAOID)
		return true;

	if (typeId == TIMETZOID)
		return true;

	/*
	 * PostgreSQL outputs BC dates/timestamps with " BC" suffix, but DuckDB
	 * expects ISO 8601 negative-year format.  We route these types through
	 * PGDuckSerialize so that ConvertBCToISOYearIfNeeded can convert.
	 */
	if (typeId == DATEOID || typeId == TIMESTAMPOID || typeId == TIMESTAMPTZOID)
		return true;

	/* also covers map */
	if (type_is_array(typeId))
		return true;

	if (get_typtype(typeId) == TYPTYPE_COMPOSITE)
		return true;

	if (IsGeometryType(postgresType))
		return true;

	return false;
}

char *
ByteAOutForPGDuck(Datum value)
{
	bytea	   *bytes = DatumGetByteaP(value);

	Size		arrayLength = VARSIZE_ANY_EXHDR(bytes);

	/* output is 4x number of input bytes plus terminator */
	char	   *outputBuffer = palloc(arrayLength * 4 + 1);
	char	   *currentPointer = outputBuffer;

	char	   *hexLookup = "0123456789abcdef";

	for (Size byteIndex = 0; byteIndex < arrayLength; byteIndex++)
	{
		char		currentByte = bytes->vl_dat[byteIndex];

		*currentPointer++ = '\\';
		*currentPointer++ = 'x';
		*currentPointer++ = hexLookup[(currentByte >> 4) & 0xF];	/* high nibble */
		*currentPointer++ = hexLookup[currentByte & 0xF];	/* low nibble */
	}

	*currentPointer++ = '\0';

	return outputBuffer;
}


/*
 * IntervalOutForPGDuck serializes a PostgreSQL interval as a DuckDB struct:
 * {'months': M, 'days': D, 'microseconds': U}
 */
char *
IntervalOutForPGDuck(Datum value)
{
	Interval   *interval = DatumGetIntervalP(value);

#if PG_VERSION_NUM >= 170000
	if (INTERVAL_NOT_FINITE(interval))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("+-Infinity intervals are not allowed in iceberg tables"),
				 errhint("Delete or replace +-Infinity values.")));
#endif

	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf,
					 "{'months': %d, 'days': %d, 'microseconds': " INT64_FORMAT "}",
					 interval->month,
					 interval->day,
					 interval->time);

	return buf.data;
}


/*
 * TimeTzOutForPGDuck converts a timetz datum to a UTC time string
 * for DuckDB. Since Iceberg only supports time (no timezone), we
 * normalize to UTC and format via PG's time_out.
 */
static char *
TimeTzOutForPGDuck(Datum value)
{
	TimeTzADT  *timetz = DatumGetTimeTzADTP(value);
	TimeADT		utcMicros = TimeTzGetUTCMicros(timetz);

	/* format via PG's time output function (produces "HH:MM:SS[.ffffff]") */
	Oid			timeOutputFunc;
	bool		typIsVarlena;

	getTypeOutputInfo(TIMEOID, &timeOutputFunc, &typIsVarlena);

	return OidOutputFunctionCall(timeOutputFunc, TimeADTGetDatum(utcMicros));
}


/* Helper to see if we are a "container" type oid */
bool
IsContainerType(Oid typeId)
{
	/* also covers map */
	if (type_is_array(typeId))
		return true;

	if (get_typtype(typeId) == TYPTYPE_COMPOSITE)
		return true;

	return false;
}
