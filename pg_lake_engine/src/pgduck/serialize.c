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
 * Serialize a Datum in a PGDuck-compatible way; a central hook for
 * special-purpose conversion of PG datatypes in preparation for PGDuck.
 *
 * By default, we call the supplied OutputFunction as provided, however we want
 * the ability to override this serialization on a type-by-type basis, in
 * particular for arrays and records, which get special handling to convert to
 * DuckDB-compatible text format.
 */
char *
PGDuckSerialize(FmgrInfo *flinfo, Oid columnType, Datum value,
				CopyDataFormat format, HTAB *tupdesc_cache)
{
	if (flinfo->fn_oid == ARRAY_OUT_OID)
	{
		/* maps are a type of array */
		if (IsMapTypeOid(columnType))
			return MapOutForPGDuck(value, format);

		return ArrayOutForPGDuck(DatumGetArrayTypeP(value), format, tupdesc_cache);
	}

	if (flinfo->fn_oid == RECORD_OUT_OID)
		return StructOutForPGDuck(value, format, tupdesc_cache);

	/*
	 * For Iceberg, intervals are serialized as struct(months, days,
	 * microseconds). We need to to this during writing to csv because duckdb
	 * interval formatting is different from postgres interval formatting.
	 */
	if (columnType == INTERVALOID && format == DATA_FORMAT_ICEBERG)
		return IntervalOutForPGDuck(value);

	if (columnType == BYTEAOID)
		return ByteAOutForPGDuck(value);

	/*
	 * TimeTZ is stored as TIME (UTC-normalized) in Iceberg. We convert to UTC
	 * in PGDuckSerialize, so DuckDB should parse as TIME.
	 */
	if (columnType == TIMETZOID && format == DATA_FORMAT_ICEBERG)
		return TimeTzOutForPGDuck(value);

	if (IsGeometryOutFunctionId(flinfo->fn_oid))
	{
		/*
		 * Postgis emits HEX WKB by default, which DuckDB does not accept.
		 * Hence, we emit as WKT.
		 */
		Datum		geomAsText = OidFunctionCall1(ST_AsTextFunctionId(), value);

		return TextDatumGetCString(geomAsText);
	}

	/*
	 * PostgreSQL outputs BC dates/timestamps with a " BC" suffix (e.g.
	 * "4712-01-01 BC"), but DuckDB expects ISO 8601 negative-year format
	 * (e.g. "-4711-01-01").  Without this conversion, DuckDB silently drops
	 * the BC era indicator and treats the value as AD.
	 */
	if (columnType == DATEOID || columnType == TIMESTAMPOID ||
		columnType == TIMESTAMPTZOID)
	{
		char	   *result = OutputFunctionCall(flinfo, value);

		return (char *) ConvertBCToISOYearIfNeeded(result);
	}

	return OutputFunctionCall(flinfo, value);
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
