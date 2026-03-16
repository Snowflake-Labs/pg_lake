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
 * C-level Iceberg write-time datum validation.
 *
 * Validates individual Datum values against Iceberg representable ranges
 * on the PostgreSQL side (non-pushdown path).  Called from partition
 * transform code (to keep partition keys consistent with clamped data)
 * and from the CSV writer (for the data itself).
 *
 * Handles both temporal boundaries (date/timestamp/timestamptz) and
 * numeric NaN (clamped to NULL or rejected).
 *
 * Temporal boundaries:
 *   - Date: proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 *   - Timestamp/TimestampTZ: 0001-01-01 .. 9999-12-31 23:59:59.999999.
 */
#include "postgres.h"

#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "pg_lake/pgduck/iceberg_datum_validation.h"
#include "pg_lake/util/temporal_utils.h"
#include "pgtime.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"


static Datum ClampOrErrorTemporal(Datum value, Oid typeOid, int year,
								  IcebergOutOfRangePolicy policy);
static Datum IcebergErrorOrClampTemporalDatum(Datum value, Oid typeOid,
											  IcebergOutOfRangePolicy policy);
static Datum IcebergErrorOrClampNumericDatum(Datum value,
											 IcebergOutOfRangePolicy policy,
											 bool *isNull);


/*
 * ClampOrErrorTemporal handles an out-of-range temporal value.
 *
 * In error mode: raises an error.
 * In clamp mode: returns the nearest boundary value.
 */
static Datum
ClampOrErrorTemporal(Datum value, Oid typeOid, int year,
					 IcebergOutOfRangePolicy policy)
{
	if (policy == ICEBERG_OOR_ERROR)
	{
		const char *errMsg = (typeOid == DATEOID) ?
			"date out of range" :
			"timestamp out of range";

		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("%s", errMsg)));
	}

	/*
	 * Clamp mode: determine if value is below or above range.
	 *
	 * For infinity values, NOBEGIN = -infinity -> clamp to min, NOEND =
	 * +infinity -> clamp to max.
	 *
	 * For finite values, use the extracted year to decide direction.
	 */
	bool		clampToMin;

	if (typeOid == DATEOID)
	{
		DateADT		d = DatumGetDateADT(value);

		if (DATE_NOT_FINITE(d))
			clampToMin = DATE_IS_NOBEGIN(d);
		else
			clampToMin = (year < TEMPORAL_DATE_MIN_YEAR);

		if (clampToMin)
			return DateADTGetDatum(MakeDateFromYMD(TEMPORAL_DATE_MIN_YEAR, 1, 1));
		else
			return DateADTGetDatum(MakeDateFromYMD(TEMPORAL_MAX_YEAR, 12, 31));
	}
	else if (typeOid == TIMESTAMPOID)
	{
		Timestamp	ts = DatumGetTimestamp(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			clampToMin = TIMESTAMP_IS_NOBEGIN(ts);
		else
			clampToMin = (year < TEMPORAL_TIMESTAMP_MIN_YEAR);

		if (clampToMin)
			return TimestampGetDatum(
									 MakeTimestampUsec(TEMPORAL_TIMESTAMP_MIN_YEAR, 1, 1, 0, 0, 0, 0));
		else
			return TimestampGetDatum(
									 MakeTimestampUsec(TEMPORAL_MAX_YEAR, 12, 31, 23, 59, 59, 999999));
	}
	else
	{
		/* TIMESTAMPTZOID: clamp to boundaries in the session timezone */
		TimestampTz ts = DatumGetTimestampTz(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			clampToMin = TIMESTAMP_IS_NOBEGIN(ts);
		else
			clampToMin = (year < TEMPORAL_TIMESTAMP_MIN_YEAR);

		int			clampYear = clampToMin ? TEMPORAL_TIMESTAMP_MIN_YEAR : TEMPORAL_MAX_YEAR;
		int			clampMon = clampToMin ? 1 : 12;
		int			clampDay = clampToMin ? 1 : 31;
		int			clampHour = clampToMin ? 0 : 23;
		int			clampMin = clampToMin ? 0 : 59;
		int			clampSec = clampToMin ? 0 : 59;
		int			clampUsec = clampToMin ? 0 : 999999;

		struct pg_tm clampTm = {0};

		clampTm.tm_year = clampYear;
		clampTm.tm_mon = clampMon;
		clampTm.tm_mday = clampDay;
		clampTm.tm_hour = clampHour;
		clampTm.tm_min = clampMin;
		clampTm.tm_sec = clampSec;

		int			tzOffset = DetermineTimeZoneOffset(&clampTm, session_timezone);
		Timestamp	localTs = MakeTimestampUsec(clampYear, clampMon, clampDay,
												clampHour, clampMin, clampSec, clampUsec);

		return TimestampTzGetDatum(localTs + (int64) tzOffset * USECS_PER_SEC);
	}
}


/*
 * IcebergErrorOrClampTemporalDatum validates a date, timestamp, or
 * timestamptz Datum against Iceberg temporal boundaries.
 */
static Datum
IcebergErrorOrClampTemporalDatum(Datum value, Oid typeOid,
								 IcebergOutOfRangePolicy policy)
{
	Assert(IsTemporalType(typeOid));

	if (typeOid == DATEOID)
	{
		DateADT		d = DatumGetDateADT(value);

		if (DATE_NOT_FINITE(d))
			return ClampOrErrorTemporal(value, typeOid, 0, policy);

		int			year = GetYearFromDate(d);

		if (year < TEMPORAL_DATE_MIN_YEAR || year > TEMPORAL_MAX_YEAR)
			return ClampOrErrorTemporal(value, typeOid, year, policy);
	}
	else
	{
		Timestamp	ts = (typeOid == TIMESTAMPTZOID) ?
			DatumGetTimestampTz(value) :
			DatumGetTimestamp(value);

		if (TIMESTAMP_NOT_FINITE(ts))
			return ClampOrErrorTemporal(value, typeOid, 0, policy);

		int			year = GetYearFromTimestamp(ts);

		if (year < TEMPORAL_TIMESTAMP_MIN_YEAR || year > TEMPORAL_MAX_YEAR)
			return ClampOrErrorTemporal(value, typeOid, year, policy);

		/*
		 * For timestamptz, also check the year in the session timezone. A
		 * value like '10000-01-01 00:00:00+03' has UTC year 9999 but local
		 * year 10000.
		 */
		if (typeOid == TIMESTAMPTZOID)
		{
			struct pg_tm tt;
			fsec_t		fsec;
			int			tz;

			if (timestamp2tm(ts, &tz, &tt, &fsec, NULL, NULL) == 0)
			{
				int			localYear = tt.tm_year;

				if (localYear < TEMPORAL_TIMESTAMP_MIN_YEAR ||
					localYear > TEMPORAL_MAX_YEAR)
					return ClampOrErrorTemporal(value, typeOid,
												localYear, policy);
			}
		}
	}

	return value;
}


/*
 * IcebergErrorOrClampNumericDatum validates a numeric Datum for NaN.
 *
 * In clamp mode: sets *isNull to true and returns 0 (caller writes NULL).
 * In error mode: raises an error.
 * For non-NaN values the datum is returned unchanged.
 */
static Datum
IcebergErrorOrClampNumericDatum(Datum value, IcebergOutOfRangePolicy policy,
								bool *isNull)
{
	if (!numeric_is_nan(DatumGetNumeric(value)))
		return value;

	if (policy == ICEBERG_OOR_CLAMP)
	{
		*isNull = true;
		return (Datum) 0;
	}

	Assert(policy == ICEBERG_OOR_ERROR);
	ereport(ERROR,
			errmsg("NaN is not supported for Iceberg decimal"),
			errhint("Use float type instead."));
}


/*
 * IcebergErrorOrClampDatum validates a Datum for Iceberg write constraints.
 *
 * Dispatches to temporal validation (date/timestamp/timestamptz) or
 * numeric NaN rejection based on typeOid.  For types that need no
 * validation the value is returned unchanged.
 *
 * *isNull is set to true only when a numeric NaN is clamped (the
 * caller should write NULL instead of the original value).
 */
Datum
IcebergErrorOrClampDatum(Datum value, Oid typeOid,
						 IcebergOutOfRangePolicy policy, bool *isNull)
{
	*isNull = false;

	if (IsTemporalType(typeOid))
		return IcebergErrorOrClampTemporalDatum(value, typeOid, policy);

	if (typeOid == NUMERICOID)
		return IcebergErrorOrClampNumericDatum(value, policy, isNull);

	return value;
}
