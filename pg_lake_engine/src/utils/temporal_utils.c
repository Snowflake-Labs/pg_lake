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

#include "catalog/pg_type.h"
#include "pg_lake/util/temporal_utils.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

static const int32 PostgresToUnixEpochDiffInDays = POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE;
static const int64 PostgresToUnixEpochDiffInMicrosecs = ((int64) PostgresToUnixEpochDiffInDays) * USECS_PER_DAY;

static void EnsureFiniteDate(DateADT date);
static void EnsureFiniteTimestamp(Timestamp ts);

#define UNIX_EPOCH_YEAR 1970

/*
 * For negative timestamps the quotient is one‐hour too high.
 * We need this formula to correctly calculate hours from epoch,
 * given a negative timestamp.
 */
#define DIV_FLOOR_INT64(x, y)  \
    ( ((x) >= 0) ? ((x) / (y)) \
                 : (-((-(x) + (y) - 1) / (y))) )


/*
 * GetYearFromDate extracts the year from a PostgreSQL DateADT.
 */
int
GetYearFromDate(DateADT d)
{
	int			y,
				m,
				day;

	j2date(d + POSTGRES_EPOCH_JDATE, &y, &m, &day);
	return y;
}


/*
 * GetYearFromTimestamp extracts the year from a PostgreSQL Timestamp
 * (works for both Timestamp and TimestampTz since they share the
 * same representation).
 */
int
GetYearFromTimestamp(Timestamp ts)
{
	struct pg_tm tt;
	fsec_t		fsec;

	if (timestamp2tm(ts, NULL, &tt, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	return tt.tm_year;
}


/*
 * IcebergTimestampMsToPostgresTimestamp converts an Iceberg timestamp in milliseconds to a Postgres timestamp.
 */
Timestamp
IcebergTimestampMsToPostgresTimestamp(Timestamp icebergTimestampMs)
{
	/* Postgres stores timestamps in microsecs precision */
	Timestamp	icebergTimestampMicros = icebergTimestampMs * 1000;

	/*
	 * Postgres epoch is 2000-01-01, while Iceberg epoch is 1970-01-01 (Unix
	 * epoch)
	 */
	return icebergTimestampMicros - PostgresToUnixEpochDiffInMicrosecs;
}


Timestamp
PostgresTimestampToIcebergTimestampMs(void)
{
	TimestampTz currentTimestamp = GetCurrentTimestamp();
	int64		epochMsecs;

	/* Convert to Unix epoch time */
	epochMsecs = (currentTimestamp - SetEpochTimestamp()) / 1000;

	return epochMsecs;
}


/*
 * AdjustDateFromUnixToPostgres adjusts a date from Unix epoch to Postgres
 * epoch. Postgres epoch is 2000-01-01, while Unix epoch is 1970-01-01.
 */
DateADT
AdjustDateFromUnixToPostgres(DateADT date)
{
	return date - PostgresToUnixEpochDiffInDays;
}


/*
 * AdjustTimestampFromPostgresToUnix adjusts date from Postgres epoch to
 * Unix epoch. Postgres epoch is 2000-01-01, while Unix epoch is 1970-01-01.
 */
DateADT
AdjustDateFromPostgresToUnix(DateADT date)
{
	return date + PostgresToUnixEpochDiffInDays;
}


/*
 * AdjustTimestampFromoUnixToPostgres adjusts timestamp or timestamptz
 * from Unix epoch to Postgres epoch. Postgres epoch is 2000-01-01, while Unix
 * epoch is 1970-01-01.
 */
Timestamp
AdjustTimestampFromUnixToPostgres(Timestamp timestamp)
{
	return timestamp - PostgresToUnixEpochDiffInMicrosecs;
}


/*
 * AdjustTimestampFromPostgresToUnix adjusts timestamp or timestamptz
 * from Postgres epoch to Unix epoch. Postgres epoch is 2000-01-01, while Unix
 * epoch is 1970-01-01.
 */
Timestamp
AdjustTimestampFromPostgresToUnix(Timestamp timestamp)
{
	return timestamp + PostgresToUnixEpochDiffInMicrosecs;
}


/*
 * DateYearFromUnixEpoch returns total years from Unix epoch to the given date.
 * It extracts the year from the date. (there is not a robust constant for
 * DAYS_PER_YEAR since years have different lengths due to leap days) Then, it
 * calculates the total years from Unix epoch to the given date.
 */
int32_t
DateYearFromUnixEpoch(DateADT date)
{
	EnsureFiniteDate(date);

	int32		years = (GetYearFromDate(date) - UNIX_EPOCH_YEAR);

#ifdef USE_ASSERT_CHECKING

	DateADT		thisYear = YearsFromEpochToDate(years);
	DateADT		nextYear = YearsFromEpochToDate(years + 1);

	Assert(date >= thisYear && date < nextYear);
#endif

	return years;
}


/*
 * YearsFromEpochToDate
 *    Given the number of years after the Unix epoch (1970-01-01),
 *    return a DateADT that represents 1 January of that calendar year.
 *
 *    For example:
 *        input  0  ->  1970-01-01
 *        input 52  ->  2022-01-01
 */
DateADT
YearsFromEpochToDate(int32 yearsSinceEpoch)
{
	/* Convert the offset back to an absolute year. */
	int			year = UNIX_EPOCH_YEAR + yearsSinceEpoch;

	/* Use the existing helpers to build a Julian day for 1 January YYYY. */
	int			julian_day = date2j(year, 1, 1);

	/* Translate Julian day to the internal Postgres DateADT representation. */
	return (DateADT) (julian_day - POSTGRES_EPOCH_JDATE);
}


/*
 * Convert the number of months since the Unix epoch (January 1970)
 * to a Postgres DateADT representing the first day of that month.
 */
DateADT
MonthsFromEpochToDate(int32 monthsSinceEpoch)
{
	/* Break the offset into absolute year / month. */
	int32		yearsOffset = monthsSinceEpoch / MONTHS_PER_YEAR;
	int32		monthOffset = monthsSinceEpoch % MONTHS_PER_YEAR;
	int			year = UNIX_EPOCH_YEAR + yearsOffset;

	/* Handle negative remainders so that monthOffset is 0-based (0–11). */
	if (monthOffset < 0)
	{
		monthOffset += MONTHS_PER_YEAR;
		year--;
	}

	/* Convert 0-based to 1-based month for date2j(). */
	int			julian_day = date2j(year, monthOffset + 1, 1);

	return (DateADT) (julian_day - POSTGRES_EPOCH_JDATE);
}


/*
 * Convert the number of days since the Unix epoch (January 1 1970)
 * to a Postgres DateADT representing that day.
 */
DateADT
DaysFromEpochToDate(int32 daysSinceEpoch)
{
	int			julian_day = UNIX_EPOCH_JDATE + daysSinceEpoch;

	return (DateADT) (julian_day - POSTGRES_EPOCH_JDATE);
}

/*
 * DaysFromUnixEpochToTimestamp converts an offset in days counted from the
 * Unix epoch (1970-01-01) back to a Postgres Timestamp.  The result is the
 * start of that day (00:00:00, microsecond precision).
 */
Timestamp
DaysFromUnixEpochToTimestamp(int32 daysSinceEpoch)
{
	int			julian_day = UNIX_EPOCH_JDATE + daysSinceEpoch;
	int32		days = julian_day - POSTGRES_EPOCH_JDATE;

	return (Timestamp) days * USECS_PER_DAY;	/* midnight on that day */
}


/*
* YearsFromEpochToTimestamp
 *    Given the number of years after the Unix epoch (1970-01-01),
 *    return a Timestamp that represents 1 January of that calendar year.
 *
 *    For example:
 *        input  0  ->  1970-01-01 00:00:00
 *        input 52  ->  2022-01-01 00:00:00
*/
Timestamp
YearsFromEpochToTimestamp(int32 yearsSinceEpoch)
{
	/* Convert the offset back to an absolute year. */
	int			year = UNIX_EPOCH_YEAR + yearsSinceEpoch;

	/* Julian day for 1 January YYYY. */
	int			julian_day = date2j(year, 1, 1);

	/* Days since PostgreSQL epoch.  Use int64 so the multiply is safe. */
	int64		days_from_epoch = (int64) (julian_day - POSTGRES_EPOCH_JDATE);

	/* Midnight at that date, in microseconds since PostgreSQL epoch. */
	return (Timestamp) (days_from_epoch * USECS_PER_DAY);
}


/*
 * DateMonthFromUnixEpoch returns total months from Unix epoch to the given
 * date. It extracts the year and the month from the date.
 * (there is not a robust constant for DAYS_PER_MONTH; since months have different
 * lengths) Then, it calculates the total months from Unix epoch to the given
 * date.
 */
int32_t
DateMonthFromUnixEpoch(DateADT date)
{
	EnsureFiniteDate(date);

	int			year;
	int			month;
	int			day;

	j2date(date + POSTGRES_EPOCH_JDATE, &year, &month, &day);
	int32		months = (year - UNIX_EPOCH_YEAR) * 12 + month - 1;

#ifdef USE_ASSERT_CHECKING
	DateADT		thisMonth = MonthsFromEpochToDate(months);
	DateADT		nextMonth = MonthsFromEpochToDate(months + 1);

	/* date must be ≥ first-of-month and < first-of-next-month */
	Assert(date >= thisMonth && date < nextMonth);
#endif

	return months;
}


/*
 * DateDayFromUnixEpoch returns total days from Unix epoch to the given
 * date.
 */
int32_t
DateDayFromUnixEpoch(DateADT date)
{
	EnsureFiniteDate(date);

	return (int32_t) AdjustDateFromPostgresToUnix(date);
}


/*
 * TimestampYearFromUnixEpoch returns total years from Unix epoch to the given
 * timestamp. It extracts the year from the timestamp.
 * (there is not a robust constant for USECS_PER_YEAR since each year have different
 * lengths, due to leap days) Then, it calculates the total years from Unix epoch
 * to the given timestamp.
 */
int32_t
TimestampYearFromUnixEpoch(Timestamp ts)
{
	EnsureFiniteTimestamp(ts);

	return GetYearFromTimestamp(ts) - UNIX_EPOCH_YEAR;
}


/*
 * TimestampMonthFromUnixEpoch returns total months from Unix epoch to the
 * given timestamp. It extracts the year and the month from the timestamp.
 * (there is not a robust constant for USECS_PER_MONTH since months have different
 * lengths) Then, it calculates the total months from Unix epoch to the given
 * timestamp.
 */
int32_t
TimestampMonthFromUnixEpoch(Timestamp ts)
{
	EnsureFiniteTimestamp(ts);

	struct pg_tm tm;
	fsec_t		fsec;

	if (timestamp2tm(ts, NULL, &tm, &fsec, NULL, NULL) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	int			months = (tm.tm_year - UNIX_EPOCH_YEAR) * 12 + (tm.tm_mon - 1);

#ifdef USE_ASSERT_CHECKING
	Timestamp	thisMonth = MonthsFromUnixEpochToTimestamp(months);
	Timestamp	nextMonth = MonthsFromUnixEpochToTimestamp(months + 1);

	/* timestamp must be ≥ first-of-month and < first-of-next-month */
	Assert(ts >= thisMonth && ts < nextMonth);
#endif

	return months;
}

/*
 * MonthsFromUnixEpochToTimestamp converts an offset in months counted from the
 * Unix epoch (1970-01) back to a Postgres Timestamp.  The result is the first
 * day of the target month at 00:00:00-00 (microsecond precision).
 */
Timestamp
MonthsFromUnixEpochToTimestamp(int32 monthsSinceEpoch)
{
	/* Split the offset into absolute year/month, handling negatives safely. */
	int32		yearsOffset = monthsSinceEpoch / 12;
	int32		monthOffset = monthsSinceEpoch % 12;
	int			year = UNIX_EPOCH_YEAR + yearsOffset;

	if (monthOffset < 0)
	{
		monthOffset += 12;
		year--;
	}

	/* Convert to a Julian day (1-based month → monthOffset + 1). */
	int			julian_day = date2j(year, monthOffset + 1, 1);

	/* Days between Postgres epoch (2000-01-01) and our target date. */
	int32		days = julian_day - POSTGRES_EPOCH_JDATE;

	/* Return microseconds since Postgres epoch. */
	return (Timestamp) days * USECS_PER_DAY;	/* midnight on that day */
}



/*
 * TimestampDayFromUnixEpoch returns total days from Unix epoch to the given
 * timestamp.
 */
int32_t
TimestampDayFromUnixEpoch(Timestamp ts)
{
	EnsureFiniteTimestamp(ts);

	Timestamp	unixTs = AdjustTimestampFromPostgresToUnix(ts);

	/*
	 * the floor division to get the number of days correctly for negative
	 * timestamps
	 */
	return (int32_t) DIV_FLOOR_INT64(unixTs, USECS_PER_DAY);
}


/*
 * TimestampHourFromUnixEpoch returns total hours from Unix epoch to the
 * given timestamp.
 */
int32_t
TimestampHourFromUnixEpoch(Timestamp ts)
{
	EnsureFiniteTimestamp(ts);

	Timestamp	unixTs = AdjustTimestampFromPostgresToUnix(ts);

	/*
	 * the floor division to get the number of hours correctly for negative
	 * timestamps
	 */
	return (int32_t) DIV_FLOOR_INT64(unixTs, USECS_PER_HOUR);
}

/*
 * HoursFromUnixEpochToTimestamp converts an offset in hours counted from the
 * Unix epoch (1970-01-01 00:00) back to a Postgres Timestamp representing
 * the start of that hour.
 */
Timestamp
HoursFromUnixEpochToTimestamp(int32 hoursSinceEpoch)
{
	Timestamp	unixTs = (Timestamp) ((int64) hoursSinceEpoch * USECS_PER_HOUR);

	return AdjustTimestampFromUnixToPostgres(unixTs);
}


/*
 * TimeHourFromUnixEpoch returns total hours from Unix epoch to the given
 * time.
 */
int32_t
TimeHourFromUnixEpoch(TimeADT time)
{
	/*
	 * the floor division to get the number of hours correctly for negative
	 * times
	 */
	return (int32_t) DIV_FLOOR_INT64(time, USECS_PER_HOUR);
}


/*
 * HoursFromUnixEpochToTime converts an offset in hours counted from the
 * Unix epoch (00:00) back to a Postgres TimeADT representing
 * the start of that hour.
 */
TimeADT
HoursFromUnixEpochToTime(int32 hoursSinceEpoch)
{
	return (TimeADT) ((int64) hoursSinceEpoch * USECS_PER_HOUR);
}


/*
 * MakeDateFromYMD creates a DateADT from year, month, day.
 */
DateADT
MakeDateFromYMD(int y, int m, int d)
{
	return date2j(y, m, d) - POSTGRES_EPOCH_JDATE;
}


/*
 * MakeTimestampUsec creates a Timestamp from date/time components
 * including microseconds (no timezone; for TimestampTz the caller casts).
 */
Timestamp
MakeTimestampUsec(int y, int m, int d, int h, int min, int sec, int usec)
{
	DateADT		date = MakeDateFromYMD(y, m, d);
	Timestamp	result;

	result = (Timestamp) date * USECS_PER_DAY +
		((((h * 60) + min) * 60) + sec) * USECS_PER_SEC + usec;

	return result;
}


static void
EnsureFiniteDate(DateADT date)
{
	if (DATE_NOT_FINITE(date))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("infinite date is not allowed in Iceberg tables")));
}


static void
EnsureFiniteTimestamp(Timestamp ts)
{
	if (TIMESTAMP_NOT_FINITE(ts))
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("infinite timestamp is not allowed in Iceberg tables")));
}
