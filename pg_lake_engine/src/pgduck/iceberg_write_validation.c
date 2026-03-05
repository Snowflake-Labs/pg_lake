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
 * Iceberg write-time value validation.
 *
 * Provides two mechanisms for enforcing temporal value constraints on
 * data written to Iceberg tables:
 *
 * 1. C-level validation (IcebergErrorOrClampTemporalDatum) used by partition
 *    transform code on the PostgreSQL side.
 *
 * 2. Query wrapping (IcebergWrapQueryWithErrorOrClampChecks) that embeds
 *    CASE WHEN checks into the write query sent to pgduck_server.  Used
 *    by both pushdown and non-pushdown write paths for temporal boundary
 *    enforcement (date/timestamp/timestamptz).
 *
 * NaN rejection for numeric columns is handled separately in the CSV
 * writer (ErrorIfSpecialNumeric in csv_writer.c) before data reaches
 * DuckDB.
 *
 * Temporal boundaries:
 *   - Date: proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 *   - Timestamp/TimestampTZ: 0001-01-01 .. 9999-12-31 23:59:59.999999.
 */
#include "postgres.h"

#include "access/tupdesc.h"
#include "catalog/pg_type.h"
#include "datatype/timestamp.h"
#include "pg_lake/pgduck/iceberg_write_validation.h"
#include "pg_lake/parsetree/options.h"
#include "pg_lake/util/rel_utils.h"
#include "pg_lake/util/table_type.h"
#include "foreign/foreign.h"
#include "pgtime.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"


/* ================================================================
 * Temporal boundary constants (Postgres DateADT / Timestamp values)
 *
 * Date: full proleptic Gregorian range -4712-01-01 .. 9999-12-31.
 * Timestamp/TimestampTZ: common-era range 0001-01-01 .. 9999-12-31.
 * ================================================================ */
#define TEMPORAL_DATE_MIN_YEAR		(-4712)
#define TEMPORAL_TIMESTAMP_MIN_YEAR	1
#define TEMPORAL_MAX_YEAR			9999

/* SQL literal boundaries for the query wrapper */
#define ICEBERG_DATE_MIN_LIT				"DATE '-4712-01-01'"
#define ICEBERG_DATE_MAX_LIT				"DATE '9999-12-31'"
#define ICEBERG_TIMESTAMP_MIN_LIT		"TIMESTAMP '0001-01-01 00:00:00'"
#define ICEBERG_TIMESTAMP_MAX_LIT		"TIMESTAMP '9999-12-31 23:59:59.999999'"
#define ICEBERG_TIMESTAMPTZ_MIN_LIT		"TIMESTAMPTZ '0001-01-01 00:00:00+00'"
#define ICEBERG_TIMESTAMPTZ_MAX_LIT		"TIMESTAMPTZ '9999-12-31 23:59:59.999999+00'"

static bool IsTemporalType(Oid typeOid);
static int	GetYearFromDate(DateADT d);
static int	GetYearFromTimestamp(Timestamp ts);
static Datum ClampOrErrorTemporal(Datum value, Oid typeOid, int year,
								  IcebergOutOfRangePolicy policy);
static DateADT MakeDateFromYMD(int y, int m, int d);
static Timestamp MakeTimestampUsec(int y, int m, int d, int h, int min, int sec, int usec);
static bool TupleDescHasTemporalColumn(TupleDesc tupleDesc);
static void AppendClampExpression(StringInfo buf, const char *quotedName,
								  Oid typeOid);
static void AppendErrorExpression(StringInfo buf, const char *quotedName,
								  Oid typeOid);

/*
 * GetIcebergOutOfRangePolicyFromOptions reads the "out_of_range_values" option
 * from a list of DefElem options (table options or COPY options).
 *
 * Returns ICEBERG_OOR_ERROR if the option is set to "error",
 * ICEBERG_OOR_CLAMP otherwise (including when not present).
 */
IcebergOutOfRangePolicy
GetIcebergOutOfRangePolicyFromOptions(List *options)
{
	char	   *value = GetStringOption(options, "out_of_range_values", false);

	if (value != NULL && strcmp(value, "error") == 0)
		return ICEBERG_OOR_ERROR;

	return ICEBERG_OOR_CLAMP;
}


/*
 * GetIcebergOutOfRangePolicyForTable reads the "out_of_range_values" table
 * option for the given relation.  Returns NONE for non-iceberg tables.
 */
IcebergOutOfRangePolicy
GetIcebergOutOfRangePolicyForTable(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		return ICEBERG_OOR_NONE;

	ForeignTable *foreignTable = GetForeignTable(relationId);

	return GetIcebergOutOfRangePolicyFromOptions(foreignTable->options);
}


/*
 * IsTemporalType returns true for date, timestamp, or timestamptz.
 */
static bool
IsTemporalType(Oid typeOid)
{
	return typeOid == DATEOID ||
		typeOid == TIMESTAMPOID ||
		typeOid == TIMESTAMPTZOID;
}


/* ================================================================
 * C-level temporal validation (used by partition transforms)
 * ================================================================ */

/*
 * GetYearFromDate extracts the year from a PostgreSQL DateADT.
 */
static int
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
static int
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
 * MakeDateFromYMD creates a DateADT from year, month, day.
 */
static DateADT
MakeDateFromYMD(int y, int m, int d)
{
	return date2j(y, m, d) - POSTGRES_EPOCH_JDATE;
}


/*
 * MakeTimestampUsec creates a Timestamp from date/time components
 * including microseconds (no timezone; for TimestampTz the caller casts).
 */
static Timestamp
MakeTimestampUsec(int y, int m, int d, int h, int min, int sec, int usec)
{
	DateADT		date = MakeDateFromYMD(y, m, d);
	Timestamp	result;

	result = (Timestamp) date * USECS_PER_DAY +
		((((h * 60) + min) * 60) + sec) * USECS_PER_SEC + usec;

	return result;
}


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
 * IcebergErrorOrClampTemporalDatum validates a date, timestamp, or timestamptz Datum.
 * This is needed for partition values to be computed correctly in case of clamping.
 */
Datum
IcebergErrorOrClampTemporalDatum(Datum value, Oid typeOid, IcebergOutOfRangePolicy policy)
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


/* ================================================================
 * Query wrapping for temporal boundary checks
 * ================================================================ */

/*
 * TupleDescHasTemporalColumn returns true if any non-dropped column
 * in the tuple descriptor is a temporal type.
 */
static bool
TupleDescHasTemporalColumn(TupleDesc tupleDesc)
{
	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (IsTemporalType(attr->atttypid))
			return true;
	}

	return false;
}


/*
 * AppendClampExpression appends a CASE WHEN expression that clamps
 * the named column to its temporal boundary.
 *
 * For timestamptz we also check the year in the PG session timezone,
 * because a value like '10000-01-01 00:00:00+03' converts to a UTC
 * value in year 9999 and would slip past a pure-UTC boundary check.
 */
static void
AppendClampExpression(StringInfo buf, const char *quotedName, Oid typeOid)
{
	const char *minLit;
	const char *maxLit;

	if (typeOid == DATEOID)
	{
		minLit = ICEBERG_DATE_MIN_LIT;
		maxLit = ICEBERG_DATE_MAX_LIT;
	}
	else if (typeOid == TIMESTAMPTZOID)
	{
		const char *tzName = pg_get_timezone_name(session_timezone);

		appendStringInfo(buf,
						 "CASE "
						 "WHEN %s < %s THEN %s "
						 "WHEN %s > %s THEN %s "
						 "WHEN year(timezone('%s', %s)) > %d "
						 "THEN timezone('%s', TIMESTAMP '9999-12-31 23:59:59.999999') "
						 "WHEN year(timezone('%s', %s)) < %d "
						 "THEN timezone('%s', TIMESTAMP '0001-01-01 00:00:00') "
						 "ELSE %s END",
						 quotedName, ICEBERG_TIMESTAMPTZ_MIN_LIT, ICEBERG_TIMESTAMPTZ_MIN_LIT,
						 quotedName, ICEBERG_TIMESTAMPTZ_MAX_LIT, ICEBERG_TIMESTAMPTZ_MAX_LIT,
						 tzName, quotedName, TEMPORAL_MAX_YEAR,
						 tzName,
						 tzName, quotedName, TEMPORAL_TIMESTAMP_MIN_YEAR,
						 tzName,
						 quotedName);
		return;
	}
	else if (typeOid == TIMESTAMPOID)
	{
		minLit = ICEBERG_TIMESTAMP_MIN_LIT;
		maxLit = ICEBERG_TIMESTAMP_MAX_LIT;
	}
	else
	{
		pg_unreachable();
	}

	appendStringInfo(buf,
					 "CASE WHEN %s < %s THEN %s "
					 "WHEN %s > %s THEN %s "
					 "ELSE %s END",
					 quotedName, minLit, minLit,
					 quotedName, maxLit, maxLit,
					 quotedName);
}


/*
 * AppendErrorExpression appends a CASE WHEN expression that triggers
 * an error (via an invalid CAST) when the column is out of range.
 */
static void
AppendErrorExpression(StringInfo buf, const char *quotedName, Oid typeOid)
{
	const char *minLit;
	const char *maxLit;
	const char *typeName;

	if (typeOid == DATEOID)
	{
		minLit = ICEBERG_DATE_MIN_LIT;
		maxLit = ICEBERG_DATE_MAX_LIT;
		typeName = "DATE";
	}
	else if (typeOid == TIMESTAMPOID)
	{
		minLit = ICEBERG_TIMESTAMP_MIN_LIT;
		maxLit = ICEBERG_TIMESTAMP_MAX_LIT;
		typeName = "TIMESTAMP";
	}
	else if (typeOid == TIMESTAMPTZOID)
	{
		const char *tzName = pg_get_timezone_name(session_timezone);

		appendStringInfo(buf,
						 "CASE WHEN %s NOT BETWEEN %s AND %s "
						 "THEN CAST(printf('timestamp out of range: %%s', %s::VARCHAR) AS TIMESTAMPTZ) "
						 "WHEN year(timezone('%s', %s)) NOT BETWEEN %d AND %d "
						 "THEN CAST(printf('timestamp out of range: %%s', %s::VARCHAR) AS TIMESTAMPTZ) "
						 "ELSE %s END",
						 quotedName, ICEBERG_TIMESTAMPTZ_MIN_LIT, ICEBERG_TIMESTAMPTZ_MAX_LIT,
						 quotedName,
						 tzName, quotedName, TEMPORAL_TIMESTAMP_MIN_YEAR, TEMPORAL_MAX_YEAR,
						 quotedName,
						 quotedName);
		return;
	}
	else
	{
		pg_unreachable();
	}

	const char *errLabel = (typeOid == DATEOID) ? "date" : "timestamp";

	appendStringInfo(buf,
					 "CASE WHEN %s NOT BETWEEN %s AND %s "
					 "THEN CAST(printf('%s out of range: %%s', %s::VARCHAR) AS %s) "
					 "ELSE %s END",
					 quotedName, minLit, maxLit,
					 errLabel, quotedName, typeName,
					 quotedName);
}


/*
 * IcebergErrorOrClampNumericDatum checks a numeric Datum for NaN.
 * Returns true when NaN is detected and the policy is CLAMP (caller
 * should write NULL); raises an error for ERROR policy.  Returns
 * false when the value is not NaN.
 */
Datum
IcebergErrorOrClampNumericDatum(Datum value,
								IcebergOutOfRangePolicy policy,
								bool *isNull)
{
	Numeric		num = DatumGetNumeric(value);

	if (!numeric_is_nan(num))
	{
		*isNull = false;
		return value;
	}

	if (policy == ICEBERG_OOR_CLAMP)
	{
		*isNull = true;
		return (Datum) 0;
	}

	ereport(ERROR,
			errmsg("NaN is not supported for Iceberg decimal"),
			errhint("Use float type instead."));

	pg_unreachable();
}


/*
 * IcebergWrapQueryWithErrorOrClampChecks wraps a query string with an
 * outer SELECT that applies CASE WHEN checks to temporal columns that
 * need Iceberg write-time validation (date/timestamp/timestamptz).
 *
 * NaN rejection for numeric columns is handled earlier in the CSV
 * writer (ErrorIfSpecialNumeric) before the data reaches DuckDB.
 *
 * Returns the original query unchanged if no temporal columns exist
 * or the policy is ICEBERG_OOR_NONE.
 *
 * Example with clamp policy (table: id int, created_at date):
 *
 *   SELECT id,
 *          CASE WHEN created_at < DATE '0001-01-01' THEN DATE '0001-01-01'
 *               WHEN created_at > DATE '9999-12-31' THEN DATE '9999-12-31'
 *               ELSE created_at END AS created_at
 *   FROM (<original_query>) AS __iceberg_oor
 *
 * Example with error policy (same table):
 *
 *   SELECT id,
 *          CASE WHEN created_at NOT BETWEEN DATE '0001-01-01' AND DATE '9999-12-31'
 *               THEN CAST(printf('date out of range: %s', created_at::VARCHAR) AS DATE)
 *               ELSE created_at END AS created_at
 *   FROM (<original_query>) AS __iceberg_oor
 */
char *
IcebergWrapQueryWithErrorOrClampChecks(char *query, TupleDesc tupleDesc,
									   IcebergOutOfRangePolicy policy,
									   bool queryHasRowId)
{
	if (policy == ICEBERG_OOR_NONE || tupleDesc == NULL)
		return query;

	if (!TupleDescHasTemporalColumn(tupleDesc))
		return query;

	StringInfoData wrapped;

	initStringInfo(&wrapped);

	appendStringInfoString(&wrapped, "SELECT ");

	bool		firstColumn = true;

	for (int i = 0; i < tupleDesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, i);

		if (attr->attisdropped)
			continue;

		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");

		const char *quotedName = quote_identifier(NameStr(attr->attname));

		if (IsTemporalType(attr->atttypid))
		{
			if (policy == ICEBERG_OOR_CLAMP)
				AppendClampExpression(&wrapped, quotedName, attr->atttypid);
			else
				AppendErrorExpression(&wrapped, quotedName, attr->atttypid);

			appendStringInfo(&wrapped, " AS %s", quotedName);
		}
		else
		{
			appendStringInfoString(&wrapped, quotedName);
		}

		firstColumn = false;
	}

	if (queryHasRowId)
	{
		if (!firstColumn)
			appendStringInfoString(&wrapped, ", ");
		appendStringInfoString(&wrapped, "_row_id");
	}

	appendStringInfo(&wrapped, " FROM (%s) AS __iceberg_oor", query);

	return wrapped.data;
}
