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
 * cron_schedule.c
 *
 * Parsing of 5-field cron expressions and computation of the next time one
 * matches. The accepted syntax is the usual Vixie cron vocabulary:
 *
 *   minute hour day-of-month month day-of-week
 *
 * where each field is a comma-separated list of '*', a single value, a
 * range 'N-M', or either of those with a '/S' step. Month and day-of-week
 * also accept three-letter names, and the whole expression may instead be
 * one of the @hourly / @daily / @weekly / @monthly / @yearly macros.
 */
#include "postgres.h"

#include <ctype.h>

#include "fmgr.h"
#include "miscadmin.h"
#include "pgtime.h"

#include "utils/builtins.h"
#include "utils/datetime.h"
#include "utils/timestamp.h"

#include "pg_job_scheduler/cron_schedule.h"

#define CRON_FIELD_COUNT 5

/*
 * How far ahead to look before declaring that an expression never matches.
 * The worst legitimate case is "0 0 29 2 *" starting just after a leap day
 * that precedes a skipped century leap year: 2096 to 2104 is eight years.
 */
#define MAX_CRON_SEARCH_DAYS (366 * 9)

/* describes one of the five fields */
typedef struct CronFieldSpec
{
	const char *fieldName;
	int			minValue;
	int			maxValue;

	/* three-letter names accepted by this field, or NULL for none */
	const char *const *valueNames;

	/* the value that valueNames[0] stands for */
	int			firstNamedValue;
}			CronFieldSpec;

static const char *const monthNames[] = {
	"jan", "feb", "mar", "apr", "may", "jun",
	"jul", "aug", "sep", "oct", "nov", "dec", NULL
};

static const char *const dayOfWeekNames[] = {
	"sun", "mon", "tue", "wed", "thu", "fri", "sat", NULL
};

/*
 * The day-of-week field accepts 7 as a second spelling of Sunday, so its
 * maximum is 7 rather than 6; ParseCronField folds bit 7 into bit 0.
 */
static const CronFieldSpec cronFieldSpecs[CRON_FIELD_COUNT] = {
	{"minute", 0, 59, NULL, 0},
	{"hour", 0, 23, NULL, 0},
	{"day of month", 1, 31, NULL, 0},
	{"month", 1, 12, monthNames, 1},
	{"day of week", 0, 7, dayOfWeekNames, 0},
};

/* whole-expression shorthands */
static const struct
{
	const char *macro;
	const char *expansion;
}			cronMacros[] = {
	{"@yearly", "0 0 1 1 *"},
	{"@annually", "0 0 1 1 *"},
	{"@monthly", "0 0 1 * *"},
	{"@weekly", "0 0 * * 0"},
	{"@daily", "0 0 * * *"},
	{"@midnight", "0 0 * * *"},
	{"@hourly", "0 * * * *"},
};


/*
 * IsCronSpace reports whether a character separates cron fields. Spelled out
 * rather than using isspace() so the answer does not depend on the locale.
 */
static bool
IsCronSpace(char character)
{
	return character == ' ' || character == '\t' ||
		character == '\n' || character == '\r';
}


/*
 * CronSyntaxError reports a malformed expression, naming the offending field
 * so the user does not have to count fields to find it.
 */
static void
CronSyntaxError(const char *expression, const char *detail)
{
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid cron expression: \"%s\"", expression),
			 errdetail("%s", detail)));
}


/*
 * SplitCronFields splits an expression on whitespace, storing up to maxFields
 * palloc'd field strings and returning the total number of fields found,
 * which may exceed maxFields.
 */
static int
SplitCronFields(const char *expression, char **fields, int maxFields)
{
	const char *cursor = expression;
	int			fieldCount = 0;

	for (;;)
	{
		while (*cursor != '\0' && IsCronSpace(*cursor))
			cursor++;

		if (*cursor == '\0')
			break;

		const char *fieldStart = cursor;

		while (*cursor != '\0' && !IsCronSpace(*cursor))
			cursor++;

		if (fieldCount < maxFields)
			fields[fieldCount] = pnstrdup(fieldStart, cursor - fieldStart);

		fieldCount++;
	}

	return fieldCount;
}


/*
 * ParseCronValue parses one endpoint of a range: either a decimal number or,
 * where the field allows it, a three-letter name. The result is checked
 * against the field's range.
 */
static int
ParseCronValue(const char *expression, const CronFieldSpec * spec,
			   const char *text)
{
	if (*text == '\0')
		CronSyntaxError(expression,
						psprintf("the %s field has an empty value",
								 spec->fieldName));

	if (spec->valueNames != NULL && !isdigit((unsigned char) *text))
	{
		for (int nameIndex = 0; spec->valueNames[nameIndex] != NULL; nameIndex++)
		{
			if (pg_strcasecmp(text, spec->valueNames[nameIndex]) == 0)
				return spec->firstNamedValue + nameIndex;
		}

		CronSyntaxError(expression,
						psprintf("\"%s\" is not a recognized %s name",
								 text, spec->fieldName));
	}

	int			value = 0;

	for (const char *cursor = text; *cursor != '\0'; cursor++)
	{
		if (!isdigit((unsigned char) *cursor))
			CronSyntaxError(expression,
							psprintf("\"%s\" is not a valid %s",
									 text, spec->fieldName));

		value = value * 10 + (*cursor - '0');

		/* stop before overflowing; any such value is out of range anyway */
		if (value > spec->maxValue)
			break;
	}

	if (value < spec->minValue || value > spec->maxValue)
		CronSyntaxError(expression,
						psprintf("%s must be between %d and %d, got \"%s\"",
								 spec->fieldName, spec->minValue,
								 spec->maxValue, text));

	return value;
}


/*
 * ParseCronStep parses the number after a '/'. A step counts positions within
 * the range it applies to, so it must be at least 1 and never needs to exceed
 * the width of the field.
 */
static int
ParseCronStep(const char *expression, const CronFieldSpec * spec,
			  const char *text)
{
	int			span = spec->maxValue - spec->minValue + 1;
	int			step = 0;

	if (*text == '\0')
		CronSyntaxError(expression,
						psprintf("the %s field has a '/' with no step after it",
								 spec->fieldName));

	for (const char *cursor = text; *cursor != '\0'; cursor++)
	{
		if (!isdigit((unsigned char) *cursor))
			CronSyntaxError(expression,
							psprintf("\"%s\" is not a valid step for the %s field",
									 text, spec->fieldName));

		step = step * 10 + (*cursor - '0');

		if (step > span)
			break;
	}

	if (step < 1 || step > span)
		CronSyntaxError(expression,
						psprintf("the %s step must be between 1 and %d, got \"%s\"",
								 spec->fieldName, span, text));

	return step;
}


/*
 * ParseCronField parses one whole field into a bitmap of the values it
 * matches, and reports through *restricted whether the field was anything
 * other than a bare '*'.
 */
static uint64
ParseCronField(const char *expression, const CronFieldSpec * spec,
			   const char *fieldText, bool *restricted)
{
	uint64		bits = 0;
	const char *elementStart = fieldText;

	*restricted = (strcmp(fieldText, "*") != 0);

	for (;;)
	{
		const char *comma = strchr(elementStart, ',');
		char	   *element = comma != NULL
			? pnstrdup(elementStart, comma - elementStart)
			: pstrdup(elementStart);

		/* an explicit step, as in "*<slash>15" or "0-30<slash>10" */
		int			step = 1;
		char	   *slash = strchr(element, '/');

		if (slash != NULL)
		{
			*slash = '\0';
			step = ParseCronStep(expression, spec, slash + 1);
		}

		int			rangeStart;
		int			rangeEnd;

		if (strcmp(element, "*") == 0)
		{
			rangeStart = spec->minValue;
			rangeEnd = spec->maxValue;
		}
		else
		{
			char	   *dash = strchr(element, '-');

			if (dash != NULL)
			{
				*dash = '\0';
				rangeStart = ParseCronValue(expression, spec, element);
				rangeEnd = ParseCronValue(expression, spec, dash + 1);
			}
			else
			{
				rangeStart = ParseCronValue(expression, spec, element);

				/*
				 * "N/S" means "from N to the end of the field, every S", the
				 * same reading Vixie cron gives it. A bare "N" is just N.
				 */
				rangeEnd = slash != NULL ? spec->maxValue : rangeStart;
			}
		}

		/*
		 * Walk the range as a sequence so that a wrapping range such as the
		 * hours "22-2" is well defined, and so a step counts positions within
		 * that sequence rather than raw values.
		 */
		int			span = spec->maxValue - spec->minValue + 1;
		int			rangeLength = rangeEnd >= rangeStart
			? rangeEnd - rangeStart + 1
			: rangeEnd - rangeStart + 1 + span;

		for (int offset = 0; offset < rangeLength; offset += step)
		{
			int			value = spec->minValue +
				((rangeStart - spec->minValue + offset) % span);

			bits |= UINT64CONST(1) << value;
		}

		if (comma == NULL)
			break;

		elementStart = comma + 1;
	}

	/* day of week accepts both 0 and 7 for Sunday */
	if (spec->maxValue == 7 && spec->valueNames == dayOfWeekNames)
	{
		if (bits & (UINT64CONST(1) << 7))
		{
			bits |= UINT64CONST(1);
			bits &= ~(UINT64CONST(1) << 7);
		}
	}

	return bits;
}


/*
 * ParseCronSchedule parses a 5-field cron expression, or one of the @macro
 * shorthands, into schedule. It raises an error on anything it cannot parse.
 */
void
ParseCronSchedule(const char *expression, CronSchedule * schedule)
{
	const char *effectiveExpression = expression;
	char	   *fields[CRON_FIELD_COUNT];

	/* a macro stands in for a whole expression */
	if (strchr(expression, '@') != NULL)
	{
		const char *trimmed = expression;

		while (*trimmed != '\0' && IsCronSpace(*trimmed))
			trimmed++;

		for (size_t macroIndex = 0; macroIndex < lengthof(cronMacros); macroIndex++)
		{
			if (pg_strcasecmp(trimmed, cronMacros[macroIndex].macro) == 0)
			{
				effectiveExpression = cronMacros[macroIndex].expansion;
				break;
			}
		}

		if (effectiveExpression == expression)
			CronSyntaxError(expression, "unrecognized schedule macro");
	}

	int			fieldCount = SplitCronFields(effectiveExpression, fields,
											 CRON_FIELD_COUNT);

	if (fieldCount != CRON_FIELD_COUNT)
		CronSyntaxError(expression,
						psprintf("expected %d fields separated by whitespace, got %d",
								 CRON_FIELD_COUNT, fieldCount));

	memset(schedule, 0, sizeof(CronSchedule));

	bool		restricted[CRON_FIELD_COUNT];
	uint64		bits[CRON_FIELD_COUNT];

	for (int fieldIndex = 0; fieldIndex < CRON_FIELD_COUNT; fieldIndex++)
	{
		bits[fieldIndex] = ParseCronField(expression,
										  &cronFieldSpecs[fieldIndex],
										  fields[fieldIndex],
										  &restricted[fieldIndex]);
	}

	schedule->minutes = bits[0];
	schedule->hours = (uint32) bits[1];
	schedule->daysOfMonth = (uint32) bits[2];
	schedule->months = (uint16) bits[3];
	schedule->daysOfWeek = (uint8) bits[4];
	schedule->dayOfMonthRestricted = restricted[2];
	schedule->dayOfWeekRestricted = restricted[4];
}


/*
 * CronDayMatches reports whether a given date is one the schedule fires on.
 */
static bool
CronDayMatches(const CronSchedule * schedule, int dayOfMonth, int dayOfWeek)
{
	bool		dayOfMonthMatches =
		(schedule->daysOfMonth & (1u << dayOfMonth)) != 0;
	bool		dayOfWeekMatches =
		(schedule->daysOfWeek & (1u << dayOfWeek)) != 0;

	/*
	 * Vixie cron treats the two day fields as a union once both are
	 * restricted, so "0 0 13 * 5" fires on the 13th *and* on every Friday.
	 * When only one is restricted the other is a bare '*' whose bitmap is
	 * full, so the conjunction below consults just the restricted one.
	 */
	if (schedule->dayOfMonthRestricted && schedule->dayOfWeekRestricted)
		return dayOfMonthMatches || dayOfWeekMatches;

	return dayOfMonthMatches && dayOfWeekMatches;
}


/*
 * CronScheduleNextRun returns the first time at or after the minute following
 * `after` that the schedule matches.
 *
 * Matching happens in the server's TimeZone, in local wall-clock terms, which
 * is what a cron user expects: "0 3 * * *" means 3am local, on both sides of a
 * daylight-saving transition.
 */
TimestampTz
CronScheduleNextRun(const CronSchedule * schedule, TimestampTz after)
{
	struct pg_tm afterTm;
	fsec_t		fsec;
	int			tzOffset;

	if (TIMESTAMP_NOT_FINITE(after))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot compute a cron run time from a non-finite timestamp")));

	if (timestamp2tm(after, &tzOffset, &afterTm, &fsec, NULL,
					 session_timezone) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
				 errmsg("timestamp out of range")));

	/*
	 * Cron has minute resolution, and the result must be strictly after
	 * `after`, so start the search at the next whole minute however many
	 * seconds into the current one we are.
	 */
	int			startJulianDay = date2j(afterTm.tm_year, afterTm.tm_mon,
										afterTm.tm_mday);
	int			startMinuteOfDay = afterTm.tm_hour * MINS_PER_HOUR +
		afterTm.tm_min + 1;

	if (startMinuteOfDay >= HOURS_PER_DAY * MINS_PER_HOUR)
	{
		startJulianDay++;
		startMinuteOfDay = 0;
	}

	int			startHour = startMinuteOfDay / MINS_PER_HOUR;

	/*
	 * Walk forward a day at a time rather than a minute at a time: the
	 * expensive fields to satisfy are the date ones, and skipping a whole
	 * non-matching day at once keeps even a once-a-leap-year expression to a
	 * few thousand iterations.
	 */
	for (int dayOffset = 0; dayOffset <= MAX_CRON_SEARCH_DAYS; dayOffset++)
	{
		int			year;
		int			month;
		int			day;

		j2date(startJulianDay + dayOffset, &year, &month, &day);

		if (!(schedule->months & (1u << month)))
			continue;

		if (!CronDayMatches(schedule, day, j2day(startJulianDay + dayOffset)))
			continue;

		/* only the first day of the search is bounded below by the clock */
		int			firstHour = dayOffset == 0 ? startHour : 0;

		for (int hour = firstHour; hour < HOURS_PER_DAY; hour++)
		{
			if (!(schedule->hours & (1u << hour)))
				continue;

			int			firstMinute = (dayOffset == 0 && hour == startHour)
				? startMinuteOfDay % MINS_PER_HOUR
				: 0;

			for (int minute = firstMinute; minute < MINS_PER_HOUR; minute++)
			{
				if (!(schedule->minutes & (UINT64CONST(1) << minute)))
					continue;

				struct pg_tm resultTm;
				TimestampTz result;

				memset(&resultTm, 0, sizeof(resultTm));
				resultTm.tm_year = year;
				resultTm.tm_mon = month;
				resultTm.tm_mday = day;
				resultTm.tm_hour = hour;
				resultTm.tm_min = minute;

				int			resultOffset =
					DetermineTimeZoneOffset(&resultTm, session_timezone);

				if (tm2timestamp(&resultTm, 0, &resultOffset, &result) != 0)
					ereport(ERROR,
							(errcode(ERRCODE_DATETIME_VALUE_OUT_OF_RANGE),
							 errmsg("timestamp out of range")));

				return result;
			}
		}
	}

	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("cron schedule never matches"),
			 errdetail("No matching time was found within %d days.",
					   MAX_CRON_SEARCH_DAYS)));
}


PG_FUNCTION_INFO_V1(pg_job_scheduler_next_cron_run);

/*
 * pg_job_scheduler_next_cron_run exposes the schedule calculation to SQL, so
 * that the scheduler can advance next_run_at in a single UPDATE and so that a
 * user can check what an expression will do before submitting a job.
 */
Datum
pg_job_scheduler_next_cron_run(PG_FUNCTION_ARGS)
{
	char	   *expression = text_to_cstring(PG_GETARG_TEXT_PP(0));
	TimestampTz fromTime = PG_GETARG_TIMESTAMPTZ(1);
	CronSchedule schedule;

	ParseCronSchedule(expression, &schedule);

	PG_RETURN_TIMESTAMPTZ(CronScheduleNextRun(&schedule, fromTime));
}
