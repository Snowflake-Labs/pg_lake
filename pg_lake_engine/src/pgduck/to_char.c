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
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/parsetree/const.h"
#include "pg_lake/parsetree/expression.h"
#include "pg_lake/pgduck/to_char.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "nodes/primnodes.h"
#include "parser/parse_func.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/pg_locale.h"

#define MAX_MONTH_LENGTH (9)
#define MAX_DAY_LENGTH (9)

/*
 * Function to create a custom expression for a to_char format specifier.
 * We can pass in an optional strftime specifier in case the expression
 * still uses strftime.
 */
typedef Node *(*CreateCustomExpressionFunc) (Node *toCharExpr,
											 char *toCharSpecifier);

/*
 * FormatSpecifierMappings specifies a mapping between a to_char format
 * specifier and strftime.
 */
typedef struct FormatSpecifierMapping
{
	/*
	 * the to_char specifier from
	 * https://www.postgresql.org/docs/current/functions-formatting.html
	 */
	char	   *toCharSpecifier;
	int			specifierLength;

	/* if true, skip the characters */
	bool		ignore;

	/* if true, check for exact match */
	bool		isCaseSensitive;

	/*
	 * if true, the output is text (not a number) and we should not append -th
	 * or -TH suffix
	 *
	 * if false, the output must be castable to int.
	 */
	bool		isTextual;

	/*
	 * The strftime specifier from
	 * https://duckdb.org/docs/sql/functions/dateformat.html or NULL if there
	 * is no direct mapping.
	 */
	char	   *strftimeSpecifier;

	/* padded version, if any */
	char	   *strftimeSpecifierPadded;

	/*
	 * an optional function pointer to create an expression to implement the
	 * specifier.
	 *
	 * (mutually exclusive with strftimeSpecifier)
	 */
	CreateCustomExpressionFunc createCustomExpr;

	/*
	 * How many characters of padding to use with which string
	 *
	 * (mutually exclusive with strftimeSpecifierPadded)
	 */
	int			padLength;
	char	   *padString;
} FormatSpecifierMapping;

static Node *MakeStrftimeExpr(Node *timeArg, char *strftimeFormat);
static Node *MakeSSSSExpr(Node *timeArg, char *tocharSpecifier);
static Node *MakeFFExpr(Node *timeArg, char *tocharSpecifier);
static Node *MakeYExpr(Node *timeArg, char *tocharSpecifier);
static Node *MakeYCommaYYYExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeIYExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeMonthNameExpr(Node *timeArg, char *tocharSpecifier);
static Node *MakeDayNameExpr(Node *timeArg, char *tocharSpecifier);
static Node *MakeDayOfWeekExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeWeekCountInMonthExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeWeekCountInYearExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeIsoWeekNumberExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeCenturyExpr(Node *timeArg, char *tocharSpecifier);
static Node *MakeQuarterExpr(Node *timeArg, char *tocharSpecifier);
static Node *MakeEraExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeAmPmExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeTimeZoneHoursExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeTimeZoneMinutesExpr(Node *timeArg, char *toCharSpecifier);
static Node *MakeNthSuffixExpr(Node *arg);

static FormatSpecifierMapping SpecifierMappings[] =
{
	/* longest match needs to go first */
	{
		/* seconds since midnight */
		.toCharSpecifier = "SSSSS",
		.createCustomExpr = MakeSSSSExpr
	},
	{
		/* seconds since midnight */
		.toCharSpecifier = "SSSS",
		.createCustomExpr = MakeSSSSExpr
	},

	{
		/* Hours 24-hour format (0-padded) */
		.toCharSpecifier = "HH24",
		.strftimeSpecifier = "%-H",
		.strftimeSpecifierPadded = "%H"
	},
	{
		/* Hours 12-hour format (0-padded) */
		.toCharSpecifier = "HH12",
		.strftimeSpecifier = "%-I",
		.strftimeSpecifierPadded = "%I"
	},
	{
		/* Hours 12-hour format (0-padded) */
		.toCharSpecifier = "HH",
		.strftimeSpecifier = "%I"
	},
	{
		/* Minutes (0-padded) */
		.toCharSpecifier = "MI",
		.strftimeSpecifier = "%-M",
		.strftimeSpecifierPadded = "%M"
	},
	{
		/* Seconds (0-padded) */
		.toCharSpecifier = "SS",
		.strftimeSpecifier = "%-S",
		.strftimeSpecifierPadded = "%S"
	},
	{
		/* Milliseconds */
		.toCharSpecifier = "MS",
		.strftimeSpecifier = "%g"
	},
	{
		/* Microseconds */
		.toCharSpecifier = "US",
		.strftimeSpecifier = "%f"
	},
	{
		/* Tenth of seconds */
		.toCharSpecifier = "FF1",
		.createCustomExpr = MakeFFExpr
	},
	{
		/* Hundredth of seconds */
		.toCharSpecifier = "FF2",
		.createCustomExpr = MakeFFExpr,
		.padLength = -2,
		.padString = "0"
	},
	{
		/* Tenth of a millisecond */
		.toCharSpecifier = "FF3",
		.createCustomExpr = MakeFFExpr,
		.padLength = -3,
		.padString = "0"
	},
	{
		/* Hundredth of a millisecond */
		.toCharSpecifier = "FF4",
		.createCustomExpr = MakeFFExpr,
		.padLength = -4,
		.padString = "0"
	},
	{
		/* Tenth of a microsecond */
		.toCharSpecifier = "FF5",
		.createCustomExpr = MakeFFExpr,
		.padLength = -5,
		.padString = "0"
	},
	{
		/* Hundredth of a microsecond */
		.toCharSpecifier = "FF6",
		.createCustomExpr = MakeFFExpr,
		.padLength = -6,
		.padString = "0"
	},
	{
		/* AM / PM */
		.toCharSpecifier = "AM",
		.strftimeSpecifier = "%p",
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* am / pm */
		.toCharSpecifier = "am",
		.createCustomExpr = MakeAmPmExpr,
		.isCaseSensitive = true,
		.isTextual = true

	},
	{
		/* AM / PM */
		.toCharSpecifier = "PM",
		.strftimeSpecifier = "%p",
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* am / pm */
		.toCharSpecifier = "pm",
		.createCustomExpr = MakeAmPmExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* A.M. / P.M. */
		.toCharSpecifier = "A.M.",
		.createCustomExpr = MakeAmPmExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* a.m. / p.m. */
		.toCharSpecifier = "a.m.",
		.createCustomExpr = MakeAmPmExpr,
		.isCaseSensitive = true,
		.isTextual = true

	},
	{
		/* A.M. / P.M. */
		.toCharSpecifier = "P.M.",
		.createCustomExpr = MakeAmPmExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* a.m. / p.m. */
		.toCharSpecifier = "p.m.",
		.createCustomExpr = MakeAmPmExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* Year (always 0-padded to 4 digits) */
		/*
		 * always padding is slightly wrong for <1000AD, but optimizes a
		 * common case
		 */
		.toCharSpecifier = "YYYY",
		.strftimeSpecifier = "%Y"
	},
	{
		/* Year formatted as 2,025  */
		/*
		 * We should always 0 pad for <1000AD, but DuckDB printf does not
		 * currently support %04,d syntax, and doing the padding with commas
		 * would be quite complicated.
		 */
		.toCharSpecifier = "Y,YYY",
		.createCustomExpr = MakeYCommaYYYExpr,
	},
	{
		/* Last 3 digits of the year */
		.toCharSpecifier = "YYY",
		.createCustomExpr = MakeYExpr,
		.padLength = -3,
		.padString = "0"
	},
	{
		/* Last 2 digits of year */
		.toCharSpecifier = "YY",
		.strftimeSpecifier = "%-y",
		.strftimeSpecifierPadded = "%y"
	},
	{
		/* Last digit of the year */
		.toCharSpecifier = "Y",
		.createCustomExpr = MakeYExpr
	},
	{
		/* ISO 8601 week aligned year (always 0-padded to 4 digits) */
		/*
		 * always padding is slightly wrong for <1000AD, but optimizes a
		 * common case
		 */
		.toCharSpecifier = "IYYY",
		.strftimeSpecifier = "%G"
	},
	{
		/* last 3 digits of ISO 8601 week aligned year */
		.toCharSpecifier = "IYY",
		.createCustomExpr = MakeIYExpr,
		.padLength = -3,
		.padString = "0"
	},
	{
		/* last 2 digits of ISO 8601 week aligned year */
		.toCharSpecifier = "IY",
		.createCustomExpr = MakeIYExpr,
		.padLength = -2,
		.padString = "0"
	},
	{
		/* AD / BC */
		.toCharSpecifier = "BC",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* ad / bc */
		.toCharSpecifier = "bc",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* AD / BC */
		.toCharSpecifier = "AD",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* ad / bc */
		.toCharSpecifier = "ad",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* A.D. / B.C. */
		.toCharSpecifier = "B.C.",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* a.d. / b.c. */
		.toCharSpecifier = "b.c.",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* A.D. / B.C. */
		.toCharSpecifier = "A.D.",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* a.d. / b.c. */
		.toCharSpecifier = "a.d.",
		.createCustomExpr = MakeEraExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* month name uppercase */
		.toCharSpecifier = "MONTH",
		.createCustomExpr = MakeMonthNameExpr,
		.padLength = MAX_MONTH_LENGTH,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* month name */
		.toCharSpecifier = "Month",
		.strftimeSpecifier = "%B",
		.padLength = MAX_MONTH_LENGTH,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* month name lowercase */
		.toCharSpecifier = "month",
		.createCustomExpr = MakeMonthNameExpr,
		.padLength = MAX_MONTH_LENGTH,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* abbreviated month name uppercase */
		.toCharSpecifier = "MON",
		.createCustomExpr = MakeMonthNameExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* abbreviated month name */
		.toCharSpecifier = "Mon",
		.strftimeSpecifier = "%b",
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* abbreviated month name lowercase */
		.toCharSpecifier = "mon",
		.createCustomExpr = MakeMonthNameExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* month number (0-padded) */
		.toCharSpecifier = "MM",
		.strftimeSpecifier = "%-m",
		.strftimeSpecifierPadded = "%m",
		.isTextual = true
	},
	{
		/* day name uppercase */
		.toCharSpecifier = "DAY",
		.createCustomExpr = MakeDayNameExpr,
		.padLength = MAX_DAY_LENGTH,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* day name */
		.toCharSpecifier = "Day",
		.strftimeSpecifier = "%A",
		.isCaseSensitive = true,
		.padLength = MAX_DAY_LENGTH,
		.isTextual = true
	},
	{
		/* day name lowercase */
		.toCharSpecifier = "day",
		.createCustomExpr = MakeDayNameExpr,
		.padLength = MAX_DAY_LENGTH,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* abbreviated day name uppercase */
		.toCharSpecifier = "DY",
		.createCustomExpr = MakeDayNameExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* abbreviated day name */
		.toCharSpecifier = "Dy",
		.strftimeSpecifier = "%a",
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* abbreviated day name lowercase */
		.toCharSpecifier = "dy",
		.createCustomExpr = MakeDayNameExpr,
		.isCaseSensitive = true,
		.isTextual = true
	},
	{
		/* day of the year */
		.toCharSpecifier = "DDD",
		.strftimeSpecifier = "%-j",
		.strftimeSpecifierPadded = "%j"
	},
	{
		/* day of the year of ISO 8601 week aligned year */
		.toCharSpecifier = "IDDD"
		/* not pushed down */
	},
	{
		/* day of the month */
		.toCharSpecifier = "DD",
		.strftimeSpecifier = "%-d",
		.strftimeSpecifierPadded = "%d"
	},
	{
		/* day of the week with Sunday = 1 */
		.toCharSpecifier = "D",
		.createCustomExpr = MakeDayOfWeekExpr,
	},
	{
		/* day of the week with Monday = 1 */
		.toCharSpecifier = "ID",
		.strftimeSpecifier = "%u"
	},
	{
		/* week of year starting on first day */
		.toCharSpecifier = "WW",
		.createCustomExpr = MakeWeekCountInYearExpr,
		.padLength = -2,
		.padString = "0"
	},
	{
		/* week of month starting on first day */
		.toCharSpecifier = "W",
		.createCustomExpr = MakeWeekCountInMonthExpr
	},
	{
		/* week number ISO 8601 week aligned year */
		.toCharSpecifier = "IW",
		.strftimeSpecifierPadded = "%V",
		.createCustomExpr = MakeIsoWeekNumberExpr,
		.padLength = -2,
		.padString = "0"
	},
	{
		/* last digit of ISO 8601 week aligned year */
		.toCharSpecifier = "I",
		.createCustomExpr = MakeIYExpr
	},
	{
		/* century */
		.toCharSpecifier = "CC",
		.createCustomExpr = MakeCenturyExpr,
		.padLength = -2,
		.padString = "0"
	},
	{
		/* julian date */
		.toCharSpecifier = "J"
		/* not pushed down */
	},
	{
		/* quarter */
		.toCharSpecifier = "Q",
		.createCustomExpr = MakeQuarterExpr,
	},
	{
		/* month in uppercase roman numerals */
		.toCharSpecifier = "RM"
		/* not pushed down */
	},
	{
		/* month in lowercase roman numerals */
		.toCharSpecifier = "rm"
		/* not pushed down */
	},
	{
		/* time zone hours */
		.toCharSpecifier = "TZH",
		.createCustomExpr = MakeTimeZoneHoursExpr,
		.isTextual = true
	},
	{
		/* time zone minutes */
		.toCharSpecifier = "TZM",
		.createCustomExpr = MakeTimeZoneMinutesExpr,
		.isTextual = true
	},
	{
		/* uppercase time zone abbreviation */
		.toCharSpecifier = "TZ"
		/* not pushed down */
	},
	{
		/* lowercase time zone abbreviation */
		.toCharSpecifier = "tz"
		/* not pushed down */
	},
	{
		/* timezone offset */
		.toCharSpecifier = "OF",
		.strftimeSpecifier = "%z",
		.isTextual = true
	},
	{
		/* fill mode, included for completeness, but handled separately */
		.toCharSpecifier = "FM"
	},
	{
		/* fixed format mode, mainly for parsing */
		.toCharSpecifier = "FX",
		.ignore = true
	},
	{
		/* translation mode, mainly for parsing */
		.toCharSpecifier = "TM",
		.ignore = true
	},
	{
		/* spell mode (not implemented in postgres) */
		.toCharSpecifier = "SP",
		.ignore = true
	},
	{
		/* ensure % is escaped as %% */
		.toCharSpecifier = "%",
		.strftimeSpecifier = "%%"
	},
};


/*
 * BuildStrftimeChain builds a chain of expressions that implement
 * a to_char format.
 */
bool
BuildStrftimeChain(FuncExpr *toCharExpr, bool checkOnly, List **chain)
{
	Node	   *timeArg = linitial(toCharExpr->args);
	Node	   *formatArg = lsecond(toCharExpr->args);
	Const	   *formatConst = ResolveConstChain(formatArg);

	if (formatConst == NULL)
		elog(ERROR, "cannot handle non-Const format");

	if (formatConst->constisnull)
		elog(ERROR, "cannot handle NULL format");

	char	   *format = TextDatumGetCString(formatConst->constvalue);
	int			mappingCount = sizeof(SpecifierMappings) / sizeof(FormatSpecifierMapping);
	bool		inStringLiteral = false;

	StringInfoData strftimeFormat;

	initStringInfo(&strftimeFormat);

	for (char *remainder = format; *remainder != '\0';)
	{
		bool		addPadding = true;
		bool		matched = false;

		/*
		 * Currently in a literal string that started with a "
		 */
		if (inStringLiteral)
		{
			if (*remainder == '"')
			{
				/* end of a literal string */
				inStringLiteral = false;

				/* skip the double quote itself */
				remainder++;
			}
			else if (*remainder == '\\')
			{
				/*
				 * Skip escape characters, since double quotes have no special
				 * meaning in strftime.
				 */
				remainder++;

				/* append the escaped character */
				appendStringInfoChar(&strftimeFormat, *remainder);
				remainder++;
			}
			else
			{
				/* append a character from the literal string */
				appendStringInfoChar(&strftimeFormat, *remainder);
				remainder++;
			}

			/* skip looking for specifiers */
			continue;
		}
		else if (remainder[0] == '"')
		{
			/* start of a literal string */
			inStringLiteral = true;

			/* skip the double quote itself */
			remainder++;

			/* skip looking for specifiers */
			continue;
		}
		else if (pg_strncasecmp(remainder, "FM", 2) == 0)
		{
			/* FM avoids additional padding */
			addPadding = false;

			/* skip the FM */
			remainder += 2;
		}
		else if (pg_strncasecmp(remainder, "TM", 2) == 0)
		{
			/* if a custom locale is set, do not push down TM */
			if (strcmp(locale_time, "C.UTF-8") != 0 &&
				strcmp(locale_time, "en_US") != 0 &&
				strcmp(locale_time, "en_US.UTF-8") != 0 &&
				strcmp(locale_time, "C") != 0 &&
				strcmp(locale_time, "POSIX") != 0 &&
				strcmp(locale_time, "") != 0)
			{
				if (checkOnly)
					return false;

				/*
				 * Should be unreachable due to the line above preventing
				 * pushdown, unless locale_time changed very recently.
				 */
				elog(ERROR, "cannot push down to_char when lc_time is set to %s", locale_time);
			}

			/* TM avoids additional padding */
			addPadding = false;

			/* skip the TM */
			remainder += 2;
		}

		/* check whether current substring matches any of our specifiers */
		for (int mappingIndex = 0; mappingIndex < mappingCount; mappingIndex++)
		{
			FormatSpecifierMapping *specifierMapping = &SpecifierMappings[mappingIndex];
			char	   *toCharSpecifier = specifierMapping->toCharSpecifier;
			int			padLength = specifierMapping->padLength;
			bool		isCaseSensitive = specifierMapping->isCaseSensitive;

			/* initialize the string length once */
			if (specifierMapping->specifierLength == 0)
				specifierMapping->specifierLength = strlen(toCharSpecifier);

			int			toCharSpecifierLength = specifierMapping->specifierLength;
			Node	   *customExpression = NULL;

			if ((!isCaseSensitive &&
				 pg_strncasecmp(remainder, toCharSpecifier, toCharSpecifierLength) == 0) ||
				strncmp(remainder, toCharSpecifier, toCharSpecifierLength) == 0)
			{
				char	   *strftimeSpecifier = specifierMapping->strftimeSpecifier;

				/*
				 * If padding is needed and there is a specifier with padding,
				 * use it.
				 */
				if (specifierMapping->strftimeSpecifierPadded != NULL && addPadding)
					strftimeSpecifier = specifierMapping->strftimeSpecifierPadded;

				/* move the remainder forward */
				remainder += specifierMapping->specifierLength;

				/* check for -TH or -th suffix */
				bool		addNthSuffix = false;
				bool		useUpperCaseNthSuffix = false;

				if (pg_strncasecmp(remainder, "TH", 2) == 0)
				{
					addNthSuffix = !specifierMapping->isTextual;
					useUpperCaseNthSuffix = remainder[0] == 'T';
					remainder += 2;
				}

				if (specifierMapping->ignore)
				{
					/* only skip characters */
				}
				else if (strftimeSpecifier != NULL)
				{
					if (checkOnly)
					{
						/* only checking whether expression pushes down */
					}
					else if ((padLength != 0 && addPadding) || addNthSuffix)
					{
						/*
						 * to_char specifier has a strftime equivalent, but it
						 * does not have padding, or we need an -th/-TH
						 * suffix.
						 */
						customExpression =
							MakeStrftimeExpr(timeArg, strftimeSpecifier);
					}
					else
					{
						/*
						 * to_char specifier has a strftime equivalent that we
						 * can append to the current format.
						 */
						appendStringInfoString(&strftimeFormat, strftimeSpecifier);
					}
				}
				else if (specifierMapping->createCustomExpr != NULL)
				{
					/* to_char specifier has a custom equivalent */

					if (!checkOnly)
						/* concatenate a custom expression */
						customExpression =
							specifierMapping->createCustomExpr(timeArg, toCharSpecifier);
				}
				else
				{
					/* to_char specifier has no equivalent */
					if (checkOnly)
						return false;

					elog(ERROR, "cannot handle specifier: %s", strftimeSpecifier);
				}

				/*
				 * We get customExpression in two cases: 1) a specifier had an
				 * explicit customExpression 2) strftime needed extra padding,
				 * in which case we add it below
				 */
				if (customExpression != NULL)
				{
					if (strftimeFormat.len > 0)
					{
						/* add a strftime call for the format so far */
						Node	   *expressionSoFar =
							MakeStrftimeExpr(timeArg, strftimeFormat.data);

						*chain = lappend(*chain, expressionSoFar);

						initStringInfo(&strftimeFormat);
					}

					/* check whether we need to add padding */
					if (padLength != 0 && addPadding)
					{
						char	   *padString = specifierMapping->padString;

						if (padString == NULL)
							/* use whitespace by default */
							padString = " ";

						if (padLength < 0)
							customExpression = MakeLpadExpr(customExpression,
															-padLength,
															padString);
						else
							customExpression = MakeRpadExpr(customExpression,
															padLength,
															padString);
					}

					*chain = lappend(*chain, customExpression);

					if (addNthSuffix)
					{
						Node	   *nthSuffix = MakeNthSuffixExpr(customExpression);

						if (useUpperCaseNthSuffix)
							nthSuffix = MakeUpperCaseExpr(nthSuffix);

						*chain = lappend(*chain, nthSuffix);
					}
				}

				matched = true;

				break;
			}
		}

		/* current char does not match a specifier, append it */
		if (!matched)
		{
			appendStringInfoChar(&strftimeFormat, *remainder);
			remainder++;
		}
	}

	if (strftimeFormat.len > 0 && !checkOnly)
		*chain = lappend(*chain, MakeStrftimeExpr(timeArg, strftimeFormat.data));

	return true;
}


/*
 * MakeStrftimeExpr builds a strftime(timestamp, format) expression
 * derived from a to_char expression.
 */
static Node *
MakeStrftimeExpr(Node *timeArg, char *strftimeFormat)
{
	const int	argCount = 2;
	List	   *strftimeName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										  makeString("strftime"));
	Oid			argTypes[] = {exprType(timeArg), TEXTOID};

	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = LookupFuncName(strftimeName, argCount, argTypes, false);
	funcExpr->funcresulttype = TEXTOID;
	funcExpr->location = -1;
	funcExpr->args = list_make2(timeArg, MakeStringConst(strftimeFormat));

	return (Node *) funcExpr;
}


/*
 * MakePrintfExpr builds a printf(format, arg) expression.
 */
static Node *
MakePrintfExpr(char *format, Node *arg)
{
	const int	argCount = 2;
	List	   *strftimeName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										  makeString("printf"));
	Oid			argTypes[] = {TEXTOID, ANYOID};

	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = LookupFuncName(strftimeName, argCount, argTypes, false);
	funcExpr->funcresulttype = TEXTOID;
	funcExpr->location = -1;
	funcExpr->args = list_make2(MakeStringConst(format), arg);

	return (Node *) funcExpr;
}


/*
 * MakeSSSSExpr constructs a strtime expression for SSSSS
 * as date_part('epoch', timestamp)::bigint - date_part('epoch', date_trunc('day', timestamp))::bigint
 */
static Node *
MakeSSSSExpr(Node *timeArg, char *toCharSpecifier)
{
	/* get seconds since epoch (can be negative) */
	Node	   *epochExpr = MakeCastExpr(MakeDatePartExpr("epoch", timeArg), INT8OID);

	/* get seconds since epoch for the start of the day */
	Node	   *dayTruncExpr = MakeDateTruncExpr("day", timeArg);
	Node	   *dayTruncEpochExpr = MakeCastExpr(MakeDatePartExpr("epoch", dayTruncExpr), INT8OID);

	/*
	 * get the difference to get the number of seconds since the start of the
	 * day
	 */
	Node	   *subtractExpr = MakeOpExpr(epochExpr, "pg_catalog", "-", dayTruncEpochExpr);
	Node	   *textCastExpr = MakeCastExpr(subtractExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeFFExpr constructs a strtime expression for FF[1-6]
 */
static Node *
MakeFFExpr(Node *timeArg, char *toCharSpecifier)
{
	static int	divisors[] = {100000, 10000, 1000, 100, 10, 1};

	Assert(strlen(toCharSpecifier) == 3);

	int			divisorIndex = toCharSpecifier[2] - '1';

	if (divisorIndex < 0 || divisorIndex > 5)
		elog(ERROR, "invalid FF specifier: %s", toCharSpecifier);

	int			divisor = divisors[divisorIndex];

	/* get microseconds since the start of the minute */
	Node	   *datePartExpr = MakeDatePartExpr("microseconds", timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);

	/* get microseconds since the start of the second */
	Node	   *modExpr = MakeModInt32Expr(intCastExpr, 1000000);

	/* apply the FF magnitude */
	Node	   *divisionExpr = divisor != 1 ? MakeDivisionExpr(modExpr, divisor) : modExpr;
	Node	   *textCastExpr = MakeCastExpr(divisionExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeYExpr constructs a strtime expression for YYY or Y.
 * as mod(date_part('year', timestamp)::int,100)::text
 */
static Node *
MakeYExpr(Node *timeArg, char *toCharSpecifier)
{
	int			modulo = 0;
	int			specifierLength = strlen(toCharSpecifier);

	if (specifierLength == 3)
		/* IYY */
		modulo = 1000;
	else if (specifierLength == 2)
		/* IY */
		modulo = 100;
	else if (specifierLength == 1)
		/* I */
		modulo = 10;

	Node	   *datePartExpr = MakeDatePartExpr("year", timeArg);

	if (modulo > 0)
	{
		Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);

		datePartExpr = MakeModInt32Expr(intCastExpr, modulo);
	}

	Node	   *textCastExpr = MakeCastExpr(datePartExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeYCommaYYYExpr constructs an expression for Y,YYY
 * a printf('%,d', date_part('year', timestamp)::int).
 */
static Node *
MakeYCommaYYYExpr(Node *timeArg, char *toCharSpecifier)
{
	Node	   *datePartExpr = MakeDatePartExpr("year", timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);
	Node	   *printfExpr = MakePrintfExpr("%,d", intCastExpr);

	return printfExpr;
}


/*
 * MakeIYExpr constructs an expression for IYY or IY or I
 * as lpad(mod(date_part('isoyear', timestamp)::int,100)::text, 3, '0')
 */
static Node *
MakeIYExpr(Node *timeArg, char *toCharSpecifier)
{
	int			modulo = 0;
	int			specifierLength = strlen(toCharSpecifier);

	if (specifierLength == 3)
		/* IYY */
		modulo = 1000;
	else if (specifierLength == 2)
		/* IY */
		modulo = 100;
	else if (specifierLength == 1)
		/* I */
		modulo = 10;

	Node	   *datePartExpr = MakeDatePartExpr("isoyear", timeArg);

	if (modulo > 0)
	{
		Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);

		datePartExpr = MakeModInt32Expr(intCastExpr, modulo);
	}


	Node	   *textCastExpr = MakeCastExpr(datePartExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeDayNameExpr constructs an upper(strftime(..)) or lower(strftime(..))
 * expression to print the day name.
 */
static Node *
MakeDayNameExpr(Node *timeArg, char *toCharSpecifier)
{
	bool		isUpperCase = toCharSpecifier[0] == 'D';
	bool		isLongForm = strlen(toCharSpecifier) == 3;
	char	   *strftimeSpecifier = isLongForm ? "%A" : "%a";
	Node	   *strftimeExpr = MakeStrftimeExpr(timeArg, strftimeSpecifier);

	return isUpperCase ? MakeUpperCaseExpr(strftimeExpr) : MakeLowerCaseExpr(strftimeExpr);
}


/*
 * MakeMonthNameExpr constructs an upper(strftime(..)) or lower(strftime(..))
 * expression to print the month name.
 */
static Node *
MakeMonthNameExpr(Node *timeArg, char *toCharSpecifier)
{
	bool		isUpperCase = toCharSpecifier[0] == 'M';
	bool		isLongForm = strlen(toCharSpecifier) > 3;
	char	   *strftimeSpecifier = isLongForm ? "%B" : "%b";
	Node	   *strftimeExpr = MakeStrftimeExpr(timeArg, strftimeSpecifier);

	return isUpperCase ? MakeUpperCaseExpr(strftimeExpr) : MakeLowerCaseExpr(strftimeExpr);
}


/*
 * MakeDayOfWeekExpr constructs an expression for D as
 * date_part('dayofweek', timestamp)::int + 1 since to_char treats Sunday
 * as 1, while dayofweek treats it as 0.
 */
static Node *
MakeDayOfWeekExpr(Node *timeArg, char *toCharSpecifier)
{
	Node	   *datePartExpr = MakeDatePartExpr("dayofweek", timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);
	Node	   *plusOneExpr = MakeOpExpr(intCastExpr, "pg_catalog", "+", (Node *) MakeIntConst(1));
	Node	   *textCastExpr = MakeCastExpr(plusOneExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeWeekCountExpr constructs a divide(date_part(datePart, ...), 7) expression.
 */
static Node *
MakeWeekCountExpr(Node *timeArg, char *datePart)
{
	Node	   *datePartExpr = MakeDatePartExpr(datePart, timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);

	/* subtract 1 such that 14 becomes 13 */
	Node	   *minusOneExpr = MakeOpExpr(intCastExpr, "pg_catalog", "-", (Node *) MakeIntConst(1));

	/* divide by 7 such that 13 becomes 1 */
	Node	   *weekNumberExpr = MakeDivisionExpr(minusOneExpr, 7);

	/* add 1 such that 1 becomes 2 */
	Node	   *plusOneExpr = MakeOpExpr(weekNumberExpr, "pg_catalog", "+", (Node *) MakeIntConst(1));
	Node	   *textCastExpr = MakeCastExpr(plusOneExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeWeekCountInMonthExpr constructs a divide(date_part('dayofyear', ...), 7) expression.
 */
static Node *
MakeWeekCountInMonthExpr(Node *timeArg, char *toCharSpecifier)
{
	return MakeWeekCountExpr(timeArg, "dayofmonth");
}


/*
 * MakeWeekCountInYearExpr constructs a divide(date_part('dayofyear', ...), 7) expression.
 */
static Node *
MakeWeekCountInYearExpr(Node *timeArg, char *toCharSpecifier)
{
	return MakeWeekCountExpr(timeArg, "dayofyear");
}


/*
 * MakeIsoWeekNumberExpr constructs an expression for IW
 * as date_part('weekofyear', timestamp)::int
 */
static Node *
MakeIsoWeekNumberExpr(Node *timeArg, char *toCharSpecifier)
{
	Node	   *datePartExpr = MakeDatePartExpr("weekofyear", timeArg);
	Node	   *textCastExpr = MakeCastExpr(datePartExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeCenturyExpr constructs an expression for CC
 * as date_part('century', timestamp)::int
 */
static Node *
MakeCenturyExpr(Node *timeArg, char *toCharSpecifier)
{
	Node	   *datePartExpr = MakeDatePartExpr("century", timeArg);
	Node	   *textCastExpr = MakeCastExpr(datePartExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeQuarterExpr constructs an expression for Q
 * as date_part('quarter', timestamp)::int
 */
static Node *
MakeQuarterExpr(Node *timeArg, char *toCharSpecifier)
{
	Node	   *datePartExpr = MakeDatePartExpr("quarter", timeArg);
	Node	   *textCastExpr = MakeCastExpr(datePartExpr, TEXTOID);

	return textCastExpr;
}


/*
 * MakeEraExpr constructs an expression for AD/BC/ad/bc/A.D./B.C./a.d./b.c
 * as CASE date_part('era', timestamp)::int::bool THEN 'AD' ELSE 'BC' END
 *
 * The result of date_part is 0 for BC and 1 for AD, so casting to int and
 * then bool gives true for AD and false for BC.
 */
static Node *
MakeEraExpr(Node *timeArg, char *toCharSpecifier)
{
	/* should be coming from SpecifierMappings */
	Assert(strlen(toCharSpecifier) >= 2);

	bool		isUpperCase = toCharSpecifier[0] == 'A' || toCharSpecifier[0] == 'B';
	bool		hasDots = toCharSpecifier[1] == '.';
	char	   *adStr;
	char	   *bcStr;

	if (isUpperCase)
	{
		if (hasDots)
		{
			adStr = "A.D.";
			bcStr = "B.C.";
		}
		else
		{
			adStr = "AD";
			bcStr = "BC";
		}
	}
	else
	{
		if (hasDots)
		{
			adStr = "a.d.";
			bcStr = "b.c.";
		}
		else
		{
			adStr = "ad";
			bcStr = "bc";
		}
	}

	/* date_part('era', timestamp)::int::bool */
	Node	   *datePartExpr = MakeDatePartExpr("era", timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);
	Node	   *eraCheckExpr = MakeCastExpr(intCastExpr, BOOLOID);

	/*
	 * CASE WHEN date_part('era', timestamp)::int::bool THEN 'AD' ELSE 'BC'
	 * END
	 */
	Node	   *caseExpr = MakeCaseExpr(eraCheckExpr,
										(Node *) MakeStringConst(adStr),
										(Node *) MakeStringConst(bcStr));

	return (Node *) caseExpr;
}


/*
 * MakeAmPmExpr constructs an expression for am/pm/a.m/p.m./A.M./P.M.
 * as CASE (date_part('hour', timestamp)::int / 7)::bool THEN 'pm' ELSE 'am' END
 */
static Node *
MakeAmPmExpr(Node *timeArg, char *toCharSpecifier)
{
	/* should be coming from SpecifierMappings */
	Assert(strlen(toCharSpecifier) >= 2);

	bool		isUpperCase = toCharSpecifier[0] == 'A' || toCharSpecifier[0] == 'P';
	bool		hasDots = toCharSpecifier[1] == '.';
	char	   *pmStr;
	char	   *amStr;

	if (isUpperCase)
	{
		if (hasDots)
		{
			pmStr = "P.M.";
			amStr = "A.M.";
		}
		else
		{
			pmStr = "PM";
			amStr = "AM";
		}
	}
	else
	{
		if (hasDots)
		{
			pmStr = "p.m.";
			amStr = "a.m.";
		}
		else
		{
			pmStr = "pm";
			amStr = "am";
		}
	}

	/* ((date_part('hour', timestamp)::int) / 12)::bool */
	Node	   *datePartExpr = MakeDatePartExpr("hour", timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);
	Node	   *dayHalfExpr = MakeDivisionExpr(intCastExpr, 12);
	Node	   *ampmCheckExpr = MakeCastExpr(dayHalfExpr, BOOLOID);

	/*
	 * CASE WHEN ((date_part('hour', timestamp)::int) / 12)::bool THEN 'PM'
	 * ELSE 'AM' END
	 */
	Node	   *caseExpr = MakeCaseExpr(ampmCheckExpr,
										(Node *) MakeStringConst(pmStr),
										(Node *) MakeStringConst(amStr));

	return (Node *) caseExpr;
}


/*
 * MakeTimeZoneHoursExpr constructs a printf('%+03d', date_part('timezone_hour', timestamp))
 */
static Node *
MakeTimeZoneHoursExpr(Node *timeArg, char *toCharSpecifier)
{
	Node	   *datePartExpr = MakeDatePartExpr("timezone_hour", timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);
	Node	   *printfExpr = MakePrintfExpr("%+03d", intCastExpr);

	return printfExpr;
}


/*
 * MakeTimeZoneMinutesExpr constructs a printf('%+03d', date_part('timezone_minute', timestamp))
 */
static Node *
MakeTimeZoneMinutesExpr(Node *timeArg, char *toCharSpecifier)
{
	Node	   *datePartExpr = MakeDatePartExpr("timezone_minute", timeArg);
	Node	   *intCastExpr = MakeCastExpr(datePartExpr, INT4OID);
	Node	   *printfExpr = MakePrintfExpr("%02d", intCastExpr);

	return printfExpr;
}


/*
 * MakeNthSuffixExpr constructs a lake_nth_suffix(arg::int) expression.
 */
static Node *
MakeNthSuffixExpr(Node *arg)
{
	const int	argCount = 1;
	List	   *nthSuffixName = list_make2(makeString(PG_LAKE_INTERNAL_NSP),
										   makeString("lake_nth_suffix"));
	Oid			argTypes[] = {INT4OID};

	FuncExpr   *funcExpr = makeNode(FuncExpr);

	funcExpr->funcid = LookupFuncName(nthSuffixName, argCount, argTypes, false);
	funcExpr->funcresulttype = TEXTOID;
	funcExpr->location = -1;
	funcExpr->args = list_make1(MakeCastExpr(arg, INT4OID));

	return (Node *) funcExpr;
}
