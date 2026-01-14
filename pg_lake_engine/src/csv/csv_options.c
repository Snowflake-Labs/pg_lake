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
 * Functions for handling CSV options.
 */
#include "postgres.h"

#include "commands/defrem.h"
#include "pg_lake/csv/csv_options.h"
#include "nodes/makefuncs.h"


/*
 * InternalCSVOptions returns a list of COPY options to use for
 * internal/temporary CSVs.
 */
List *
InternalCSVOptions(bool includeHeader)
{
	List	   *options = NIL;

	options = lappend(options,
					  makeDefElem("format", (Node *) makeString("csv"), -1));
	if (includeHeader)
		options = lappend(options,
						  makeDefElem("header", (Node *) makeBoolean(true), -1));

	options = lappend(options,
					  makeDefElem("delimiter", (Node *) makeString(","), -1));
	options = lappend(options,
					  makeDefElem("quote", (Node *) makeString("\""), -1));
	options = lappend(options,
					  makeDefElem("escape", (Node *) makeString("\""), -1));
	options = lappend(options,
					  makeDefElem("null", (Node *) makeString("\\N"), -1));

	return options;
}


/*
 * NormalizedExternalCSVOptions normalizes a list of CSV options.
 *
 * We normalize the list of options to include default values
 * for all options, unless auto_detect is on, in which case
 * we only include the explicitly defined ones.
 *
 * Non-CSV-related options are omitted from the returned list.
 */
List *
NormalizedExternalCSVOptions(List *inputOptions)
{
	bool		autoDetect = HasAutoDetect(inputOptions);

	char	   *format = NULL;
	CopyHeaderChoice header = false;
	char	   *delimiter = NULL;
	char	   *quote = NULL;
	char	   *escape = NULL;
	char	   *nullStr = NULL;
	char	   *newLineStr = NULL;
	Node	   *forceQuote = NULL;
	bool		nullPadding = false;

	bool		hasHeader = false;
	bool		hasQuote = false;
	bool		hasEscape = false;
	bool		hasSkip = false;
	bool		hasNullPadding = false;

	if (!autoDetect)
	{
		/* when auto_detect = false, use PostgreSQL's defaults */
		format = "csv";
		header = COPY_HEADER_FALSE;
		hasHeader = true;
		delimiter = ",";
		quote = "\"";
		escape = "\"";
		newLineStr = "\\n";
		nullStr = "";
		forceQuote = NULL;

		/* not exposed to user */
		hasSkip = true;
	}

	/* user-defined values */
	ListCell   *optionCell = NULL;

	foreach(optionCell, inputOptions)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, "header") == 0)
		{
			header = GetCopyHeaderChoice(option, true);
			hasHeader = true;
		}
		else if (strcmp(option->defname, "delimiter") == 0)
		{
			delimiter = defGetString(option);
		}
		else if (strcmp(option->defname, "quote") == 0)
		{
			quote = defGetString(option);
			hasQuote = true;
		}
		else if (strcmp(option->defname, "escape") == 0)
		{
			escape = defGetString(option);
			hasEscape = true;
		}
		else if (strcmp(option->defname, "new_line") == 0)
		{
			newLineStr = defGetString(option);
		}
		else if (strcmp(option->defname, "null") == 0)
		{
			nullStr = defGetString(option);
		}
		else if (strcmp(option->defname, "null_padding") == 0)
		{
			nullPadding = defGetBoolean(option);
			hasNullPadding = true;
		}
		else if (strcmp(option->defname, "force_quote") == 0)
		{
			forceQuote = copyObject(option->arg);
		}
	}

	if (!hasEscape && hasQuote)
	{
		/* PostgreSQL defaults to escape being the same as quote */
		escape = quote;
	}

	/* merged values */
	List	   *options = NIL;

	options = lappend(options,
					  makeDefElem("format", (Node *) makeString(format), -1));

	if (hasHeader)
	{
		int			headerInt = header == COPY_HEADER_FALSE ? 0 : 1;

		options = lappend(options,
						  makeDefElem("header", (Node *) makeBoolean(headerInt), -1));
	}

	if (delimiter != NULL)
		options = lappend(options,
						  makeDefElem("delimiter", (Node *) makeString(delimiter), -1));
	if (quote != NULL)
		options = lappend(options,
						  makeDefElem("quote", (Node *) makeString(quote), -1));

	if (escape != NULL)
		options = lappend(options,
						  makeDefElem("escape", (Node *) makeString(escape), -1));

	if (nullStr != NULL)
		options = lappend(options,
						  makeDefElem("null", (Node *) makeString(nullStr), -1));

	if (hasNullPadding)
		options = lappend(options,
						  makeDefElem("null_padding", (Node *) makeBoolean(nullPadding), -1));

	if (forceQuote != NULL)
		options = lappend(options,
						  makeDefElem("force_quote", forceQuote, -1));

	if (newLineStr != NULL)
		options = lappend(options,
						  makeDefElem("new_line", (Node *) makeString(newLineStr), -1));

	if (hasSkip)
		options = lappend(options,
						  makeDefElem("skip", (Node *) makeInteger(0), -1));

	return options;

}


/*
 * Extract a CopyHeaderChoice value from a DefElem.  This is like
 * defGetBoolean() but also accepts the special value "match".
 *
 * Copied from defGetCopyHeaderChoice in PostgreSQL.
 */
CopyHeaderChoice
GetCopyHeaderChoice(DefElem *def, bool is_from)
{
	/*
	 * If no parameter value given, assume "true" is meant.
	 */
	if (def->arg == NULL)
		return COPY_HEADER_TRUE;

	/*
	 * Allow 0, 1, "true", "false", "on", "off", or "match".
	 */
	switch (nodeTag(def->arg))
	{
		case T_Integer:
			switch (intVal(def->arg))
			{
				case 0:
					return COPY_HEADER_FALSE;
				case 1:
					return COPY_HEADER_TRUE;
				default:
					/* otherwise, error out below */
					break;
			}
			break;
		default:
			{
				char	   *sval = defGetString(def);

				/*
				 * The set of strings accepted here should match up with the
				 * grammar's opt_boolean_or_string production.
				 */
				if (pg_strcasecmp(sval, "true") == 0)
					return COPY_HEADER_TRUE;
				if (pg_strcasecmp(sval, "false") == 0)
					return COPY_HEADER_FALSE;
				if (pg_strcasecmp(sval, "on") == 0)
					return COPY_HEADER_TRUE;
				if (pg_strcasecmp(sval, "off") == 0)
					return COPY_HEADER_FALSE;
				if (pg_strcasecmp(sval, "match") == 0)
				{
					if (!is_from)
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("cannot use \"%s\" with HEADER in COPY TO",
										sval)));
					return COPY_HEADER_MATCH;
				}
			}
			break;
	}
	ereport(ERROR,
			(errcode(ERRCODE_SYNTAX_ERROR),
			 errmsg("%s requires a Boolean value or \"match\"",
					def->defname)));
	return COPY_HEADER_FALSE;	/* keep compiler quiet */
}


/*
 * HasAutoDetect returns whether the auto_detect option is true.
 */
bool
HasAutoDetect(List *options)
{
	bool		autoDetect = false;

	ListCell   *optionCell = NULL;

	foreach(optionCell, options)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, "auto_detect") == 0)
		{
			autoDetect = defGetBoolean(option);
			break;
		}
	}

	return autoDetect;
}
