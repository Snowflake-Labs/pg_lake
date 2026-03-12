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

#include "commands/defrem.h"
#include "utils/memutils.h"
#include "pg_lake/parsetree/options.h"


/*
 * HasOption determines whether an option with a given name exists.
 */
bool
HasOption(List *options, char *optionName)
{
	ListCell   *optionCell = NULL;
	bool		found = false;

	foreach(optionCell, options)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, optionName) == 0)
		{
			if (found)
			{
				errorConflictingDefElem(option, NULL);
			}

			found = true;

			/* keep iterating to error on duplicates */
		}
	}

	return found;
}


/*
 * GetOption returns an option with the given name, or NULL if
 * no option can be found.
 */
DefElem *
GetOption(List *options, char *optionName)
{
	ListCell   *optionCell = NULL;
	DefElem    *foundOption = NULL;

	foreach(optionCell, options)
	{
		DefElem    *option = lfirst(optionCell);

		if (strcmp(option->defname, optionName) == 0)
		{
			if (foundOption != NULL)
			{
				errorConflictingDefElem(option, NULL);
			}

			foundOption = option;

			/* keep iterating to error on duplicates */
		}
	}

	return foundOption;
}


/*
* GetStringOption returns the value of the given option as a string.
*/
char *
GetStringOption(List *options, char *optionName, bool errorOnMissing)
{
	DefElem    *option = GetOption(options, optionName);

	if (option == NULL && errorOnMissing)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("%s option not found", optionName)));
	else if (option == NULL)
		return NULL;

	return defGetString(option);
}


/*
 * GetBoolOption returns the value of a boolean option.
 */
bool
GetBoolOption(List *options, char *optionName, bool defaultValue)
{
	DefElem    *option = GetOption(options, optionName);

	if (option == NULL)
		return defaultValue;

	return defGetBoolean(option);
}
