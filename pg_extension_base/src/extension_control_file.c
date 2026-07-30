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
 * Functions for dealing with extension control files.
 */
#include "postgres.h"
#include "miscadmin.h"

#include <sys/stat.h>

#include "commands/extension.h"
#include "pg_extension_base/extension_control_file.h"
#include "storage/fd.h"
#include "utils/conffiles.h"
#include "utils/guc.h"
#include "utils/varlena.h"


/*
 * IsExtensionControlFilename determines whether the given path
 * ends in .control.
 */
bool
IsExtensionControlFilename(const char *filename)
{
	const char *extension = strrchr(filename, '.');

	return (extension != NULL) && (strcmp(extension, ".control") == 0);
}


/*
 * GetExtensionControlDirectories returns the list of directories that can
 * contain .control files for extensions, in the order in which PostgreSQL
 * searches them.
 *
 * On PostgreSQL 18 and above, the directories come from the
 * extension_control_path setting, which we interpret the same way as
 * get_extension_control_directories() in the PostgreSQL source: the special
 * value $system refers to the extension directory in the installation's
 * share directory, while every other entry gets /extension appended.
 *
 * Before PostgreSQL 18 there is no such setting and control files can only
 * live in the share directory.
 */
List *
GetExtensionControlDirectories(void)
{
	char		sharepath[MAXPGPATH];

	get_share_path(my_exec_path, sharepath);

	char	   *systemDirectory = psprintf("%s/extension", sharepath);

#if PG_VERSION_NUM >= 180000
	if (Extension_control_path == NULL || Extension_control_path[0] == '\0')
	{
		return list_make1(systemDirectory);
	}

	List	   *directories = NIL;
	char	   *remainder = Extension_control_path;

	for (;;)
	{
		char	   *separator = first_path_var_separator(remainder);
		int			entryLength = separator != NULL ?
			(int) (separator - remainder) : (int) strlen(remainder);
		char	   *entry = pnstrdup(remainder, entryLength);
		char	   *directory = NULL;

		if (strcmp(entry, "$system") == 0)
		{
			directory = systemDirectory;
		}
		else
		{
			directory = psprintf("%s/extension", entry);
		}

		pfree(entry);

		canonicalize_path(directory);
		directories = lappend(directories, directory);

		if (remainder[entryLength] == '\0')
		{
			break;
		}

		/* skip past the separator and continue with the next entry */
		remainder += entryLength + 1;
	}

	return directories;
#else
	return list_make1(systemDirectory);
#endif
}


/*
 * GetExtensionControlFilename returns the .control file path for a given
 * extension, or NULL if the extension does not have a control file in any
 * of the directories that PostgreSQL searches.
 */
char *
GetExtensionControlFilename(const char *extname)
{
	ListCell   *directoryCell = NULL;

	foreach(directoryCell, GetExtensionControlDirectories())
	{
		char	   *directory = lfirst(directoryCell);
		char	   *filename = psprintf("%s/%s.control", directory, extname);
		struct stat statBuffer;

		if (stat(filename, &statBuffer) == 0)
		{
			return filename;
		}

		pfree(filename);
	}

	return NULL;
}


/*
 * GetExtensionDependencyList returns a list of extension names from
 * the requires line of the extension control file.
 *
 * Returns an empty list if the extension is not installed, in which case
 * we leave it to PostgreSQL to report the error.
 */
List *
GetExtensionDependencyList(char *extensionName)
{
	List	   *requires = NIL;

	char	   *filename = GetExtensionControlFilename(extensionName);

	if (filename == NULL)
	{
		return NIL;
	}

	FILE	   *file;

	if ((file = AllocateFile(filename, "r")) == NULL)
	{
		if (errno == ENOENT)
		{
			/* control file disappeared after we found it */
			return NIL;
		}
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open extension control file \"%s\": %m",
						filename)));
	}

	/* parse all the settings */
	ConfigVariable *head = NULL;
	ConfigVariable *tail = NULL;

	ParseConfigFp(file, filename, CONF_FILE_START_DEPTH, ERROR, &head, &tail);

	/* find the requires line */
	for (ConfigVariable *item = head; item != NULL; item = item->next)
	{
		if (strcmp(item->name, "requires") == 0)
		{
			char	   *requiresLine = pstrdup(item->value);

			/* Parse string into list of identifiers */
			if (!SplitIdentifierString(requiresLine, ',', &requires))
			{
				/* syntax error in name list */
				ereport(ERROR,
						(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
						 errmsg("parameter \"%s\" must be a list of extension names",
								item->name)));
			}
		}
	}

	FreeConfigVariables(head);
	FreeFile(file);

	return requires;
}
