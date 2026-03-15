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
 * GetExtensionControlDirectory returns the directory containing
 * .control files for extensions.
 */
char *
GetExtensionControlDirectory(void)
{
	char		sharepath[MAXPGPATH];
	char	   *result;

	get_share_path(my_exec_path, sharepath);
	result = (char *) palloc(MAXPGPATH);
	snprintf(result, MAXPGPATH, "%s/extension", sharepath);

	return result;
}


/*
 * GetExtensionControlFilename returns the .control file path
 * for a given extension.
 */
char *
GetExtensionControlFilename(const char *extname)
{
	char		sharepath[MAXPGPATH];
	char	   *result;

	get_share_path(my_exec_path, sharepath);
	result = (char *) palloc(MAXPGPATH);
	snprintf(result, MAXPGPATH, "%s/extension/%s.control",
			 sharepath, extname);

	return result;
}


/*
 * GetExtensionDependencyList returns a list of extension names from
 * the requires line of the extension control file.
 */
List *
GetExtensionDependencyList(char *extensionName)
{
	List	   *requires = NIL;

	char	   *filename = GetExtensionControlFilename(extensionName);

	FILE	   *file;

	if ((file = AllocateFile(filename, "r")) == NULL)
	{
		if (errno == ENOENT)
		{
			/* missing control file indicates extension is not installed */
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("extension \"%s\" is not available", extensionName),
					 errdetail("Could not open extension control file \"%s\": %m.",
							   filename),
					 errhint("The extension must first be installed on the system where PostgreSQL is running.")));
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
