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
 * Infrastructure for automatically adding other extensions to
 * shared_preload_libraries if they request it in their .control
 * file via the #!shared_preload_libraries comment.
 */
#include <dirent.h>

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"

#include "common/string.h"
#include "pg_extension_base/extension_control_file.h"
#include "pg_extension_base/pg_compat.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "storage/fd.h"
#include "utils/builtins.h"
#include "utils/conffiles.h"
#include "utils/guc.h"

/* PreloadLibrary represents a library to preload */
typedef struct PreloadLibrary
{
	/* name of the extension */
	char	   *extensionName;

	/* name of the library */
	char	   *libraryName;
}			PreloadLibrary;

/* ExtensionControlFileEntry represents the control file of one extension */
typedef struct ExtensionControlFileEntry
{
	/* name of the extension */
	char	   *extensionName;

	/* path of the .control file of the extension */
	char	   *filename;
}			ExtensionControlFileEntry;

/* internal functions */
static List *FindAllPreloadLibraries(void);
static List *FindAllExtensionControlFiles(void);
static bool HasExtensionControlFile(List *controlFiles, const char *extensionName);
static List *FindExtensionPreloadLibraryNames(ExtensionControlFileEntry * controlFile);
static List *AddPreloadLibrary(List *libraries, PreloadLibrary * preloadLibrary);

/* SQL-callable functions */
PG_FUNCTION_INFO_V1(pg_extension_base_list_preload_libraries);


/*
 * PgExtensionBasePreloadLibraries preloads the libraries of extensions that
 * have asked for this in their .control file.
 */
void
PgExtensionBasePreloadLibraries(void)
{
	List	   *libraries = FindAllPreloadLibraries();
	ListCell   *libraryCell = NULL;

	foreach(libraryCell, libraries)
	{
		PreloadLibrary *library = lfirst(libraryCell);

		ereport(LOG, (errmsg("pg_extension_base: preloading %s", library->libraryName)));

		bool		restricted = false;

		load_file(library->libraryName, restricted);
	}
}


/*
 * FindAllPreloadLibraries finds all libraries that extensions want preloaded.
 */
static List *
FindAllPreloadLibraries(void)
{
	List	   *libraries = NIL;
	ListCell   *controlFileCell = NULL;

	foreach(controlFileCell, FindAllExtensionControlFiles())
	{
		ExtensionControlFileEntry *controlFile = lfirst(controlFileCell);

		List	   *libraryNames = FindExtensionPreloadLibraryNames(controlFile);

		if (libraryNames == NIL)
		{
			/* no shared_preload_libraries requested */
			continue;
		}

		ListCell   *libraryNameCell = NULL;

		foreach(libraryNameCell, libraryNames)
		{
			char	   *libraryName = lfirst(libraryNameCell);

			PreloadLibrary *preloadLibrary = palloc0(sizeof(PreloadLibrary));

			preloadLibrary->extensionName = controlFile->extensionName;
			preloadLibrary->libraryName = libraryName;

			libraries = AddPreloadLibrary(libraries, preloadLibrary);
		}
	}

	return libraries;
}


/*
 * FindAllExtensionControlFiles returns the control file of every extension
 * that is installed, except for pg_extension_base itself.
 *
 * Extensions can be installed in any of the directories that PostgreSQL
 * searches for control files, and the same extension may appear in several
 * of them. We only return the control file that PostgreSQL would use, which
 * is the one in the first directory that has it.
 */
static List *
FindAllExtensionControlFiles(void)
{
	List	   *controlFiles = NIL;
	ListCell   *directoryCell = NULL;

	foreach(directoryCell, GetExtensionControlDirectories())
	{
		char	   *location = lfirst(directoryCell);
		DIR		   *extensionDirectory = AllocateDir(location);
		struct dirent *dirEntry;

		if (extensionDirectory == NULL && errno == ENOENT)
		{
			/* directory does not exist, so it has no extensions */
			continue;
		}

		while ((dirEntry = ReadDir(extensionDirectory, location)) != NULL)
		{
			if (!IsExtensionControlFilename(dirEntry->d_name))
			{
				continue;
			}

			/* extract extension name from 'name.control' filename */
			char	   *extensionName = pstrdup(dirEntry->d_name);

			/* strip the file suffix */
			char	   *dotPosition = strrchr(extensionName, '.');

			*dotPosition = '\0';

			if (strcmp(extensionName, "pg_extension_base") == 0)
			{
				/* we can skip ourselves, we're already being loaded */
				continue;
			}

			if (HasExtensionControlFile(controlFiles, extensionName))
			{
				/* an earlier directory has this extension, which wins */
				continue;
			}

			ExtensionControlFileEntry *controlFile = palloc0(sizeof(ExtensionControlFileEntry));

			controlFile->extensionName = extensionName;
			controlFile->filename = psprintf("%s/%s", location, dirEntry->d_name);

			controlFiles = lappend(controlFiles, controlFile);
		}

		FreeDir(extensionDirectory);
	}

	return controlFiles;
}


/*
 * HasExtensionControlFile returns whether the given list already contains a
 * control file for the given extension.
 */
static bool
HasExtensionControlFile(List *controlFiles, const char *extensionName)
{
	ListCell   *controlFileCell = NULL;

	foreach(controlFileCell, controlFiles)
	{
		ExtensionControlFileEntry *controlFile = lfirst(controlFileCell);

		if (strcmp(controlFile->extensionName, extensionName) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * FindExtensionPreloadLibraryNames finds which library the given extension
 * wants preloaded on start-up.
 *
 * There are two ways to specify preloading in the extension.control file.
 *
 * 1) Implicitly load the module_pathname library:
 *    #!shared_preload_libraries
 *
 * 2) Explicitly list the libraries:
 *    #!shared_preload_libraries = 'common,$libdir/foo'
 *
 * The second case is useful if the extension depends on a separate
 * module.
 */
static List *
FindExtensionPreloadLibraryNames(ExtensionControlFileEntry * controlFile)
{
	char	   *extensionName = controlFile->extensionName;
	char	   *filename = controlFile->filename;

	FILE	   *file;

	if ((file = AllocateFile(filename, "r")) == NULL)
	{
		if (errno == ENOENT)
		{
			/* control file disappeared after we listed the directory */
			return NIL;
		}
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open extension control file \"%s\": %m",
						filename)));
	}

	bool		preloadModulePathname = false;
	List	   *libraryNames = NULL;

	StringInfoData line;

	initStringInfo(&line);

	/* look for the shared_preload_libraries line */
	while (pg_get_line_buf(file, &line))
	{
		const char *prefix = "#!shared_preload_libraries";
		int			prefixLength = strlen(prefix);

		if (strncmp(prefix, line.data, prefixLength) == 0)
		{
			char	   *remainder = line.data + prefixLength;

			/* might this be #!shared_preload_libraries = '...' ? */
			char	   *equalsPosition = strchr(remainder, '=');

			if (equalsPosition == NULL)
			{
				/* no value specified, find module_pathname below */
				preloadModulePathname = true;
				break;
			}

			remainder = equalsPosition + 1;

			char	   *openingQuotePosition = strchr(remainder, '\'');

			if (openingQuotePosition == NULL)
			{
				ereport(WARNING, (errmsg("pg_extension_base: missing quotes in preload line "
										 "in %s.control: %s",
										 extensionName, line.data)));
				/* proceed without loading */
				break;
			}

			char	   *valueStart = openingQuotePosition + 1;

			char	   *closingQuotePosition = strchr(valueStart, '\'');

			if (closingQuotePosition == NULL)
			{
				ereport(WARNING, (errmsg("pg_extension_base: missing closing quote in preload "
										 "line in %s.control: %s",
										 extensionName, line.data)));
				/* proceed without loading */
				break;
			}

			if (closingQuotePosition == valueStart)
			{
				ereport(WARNING, (errmsg("pg_extension_base: preload line in %s.control is "
										 "empty: %s ",
										 extensionName, line.data)));
				/* proceed without loading */
				break;
			}

			/* terminate the string at the closing quote */
			*closingQuotePosition = '\0';

			/* split the string by , and add each part */
			char	   *libraryName = NULL;

			while ((libraryName = strsep(&valueStart, ",")))
			{
				libraryNames = lappend(libraryNames, libraryName);
			}

			break;
		}
	}

	/* now look for the library name */
	if (preloadModulePathname)
	{
		char	   *libraryName = NULL;

		/* start from the beginning to parse settings */
		rewind(file);

		/* parse all the settings */
		ConfigVariable *head = NULL;
		ConfigVariable *tail = NULL;

		ParseConfigFp(file, filename, CONF_FILE_START_DEPTH, ERROR, &head, &tail);

		/* find the module_pathname */
		for (ConfigVariable *item = head; item != NULL; item = item->next)
		{
			if (strcmp(item->name, "module_pathname") == 0)
			{
				libraryName = pstrdup(item->value);
			}
		}

		if (libraryName == NULL)
		{
			/* if no module_pathname specified, assume the extension name */
			libraryName = extensionName;
		}

		libraryNames = lappend(libraryNames, libraryName);

		FreeConfigVariables(head);
	}

	FreeFile(file);

	return libraryNames;
}


/*
 * AddPreloadLibrary adds the given preload library to the list,
 * unless the library already appears.
 */
static List *
AddPreloadLibrary(List *libraries, PreloadLibrary * preloadLibrary)
{
	ListCell   *libraryCell = NULL;

	foreach(libraryCell, libraries)
	{
		PreloadLibrary *existingLibrary = lfirst(libraryCell);

		if (strcmp(preloadLibrary->libraryName, existingLibrary->libraryName) == 0)
		{
			return libraries;
		}
	}

	return lappend(libraries, preloadLibrary);
}


/*
 * pg_extension_base_list_preload_libraries returns the list of extensions
 * whose module is preloaded on start-up.
 */
Datum
pg_extension_base_list_preload_libraries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;

	InitMaterializedSRF(fcinfo, 0);

	List	   *libraries = FindAllPreloadLibraries();
	ListCell   *libraryCell = NULL;

	foreach(libraryCell, libraries)
	{
		PreloadLibrary *library = lfirst(libraryCell);

		bool		nulls[] = {false, false};
		Datum		values[] = {
			CStringGetTextDatum(library->extensionName),
			CStringGetTextDatum(library->libraryName)
		};

		tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
	}

	PG_RETURN_VOID();
}
