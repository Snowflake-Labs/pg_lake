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
 * Functions for reading and writing local files.
 */
#include <unistd.h>
#include <sys/stat.h>

#include "postgres.h"
#include "miscadmin.h"

#include "common/file_utils.h"
#include "pg_lake/storage/local_storage.h"
#include "storage/fd.h"

static void EnsureTempFileCleanup(char *path);


/*
 * GenerateTempFileName generates an appropriate path for a temporary file
 * and ensures the file is deleted if the current memory context is reset.
 *
 * The pattern is included in the file name, which is of the form:
 * $PGDATA/base/pgsql_tmp/pgsql_tmp.<pattern>_<pid>.<counter>
 *
 * which ensures uniqueness (pattern just for recognizability).
 */
char *
GenerateTempFileName(char *pattern, bool ensureCleanup)
{
	static long tempFileCounter = 0;
	char		tempDirPath[MAXPGPATH];

	/* typically points to base/pgsql_tmp */
	TempTablespacePath(tempDirPath, InvalidOid);

	(void) MakePGDirectory(tempDirPath);


	/*
	 * Generate a tempfile name that should be unique within the current
	 * database instance.
	 */
	char	   *filePath = psprintf("%s/%s/%s.%s_%d.%ld",
									DataDir, tempDirPath, PG_TEMP_FILE_PREFIX,
									pattern, MyProcPid, tempFileCounter++);

	if (ensureCleanup)
	{
		EnsureTempFileCleanup(filePath);
	}

	return filePath;
}


/*
 * EnsureTempFileCleanup registers a memory context callback that deletes
 * the file pointed to by the given path.
 */
static void
EnsureTempFileCleanup(char *path)
{
	MemoryContextCallback *cb = MemoryContextAllocZero(CurrentMemoryContext,
													   sizeof(MemoryContextCallback));

	cb->func = (MemoryContextCallbackFunction) DeleteLocalFile;
	cb->arg = path;
	MemoryContextRegisterResetCallback(CurrentMemoryContext, cb);
}


/*
 * DeleteLocalFile deletes the file with the given path.
 */
void
DeleteLocalFile(char *path)
{
	if (unlink(path) < 0)
	{
		if (errno != ENOENT)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not unlink temporary file \"%s\": %m",
							path)));
		}
	}
}

/*
 * GetLocalFileSize returns the size of a local file.
 */
int64
GetLocalFileSize(char *path)
{
	struct stat fileStat;

	if (stat(path, &fileStat) < 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not stat file \"%s\": %m", path)));

	return (int64) fileStat.st_size;
}



/*
* WriteStringToFile writes a string to a file.
*/
void
WriteStringToFile(char *content, FILE *file)
{
	int			contentLength = strlen(content);
	int			writtenLength = fwrite(content, 1, contentLength, file);

	if (writtenLength != contentLength)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to write to file")));
	}

	/*
	 * fwrite may have only filled the stdio buffer, so the flush is where a
	 * write error surfaces. ferror() is sticky, so it also reports one whose
	 * flush already consumed the buffer.
	 */
	if (fflush(file) != 0 || ferror(file))
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("failed to write to file: %m")));
	}
}


/*
 * WriteStringToFilePath writes a string to a given file path.
 */
void
WriteStringToFilePath(char *content, char *localFilePath)
{
	FILE	   *localFile = AllocateFile(localFilePath, PG_BINARY_W);

	if (localFile == NULL)
	{
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not create file \"%s\": %m",
							   localFilePath)));
	}

	WriteStringToFile(content, localFile);

	/* the final fclose() can still fail, e.g. out of space */
	if (FreeFile(localFile) != 0)
		ereport(ERROR, (errcode_for_file_access(),
						errmsg("could not close file \"%s\": %m",
							   localFilePath)));
}
