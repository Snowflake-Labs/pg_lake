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
#include "fmgr.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/cleanup/in_progress_files.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/parallel_command.h"
#include "pg_lake/util/s3_reader_utils.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/util/rel_utils.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"


/*
 * ScheduledUpload represents an upload to be performed by FinishAllUpload.
 */
typedef struct ScheduledUpload
{
	char		remoteUrl[MAX_S3_PATH_LENGTH];
	char		localFile[MAXPGPATH];
}			ScheduledUpload;


static char *CopyLocalFileToS3Command(char *localFileUri, char *s3Uri);
static void ScheduleFileUpload(char *localFile, char *remoteUrl);
static void InitUploadScheduling(void);
static void ResetPendingUploads(void);

static MemoryContext UploadSchedulingContext = NULL;
static HTAB *PendingUploads = NULL;

/* pg_lake_engine.max_parallel_file_uploads setting */
int			MaxParallelFileUploads = DEFAULT_MAX_PARALLEL_FILE_UPLOADS;


/*
 * ScheduleFileCopyToS3WithCleanup schedules a file upload for later execution, and also ensures
 * that the file is deleted in case of abort.
 */
void
ScheduleFileCopyToS3WithCleanup(char *localFilePath, char *s3Uri, bool autoDeleteRecord)
{
	bool		isPrefix = false;

	InsertInProgressFileRecordExtended(s3Uri, isPrefix, autoDeleteRecord);

	ScheduleFileUpload(localFilePath, s3Uri);
}


/*
 * CopyLocalFileToS3 copies the content of a local file to an S3 bucket.
 *
 * Note: If the transaction rolls back this file will not be cleaned up.
 */
void
CopyLocalFileToS3(char *localFilePath, char *s3Uri)
{
	ExecuteCommandInPGDuck(CopyLocalFileToS3Command(localFilePath, s3Uri));
}


/*
* CopyLocalFileToS3Command returns the SQL command to copy
* the content of a local JSON file to an S3 bucket.
*/
static char *
CopyLocalFileToS3Command(char *localFileUri, char *s3Uri)
{
	StringInfoData command;

	initStringInfo(&command);

	appendStringInfo(&command, "SELECT * FROM pg_lake_copy_file(%s,%s);",
					 quote_literal_cstr(localFileUri), quote_literal_cstr(s3Uri));

	return command.data;
}


/*
 * ScheduleFileUpload schedules a file for being uploaded into object storage
 * when FinishAllUploads is called.
 */
static void
ScheduleFileUpload(char *localFile, char *remoteUrl)
{
	InitUploadScheduling();

	bool		found = false;
	ScheduledUpload *upload = hash_search(PendingUploads, remoteUrl, HASH_ENTER, &found);

	if (found)
		elog(ERROR, "%s scheduled for upload twice", remoteUrl);

	strlcpy(upload->localFile, localFile, MAXPGPATH);
}


/*
 * InitUploadScheduling creates a memory context used to schedule
 * uploads that happen at a later time in the transaction (read: commit)
 * and a hash to track them.
 */
static void
InitUploadScheduling(void)
{
	if (PendingUploads != NULL)
		return;

	/* create a memory context that lasts until the end of the transaction */
	UploadSchedulingContext = AllocSetContextCreate(TopTransactionContext,
													"Upload scheduler",
													ALLOCSET_DEFAULT_SIZES);

	/* reset PendingUploads on abort */
	MemoryContextCallback *cb = MemoryContextAllocZero(UploadSchedulingContext,
													   sizeof(MemoryContextCallback));

	cb->func = (MemoryContextCallbackFunction) ResetPendingUploads;
	cb->arg = NULL;
	MemoryContextRegisterResetCallback(UploadSchedulingContext, cb);

	/* create a URL -> local file hash */
	HASHCTL		hashCtl;

	memset(&hashCtl, 0, sizeof(hashCtl));
	hashCtl.keysize = MAX_S3_PATH_LENGTH;
	hashCtl.entrysize = sizeof(ScheduledUpload);
	hashCtl.hcxt = UploadSchedulingContext;

	PendingUploads = hash_create("scheduled uploads", 32, &hashCtl,
								 HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
}


/*
 * ResetPendingUploads prevents PendingUploads and UploadSchedulingContext containing
 * dangling pointers when the memory context is reset.
 */
static void
ResetPendingUploads(void)
{
	PendingUploads = NULL;
	UploadSchedulingContext = NULL;
}


/*
 * FinishAllUploads completes all of the pending uploads in parallel using
 * a state machine approach.
 *
 * Each upload connection progresses through states independently, with up to
 * pg_lake_engine.max_parallel_file_uploads (default 8) connections active
 * concurrently.
 */
void
FinishAllUploads(void)
{
	if (PendingUploads == NULL)
		return;

	/* build command list */
	List	   *commandList = NIL;
	ScheduledUpload *upload = NULL;

	HASH_SEQ_STATUS status;

	hash_seq_init(&status, PendingUploads);

	while ((upload = hash_seq_search(&status)) != NULL)
	{
		char	   *command = CopyLocalFileToS3Command(upload->localFile,
													   upload->remoteUrl);

		commandList = lappend(commandList, command);
	}

	ExecuteCommandsInParallelInPGDuck(commandList, MaxParallelFileUploads);

	MemoryContextDelete(UploadSchedulingContext);
}


/*
 * GetPendingUploadLocalPath returns the local path of a remote URL, if any.
 */
char *
GetPendingUploadLocalPath(const char *remoteUrl)
{
	if (PendingUploads == NULL)
		return NULL;

	char	   *localFile = NULL;
	bool		found = false;

	ScheduledUpload *upload =
		hash_search(PendingUploads, remoteUrl, HASH_FIND, &found);

	if (found)
		localFile = upload->localFile;

	return localFile;
}
