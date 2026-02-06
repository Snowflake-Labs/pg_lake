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

#pragma once

/* we use a multiple of 3 since metadata files always come in triplets */
#define DEFAULT_MAX_PARALLEL_FILE_UPLOADS 12

/* pg_lake_engine.max_parallel_file_uploads */
extern int32 MaxParallelFileUploads;

extern PGDLLEXPORT void ScheduleFileCopyToS3WithCleanup(char *localFilePath, char *s3Uri, bool autoDeleteRecord);
extern PGDLLEXPORT void CopyLocalFileToS3(char *localFilePath, char *s3Uri);
extern PGDLLEXPORT void FinishAllUploads(void);
extern PGDLLEXPORT char *GetPendingUploadLocalPath(const char *remoteUrl);
