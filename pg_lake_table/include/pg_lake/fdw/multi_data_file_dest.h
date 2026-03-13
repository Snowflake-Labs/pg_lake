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

#include "pg_lake/copy/copy_format.h"
#include "nodes/pg_list.h"
#include "tcop/dest.h"
#include "utils/relcache.h"

/* by default, we allow temp files to grow up 3GiB */
#define DEFAULT_MAX_WRITE_TEMP_FILE_SIZE_MB (3*1024)

/* pg_lake_table.max_write_temp_file_size_mb setting */
extern PGDLLEXPORT int MaxWriteTempFileSizeMB;

extern PGDLLEXPORT DestReceiver *CreateMultiDataFileDestReceiver(Oid relationId,
																 CopyDataFormat targetFormat,
																 int MaxWriteTempFileSizeMB,
																 int32 partitionSpecId,
																 int64 rowIdStart);
extern PGDLLEXPORT List *GetMultiDataFileDestReceiverModifications(DestReceiver *dest);
