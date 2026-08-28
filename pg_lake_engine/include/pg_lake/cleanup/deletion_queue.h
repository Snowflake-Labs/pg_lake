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

#include "postgres.h"
#include "nodes/pg_list.h"
#include "datatype/timestamp.h"

#include "pg_lake/pgduck/remote_storage.h"

/* deletion batches issued per drain pass, see PER_LOOP_FILE_CLEANUP_LIMIT */
#define FILE_DELETION_BATCHES_PER_LOOP 5

/*
 * Upper bound on the queue rows claimed per drain pass. Each pass runs in its
 * own transaction, so this is what makes a pass end at all: the caller gets
 * control back and can let the rest of the vacuum cycle -- including the
 * object-store catalog export -- have a turn.
 *
 * It is a row count, not a duration. Two row kinds still cost unbounded work,
 * so a pass holding this many of them can run for an unbounded time with the
 * claim's FOR UPDATE held on all of them: a resolve_metadata row is a metadata
 * walk over a dropped table, and an is_prefix row is a listing of everything
 * under a prefix. Bounding those is tracked in #538.
 *
 * A caller that has its own budget for the whole cycle asks for fewer rows via
 * the maxRecords argument of GetDeletionQueueRecords.
 */
#define PER_LOOP_FILE_CLEANUP_LIMIT \
	(FILE_DELETION_BATCH_SIZE * FILE_DELETION_BATCHES_PER_LOOP)

/* managed by a GUC */
extern int	OrphanedFileRetentionPeriod;
extern int	VacuumFileRemoveMaxRetries;
extern int	VacuumFileRemoveRetryInterval;

extern PGDLLEXPORT List *GetDeletionQueueRecords(Oid relationId, bool isFull, int maxRecords);
extern PGDLLEXPORT bool RemoveDeletionQueueRecords(List *deletionQueueRecords, bool isVerbose,
												   int *filesRemoved);
extern PGDLLEXPORT void InsertDeletionQueueRecord(char *path, Oid relationId, TimestampTz deleteAfterTime);
extern PGDLLEXPORT void InsertPrefixDeletionRecord(char *path, TimestampTz orphanedAt);
extern PGDLLEXPORT void InsertMetadataResolveRecord(char *metadataPath, Oid relationId,
													TimestampTz orphanedAt, char *fallbackPrefix);
extern PGDLLEXPORT void InsertDeletionQueueRecordExtended(char *path, Oid relationId, TimestampTz orphanedAt,
														  bool isPrefix, bool resolveMetadata,
														  char *fallbackPrefix);
