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
 * Ids of the data and position-delete files the current top-level
 * transaction added, read by the newFilesOnly catalog query and the
 * append-only commit path.
 *
 * They are kept in backend memory rather than in a temp table: a temp table
 * can be pre-created or dropped (DISCARD TEMP) by the session user, while the
 * reads and writes of it run as the extension owner.
 */
#include "postgres.h"
#include "access/xact.h"
#include "catalog/pg_type_d.h"
#include "utils/array.h"
#include "utils/memutils.h"

#include "pg_lake/fdw/tx_data_file_ids.h"


typedef struct TrackedDataFileId
{
	int64		fileId;
	SubTransactionId subid;
}			TrackedDataFileId;

/* lives in TopTransactionContext, cleared by a reset callback on it */
static TrackedDataFileId * TrackedIds = NULL;
static int	TrackedIdCount = 0;
static int	TrackedIdCapacity = 0;

static void ForgetTrackedDataFileIds(void *arg);


/*
 * TrackAddedDataFileId records that the current (sub)transaction added the
 * file with the given id.
 */
void
TrackAddedDataFileId(int64 fileId)
{
	if (TrackedIds == NULL)
	{
		static MemoryContextCallback resetCallback = {
			.func = ForgetTrackedDataFileIds
		};

		TrackedIdCapacity = 64;
		TrackedIds = MemoryContextAlloc(TopTransactionContext,
										sizeof(TrackedDataFileId) * TrackedIdCapacity);
		MemoryContextRegisterResetCallback(TopTransactionContext, &resetCallback);
	}
	else if (TrackedIdCount == TrackedIdCapacity)
	{
		TrackedIdCapacity *= 2;
		TrackedIds = repalloc_huge(TrackedIds,
								   sizeof(TrackedDataFileId) * TrackedIdCapacity);
	}

	TrackedIds[TrackedIdCount].fileId = fileId;
	TrackedIds[TrackedIdCount].subid = GetCurrentSubTransactionId();
	TrackedIdCount++;
}


/*
 * GetTrackedDataFileIdArray returns the tracked file ids as an int8[].
 */
ArrayType *
GetTrackedDataFileIdArray(void)
{
	Datum	   *fileIdDatums = palloc(sizeof(Datum) * Max(TrackedIdCount, 1));

	for (int idIndex = 0; idIndex < TrackedIdCount; idIndex++)
		fileIdDatums[idIndex] = Int64GetDatum(TrackedIds[idIndex].fileId);

	return construct_array(fileIdDatums, TrackedIdCount, INT8OID,
						   sizeof(int64), FLOAT8PASSBYVAL, TYPALIGN_DOUBLE);
}


/*
 * TrackedDataFileIdsSubXactCallback drops the ids an aborted subtransaction
 * added and hands those of a committed one to its parent.
 *
 * Subtransaction ids only grow along the stack and committed children are
 * handed to their parent, so the ids of the innermost open subtransaction
 * are always at the tail of the array.
 */
void
TrackedDataFileIdsSubXactCallback(SubXactEvent event, SubTransactionId mySubid,
								  SubTransactionId parentSubid, void *arg)
{
	if (event != SUBXACT_EVENT_ABORT_SUB && event != SUBXACT_EVENT_COMMIT_SUB)
		return;

	for (int idIndex = TrackedIdCount - 1;
		 idIndex >= 0 && TrackedIds[idIndex].subid >= mySubid;
		 idIndex--)
	{
		if (event == SUBXACT_EVENT_ABORT_SUB)
			TrackedIdCount = idIndex;
		else
			TrackedIds[idIndex].subid = parentSubid;
	}
}


/*
 * ForgetTrackedDataFileIds runs when TopTransactionContext is reset or
 * deleted at the end of the transaction.
 */
static void
ForgetTrackedDataFileIds(void *arg)
{
	TrackedIds = NULL;
	TrackedIdCount = 0;
	TrackedIdCapacity = 0;
}
