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
#include "funcapi.h"
#include "libpq-fe.h"
#include "miscadmin.h"

#include "pg_lake/iceberg/api/snapshot.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/util/string_utils.h"

#include "utils/lsyscache.h"
#include "utils/uuid.h"

static int64_t GenerateIcebergSnapshotId(void);

/*
 * IsCurrentSnapshot checks if the given snapshot is the current snapshot.
 */
bool
IsCurrentSnapshot(IcebergTableMetadata * metadata, IcebergSnapshot * snapshot)
{
	return snapshot->snapshot_id == metadata->current_snapshot_id;
}

/*
 * FetchSnapshotsFromTableMetadata fetches snapshots, which is filtered by the callback,
 * from given table metadata.
 */
List *
FetchSnapshotsFromTableMetadata(IcebergTableMetadata * metadata, SnapshotPredicateFn snapshotPredicateFn)
{
	if (metadata == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table metadata is NULL")));
	}

	List	   *snapshots = NIL;

	int			snapshotIdx = 0;

	for (snapshotIdx = 0; snapshotIdx < metadata->snapshots_length; snapshotIdx++)
	{
		IcebergSnapshot *snapshot = &metadata->snapshots[snapshotIdx];

		if (snapshotPredicateFn == NULL || snapshotPredicateFn(metadata, snapshot))
		{
			snapshots = lappend(snapshots, snapshot);
		}
	}

	return snapshots;
}

/*
 * GetCurrentSnapshot gets the current snapshot from given table metadata.
 */
IcebergSnapshot *
GetCurrentSnapshot(IcebergTableMetadata * metadata, bool missingOk)
{
	if (metadata == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("table metadata is NULL")));
	}

	List	   *snapshots = FetchSnapshotsFromTableMetadata(metadata, IsCurrentSnapshot);

	/* bogus metadata */
	if (list_length(snapshots) > 1)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("Multiple current snapshots found")));
	}

	if (list_length(snapshots) == 0 && !missingOk)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("No current snapshot found")));
	}
	else if (list_length(snapshots) == 0)
	{
		return NULL;
	}

	return linitial(snapshots);
}


/*
* GetIcebergSnapshotViaId gets the snapshot with the given snapshot
* id from the given table metadata. If the snapshot is not found,
* an error is thrown.
*/
IcebergSnapshot *
GetIcebergSnapshotViaId(IcebergTableMetadata * metadata, uint64_t snapshotId, bool missingOk)
{
	int			snapshotIndex = 0;

	for (snapshotIndex = 0; snapshotIndex < metadata->snapshots_length; snapshotIndex++)
	{
		IcebergSnapshot *snapshot = &metadata->snapshots[snapshotIndex];

		if (snapshot->snapshot_id == snapshotId)
		{
			return snapshot;
		}
	}

	if (missingOk)
		return NULL;

	ereport(ERROR,
			(errcode(ERRCODE_INTERNAL_ERROR),
			 errmsg("snapshot with id %" PRIu64 " not found", snapshotId)));

}


/*
 * CreateNewIcebergSnapshot creates a new snapshot for the given table metadata.
 */
IcebergSnapshot *
CreateNewIcebergSnapshot(IcebergTableMetadata * metadata)
{
	IcebergSnapshot *newSnapshot = palloc0(sizeof(IcebergSnapshot));

	bool		missingOk = true;
	IcebergSnapshot *currentSnapshot = GetCurrentSnapshot(metadata, missingOk);

	if (currentSnapshot == NULL)
	{
		/* first time we are creating a snapshot for the table */
		newSnapshot->snapshot_id = GenerateIcebergSnapshotId();
		newSnapshot->parent_snapshot_id = -1;
		newSnapshot->sequence_number = metadata->last_sequence_number;
		newSnapshot->timestamp_ms = PostgresTimestampToIcebergTimestampMs();
		newSnapshot->schema_id = metadata->current_schema_id;
		newSnapshot->schema_id_set = true;
	}
	else
	{
		newSnapshot->snapshot_id = GenerateIcebergSnapshotId();
		newSnapshot->parent_snapshot_id = currentSnapshot->snapshot_id;
		newSnapshot->sequence_number = metadata->last_sequence_number;
		newSnapshot->timestamp_ms = PostgresTimestampToIcebergTimestampMs();
		newSnapshot->schema_id = currentSnapshot->schema_id;
		newSnapshot->schema_id_set = true;
	}

	return newSnapshot;
}

/*
* Imitates the UUID generation logic from Spark
*/
static int64_t
GenerateIcebergSnapshotId(void)
{
	pg_uuid_t  *uuid = GeneratePGUUID();

	/* Extract the most and least significant bits */
	uint64_t	mostSignificantBits = ((uint64_t) uuid->data[0] << 56) |
		((uint64_t) uuid->data[1] << 48) |
		((uint64_t) uuid->data[2] << 40) |
		((uint64_t) uuid->data[3] << 32) |
		((uint64_t) uuid->data[4] << 24) |
		((uint64_t) uuid->data[5] << 16) |
		((uint64_t) uuid->data[6] << 8) |
		(uint64_t) uuid->data[7];

	uint64_t	leastSignificantBits = ((uint64_t) uuid->data[8] << 56) |
		((uint64_t) uuid->data[9] << 48) |
		((uint64_t) uuid->data[10] << 40) |
		((uint64_t) uuid->data[11] << 32) |
		((uint64_t) uuid->data[12] << 24) |
		((uint64_t) uuid->data[13] << 16) |
		((uint64_t) uuid->data[14] << 8) |
		(uint64_t) uuid->data[15];

	/* Perform XOR and ensure the result is positive */
	int64_t		snapshotId = (mostSignificantBits ^ leastSignificantBits) & INT64_MAX;

	return snapshotId;
}
