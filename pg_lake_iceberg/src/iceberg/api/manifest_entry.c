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

#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/iceberg/api/manifest_entry.h"
#include "pg_lake/iceberg/api/manifest_list.h"
#include "pg_lake/iceberg/api/snapshot.h"
#include "pg_lake/iceberg/operations/manifest_merge.h"
#include "pg_lake/storage/local_storage.h"
#include "pg_lake/util/s3_writer_utils.h"
#include "pg_lake/util/string_utils.h"

/*
 * IsManifestEntryStatusScannable checks if the given manifest entry is scannable.
 */
bool
IsManifestEntryStatusScannable(IcebergManifestEntry *manifestEntry)
{
	/*
	 * Scans are planned by reading the manifest files for the current
	 * snapshot. Deleted entries in data and delete manifests (those marked
	 * with status "DELETED") are not used in a scan.
	 */
	return manifestEntry->status != ICEBERG_MANIFEST_ENTRY_STATUS_DELETED;
}

/*
 * FetchManifestEntriesFromManifest fetches manifest entries, which are filtered by predicate,
 * from given manifest.
 */
List *
FetchManifestEntriesFromManifest(IcebergManifest *manifest, ManifestEntryPredicateFn manifestEntryPredicateFn)
{
	if (manifest == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("manifest is NULL")));
	}

	List	   *manifestEntries = ReadManifestEntries(manifest->manifest_path);

	List	   *filteredManifestEntries = NIL;

	ListCell   *manifestEntryCell = NULL;

	foreach(manifestEntryCell, manifestEntries)
	{
		IcebergManifestEntry *manifestEntry = lfirst(manifestEntryCell);

		if (manifestEntryPredicateFn == NULL || manifestEntryPredicateFn(manifestEntry))
		{
			filteredManifestEntries = lappend(filteredManifestEntries, manifestEntry);
		}
	}

	return filteredManifestEntries;
}


/*
* UpdateManifestEntryStateToDeleted updates the manifest entry state to deleted.
* It also adjusts the row/file counts in the manifest.
*/
void
UpdateManifestEntryStateToDeleted(IcebergManifest *manifest, IcebergManifestEntry *dataManifestEntry, int64_t snapshotId)
{
	manifest->deleted_rows_count += dataManifestEntry->data_file.record_count;
	manifest->deleted_files_count++;
	if (dataManifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_ADDED)
	{
		manifest->added_rows_count -= dataManifestEntry->data_file.record_count;
		manifest->added_files_count--;
	}
	else if (dataManifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_EXISTING)
	{
		manifest->existing_rows_count -= dataManifestEntry->data_file.record_count;
		manifest->existing_files_count--;
	}

	dataManifestEntry->status = ICEBERG_MANIFEST_ENTRY_STATUS_DELETED;
	dataManifestEntry->snapshot_id = snapshotId;
}

/*
 * SetExistingStatusForOldSnapshotAddedEntries updates status of added entries from old snapshots
 * to EXISTING.
 */
void
SetExistingStatusForOldSnapshotAddedEntries(List *manifestEntries, int64_t currentSnapshotId)
{
	ListCell   *manifestEntryCell = NULL;

	foreach(manifestEntryCell, manifestEntries)
	{
		IcebergManifestEntry *manifestEntry = lfirst(manifestEntryCell);

		if (manifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_ADDED &&
			manifestEntry->snapshot_id != currentSnapshotId)
		{
			manifestEntry->status = ICEBERG_MANIFEST_ENTRY_STATUS_EXISTING;
		}
	}
}

/*
* FindAndAdjustDeletedManifestEntries finds the manifest entries that are marked
* for deletion. It iterates over the removeManifestEntryList and checks
* if the data manifest entries match the entries in the removeManifestEntryList.
* If a match is found, the function updates the manifest entry state to deleted
* and adjusts the row/file counts in the manifest as well as the sequence and
* snapshots. In other words, the entries in the removeManifestEntryList are
* prepared to be created as a new manifest file with the deleted entries.
*/
List *
FindAndAdjustDeletedManifestEntries(IcebergManifest *manifest, List *manifestEntryList,
									List *removeManifestEntryList, int64_t snapshotId,
									bool allEntries)
{
	List	   *deletedManifestEntries = NIL;
	ListCell   *dataCell = NULL;

	foreach(dataCell, manifestEntryList)
	{
		IcebergManifestEntry *dataManifestEntry = lfirst(dataCell);

		/* shortcut for removing all entries, such as TRUNCATE */
		if (allEntries)
		{
			if (dataManifestEntry->status == ICEBERG_MANIFEST_ENTRY_STATUS_DELETED)
			{
				/* already deleted, nothing to do */
				continue;
			}

			UpdateManifestEntryStateToDeleted(manifest, dataManifestEntry, snapshotId);
			deletedManifestEntries = lappend(deletedManifestEntries, dataManifestEntry);
			continue;
		}

		ListCell   *removeCell = NULL;

		foreach(removeCell, removeManifestEntryList)
		{
			IcebergManifestEntry *removeManifestEntry = lfirst(removeCell);

			if (strcmp(dataManifestEntry->data_file.file_path, removeManifestEntry->data_file.file_path) == 0)
			{
				UpdateManifestEntryStateToDeleted(manifest, dataManifestEntry, snapshotId);
				deletedManifestEntries = lappend(deletedManifestEntries, dataManifestEntry);
			}
		}
	}

	return deletedManifestEntries;
}


/*
 * IcebergAvroPhysicalTypeName returns avro physical type name.
 */
const char *
IcebergAvroPhysicalTypeName(IcebergScalarAvroType value_type)
{
	switch (value_type.physical_type)
	{
		case ICEBERG_AVRO_PHYSICAL_TYPE_INT32:
			return "int";
		case ICEBERG_AVRO_PHYSICAL_TYPE_INT64:
			return "long";
		case ICEBERG_AVRO_PHYSICAL_TYPE_STRING:
			return "string";
		case ICEBERG_AVRO_PHYSICAL_TYPE_BINARY:
			return "bytes";
		case ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT:
			return "float";
		case ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE:
			return "double";
		case ICEBERG_AVRO_PHYSICAL_TYPE_BOOL:
			return "boolean";
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected avro physical type")));
	}

	return NULL;
}


/*
 * IcebergAvroLogicalTypeName returns avro logical type name.
 */
const char *
IcebergAvroLogicalTypeName(IcebergScalarAvroType value_type)
{
	switch (value_type.logical_type)
	{
		case ICEBERG_AVRO_LOGICAL_TYPE_DATE:
			return "date";
		case ICEBERG_AVRO_LOGICAL_TYPE_TIMESTAMP:
			return "timestamp";
		case ICEBERG_AVRO_LOGICAL_TYPE_TIME:
			return "time";
		case ICEBERG_AVRO_LOGICAL_TYPE_UUID:
			return "uuid";
		case ICEBERG_AVRO_LOGICAL_TYPE_DECIMAL:
			return "decimal";
		case ICEBERG_AVRO_LOGICAL_TYPE_NONE:
			return NULL;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("unexpected avro logical type")));
	}
}


/*
 * IcebergAvroTypeSchemaString returns avro type schema string.
 */
const char *
IcebergAvroTypeSchemaString(IcebergScalarAvroType type)
{
	const char *physicalType = IcebergAvroPhysicalTypeName(type);
	const char *logicalType = IcebergAvroLogicalTypeName(type);

	if (logicalType == NULL)
		return psprintf("\"%s\"", physicalType);

	StringInfo	typeSchemaString = makeStringInfo();

	appendStringInfoChar(typeSchemaString, '{');

	appendStringInfo(typeSchemaString, "\"type\": \"%s\",", physicalType);

	appendStringInfo(typeSchemaString, "\"logicalType\": \"%s\"", logicalType);

	if (strcmp(logicalType, "decimal") == 0)
	{
		appendStringInfoChar(typeSchemaString, ',');
		appendStringInfo(typeSchemaString, "\"scale\": %d,", type.scale);
		appendStringInfo(typeSchemaString, "\"precision\": %d", type.precision);
	}

	appendStringInfoChar(typeSchemaString, '}');

	return typeSchemaString->data;
}
