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

#include "common/int.h"

#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/iceberg/metadata_operations.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/snapshot.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/transaction/transaction_hooks.h"

#ifdef USE_ASSERT_CHECKING
static void EnsureIcebergPartitionMetadataInSync(Oid relationId, int largestSpecId,
												 int currentSpecId,
												 int largestPartitionFieldId);
static void AssertInternalAndExternalIcebergStatsMatchForAllDataFiles(Oid relationId, bool dataOnly, List *internalDataFiles);
static void AssertInternalAndExternalIcebergDataFileColumnStatsMatch(List *internalColumnStatsList,
																	 List *externalLowerBounds,
																	 List *externalUpperBounds);
static List *RemoveDroppedFieldBounds(const List *existingLeafFields, ColumnBound * bounds, size_t boundsLen);
static int	TableDataFileCompare(const ListCell *a, const ListCell *b);
static int	DataFileCompare(const ListCell *a, const ListCell *b);
static int	DataFileColumnStatsCompare(const ListCell *a, const ListCell *b);
static int	PartitionFieldCompare(const ListCell *a, const ListCell *b);
static List *PartitionArrayToList(PartitionField * fields, size_t fieldsLength);
static int	ColumnBoundCompare(const ListCell *a, const ListCell *b);
static void DataFileToTableScanList(List *dataFileList, List **fileScans,
									List **positionDeletes);
static void ErrorIfIcebergMetadataIsOutOfSync(Oid relationId, List *fileScans,
											  List *positionDeleteScans);
static int	ComparePgLakeFileScan(const ListCell *p1, const ListCell *p2);
static void ErrorIfScanListsAreNotEqual(List *scanList1, List *scanList2, const char *scanType);
static void AssertInternalAndExternalTableSchemaMatch(Oid relationId, DataFileSchema * internalSchema);
static int	FieldCompare(const ListCell *a, const ListCell *b);
static void AssertInternalAndExternalLeafFieldsMatch(Oid relationId, List *internalLeafFields);

#endif


/*
* ExternalHeavyAssertsOnIcebergMetadataChange is the entry point for many
* heavy assertions where we compare the internal metadata
* state (e.g., lake_table.files) vs the
* data files pointed by metadata.json for a given iceberg
* table.
* We call these as heavy asserts as they require significant
* time to run, so we only enable it in builds with assertions
* as well as protected by a GUC. We enable the GUC in the CI
* pipelines, not in local development environments.
*/
void
ExternalHeavyAssertsOnIcebergMetadataChange(void)
{
#ifdef USE_ASSERT_CHECKING

	if (!EnableHeavyAsserts)
	{
		return;
	}

	HTAB	   *trackedRelations = GetTrackedIcebergMetadataOperations();

	if (trackedRelations == NULL)
	{
		return;
	}

	HASH_SEQ_STATUS status;
	TableMetadataOperationTracker *opTracker;

	hash_seq_init(&status, trackedRelations);
	while ((opTracker = hash_seq_search(&status)) != NULL)
	{
		Oid			relationId = opTracker->relationId;

		/* relation is dropped */
		if (!RelationExistsInTheIcebergCatalog(relationId))
			continue;

		if (opTracker->relationDataFileChanged)
		{
			bool		dataOnly = false;

			List	   *dataFiles =
				GetTableDataFilesFromCatalog(relationId, dataOnly,
											 false, false, NULL, NULL);

			AssertInternalAndExternalIcebergStatsMatchForAllDataFiles(relationId, dataOnly, dataFiles);

			List	   *assertFileScans = NIL;
			List	   *assertPositionDeleteScans = NIL;

			DataFileToTableScanList(dataFiles, &assertFileScans, &assertPositionDeleteScans);

			ErrorIfIcebergMetadataIsOutOfSync(relationId, assertFileScans, assertPositionDeleteScans);
		}
		else if (opTracker->relationPartitionByChanged)
		{
			int			currentSpecId = GetCurrentSpecId(relationId);
			int			largestSpecId = GetLargestSpecId(relationId);
			int			largestPartitionFieldId = GetLargestPartitionFieldId(relationId);

			EnsureIcebergPartitionMetadataInSync(relationId, currentSpecId, largestSpecId, largestPartitionFieldId);
		}
		else if (opTracker->relationCreated || opTracker->relationAltered)
		{
			DataFileSchema *schema = GetDataFileSchemaForInternalIcebergTable(relationId);

			AssertInternalAndExternalTableSchemaMatch(relationId, schema);

			List	   *leafFields = GetLeafFieldsForInternalIcebergTable(relationId);

			AssertInternalAndExternalLeafFieldsMatch(relationId, leafFields);
		}
	}
#endif
}





#ifdef USE_ASSERT_CHECKING
/*
* EnsureIcebergPartitionMetadataInSync checks if the iceberg metadata
* is in sync with the lake_table partition_specs and
* partition_fields tables. If not, it raises an error.
*/
static void
EnsureIcebergPartitionMetadataInSync(Oid relationId, int currentSpecId, int largestSpecId, int largestPartitionFieldId)
{
	char	   *currentMetadataPath = GetIcebergMetadataLocation(relationId, false);
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(currentMetadataPath);

	if (metadata->default_spec_id != currentSpecId)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Iceberg spec id in metadata.json and lake_table.partition_specs "
							   "is not in sync: %d != %d", metadata->default_spec_id, currentSpecId)));
	}

	IcebergPartitionSpec *latestSpec = &metadata->partition_specs[metadata->partition_specs_length - 1];

	if (latestSpec->spec_id != largestSpecId)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Largest spec id and largest partition spec field id in "
							   "is not in sync: %d != %d", latestSpec->spec_id, largestSpecId)));
	}

	if (metadata->last_partition_id != largestPartitionFieldId)
	{
		ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
						errmsg("Iceberg partition field id in metadata.json and "
							   "lake_table.partition_specs is not in sync: %d != %d",
							   metadata->last_partition_id, largestPartitionFieldId)));
	}

	if (latestSpec->fields_length > 0)
	{
		IcebergPartitionSpecField *latestField = &latestSpec->fields[latestSpec->fields_length - 1];

		if (latestField->field_id != largestPartitionFieldId)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("Iceberg partition field id in metadata.json and "
								   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %d != %d",
								   latestField->field_id, largestPartitionFieldId)));
		}

		for (int fieldIndex = 0; fieldIndex < latestSpec->fields_length; fieldIndex++)
		{
			IcebergPartitionSpecField *field = &latestSpec->fields[fieldIndex];
			IcebergPartitionSpecField *fieldFromCatalog =
				GetIcebergPartitionFieldFromCatalog(relationId, field->field_id);

			/* error if we get NULL from catalog */
			if (fieldFromCatalog == NULL)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Iceberg partition field id in metadata.json and "
									   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %d != %d",
									   field->field_id, fieldFromCatalog->field_id)));
			}

			/* error if fieldIds are not equal */
			if (field->field_id != fieldFromCatalog->field_id)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Iceberg partition field id in metadata.json and "
									   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %d != %d",
									   field->field_id, fieldFromCatalog->field_id)));
			}

			/* error if names are not equal */
			if (strcmp(field->name, fieldFromCatalog->name) != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Iceberg partition field name in metadata.json and "
									   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %s != %s",
									   field->name, fieldFromCatalog->name)));
			}

			/* error if transforms are not equal */
			if (strcmp(field->transform, fieldFromCatalog->transform) != 0)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Iceberg partition field transform in metadata.json and "
									   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %s != %s",
									   field->transform, fieldFromCatalog->transform)));
			}

			/* error if sourceIds are not equal */
			if (field->source_id != fieldFromCatalog->source_id)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Iceberg partition field source id in metadata.json and "
									   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %d != %d",
									   field->source_id, fieldFromCatalog->source_id)));
			}

			/* error if sourceIds are not equal */
			if (field->source_ids_length != fieldFromCatalog->source_ids_length)
			{
				ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
								errmsg("Iceberg partition field source ids length in metadata.json and "
									   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %zu != %zu",
									   field->source_ids_length, fieldFromCatalog->source_ids_length)));
			}

			for (int sourceIdIndex = 0; sourceIdIndex < field->source_ids_length; sourceIdIndex++)
			{
				if (field->source_ids[sourceIdIndex] != fieldFromCatalog->source_ids[sourceIdIndex])
				{
					ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
									errmsg("Iceberg partition field source ids in metadata.json and "
										   PARTITION_FIELDS_TABLE_QUALIFIED " is not in sync: %d != %d",
										   field->source_ids[sourceIdIndex], fieldFromCatalog->source_ids[sourceIdIndex])));
				}
			}

		}
	}
}


/*
 * AssertInternalAndExternalIcebergStatsMatchForAllDataFiles checks if the internal and external
 * iceberg data file stats match for all data files.
 */
static void
AssertInternalAndExternalIcebergStatsMatchForAllDataFiles(Oid relationId, bool dataOnly, List *internalDataFiles)
{
	PgLakeTableProperties properties = GetPgLakeTableProperties(relationId);

	/* only iceberg tables has stats at internal catalog */
	if (properties.tableType != PG_LAKE_ICEBERG_TABLE_TYPE)
	{
		return;
	}

	/* fetch external data files and convert them to TableDataFile */
	char	   *metadataPath = GetIcebergMetadataLocation(relationId, false);
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataPath);

	List	   *externalDataFiles = NIL;
	List	   *externalDeletionFiles = NIL;

	FetchAllDataAndDeleteFilesFromCurrentSnapshot(metadata, &externalDataFiles,
												  &externalDeletionFiles);
	/* internal files will contain deletion files as well */
	if (!dataOnly)
		externalDataFiles = list_concat(externalDataFiles, externalDeletionFiles);

	/* sort internal and external data files to compare them */
	List	   *internalDataFilesCopy = list_copy(internalDataFiles);

	list_sort(internalDataFilesCopy, TableDataFileCompare);
	list_sort(externalDataFiles, DataFileCompare);

	if (list_length(internalDataFilesCopy) != list_length(externalDataFiles))
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("internal and external iceberg data files count mismatch %d %d",
						list_length(internalDataFilesCopy),
						list_length(externalDataFiles))));
	}

	List	   *existingLeafFields = GetLeafFieldsForInternalIcebergTable(relationId);

	ListCell   *internalDataFileCell = NULL;
	ListCell   *externalDataFileCell = NULL;

	forboth(internalDataFileCell, internalDataFilesCopy,
			externalDataFileCell, externalDataFiles)
	{
		TableDataFile *internalDataFile = lfirst(internalDataFileCell);

		DataFile   *externalDataFile = lfirst(externalDataFileCell);

		if (strcmp(internalDataFile->path, externalDataFile->file_path) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file path mismatch %s-%s",
							internalDataFile->path,
							externalDataFile->file_path)));
		}

		if (internalDataFile->content != (DataFileContent) externalDataFile->content)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file content mismatch")));
		}

		if (externalDataFile->lower_bounds_length != externalDataFile->upper_bounds_length)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("external iceberg data file lower and upper bounds length mismatch")));
		}

		if (internalDataFile->stats.rowCount != externalDataFile->record_count)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file stats row count mismatch")));
		}

		if (internalDataFile->stats.fileSize != externalDataFile->file_size_in_bytes)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file stats file size mismatch")));
		}

		/*
		 * dropped fields' bounds do not exist in internal column stats so
		 * filter out them from external bounds
		 */
		List	   *externalLowerBounds = RemoveDroppedFieldBounds(existingLeafFields,
																   externalDataFile->lower_bounds,
																   externalDataFile->lower_bounds_length);
		List	   *externalUpperBounds = RemoveDroppedFieldBounds(existingLeafFields,
																   externalDataFile->upper_bounds,
																   externalDataFile->upper_bounds_length);

		Assert(list_length(externalLowerBounds) == list_length(externalUpperBounds));

		/* copy internal column stats to compare them */
		List	   *internalColumnStatsList = list_copy(internalDataFile->stats.columnStats);

		if ((size_t) list_length(internalColumnStatsList) != list_length(externalLowerBounds))
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file column stats count mismatch %d %d",
							list_length(internalColumnStatsList),
							list_length(externalLowerBounds))));
		}

		AssertInternalAndExternalIcebergDataFileColumnStatsMatch(internalColumnStatsList,
																 externalLowerBounds,
																 externalUpperBounds);

		if (internalDataFile->partition->fields_length != externalDataFile->partition.fields_length)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file partition fields length mismatch %zu %zu %s",
							internalDataFile->partition->fields_length,
							externalDataFile->partition.fields_length,
							internalDataFile->path)));
		}

		List	   *internalPartitionFields = PartitionArrayToList(internalDataFile->partition->fields,
																   internalDataFile->partition->fields_length);
		List	   *externalPartitionFields = PartitionArrayToList(externalDataFile->partition.fields,
																   externalDataFile->partition.fields_length);

		list_sort(internalPartitionFields, PartitionFieldCompare);
		list_sort(externalPartitionFields, PartitionFieldCompare);

		/* now compare individual partition fields */
		ListCell   *lc1,
				   *lc2;

		forboth(lc1, internalPartitionFields,
				lc2, externalPartitionFields)
		{
			PartitionField *internalPartitionField = lfirst(lc1);
			PartitionField *externalPartitionField = lfirst(lc2);

			if (internalPartitionField->field_id != externalPartitionField->field_id)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("internal and external iceberg data file partition field id mismatch")));
			}


			if (strcmp(internalPartitionField->field_name,
					   externalPartitionField->field_name) != 0)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("internal and external iceberg data file partition field name mismatch")));
			}

			if (internalPartitionField->value_type.physical_type != externalPartitionField->value_type.physical_type)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("internal and external iceberg data file partition field physical type mismatch")));
			}

			/*
			 * we serialize boolean 1 byte per spec but spark serializes it as
			 * 4 bytes (int)
			 */
			bool		mismatchBoolLength = internalPartitionField->value_type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_BOOL &&
				internalPartitionField->value_length == 1 && externalPartitionField->value_length == 4;

			if (internalPartitionField->value_length != externalPartitionField->value_length && !mismatchBoolLength)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("internal and external iceberg data file partition field value length mismatch %zu %zu for %s",
								internalPartitionField->value_length,
								externalPartitionField->value_length,
								internalPartitionField->field_name)));
			}

			if (memcmp(internalPartitionField->value,
					   externalPartitionField->value,
					   internalPartitionField->value_length) != 0 && !mismatchBoolLength)
			{
				ereport(ERROR,
						(errcode(ERRCODE_INTERNAL_ERROR),
						 errmsg("internal and external iceberg data file partition field value mismatch %s:%s for %s",
								(char *) internalPartitionField->value,
								(char *) externalPartitionField->value,
								internalPartitionField->field_name)));
			}
		}
	}
}

/*
 * AssertInternalAndExternalIcebergDataFileColumnStatsMatch checks if the internal and external
 * iceberg data file column stats match.
 */
static void
AssertInternalAndExternalIcebergDataFileColumnStatsMatch(List *internalColumnStatsList,
														 List *externalLowerBounds,
														 List *externalUpperBounds)
{
	if (internalColumnStatsList == NIL)
	{
		return;
	}

	int			boundsLength = list_length(internalColumnStatsList);

	/* sort them to compare the same column bounds */
	list_sort(internalColumnStatsList, DataFileColumnStatsCompare);
	list_sort(externalLowerBounds, ColumnBoundCompare);
	list_sort(externalUpperBounds, ColumnBoundCompare);

	for (int boundIdx = 0; boundIdx < boundsLength; boundIdx++)
	{
		DataFileColumnStats *internalColumnStats = list_nth(internalColumnStatsList, boundIdx);

		ColumnBound *externalLowerBound = list_nth(externalLowerBounds, boundIdx);

		ColumnBound *externalUpperBound = list_nth(externalUpperBounds, boundIdx);

		if (internalColumnStats->leafField.fieldId != externalLowerBound->column_id ||
			internalColumnStats->leafField.fieldId != externalUpperBound->column_id)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file stats column fieldId mismatch")));
		}

		size_t		internalLowerBoundLen = 0;
		unsigned char *internalLowerBound = IcebergSerializeColumnBoundText(pstrdup(internalColumnStats->lowerBoundText),
																			internalColumnStats->leafField.field,
																			&internalLowerBoundLen);

		if (internalLowerBoundLen != externalLowerBound->value_length ||
			memcmp(internalLowerBound, externalLowerBound->value, internalLowerBoundLen) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file stats column lower bound mismatch")));
		}

		size_t		internalUpperBoundLen = 0;
		unsigned char *internalUpperBound = IcebergSerializeColumnBoundText(pstrdup(internalColumnStats->upperBoundText),
																			internalColumnStats->leafField.field,
																			&internalUpperBoundLen);

		if (internalUpperBoundLen != externalUpperBound->value_length ||
			memcmp(internalUpperBound, externalUpperBound->value, internalUpperBoundLen) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file stats column upper bound mismatch")));
		}

		if (internalColumnStats->leafField.field->type != FIELD_TYPE_SCALAR)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internal and external iceberg data file stats column field type is not scalar")));
		}
	}
}


/*
 * RemoveDroppedFieldBounds removes the bounds of dropped fields.
 */
static List *
RemoveDroppedFieldBounds(const List *existingLeafFields, ColumnBound * bounds, size_t boundsLen)
{
	List	   *nonDroppedBounds = NIL;

	for (size_t boundIdx = 0; boundIdx < boundsLen; boundIdx++)
	{
		ColumnBound *bound = &bounds[boundIdx];

		int			fieldId = bound->column_id;

		bool		fieldExist = false;

		ListCell   *leafFieldCell = NULL;

		foreach(leafFieldCell, existingLeafFields)
		{
			LeafField  *leafField = lfirst(leafFieldCell);

			if (leafField->fieldId == fieldId)
			{
				fieldExist = true;
				break;
			}
		}

		if (fieldExist)
		{
			nonDroppedBounds = lappend(nonDroppedBounds, bound);
		}
	}

	return nonDroppedBounds;
}


/*
 * TableDataFileCompare compares two TableDataFile by path.
 */
static int
TableDataFileCompare(const ListCell *a, const ListCell *b)
{
	TableDataFile *dataFileA = lfirst(a);
	TableDataFile *dataFileB = lfirst(b);

	return strcmp(dataFileA->path, dataFileB->path);
}


/*
 * DataFileCompare compares two DataFile by path.
 */
static int
DataFileCompare(const ListCell *a, const ListCell *b)
{
	DataFile   *dataFileA = lfirst(a);
	DataFile   *dataFileB = lfirst(b);

	return strcmp(dataFileA->file_path, dataFileB->file_path);
}

static int
PartitionFieldCompare(const ListCell *a, const ListCell *b)
{
	PartitionField *fieldA = lfirst(a);
	PartitionField *fieldB = lfirst(b);

	int32		fieldIdA = fieldA->field_id;
	int32		fieldIdB = fieldB->field_id;

	return (fieldIdA > fieldIdB) - (fieldIdA < fieldIdB);
}

static List *
PartitionArrayToList(PartitionField * fields, size_t fieldsLength)
{
	List	   *partitionFields = NIL;

	for (size_t fieldIndex = 0; fieldIndex < fieldsLength; fieldIndex++)
	{
		PartitionField *field = &fields[fieldIndex];

		partitionFields = lappend(partitionFields, field);
	}

	return partitionFields;
}


/*
 * DataFileColumnStatsCompare compares two DataFileColumnStats by fieldId.
 */
static int
DataFileColumnStatsCompare(const ListCell *a, const ListCell *b)
{
	DataFileColumnStats *columnStatsA = lfirst(a);
	DataFileColumnStats *columnStatsB = lfirst(b);

	int32		fieldIdA = columnStatsA->leafField.fieldId;
	int32		fieldIdB = columnStatsB->leafField.fieldId;

	return (fieldIdA > fieldIdB) - (fieldIdA < fieldIdB);
}


/*
 * ColumnBoundCompare compares two ColumnBound by column id (a.k.a. field id).
 */
static int
ColumnBoundCompare(const ListCell *a, const ListCell *b)
{
	ColumnBound *ca = lfirst(a);
	ColumnBound *cb = lfirst(b);

	if (ca->column_id > cb->column_id)
		return 1;
	else if (ca->column_id < cb->column_id)
		return -1;

	return 0;
}

static void
DataFileToTableScanList(List *dataFileList, List **fileScans, List **positionDeletes)
{
	*fileScans = NIL;
	*positionDeletes = NIL;

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFileList)
	{
		TableDataFile *dataFile = lfirst(dataFileCell);

		PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

		fileScan->path = dataFile->path;
		fileScan->rowCount = dataFile->stats.rowCount;
		fileScan->deletedRowCount = dataFile->stats.deletedRowCount;

		if (dataFile->content == CONTENT_DATA)
			*fileScans = lappend(*fileScans, fileScan);
		else if (dataFile->content == CONTENT_POSITION_DELETES)
			*positionDeletes = lappend(*positionDeletes, fileScan);
		else
			elog(ERROR, "equality deletes are not yet supported");
	}

	return;
}


/*
* We mirror the data/deletion files of PG_LAKE_ICEBERG_TABLE_TYPE in
* lake_table.data_file table.
*
* This function asserts that the iceberg metadata is in sync with the
* data files in the catalog.
*
* Note that we only use this when assertions are enabled. Otherwise, we
* have to read the iceberg metadata for every scan, which is expensive.
* So, that'd defeat the purpose of using the catalog.
*/
static void
ErrorIfIcebergMetadataIsOutOfSync(Oid relationId, List *fileScans,
								  List *positionDeleteScans)
{
	/*
	 * If we ever need to use iceberg metadata for fetching data/deletion
	 * files, we should be careful about the concurrency behavior.
	 *
	 * Passing forUpdate = false ensures the same concurrency behavior as the
	 * files catalog.
	 *
	 * We are protected by the update-update command conflicts via the
	 * LockTableForUpdate().
	 *
	 * We need to be careful about the update-insert command conflicts. In
	 * such conflicts, none of them should see the concurrent command's
	 * changes. So, passing forUpdate = false is fine.
	 *
	 * All other conflicts, like TRUNCATE/DDL/VACUUM etc. are handled by the
	 * regular Postgres relation locks.
	 */
	bool		forUpdate = false;
	char	   *metadataPath = GetIcebergMetadataLocation(relationId, forUpdate);
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataPath);

	List	   *icebergMetadataFileScans = NIL;
	List	   *icebergPositionDeleteFileScans = NIL;

	CreateTableScanForIcebergMetadata(relationId, metadata, NIL, &icebergMetadataFileScans, &icebergPositionDeleteFileScans);

	ErrorIfScanListsAreNotEqual(icebergMetadataFileScans, fileScans, "fileScan");
	ErrorIfScanListsAreNotEqual(icebergPositionDeleteFileScans, positionDeleteScans, "posDeleteScan");
}

static int
ComparePgLakeFileScan(const ListCell *p1, const ListCell *p2)
{
	PgLakeFileScan *scan1 = (PgLakeFileScan *) lfirst(p1);
	PgLakeFileScan *scan2 = (PgLakeFileScan *) lfirst(p2);

	return strcmp(scan1->path, scan2->path);
}
static void
ErrorIfScanListsAreNotEqual(List *scanList1, List *scanList2, const char *scanType)
{
	ListCell   *scanCell1 = NULL;
	ListCell   *scanCell2 = NULL;

	if (list_length(scanList1) != list_length(scanList2))
		ereport(ERROR, (errmsg("%s: scan lists are not equal length %d:%d",
							   scanType, list_length(scanList1),
							   list_length(scanList2))));


	list_sort(scanList1, ComparePgLakeFileScan);
	list_sort(scanList2, ComparePgLakeFileScan);
	forboth(scanCell1, scanList1, scanCell2, scanList2)
	{
		PgLakeFileScan *scan1 = lfirst(scanCell1);
		PgLakeFileScan *scan2 = lfirst(scanCell2);

		if (strcmp(scan1->path, scan2->path) != 0)
			elog(ERROR, "scan paths are not equal");
		if (scan1->rowCount != scan2->rowCount)
			elog(ERROR, "%s: scan row counts are not equal: " INT64_FORMAT " != " INT64_FORMAT,
				 scanType, scan1->rowCount, scan2->rowCount);
	}
}


/*
* Only used for testing purposes, ensures that the fields ids in
* MAPPING_TABLE_NAME match the field ids in the
* iceberg metadata.
*/
static void
AssertInternalAndExternalTableSchemaMatch(Oid relationId, DataFileSchema * internalSchema)
{

	char	   *metadataPath = GetIcebergMetadataLocation(relationId, false);
	DataFileSchema *externalSchema = GetDataFileSchemaForExternalIcebergTable(metadataPath);

	if (internalSchema->nfields != externalSchema->nfields)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("internalFields %zu and externalFields %zu have different lengths",
						internalSchema->nfields, externalSchema->nfields)));
	}

	List	   *internalFields = NIL;
	List	   *externalFields = NIL;

	for (int fieldIndex = 0; fieldIndex < internalSchema->nfields; fieldIndex++)
	{
		DataFileSchemaField *internalField = &internalSchema->fields[fieldIndex];
		DataFileSchemaField *externalField = &externalSchema->fields[fieldIndex];

		internalFields = lappend(internalFields, internalField);
		externalFields = lappend(externalFields, externalField);
	}

	/* lets sort lists */
	list_sort(internalFields, FieldCompare);
	list_sort(externalFields, FieldCompare);

	ListCell   *internalFieldCell = NULL;
	ListCell   *externalFieldCell = NULL;

	forboth(internalFieldCell, internalFields, externalFieldCell, externalFields)
	{
		DataFileSchemaField *internalField = lfirst(internalFieldCell);
		DataFileSchemaField *externalField = lfirst(externalFieldCell);

		if (internalField->id != externalField->id)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internalField(%d) and externalField(%d) have different ids",
							internalField->id, externalField->id)));
		}

		if (internalField->type->type != externalField->type->type)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internalField and externalField have different types")));
		}

		/* compare field names via strcmp */
		if (strcmp(internalField->name, externalField->name) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internalField and externalField have different names")));
		}

		if (internalField->initialDefault != NULL &&
			strcmp(internalField->initialDefault, externalField->initialDefault) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("internalField and externalField have different initialDefault")));
		}

		if (internalField->writeDefault != NULL &&
			strcmp(internalField->writeDefault, externalField->writeDefault) != 0)

		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("internalField and externalField have different writeDefault %s and %s",
								   internalField->writeDefault, externalField->writeDefault)));
		}
	}
}

static int
FieldCompare(const ListCell *a, const ListCell *b)
{
	FieldStructElement *fieldA = lfirst(a);
	FieldStructElement *fieldB = lfirst(b);

	return pg_cmp_s32(fieldA->id, fieldB->id);
}


/*
 * Only used for testing purposes, ensures that the leaf fields in the internal schema
 * match the leaf fields in the external schema.
 */
static void
AssertInternalAndExternalLeafFieldsMatch(Oid relationId, List *internalLeafFields)
{
	char	   *metadataPath = GetIcebergMetadataLocation(relationId, false);
	List	   *externalLeafFields = GetLeafFieldsForExternalIcebergTable(metadataPath);

	int			internalLeafFieldsCount = list_length(internalLeafFields);
	int			externalLeafFieldsCount = list_length(externalLeafFields);

	if (internalLeafFieldsCount != externalLeafFieldsCount)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("internalLeafFields %d and externalLeafFields %d have different lengths",
						internalLeafFieldsCount, externalLeafFieldsCount)));
	}

	/* copy lists to not change the order of the actual lists */
	List	   *internalLeafFieldsCopy = list_copy(internalLeafFields);
	List	   *externalLeafFieldsCopy = list_copy(externalLeafFields);

	list_sort(internalLeafFieldsCopy, LeafFieldCompare);
	list_sort(externalLeafFieldsCopy, LeafFieldCompare);

	ListCell   *internalLeafFieldCell = NULL;
	ListCell   *externalLeafFieldCell = NULL;

	forboth(internalLeafFieldCell, internalLeafFieldsCopy, externalLeafFieldCell, externalLeafFieldsCopy)
	{
		LeafField  *internalLeafField = lfirst(internalLeafFieldCell);
		LeafField  *externalLeafField = lfirst(externalLeafFieldCell);

		if (internalLeafField->fieldId != externalLeafField->fieldId)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internalLeafField(%d) and externalLeafField(%d) have different ids",
							internalLeafField->fieldId, externalLeafField->fieldId)));
		}

		if (internalLeafField->field->type != externalLeafField->field->type)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internalLeafField and externalLeafField have different types")));
		}

		if (internalLeafField->field->type != FIELD_TYPE_SCALAR)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internalLeafField and externalLeafField are not scalar")));
		}

		const char *internalFieldTypeName = internalLeafField->field->field.scalar.typeName;

		const char *externalFieldTypeName = externalLeafField->field->field.scalar.typeName;

		if (strcmp(internalFieldTypeName, externalFieldTypeName) != 0)
		{
			ereport(ERROR,
					(errcode(ERRCODE_INTERNAL_ERROR),
					 errmsg("internalLeafField and externalLeafField have different types")));
		}

		if (strcmp(internalLeafField->duckTypeName,
				   externalLeafField->duckTypeName) != 0)
		{
			ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
							errmsg("internalLeafField and externalLeafField have different duck types")));
		}
	}
}



#endif
