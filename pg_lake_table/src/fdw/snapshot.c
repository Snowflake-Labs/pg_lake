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
#include "miscadmin.h"

#include <inttypes.h>

#include "catalog/pg_inherits.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/iceberg/api/datafile.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/snapshot.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/parsetree/options.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/fdw/data_file_pruning.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/util/rel_utils.h"
#include "foreign/foreign.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/typcache.h"

static PgLakeTableScan * CreateTableScanForRelation(Oid relationId,
													Snapshot snapshot,
													int uniqueRelationIdentifier,
													List *baseRestrictInfoList,
													bool includeChildren,
													bool isResultRelation);
static List *GetPositionDeleteTableDataFileForDataFiles(Oid relationId, List *dataFileList,
														Snapshot snapshot);
static List *GetBaseRestrictInfoForRelation(List *relationRestrictionsList,
											int uniqueRelationIdentifier);
static void ConvertIcebergDataFilesToFileScan(List *dataFiles, List *deleteFiles,
											  List **fileScans,
											  List **positionDeleteFileScans);
static void ErrorIfSchemasDoNotMatch(Oid relationId, IcebergTableMetadata * metadata);
static int	NullSafeStrcmp(const char *a, const char *b);
static bool TypesAreCompatible(PGType pgType, PGType icebergType);

/*
 * CreatePgLakeScanSnapshot generates a current snapshot for a list
 * of pg_lake relation rtes, where a snapshot is a list
 * of files for each table.
 *
 * In the future, we may want to prune files based on statistics.
 */
PgLakeScanSnapshot *
CreatePgLakeScanSnapshot(List *rteList,
						 List *relationRestrictionsList,
						 ParamListInfo externalParams,
						 bool includeChildren,
						 Oid resultRelationId)
{
	/*
	 * Make sure: a) We see the concurrent changes that might have happened on
	 * the data_files catalog b) We have a consistent view of multiple tables
	 * if the query involves multiple tables.
	 */
	Snapshot	postgresSnapshot = GetTransactionSnapshot();

	List	   *tableScans = NIL;

	ListCell   *relationCell = NULL;

	foreach(relationCell, rteList)
	{
		RangeTblEntry *rte = (RangeTblEntry *) lfirst(relationCell);
		Oid			relationId = rte->relid;
		int			uniqueRelationIdentifier = GetUniqueRelationIdentifier(rte);
		bool		isResultRelation = false;

		if (resultRelationId != InvalidOid && relationId == resultRelationId)
			isResultRelation = true;

		List	   *baseRestrictInfoList =
			GetBaseRestrictInfoForRelation(relationRestrictionsList, uniqueRelationIdentifier);

		/*
		 * We will destructively modify the restrictions during execution, so
		 * perform the modifications on a copy to ensure the next prepared
		 * statement execution is not affected.
		 */
		baseRestrictInfoList = list_copy_deep(baseRestrictInfoList);

		if (externalParams)
		{
			ReplaceParamsInRestrictInfo(baseRestrictInfoList, externalParams);
		}

		if (message_level_is_interesting(DEBUG2))
		{
			PrettyPrintBaseRestrictInfo(DEBUG2, rte, baseRestrictInfoList);
		}

		PgLakeTableScan *tableScan =
			CreateTableScanForRelation(relationId, postgresSnapshot,
									   uniqueRelationIdentifier,
									   baseRestrictInfoList,
									   includeChildren && rte->inh,
									   isResultRelation);

		tableScans = lappend(tableScans, tableScan);
	}

	PgLakeScanSnapshot *snapshot = palloc0(sizeof(PgLakeScanSnapshot));

	snapshot->tableScans = tableScans;

	return snapshot;
}


/*
* GetBaseRestrictInfoForRelation goes over the relation restrictions, and collects
* all the base restrictions that are on the relation identified by
* uniqueRelationIdentifier.
*/
static List *
GetBaseRestrictInfoForRelation(List *relationRestrictionsList, int uniqueRelationIdentifier)
{
	List	   *baseRestrictionList = NIL;

	ListCell   *restrictionCell = NULL;

	foreach(restrictionCell, relationRestrictionsList)
	{
		PlannerRelationRestriction *restriction = lfirst(restrictionCell);

		if (restriction->baseRestrictionList == NIL)
		{
			/* no restriction to add */
			continue;
		}
		else if (GetUniqueRelationIdentifier(restriction->rte) == uniqueRelationIdentifier)
		{
			/*
			 * Concat all the restrictions that Postgres planner knows about a
			 * relation during query planning. Use concat_unique to avoid
			 * possibly large number of duplicates.
			 */
			baseRestrictionList =
				list_concat(baseRestrictionList, restriction->baseRestrictionList);
		}
	}

	return baseRestrictionList;
}



/*
 * CreateTableScanForRelation creates a table scan for the given relation.
 */
static PgLakeTableScan *
CreateTableScanForRelation(Oid relationId, Snapshot snapshot, int uniqueRelationIdentifier, List *baseRestrictInfoList,
						   bool includeChildren, bool isResultRelation)
{
	List	   *fileScans = NIL;
	List	   *positionDeleteScans = NIL;

	if (IsWritablePgLakeTable(relationId) || IsInternalIcebergTable(relationId))
	{
		/*
		 * Read only the data files, do not yet include the deletion files.
		 * We'll calculate the deletion files based on the pruned data files
		 * using the same snapshot.
		 */
		bool		dataOnly = true;
		bool		newFilesOnly = false;
		List	   *dataFiles =
			GetTableDataFilesFromCatalog(relationId, dataOnly, newFilesOnly,
										 isResultRelation, NULL, snapshot);

		/* prune the data files based on the filters in the query execution */
		List	   *prunedDataFiles = PruneDataFiles(relationId, dataFiles, baseRestrictInfoList, PARTIAL_MATCH);

		/* for the pruned dataFiles, read the corresponding deletion files */
		List	   *positionDeleteFiles =
			GetPositionDeleteTableDataFileForDataFiles(relationId, prunedDataFiles, snapshot);

		/*
		 * for result relations, mark the files which are fully matched by
		 * filters
		 */
		List	   *fullMatches = NIL;

		/*
		 * For result relations we check whether the whole file matches the
		 * filter, since that might allow us to skip work. We only do this for
		 * Iceberg, since we only have statistics for Iceberg.
		 */
		if (isResultRelation)
			fullMatches = PruneDataFiles(relationId, prunedDataFiles, baseRestrictInfoList, FULL_MATCH);

		foreach_ptr(TableDataFile, dataFile, prunedDataFiles)
		{
			PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

			fileScan->path = dataFile->path;
			fileScan->rowCount = dataFile->stats.rowCount;
			fileScan->deletedRowCount = dataFile->stats.deletedRowCount;
			fileScan->allRowsMatch = list_member_ptr(fullMatches, dataFile);

			fileScans = lappend(fileScans, fileScan);
		}

		foreach_ptr(TableDataFile, deletionFile, positionDeleteFiles)
		{
			PgLakeFileScan *positionDeleteScan = palloc0(sizeof(PgLakeFileScan));

			positionDeleteScan->path = deletionFile->path;
			positionDeleteScan->rowCount = deletionFile->stats.rowCount;

			positionDeleteScans = lappend(positionDeleteScans, positionDeleteScan);
		}
	}
	else if (IsExternalIcebergTable(relationId))
	{
		char	   *path = GetIcebergMetadataLocation(relationId, false);

		IcebergTableMetadata *metadata = ReadIcebergTableMetadata(path);

		/*
		 * We cannot afford to have a different schema between the Postgres
		 * catalogs and the iceberg catalog.
		 */
		IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

		if (catalogType == REST_CATALOG_READ_ONLY ||
			catalogType == OBJECT_STORE_READ_ONLY)
			ErrorIfSchemasDoNotMatch(relationId, metadata);

		CreateTableScanForIcebergMetadata(relationId, metadata, baseRestrictInfoList, &fileScans, &positionDeleteScans);
	}
	else
	{
		ForeignTable *foreignTable = GetForeignTable(relationId);
		List	   *options = foreignTable->options;

		/* error if path is missing */
		char	   *path = GetStringOption(options, "path", true);

		/* for wildcard paths, check if we have a _filename filter */
		if (EnableDataFilePruning &&
			strchr(path, '*') != NULL &&
			HasOption(options, "filename") &&
			GetFilenameFilterColumn(relationId, baseRestrictInfoList) != NULL)
		{
			List	   *dataFiles = ListRemoteFileNames(path);
			List	   *prunedDataFiles = PruneByFilename(dataFiles,
														  relationId,
														  baseRestrictInfoList);

			ListCell   *dataFileCell = NULL;

			foreach(dataFileCell, prunedDataFiles)
			{
				char	   *dataFile = lfirst(dataFileCell);

				PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

				fileScan->path = dataFile;
				fileScan->rowCount = ROW_COUNT_NOT_SET;

				fileScans = lappend(fileScans, fileScan);
			}
		}
		else
		{
			PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

			fileScan->path = pstrdup(path);
			fileScan->rowCount = ROW_COUNT_NOT_SET;

			fileScans = lappend(fileScans, fileScan);
		}
	}

	List	   *childScans = NIL;

	if (includeChildren && has_subclass(relationId))
	{
		List	   *childIds = find_inheritance_children(relationId, NoLock);

		foreach_oid(childId, childIds)
		{
			/* child scans do not need an RTE identifier */
			int			childRelationIdentifier = -1;

			/*
			 * XXX - do we want a HeavyAssert to validate that the children
			 * didn't add columns?  Any downside if so, other than only parent
			 * table's columns are visible when queried via inheritance
			 * hierarchy?
			 */
			PgLakeTableScan *childScan =
				CreateTableScanForRelation(childId, snapshot,
										   childRelationIdentifier,
										   baseRestrictInfoList,
										   includeChildren,
										   isResultRelation);

			childScans = lappend(childScans, childScan);
		}
	}

	PgLakeTableScan *tableScan = palloc0(sizeof(PgLakeTableScan));

	tableScan->relationId = relationId;
	tableScan->uniqueRelationIdentifier = uniqueRelationIdentifier;
	tableScan->fileScans = fileScans;
	tableScan->positionDeleteScans = positionDeleteScans;
	tableScan->childScans = childScans;
	tableScan->isUpdateDelete = isResultRelation;

	return tableScan;
}

/*
* NullSafeStrcmp is a helper function that compares two strings for equality,
* treating NULL as equal to NULL.
*/
static int
NullSafeStrcmp(const char *a, const char *b)
{
	/* treat both NULL as equal */
	if (a == NULL && b == NULL)
		return 0;
	if (a == NULL)
		return -1;
	if (b == NULL)
		return 1;
	return strcmp(a, b);
}


/*
* ErrorIfSchemasDoNotMatch is a helper function that checks if the Iceberg
* table schema matches the Postgres table schema.
*/
static void
ErrorIfSchemasDoNotMatch(Oid relationId, IcebergTableMetadata * metadata)
{
	IcebergTableSchema *icebergTableSchema = GetCurrentIcebergTableSchema(metadata);
	List	   *postgresColumnMappings =
		CreatePostgresColumnMappingsForIcebergTableFromExternalMetadata(relationId);

	/* if field counts do not match, a DDL happened */
	if (icebergTableSchema->fields_length != list_length(postgresColumnMappings))
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Schema mismatch between Iceberg and Postgres for relation \"%s\": field count %zu vs %d",
						get_rel_name(relationId),
						icebergTableSchema->fields_length,
						list_length(postgresColumnMappings)),
				 errhint("Please drop and recreate the table \"%s\"", get_rel_name(relationId))));
	}

	/*
	 * now iterate on the fields, and throw error in case anything doesn't
	 * match
	 */
	for (int i = 0; i < icebergTableSchema->fields_length; i++)
	{
		DataFileSchemaField *icebergField = &icebergTableSchema->fields[i];
		PostgresColumnMapping *columnMapping = list_nth(postgresColumnMappings, i);
		DataFileSchemaField *postgresField = columnMapping->field;
		PGType		postgresType = columnMapping->pgType;
		PGType		icebergType = IcebergFieldToPostgresType(icebergField->type);
		bool		hasIcebergDefault =
			(icebergField->writeDefault != NULL) ||
			(icebergField->initialDefault != NULL);

		/*
		 * Compare the id fields.
		 */
		if (icebergField->id != postgresField->id ||
			NullSafeStrcmp(icebergField->name, postgresField->name) != 0 ||
			!TypesAreCompatible(postgresType, icebergType) ||
			columnMapping->attNotNull != icebergField->required ||
			columnMapping->attHasDef != hasIcebergDefault)
		{
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("Schema mismatch between Iceberg and Postgres for field ids %d vs %d", icebergField->id, postgresField->id),
					 errhint("Please drop and recreate the table \"%s\"", get_rel_name(relationId))));
		}
	}

}


/*
* For base types, compatibility means exact match. For composite types,
* compatibility means all fields are compatible. For arrays, compatibility
* means the element types are compatible. For maps, compatibility means the
* key and value types are compatible.
*/
static bool
TypesAreCompatible(PGType pgType, PGType icebergType)
{
	/* if both are the same base type, they are compatible */
	if (pgType.postgresTypeOid == icebergType.postgresTypeOid &&
		pgType.postgresTypeMod == icebergType.postgresTypeMod)
		return true;

	/* not composite type, we are done */
	if (pgType.postgresTypeOid <= FirstNormalObjectId)
		return false;

	if (type_is_array(pgType.postgresTypeOid) && type_is_array(icebergType.postgresTypeOid))
	{
		Oid			pgElementType = get_element_type(pgType.postgresTypeOid);
		Oid			icebergElementType = get_element_type(icebergType.postgresTypeOid);

		return TypesAreCompatible(MakePGType(pgElementType, -1), MakePGType(icebergElementType, -1));
	}
	else if (get_typtype(pgType.postgresTypeOid) == TYPTYPE_COMPOSITE &&
			 get_typtype(icebergType.postgresTypeOid) == TYPTYPE_COMPOSITE)
	{
		TupleDesc	pgTupleDesc = lookup_rowtype_tupdesc(pgType.postgresTypeOid, pgType.postgresTypeMod);
		TupleDesc	icebergTupleDesc = lookup_rowtype_tupdesc(icebergType.postgresTypeOid, icebergType.postgresTypeMod);
		bool		compatible = true;

		if (pgTupleDesc->natts != icebergTupleDesc->natts)
			compatible = false;
		else
		{
			for (int i = 0; i < pgTupleDesc->natts; i++)
			{
				Form_pg_attribute pgAttr = TupleDescAttr(pgTupleDesc, i);
				Form_pg_attribute icebergAttr = TupleDescAttr(icebergTupleDesc, i);
				PGType		pgAttrType = MakePGType(pgAttr->atttypid, pgAttr->atttypmod);
				PGType		icebergAttrType = MakePGType(icebergAttr->atttypid, icebergAttr->atttypmod);

				if (!TypesAreCompatible(pgAttrType, icebergAttrType))
				{
					compatible = false;
					break;
				}
			}
		}

		ReleaseTupleDesc(pgTupleDesc);
		ReleaseTupleDesc(icebergTupleDesc);

		return compatible;
	}
	else if (IsMapTypeOid(pgType.postgresTypeOid) &&
			 IsMapTypeOid(icebergType.postgresTypeOid))
	{
		PGType		keyPgType = GetMapKeyType(pgType.postgresTypeOid);
		PGType		valuePgType = GetMapValueType(pgType.postgresTypeOid);
		PGType		keyIcebergType = GetMapKeyType(icebergType.postgresTypeOid);
		PGType		valueIcebergType = GetMapValueType(icebergType.postgresTypeOid);

		return TypesAreCompatible(keyPgType, keyIcebergType) &&
			TypesAreCompatible(valuePgType, valueIcebergType);
	}
	else
		return false;

}

/*
* GetPositionDeleteTableDataFileForDataFiles gets the position delete files
* for the given data files. This is basically a wrapper around
* GetPositionDeleteFilesForDataFiles() and wraps the result in a TableDataFile.
*/
static List *
GetPositionDeleteTableDataFileForDataFiles(Oid relationId, List *dataFileList,
										   Snapshot snapshot)
{
	List	   *positionDeletes = NIL;

	/*
	 * In the above loop, we only add the data files that are not refuted by
	 * the constraints. Now, we add the position delete files to the list of
	 * unpruned data files.
	 */
	uint64		rowCount = 0;
	List	   *positionDeleteFilePathList =
		GetPositionDeleteFilesForDataFiles(relationId, dataFileList, snapshot, &rowCount);

	ListCell   *positionDeleteFilePathCell = NULL;

	foreach(positionDeleteFilePathCell, positionDeleteFilePathList)
	{
		char	   *positionDeleteFilePath = lfirst(positionDeleteFilePathCell);
		TableDataFile *positionDeleteFile = palloc0(sizeof(TableDataFile));

		positionDeleteFile->path = positionDeleteFilePath;
		positionDeleteFile->content = CONTENT_POSITION_DELETES;

		positionDeletes = lappend(positionDeletes, positionDeleteFile);
	}

	return positionDeletes;
}



/*
* CreateTableScanForIcebergMetadata creates a table scan for the given
* Iceberg table metadata with the currentSnapshot.
*/
void
CreateTableScanForIcebergMetadata(Oid relationId, IcebergTableMetadata * metadata, List *baseRestrictInfoList,
								  List **fileScans, List **positionDeleteScans)
{
	List	   *dataFiles = NIL;
	List	   *deleteFiles = NIL;

	FetchAllDataAndDeleteFilesFromCurrentSnapshot(metadata, &dataFiles, &deleteFiles);

	List	   *retainedFiles = PruneDataFiles(relationId, dataFiles, baseRestrictInfoList, PARTIAL_MATCH);

	ConvertIcebergDataFilesToFileScan(retainedFiles, deleteFiles, fileScans, positionDeleteScans);
}


/*
* ConvertIcebergDataFilesToFileScan converts a list of Iceberg data files
* to a list of PgLakeFileScan.
*/
static void
ConvertIcebergDataFilesToFileScan(List *dataFiles, List *deleteFiles,
								  List **fileScans, List **positionDeleteFileScans)
{
	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);

		PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

		fileScan->path = (char *) dataFile->file_path;
		fileScan->rowCount = dataFile->record_count;
		fileScan->deletedRowCount = 0;

		*fileScans = lappend(*fileScans, fileScan);
	}

	dataFileCell = NULL;
	foreach(dataFileCell, deleteFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);

		PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

		fileScan->path = (char *) dataFile->file_path;
		fileScan->rowCount = dataFile->record_count;
		fileScan->deletedRowCount = 0;

		*positionDeleteFileScans = lappend(*positionDeleteFileScans, fileScan);
	}

}


/*
 * GetTableScanByRelationId returns the table scan for a given relation ID.
 */
PgLakeTableScan *
GetTableScanByRelationId(PgLakeScanSnapshot * snapshot, Oid relationId)
{
	ListCell   *tableCell = NULL;

	foreach(tableCell, snapshot->tableScans)
	{
		PgLakeTableScan *tableScan = lfirst(tableCell);

		if (tableScan->relationId == relationId)
			return tableScan;
	}

	return NULL;
}


/*
 * GetFileScanPathList extracts a list of paths from the table scan.
 */
List *
GetFileScanPathList(List *fileScans, uint64 *rowCount, bool skipFullScans)
{
	List	   *pathList = NIL;
	ListCell   *fileScanCell = NULL;

	*rowCount = 0;

	foreach(fileScanCell, fileScans)
	{
		PgLakeFileScan *fileScan = lfirst(fileScanCell);

		if (skipFullScans && fileScan->allRowsMatch)
			continue;

		if (fileScan->rowCount != ROW_COUNT_NOT_SET)
		{
			/* non-writable tables do not have rowCount set */
			*rowCount += (uint64) fileScan->rowCount;
		}

		pathList = lappend(pathList, fileScan->path);
	}

	return pathList;
}


/*
* SnapshotFilesScanned is a utility function that sets the number of data
* and delete file scans in the snapshot.
*/
void
SnapshotFilesScanned(PgLakeScanSnapshot * scanSnapshot, int *dataFileScans,
					 int *deleteFileScans)
{
	*dataFileScans = 0;
	*deleteFileScans = 0;

	ListCell   *lc;

	foreach(lc, scanSnapshot->tableScans)
	{
		PgLakeTableScan *tableScan = (PgLakeTableScan *) lfirst(lc);

		int			curDataFileScans = list_length(tableScan->fileScans);
		int			curDeleteFileScans = list_length(tableScan->positionDeleteScans);

		*dataFileScans += curDataFileScans;
		*deleteFileScans += curDeleteFileScans;
	}
}
