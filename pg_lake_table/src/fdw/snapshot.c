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

#include "access/relation.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_type.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "common/string.h"
#include "fmgr.h"
#include "pg_lake/copy/copy_format.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/extensions/pg_lake_engine.h"
#include "pg_lake/iceberg/api/datafile.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/snapshot.h"
#include "pg_lake/fdw/equality_delete.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/pgduck/map.h"
#include "pg_lake/parsetree/options.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/pgduck/remote_storage.h"
#include "pg_lake/planner/restriction_collector.h"
#include "pg_lake/storage/storage_credentials.h"
#include "pg_lake/object_store_catalog/object_store_catalog.h"
#include "pg_lake/rest_catalog/rest_catalog.h"
#include "pg_lake/fdw/data_file_pruning.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/pg_lake_table.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/fdw/schema_operations/register_field_ids.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/util/rel_utils.h"
#include "foreign/foreign.h"
#include "nodes/execnodes.h"
#include "nodes/pg_list.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
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
											  List *fileConstants,
											  List **fileScans,
											  List **positionDeleteFileScans);
static List *IdentityPartitionFieldsForFileConstants(IcebergTableMetadata * metadata);
static List *FileConstantsForDataFile(DataFile * dataFile, List *identityPartitionFields);
static char *FileConstantText(Datum value, PGType pgType);
static void AppendEscapedFileConstant(StringInfo buffer, const char *text);
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
	List	   *equalityDeleteScans = NIL;
	List	   *equalityDeleteReadGroups = NIL;

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
										 isResultRelation, snapshot);

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

		/*
		 * fullMatches is produced by filtering prunedDataFiles in order, so
		 * it is an ordered sublist and a tandem cursor suffices to mark the
		 * fully matched files, instead of an O(files * matches)
		 * list_member_ptr scan.
		 *
		 * The cursor only ever dereferences cells of fullMatches, so it
		 * cannot mark a file that PruneDataFiles did not return. If that
		 * ordering assumption were ever broken the cursor would desync and
		 * leave files unmarked, which costs us the optimization but never
		 * skips a file that still has rows to keep.
		 */
		ListCell   *fullMatchCell = list_head(fullMatches);

		foreach_ptr(TableDataFile, dataFile, prunedDataFiles)
		{
			PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

			fileScan->path = dataFile->path;
			fileScan->rowCount = dataFile->stats.rowCount;
			fileScan->deletedRowCount = dataFile->stats.deletedRowCount;

			if (fullMatchCell != NULL && lfirst(fullMatchCell) == dataFile)
			{
				fileScan->allRowsMatch = true;
				fullMatchCell = lnext(fullMatches, fullMatchCell);
			}

			fileScans = lappend(fileScans, fileScan);
		}

		/* every full match is in prunedDataFiles, so all cells were consumed */
		Assert(fullMatchCell == NULL);

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

		/*
		 * Single choke point for storage credentials on the read path: both
		 * the plain FDW scan path and the query-pushdown path funnel through
		 * CreatePgLakeScanSnapshot -> here, and a table pg_lake did not
		 * create is the only kind whose files a catalog vends credentials
		 * for.  It has to happen before the metadata read just below, which
		 * is itself a read of that storage, and after the catalog lookup
		 * above, whose answer decides whether credentials vended earlier
		 * still stand.  Under REPEATABLE READ and SERIALIZABLE that lookup is
		 * answered once per transaction, so the answer its first statement
		 * got stands until the transaction ends, or until those credentials
		 * near expiry.  The resolver no-ops for everything but a read-only
		 * REST-catalog table.
		 */
		EnsureStorageCredentialsForRelation(relationId);

		IcebergTableMetadata *metadata = ReadIcebergTableMetadataForRelation(relationId, path);

		/* fold names before the schema check, which looks them up as attnames */
		if (LowercasesColumnNames(relationId))
			LowercaseIcebergTableMetadataNames(metadata);
		TruncateIcebergTableMetadataNames(metadata, false);

		/*
		 * We cannot afford to have a different schema between the Postgres
		 * catalogs and the iceberg catalog.
		 */
		IcebergCatalogType catalogType = GetIcebergCatalogType(relationId);

		if (catalogType == REST_CATALOG_READ_ONLY ||
			catalogType == OBJECT_STORE_READ_ONLY ||
			catalogType == NONE_CATALOG)
			ErrorIfSchemasDoNotMatch(relationId, metadata);

		CreateTableScanForIcebergMetadata(relationId, metadata, baseRestrictInfoList, &fileScans, &positionDeleteScans,
										  &equalityDeleteScans, &equalityDeleteReadGroups);
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
	tableScan->equalityDeleteScans = equalityDeleteScans;
	tableScan->equalityDeleteReadGroups = equalityDeleteReadGroups;
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
		StringInfo	unmatched = makeStringInfo();

		/*
		 * Mappings are built by looking up each Iceberg field name among the
		 * relation's columns, and a read-only table silently skips the ones
		 * that find no column.  A short count is therefore just as likely to
		 * mean the names differ as it is to mean a concurrent DDL, and the
		 * two call for opposite responses, so name the fields that did not
		 * match instead of leaving it to be guessed.
		 */
		for (int i = 0; i < icebergTableSchema->fields_length; i++)
		{
			const char *fieldName = icebergTableSchema->fields[i].name;

			if (fieldName != NULL &&
				get_attnum(relationId, fieldName) == InvalidAttrNumber)
				appendStringInfo(unmatched, "%s\"%s\"",
								 unmatched->len > 0 ? ", " : "", fieldName);
		}

		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Schema mismatch between Iceberg and Postgres for relation \"%s\": field count %zu vs %d",
						get_rel_name(relationId),
						icebergTableSchema->fields_length,
						list_length(postgresColumnMappings)),
				 unmatched->len > 0 ?
				 errdetail("Iceberg fields with no matching Postgres column: %s",
						   unmatched->data) : 0,
				 errhint("Please drop and recreate the table \"%s\"", get_rel_name(relationId))));
	}

	/*
	 * Each field found a column above, but a column that no field names (one
	 * dropped or renamed in the catalog) leaves the counts equal.
	 */
	Bitmapset  *mappedAttnums = NULL;

	foreach_ptr(PostgresColumnMapping, columnMapping, postgresColumnMappings)
	{
		if (columnMapping->attrNum > InvalidAttrNumber)
			mappedAttnums = bms_add_member(mappedAttnums, columnMapping->attrNum);
	}

	bool		emitsFilename =
		GetBoolOption(GetForeignTable(relationId)->options, "filename", false);
	Relation	rel = relation_open(relationId, NoLock);
	TupleDesc	tupleDesc = RelationGetDescr(rel);
	char	   *unmatchedColumn = NULL;

	for (int attIdx = 0; attIdx < tupleDesc->natts; attIdx++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupleDesc, attIdx);

		if (attr->attisdropped || bms_is_member(attr->attnum, mappedAttnums))
			continue;

		/* the scan fills the filename column, not a field */
		if (emitsFilename && strcmp(NameStr(attr->attname), "_filename") == 0)
			continue;

		unmatchedColumn = pstrdup(NameStr(attr->attname));
		break;
	}

	relation_close(rel, NoLock);

	if (unmatchedColumn != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("Schema mismatch between Iceberg and Postgres for relation \"%s\"",
						get_rel_name(relationId)),
				 errdetail("Postgres column \"%s\" has no matching Iceberg field.",
						   unmatchedColumn),
				 errhint("Please drop and recreate the table \"%s\"", get_rel_name(relationId))));

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

		/*
		 * Compare the id fields. We deliberately don't compare defaults:
		 * read-only external Iceberg tables don't carry iceberg
		 * writeDefault/initialDefault onto pg_attribute, and the scan takes
		 * initialDefault from the metadata.
		 */
		if (icebergField->id != postgresField->id ||
			NullSafeStrcmp(icebergField->name, postgresField->name) != 0 ||
			!TypesAreCompatible(postgresType, icebergType) ||
			columnMapping->attNotNull != icebergField->required)
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
* compatibility means all fields have compatible types and the same names
* but for case: DuckDB looks a struct field up by name regardless of case, so
* a field renamed in the catalog cannot be read through its old name. For
* arrays, compatibility means the element types are compatible. For maps,
* compatibility means the key and value types are compatible.
*/
static bool
TypesAreCompatible(PGType pgType, PGType icebergType)
{
	/* if both are the same base type, they are compatible */
	if (pgType.postgresTypeOid == icebergType.postgresTypeOid &&
		pgType.postgresTypeMod == icebergType.postgresTypeMod)
		return true;

	/*
	 * timetz is stored as Iceberg "time" which maps to PG TIMEOID. Allow this
	 * mismatch since we normalize to UTC on write and parse back as timetz
	 * +00 on read.
	 */
	if (pgType.postgresTypeOid == TIMETZOID &&
		icebergType.postgresTypeOid == TIMEOID)
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
		int			pgIndex = 0;
		int			icebergIndex = 0;

		/*
		 * Dropped attributes are part of the tuple descriptor but are not
		 * represented in the Iceberg schema, so compare only the live
		 * attributes on each side, in order.
		 */
		while (compatible)
		{
			while (pgIndex < pgTupleDesc->natts &&
				   TupleDescAttr(pgTupleDesc, pgIndex)->attisdropped)
				pgIndex++;

			while (icebergIndex < icebergTupleDesc->natts &&
				   TupleDescAttr(icebergTupleDesc, icebergIndex)->attisdropped)
				icebergIndex++;

			bool		pgDone = pgIndex >= pgTupleDesc->natts;
			bool		icebergDone = icebergIndex >= icebergTupleDesc->natts;

			if (pgDone || icebergDone)
			{
				/* compatible only if both sides ran out of live attributes */
				if (pgDone != icebergDone)
					compatible = false;
				break;
			}

			Form_pg_attribute pgAttr = TupleDescAttr(pgTupleDesc, pgIndex);
			Form_pg_attribute icebergAttr = TupleDescAttr(icebergTupleDesc, icebergIndex);
			PGType		pgAttrType = MakePGType(pgAttr->atttypid, pgAttr->atttypmod);
			PGType		icebergAttrType = MakePGType(icebergAttr->atttypid, icebergAttr->atttypmod);

			if (pg_strcasecmp(NameStr(pgAttr->attname), NameStr(icebergAttr->attname)) != 0 ||
				!TypesAreCompatible(pgAttrType, icebergAttrType))
				compatible = false;

			pgIndex++;
			icebergIndex++;
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
								  List **fileScans, List **positionDeleteScans,
								  List **equalityDeleteScans, List **equalityDeleteReadGroups)
{
	List	   *dataFiles = NIL;
	List	   *deleteFiles = NIL;

	FetchAllDataAndDeleteFilesFromCurrentSnapshot(metadata, &dataFiles, &deleteFiles);

	List	   *retainedFiles = PruneDataFiles(relationId, dataFiles, baseRestrictInfoList, PARTIAL_MATCH);
	List	   *fileConstants = FileConstantsForDataFiles(metadata, retainedFiles);

	ConvertIcebergDataFilesToFileScan(retainedFiles, deleteFiles, fileConstants,
									  fileScans, positionDeleteScans);
	*equalityDeleteReadGroups = PlanIcebergEqualityDeletes(metadata, retainedFiles, deleteFiles,
														   *fileScans, equalityDeleteScans);
}


/*
* ConvertIcebergDataFilesToFileScan converts a list of Iceberg data files
* to a list of PgLakeFileScan.
*/
static void
ConvertIcebergDataFilesToFileScan(List *dataFiles, List *deleteFiles,
								  List *fileConstants,
								  List **fileScans, List **positionDeleteFileScans)
{
	if (dataFiles == NIL)
		return;

	ListCell   *dataFileCell = NULL;

	foreach(dataFileCell, dataFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);

		ErrorIfDataFileNotParquet(dataFile);

		PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

		fileScan->path = (char *) dataFile->file_path;
		fileScan->rowCount = dataFile->record_count;
		fileScan->deletedRowCount = 0;
		if (fileConstants != NIL)
			fileScan->constants = list_nth(fileConstants, foreach_current_index(dataFileCell));

		*fileScans = lappend(*fileScans, fileScan);
	}

	dataFileCell = NULL;
	foreach(dataFileCell, deleteFiles)
	{
		DataFile   *dataFile = lfirst(dataFileCell);

		if (dataFile->content != ICEBERG_DATA_FILE_CONTENT_POSITION_DELETES)
			continue;

		ErrorIfDataFileNotParquet(dataFile);

		PgLakeFileScan *fileScan = palloc0(sizeof(PgLakeFileScan));

		fileScan->path = (char *) dataFile->file_path;
		fileScan->rowCount = dataFile->record_count;
		fileScan->deletedRowCount = 0;

		*positionDeleteFileScans = lappend(*positionDeleteFileScans, fileScan);
	}

}


/*
 * IdentityPartitionSource is an identity partition field of any of a table's
 * partition specs, whose source is a column of the current schema.
 */
typedef struct IdentityPartitionSource
{
	int32_t		partitionFieldId;
	int32_t		sourceFieldId;
	PGType		pgType;
}			IdentityPartitionSource;


/*
 * IdentityPartitionFieldsForFileConstants returns the IdentityPartitionSources
 * whose values a scan takes from each data file's partition rather than from
 * the file.
 *
 * The Iceberg spec reads a field a data file does not have from its identity
 * partition value. Files written without field ids, such as those Spark's
 * add_files, migrate and snapshot procedures take over from a Hive table,
 * keep a partition column's value only in their path, and those tables have
 * a name mapping. Files with field ids hold every column, so a table without
 * a name mapping gets none.
 *
 * The value is the same in every row of a file either way, so it is taken
 * whether or not the file has the column.
 */
static List *
IdentityPartitionFieldsForFileConstants(IcebergTableMetadata * metadata)
{
	bool		hasNameMapping = false;

	for (size_t propertyIdx = 0; propertyIdx < metadata->properties_length; propertyIdx++)
	{
		if (strcmp(metadata->properties[propertyIdx].key, "schema.name-mapping.default") == 0)
			hasNameMapping = true;
	}

	if (!hasNameMapping)
		return NIL;

	IcebergTableSchema *schema = GetCurrentIcebergTableSchema(metadata);
	List	   *identityFields = NIL;

	for (size_t specIdx = 0; specIdx < metadata->partition_specs_length; specIdx++)
	{
		IcebergPartitionSpec *spec = &metadata->partition_specs[specIdx];

		for (size_t fieldIdx = 0; fieldIdx < spec->fields_length; fieldIdx++)
		{
			IcebergPartitionSpecField *specField = &spec->fields[fieldIdx];

			if (strcmp(specField->transform, "identity") != 0)
				continue;

			for (size_t schemaIdx = 0; schemaIdx < schema->fields_length; schemaIdx++)
			{
				DataFileSchemaField *column = &schema->fields[schemaIdx];

				if (column->id != specField->source_id ||
					column->type->type != FIELD_TYPE_SCALAR)
					continue;

				IdentityPartitionSource *source = palloc0(sizeof(IdentityPartitionSource));

				source->partitionFieldId = specField->field_id;
				source->sourceFieldId = column->id;
				source->pgType = IcebergFieldToPostgresType(column->type);
				identityFields = lappend(identityFields, source);
				break;
			}
		}
	}

	return identityFields;
}


/*
 * FileConstantsForDataFiles returns, for each of the given data files of the
 * table, the PgLakeFileConstants a scan takes for its identity partition
 * columns (see IdentityPartitionFieldsForFileConstants), or NIL if the table
 * has none.
 */
List *
FileConstantsForDataFiles(IcebergTableMetadata * metadata, List *dataFiles)
{
	List	   *identityPartitionFields = IdentityPartitionFieldsForFileConstants(metadata);
	List	   *fileConstants = NIL;

	if (identityPartitionFields == NIL)
		return NIL;

	int			nestLevel = set_transmission_modes();

	foreach_ptr(DataFile, dataFile, dataFiles)
		fileConstants = lappend(fileConstants,
								FileConstantsForDataFile(dataFile, identityPartitionFields));

	reset_transmission_modes(nestLevel);

	return fileConstants;
}


/*
 * FileConstantsForDataFile returns PgLakeFileConstants for the values of the
 * data file's identity partition fields.
 */
static List *
FileConstantsForDataFile(DataFile * dataFile, List *identityPartitionFields)
{
	List	   *constants = NIL;

	for (size_t fieldIdx = 0; fieldIdx < dataFile->partition.fields_length; fieldIdx++)
	{
		PartitionField *partitionField = &dataFile->partition.fields[fieldIdx];

		foreach_ptr(IdentityPartitionSource, source, identityPartitionFields)
		{
			if (source->partitionFieldId != partitionField->field_id)
				continue;

			PgLakeFileConstant *constant = palloc0(sizeof(PgLakeFileConstant));
			bool		isNull = false;
			Datum		value = PartitionValueToDatum(PARTITION_TRANSFORM_IDENTITY,
													  partitionField->value,
													  partitionField->value_length,
													  source->pgType, &isNull);

			constant->fieldId = source->sourceFieldId;
			if (!isNull)
				constant->value = FileConstantText(value, source->pgType);

			constants = lappend(constants, constant);
			break;
		}
	}

	return constants;
}


/*
 * FileConstantText returns the text of a value that DuckDB casts back to the
 * same value of the column's type.
 */
static char *
FileConstantText(Datum value, PGType pgType)
{
	if (pgType.postgresTypeOid == BYTEAOID)
	{
		bytea	   *bytes = DatumGetByteaPP(value);
		StringInfoData escaped;

		initStringInfo(&escaped);
		for (int i = 0; i < VARSIZE_ANY_EXHDR(bytes); i++)
			appendStringInfo(&escaped, "\\x%02X", (unsigned char) VARDATA_ANY(bytes)[i]);

		return escaped.data;
	}

	Oid			typeOutput;
	bool		typeIsVarlena;

	getTypeOutputInfo(pgType.postgresTypeOid, &typeOutput, &typeIsVarlena);

	char	   *output = OidOutputFunctionCall(typeOutput, value);

	/* DuckDB reads "<date> BC[ <time>]" as AD, but "<date> (BC)[ <time>]" */
	if ((pgType.postgresTypeOid == DATEOID ||
		 pgType.postgresTypeOid == TIMESTAMPOID ||
		 pgType.postgresTypeOid == TIMESTAMPTZOID) &&
		pg_str_endswith(output, " BC"))
	{
		output[strlen(output) - strlen(" BC")] = '\0';

		char	   *time = strchr(output, ' ');

		if (time == NULL)
			return psprintf("%s (BC)", output);

		*time = '\0';
		return psprintf("%s (BC) %s", output, time + 1);
	}

	return output;
}


/*
 * FileConstantsArgument returns the file_constants argument of read_parquet
 * for the PgLakeFileConstants lists of the files at the given paths, or NULL
 * if they have none: a line per file and field, "<path>\t<field id>\t<value>",
 * with \N for NULL and backslash escapes, quoted as one string, which DuckDB
 * parses much faster than a literal with an entry per file.
 */
char *
FileConstantsArgument(List *paths, List *fileConstants)
{
	StringInfoData lines;
	ListCell   *pathCell = NULL;
	ListCell   *constantsCell = NULL;

	initStringInfo(&lines);

	forboth(pathCell, paths, constantsCell, fileConstants)
	{
		foreach_ptr(PgLakeFileConstant, constant, (List *) lfirst(constantsCell))
		{
			AppendEscapedFileConstant(&lines, lfirst(pathCell));
			appendStringInfo(&lines, "\t%d\t", constant->fieldId);
			if (constant->value == NULL)
				appendStringInfoString(&lines, "\\N");
			else
				AppendEscapedFileConstant(&lines, constant->value);
			appendStringInfoChar(&lines, '\n');
		}
	}

	if (lines.len == 0)
		return NULL;

	return quote_literal_cstr(lines.data);
}


/*
 * AppendEscapedFileConstant appends text with its backslashes, tabs and
 * newlines escaped for FileConstantsArgument.
 */
static void
AppendEscapedFileConstant(StringInfo buffer, const char *text)
{
	for (const char *c = text; *c != '\0'; c++)
	{
		if (*c == '\\')
			appendStringInfoString(buffer, "\\\\");
		else if (*c == '\t')
			appendStringInfoString(buffer, "\\t");
		else if (*c == '\n')
			appendStringInfoString(buffer, "\\n");
		else
			appendStringInfoChar(buffer, *c);
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
		int			curDeleteFileScans = list_length(tableScan->positionDeleteScans) + list_length(tableScan->equalityDeleteScans);

		*dataFileScans += curDataFileScans;
		*deleteFileScans += curDeleteFileScans;
	}
}
