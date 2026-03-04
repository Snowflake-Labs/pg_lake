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
 * sync_external_metadata.c
 *
 * Syncs the internal pg_lake catalog state (schema, partition specs, data files)
 * from new Iceberg metadata written by an external client.
 *
 * Called via:
 *   SELECT lake_table.sync_iceberg_metadata_from_external_write(regclass)
 *
 * This is triggered by an UPDATE to the iceberg_tables view for tables in the
 * current database catalog.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/data_file/data_files.h"
#include "pg_lake/data_file/data_file_stats.h"
#include "pg_lake/ddl/alter_table.h"
#include "pg_lake/extensions/pg_lake_iceberg.h"
#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/fdw/data_file_stats_catalog.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/fdw/writable_table.h"
#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/data_file_stats.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_extension_base/spi_helpers.h"
#include "utils/hsearch.h"

PG_FUNCTION_INFO_V1(sync_iceberg_metadata_from_external_write);

static void SyncSchemaFromMetadata(Oid relationId, IcebergTableMetadata *metadata);
static void FetchExistingFieldMappings(Oid relationId, int **fieldIds,
									   int16 **attnums, int *count);
static void ExecuteAlterTableViaSPI(const char *cmd);
static void SyncPartitionSpecsFromMetadata(Oid relationId, IcebergTableMetadata *metadata);
static void SyncDataFilesFromMetadata(Oid relationId, IcebergTableMetadata *metadata);
static char *ColumnBoundBinaryToText(ColumnBound *bound, int fieldId,
									 IcebergTableSchema *schema);
static List *BuildColumnStatsForManifestEntry(IcebergManifestEntry *manifestEntry,
											  IcebergTableSchema *schema);


/*
 * sync_iceberg_metadata_from_external_write syncs the pg_lake internal
 * catalog state from the current Iceberg metadata for the given table.
 *
 * This performs three sub-steps:
 * 1. Schema sync: add/drop columns on the foreign table to match the
 *    Iceberg schema, and update field_id_mappings.
 * 2. Partition spec sync: register any new partition specs.
 * 3. Data file full resync: clear and repopulate lake_table.files and
 *    associated stats/partition values from the metadata snapshot.
 */
Datum
sync_iceberg_metadata_from_external_write(PG_FUNCTION_ARGS)
{
	Oid			relationId = PG_GETARG_OID(0);

	/* read the new metadata from object storage */
	bool		forUpdate = false;
	char	   *metadataLocation = GetIcebergMetadataLocation(relationId, forUpdate);

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataLocation);

	/*
	 * Suppress the ProcessAlterTable hook's Iceberg DDL processing while we
	 * add/drop columns.  We manage field_id_mappings ourselves and do not want
	 * the hook to register duplicate mappings or schedule a metadata write.
	 */
	SkipIcebergDDLProcessing = true;

	PG_TRY();
	{
		SyncSchemaFromMetadata(relationId, metadata);
		SyncPartitionSpecsFromMetadata(relationId, metadata);
		SyncDataFilesFromMetadata(relationId, metadata);
	}
	PG_FINALLY();
	{
		SkipIcebergDDLProcessing = false;
	}
	PG_END_TRY();

	PG_RETURN_VOID();
}


/*
 * FetchExistingFieldMappings retrieves all top-level field_id_mappings
 * for the given relation via SPI.
 */
static void
FetchExistingFieldMappings(Oid relationId, int **fieldIds,
						   int16 **attnums, int *count)
{
	MemoryContext callerContext = CurrentMemoryContext;

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeIceberg);

	bool		readOnly = true;

	SPI_EXECUTE("SELECT field_id, pg_attnum FROM " MAPPING_TABLE_NAME
				" WHERE table_name OPERATOR(pg_catalog.=) $1"
				" AND parent_field_id IS NULL", readOnly);

	*count = SPI_processed;
	*fieldIds = NULL;
	*attnums = NULL;

	if (*count > 0)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		*fieldIds = palloc(sizeof(int) * *count);
		*attnums = palloc(sizeof(int16) * *count);

		MemoryContextSwitchTo(spiContext);

		for (int i = 0; i < *count; i++)
		{
			bool		isNull = false;

			(*fieldIds)[i] = GET_SPI_VALUE(INT4OID, i, 1, &isNull);
			(*attnums)[i] = GET_SPI_VALUE(INT2OID, i, 2, &isNull);
		}
	}

	SPI_END();
}


/*
 * ExecuteAlterTableViaSPI runs an ALTER TABLE DDL command via SPI as the
 * pg_lake_table extension owner.
 */
static void
ExecuteAlterTableViaSPI(const char *cmd)
{
	SPI_START_EXTENSION_OWNER(PgLakeTable);
	SPI_execute(cmd, false, 0);
	SPI_END();
}


/*
 * SyncSchemaFromMetadata syncs the foreign table columns and field_id_mappings
 * with the current Iceberg schema from the metadata.
 *
 * For each field in the new Iceberg schema that doesn't have a mapping in
 * field_id_mappings, we ADD COLUMN and register the mapping.
 *
 * For each top-level mapping whose field_id is no longer in the current
 * Iceberg schema, we DROP COLUMN (but keep the mapping for reading old files).
 */
static void
SyncSchemaFromMetadata(Oid relationId, IcebergTableMetadata *metadata)
{
	IcebergTableSchema *icebergSchema = GetCurrentIcebergTableSchema(metadata);

	/* fetch existing field_id_mappings */
	int		   *existingFieldIds = NULL;
	int16	   *existingAttnums = NULL;
	int			existingMappingCount = 0;

	FetchExistingFieldMappings(relationId, &existingFieldIds,
							   &existingAttnums, &existingMappingCount);

	/* get relation's schema and namespace names for ALTER TABLE */
	char	   *schemaName = get_namespace_name(get_rel_namespace(relationId));
	char	   *tableName = get_rel_name(relationId);

	/*
	 * Step 1: Add new columns. For each field in the Iceberg schema that
	 * doesn't have a mapping, add it.
	 */
	for (size_t fieldIdx = 0; fieldIdx < icebergSchema->fields_length; fieldIdx++)
	{
		DataFileSchemaField *icebergField = &icebergSchema->fields[fieldIdx];
		int			fieldId = icebergField->id;

		/* check if this field_id already has a mapping */
		bool		found = false;

		for (int mappingIdx = 0; mappingIdx < existingMappingCount; mappingIdx++)
		{
			if (existingFieldIds[mappingIdx] == fieldId)
			{
				found = true;
				break;
			}
		}

		if (found)
			continue;

		/* new field: convert Iceberg type to Postgres type */
		PGType		pgType = IcebergFieldToPostgresType(icebergField->type);

		char	   *pgTypeName = format_type_with_typemod(pgType.postgresTypeOid,
														  pgType.postgresTypeMod);

		/* execute ALTER FOREIGN TABLE ... ADD COLUMN via SPI */
		StringInfo	alterCmd = makeStringInfo();

		appendStringInfo(alterCmd,
						 "ALTER FOREIGN TABLE %s.%s ADD COLUMN %s %s",
						 quote_identifier(schemaName),
						 quote_identifier(tableName),
						 quote_identifier(icebergField->name),
						 pgTypeName);

		ExecuteAlterTableViaSPI(alterCmd->data);

		/*
		 * After ADD COLUMN, look up the new attnum from pg_attribute.
		 */
		Relation	rel = RelationIdGetRelation(relationId);
		TupleDesc	tupdesc = RelationGetDescr(rel);
		AttrNumber	newAttNum = InvalidAttrNumber;

		for (int attIdx = 0; attIdx < tupdesc->natts; attIdx++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attIdx);

			if (!attr->attisdropped &&
				strcmp(NameStr(attr->attname), icebergField->name) == 0)
			{
				newAttNum = attr->attnum;
				break;
			}
		}

		RelationClose(rel);

		if (newAttNum == InvalidAttrNumber)
			elog(ERROR, "could not find column \"%s\" after ADD COLUMN",
				 icebergField->name);

		/* register the field mapping */
		int			parentFieldId = INVALID_FIELD_ID;
		const char *writeDefault = icebergField->writeDefault;
		const char *initialDefault = icebergField->initialDefault;

		RegisterIcebergColumnMapping(relationId, icebergField->type,
									 newAttNum, parentFieldId, pgType,
									 fieldId, writeDefault, initialDefault);
	}

	/*
	 * Step 2: Drop removed columns. For each existing mapping whose field_id
	 * is not in the current Iceberg schema, drop the column from the foreign
	 * table.
	 */
	for (int mappingIdx = 0; mappingIdx < existingMappingCount; mappingIdx++)
	{
		int			mappedFieldId = existingFieldIds[mappingIdx];
		AttrNumber	mappedAttnum = existingAttnums[mappingIdx];

		/* check if this field_id still exists in the Iceberg schema */
		bool		stillExists = false;

		for (size_t fieldIdx = 0; fieldIdx < icebergSchema->fields_length; fieldIdx++)
		{
			if (icebergSchema->fields[fieldIdx].id == mappedFieldId)
			{
				stillExists = true;
				break;
			}
		}

		if (stillExists)
			continue;

		/*
		 * Check if the column is already dropped in pg_attribute (could happen
		 * if this sync runs multiple times).
		 */
		Relation	rel = RelationIdGetRelation(relationId);
		TupleDesc	tupdesc = RelationGetDescr(rel);

		if (mappedAttnum <= 0 || mappedAttnum > tupdesc->natts)
		{
			RelationClose(rel);
			continue;
		}

		Form_pg_attribute attr = TupleDescAttr(tupdesc, mappedAttnum - 1);

		if (attr->attisdropped)
		{
			RelationClose(rel);
			continue;
		}

		char	   *colName = pstrdup(NameStr(attr->attname));

		RelationClose(rel);

		/* execute ALTER FOREIGN TABLE ... DROP COLUMN via SPI */
		StringInfo	dropCmd = makeStringInfo();

		appendStringInfo(dropCmd,
						 "ALTER FOREIGN TABLE %s.%s DROP COLUMN %s",
						 quote_identifier(schemaName),
						 quote_identifier(tableName),
						 quote_identifier(colName));

		ExecuteAlterTableViaSPI(dropCmd->data);

		/*
		 * We keep the field_id_mapping row: it is needed for reading older
		 * data files that reference this field.
		 */
	}
}


/*
 * SyncPartitionSpecsFromMetadata syncs the partition specs from the
 * Iceberg metadata to the pg_lake catalog.
 *
 * For each spec in the metadata that doesn't already exist in the catalog,
 * we register it. We also update the default_spec_id.
 */
static void
SyncPartitionSpecsFromMetadata(Oid relationId, IcebergTableMetadata *metadata)
{
	/* get the largest spec_id currently in catalog */
	int			largestCatalogSpecId = GetLargestSpecId(relationId);

	for (int specIdx = 0; specIdx < metadata->partition_specs_length; specIdx++)
	{
		IcebergPartitionSpec *spec = &metadata->partition_specs[specIdx];

		if (spec->spec_id <= largestCatalogSpecId)
			continue;

		InsertPartitionSpecAndPartitionFields(relationId, spec);
	}

	/* update the default spec id */
	UpdateDefaultPartitionSpecId(relationId, metadata->default_spec_id);
}


/*
 * SyncDataFilesFromMetadata performs a full resync of the data files in the
 * pg_lake catalog from the current snapshot in the Iceberg metadata.
 *
 * This clears all existing data files and repopulates them from the metadata.
 * Files that are no longer referenced are added to the deletion queue.
 */
static void
SyncDataFilesFromMetadata(Oid relationId, IcebergTableMetadata *metadata)
{
	TimestampTz orphanedAt = GetCurrentTransactionStartTimestamp();

	/*
	 * Get the list of old file paths before clearing, so we can queue
	 * unreferenced files for deletion.
	 */
	bool		dataOnly = false;
	bool		newFilesOnly = false;
	bool		forUpdate = false;
	Snapshot	snapshot = GetTransactionSnapshot();

	List	   *oldDataFiles = GetTableDataFilesFromCatalog(relationId, dataOnly,
															newFilesOnly, forUpdate,
															NULL, snapshot);

	/* build a hash table of old file paths for quick lookup */
	HTAB	   *oldFileHash = NULL;

	if (oldDataFiles != NIL)
	{
		HASHCTL		hashCtl;

		memset(&hashCtl, 0, sizeof(hashCtl));
		hashCtl.keysize = MAX_S3_PATH_LENGTH;
		hashCtl.entrysize = MAX_S3_PATH_LENGTH;
		hashCtl.hcxt = CurrentMemoryContext;

		oldFileHash = hash_create("old file paths",
								  list_length(oldDataFiles),
								  &hashCtl,
								  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);

		ListCell   *oldFileCell = NULL;

		foreach(oldFileCell, oldDataFiles)
		{
			TableDataFile *oldFile = lfirst(oldFileCell);
			bool		found = false;

			hash_search(oldFileHash, oldFile->path, HASH_ENTER, &found);
		}
	}

	/* clear all existing data files from the catalog */
	RemoveAllDataFilesFromPgLakeCatalogFromTable(relationId);

	/* get the current snapshot */
	bool		missingOk = true;
	IcebergSnapshot *currentSnapshot = GetCurrentSnapshot(metadata, missingOk);

	if (currentSnapshot == NULL)
	{
		/*
		 * Empty table after external write. All old files are now unreferenced,
		 * queue them for deletion.
		 */
		if (oldFileHash != NULL)
		{
			HASH_SEQ_STATUS hashSeq;
			char	   *filePath = NULL;

			hash_seq_init(&hashSeq, oldFileHash);
			while ((filePath = hash_seq_search(&hashSeq)) != NULL)
			{
				InsertDeletionQueueRecord(filePath, relationId, orphanedAt);
			}
		}
		return;
	}

	IcebergTableSchema *icebergSchema = GetCurrentIcebergTableSchema(metadata);

	/* fetch all manifests from the current snapshot */
	List	   *manifests = FetchManifestsFromSnapshot(currentSnapshot, NULL);

	/* track new file paths to identify unreferenced files later */
	HTAB	   *newFileHash = NULL;

	if (oldFileHash != NULL)
	{
		HASHCTL		hashCtl;

		memset(&hashCtl, 0, sizeof(hashCtl));
		hashCtl.keysize = MAX_S3_PATH_LENGTH;
		hashCtl.entrysize = MAX_S3_PATH_LENGTH;
		hashCtl.hcxt = CurrentMemoryContext;

		newFileHash = hash_create("new file paths",
								  1024,		/* initial estimate */
								  &hashCtl,
								  HASH_ELEM | HASH_STRINGS | HASH_CONTEXT);
	}

	ListCell   *manifestCell = NULL;

	foreach(manifestCell, manifests)
	{
		IcebergManifest *manifest = lfirst(manifestCell);

		List	   *manifestEntries =
			FetchManifestEntriesFromManifest(manifest,
											 IsManifestEntryStatusScannable);

		ListCell   *entryCell = NULL;

		foreach(entryCell, manifestEntries)
		{
			IcebergManifestEntry *entry = lfirst(entryCell);

			DataFile   *dataFile = &entry->data_file;

			/* track this new file path */
			if (newFileHash != NULL)
			{
				bool		found = false;

				hash_search(newFileHash, dataFile->file_path, HASH_ENTER, &found);
			}

			/* map Iceberg content type to pg_lake content type */
			DataFileContent content;

			switch (dataFile->content)
			{
				case ICEBERG_DATA_FILE_CONTENT_DATA:
					content = CONTENT_DATA;
					break;
				case ICEBERG_DATA_FILE_CONTENT_POSITION_DELETES:
					content = CONTENT_POSITION_DELETES;
					break;
				case ICEBERG_DATA_FILE_CONTENT_EQUALITY_DELETES:
					content = CONTENT_EQUALITY_DELETES;
					break;
				default:
					elog(ERROR, "unsupported data file content type: %d",
						 dataFile->content);
			}

			/* insert the data file into the catalog */
			int64		fileId = AddDataFileToTable(relationId,
												   dataFile->file_path,
												   dataFile->record_count,
												   dataFile->file_size_in_bytes,
												   content,
												   INVALID_ROW_ID);

			/* insert column stats if available */
			if (content == CONTENT_DATA)
			{
				List	   *columnStatsList =
					BuildColumnStatsForManifestEntry(entry, icebergSchema);

				if (columnStatsList != NIL)
					AddDataFileColumnStatsToCatalog(relationId,
													dataFile->file_path,
													columnStatsList);
			}

			/* insert partition values if available */
			if (dataFile->partition.fields_length > 0 &&
				(content == CONTENT_DATA ||
				 content == CONTENT_POSITION_DELETES))
			{
				AddDataFilePartitionValueToCatalog(relationId,
												   manifest->partition_spec_id,
												   fileId,
												   &dataFile->partition);
			}
		}
	}

	/*
	 * Queue old files that are no longer referenced for deletion.
	 * Compare oldFileHash with newFileHash to find unreferenced files.
	 */
	if (oldFileHash != NULL && newFileHash != NULL)
	{
		HASH_SEQ_STATUS hashSeq;
		char	   *oldFilePath = NULL;

		hash_seq_init(&hashSeq, oldFileHash);
		while ((oldFilePath = hash_seq_search(&hashSeq)) != NULL)
		{
			bool		found = false;

			hash_search(newFileHash, oldFilePath, HASH_FIND, &found);

			if (!found)
			{
				/* file is no longer referenced, queue for deletion */
				InsertDeletionQueueRecord(oldFilePath, relationId, orphanedAt);
			}
		}
	}
}


/*
 * BuildColumnStatsForManifestEntry builds a list of DataFileColumnStats from
 * the lower_bounds and upper_bounds in the manifest entry's data file.
 *
 * Each bound is stored in Iceberg binary format and needs to be converted
 * to Postgres text representation for the catalog.
 */
static List *
BuildColumnStatsForManifestEntry(IcebergManifestEntry *manifestEntry,
								 IcebergTableSchema *schema)
{
	DataFile   *dataFile = &manifestEntry->data_file;

	if (dataFile->lower_bounds_length == 0)
		return NIL;

	List	   *statsList = NIL;

	for (size_t lowerIdx = 0; lowerIdx < dataFile->lower_bounds_length; lowerIdx++)
	{
		ColumnBound *lowerBound = &dataFile->lower_bounds[lowerIdx];
		int			fieldId = lowerBound->column_id;

		/* find matching upper bound */
		ColumnBound *upperBound = NULL;

		for (size_t upperIdx = 0; upperIdx < dataFile->upper_bounds_length; upperIdx++)
		{
			if (dataFile->upper_bounds[upperIdx].column_id == fieldId)
			{
				upperBound = &dataFile->upper_bounds[upperIdx];
				break;
			}
		}

		char	   *lowerBoundText = ColumnBoundBinaryToText(lowerBound, fieldId, schema);
		char	   *upperBoundText = NULL;

		if (upperBound != NULL)
			upperBoundText = ColumnBoundBinaryToText(upperBound, fieldId, schema);

		if (lowerBoundText == NULL)
			continue;

		/* find the iceberg field in the schema to get the PGType */
		DataFileSchemaField *icebergField = NULL;

		for (size_t fieldIdx = 0; fieldIdx < schema->fields_length; fieldIdx++)
		{
			if (schema->fields[fieldIdx].id == fieldId)
			{
				icebergField = &schema->fields[fieldIdx];
				break;
			}
		}

		if (icebergField == NULL)
			continue;

		PGType		pgType = IcebergFieldToPostgresType(icebergField->type);

		DataFileColumnStats *colStats = palloc0(sizeof(DataFileColumnStats));

		colStats->leafField.fieldId = fieldId;
		colStats->leafField.pgType = pgType;
		colStats->lowerBoundText = lowerBoundText;
		colStats->upperBoundText = upperBoundText;

		bool		forAddColumn = false;
		int			subFieldIndex = fieldId;

		colStats->leafField.field = PostgresTypeToIcebergField(pgType, forAddColumn,
															   &subFieldIndex);
		colStats->leafField.duckTypeName =
			IcebergTypeNameToDuckdbTypeName(colStats->leafField.field->field.scalar.typeName);

		statsList = lappend(statsList, colStats);
	}

	return statsList;
}


/*
 * ColumnBoundBinaryToText converts an Iceberg binary column bound to
 * Postgres text representation.
 *
 * Returns NULL if the field is not found in the schema or if the
 * deserialization fails.
 */
static char *
ColumnBoundBinaryToText(ColumnBound *bound, int fieldId,
						IcebergTableSchema *schema)
{
	if (bound->value == NULL || bound->value_length == 0)
		return NULL;

	/* find the Iceberg field in the schema */
	DataFileSchemaField *icebergField = NULL;

	for (size_t fieldIdx = 0; fieldIdx < schema->fields_length; fieldIdx++)
	{
		if (schema->fields[fieldIdx].id == fieldId)
		{
			icebergField = &schema->fields[fieldIdx];
			break;
		}
	}

	if (icebergField == NULL)
		return NULL;

	PGType		pgType = IcebergFieldToPostgresType(icebergField->type);

	/* deserialize from Iceberg binary to Postgres Datum */
	Datum		boundDatum = PGIcebergBinaryDeserialize(bound->value,
													   bound->value_length,
													   icebergField->type,
													   pgType);

	/* convert Datum to text representation */
	Oid			typoutput;
	bool		typIsVarlena;

	getTypeOutputInfo(pgType.postgresTypeOid, &typoutput, &typIsVarlena);

	char	   *boundText = OidOutputFunctionCall(typoutput, boundDatum);

	return boundText;
}
