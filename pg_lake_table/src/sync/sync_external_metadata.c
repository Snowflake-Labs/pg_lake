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
 * Syncs the internal pg_lake catalog state (schema, partition specs, data
 * files) from new Iceberg metadata written by an external client. Driven
 * by the INSTEAD OF trigger on pg_catalog.iceberg_tables (UPDATE for
 * existing tables, INSERT for newly registered tables).
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/htup_details.h"
#include "access/relation.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "commands/dbcommands.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

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
#include "pg_lake/iceberg/operations/find_referenced_files.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_extension_base/spi_helpers.h"
#include "utils/hsearch.h"

PG_FUNCTION_INFO_V1(internal_catalog_modification);

static void SyncFromExternalMetadata(Oid relationId);
static void HandleInternalCatalogUpdate(char *namespaceName, char *tableName,
										char *metadataLocation, char *prevMetadataLocation);
static void HandleInternalCatalogInsert(char *namespaceName, char *tableName,
										char *metadataLocation);
static void HandleInternalCatalogDelete(char *namespaceName, char *tableName);
static void SyncSchemaFromMetadata(Oid relationId, IcebergTableMetadata * metadata);
static void FetchExistingFieldMappings(Oid relationId, int **fieldIds,
									   int16 **attnums, int *count);
static void ExecuteAlterTableViaSPI(const char *cmd);
static void SyncPartitionSpecsFromMetadata(Oid relationId, IcebergTableMetadata * metadata);
static void SyncDataFilesFromMetadata(Oid relationId, IcebergTableMetadata * metadata,
									  const char *metadataLocation);
static char *ColumnBoundBinaryToText(ColumnBound * bound, int fieldId,
									 IcebergTableSchema * schema);
static List *BuildColumnStatsForManifestEntry(IcebergManifestEntry * manifestEntry,
											  IcebergTableSchema * schema);


/*
 * SyncFromExternalMetadata syncs the pg_lake internal catalog state from
 * the current Iceberg metadata for the given table:
 *   1. Schema sync: add/drop columns on the foreign table to match the
 *      Iceberg schema, and update field_id_mappings.
 *   2. Partition spec sync: register any new partition specs.
 *   3. Data file full resync: clear and repopulate lake_table.files and
 *      associated stats/partition values from the metadata snapshot.
 *
 * Used by both the UPDATE trigger path (existing tables) and the INSERT
 * trigger path (newly registered tables).
 */
static void
SyncFromExternalMetadata(Oid relationId)
{
	/* read the new metadata from object storage */
	bool		forUpdate = false;
	char	   *metadataLocation = GetIcebergMetadataLocation(relationId, forUpdate);

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataLocation);

	/*
	 * Suppress the ProcessAlterTable / PostProcessRename hooks' Iceberg DDL
	 * processing while we add/drop/rename columns. We manage
	 * field_id_mappings ourselves and the metadata is already final on disk,
	 * so we don't want the hooks to register duplicate mappings or schedule a
	 * metadata write.
	 *
	 * The flag is process-global; we restore it in PG_FINALLY to avoid
	 * leaking state if the sync errors out partway through.
	 */
	bool		previousSkip = SkipIcebergDDLProcessing;

	SkipIcebergDDLProcessing = true;

	PG_TRY();
	{
		SyncSchemaFromMetadata(relationId, metadata);
		SyncPartitionSpecsFromMetadata(relationId, metadata);
		SyncDataFilesFromMetadata(relationId, metadata, metadataLocation);
	}
	PG_FINALLY();
	{
		SkipIcebergDDLProcessing = previousSkip;
	}
	PG_END_TRY();
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
SyncSchemaFromMetadata(Oid relationId, IcebergTableMetadata * metadata)
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
		AttrNumber	existingAttnum = InvalidAttrNumber;

		for (int mappingIdx = 0; mappingIdx < existingMappingCount; mappingIdx++)
		{
			if (existingFieldIds[mappingIdx] == fieldId)
			{
				found = true;
				existingAttnum = existingAttnums[mappingIdx];
				break;
			}
		}

		if (found)
		{
			/*
			 * Field is already mapped — but the new schema may have renamed
			 * it. Compare names and issue RENAME COLUMN if so. Iceberg field
			 * IDs are stable across rename, so this is the only signal we
			 * have that a rename happened.
			 */
			Relation	rel = RelationIdGetRelation(relationId);
			TupleDesc	tupdesc = RelationGetDescr(rel);

			if (existingAttnum <= 0 || existingAttnum > tupdesc->natts)
			{
				RelationClose(rel);
				continue;
			}

			Form_pg_attribute attr = TupleDescAttr(tupdesc, existingAttnum - 1);

			if (attr->attisdropped)
			{
				RelationClose(rel);
				continue;
			}

			if (strcmp(NameStr(attr->attname), icebergField->name) == 0)
			{
				RelationClose(rel);
				continue;
			}

			char	   *currentName = pstrdup(NameStr(attr->attname));

			RelationClose(rel);

			StringInfo	renameCmd = makeStringInfo();

			appendStringInfo(renameCmd,
							 "ALTER FOREIGN TABLE %s.%s RENAME COLUMN %s TO %s",
							 quote_identifier(schemaName),
							 quote_identifier(tableName),
							 quote_identifier(currentName),
							 quote_identifier(icebergField->name));

			ExecuteAlterTableViaSPI(renameCmd->data);

			continue;
		}

		/*
		 * No mapping for this field_id yet. The column may or may not already
		 * exist in the foreign table — if the table was created via an
		 * iceberg_tables INSERT trigger from an external client, the columns
		 * are already there from the CREATE FOREIGN TABLE but no
		 * field_id_mappings have been registered yet. Look up pg_attribute by
		 * name first; only ADD COLUMN if it isn't there.
		 */
		PGType		pgType = IcebergFieldToPostgresType(icebergField->type);

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
		{
			char	   *pgTypeName = format_type_with_typemod(pgType.postgresTypeOid,
															  pgType.postgresTypeMod);

			StringInfo	alterCmd = makeStringInfo();

			appendStringInfo(alterCmd,
							 "ALTER FOREIGN TABLE %s.%s ADD COLUMN %s %s",
							 quote_identifier(schemaName),
							 quote_identifier(tableName),
							 quote_identifier(icebergField->name),
							 pgTypeName);

			ExecuteAlterTableViaSPI(alterCmd->data);

			rel = RelationIdGetRelation(relationId);
			tupdesc = RelationGetDescr(rel);

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
		}

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
		 * Check if the column is already dropped in pg_attribute (could
		 * happen if this sync runs multiple times).
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
SyncPartitionSpecsFromMetadata(Oid relationId, IcebergTableMetadata * metadata)
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
 * The pg_lake catalog only ever indexes the current snapshot (we don't expose
 * time-travel reads), so we clear the catalog and repopulate it by walking
 * the current snapshot's manifests.
 *
 * For deletion-queue purposes we need to be more careful: Iceberg metadata
 * typically retains older snapshots, and external clients may keep them for
 * their own time-travel reads. A file referenced only by a non-current but
 * still-retained snapshot must NOT be queued for deletion. We compute the
 * set of files referenced by ANY snapshot in the new metadata using
 * IcebergFindAllReferencedFiles, and only queue old cataloged files that
 * are absent from that set.
 */
static void
SyncDataFilesFromMetadata(Oid relationId, IcebergTableMetadata * metadata,
						  const char *metadataLocation)
{
	TimestampTz orphanedAt = GetCurrentTransactionStartTimestamp();

	/*
	 * Get the list of old cataloged file paths before clearing, so we can
	 * later queue files no longer referenced by any retained snapshot.
	 */
	bool		dataOnly = false;
	bool		newFilesOnly = false;
	bool		forUpdate = false;
	Snapshot	snapshot = GetTransactionSnapshot();

	List	   *oldDataFiles = GetTableDataFilesFromCatalog(relationId, dataOnly,
															newFilesOnly, forUpdate,
															NULL, snapshot);

	/*
	 * Build the set of files referenced by ANY snapshot in the new metadata.
	 * IcebergFindAllReferencedFiles walks every snapshot in the metadata, not
	 * just the current one, so files held only by retained-but-not-current
	 * snapshots stay in this set and won't be queued for deletion below.
	 */
	HTAB	   *referencedFileHash = CreateFilesHash();

	if (oldDataFiles != NIL)
	{
		List	   *referencedFiles = IcebergFindAllReferencedFiles((char *) metadataLocation);
		ListCell   *fileCell = NULL;

		foreach(fileCell, referencedFiles)
		{
			char	   *path = lfirst(fileCell);

			AppendFileToHash(path, referencedFileHash);
		}
	}

	/* clear all existing data files from the catalog */
	RemoveAllDataFilesFromPgLakeCatalogFromTable(relationId);

	/* repopulate the catalog from the current snapshot, if any */
	bool		missingOk = true;
	IcebergSnapshot *currentSnapshot = GetCurrentSnapshot(metadata, missingOk);

	if (currentSnapshot != NULL)
	{
		IcebergTableSchema *icebergSchema = GetCurrentIcebergTableSchema(metadata);

		List	   *manifests = FetchManifestsFromSnapshot(currentSnapshot, NULL);
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

				int64		fileId = AddDataFileToTable(relationId,
														dataFile->file_path,
														dataFile->record_count,
														dataFile->file_size_in_bytes,
														content,
														INVALID_ROW_ID);

				if (content == CONTENT_DATA)
				{
					List	   *columnStatsList =
						BuildColumnStatsForManifestEntry(entry, icebergSchema);

					if (columnStatsList != NIL)
						AddDataFileColumnStatsToCatalog(relationId,
														dataFile->file_path,
														columnStatsList);
				}

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
	}

	/*
	 * Queue any previously-cataloged file that is not referenced by any
	 * retained snapshot in the new metadata. Files referenced only by older
	 * but still-retained snapshots are intentionally left alone.
	 */
	ListCell   *oldFileCell = NULL;

	foreach(oldFileCell, oldDataFiles)
	{
		TableDataFile *oldFile = lfirst(oldFileCell);
		bool		found = false;

		hash_search(referencedFileHash, oldFile->path, HASH_FIND, &found);

		if (!found)
			InsertDeletionQueueRecord(oldFile->path, relationId, orphanedAt);
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
BuildColumnStatsForManifestEntry(IcebergManifestEntry * manifestEntry,
								 IcebergTableSchema * schema)
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
ColumnBoundBinaryToText(ColumnBound * bound, int fieldId,
						IcebergTableSchema * schema)
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


/*
 * LocationPrefixFromMetadataLocation strips the trailing
 * "metadata/<n>-<uuid>.metadata.json" off an Iceberg metadata file URI to
 * derive the table's storage prefix, which is what pg_lake_iceberg
 * requires as its `location` server option.
 *
 * Iceberg metadata files always live at <prefix>/metadata/<file>.json,
 * so a string-level rfind on "/metadata/" is enough.
 */
static char *
LocationPrefixFromMetadataLocation(const char *metadataLocation)
{
	const char *cursor = strstr(metadataLocation, "/metadata/");
	const char *lastMarker = cursor;

	while (cursor != NULL)
	{
		lastMarker = cursor;
		cursor = strstr(cursor + 1, "/metadata/");
	}

	if (lastMarker == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("metadata_location \"%s\" does not contain a /metadata/ path segment",
						metadataLocation)));

	return pnstrdup(metadataLocation, lastMarker - metadataLocation);
}


/*
 * internal_catalog_modification is the INSTEAD OF trigger on
 * pg_catalog.iceberg_tables that pg_lake_table installs. It handles
 * only the *internal*-catalog path: rows whose catalog_name matches
 * the current database, i.e., tables backed by pg_lake foreign tables.
 *
 *   UPDATE  -> apply external metadata write to an existing pg_lake
 *              iceberg table (run sync on new metadata).
 *   INSERT  -> register a brand-new pg_lake iceberg table from a
 *              metadata file an external client wrote (synthesize
 *              CREATE FOREIGN TABLE, then sync).
 *   DELETE  -> drop the corresponding foreign table.
 *
 * The external-catalog path is owned by a sibling trigger in
 * pg_lake_iceberg — see lake_iceberg.external_catalog_modification.
 * Cross-boundary catalog renames are rejected by the sibling trigger
 * before we get here, so we don't re-validate.
 */
Datum
internal_catalog_modification(PG_FUNCTION_ARGS)
{
	TriggerData *trigdata = (TriggerData *) fcinfo->context;
	HeapTuple	rettuple;

	if (!CALLED_AS_TRIGGER(fcinfo))
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as a trigger")));

	if (!TRIGGER_FIRED_INSTEAD(trigdata->tg_event))
		ereport(ERROR,
				(errcode(ERRCODE_TRIGGERED_ACTION_EXCEPTION),
				 errmsg("must be called as an INSTEAD OF trigger")));

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		rettuple = trigdata->tg_newtuple;
	else
		rettuple = trigdata->tg_trigtuple;

	bool		isnull = false;
	Datum		catalogNameDatum = heap_getattr(rettuple, 1,
												trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "catalog_name cannot be NULL");

	Datum		namespaceDatum = heap_getattr(rettuple, 2,
											  trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_namespace cannot be NULL");

	Datum		tableNameDatum = heap_getattr(rettuple, 3,
											  trigdata->tg_relation->rd_att, &isnull);

	if (isnull)
		elog(ERROR, "table_name cannot be NULL");

	bool		metadataLocationIsNull = false;
	Datum		metadataLocationDatum = heap_getattr(rettuple, 4,
													 trigdata->tg_relation->rd_att,
													 &metadataLocationIsNull);
	bool		prevMetadataLocationIsNull = false;
	Datum		prevMetadataLocationDatum = heap_getattr(rettuple, 5,
														 trigdata->tg_relation->rd_att,
														 &prevMetadataLocationIsNull);

	char	   *catalogName = TextDatumGetCString(catalogNameDatum);
	char	   *namespaceName = TextDatumGetCString(namespaceDatum);
	char	   *tableName = TextDatumGetCString(tableNameDatum);
	char	   *metadataLocation = metadataLocationIsNull ? NULL :
		TextDatumGetCString(metadataLocationDatum);
	char	   *prevMetadataLocation = prevMetadataLocationIsNull ? NULL :
		TextDatumGetCString(prevMetadataLocationDatum);

	char	   *databaseName = get_database_name(MyDatabaseId);
	bool		isInternalCatalog = (strcmp(catalogName, databaseName) == 0);

	/* External-catalog rows are pg_lake_iceberg's responsibility. */
	if (!isInternalCatalog)
		return PointerGetDatum(rettuple);

	if (TRIGGER_FIRED_BY_UPDATE(trigdata->tg_event))
		HandleInternalCatalogUpdate(namespaceName, tableName,
									metadataLocation, prevMetadataLocation);
	else if (TRIGGER_FIRED_BY_INSERT(trigdata->tg_event))
		HandleInternalCatalogInsert(namespaceName, tableName, metadataLocation);
	else if (TRIGGER_FIRED_BY_DELETE(trigdata->tg_event))
		HandleInternalCatalogDelete(namespaceName, tableName);
	else
		pg_unreachable();

	return PointerGetDatum(rettuple);
}


/*
 * HandleInternalCatalogUpdate: UPDATE on iceberg_tables for tables in
 * the current database catalog. Validates optimistic concurrency via
 * previous_metadata_location, updates the internal catalog's stored
 * metadata location, and runs the sync.
 */
static void
HandleInternalCatalogUpdate(char *namespaceName, char *tableName,
							char *metadataLocation, char *prevMetadataLocation)
{
	if (metadataLocation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("metadata_location cannot be NULL")));

	if (prevMetadataLocation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("previous_metadata_location is required for "
						"optimistic concurrency control")));

	bool		missingOk = false;
	Oid			namespaceOid = get_namespace_oid(namespaceName, missingOk);
	Oid			relationId = get_relname_relid(tableName, namespaceOid);

	if (!OidIsValid(relationId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table \"%s.%s\" does not exist",
						namespaceName, tableName)));

	bool		forUpdate = true;
	char	   *currentMetadataLocation =
		GetIcebergMetadataLocation(relationId, forUpdate);

	if (currentMetadataLocation == NULL ||
		strcmp(currentMetadataLocation, prevMetadataLocation) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_T_R_SERIALIZATION_FAILURE),
				 errmsg("metadata_location has been modified concurrently"),
				 errdetail("Expected previous_metadata_location \"%s\" but found \"%s\".",
						   prevMetadataLocation,
						   currentMetadataLocation ? currentMetadataLocation : "(null)")));

	UpdateInternalCatalogMetadataLocation(relationId, metadataLocation,
										  prevMetadataLocation);

	SyncFromExternalMetadata(relationId);
}


/*
 * HandleInternalCatalogInsert: INSERT into iceberg_tables for the
 * current database catalog. The metadata_location must point at a
 * real Iceberg metadata file the external client wrote; we read it,
 * translate the schema to Postgres column types, issue a
 * CREATE FOREIGN TABLE that pg_lake's own hook turns into a
 * tables_internal row, then overwrite that row's metadata_location
 * with the user-supplied one and run the sync. The sync runs eagerly
 * so type-translation or unreachable-metadata errors surface at INSERT
 * time.
 *
 * SkipIcebergDDLProcessing is set around the CREATE FOREIGN TABLE so
 * pg_lake_table's own DDL path does not re-validate the
 * location-is-empty invariant (the caller wrote a metadata file there)
 * and does not queue a metadata write that would clobber it.
 */
static void
HandleInternalCatalogInsert(char *namespaceName, char *tableName,
							char *metadataLocation)
{
	if (metadataLocation == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_NULL_VALUE_NOT_ALLOWED),
				 errmsg("metadata_location cannot be NULL")));

	bool		missingOk = true;
	Oid			namespaceOid = get_namespace_oid(namespaceName, missingOk);

	if (!OidIsValid(namespaceOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema \"%s\" does not exist", namespaceName)));

	Oid			existingId = get_relname_relid(tableName, namespaceOid);

	if (OidIsValid(existingId))
		ereport(ERROR,
				(errcode(ERRCODE_DUPLICATE_TABLE),
				 errmsg("table \"%s.%s\" already exists",
						namespaceName, tableName)));

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataLocation);
	IcebergTableSchema *schema = GetCurrentIcebergTableSchema(metadata);

	StringInfo	columns = makeStringInfo();

	for (size_t i = 0; i < schema->fields_length; i++)
	{
		DataFileSchemaField *field = &schema->fields[i];

		PGType		pgType = IcebergFieldToPostgresType(field->type);
		char	   *typeName = format_type_with_typemod(pgType.postgresTypeOid,
														pgType.postgresTypeMod);

		if (i > 0)
			appendStringInfoString(columns, ", ");

		appendStringInfo(columns, "%s %s",
						 quote_identifier(field->name), typeName);
	}

	char	   *locationPrefix = LocationPrefixFromMetadataLocation(metadataLocation);

	StringInfo	createCmd = makeStringInfo();

	appendStringInfo(createCmd,
					 "CREATE FOREIGN TABLE %s.%s (%s) SERVER pg_lake_iceberg "
					 "OPTIONS (location %s)",
					 quote_identifier(namespaceName),
					 quote_identifier(tableName),
					 columns->data,
					 quote_literal_cstr(locationPrefix));

	SkipIcebergDDLProcessing = true;

	PG_TRY();
	{
		SPI_START_EXTENSION_OWNER(PgLakeTable);
		SPI_execute(createCmd->data, false, 0);
		SPI_END();
	}
	PG_FINALLY();
	{
		SkipIcebergDDLProcessing = false;
	}
	PG_END_TRY();

	/*
	 * pg_lake's CREATE FOREIGN TABLE hook inserted a tables_internal row
	 * (with NULL metadata_location, since we suppressed the metadata write).
	 * Update it to the caller's metadata, then sync.
	 */
	Oid			newRelationId = get_relname_relid(tableName, namespaceOid);

	if (!OidIsValid(newRelationId))
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("failed to resolve newly-created table \"%s.%s\"",
						namespaceName, tableName)));

	bool		forUpdate = true;
	char	   *currentLocation = GetIcebergMetadataLocation(newRelationId, forUpdate);

	UpdateInternalCatalogMetadataLocation(newRelationId, metadataLocation,
										  currentLocation);

	SyncFromExternalMetadata(newRelationId);
}


/*
 * HandleInternalCatalogDelete: DELETE on iceberg_tables for the current
 * database catalog. Drops the corresponding foreign table — pg_lake's
 * own DROP hook removes the tables_internal row, completing the
 * deregistration. No CASCADE: dependent objects (views, materialized
 * views) belong to Postgres-side users, not the external client; the
 * user must drop them first.
 */
static void
HandleInternalCatalogDelete(char *namespaceName, char *tableName)
{
	bool		missingOk = true;
	Oid			namespaceOid = get_namespace_oid(namespaceName, missingOk);

	if (!OidIsValid(namespaceOid))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("schema \"%s\" does not exist", namespaceName)));

	Oid			relationId = get_relname_relid(tableName, namespaceOid);

	if (!OidIsValid(relationId))
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("table \"%s.%s\" does not exist",
						namespaceName, tableName)));

	StringInfo	dropCmd = makeStringInfo();

	appendStringInfo(dropCmd,
					 "DROP FOREIGN TABLE %s.%s",
					 quote_identifier(namespaceName),
					 quote_identifier(tableName));

	SPI_START_EXTENSION_OWNER(PgLakeTable);
	SPI_execute(dropCmd->data, false, 0);
	SPI_END();
}
