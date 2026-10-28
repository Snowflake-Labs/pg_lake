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
 * pg_lake_iceberg calls SyncIcebergMetadataFromExternalWrite after an
 * UPDATE on the iceberg_tables view moves metadata_location of a table in
 * the current database's catalog.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/relation.h"
#include "access/table.h"
#include "catalog/namespace.h"
#include "catalog/pg_inherits.h"
#include "commands/trigger.h"
#include "executor/spi.h"
#include "foreign/foreign.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pg_lake/cleanup/deletion_queue.h"
#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
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
#include "pg_lake/parsetree/options.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/read_data.h"
#include "pg_lake/sync/sync_external_metadata.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_extension_base/spi_helpers.h"
#include "pg_lake/util/path_hash.h"
#include "pg_lake/util/string_utils.h"
#include "utils/hsearch.h"

typedef struct FieldMapping
{
	int			fieldId;
	AttrNumber	attnum;
}			FieldMapping;

static void SyncIcebergMetadataFromExternalWrite(Oid relationId,
												 const char *metadataLocation);
static void ErrorIfTableHasPostgresOnlyRules(Oid relationId);
static void ErrorIfNullsInRequiredFields(DataFile * dataFile,
										 IcebergTableSchema * schema);
static char *GetTableLocationFromMetadataLocation(const char *metadataLocation);
static void ErrorIfOutsideTableLocation(const char *path, const char *tableLocation);
static void SyncSchemaFromMetadata(Oid relationId, IcebergTableMetadata * metadata);
static List *FetchExistingFieldMappings(Oid relationId);
static FieldMapping * FindFieldMapping(List *mappings, int fieldId);
static char *GetLiveAttributeName(Oid relationId, AttrNumber attnum);
static bool IsAttributeNotNull(Oid relationId, AttrNumber attnum);
static bool SameFieldType(Field * left, Field * right);
static bool SameScalarTypeName(const char *left, const char *right);
static void ExecuteAlterTableViaSPI(const char *cmd);
static void SyncPartitionSpecsFromMetadata(Oid relationId, IcebergTableMetadata * metadata);
static void SyncDataFilesFromMetadata(Oid relationId, IcebergTableMetadata * metadata,
									  const char *metadataLocation,
									  const char *tableLocation);
static void SyncPositionDeleteMappings(Oid relationId, List *deleteFilePaths,
									   HTAB *dataFilePaths);
static char *ColumnBoundBinaryToText(ColumnBound * bound,
									 Field * icebergType, PGType pgType);
static List *BuildColumnStatsForManifestEntry(IcebergManifestEntry * manifestEntry,
											  IcebergTableSchema * schema);
static void InsertDataFileColumnStats(Oid relationId, const char *path,
									  List *columnStatsList);
static void InsertDataFilePartitionValues(Oid relationId, int32 partitionSpecId,
										  int64 fileId, Partition * partition);


/*
 * RegisterIcebergExternalWriteCallback makes pg_lake_iceberg call our sync
 * after an external write.
 */
void
RegisterIcebergExternalWriteCallback(void)
{
	PgLake_IcebergExternalWriteCallback = SyncIcebergMetadataFromExternalWrite;
}


/*
 * SyncIcebergMetadataFromExternalWrite syncs the pg_lake catalog state of
 * the table from the new metadata an external client (Spark, PyIceberg)
 * wrote:
 * 1. Schema sync: drop, rename, retype and add columns to match the
 *    Iceberg schema, and update field_id_mappings.
 * 2. Partition spec sync: register any new partition specs.
 * 3. Data file full resync: clear and repopulate lake_table.files and
 *    associated stats/partition values from the metadata snapshot.
 */
static void
SyncIcebergMetadataFromExternalWrite(Oid relationId, const char *metadataLocation)
{
	/*
	 * pg_lake would write its own metadata for this table at commit, based on
	 * a catalog that no longer matches the external metadata.
	 */
	if (HasTrackedIcebergMetadataChangesForRelation(relationId))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot update metadata_location of a table that was "
						"modified in the same transaction")));

	/*
	 * External writers do not carry _row_id through file rewrites, and the
	 * resync would drop the row ID mappings of every file.
	 */
	if (GetBoolOption(GetForeignTable(relationId)->options, "row_ids", false))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("external writes are not supported on tables with row_ids")));

	ErrorIfTableHasPostgresOnlyRules(relationId);

	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataLocation);

	/*
	 * We read and later delete the files the metadata points to with the
	 * extension's credentials, so they must all belong to this table.
	 */
	char	   *tableLocation = GetTableLocationFromMetadataLocation(metadataLocation);

	if (metadata->location == NULL ||
		strcmp(StripTrailingSlash(pstrdup(metadata->location), true), tableLocation) != 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("cannot change the location of table \"%s\"",
						get_rel_name(relationId))));

	for (int snapshotIndex = 0; snapshotIndex < metadata->snapshots_length; snapshotIndex++)
		ErrorIfOutsideTableLocation(metadata->snapshots[snapshotIndex].manifest_list,
									tableLocation);

	/*
	 * Suppress the ProcessAlterTable / PostProcessRename hooks' Iceberg DDL
	 * processing while we change columns. We manage field_id_mappings
	 * ourselves and the metadata is already final on disk, so we don't want
	 * the hooks to register duplicate mappings or schedule a metadata write.
	 */
	SkipIcebergDDLProcessing = true;

	PG_TRY();
	{
		SyncSchemaFromMetadata(relationId, metadata);
		SyncPartitionSpecsFromMetadata(relationId, metadata);
		SyncDataFilesFromMetadata(relationId, metadata, metadataLocation,
								  tableLocation);
	}
	PG_FINALLY();
	{
		SkipIcebergDDLProcessing = false;
	}
	PG_END_TRY();
}


/*
 * ErrorIfTableHasPostgresOnlyRules throws an error if the table has rules
 * that Postgres enforces on writes but an external writer cannot see: CHECK
 * constraints, partition bounds, stored generated columns and triggers.
 * Postgres keeps relying on the constraints after the write, for instance to
 * prune partitions.
 */
static void
ErrorIfTableHasPostgresOnlyRules(Oid relationId)
{
	const char *rule = NULL;

	if (get_rel_relispartition(relationId) || has_superclass(relationId))
		rule = "a partition or inheritance child";

	Relation	rel = RelationIdGetRelation(relationId);
	TupleConstr *constr = RelationGetDescr(rel)->constr;

	if (rule == NULL && constr != NULL && constr->num_check > 0)
		rule = "CHECK constraints";
	else if (rule == NULL && constr != NULL && constr->has_generated_stored)
		rule = "generated columns";

	for (int triggerIndex = 0;
		 rule == NULL && rel->trigdesc != NULL && triggerIndex < rel->trigdesc->numtriggers;
		 triggerIndex++)
	{
		if (!rel->trigdesc->triggers[triggerIndex].tgisinternal)
			rule = "triggers";
	}

	RelationClose(rel);

	if (rule != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("external writes are not supported on tables with %s", rule),
				 errdetail("External writers do not apply them.")));
}


/*
 * ErrorIfNullsInRequiredFields throws an error if the manifest says a data
 * file has nulls in a required (NOT NULL) column. Files without null counts
 * are accepted.
 */
static void
ErrorIfNullsInRequiredFields(DataFile * dataFile, IcebergTableSchema * schema)
{
	for (size_t statIndex = 0; statIndex < dataFile->null_value_counts_length; statIndex++)
	{
		ColumnStat *nullCount = &dataFile->null_value_counts[statIndex];

		if (nullCount->value <= 0)
			continue;

		for (size_t fieldIdx = 0; fieldIdx < schema->fields_length; fieldIdx++)
		{
			DataFileSchemaField *field = &schema->fields[fieldIdx];

			if (field->id == nullCount->column_id && field->required)
				ereport(ERROR,
						(errcode(ERRCODE_NOT_NULL_VIOLATION),
						 errmsg("file \"%s\" has null values in column \"%s\", which is required",
								dataFile->file_path, field->name)));
		}
	}
}


/*
 * GetTableLocationFromMetadataLocation returns the table location, which is
 * the directory that holds the metadata/ directory.
 */
static char *
GetTableLocationFromMetadataLocation(const char *metadataLocation)
{
	const char *metadataDirectory = NULL;

	for (const char *match = strstr(metadataLocation, "/metadata/");
		 match != NULL;
		 match = strstr(match + 1, "/metadata/"))
		metadataDirectory = match;

	if (metadataDirectory == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("metadata_location must be in the table's metadata directory")));

	return pnstrdup(metadataLocation, metadataDirectory - metadataLocation);
}


/*
 * ErrorIfOutsideTableLocation throws an error if the path is not under the
 * table location.
 */
static void
ErrorIfOutsideTableLocation(const char *path, const char *tableLocation)
{
	size_t		locationLength = strlen(tableLocation);

	if (path == NULL ||
		strncmp(path, tableLocation, locationLength) != 0 ||
		path[locationLength] != '/' ||
		strstr(path, "/../") != NULL)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("file \"%s\" is not in the table location \"%s\"",
						path ? path : "(null)", tableLocation)));
}


/*
 * FetchExistingFieldMappings retrieves all top-level field_id_mappings
 * for the given relation via SPI.
 */
static List *
FetchExistingFieldMappings(Oid relationId)
{
	MemoryContext callerContext = CurrentMemoryContext;
	List	   *mappings = NIL;

	DECLARE_SPI_ARGS(1);
	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeIceberg);

	bool		readOnly = true;

	SPI_EXECUTE("SELECT field_id, pg_attnum FROM " MAPPING_TABLE_NAME
				" WHERE table_name OPERATOR(pg_catalog.=) $1"
				" AND parent_field_id IS NULL", readOnly);

	if (SPI_processed > 0)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		for (uint64 i = 0; i < SPI_processed; i++)
		{
			bool		isNull = false;
			FieldMapping *mapping = palloc(sizeof(FieldMapping));

			mapping->fieldId = GET_SPI_VALUE(INT4OID, i, 1, &isNull);
			mapping->attnum = GET_SPI_VALUE(INT2OID, i, 2, &isNull);
			mappings = lappend(mappings, mapping);
		}

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return mappings;
}


/*
 * FindFieldMapping returns the mapping for the given field ID, or NULL.
 */
static FieldMapping *
FindFieldMapping(List *mappings, int fieldId)
{
	ListCell   *mappingCell = NULL;

	foreach(mappingCell, mappings)
	{
		FieldMapping *mapping = lfirst(mappingCell);

		if (mapping->fieldId == fieldId)
			return mapping;
	}

	return NULL;
}


/*
 * GetLiveAttributeName returns the name of a column, or NULL if the column
 * is dropped or does not exist.
 */
static char *
GetLiveAttributeName(Oid relationId, AttrNumber attnum)
{
	char	   *attributeName = NULL;
	Relation	rel = RelationIdGetRelation(relationId);
	TupleDesc	tupdesc = RelationGetDescr(rel);

	if (attnum > 0 && attnum <= tupdesc->natts &&
		!TupleDescAttr(tupdesc, attnum - 1)->attisdropped)
		attributeName = pstrdup(NameStr(TupleDescAttr(tupdesc, attnum - 1)->attname));

	RelationClose(rel);

	return attributeName;
}


/*
 * IsAttributeNotNull returns whether a column is NOT NULL.
 */
static bool
IsAttributeNotNull(Oid relationId, AttrNumber attnum)
{
	Relation	rel = RelationIdGetRelation(relationId);
	bool		notNull = TupleDescAttr(RelationGetDescr(rel), attnum - 1)->attnotnull;

	RelationClose(rel);

	return notNull;
}


/*
 * SameFieldType returns whether two Iceberg types are the same, including
 * the IDs, names and types of nested fields. Whether a nested field is
 * required is ignored, since Postgres cannot express it.
 */
static bool
SameFieldType(Field * left, Field * right)
{
	if (left->type != right->type)
		return false;

	switch (left->type)
	{
		case FIELD_TYPE_SCALAR:
			return SameScalarTypeName(left->field.scalar.typeName,
									  right->field.scalar.typeName);

		case FIELD_TYPE_LIST:
			return left->field.list.elementId == right->field.list.elementId &&
				SameFieldType(left->field.list.element, right->field.list.element);

		case FIELD_TYPE_MAP:
			return left->field.map.keyId == right->field.map.keyId &&
				left->field.map.valueId == right->field.map.valueId &&
				SameFieldType(left->field.map.key, right->field.map.key) &&
				SameFieldType(left->field.map.value, right->field.map.value);

		case FIELD_TYPE_STRUCT:
			{
				FieldStruct *leftStruct = &left->field.structType;
				FieldStruct *rightStruct = &right->field.structType;

				if (leftStruct->nfields != rightStruct->nfields)
					return false;

				for (size_t fieldIdx = 0; fieldIdx < leftStruct->nfields; fieldIdx++)
				{
					FieldStructElement *leftField = &leftStruct->fields[fieldIdx];
					FieldStructElement *rightField = &rightStruct->fields[fieldIdx];

					if (leftField->id != rightField->id ||
						strcmp(leftField->name, rightField->name) != 0 ||
						!SameFieldType(leftField->type, rightField->type))
						return false;
				}

				return true;
			}
	}

	return false;
}


/*
 * SameScalarTypeName compares Iceberg type names ignoring spaces, since
 * writers format decimal(10, 2) differently.
 */
static bool
SameScalarTypeName(const char *left, const char *right)
{
	while (*left != '\0' || *right != '\0')
	{
		if (*left == ' ')
			left++;
		else if (*right == ' ')
			right++;
		else if (*left++ != *right++)
			return false;
	}

	return true;
}


/*
 * ExecuteAlterTableViaSPI runs an ALTER TABLE DDL command via SPI as the
 * pg_lake_table extension owner.
 */
static void
ExecuteAlterTableViaSPI(const char *cmd)
{
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	if (SPI_execute(cmd, false, 0) < 0)
		elog(ERROR, "could not run \"%s\"", cmd);

	SPI_END();
}


/*
 * SyncSchemaFromMetadata syncs the foreign table columns and field_id_mappings
 * with the current Iceberg schema from the metadata.
 *
 * Drops run first and renames go through a temporary name, so that a column
 * can be dropped and re-added under the same name, or two columns can swap
 * names, in one commit. Dropped columns keep their mapping for reading old
 * files.
 */
static void
SyncSchemaFromMetadata(Oid relationId, IcebergTableMetadata * metadata)
{
	IcebergTableSchema *icebergSchema = GetCurrentIcebergTableSchema(metadata);
	List	   *existingMappings = FetchExistingFieldMappings(relationId);
	char	   *qualifiedName =
		quote_qualified_identifier(get_namespace_name(get_rel_namespace(relationId)),
								   get_rel_name(relationId));
	ListCell   *mappingCell = NULL;

	/* drop columns whose field ID is no longer in the schema */
	foreach(mappingCell, existingMappings)
	{
		FieldMapping *mapping = lfirst(mappingCell);
		bool		stillExists = false;

		for (size_t fieldIdx = 0; fieldIdx < icebergSchema->fields_length; fieldIdx++)
		{
			if (icebergSchema->fields[fieldIdx].id == mapping->fieldId)
			{
				stillExists = true;
				break;
			}
		}

		char	   *columnName = GetLiveAttributeName(relationId, mapping->attnum);

		if (stillExists || columnName == NULL)
			continue;

		ExecuteAlterTableViaSPI(psprintf("ALTER FOREIGN TABLE %s DROP COLUMN %s",
										 qualifiedName,
										 quote_identifier(columnName)));
	}

	/* move renamed columns to a temporary name first */
	List	   *renamedFields = NIL;

	for (size_t fieldIdx = 0; fieldIdx < icebergSchema->fields_length; fieldIdx++)
	{
		DataFileSchemaField *icebergField = &icebergSchema->fields[fieldIdx];
		FieldMapping *mapping = FindFieldMapping(existingMappings, icebergField->id);

		if (mapping == NULL)
			continue;

		char	   *columnName = GetLiveAttributeName(relationId, mapping->attnum);

		if (columnName == NULL || strcmp(columnName, icebergField->name) == 0)
			continue;

		ExecuteAlterTableViaSPI(psprintf("ALTER FOREIGN TABLE %s RENAME COLUMN %s TO %s",
										 qualifiedName,
										 quote_identifier(columnName),
										 quote_identifier(psprintf("pg_lake_rename_%d",
																   icebergField->id))));

		renamedFields = lappend(renamedFields, icebergField);
	}

	foreach(mappingCell, renamedFields)
	{
		DataFileSchemaField *icebergField = lfirst(mappingCell);

		ExecuteAlterTableViaSPI(psprintf("ALTER FOREIGN TABLE %s RENAME COLUMN %s TO %s",
										 qualifiedName,
										 quote_identifier(psprintf("pg_lake_rename_%d",
																   icebergField->id)),
										 quote_identifier(icebergField->name)));
	}

	for (size_t fieldIdx = 0; fieldIdx < icebergSchema->fields_length; fieldIdx++)
	{
		DataFileSchemaField *icebergField = &icebergSchema->fields[fieldIdx];
		FieldMapping *mapping = FindFieldMapping(existingMappings, icebergField->id);
		PGType		pgType = IcebergFieldToPostgresType(icebergField->type);
		char	   *pgTypeName = format_type_with_typemod(pgType.postgresTypeOid,
														  pgType.postgresTypeMod);

		if (mapping != NULL)
		{
			if (GetLiveAttributeName(relationId, mapping->attnum) == NULL)
				continue;

			/* Iceberg allows required -> optional, but not the reverse */
			bool		notNull = IsAttributeNotNull(relationId, mapping->attnum);

			if (notNull && !icebergField->required)
				ExecuteAlterTableViaSPI(psprintf("ALTER FOREIGN TABLE %s ALTER COLUMN %s DROP NOT NULL",
												 qualifiedName,
												 quote_identifier(icebergField->name)));
			else if (!notNull && icebergField->required)
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("cannot make column \"%s\" required in an external write",
								icebergField->name)));

			DataFileSchemaField *registeredField =
				GetRegisteredFieldForAttribute(relationId, mapping->attnum);

			if (registeredField->type->type != FIELD_TYPE_SCALAR ||
				icebergField->type->type != FIELD_TYPE_SCALAR)
			{
				if (!SameFieldType(registeredField->type, icebergField->type))
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot change the type of column \"%s\" in an external write",
									icebergField->name),
							 errdetail("Changes inside struct, list and map columns are not supported.")));
				continue;
			}

			/*
			 * Promote the column when the Iceberg type changed (e.g. int ->
			 * long). Postgres types that map to the same Iceberg type, like
			 * smallint and integer, are left alone.
			 */
			if (SameFieldType(registeredField->type, icebergField->type))
				continue;

			ExecuteAlterTableViaSPI(psprintf("ALTER FOREIGN TABLE %s ALTER COLUMN %s TYPE %s",
											 qualifiedName,
											 quote_identifier(icebergField->name),
											 pgTypeName));

			UpdateRegisteredFieldTypeForAttribute(relationId, mapping->attnum, pgType);
			continue;
		}

		/* Postgres cannot add a NOT NULL column to a table with rows */
		if (icebergField->required)
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("cannot add required column \"%s\" in an external write",
							icebergField->name)));

		/* new field */
		ExecuteAlterTableViaSPI(psprintf("ALTER FOREIGN TABLE %s ADD COLUMN %s %s",
										 qualifiedName,
										 quote_identifier(icebergField->name),
										 pgTypeName));

		AttrNumber	newAttNum = get_attnum(relationId, icebergField->name);

		if (newAttNum == InvalidAttrNumber)
			elog(ERROR, "could not find column \"%s\" after ADD COLUMN",
				 icebergField->name);

		int			parentFieldId = INVALID_FIELD_ID;

		RegisterIcebergColumnMapping(relationId, icebergField->type,
									 newAttNum, parentFieldId, pgType,
									 icebergField->id, icebergField->writeDefault,
									 icebergField->initialDefault, NIL);
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
 *
 * The catalog changes are applied directly rather than through
 * ApplyMetadataChanges, so pg_lake does not write metadata of its own for
 * this table at commit.
 */
static void
SyncDataFilesFromMetadata(Oid relationId, IcebergTableMetadata * metadata,
						  const char *metadataLocation, const char *tableLocation)
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

	List	   *catalogFiles = GetTableDataFilesFromCatalog(relationId, dataOnly,
															newFilesOnly, forUpdate,
															NULL, snapshot);

	/*
	 * Copy the paths into the current memory context. The TableDataFile
	 * structs returned by GetTableDataFilesFromCatalog point into SPI memory
	 * that gets freed when a later SPI_connect/SPI_finish cycle runs.
	 */
	List	   *oldFilePaths = NIL;
	ListCell   *cfCell = NULL;

	foreach(cfCell, catalogFiles)
	{
		TableDataFile *file = lfirst(cfCell);

		oldFilePaths = lappend(oldFilePaths, pstrdup(file->path));
	}

	/*
	 * Build the set of files referenced by ANY snapshot in the new metadata.
	 * IcebergFindAllReferencedFiles walks every snapshot in the metadata, not
	 * just the current one, so files held only by retained-but-not-current
	 * snapshots stay in this set and won't be queued for deletion below.
	 */
	HTAB	   *referencedFileHash = CreateFilesHash();

	if (oldFilePaths != NIL)
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
	TableMetadataOperation *removeAllOperation = palloc0(sizeof(TableMetadataOperation));

	removeAllOperation->type = DATA_FILE_REMOVE_ALL;
	ApplyDataFileCatalogChanges(relationId, list_make1(removeAllOperation));

	/* repopulate the catalog from the current snapshot, if any */
	bool		missingOk = true;
	IcebergSnapshot *currentSnapshot = GetCurrentSnapshot(metadata, missingOk);
	HTAB	   *dataFilePaths = CreateFilesHash();
	List	   *deleteFilePaths = NIL;

	if (currentSnapshot != NULL)
	{
		IcebergTableSchema *icebergSchema = GetCurrentIcebergTableSchema(metadata);

		List	   *manifests = FetchManifestsFromSnapshot(currentSnapshot, NULL);
		ListCell   *manifestCell = NULL;

		foreach(manifestCell, manifests)
		{
			IcebergManifest *manifest = lfirst(manifestCell);

			ErrorIfOutsideTableLocation(manifest->manifest_path, tableLocation);

			List	   *manifestEntries =
				FetchManifestEntriesFromManifest(manifest,
												 IsManifestEntryStatusScannable);

			ListCell   *entryCell = NULL;

			foreach(entryCell, manifestEntries)
			{
				IcebergManifestEntry *entry = lfirst(entryCell);

				DataFile   *dataFile = &entry->data_file;

				ErrorIfOutsideTableLocation(dataFile->file_path, tableLocation);

				/* deletion vectors are Puffin files, which we cannot read */
				if (dataFile->file_format == NULL ||
					pg_strcasecmp(dataFile->file_format, "parquet") != 0)
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("file \"%s\" has unsupported format \"%s\"",
									dataFile->file_path,
									dataFile->file_format ? dataFile->file_format : "(null)"),
							 errdetail("Only Parquet data and position delete files are supported.")));

				/* map Iceberg content type to pg_lake content type */
				DataFileContent content;

				switch (dataFile->content)
				{
					case ICEBERG_DATA_FILE_CONTENT_DATA:
						content = CONTENT_DATA;
						AppendFileToHash(dataFile->file_path, dataFilePaths);
						break;
					case ICEBERG_DATA_FILE_CONTENT_POSITION_DELETES:
						content = CONTENT_POSITION_DELETES;
						deleteFilePaths = lappend(deleteFilePaths,
												  pstrdup(dataFile->file_path));
						break;
					default:
						ereport(ERROR,
								(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								 errmsg("equality delete files are not supported in external writes")));
				}

				int64		fileId = AddDataFileToTable(relationId,
														dataFile->file_path,
														dataFile->record_count,
														dataFile->file_size_in_bytes,
														content,
														INVALID_ROW_ID);

				if (content == CONTENT_DATA)
				{
					ErrorIfNullsInRequiredFields(dataFile, icebergSchema);

					List	   *columnStatsList =
						BuildColumnStatsForManifestEntry(entry, icebergSchema);

					if (columnStatsList != NIL)
						InsertDataFileColumnStats(relationId,
												  dataFile->file_path,
												  columnStatsList);
				}

				if (dataFile->partition.fields_length > 0)
				{
					InsertDataFilePartitionValues(relationId,
												  manifest->partition_spec_id,
												  fileId,
												  &dataFile->partition);
				}
			}
		}
	}

	SyncPositionDeleteMappings(relationId, deleteFilePaths, dataFilePaths);

	/*
	 * Queue any previously-cataloged file that is not referenced by any
	 * retained snapshot in the new metadata. Files referenced only by older
	 * but still-retained snapshots are intentionally left alone.
	 */
	ListCell   *oldPathCell = NULL;

	foreach(oldPathCell, oldFilePaths)
	{
		char	   *oldPath = lfirst(oldPathCell);
		bool		found = false;

		PathHashSearch(referencedFileHash, oldPath, HASH_FIND, &found);

		if (!found)
			InsertDeletionQueueRecord(oldPath, relationId, orphanedAt);
	}
}


/*
 * SyncPositionDeleteMappings reads the position delete files to find which
 * data files they delete from, since reads only apply a delete file to the
 * data files it is mapped to. It also sets the deleted row count of each data
 * file.
 */
static void
SyncPositionDeleteMappings(Oid relationId, List *deleteFilePaths, HTAB *dataFilePaths)
{
	if (deleteFilePaths == NIL)
		return;

	char	   *query =
		psprintf("SELECT filename, file_path, "
				 "sum(count(*)) OVER (PARTITION BY file_path) "
				 "FROM read_parquet(%s, filename=true) "
				 "GROUP BY filename, file_path ORDER BY file_path",
				 PathListToString(deleteFilePaths));

	PGDuckConnection *pgDuckConn = GetPGDuckConnection();
	PGresult   *result = ExecuteQueryOnPGDuckConnection(pgDuckConn, query);

	CheckPGDuckResult(pgDuckConn, result);

	List	   *operations = NIL;

	/* make sure we PQclear the result */
	PG_TRY();
	{
		char	   *previousDataFilePath = NULL;

		for (int rowIndex = 0; rowIndex < PQntuples(result); rowIndex++)
		{
			char	   *deleteFilePath = pstrdup(PQgetvalue(result, rowIndex, 0));
			char	   *dataFilePath = pstrdup(PQgetvalue(result, rowIndex, 1));
			bool		found = false;

			/* rows of data files that are no longer in the table */
			PathHashSearch(dataFilePaths, dataFilePath, HASH_FIND, &found);
			if (!found)
				continue;

			operations = lappend(operations,
								 AddDeleteMappingOperation(deleteFilePath, dataFilePath));

			if (previousDataFilePath == NULL ||
				strcmp(previousDataFilePath, dataFilePath) != 0)
			{
				int64		deletedRowCount =
					pg_strtoint64(PQgetvalue(result, rowIndex, 2));

				operations = lappend(operations,
									 UpdateDeletedRowCountOperation(dataFilePath,
																	deletedRowCount));
			}

			previousDataFilePath = dataFilePath;
		}

		PQclear(result);
	}
	PG_CATCH();
	{
		PQclear(result);
		PG_RE_THROW();
	}
	PG_END_TRY();

	ReleasePGDuckConnection(pgDuckConn);

	ApplyDataFileCatalogChanges(relationId, operations);
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

		/* find the iceberg field in the schema */
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

		char	   *lowerBoundText = ColumnBoundBinaryToText(lowerBound,
															 icebergField->type,
															 pgType);
		char	   *upperBoundText = NULL;

		if (upperBound != NULL)
			upperBoundText = ColumnBoundBinaryToText(upperBound,
													 icebergField->type,
													 pgType);

		if (lowerBoundText == NULL)
			continue;

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
ColumnBoundBinaryToText(ColumnBound * bound, Field * icebergType, PGType pgType)
{
	if (bound->value == NULL || bound->value_length == 0)
		return NULL;

	Datum		boundDatum = PGIcebergBinaryDeserialize(bound->value,
														bound->value_length,
														icebergType,
														pgType);

	Oid			typoutput;
	bool		typIsVarlena;

	getTypeOutputInfo(pgType.postgresTypeOid, &typoutput, &typIsVarlena);

	return OidOutputFunctionCall(typoutput, boundDatum);
}

/*
 * InsertDataFileColumnStats inserts column stats for a data file.
 */
static void
InsertDataFileColumnStats(Oid relationId, const char *path, List *columnStatsList)
{
	SPI_START_EXTENSION_OWNER(PgLakeTable);

	ListCell   *columnStatsCell = NULL;

	foreach(columnStatsCell, columnStatsList)
	{
		DataFileColumnStats *columnStats = lfirst(columnStatsCell);

		if (columnStats->lowerBoundText == NULL)
		{
			Assert(columnStats->upperBoundText == NULL);
			continue;
		}

		char	   *query =
			"insert into " DATA_FILE_COLUMN_STATS_TABLE_QUALIFIED " "
			"(table_name, path, field_id, lower_bound, upper_bound) "
			"values ($1,$2,$3,$4,$5)";

		DECLARE_SPI_ARGS(5);
		SPI_ARG_VALUE(1, OIDOID, relationId, false);
		SPI_ARG_VALUE(2, TEXTOID, path, false);
		SPI_ARG_VALUE(3, INT8OID, columnStats->leafField.fieldId, false);
		SPI_ARG_VALUE(4, TEXTOID, columnStats->lowerBoundText, columnStats->lowerBoundText == NULL);
		SPI_ARG_VALUE(5, TEXTOID, columnStats->upperBoundText, columnStats->upperBoundText == NULL);

		bool		readOnly = false;

		SPI_EXECUTE(query, readOnly);
	}

	SPI_END();
}


/*
 * InsertDataFilePartitionValues inserts partition values for a data file.
 */
static void
InsertDataFilePartitionValues(Oid relationId, int32 partitionSpecId, int64 fileId,
							  Partition * partition)
{
	Assert(partition != NULL);
	Assert(partition->fields_length > 0);
	Assert(partition->fields != NULL);
	Assert(partitionSpecId != DEFAULT_SPEC_ID);

	List	   *transforms = AllPartitionTransformList(relationId);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	for (size_t fieldIndex = 0; fieldIndex < partition->fields_length; fieldIndex++)
	{
		PartitionField *partitionField = &partition->fields[fieldIndex];

		bool		errorIfMissing = true;

		IcebergPartitionTransform *transform =
			FindPartitionTransformById(transforms, partitionField->field_id, errorIfMissing);

		const char *partitionValue =
			SerializePartitionValueToPGText(partitionField->value,
											partitionField->value_length,
											transform);

		char	   *query =
			"INSERT INTO " DATA_FILE_PARTITION_VALUES_TABLE_QUALIFIED " "
			"(table_name, id, partition_field_id, value) "
			"VALUES ($1,$2,$3,$4)";

		DECLARE_SPI_ARGS(4);

		SPI_ARG_VALUE(1, OIDOID, relationId, false);
		SPI_ARG_VALUE(2, INT8OID, fileId, false);
		SPI_ARG_VALUE(3, INT4OID, partitionField->field_id, false);
		SPI_ARG_VALUE(4, TEXTOID, partitionValue, partitionValue == NULL);

		bool		readOnly = false;

		SPI_EXECUTE(query, readOnly);
	}

	SPI_END();
}
