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

#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/data_files_catalog.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/catalog.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_extension_base/spi_helpers.h"

static void InsertIcebergPartitionSpec(Oid relationId,
									   IcebergPartitionSpec * spec);
static void InsertIcebergPartitionField(Oid relationId,
										int specId,
										IcebergPartitionSpecField * field);
static List *GetAllPartitionSpecFieldsForExternalIcebergTable(char *metadataPath);
static List *GetAllPartitionSpecFieldsForInternalIcebergTable(Oid relationId);


/*
* GetLargestSpecId reads the largest spec id for the given relation from
* the lake_table.partition_specs table.
*/
int
GetLargestSpecId(Oid relationId)
{
	int			largestSpecId = DEFAULT_SPEC_ID;

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE("SELECT MAX(spec_id) FROM " PARTITION_SPECS_TABLE_QUALIFIED " "
				"WHERE table_name OPERATOR(pg_catalog.=) $1", readOnly);

	/* this is an aggregate query, so always have 1 row returned */
	Assert(SPI_processed == 1);

	bool		isNull = false;
	int			specId = GET_SPI_VALUE(INT4OID, 0, 1, &isNull);

	if (!isNull)
	{
		largestSpecId = specId;
	}

	SPI_END();

	return largestSpecId;
}


int
GetCurrentSpecId(Oid relationId)
{
	int			currentSpecId = DEFAULT_SPEC_ID;

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE("SELECT default_spec_id FROM lake_iceberg.tables_internal "
				"WHERE table_name OPERATOR(pg_catalog.=) $1", readOnly);

	if (SPI_processed == 1)
	{
		bool		isNull = false;

		currentSpecId = GET_SPI_VALUE(INT4OID, 0, 1, &isNull);
	}

	SPI_END();

	return currentSpecId;
}


/*
* GetLargestPartitionFieldId reads the largest partition field id for
* the given relation from the lake_table.partition_fields table.
*/
int
GetLargestPartitionFieldId(Oid relationId)
{
	int			largestPartitionFieldId = 0;

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	/*
	 * Although this is a read-only query, we need the execution to use the
	 * current transaction's snapshot (e.g., GetTransactionSnapshot()) to get
	 * the snapshot that the current transaction modified.
	 *
	 * So we trick the SPI_EXECUTE function to think that the query is not
	 * read-only and read the transaction snapshot.
	 */
	bool		readOnly = false;

	SPI_EXECUTE("SELECT MAX(partition_field_id) FROM " PARTITION_FIELDS_TABLE_QUALIFIED " "
				"WHERE table_name OPERATOR(pg_catalog.=) $1", readOnly);

	/* this is an aggregate query, so always have 1 row returned */
	Assert(SPI_processed == 1);

	bool		isNull = false;

	int			partitionFieldId = GET_SPI_VALUE(INT4OID, 0, 1, &isNull);

	if (!isNull)
	{
		largestPartitionFieldId = partitionFieldId;
	}
	else
	{
		/*
		 * First partition field for the table, set the initial value as the
		 * reference implementation does. This is a bit arbitrary, but still
		 * let's be consistent with the reference implementation.
		 */
		largestPartitionFieldId = ICEBERG_PARTITION_FIELD_ID_START;
	}

	SPI_END();

	return largestPartitionFieldId;
}


/*
* GetIcebergPartitionFieldFromCatalog reads the partition field from
* the lake_table.partition_fields table for the given
* relation and field id.
*/
IcebergPartitionSpecField *
GetIcebergPartitionFieldFromCatalog(Oid relationId, int fieldId)
{
	MemoryContext currentMemoryContext = CurrentMemoryContext;
	IcebergPartitionSpecField *field = NULL;

	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT4OID, fieldId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE("SELECT source_field_id, partition_field_id, partition_field_name, transform_name "
				"FROM " PARTITION_FIELDS_TABLE_QUALIFIED " "
				"WHERE table_name OPERATOR(pg_catalog.=) $1 AND "
				"partition_field_id OPERATOR(pg_catalog.=) $2", readOnly);

	/* we have a primary key on these fields, so we can at most get one */
	if (SPI_processed == 1)
	{
		/* switch to previous context */
		MemoryContext spiContext = MemoryContextSwitchTo(currentMemoryContext);
		bool		isNull = false;

		field = palloc0(sizeof(IcebergPartitionSpecField));

		field->source_id = GET_SPI_VALUE(INT4OID, 0, 1, &isNull);
		field->field_id = GET_SPI_VALUE(INT4OID, 0, 2, &isNull);
		field->name = pstrdup(GET_SPI_VALUE(TEXTOID, 0, 3, &isNull));
		field->transform = pstrdup(GET_SPI_VALUE(TEXTOID, 0, 4, &isNull));

		field->name_length = strlen(field->name);
		field->transform_length = strlen(field->transform);
		field->source_ids_length = 1;
		field->source_ids = palloc0(sizeof(int) * field->source_ids_length);
		field->source_ids[0] = field->source_id;

		/* switch back to the previous context */
		MemoryContextSwitchTo(spiContext);
	}
	SPI_END();

	return field;
}

/*
* GetIcebergSpecPartitionFieldsFromCatalog reads the partition field
* from the lake_table.partition_fields table for the given
* relation and spec id. It is similar to GetIcebergPartitionFieldFromCatalog,
* but it returns a list of partition fields for the given spec id.
*/
List *
GetIcebergSpecPartitionFieldsFromCatalog(Oid relationId, int specId)
{
	MemoryContext currentMemoryContext = CurrentMemoryContext;

	List	   *partitionFields = NIL;

	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT4OID, specId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE("SELECT source_field_id, partition_field_id, partition_field_name, transform_name "
				"FROM " PARTITION_FIELDS_TABLE_QUALIFIED " "
				"WHERE table_name OPERATOR(pg_catalog.=) $1 AND spec_id OPERATOR(pg_catalog.=) $2", readOnly);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(currentMemoryContext);

		bool		isNull = false;

		IcebergPartitionSpecField *field = palloc0(sizeof(IcebergPartitionSpecField));

		field->source_id = GET_SPI_VALUE(INT4OID, rowIndex, 1, &isNull);
		field->field_id = GET_SPI_VALUE(INT4OID, rowIndex, 2, &isNull);
		field->name = pstrdup(GET_SPI_VALUE(TEXTOID, rowIndex, 3, &isNull));
		field->transform = pstrdup(GET_SPI_VALUE(TEXTOID, rowIndex, 4, &isNull));

		field->name_length = strlen(field->name);
		field->transform_length = strlen(field->transform);
		field->source_ids_length = 1;
		field->source_ids = palloc0(sizeof(int) * field->source_ids_length);
		field->source_ids[0] = field->source_id;

		partitionFields = lappend(partitionFields, field);

		/* switch back to the previous context */
		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return partitionFields;
}


/*
* GetAllPartitionSpecFieldsForInternalIcebergTable reads the partition field
* from the lake_table.partition_fields table for the given
* relation.
*/
List *
GetAllPartitionSpecFieldsForInternalIcebergTable(Oid relationId)
{
	MemoryContext currentMemoryContext = CurrentMemoryContext;

	List	   *partitionFields = NIL;


	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE("SELECT source_field_id, partition_field_id, partition_field_name, transform_name "
				"FROM " PARTITION_FIELDS_TABLE_QUALIFIED " "
				"WHERE table_name OPERATOR(pg_catalog.=) $1", readOnly);



	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(currentMemoryContext);

		bool		isNull = false;

		IcebergPartitionSpecField *field = palloc0(sizeof(IcebergPartitionSpecField));

		field->source_id = GET_SPI_VALUE(INT4OID, rowIndex, 1, &isNull);
		field->field_id = GET_SPI_VALUE(INT4OID, rowIndex, 2, &isNull);
		field->name = pstrdup(GET_SPI_VALUE(TEXTOID, rowIndex, 3, &isNull));
		field->transform = pstrdup(GET_SPI_VALUE(TEXTOID, rowIndex, 4, &isNull));

		field->name_length = strlen(field->name);
		field->transform_length = strlen(field->transform);
		field->source_ids_length = 1;
		field->source_ids = palloc0(sizeof(int) * field->source_ids_length);
		field->source_ids[0] = field->source_id;

		partitionFields = lappend(partitionFields, field);

		/* switch back to the previous context */
		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return partitionFields;
}

/*
* User has changed the default partition spec id for the table. We need to
* update the default_spec_id in the lake_table.tables_internal
* table.
*/
void
UpdateDefaultPartitionSpecId(Oid relationId, int specId)
{
	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT4OID, specId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE("UPDATE lake_iceberg.tables_internal "
				"SET default_spec_id = $2 "
				"WHERE table_name OPERATOR(pg_catalog.=) $1", readOnly);

	SPI_END();
}


/*
* InsertPartitionSpecAndPartitionFields inserts the partition spec and
* partition fields into the lake_table.partition_specs and
* lake_table.partition_fields tables.
*/
void
InsertPartitionSpecAndPartitionFields(Oid relationId, IcebergPartitionSpec * spec)
{
	/* insert the partition spec */
	InsertIcebergPartitionSpec(relationId, spec);

	/* insert the partition fields */
	for (int i = 0; i < spec->fields_length; i++)
	{
		IcebergPartitionSpecField *field = &spec->fields[i];

		InsertIcebergPartitionField(relationId, spec->spec_id, field);
	}
}

/*
* InsertIcebergPartitionSpec inserts the partition spec into the
* lake_table.partition_specs table.
*/
static void
InsertIcebergPartitionSpec(Oid relationId, IcebergPartitionSpec * spec)
{
	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT4OID, spec->spec_id, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE("INSERT INTO " PARTITION_SPECS_TABLE_QUALIFIED " "
				"(table_name, spec_id) "
				"VALUES ($1, $2)", readOnly);

	SPI_END();
}


/*
* InsertIcebergPartitionField inserts the partition field into the
* lake_table.partition_fields table.
*/
static void
InsertIcebergPartitionField(Oid relationId, int specId, IcebergPartitionSpecField * field)
{
	DECLARE_SPI_ARGS(6);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, INT4OID, specId, false);
	SPI_ARG_VALUE(3, INT4OID, field->source_id, false);
	SPI_ARG_VALUE(4, INT4OID, field->field_id, false);
	SPI_ARG_VALUE(5, TEXTOID, field->name, false);
	SPI_ARG_VALUE(6, TEXTOID, field->transform, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	SPI_EXECUTE("INSERT INTO " PARTITION_FIELDS_TABLE_QUALIFIED " "
				"(table_name, spec_id, source_field_id, partition_field_id, partition_field_name, transform_name) "
				"VALUES ($1, $2, $3, $4, $5, $6)", readOnly);

	SPI_END();
}


/*
 * GetAllPartitionSpecFieldsForExternalIcebergTable gets all the partition spec fields
 * for the external Iceberg table.
 */
static List *
GetAllPartitionSpecFieldsForExternalIcebergTable(char *metadataPath)
{
	IcebergTableMetadata *metadata = ReadIcebergTableMetadata(metadataPath);

	List	   *allPartitionFields = NIL;

	for (int specIdx = 0; specIdx < metadata->partition_specs_length; specIdx++)
	{
		IcebergPartitionSpec *spec = &metadata->partition_specs[specIdx];

		for (int partitionFieldIdx = 0; partitionFieldIdx < spec->fields_length; partitionFieldIdx++)
		{
			IcebergPartitionSpecField *field = &spec->fields[partitionFieldIdx];

			allPartitionFields = lappend(allPartitionFields, field);
		}
	}

	return allPartitionFields;
}


/*
* For a given relationId and path, this function returns the partition
* information for the data file. If used for write-path, the partition
* transforms should only include the current partition transforms. If used
* for read-path, the partition transforms should include all the
* partition transforms for the table.
*/
Partition *
GetDataFilePartition(Oid relationId, List *partitionTransforms, const char *path,
					 int32 *partitionSpecId)
{
	*partitionSpecId = DEFAULT_SPEC_ID;

	/* read with SPI */
	MemoryContext callerContext = CurrentMemoryContext;

	Partition  *partition = palloc0(sizeof(Partition));

	DECLARE_SPI_ARGS(2);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);
	SPI_ARG_VALUE(2, TEXTOID, path, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	SPI_EXECUTE("select pf.spec_id, pv.partition_field_id, pf.partition_field_name, pv.value "
				"from " DATA_FILE_PARTITION_VALUES_TABLE_QUALIFIED " pv JOIN "
				PARTITION_FIELDS_TABLE_QUALIFIED " pf USING (table_name, partition_field_id) JOIN "
				DATA_FILES_TABLE_QUALIFIED " df ON (df.id = pv.id) "
				"where df.table_name OPERATOR(pg_catalog.=) $1 "
				"and path OPERATOR(pg_catalog.=) $2",
				false);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		bool		isSpecIdNull = false;
		int32		specId = GET_SPI_VALUE(INT4OID, rowIndex, 1, &isSpecIdNull);

		/* enforced in the catalog */
		Assert(!isSpecIdNull);

		*partitionSpecId = specId;

		bool		isPartitionFieldIdNull = false;
		int64		partitionFieldId = GET_SPI_VALUE(INT8OID, rowIndex, 2, &isPartitionFieldIdNull);

		if (isPartitionFieldIdNull)
			continue;

		bool		isPartitionFieldNameNull = false;
		char	   *partitionFieldName = GET_SPI_VALUE(TEXTOID, rowIndex, 3, &isPartitionFieldNameNull);

		bool		isValueNull = false;
		char	   *valueText = NULL;

		Datum		valueDatum = GET_SPI_DATUM(rowIndex, 4, &isValueNull);

		if (!isValueNull)
			valueText = TextDatumGetCString(valueDatum);

		PartitionField *partitionField = palloc0(sizeof(PartitionField));

		partitionField->field_id = partitionFieldId;
		partitionField->field_name = pstrdup(partitionFieldName);

		/* prepare the partition field */
		bool		errorIfMissing = true;

		IcebergPartitionTransform *partitionTransform =
			FindPartitionTransformById(partitionTransforms, partitionFieldId, errorIfMissing);

		partitionField->value_type = GetTransformResultAvroType(partitionTransform);

		partitionField->value = DeserializePartitionValueFromPGText(partitionTransform, valueText,
																	&partitionField->value_length);

		/* now append this to the partition */
		AppendPartitionField(partition, partitionField);

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return partition->fields_length > 0 ? partition : NULL;
}


/*
 * GetAllPartitionSpecsFromCatalog retrieves all partition specifications
 * for a given relation from the catalog.
 */
HTAB *
GetAllPartitionSpecsFromCatalog(Oid relationId)
{
	HTAB	   *specsHash = CreatePartitionSpecHash();

	MemoryContext callerContext = CurrentMemoryContext;

	DECLARE_SPI_ARGS(1);

	SPI_ARG_VALUE(1, OIDOID, relationId, false);

	SPI_START_EXTENSION_OWNER(PgLakeTable);

	bool		readOnly = false;

	/* default spec has no partition field so LEFT JOIN */
	SPI_EXECUTE("SELECT ps.spec_id, pf.partition_field_id, pf.source_field_id, "
				"pf.partition_field_name, pf.transform_name "
				"FROM " PARTITION_SPECS_TABLE_QUALIFIED " ps "
				"LEFT JOIN " PARTITION_FIELDS_TABLE_QUALIFIED " pf "
				"ON ps.table_name OPERATOR(pg_catalog.=) pf.table_name "
				"AND ps.spec_id OPERATOR(pg_catalog.=) pf.spec_id "
				"WHERE ps.table_name OPERATOR(pg_catalog.=) $1 "
				"ORDER BY ps.spec_id, pf.partition_field_id", readOnly);

	for (int rowIndex = 0; rowIndex < SPI_processed; rowIndex++)
	{
		MemoryContext spiContext = MemoryContextSwitchTo(callerContext);

		bool		isSpecIdNull = false;
		int32		specId = GET_SPI_VALUE(INT4OID, rowIndex, 1, &isSpecIdNull);

		Assert(!isSpecIdNull);

		IcebergPartitionSpecField *field = NULL;

		bool		isPartitionFieldIdNull = false;
		Datum		partitionFieldIdDatum = GET_SPI_DATUM(rowIndex, 2, &isPartitionFieldIdNull);

		if (!isPartitionFieldIdNull)
		{
			int32		partitionFieldId = DatumGetInt32(partitionFieldIdDatum);

			bool		isSourceFieldIdNull = false;
			int32		sourceFieldId = GET_SPI_VALUE(INT4OID, rowIndex, 3, &isSourceFieldIdNull);

			bool		isPartitionFieldNameNull = false;
			char	   *partitionFieldName = GET_SPI_VALUE(TEXTOID, rowIndex, 4, &isPartitionFieldNameNull);

			bool		isTransformNameNull = false;
			char	   *transformName = GET_SPI_VALUE(TEXTOID, rowIndex, 5, &isTransformNameNull);

			field = palloc0(sizeof(IcebergPartitionSpecField));

			field->source_id = sourceFieldId;
			field->source_ids_length = 1;
			field->source_ids = palloc0(sizeof(int) * field->source_ids_length);
			field->source_ids[0] = sourceFieldId;
			field->field_id = partitionFieldId;
			field->name = pstrdup(partitionFieldName);
			field->name_length = strlen(partitionFieldName);
			field->transform = pstrdup(transformName);
			field->transform_length = strlen(transformName);
		}

		bool		found = false;
		IcebergPartitionSpecHashEntry *entry =
			(IcebergPartitionSpecHashEntry *) hash_search(specsHash, &specId, HASH_ENTER, &found);

		if (!found)
		{
			entry->spec = palloc0(sizeof(IcebergPartitionSpec));

			entry->spec->spec_id = specId;

			if (field != NULL)
			{
				entry->spec->fields_length = 1;
				entry->spec->fields = palloc0(sizeof(IcebergPartitionSpecField));
				entry->spec->fields[entry->spec->fields_length - 1] = *field;
			}
		}
		else if (field != NULL)
		{
			entry->spec->fields_length += 1;
			entry->spec->fields = repalloc(entry->spec->fields,
										   sizeof(IcebergPartitionSpecField) * (entry->spec->fields_length));
			entry->spec->fields[entry->spec->fields_length - 1] = *field;
		}

		MemoryContextSwitchTo(spiContext);
	}

	SPI_END();

	return specsHash;
}


/*
 * GetAllPartitionSpecFields gets all the partition spec fields for the given
 * relationId.
 */
List *
GetAllPartitionSpecFields(Oid relationId)
{
	if (!IsIcebergTable(relationId))
		/* partitioning is an iceberg concept */
		return NIL;
	else if (IsInternalIcebergTable(relationId))
	{
		/* internal iceberg table, get from catalog */
		return GetAllPartitionSpecFieldsForInternalIcebergTable(relationId);
	}
	else
	{
		Assert(IsExternalIcebergTable(relationId));

		/* external iceberg table, get from remote metadata */
		char	   *currentMetadataPath = GetIcebergMetadataLocation(relationId, false);

		return GetAllPartitionSpecFieldsForExternalIcebergTable(currentMetadataPath);
	}
}


/*
 * CreatePartitionSpecHash creates a hash table to store partition specs.
 */
HTAB *
CreatePartitionSpecHash(void)
{
	HASHCTL		hashCtl = {0};

	hashCtl.keysize = sizeof(int32);
	hashCtl.entrysize = sizeof(IcebergPartitionSpecHashEntry);
	hashCtl.hcxt = CurrentMemoryContext;

	HTAB	   *partitionSpecsHash = hash_create("Partition Specs Hash", 16,
												 &hashCtl,
												 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	return partitionSpecsHash;
}
