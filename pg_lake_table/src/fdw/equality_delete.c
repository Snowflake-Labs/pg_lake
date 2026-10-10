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

#include <math.h>

#include "common/hashfn.h"
#include "catalog/pg_type_d.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "nodes/bitmapset.h"
#include "port/pg_bswap.h"
#include "pg_extension_base/pg_compat.h"
#include "pg_lake/fdw/equality_delete.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/parquet/leaf_field.h"
#include "pg_lake/pgduck/client.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"

/* pg_lake_table.enable_equality_delete_validation */
bool		EnableEqualityDeleteValidation = true;

/* Hashes only locate candidates. Every bucket has an exact comparison. */
typedef struct DeleteCandidateBucket
{
	uint64		hash;
	List	   *files;
}			DeleteCandidateBucket;

typedef struct ReadGroupBucket
{
	uint32		hash;
	List	   *groups;
}			ReadGroupBucket;

typedef struct EqualityDeleteFile
{
	DataFile   *file;
	DataFileSchema *schema;
	int			index;
	bool		isGlobal;
}			EqualityDeleteFile;

typedef struct EqualityReadGroup
{
	PgLakeEqualityDeleteReadGroup scan;
	Bitmapset  *mask;
}			EqualityReadGroup;

/* Canonical value view, including NULL independently of byte length. */
typedef struct PartitionValue
{
	IcebergScalarAvroType type;
	const void *bytes;
	size_t		length;
	int64		integer;
	double		floating;
}			PartitionValue;

static IcebergPartitionSpec * ValidatePartition(IcebergTableMetadata * metadata, DataFile * file);
static PartitionValue PartitionValueView(const PartitionField * field);
static uint64 PartitionHash(DataFile * file);
static bool PartitionsEqual(const Partition * left, const Partition * right);
static bool AvroTypesEqual(IcebergScalarAvroType left, IcebergScalarAvroType right);
static DataFileSchema * EqualityKeySchema(IcebergTableMetadata * metadata, DataFile * file);
static bool KeySchemasEqual(DataFileSchema * left, DataFileSchema * right);
static void AddCandidates(Bitmapset **mask, List *candidates, DataFile * dataFile);
static bool ReadIntegerBound(ColumnBound * bounds, size_t count, int id, bool isLong, int64 *value, size_t *width);
static bool IntegerBoundsDisjoint(DataFile * dataFile, EqualityDeleteFile * deletion);
static void ValidateEqualityDeletesForTableScan(PgLakeTableScan * tableScan);
static bool TableScanHasEqualityDeletes(PgLakeTableScan * tableScan);
static void ValidateEqualityDeleteFile(PGresult *result, PgLakeEqualityDeleteScan * scan, int startRow, int endRow);
static bool EqualityDeleteKeyTypeMatches(const char *icebergType, const char *parquetType);

static DataFileSchemaField *
FindTopLevelField(DataFileSchema * schema, int id)
{
	DataFileSchemaField *result = NULL;

	for (size_t i = 0; i < schema->nfields; i++)
		if (schema->fields[i].id == id)
		{
			if (result != NULL)
				ereport(ERROR, (errmsg("duplicate Iceberg schema field ID %d", id)));
			result = &schema->fields[i];
		}
	return result;
}

/*
 * Validate against IDs and transform types before using a partition for
 * applicability. Names and Avro field order are not partition identity.
 * Empty equality-delete tuples are accepted as a global-delete compatibility
 * case even when their spec ID names a partitioned spec.
 */
static IcebergPartitionSpec *
ValidatePartition(IcebergTableMetadata * metadata, DataFile * file)
{
	IcebergPartitionSpec *spec = NULL;

	for (size_t i = 0; i < metadata->partition_specs_length; i++)
	{
		if (metadata->partition_specs[i].spec_id == file->partition_spec_id)
		{
			if (spec != NULL)
				ereport(ERROR, (errmsg("duplicate Iceberg partition spec %d", file->partition_spec_id)));
			spec = &metadata->partition_specs[i];
		}
	}

	/*
	 * PartitionSpec.unpartitioned() can reuse spec ID 0 in a partitioned
	 * table. An empty equality-delete tuple still denotes a global delete.
	 * Data files and nonempty delete tuples must match the registered spec.
	 */
	bool		emptyEqualityDelete = file->content == ICEBERG_DATA_FILE_CONTENT_EQUALITY_DELETES &&
		file->partition.fields_length == 0;

	if (spec == NULL || (!emptyEqualityDelete && spec->fields_length != file->partition.fields_length))
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
						errmsg("invalid partition spec or tuple for file \"%s\"", file->file_path)));
	if (emptyEqualityDelete)
		return spec;

	for (size_t i = 0; i < spec->fields_length; i++)
	{
		IcebergPartitionSpecField *specField = &spec->fields[i];

		for (size_t j = 0; j < i; j++)
			if (spec->fields[j].field_id == specField->field_id)
				ereport(ERROR, (errmsg("duplicate partition spec field ID %d", specField->field_id)));
		PartitionField *field = NULL;

		for (size_t j = 0; j < file->partition.fields_length; j++)
		{
			if (file->partition.fields[j].field_id == specField->field_id)
			{
				if (field != NULL)
					ereport(ERROR, (errmsg("duplicate partition field ID %d in file \"%s\"",
										   specField->field_id, file->file_path)));
				field = &file->partition.fields[j];
			}
		}
		if (field == NULL)
			ereport(ERROR, (errmsg("missing partition field ID %d in file \"%s\"",
								   specField->field_id, file->file_path)));

		bool		typeMatches = false;
		bool		isDay = strcmp(specField->transform, "day") == 0;
		bool		isVoid = strcmp(specField->transform, "void") == 0;

		for (size_t s = 0; s < metadata->schemas_length; s++)
		{
			IcebergTableSchema *schema = &metadata->schemas[s];
			DataFileSchema dataSchema = {schema->fields, schema->fields_length};
			DataFileSchemaField *source = FindTopLevelField(&dataSchema, specField->source_id);
			PGType		sourceType;

			if (source != NULL && source->type->type == FIELD_TYPE_SCALAR)
				sourceType = IcebergFieldToPostgresType(source->type);
			else
			{
				LeafField  *leaf = FindLeafField(GetLeafFieldsForIcebergSchema(schema), specField->source_id);

				if (leaf == NULL)
					continue;
				sourceType = leaf->pgType;
			}

			IcebergPartitionTransform transform = {0};

			transform.pgType = sourceType;
			transform.resultPgType = transform.pgType;
			if (strcmp(specField->transform, "year") == 0 ||
				strcmp(specField->transform, "month") == 0 ||
				strcmp(specField->transform, "hour") == 0 ||
				strncmp(specField->transform, "bucket[", 7) == 0)
				transform.resultPgType = MakePGType(INT4OID, -1);
			else if (isDay)
				transform.resultPgType = MakePGType(DATEOID, -1);
			else if (strcmp(specField->transform, "identity") != 0 &&
					 strcmp(specField->transform, "void") != 0 &&
					 strncmp(specField->transform, "truncate[", 9) != 0)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("unsupported equality delete partition transform %s", specField->transform)));
			typeMatches |= AvroTypesEqual(field->value_type, GetTransformResultAvroType(&transform));

			/*
			 * day also permits legacy int; void permits int or the source
			 * type.
			 */
			if (isDay || isVoid)
			{
				transform.resultPgType = MakePGType(INT4OID, -1);
				typeMatches |= AvroTypesEqual(field->value_type, GetTransformResultAvroType(&transform));
			}
		}
		if (!typeMatches)
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("partition field ID %d has an incompatible type in file \"%s\"",
								   specField->field_id, file->file_path)));
		/* Canonicalize equivalent encodings only after validating the spec. */
		if (isVoid)
		{
			if (field->value != NULL)
				ereport(ERROR, (errmsg("void partition field ID %d must be NULL in file \"%s\"",
									   specField->field_id, file->file_path)));
			field->value_type = (IcebergScalarAvroType)
			{
				0
			};
		}
		else if (isDay)
			field->value_type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_DATE;
	}
	return spec;
}

static bool
AvroTypesEqual(IcebergScalarAvroType left, IcebergScalarAvroType right)
{
	return left.physical_type == right.physical_type &&
		left.logical_type == right.logical_type &&
		left.precision == right.precision && left.scale == right.scale;
}

/*
 * Normalize values for both hashing and equality. Iceberg considers all NaNs
 * equal but preserves the IEEE 754 distinction between -0 and +0. Type
 * promotions and redundant decimal sign bytes must not split equal partitions.
 */
static PartitionValue
PartitionValueView(const PartitionField * field)
{
	PartitionValue view = {0};

	view.type = field->value_type;
	view.bytes = field->value;
	view.length = field->value_length;
	int			physical = field->value_type.physical_type;

	if (physical == ICEBERG_AVRO_PHYSICAL_TYPE_INT32)
		view.type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT64;
	else if (physical == ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT)
		view.type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE;
	if (view.type.logical_type == ICEBERG_AVRO_LOGICAL_TYPE_DECIMAL)
		view.type.precision = 0;
	if (field->value == NULL)
		return view;
	size_t		expectedLength = 0;

	switch (physical)
	{
		case ICEBERG_AVRO_PHYSICAL_TYPE_INT32:
			expectedLength = sizeof(int32);
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_INT64:
			expectedLength = sizeof(int64);
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT:
			expectedLength = sizeof(float);
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE:
			expectedLength = sizeof(double);
			break;
		case ICEBERG_AVRO_PHYSICAL_TYPE_BOOL:
			expectedLength = 1;
			break;
		default:
			break;
	}
	if (expectedLength && view.length != expectedLength)
		ereport(ERROR, (errmsg("invalid partition value length for field ID %d", field->field_id)));

	if (physical == ICEBERG_AVRO_PHYSICAL_TYPE_INT32 || physical == ICEBERG_AVRO_PHYSICAL_TYPE_INT64)
	{
		if (physical == ICEBERG_AVRO_PHYSICAL_TYPE_INT32)
		{
			int32		value;

			memcpy(&value, view.bytes, sizeof(value));
			view.integer = value;
		}
		else
			memcpy(&view.integer, view.bytes, sizeof(view.integer));
		view.type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT64;
	}
	else if (physical == ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT || physical == ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE)
	{
		if (physical == ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT)
		{
			float		value;

			memcpy(&value, view.bytes, sizeof(value));
			view.floating = value;
		}
		else
			memcpy(&view.floating, view.bytes, sizeof(view.floating));
		if (isnan(view.floating))
			view.floating = NAN;
		view.type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE;
	}
	else if (view.type.logical_type == ICEBERG_AVRO_LOGICAL_TYPE_DECIMAL)
	{
		const unsigned char *bytes = view.bytes;

		if (view.length == 0)
			ereport(ERROR, (errmsg("empty decimal partition value for field ID %d", field->field_id)));

		while (view.length > 1 &&
			   ((bytes[0] == 0 && !(bytes[1] & 0x80)) ||
				(bytes[0] == 0xff && (bytes[1] & 0x80))))
		{
			bytes++;
			view.length--;
		}
		view.bytes = bytes;
	}
	return view;
}

static const void *
PartitionValueBytes(PartitionValue * value, size_t *length)
{
	if (value->type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_INT64)
	{
		*length = sizeof(value->integer);
		return &value->integer;
	}
	if (value->type.physical_type == ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE)
	{
		*length = sizeof(value->floating);
		return &value->floating;
	}
	*length = value->length;
	return value->bytes;
}

static uint64
PartitionHash(DataFile * file)
{
	uint64		hash = hash_uint32(file->partition_spec_id);

	/* XOR makes field order irrelevant; IDs participate in each field hash. */
	for (size_t i = 0; i < file->partition.fields_length; i++)
	{
		PartitionField *field = &file->partition.fields[i];
		PartitionValue value = PartitionValueView(field);
		uint64		fieldHash = hash_uint32(field->field_id);

		fieldHash = hash_combine64(fieldHash, value.type.physical_type);
		fieldHash = hash_combine64(fieldHash, value.type.logical_type);
		fieldHash = hash_combine64(fieldHash, value.type.precision);
		fieldHash = hash_combine64(fieldHash, value.type.scale);
		fieldHash = hash_combine64(fieldHash, value.bytes != NULL);
		if (value.bytes != NULL)
		{
			size_t		length;
			const void *bytes = PartitionValueBytes(&value, &length);

			fieldHash = hash_combine64(fieldHash, hash_bytes_extended(bytes, length, 0));
		}
		hash ^= fieldHash;
	}
	return hash;
}

static bool
PartitionsEqual(const Partition * left, const Partition * right)
{
	if (left->fields_length != right->fields_length)
		return false;
	for (size_t i = 0; i < left->fields_length; i++)
	{
		const		PartitionField *a = &left->fields[i];
		const		PartitionField *b = NULL;

		for (size_t j = 0; j < right->fields_length; j++)
			if (right->fields[j].field_id == a->field_id)
				b = &right->fields[j];
		if (b == NULL)
			return false;
		PartitionValue av = PartitionValueView(a);
		PartitionValue bv = PartitionValueView(b);

		if (!AvroTypesEqual(av.type, bv.type))
			return false;
		if (av.bytes == NULL || bv.bytes == NULL)
		{
			if (av.bytes != bv.bytes)
				return false;
			continue;
		}
		size_t		alength,
					blength;
		const void *abytes = PartitionValueBytes(&av, &alength);
		const void *bbytes = PartitionValueBytes(&bv, &blength);

		if (alength != blength || memcmp(abytes, bbytes, alength) != 0)
			return false;
	}
	return true;
}

static int
CompareEqualityId(const void *left, const void *right)
{
	int			a = *(const int *) left;
	int			b = *(const int *) right;

	return (a > b) - (a < b);
}

static DataFileSchema *
EqualityKeySchema(IcebergTableMetadata * metadata, DataFile * file)
{
	if (pg_strcasecmp(file->file_format, "PARQUET") != 0)
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("equality delete file \"%s\" must use Parquet", file->file_path)));
	if (file->equality_ids_length == 0)
		ereport(ERROR, (errmsg("equality delete file \"%s\" has no equality IDs", file->file_path)));

	qsort(file->equality_ids, file->equality_ids_length, sizeof(int), CompareEqualityId);
	IcebergTableSchema *current = GetCurrentIcebergTableSchema(metadata);
	DataFileSchema currentSchema = {current->fields, current->fields_length};
	DataFileSchema *keys = palloc0(sizeof(DataFileSchema));

	keys->nfields = file->equality_ids_length;
	keys->fields = palloc0(sizeof(DataFileSchemaField) * keys->nfields);
	for (size_t i = 0; i < keys->nfields; i++)
	{
		int			id = file->equality_ids[i];

		if (id <= 0 || (i > 0 && id == file->equality_ids[i - 1]))
			ereport(ERROR, (errmsg("invalid or duplicate equality field ID %d in file \"%s\"", id, file->file_path)));
		DataFileSchemaField *field = FindTopLevelField(&currentSchema, id);

		if (field == NULL || field->type->type != FIELD_TYPE_SCALAR ||
			(strcmp(field->type->field.scalar.typeName, "int") != 0 &&
			 strcmp(field->type->field.scalar.typeName, "long") != 0 &&
			 strcmp(field->type->field.scalar.typeName, "string") != 0))
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("unsupported equality field ID %d in file \"%s\"", id, file->file_path),
							errdetail("Equality keys must be current top-level int, long, or string fields.")));

		/*
		 * Renames are safe because Parquet binds by ID. Key deletion and
		 * nesting need historical projections we do not yet have. Existing
		 * NULL projection and integer widening are safe to reuse for added
		 * keys and int-to-long promotion.
		 */
		for (size_t s = 0; s < metadata->schemas_length; s++)
		{
			IcebergTableSchema *history = &metadata->schemas[s];
			DataFileSchema historySchema = {history->fields, history->fields_length};
			DataFileSchemaField *old = FindTopLevelField(&historySchema, id);

			if (old == NULL)
			{
				LeafField  *leaf = FindLeafField(GetLeafFieldsForIcebergSchema(history), id);

				if (leaf == NULL)
					continue;	/* An added key projects as NULL in older
								 * data. */
			}
			bool		compatible = old != NULL && old->type->type == FIELD_TYPE_SCALAR &&
				(strcmp(old->type->field.scalar.typeName, field->type->field.scalar.typeName) == 0 ||
				 (strcmp(old->type->field.scalar.typeName, "int") == 0 &&
				  strcmp(field->type->field.scalar.typeName, "long") == 0));

			if (!compatible)
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("unsupported schema evolution for equality field ID %d in file \"%s\"", id, file->file_path),
								errdetail("Keys must remain top-level fields of the same type, allowing int-to-long promotion.")));
		}
		keys->fields[i] = *field;
		keys->fields[i].initialDefault = NULL;
		keys->fields[i].duckSerializedInitialDefault = NULL;
	}
	return keys;
}

static bool
KeySchemasEqual(DataFileSchema * left, DataFileSchema * right)
{
	if (left->nfields != right->nfields)
		return false;
	for (size_t i = 0; i < left->nfields; i++)
		if (left->fields[i].id != right->fields[i].id)
			return false;
	return true;
}

/* Keep only older data files whose known integer bounds may overlap. */
static void
AddCandidates(Bitmapset **mask, List *candidates, DataFile * dataFile)
{
	foreach_ptr(EqualityDeleteFile, deletion, candidates)
	{
		DataFile   *file = deletion->file;

		if (dataFile->data_sequence_number < file->data_sequence_number &&
			(deletion->isGlobal ||
			 (dataFile->partition_spec_id == file->partition_spec_id &&
			  PartitionsEqual(&dataFile->partition, &file->partition))) &&
			!IntegerBoundsDisjoint(dataFile, deletion))
			*mask = bms_add_member(*mask, deletion->index);
	}
}

/* Missing, ambiguous or invalid metrics cannot prove that a delete is irrelevant. */
static bool
ReadIntegerBound(ColumnBound * bounds, size_t count, int id, bool isLong, int64 *value, size_t *width)
{
	ColumnBound *match = NULL;

	for (size_t i = 0; i < count; i++)
	{
		if (bounds[i].column_id != id)
			continue;
		if (match != NULL)
			return false;
		match = &bounds[i];
	}
	if (match == NULL || match->value == NULL)
		return false;

	/* Iceberg bounds are little-endian; memcpy avoids alignment assumptions. */
	if (match->value_length == sizeof(int32))
	{
		uint32		encoded;

		memcpy(&encoded, match->value, sizeof(encoded));
#ifdef WORDS_BIGENDIAN
		encoded = pg_bswap32(encoded);
#endif
		*value = (int32) encoded;
		*width = sizeof(encoded);
		return true;
	}
	/* A promoted long may still carry four-byte int bounds in an older file. */
	if (isLong && match->value_length == sizeof(int64))
	{
		uint64		encoded;

		memcpy(&encoded, match->value, sizeof(encoded));
#ifdef WORDS_BIGENDIAN
		encoded = pg_bswap64(encoded);
#endif
		*value = (int64) encoded;
		*width = sizeof(encoded);
		return true;
	}
	return false;
}

/*
 * One disjoint component proves a composite equality key cannot match.
 * Bounds omit NULLs, so require an explicit zero delete null count for that
 * component. Otherwise NULL-safe equality could still delete NULL data keys.
 */
static bool
IntegerBoundsDisjoint(DataFile * dataFile, EqualityDeleteFile * deletion)
{
	DataFile   *deleteFile = deletion->file;

	for (size_t i = 0; i < deletion->schema->nfields; i++)
	{
		DataFileSchemaField *key = &deletion->schema->fields[i];
		const char *type = key->type->field.scalar.typeName;
		bool		isLong = strcmp(type, "long") == 0;

		if (!isLong && strcmp(type, "int") != 0)
			continue;

		int			nullStats = 0;
		bool		noDeleteNulls = false;

		for (size_t j = 0; j < deleteFile->null_value_counts_length; j++)
		{
			ColumnStat *stat = &deleteFile->null_value_counts[j];

			if (stat->column_id == key->id)
			{
				nullStats++;
				noDeleteNulls = stat->value == 0;
			}
		}
		if (nullStats != 1 || !noDeleteNulls)
			continue;

		int64		dataLower,
					dataUpper,
					deleteLower,
					deleteUpper;
		size_t		dataLowerWidth,
					dataUpperWidth,
					deleteLowerWidth,
					deleteUpperWidth;

		if (!ReadIntegerBound(dataFile->lower_bounds, dataFile->lower_bounds_length,
							  key->id, isLong, &dataLower, &dataLowerWidth) ||
			!ReadIntegerBound(dataFile->upper_bounds, dataFile->upper_bounds_length,
							  key->id, isLong, &dataUpper, &dataUpperWidth) ||
			!ReadIntegerBound(deleteFile->lower_bounds, deleteFile->lower_bounds_length,
							  key->id, isLong, &deleteLower, &deleteLowerWidth) ||
			!ReadIntegerBound(deleteFile->upper_bounds, deleteFile->upper_bounds_length,
							  key->id, isLong, &deleteUpper, &deleteUpperWidth))
			continue;
		if (dataLowerWidth != dataUpperWidth || deleteLowerWidth != deleteUpperWidth ||
			dataLower > dataUpper || deleteLower > deleteUpper)
			continue;
		if (dataUpper < deleteLower || deleteUpper < dataLower)
			return true;
	}
	return false;
}


List *
PlanIcebergEqualityDeletes(IcebergTableMetadata * metadata, List *dataFiles,
						   List *deleteFiles, List *fileScans, List **equalityDeleteScans)
{
	List	   *deletions = NIL;

	foreach_ptr(DataFile, file, deleteFiles)
		if (file->content == ICEBERG_DATA_FILE_CONTENT_EQUALITY_DELETES)
		deletions = lappend(deletions, file);
	if (deletions == NIL || dataFiles == NIL)
		return NIL;
	Assert(list_length(dataFiles) == list_length(fileScans));

	/*
	 * Scan paths and key field definitions borrow the snapshot's metadata.
	 * All plans, schemas, groups and candidate lists use its query-lifetime
	 * memory context and are not retained outside the scan snapshot.
	 */

	HASHCTL		ctl = {0};

	ctl.keysize = sizeof(uint64);
	ctl.entrysize = sizeof(DeleteCandidateBucket);
	ctl.hcxt = CurrentMemoryContext;
	HTAB	   *candidates = hash_create("equality partition candidates", 32, &ctl,
										 HASH_BLOBS | HASH_ELEM | HASH_CONTEXT);

	ctl.keysize = sizeof(uint32);
	ctl.entrysize = sizeof(ReadGroupBucket);
	HTAB	   *groups = hash_create("equality read groups", 32, &ctl,
									 HASH_BLOBS | HASH_ELEM | HASH_CONTEXT);
	List	   *globalDeletes = NIL;
	List	   *deletePlans = NIL;
	List	   *schemas = NIL;

	foreach_ptr(DataFile, file, deletions)
	{
		EqualityDeleteFile *plan = palloc0(sizeof(EqualityDeleteFile));

		plan->file = file;
		plan->index = list_length(deletePlans);
		plan->schema = EqualityKeySchema(metadata, file);
		foreach_ptr(DataFileSchema, schema, schemas)
			if (KeySchemasEqual(schema, plan->schema))
		{
			pfree(plan->schema->fields);
			pfree(plan->schema);
			plan->schema = schema;
			break;
		}
		if (!list_member_ptr(schemas, plan->schema))
			schemas = lappend(schemas, plan->schema);
		deletePlans = lappend(deletePlans, plan);
		IcebergPartitionSpec *spec = ValidatePartition(metadata, file);

		/* Specs retaining only removed (void) fields are also unpartitioned. */
		plan->isGlobal = true;
		if (file->partition.fields_length > 0)
		{
			for (size_t i = 0; i < spec->fields_length; i++)
				if (strcmp(spec->fields[i].transform, "void") != 0)
					plan->isGlobal = false;
		}
		if (plan->isGlobal)
			globalDeletes = lappend(globalDeletes, plan);
		else
		{
			uint64		hash = PartitionHash(file);
			bool		found;
			DeleteCandidateBucket *bucket = hash_search(candidates, &hash, HASH_ENTER, &found);

			if (!found)
				bucket->files = NIL;
			bucket->files = lappend(bucket->files, plan);
		}
	}

	Bitmapset  *used = NULL;
	List	   *result = NIL;
	ListCell   *dataCell,
			   *scanCell;

	forboth(dataCell, dataFiles, scanCell, fileScans)
	{
		DataFile   *file = lfirst(dataCell);
		PgLakeFileScan *scan = lfirst(scanCell);

		ValidatePartition(metadata, file);
		uint64		hash = PartitionHash(file);
		DeleteCandidateBucket *bucket = hash_search(candidates, &hash, HASH_FIND, NULL);
		Bitmapset  *mask = NULL;

		AddCandidates(&mask, globalDeletes, file);
		if (bucket != NULL)
			AddCandidates(&mask, bucket->files, file);
		used = bms_add_members(used, mask);

		uint32		groupHash = bms_hash_value(mask);
		bool		found;
		ReadGroupBucket *groupBucket = hash_search(groups, &groupHash, HASH_ENTER, &found);

		if (!found)
			groupBucket->groups = NIL;
		EqualityReadGroup *group = NULL;

		foreach_ptr(EqualityReadGroup, candidate, groupBucket->groups)
			if (bms_equal(candidate->mask, mask))
		{
			group = candidate;
			break;
		}
		if (group == NULL)
		{
			group = palloc0(sizeof(EqualityReadGroup));
			group->mask = mask;
			mask = NULL;
			groupBucket->groups = lappend(groupBucket->groups, group);
			result = lappend(result, &group->scan);
			int			index = -1;

			while ((index = bms_next_member(group->mask, index)) >= 0)
			{
				EqualityDeleteFile *plan = list_nth(deletePlans, index);
				PgLakeEqualityDeleteScan *deleteScan = NULL;

				foreach_ptr(PgLakeEqualityDeleteScan, candidate, group->scan.deleteScans)
					if (candidate->schema == plan->schema)
					deleteScan = candidate;
				if (deleteScan == NULL)
				{
					deleteScan = palloc0(sizeof(PgLakeEqualityDeleteScan));
					deleteScan->schema = plan->schema;
					group->scan.deleteScans = lappend(group->scan.deleteScans, deleteScan);
				}
				deleteScan->paths = lappend(deleteScan->paths, (char *) plan->file->file_path);
			}
		}
		bms_free(mask);
		group->scan.fileScans = lappend(group->scan.fileScans, scan);
	}

	/* Unique planned files for footer validation and EXPLAIN statistics. */
	foreach_ptr(EqualityDeleteFile, plan, deletePlans)
		if (bms_is_member(plan->index, used))
	{
		PgLakeEqualityDeleteScan *scan = palloc0(sizeof(PgLakeEqualityDeleteScan));

		scan->schema = plan->schema;
		scan->paths = list_make1((char *) plan->file->file_path);
		*equalityDeleteScans = lappend(*equalityDeleteScans, scan);
	}
	bms_free(used);
	/* The public scan is the first member of each private read group. */
	foreach_ptr(EqualityReadGroup, group, result)
	{
		bms_free(group->mask);
		group->mask = NULL;
	}
	hash_destroy(candidates);
	hash_destroy(groups);
	return result;
}

/*
 * TableScanHasEqualityDeletes includes inherited child scans when deciding
 * whether plain EXPLAIN must avoid physical equality delete files.
 */
static bool
TableScanHasEqualityDeletes(PgLakeTableScan * tableScan)
{
	if (tableScan->equalityDeleteReadGroups != NIL)
		return true;

	foreach_ptr(PgLakeTableScan, childScan, tableScan->childScans)
	{
		if (TableScanHasEqualityDeletes(childScan))
			return true;
	}

	return false;
}

/*
 * SnapshotHasEqualityDeletes returns whether any table or inherited child
 * in the snapshot has planned equality delete read groups.
 */
bool
SnapshotHasEqualityDeletes(PgLakeScanSnapshot * snapshot)
{
	if (snapshot == NULL)
		return false;

	foreach_ptr(PgLakeTableScan, tableScan, snapshot->tableScans)
	{
		if (TableScanHasEqualityDeletes(tableScan))
			return true;
	}

	return false;
}

/* Validate at execution time; SQL generation and EXPLAIN need no footer I/O. */
void
ValidateEqualityDeletesForSnapshot(PgLakeScanSnapshot * snapshot)
{
	if (!EnableEqualityDeleteValidation)
		return;

	foreach_ptr(PgLakeTableScan, tableScan, snapshot->tableScans)
		ValidateEqualityDeletesForTableScan(tableScan);
}

static void
ValidateEqualityDeletesForTableScan(PgLakeTableScan * tableScan)
{
	if (!tableScan->equalityDeleteFilesValidated)
	{
		ValidateEqualityDeleteFiles(tableScan->equalityDeleteScans);
		tableScan->equalityDeleteFilesValidated = true;
	}

	foreach_ptr(PgLakeTableScan, childScan, tableScan->childScans)
		ValidateEqualityDeletesForTableScan(childScan);
}

/*
 * Unlike old data files, a delete file must physically contain every declared
 * key. The ordinary schema reader fills missing columns with NULL, which here
 * could incorrectly delete real NULL keys. deleteScans is the per-file list
 * from PlanIcebergEqualityDeletes: each entry deliberately has one path. The
 * grouped, multi-path scans are used only for SQL generation, so linitial here
 * does not skip other files sharing the same equality key schema.
 *
 * Batch footer queries to bound query size and reduce pgduck_server round trips.
 * No delete rows are read.
 */
void
ValidateEqualityDeleteFiles(List *deleteScans)
{
	ListCell   *cell = list_head(deleteScans);

	while (cell != NULL)
	{
		PgLakeEqualityDeleteScan *scans[32];
		int			nscans = 0;
		StringInfo	query = makeStringInfo();

		while (cell != NULL && nscans < lengthof(scans))
		{
			PgLakeEqualityDeleteScan *scan = lfirst(cell);

			Assert(list_length(scan->paths) == 1);
			if (nscans > 0)
				appendStringInfoString(query, " UNION ALL ");

			/* column_id is the footer's schema-tree index, not its field ID. */
			appendStringInfo(query,
							 "SELECT field_id, num_children, duckdb_type, %d AS file_index, "
							 "column_id AS schema_index FROM parquet_schema(%s)",
							 nscans, quote_literal_cstr(linitial(scan->paths)));
			scans[nscans++] = scan;
			cell = lnext(deleteScans, cell);
		}
		appendStringInfoString(query, " ORDER BY file_index, schema_index");

		PGDuckConnection *connection = GetPGDuckConnection();
		PGresult   *volatile result = NULL;

		PG_TRY();
		{
			/* The FINALLY block owns both result and connection on errors. */
			result = ExecuteQueryOnPGDuckConnection(connection, query->data);
			ThrowIfPGDuckResultHasError(connection, result);
			int			row = 0;
			int			rows = PQntuples(result);

			for (int i = 0; i < nscans; i++)
			{
				int			startRow = row;

				while (row < rows && atoi(PQgetvalue(result, row, 3)) == i)
					row++;
				ValidateEqualityDeleteFile(result, scans[i], startRow, row);
			}
		}
		PG_FINALLY();
		{
			PQclear(result);
			ReleasePGDuckConnection(connection);
		}
		PG_END_TRY();
		pfree(query->data);
		pfree(query);
	}
}

/* Validate one complete footer, retaining the per-file error context. */
static void
ValidateEqualityDeleteFile(PGresult *result, PgLakeEqualityDeleteScan * scan,
						   int startRow, int endRow)
{
	const char *path = linitial(scan->paths);
	int		   *remainingChildren = palloc0(sizeof(int) * Max(endRow - startRow, 1));
	int			depth = 0;
	int		   *keyOccurrences = palloc0(sizeof(int) * scan->schema->nfields);

	for (int row = startRow; row < endRow; row++)
	{
		/*
		 * parquet_schema emits the root and its descendants in preorder.
		 * Track unfinished parents: depth 1 is a direct child of the root.
		 * Visit non-key columns too, since they can contain nested fields.
		 */
		while (depth > 0 && remainingChildren[depth - 1] == 0)
			depth--;
		if (depth > 0)
			remainingChildren[depth - 1]--;
		int			fieldDepth = depth;
		bool		isScalar = PQgetisnull(result, row, 1);

		if (!isScalar)
			remainingChildren[depth++] = atoi(PQgetvalue(result, row, 1));

		/* Only declared equality keys need the checks below. */
		if (PQgetisnull(result, row, 0))
			continue;
		int			id = atoi(PQgetvalue(result, row, 0));

		for (size_t i = 0; i < scan->schema->nfields; i++)
		{
			DataFileSchemaField *key = &scan->schema->fields[i];

			if (key->id != id)
				continue;
			/* Each key must occur once as a scalar directly under the root. */
			if (fieldDepth != 1 || !isScalar)
				ereport(ERROR, (errmsg("equality field ID %d is not a top-level scalar in file \"%s\"", id, path)));
			if (++keyOccurrences[i] > 1)
				ereport(ERROR, (errmsg("equality field ID %d is missing or duplicated in Parquet file \"%s\"", id, path)));

			const char *parquetType = PQgetvalue(result, row, 2);
			const char *icebergType = key->type->field.scalar.typeName;

			if (!EqualityDeleteKeyTypeMatches(icebergType, parquetType))
				ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
								errmsg("equality field ID %d has an incompatible Parquet type in file \"%s\"", id, path)));
		}
	}

	/* A missing key would otherwise be projected as NULL by the reader. */
	for (size_t i = 0; i < scan->schema->nfields; i++)
		if (keyOccurrences[i] != 1)
			ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION),
							errmsg("equality field ID %d is missing or duplicated in Parquet file \"%s\"", scan->schema->fields[i].id, path)));
	pfree(remainingChildren);
	pfree(keyOccurrences);
}


/* Match the supported key types, including Iceberg's int-to-long promotion. */
static bool
EqualityDeleteKeyTypeMatches(const char *icebergType, const char *parquetType)
{
	if (strcmp(icebergType, "int") == 0)
		return strcmp(parquetType, "INTEGER") == 0;
	if (strcmp(icebergType, "long") == 0)
		return strcmp(parquetType, "BIGINT") == 0 || strcmp(parquetType, "INTEGER") == 0;
	if (strcmp(icebergType, "string") == 0)
		return strcmp(parquetType, "VARCHAR") == 0;
	return false;
}
