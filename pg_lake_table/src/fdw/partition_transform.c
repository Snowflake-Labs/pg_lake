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
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/lsyscache.h"

#include "pg_lake/partitioning/partition_by_parser.h"
#include "pg_lake/partitioning/partition_spec_catalog.h"
#include "pg_lake/pgduck/numeric.h"
#include "pg_lake/fdw/partition_transform.h"
#include "pg_lake/fdw/schema_operations/field_id_mapping_catalog.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/iceberg/iceberg_type_numeric_binary_serde.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/utils.h"
#include "pg_lake/iceberg/hash_utils.h"
#include "pg_lake/iceberg/truncate_utils.h"
#include "pg_lake/util/numeric.h"

static PartitionField * ApplyPartitionTransformToTuple(IcebergPartitionTransform * transform,
													   TupleTableSlot *slot);
static void *ApplyIdentityTransformToColumn(IcebergPartitionTransform * transform,
											Datum columnValue, bool isNull,
											size_t *valueSize);
static void *ApplyTruncateTransformToColumn(IcebergPartitionTransform * transform,
											Datum columnValue, bool isNull,
											size_t *valueSize);
static void *ApplyYearTransformToColumn(IcebergPartitionTransform * transform,
										Datum columnValue, bool isNull,
										size_t *valueSize);
static void *ApplyMonthTransformToColumn(IcebergPartitionTransform * transform,
										 Datum columnValue, bool isNull,
										 size_t *valueSize);
static void *ApplyDayTransformToColumn(IcebergPartitionTransform * transform,
									   Datum columnValue, bool isNull,
									   size_t *valueSize);
static void *ApplyHourTransformToColumn(IcebergPartitionTransform * transform,
										Datum columnValue, bool isNull,
										size_t *valueSize);
static IcebergPartitionTransform * GetPartitionTransformFromSpecField(Oid relationId,
																	  IcebergPartitionSpecField * specField);
static void ParseTransformName(const char *name, IcebergPartitionTransformType * type,
							   size_t *bucketCount, size_t *truncateLen);
static bool ParseBracketUintSize(const char *name, const char *prefix, size_t *outVal);
static void *DatumToPartitionValue(IcebergPartitionTransform * transform, Datum columnValue, bool isNull,
								   size_t *valuelength);
static bool PartitionTransformsEqual(IcebergPartitionSpec * spec, List *partitionTransforms);

/*
 * ComputePartitionTupleForTuple applies relative partition transforms
 * to the given tuple and returns partition tuple for data file.
 */
Partition *
ComputePartitionTupleForTuple(List *transforms, TupleTableSlot *slot)
{
	Assert(transforms != NIL);

	Partition  *partition = palloc0(sizeof(Partition));

	partition->fields = palloc0(sizeof(PartitionField) * list_length(transforms));
	partition->fields_length = list_length(transforms);

	for (int i = 0; i < list_length(transforms); i++)
	{
		IcebergPartitionTransform *transform = list_nth(transforms, i);

		PartitionField *field = ApplyPartitionTransformToTuple(transform, slot);

		partition->fields[i] = *field;
	}

	return partition;
}


/*
* If a partition spec with the given partition transforms already exists
* for the given relation, returns the partition spec. Otherwise, it
* returns NULL.
*/
IcebergPartitionSpec *
GetPartitionSpecIfAlreadyExist(Oid relationId, List *partitionTransforms)
{
	HTAB	   *allSpecs = GetAllPartitionSpecsFromCatalog(relationId);

	HASH_SEQ_STATUS currentSpecsStatus;

	hash_seq_init(&currentSpecsStatus, allSpecs);
	IcebergPartitionSpecHashEntry *currentSpec = NULL;

	while ((currentSpec = hash_seq_search(&currentSpecsStatus)) != NULL)
	{
		/*
		 * Partition fields in spec is already ordered by partition field id,
		 * so we can compare them one by one with the given partition
		 * transforms.
		 */
		if (PartitionTransformsEqual(currentSpec->spec, partitionTransforms))
		{
			IcebergPartitionSpec *foundSpec = currentSpec->spec;

			hash_seq_term(&currentSpecsStatus);

			return foundSpec;
		}
	}

	return NULL;
}

/*
* PartitionTransformsEqual compares the given partition spec
* with the given list of partition transforms. It returns true
* if they are equal, false otherwise.
*/
static bool
PartitionTransformsEqual(IcebergPartitionSpec * spec, List *partitionTransforms)
{
	if (spec->fields_length != list_length(partitionTransforms))
		return false;

	for (int i = 0; i < spec->fields_length; i++)
	{
		IcebergPartitionSpecField *specField = &spec->fields[i];
		IcebergPartitionTransform *transform = list_nth(partitionTransforms, i);

		/*
		 * Normally, the name check should be sufficient, as we currently do
		 * not allow renaming any column that is used in partitioning, see
		 * ErrorIfColumnEverUsedInIcebergPartitionSpec(). Still, let's be
		 * defensive and also check source field ids.
		 */
		if (specField->source_id != transform->sourceField->id)
			return false;

		/*
		 * This like a_bucket_10, c_year, etc., so we it includes both the
		 * column name, the transform type and the parameters to
		 * bucket/truncate transforms. We are essentially following what
		 * Iceberg does here:
		 * https://github.com/apache/iceberg/blob/8b55ac834015ce664f879ecfe1e80a941a994420/api/src/main/java/org/apache/iceberg/PartitionSpec.java#L239-L259
		 */
		if (strcasecmp(specField->name, transform->partitionFieldName) != 0)
		{
			return false;
		}
	}

	return true;
}



/*
* For a given relationId, this function returns the list of
* IcebergPartitionTransform for the table with the current
* partition spec. For non-partitioned tables, it returns NIL.
*/
List *
CurrentPartitionTransformList(Oid relationId)
{
	List	   *partitionTransforms = NIL;
	int			specId = GetCurrentSpecId(relationId);

	/* partitioned table, fill the partitionTransforms */
	if (specId != DEFAULT_SPEC_ID)
	{
		List	   *partitionFields = GetIcebergSpecPartitionFieldsFromCatalog(relationId, specId);

		/* get the partition transforms for the table */
		partitionTransforms = GetPartitionTransformsFromSpecFields(relationId, partitionFields);
	}

	return partitionTransforms;
}

/*
* For a given relationId, this function returns all the
* IcebergPartitionTransform for the table for any spec id.
* When reading data files, this function is used to get the
* partition transforms for the table. We might read data files
* from any previous specId.
* For non-partitioned tables, it returns NIL.
*/
List *
AllPartitionTransformList(Oid relationId)
{
	List	   *partitionFields = GetAllIcebergSpecPartitionFieldsFromCatalog(relationId);

	if (partitionFields == NIL)
	{
		/* not partitioned */
		return NIL;
	}

	return GetPartitionTransformsFromSpecFields(relationId, partitionFields);
}



/*
* GetPartitionTransformsFromSpecFields is a wrapper around
* GetPartitionTransformFromSpecField, which takes a list of
* IcebergPartitionSpecField and returns a list of
* IcebergPartitionTransform.
*/
List *
GetPartitionTransformsFromSpecFields(Oid relationId, List *specFields)
{
	List	   *transformList = NIL;

	ListCell   *lc;

	foreach(lc, specFields)
	{
		IcebergPartitionSpecField *specField = lfirst(lc);

		IcebergPartitionTransform *transform = GetPartitionTransformFromSpecField(relationId, specField);

		Assert(transform != NULL);
		transformList = lappend(transformList, transform);
	}

	return transformList;

}

/*
* For a given relationId and specField, this function returns the
* corresponding IcebergPartitionTransform. The latter is a superset of
* IcebergPartitionSpecField, and contains the transform name, type,
* bucket count, and truncate length for ease of use.
*/
static IcebergPartitionTransform *
GetPartitionTransformFromSpecField(Oid relationId, IcebergPartitionSpecField * specField)
{
	IcebergPartitionTransform *transform = palloc0(sizeof(IcebergPartitionTransform));

	transform->partitionFieldId = specField->field_id;
	transform->partitionFieldName = pstrdup(specField->name);
	transform->transformName = pstrdup(specField->transform);

	transform->attnum =
		GetAttributeForFieldId(relationId, specField->source_id);
	transform->columnName = get_attname(relationId, transform->attnum, false);
	transform->pgType = GetAttributePGType(relationId, transform->attnum);
	transform->sourceField = GetRegisteredFieldForAttribute(relationId, transform->attnum);

	/* parse transform name */
	ParseTransformName(transform->transformName,
					   &transform->type,
					   &transform->bucketCount,
					   &transform->truncateLen);

	/* set transform's postgres type */
	transform->resultPgType = GetTransformResultPGType(transform);

	return transform;
}


/*
 * ParseTransformName
 *    Reverse of GenerateTransformName().
 *
 *    Input  :   name          – text like "identity", "bucket[16]" …
 *    Output :  *type          – transform enum
 *              *bucketCount   – set when type == BUCKET
 *              *truncateLen   – set when type == TRUNCATE
 */
static void
ParseTransformName(const char *name,
				   IcebergPartitionTransformType * type,
				   size_t *bucketCount,
				   size_t *truncateLen)
{
	/* Defensive – clear outputs first */
	if (bucketCount)
		*bucketCount = 0;
	if (truncateLen)
		*truncateLen = 0;

	/* Check for empty name */
	if (name == NULL || *name == '\0')
	{
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("empty partition transform name")));
	}
	else if (pg_strncasecmp(name, "identity", strlen("identity")) == 0)
	{
		*type = PARTITION_TRANSFORM_IDENTITY;
		return;
	}
	else if (pg_strncasecmp(name, "year", strlen("year")) == 0)
	{
		*type = PARTITION_TRANSFORM_YEAR;
		return;
	}
	else if (pg_strncasecmp(name, "month", strlen("month")) == 0)
	{
		*type = PARTITION_TRANSFORM_MONTH;
		return;
	}
	else if (pg_strncasecmp(name, "day", strlen("day")) == 0)
	{
		*type = PARTITION_TRANSFORM_DAY;
		return;
	}
	else if (pg_strncasecmp(name, "hour", strlen("hour")) == 0)
	{
		*type = PARTITION_TRANSFORM_HOUR;
		return;
	}
	else if (pg_strncasecmp(name, "void", strlen("void")) == 0)
	{
		*type = PARTITION_TRANSFORM_VOID;
		return;
	}
	else if (pg_strncasecmp(name, "bucket[", strlen("bucket[")) == 0 &&
			 ParseBracketUintSize(name, "bucket[", bucketCount))
	{
		*type = PARTITION_TRANSFORM_BUCKET;
		return;
	}
	else if (pg_strncasecmp(name, "truncate[", strlen("truncate[")) == 0 &&
			 ParseBracketUintSize(name, "truncate[", truncateLen))
	{
		*type = PARTITION_TRANSFORM_TRUNCATE;
		return;
	}

	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("unknown partition transform \"%s\"", name)));
}


/*
* ParseBracketUintSize parses a string of the form "prefix[digits]"
* and returns the digits as a size_t value.
*/
static bool
ParseBracketUintSize(const char *name, const char *prefix, size_t *outVal)
{
	size_t		prefixLen = strlen(prefix);

	if (pg_strncasecmp(name, prefix, prefixLen) != 0)
		return false;

	const char *digits = name + prefixLen;
	const char *endOfStr = strchr(digits, ']');

	if (endOfStr == NULL ||
		digits == endOfStr ||
		endOfStr - digits >= 32)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("invalid transform \"%s\"", name)));

	char		numbuf[32];

	memcpy(numbuf, digits, endOfStr - digits);
	numbuf[endOfStr - digits] = '\0';

	int64		parsedValue = pg_strtoint64(numbuf);

	if (parsedValue <= 0 || parsedValue > INT32_MAX)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("numeric argument out of range in \"%s\"", name)));

	if (outVal)
		*outVal = (size_t) parsedValue;

	return true;
}


/*
 * ApplyPartitionTransformToTuple applies the partition transform to the given tuple
 * and returns the partition field.
 */
static PartitionField *
ApplyPartitionTransformToTuple(IcebergPartitionTransform * transform, TupleTableSlot *slot)
{
	PartitionField *field = palloc0(sizeof(PartitionField));

	field->field_name = pstrdup(transform->partitionFieldName);
	field->field_id = transform->partitionFieldId;

	bool		isNull = false;
	Datum		columnValue = slot_getattr(slot, transform->attnum, &isNull);

	switch (transform->type)
	{
		case PARTITION_TRANSFORM_IDENTITY:
			field->value = ApplyIdentityTransformToColumn(transform, columnValue, isNull,
														  &field->value_length);
			break;
		case PARTITION_TRANSFORM_TRUNCATE:
			field->value = ApplyTruncateTransformToColumn(transform, columnValue, isNull,
														  &field->value_length);
			break;
		case PARTITION_TRANSFORM_YEAR:
			field->value = ApplyYearTransformToColumn(transform, columnValue, isNull,
													  &field->value_length);
			break;
		case PARTITION_TRANSFORM_MONTH:
			field->value = ApplyMonthTransformToColumn(transform, columnValue, isNull,
													   &field->value_length);
			break;
		case PARTITION_TRANSFORM_DAY:
			field->value = ApplyDayTransformToColumn(transform, columnValue, isNull,
													 &field->value_length);
			break;
		case PARTITION_TRANSFORM_HOUR:
			field->value = ApplyHourTransformToColumn(transform, columnValue, isNull,
													  &field->value_length);
			break;
		case PARTITION_TRANSFORM_BUCKET:
			field->value = ApplyBucketTransformToColumn(transform, columnValue, isNull,
														&field->value_length);
			break;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("applying transform %s is not yet support ",
							transform->transformName)));
	}

	field->value_type = GetTransformResultAvroType(transform);

	return field;
}



/*
 * ApplyIdentityTransformToColumn applies identity transform to the given column value
 * and returns the computed partition value.
 */
static void *
ApplyIdentityTransformToColumn(IcebergPartitionTransform * transform, Datum columnValue,
							   bool isNull, size_t *valueSize)
{
	if (isNull)
	{
		*valueSize = 0;
		return NULL;
	}

	return PGIcebergBinarySerializePartitionFieldValue(columnValue, transform->sourceField->type,
													   transform->pgType, valueSize);
}


/*
 * ApplyTruncateTransformToColumn applies truncate transform to the given column value
 * and returns the computed partition value.
 */
static void *
ApplyTruncateTransformToColumn(IcebergPartitionTransform * transform, Datum columnValue,
							   bool isNull, size_t *valueSize)
{
	if (isNull)
	{
		*valueSize = 0;
		return NULL;
	}

	PGType		sourceType = transform->pgType;
	PGType		resultType = transform->resultPgType;
	int64_t		truncateLen = (int64_t) transform->truncateLen;
	Datum		truncatedColumnValue = 0;

	if (sourceType.postgresTypeOid == INT2OID)
	{
		/*
		 * there is no short type in iceberg spec, we cast it to int to
		 * prevent overflow during truncation
		 */
		int32_t		value = (int32_t) DatumGetInt16(columnValue);

		truncatedColumnValue = IcebergTruncateTransformInt32(Int32GetDatum(value), truncateLen);
	}
	else if (sourceType.postgresTypeOid == INT4OID)
	{
		truncatedColumnValue = IcebergTruncateTransformInt32(columnValue, truncateLen);
	}
	else if (sourceType.postgresTypeOid == INT8OID)
	{
		truncatedColumnValue = IcebergTruncateTransformInt64(columnValue, truncateLen);
	}
	else if (sourceType.postgresTypeOid == TEXTOID ||
			 sourceType.postgresTypeOid == VARCHAROID ||
			 sourceType.postgresTypeOid == BPCHAROID)
	{
		truncatedColumnValue = IcebergTruncateTransformText(columnValue, truncateLen);
	}
	else if (sourceType.postgresTypeOid == BYTEAOID)
	{
		truncatedColumnValue = IcebergTruncateTransformBytea(columnValue, truncateLen);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot perform truncate transform on type %s", format_type_be(sourceType.postgresTypeOid))));
	}

	Field	   *resultField = PostgresTypeToIcebergField(resultType, false, NULL);

	return PGIcebergBinarySerializePartitionFieldValue(truncatedColumnValue, resultField,
													   resultType, valueSize);
}


/*
 * ApplyYearTransformToColumn applies year transform to the given column value
 * and returns the computed partition value.
 */
static void *
ApplyYearTransformToColumn(IcebergPartitionTransform * transform, Datum columnValue, bool isNull,
						   size_t *valueSize)
{
	if (isNull)
	{
		*valueSize = 0;
		return NULL;
	}

	int32_t		year = 0;

	if (transform->pgType.postgresTypeOid == DATEOID)
	{
		DateADT		dateValue = DatumGetDateADT(columnValue);

		year = DateYearFromUnixEpoch(dateValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	timestampValue = DatumGetTimestamp(columnValue);

		year = TimestampYearFromUnixEpoch(timestampValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz timestamptzValue = DatumGetTimestampTz(columnValue);

		year = TimestampYearFromUnixEpoch(timestamptzValue);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot perform year transform on type %s", format_type_be(transform->pgType.postgresTypeOid))));
	}

	int32_t    *value = palloc0(sizeof(int32_t));

	*value = year;
	*valueSize = sizeof(int32_t);

	return value;
}


/*
 * ApplyMonthTransformToColumn applies month transform to the given column value
 * and returns the computed partition value.
 */
static void *
ApplyMonthTransformToColumn(IcebergPartitionTransform * transform, Datum columnValue, bool isNull,
							size_t *valueSize)
{
	if (isNull)
	{
		*valueSize = 0;
		return NULL;
	}

	int32_t		month = 0;

	if (transform->pgType.postgresTypeOid == DATEOID)
	{
		DateADT		dateValue = DatumGetDateADT(columnValue);

		month = DateMonthFromUnixEpoch(dateValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	timestampValue = DatumGetTimestamp(columnValue);

		month = TimestampMonthFromUnixEpoch(timestampValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz timestamptzValue = DatumGetTimestampTz(columnValue);

		month = TimestampMonthFromUnixEpoch(timestamptzValue);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot perform month transform on type %s", format_type_be(transform->pgType.postgresTypeOid))));
	}

	int32_t    *value = palloc0(sizeof(int32_t));

	*value = month;
	*valueSize = sizeof(int32_t);

	return value;
}


/*
 * ApplyDayTransformToColumn applies day transform to the given column value
 * and returns the computed partition value.
 */
static void *
ApplyDayTransformToColumn(IcebergPartitionTransform * transform, Datum columnValue, bool isNull,
						  size_t *valueSize)
{
	if (isNull)
	{
		*valueSize = 0;
		return NULL;
	}

	int32_t		day = 0;

	if (transform->pgType.postgresTypeOid == DATEOID)
	{
		DateADT		dateValue = DatumGetDateADT(columnValue);

		day = DateDayFromUnixEpoch(dateValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	timestampValue = DatumGetTimestamp(columnValue);

		day = TimestampDayFromUnixEpoch(timestampValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz timestamptzValue = DatumGetTimestampTz(columnValue);

		day = TimestampDayFromUnixEpoch(timestamptzValue);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot perform day transform on type %s", format_type_be(transform->pgType.postgresTypeOid))));
	}

	int32_t    *value = palloc0(sizeof(int32_t));

	*value = day;
	*valueSize = sizeof(int32_t);

	return value;
}


/*
 * ApplyHourTransformToColumn applies hour transform to the given column value
 * and returns the computed partition value.
 */
static void *
ApplyHourTransformToColumn(IcebergPartitionTransform * transform, Datum columnValue, bool isNull,
						   size_t *valueSize)
{
	if (isNull)
	{
		*valueSize = 0;
		return NULL;
	}

	int32_t		hour = 0;

	if (transform->pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	timestampValue = DatumGetTimestamp(columnValue);

		hour = TimestampHourFromUnixEpoch(timestampValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz timestamptzValue = DatumGetTimestampTz(columnValue);

		hour = TimestampHourFromUnixEpoch(timestamptzValue);
	}
	else if (transform->pgType.postgresTypeOid == TIMEOID)
	{
		TimeADT		timeValue = DatumGetTimeADT(columnValue);

		hour = TimeHourFromUnixEpoch(timeValue);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot perform hour transform on type %s", format_type_be(transform->pgType.postgresTypeOid))));
	}

	int32_t    *value = palloc0(sizeof(int32_t));

	*value = hour;
	*valueSize = sizeof(int32_t);

	return value;
}


/*
 * ApplyBucketTransformToColumn applies bucket transform to the given column value
 * and returns the computed partition value.
 */
void *
ApplyBucketTransformToColumn(IcebergPartitionTransform * transform, Datum columnValue, bool isNull,
							 size_t *bucketSize)
{
	if (isNull)
	{
		*bucketSize = 0;
		return NULL;
	}

	/* (murmur3_x86_32_hash(x) & Integer.MAX_VALUE) % N */
	int32_t    *bucketValue = palloc0(sizeof(int32_t));

	/* bucket is always int */
	*bucketSize = sizeof(int32_t);

	if (transform->pgType.postgresTypeOid == INT2OID)
	{
		int64_t		value = (int64_t) DatumGetInt16(columnValue);

		*bucketValue = (MurmurHash3_32_Long(value) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == INT4OID)
	{
		/*
		 * spec hashes long bytes of int type to ensure schema evolution does
		 * not change partition bucket when int promoted to long
		 */
		int64_t		value = (int64_t) DatumGetInt32(columnValue);

		*bucketValue = (MurmurHash3_32_Long(value) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == INT8OID)
	{
		int64_t		value = DatumGetInt64(columnValue);

		*bucketValue = (MurmurHash3_32_Long(value) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == TEXTOID ||
			 transform->pgType.postgresTypeOid == VARCHAROID ||
			 transform->pgType.postgresTypeOid == BPCHAROID)
	{
		const char *value = TextDatumGetCString(columnValue);

		*bucketValue = (MurmurHash3_32_Bytes(value, strlen(value)) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == BYTEAOID)
	{
		bytea	   *value = DatumGetByteaP(columnValue);

		*bucketValue = (MurmurHash3_32_Bytes(VARDATA_ANY(value), VARSIZE_ANY_EXHDR(value)) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == DATEOID)
	{
		DateADT		value = DatumGetDateADT(columnValue);

		int32_t		daysFromEpoch = AdjustDateFromPostgresToUnix(value);

		/*
		 * spec normally hashes int bytes for date type but spark hashes long
		 * bytes of date. We follow spark here.
		 */
		*bucketValue = (MurmurHash3_32_Long(daysFromEpoch) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPOID)
	{
		Timestamp	value = DatumGetTimestamp(columnValue);

		int64_t		microsecsFromEpoch = AdjustTimestampFromPostgresToUnix(value);

		*bucketValue = (MurmurHash3_32_Long(microsecsFromEpoch) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == TIMESTAMPTZOID)
	{
		TimestampTz value = DatumGetTimestampTz(columnValue);

		int64_t		microsecsFromEpoch = AdjustTimestampFromPostgresToUnix(value);

		*bucketValue = (MurmurHash3_32_Long(microsecsFromEpoch) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == TIMEOID)
	{
		TimeADT		value = DatumGetTimeADT(columnValue);

		int64_t		microsecsFromMidnight = value;

		*bucketValue = (MurmurHash3_32_Long(microsecsFromMidnight) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == UUIDOID)
	{
		size_t		valueSize = 0;
		unsigned char *value = PGIcebergBinarySerializePartitionFieldValue(columnValue, transform->sourceField->type,
																		   transform->pgType, &valueSize);

		*bucketValue = (MurmurHash3_32_Bytes(value, valueSize) & INT32_MAX) % transform->bucketCount;
	}
	else if (transform->pgType.postgresTypeOid == NUMERICOID)
	{
		size_t		valueSize = 0;
		unsigned char *value = PGIcebergBinarySerializePartitionFieldValue(columnValue, transform->sourceField->type,
																		   transform->pgType, &valueSize);

		*bucketValue = (MurmurHash3_32_Bytes(value, valueSize) & INT32_MAX) % transform->bucketCount;
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot perform bucket transform on type %s", format_type_be(transform->pgType.postgresTypeOid))));
	}

	return bucketValue;
}


/*
 * GetTransformResultAvroType returns the result type of the transform.
 */
IcebergScalarAvroType
GetTransformResultAvroType(IcebergPartitionTransform * transform)
{
	IcebergScalarAvroType type = {0};

	switch (transform->resultPgType.postgresTypeOid)
	{
		case INT2OID:
		case INT4OID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT32;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
				break;
			}
		case INT8OID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT64;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
				break;
			}
		case FLOAT4OID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_FLOAT;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
				break;
			}
		case FLOAT8OID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_DOUBLE;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
				break;
			}
		case BOOLOID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_BOOL;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
				break;
			}
		case TEXTOID:
		case VARCHAROID:
		case BPCHAROID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_STRING;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
				break;
			}
		case BYTEAOID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_BINARY;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_NONE;
				break;
			}
		case DATEOID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT32;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_DATE;
				break;
			}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT64;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_TIMESTAMP;
				break;
			}
		case TIMEOID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_INT64;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_TIME;
				break;
			}
		case NUMERICOID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_BINARY;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_DECIMAL;

				GetDuckdbAdjustedPrecisionAndScaleFromNumericTypeMod(transform->pgType.postgresTypeMod,
																	 &type.precision, &type.scale);
				break;
			}
		case UUIDOID:
			{
				type.physical_type = ICEBERG_AVRO_PHYSICAL_TYPE_BINARY;
				type.logical_type = ICEBERG_AVRO_LOGICAL_TYPE_UUID;
				break;
			}
		default:
			{
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("unexpected type %s", format_type_be(transform->pgType.postgresTypeOid))));
			}
	}

	return type;
}


/*
 * SerializePartitionValueToPGText returns Postgres text representation of the partition field value.
 */
const char *
SerializePartitionValueToPGText(void *value, size_t valueLength, IcebergPartitionTransform * transform)
{
	if (value == NULL)
	{
		return NULL;
	}

	/* First, deserialize back */
	bool		isNull = false;
	Datum		partitionDatum =
		PartitionValueToDatum(transform->type, value, valueLength,
							  transform->resultPgType, &isNull);

	if (isNull)
		return NULL;

	Oid			resultPgType = transform->resultPgType.postgresTypeOid;

	Oid			typoutput;
	bool		typIsVarlena;

	getTypeOutputInfo(resultPgType, &typoutput, &typIsVarlena);
	return OidOutputFunctionCall(typoutput, partitionDatum);
}


/*
 * DeserializePartitionValueFromPGText deserializes the partition field value
 * from the given Postgres text representation.
 */
void *
DeserializePartitionValueFromPGText(IcebergPartitionTransform * transform,
									const char *valueText, size_t *valueLength)
{
	if (valueText == NULL)
	{
		*valueLength = 0;
		return NULL;
	}

	PGType		resultType = transform->resultPgType;

	Oid			typoinput;
	Oid			typioparam;

	getTypeInputInfo(resultType.postgresTypeOid, &typoinput, &typioparam);

	Datum		valueDatum = OidInputFunctionCall(typoinput, (char *) valueText, typioparam, resultType.postgresTypeMod);

	bool		isNull = (valueText == NULL);

	void	   *value = DatumToPartitionValue(transform, valueDatum, isNull, valueLength);

#ifdef USE_ASSERT_CHECKING
	const char *reserializedValue = SerializePartitionValueToPGText(value, *valueLength, transform);

	if (reserializedValue != NULL &&
		strcmp(reserializedValue, valueText) != 0)
	{
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("SerializePartitionValueToPGText failed to serialize value: \"%s\" != \"%s\"",
						reserializedValue, valueText)));
	}
#endif

	return value;
}

/*
* PartitionValueToDatum converts the partition value to a datum by deserializing
* the value.
*/
Datum
PartitionValueToDatum(IcebergPartitionTransformType transformType, void *value, size_t valueLength,
					  PGType pgType, bool *isNull)
{
	if (value == NULL)
	{
		*isNull = true;
		return (Datum) 0;
	}
	else if (transformType == PARTITION_TRANSFORM_IDENTITY ||
			 transformType == PARTITION_TRANSFORM_TRUNCATE)
	{
		*isNull = false;

		Field	   *field = PostgresTypeToIcebergField(pgType, false, NULL);

		Assert(field->type == FIELD_TYPE_SCALAR);

		return PGIcebergBinaryDeserialize(value, valueLength,
										  field, pgType);
	}
	else if (transformType == PARTITION_TRANSFORM_YEAR ||
			 transformType == PARTITION_TRANSFORM_MONTH ||
			 transformType == PARTITION_TRANSFORM_DAY ||
			 transformType == PARTITION_TRANSFORM_HOUR ||
			 transformType == PARTITION_TRANSFORM_BUCKET)
	{
		*isNull = false;

		return Int32GetDatum(*(int32_t *) value);
	}
	else
	{
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("unsupported partition transform type %d", transformType)));
	}
}


/*
 * DatumToPartitionValue converts the column value to a partition value.
 */
static void *
DatumToPartitionValue(IcebergPartitionTransform * transform, Datum columnValue, bool isNull,
					  size_t *valueLength)
{
	if (isNull)
	{
		*valueLength = 0;
		return NULL;
	}

	PGType		resultType = transform->resultPgType;

	Field	   *resultField = PostgresTypeToIcebergField(resultType, false, NULL);

	return PGIcebergBinarySerializePartitionFieldValue(columnValue, resultField, resultType, valueLength);
}


void
ErrorIfColumnEverUsedInIcebergPartitionSpec(Oid relationId, AttrNumber attrNumber, char *operation)
{
	List	   *partitionTransforms = AllPartitionTransformList(relationId);

	ListCell   *lc = NULL;

	foreach(lc, partitionTransforms)
	{
		IcebergPartitionTransform *partitionTransform = lfirst(lc);

		if (partitionTransform->attnum == attrNumber)
			ereport(ERROR,
					(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
					 errmsg("cannot %s from table %s because it is used in the partition spec",
							operation, get_rel_name(relationId))));
	}
}
