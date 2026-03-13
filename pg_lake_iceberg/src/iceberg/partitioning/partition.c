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
#include "common/hashfn.h"
#include "utils/builtins.h"
#include "utils/jsonb.h"
#include "utils/lsyscache.h"

#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/iceberg/iceberg_type_binary_serde.h"
#include "pg_lake/iceberg/iceberg_type_numeric_binary_serde.h"
#include "pg_lake/iceberg/partitioning/partition.h"
#include "pg_lake/iceberg/temporal_utils.h"

static int	PartitionFieldCompare(const void *a, const void *b);

/*
* CopyPartition creates a copy of the given partition.
*/
Partition *
CopyPartition(Partition * partition)
{
	Partition  *copy = palloc0(sizeof(Partition));

	copy->fields_length = partition->fields_length;
	copy->fields = palloc0(sizeof(PartitionField) * copy->fields_length);

	for (size_t i = 0; i < copy->fields_length; i++)
	{
		PartitionField *copyField = &copy->fields[i];
		PartitionField *sourceField = &partition->fields[i];

		copyField->field_id = sourceField->field_id;
		copyField->field_name = pstrdup(sourceField->field_name);
		copyField->value_type = sourceField->value_type;
		if (sourceField->value_length > 0)
		{
			copyField->value = palloc0(sourceField->value_length);

			memcpy(copyField->value, sourceField->value, sourceField->value_length);
		}
		else
		{
			copyField->value = NULL;
		}
		copyField->value_length = sourceField->value_length;
	}

	return copy;
}


/*
* AppendPartitionField appends a partition field to the given partition.
*/
void
AppendPartitionField(Partition * partition, PartitionField * partitionField)
{
	/*
	 * if first element, do palloc, otherwise repalloc
	 */
	if (partition->fields_length == 0)
	{
		partition->fields = palloc0(sizeof(PartitionField));
	}
	else
	{
		partition->fields =
			repalloc0(partition->fields,
					  sizeof(PartitionField) * (partition->fields_length),
					  sizeof(PartitionField) * (partition->fields_length + 1));
	}

	PartitionField *newField = &partition->fields[partition->fields_length];

	newField->field_id = partitionField->field_id;
	newField->field_name = pstrdup(partitionField->field_name);
	newField->value_type = partitionField->value_type;
	newField->value_length = partitionField->value_length;

	if (newField->value_length == 0)
	{
		newField->value = NULL; /* no value */
	}
	else
	{
		newField->value = palloc0(partitionField->value_length);
		memcpy(newField->value, partitionField->value, partitionField->value_length);
	}

	partition->fields_length++;

	/*
	 * Now, sort based on field_id, we should always have a consistent order.
	 * This is important for the getting consistent hash value for the
	 * partition. Note that this sorting can be done in the catalog query, but
	 * the catalog query is quite complex, adding ORDER BY might complicate it
	 * further. Plus, we have multiple callers of this function, so we do it
	 * here.
	 */
	qsort(partition->fields, partition->fields_length,
		  sizeof(PartitionField), PartitionFieldCompare);
}

/*
* PartitionFieldCompare compares two partition fields based on their field_id.
*/
static int
PartitionFieldCompare(const void *a, const void *b)
{
	const		PartitionField *fieldA = (const PartitionField *) a;
	const		PartitionField *fieldB = (const PartitionField *) b;

	if (fieldA->field_id < fieldB->field_id)
		return -1;
	else if (fieldA->field_id > fieldB->field_id)
		return 1;
	else
		return 0;
}


/*
 * ComputePartitionKey compute a hash value for a partition. This is used to determine
 * the partition to which a tuple belongs.
 * There is a small chance of collision, but which we expect to be negligible
 * due to 64 bits.
 */
uint64
ComputePartitionKey(const Partition * partition)
{
	/* seed – 0 is fine; we could pick any value */
	uint64		hashValue = 0;

	for (size_t partitionIndex = 0; partitionIndex < partition->fields_length; partitionIndex++)
	{
		const		PartitionField *partitionField = &partition->fields[partitionIndex];

		/*
		 * 1. Mix in field_id Each partition can hold many fields.  Two
		 * different fields can carry the same literal value (for example "US"
		 * in both a country column and a currency column).  If we hashed only
		 * the value bytes, those two fields would collide and look identical
		 * in the hash table.  Adding the numeric field_id makes the hash
		 * record *which* column the value came from, so field_id = 1, value =
		 * "US" field_id = 7, value = "US" land in different buckets.
		 */
		hashValue = hash_combine64(hashValue,
								   (uint64) hash_uint32((uint32) partitionField->field_id));

		/*
		 * 2. Mix in value_type The same logical value can be stored in more
		 * than one physical form: the text "123", an int32 123, or maybe a
		 * bigint 123. Tomorrow someone might even pack it with a new
		 * encoding. Hashing the value_type tag makes the representation
		 * explicit, so 123 as INT and "123" as TEXT never collide, and the
		 * hash stays stable even if byte-level encodings change in the
		 * future.
		 */
		hashValue = hash_combine64(hashValue,
								   (uint64) hash_uint32((uint32) partitionField->value_type.physical_type));

		/*
		 * 3. Mix in the raw value bytes After the id and type have marked
		 * *where* the data lives and *how* it is stored, we still need the
		 * actual contents to tell one row from another.  Hashing the byte
		 * stream pointed to by value, together with its length, captures that
		 * content exactly, with no endianness or alignment surprises, and
		 * completes the unique fingerprint for this field.
		 */
		uint64		valueHash =
			hash_bytes_extended((const unsigned char *) partitionField->value,
								partitionField->value_length,
								0); /* same seed as outer hash   */

		hashValue = hash_combine64(hashValue, valueHash);
	}

	return hashValue;
}


/*
 * ComputeSpecPartitionKey computes a hash value given a partition spec and partition tuple.
 * This can be used to determine compact data files of the same the partition spec and tuple.
 *
 * There is a small chance of collision, but which we expect to be negligible
 * due to 64 bits.
 */
uint64
ComputeSpecPartitionKey(int32_t partitionSpecId, const Partition * partition)
{
	uint64		specHash = hash_uint32((uint32) (partitionSpecId));

	uint64		partitionHash = ComputePartitionKey(partition);

	return hash_combine64(specHash, partitionHash);
}


/*
 * FindPartitionTransformById finds a partition transform by partition field id.
 */
IcebergPartitionTransform *
FindPartitionTransformById(List *transforms, int32_t partitionFieldId, bool errorIfMissing)
{
	ListCell   *cell = NULL;

	foreach(cell, transforms)
	{
		IcebergPartitionTransform *transform = (IcebergPartitionTransform *) lfirst(cell);

		if (transform->specField->field_id == partitionFieldId)
			return transform;
	}

	if (errorIfMissing)
		ereport(ERROR,
				(errcode(ERRCODE_INTERNAL_ERROR),
				 errmsg("could not find partition transform for field id %d",
						partitionFieldId)));

	return NULL;
}
