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

#include "pg_lake/iceberg/partitioning/spec_generation.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/metadata_spec.h"


/*
* BuildPartitionSpecFromPartitionTransforms builds an IcebergPartitionSpec
* from the given partition transforms. It also reads the current metadata
* and finds the last partition id and spec id to use in this new spec.
*/
IcebergPartitionSpec *
BuildPartitionSpecFromPartitionTransforms(Oid relationId, List *partitionTransforms, int largestSpecId)
{
	IcebergPartitionSpec *spec = palloc0(sizeof(IcebergPartitionSpec));

	if (partitionTransforms == NIL)
	{
		/*
		 * We follow Spark's behavior here. If user drops the partitioning
		 * transforms, we set the spec id to 0, which is equivalent to
		 * non-partitioned table. This is useful to treat all non-partitioned
		 * data/deletion files to be treated as non-partitioned in the same
		 * table.
		 */
		spec->spec_id = DEFAULT_SPEC_ID;
		return spec;
	}

	/* spec-id starts from 0, so use 0 as the first spec-id */
	spec->spec_id = largestSpecId + 1;

	int			transformCount = list_length(partitionTransforms);

	spec->fields = transformCount > 0 ? palloc0(sizeof(IcebergPartitionSpecField) * transformCount) : NULL;
	spec->fields_length = transformCount;

	ListCell   *transformCell = NULL;
	size_t		fieldIndex = 0;

	foreach(transformCell, partitionTransforms)
	{
		IcebergPartitionTransform *transform = lfirst(transformCell);

		spec->fields[fieldIndex] = transform->specField;
		fieldIndex++;
	}

	return spec;
}
