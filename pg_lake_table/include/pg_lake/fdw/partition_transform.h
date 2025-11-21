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

#pragma once

#include "postgres.h"
#include "utils/tuplestore.h"

#include "pg_lake/iceberg/api.h"
#include "pg_lake/iceberg/api/partitioning.h"

extern Partition * ComputePartitionTupleForTuple(List *transforms, TupleTableSlot *slot);
extern void *ApplyBucketTransformToColumn(IcebergPartitionTransform * transform,
										  Datum columnValue, bool isNull,
										  size_t *bucketSize);
extern List *CurrentPartitionTransformList(Oid relationId);
extern IcebergPartitionSpec * GetPartitionSpecIfAlreadyExist(Oid relationId, List *partitionTransforms);
extern List *AllPartitionTransformList(Oid relationId);
extern List *GetPartitionTransformsFromSpecFields(Oid relationId, List *specFields);
extern void *DeserializePartitionValueFromPGText(IcebergPartitionTransform * transform,
												 const char *valueText, size_t *valueLength);
extern const char *SerializePartitionValueToPGText(void *value, size_t valueLength, IcebergPartitionTransform * transform);
extern Datum PartitionValueToDatum(IcebergPartitionTransformType transformType, void *value, size_t valueLength,
								   PGType pgType, bool *isNull);
extern void ErrorIfColumnEverUsedInIcebergPartitionSpec(Oid relationId, AttrNumber attrNumber, char *operation);
extern IcebergScalarAvroType GetTransformResultAvroType(IcebergPartitionTransform * transform);
