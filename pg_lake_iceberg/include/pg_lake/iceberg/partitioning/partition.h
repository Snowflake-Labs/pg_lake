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

#include "pg_lake/iceberg/manifest_spec.h"
#include "pg_lake/iceberg/api/partitioning.h"

extern PGDLLEXPORT Partition * CopyPartition(Partition * partition);
extern PGDLLEXPORT void AppendPartitionField(Partition * partition, PartitionField * partitionField);
extern PGDLLEXPORT uint64 ComputePartitionKey(const Partition * partition);
extern PGDLLEXPORT uint64 ComputeSpecPartitionKey(int32_t partitionSpecId, const Partition * partition);
extern PGDLLEXPORT IcebergPartitionTransform * FindPartitionTransformById(List *transforms, int32_t partitionFieldId, bool errorIfMissing);
