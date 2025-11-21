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

#include "pg_lake/extensions/pg_lake_table.h"
#include "pg_lake/iceberg/api/partitioning.h"
#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/iceberg/manifest_spec.h"

#define DATA_FILE_PARTITION_VALUES_TABLE_QUALIFIED \
	PG_LAKE_TABLE_SCHEMA "." PG_LAKE_TABLE_DATA_FILE_PARTITION_VALUES_TABLE_NAME

#define PARTITION_FIELDS_TABLE_QUALIFIED \
    PG_LAKE_TABLE_SCHEMA "." PG_LAKE_TABLE_PARTITION_FIELDS

#define PARTITION_SPECS_TABLE_QUALIFIED \
	PG_LAKE_TABLE_SCHEMA "." PG_LAKE_TABLE_PARTITION_SPECS

typedef struct IcebergPartitionSpecHashEntry
{
	int32		specId;			/* hash key: spec id */
	IcebergPartitionSpec *spec; /* the partition spec */
}			IcebergPartitionSpecHashEntry;


extern void UpdateDefaultPartitionSpecId(Oid relationId, int specId);
extern void InsertPartitionSpecAndPartitionFields(Oid relationId, IcebergPartitionSpec * spec);
extern int	GetLargestSpecId(Oid relationId);
extern List *GetAllIcebergPartitionSpecIds(Oid relationId);
extern PGDLLEXPORT int GetCurrentSpecId(Oid relationId);
extern int	GetLargestPartitionFieldId(Oid relationId);
extern IcebergPartitionSpecField * GetIcebergPartitionFieldFromCatalog(Oid relationId, int fieldId);
extern List *GetIcebergSpecPartitionFieldsFromCatalog(Oid relationId, int specId);
extern List *GetAllIcebergSpecPartitionFieldsFromCatalog(Oid relationId);
extern Partition * GetDataFilePartition(Oid relationId, List *partitionTransforms,
										const char *path, int32 *partitionSpecId);
extern HTAB *GetAllPartitionSpecsFromCatalog(Oid relationId);
extern HTAB *CreatePartitionSpecHash(void);
