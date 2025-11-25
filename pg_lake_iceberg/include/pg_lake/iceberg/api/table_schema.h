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

#include "nodes/pg_list.h"

#include "pg_lake/iceberg/metadata_spec.h"


/* read api */
extern PGDLLEXPORT IcebergTableSchema * GetIcebergTableSchemaByIdFromTableMetadata(IcebergTableMetadata * metadata, int schemaId);
extern PGDLLEXPORT IcebergTableSchema * GetCurrentIcebergTableSchema(IcebergTableMetadata * metadata);
extern PGDLLEXPORT List *GetLeafFieldsFromIcebergMetadata(IcebergTableMetadata * metadata);
extern PGDLLEXPORT List *GetLeafFieldsForIcebergSchema(IcebergTableSchema * schema);
extern PGDLLEXPORT DataFileSchemaField * GetDataFileSchemaFieldById(DataFileSchema * schema, int fieldId);

/* write api */
extern PGDLLEXPORT IcebergTableSchema * RebuildIcebergSchemaFromDataFileSchema(Oid foreignTableOid,
																			   DataFileSchema * schema,
																			   int *last_column_id);
