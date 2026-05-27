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

#include "access/attnum.h"
#include "nodes/pg_list.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/type.h"

#define MAPPING_TABLE_NAME "lake_table.field_id_mappings"

#define INVALID_FIELD_ID 0

extern PGDLLEXPORT DataFileSchema * GetDataFileSchemaForInternalIcebergTable(Oid relationId);
extern PGDLLEXPORT List *GetLeafFieldsForInternalIcebergTable(Oid relationId);
extern PGDLLEXPORT List *GetRegisteredFieldForAttributes(Oid relationId, List *attrNos);
extern PGDLLEXPORT DataFileSchemaField * GetRegisteredFieldForAttribute(Oid relationId, AttrNumber attrNo);
extern PGDLLEXPORT AttrNumber GetAttributeForFieldId(Oid relationId, int fieldId);
extern PGDLLEXPORT void UpdateRegisteredFieldWriteDefaultForAttribute(Oid relationId, AttrNumber attNum, const char *writeDefault);
extern PGDLLEXPORT int GetLargestRegisteredFieldId(Oid relationId);
extern PGDLLEXPORT void RegisterIcebergColumnMapping(Oid relationId, Field * field,
													 AttrNumber attNo, int parentFieldId, PGType pgType,
													 int fieldId, const char *writeDefault, const char *initialDefault);
extern PGDLLEXPORT List *GetLeafFieldsForDucklakeTable(Oid relationId);
