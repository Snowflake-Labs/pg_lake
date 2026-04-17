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

#include "pg_lake/iceberg/api/table_schema.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/parquet/field.h"

/*
 * A top-level structure that represents a mapping between
 * a column in a Postgres table and its corresponding field.
 */
typedef struct PostgresColumnMapping
{
	Oid			relationId;
	AttrNumber	attrNum;
	char	   *attname;
	bool		attNotNull;
	bool		attHasDef;
	PGType		pgType;

	/* field that is mapped to postgres column */
	DataFileSchemaField *field;
} PostgresColumnMapping;

extern PGDLLEXPORT void RegisterPostgresColumnMappings(List *pgColumnMappingList);
extern PGDLLEXPORT List *CreatePostgresColumnMappingsForColumnDefs(Oid relationId, List *columnDefList, bool forAddColumn);
extern PGDLLEXPORT List *CreatePostgresColumnMappingsForIcebergTableFromExternalMetadata(Oid relationId);
extern PGDLLEXPORT DataFileSchema *GetDataFileSchemaForTable(Oid relationId);
extern PGDLLEXPORT DataFileSchema *GetDataFileSchemaForTableWithExclusion(Oid relationId, List *excludedColumns);
extern PGDLLEXPORT DataFileSchema *GetDataFileSchemaForExternalIcebergTable(char *metadataPath);
extern PGDLLEXPORT List *GetLeafFieldsForExternalIcebergTable(char *metadataPath);
extern PGDLLEXPORT List *GetLeafFieldsForTable(Oid relationId);
extern PGDLLEXPORT const char *GetDuckSerializedIcebergFieldInitialDefault(const char *initialDefault,
																		   Field *field);
