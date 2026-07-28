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
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"

#include "pg_lake/pgduck/type.h"

/*
 * DDLOperationType describes a type of ddl operation, which
 * cause catalog modification and possibly iceberg metadata modification.
 */
typedef enum DDLOperationType
{
	DDL_TABLE_CREATE = 1,
	DDL_TABLE_DROP = 2,
	DDL_TABLE_RENAME = 3,
	DDL_COLUMN_ADD = 4,
	DDL_COLUMN_DROP = 5,
	DDL_COLUMN_SET_DEFAULT = 6,
	DDL_COLUMN_DROP_DEFAULT = 7,
	DDL_COLUMN_DROP_NOT_NULL = 8,
	DDL_TABLE_SET_PARTITION_BY = 9,
	DDL_TABLE_DROP_PARTITION_BY = 10,
	DDL_COLUMN_ALTER_TYPE = 11,
}			DDLOperationType;

/*
 * IcebergDDLOperation represents a ddl operation on table metadata.
 */
typedef struct IcebergDDLOperation
{
	DDLOperationType type;

	/* applicable for create table or add column */
	List	   *columnDefs;

	/* applicable for alter column ddls except create table and add column */
	AttrNumber	attrNumber;

	/* applicable for set/drop default */
	const char *writeDefault;

	/* applicable for alter column type */
	PGType		newPgType;

	/* applicable for create iceberg table */
	bool		hasCustomLocation;

	/* applicable for changing partition_by */
	List	   *parsedTransforms;
}			IcebergDDLOperation;

extern void ApplyDDLChanges(Oid relationId, List *ddlOperations);
