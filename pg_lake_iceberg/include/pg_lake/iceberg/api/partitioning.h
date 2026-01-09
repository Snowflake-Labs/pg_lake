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

#include "pg_lake/iceberg/metadata_spec.h"
#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/type.h"
#include "access/attnum.h"

/* this is a bit arbitrary, we follow Spark's convention */
#define ICEBERG_PARTITION_FIELD_ID_START 999

typedef enum IcebergPartitionTransformType
{
	PARTITION_TRANSFORM_IDENTITY,
	PARTITION_TRANSFORM_YEAR,
	PARTITION_TRANSFORM_MONTH,
	PARTITION_TRANSFORM_DAY,
	PARTITION_TRANSFORM_HOUR,
	PARTITION_TRANSFORM_BUCKET,
	PARTITION_TRANSFORM_TRUNCATE,
	PARTITION_TRANSFORM_VOID
}			IcebergPartitionTransformType;

typedef struct IcebergPartitionTransform
{
	IcebergPartitionTransformType type;

	union
	{
		/* valid for bucket transform */
		size_t		bucketCount;

		/* valid for truncate transform */
		size_t		truncateLen;
	};

	IcebergPartitionSpecField *specField;

	/* source field of the column to which transform applies */
	DataFileSchemaField *sourceField;

	/* Postgres column info to which transform applies */
	const char *columnName;
	AttrNumber	attnum;
	PGType		pgType;

	/* transform result's postgres type */
	PGType		resultPgType;
}			IcebergPartitionTransform;
