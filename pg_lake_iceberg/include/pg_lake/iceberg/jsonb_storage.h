/*
 * Copyright 2026 Snowflake Inc.
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

#include "pg_lake/extensions/pg_lake_engine.h"

/*
 * Name of the per-table iceberg option that picks the storage encoding for
 * the table's jsonb columns.  Defined here so option validation
 * (pg_lake_table) and the column registration path share one spelling.
 *
 * The JsonbStorage enum itself lives in pg_lake_engine, because the same
 * choice is made by COPY TO a Parquet file, which never reaches this module.
 */
#define ICEBERG_JSONB_STORAGE_OPTION "jsonb_storage"

/*
 * Reads the option from an existing relation.  An absent option, or a
 * non-iceberg relation, means JSONB_STORAGE_STRING: the option is seeded at
 * CREATE whenever the GUC asks for anything else, so a table that does not
 * carry it was created wanting strings.
 *
 * Deliberately does not fall back to pg_lake_engine.jsonb_storage. The GUC
 * decides what a *new table* adopts; once the table exists, the columns it
 * grows follow the table, not whoever happens to run the ALTER.
 */
extern PGDLLEXPORT JsonbStorage IcebergJsonbStorageFromRelation(Oid relationId);
