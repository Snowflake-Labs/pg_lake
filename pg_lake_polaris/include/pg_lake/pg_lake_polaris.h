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

#ifndef PG_LAKE_POLARIS_H
#define PG_LAKE_POLARIS_H

#include "postgres.h"

/*
 * Process-global flag that suppresses both the outbound and inbound triggers
 * while one of them is running, to break the round-trip loop between
 * lake_iceberg.tables_internal and polaris_schema.entities. Set in PG_TRY,
 * restored in PG_FINALLY.
 *
 * Mirrors the SkipIcebergDDLProcessing pattern in pg_lake_table.
 */
extern bool PgLakePolarisSuppress;

#endif							/* PG_LAKE_POLARIS_H */
