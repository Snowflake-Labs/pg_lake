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

/*
 * Hook invoked at the end of pg_lake_finish_postgres_recovery, after the
 * per-database lake_table recovery has run and committed. Other extensions
 * (such as pg_lake_replication) can set this to perform their own recovery
 * steps.
 */
typedef void (*PgLakeFinishPostgresRecoveryHookType) (void);
extern PGDLLEXPORT PgLakeFinishPostgresRecoveryHookType PgLakeFinishPostgresRecoveryHook;
