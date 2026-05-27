/*
 * Copyright 2025 Snowflake Inc.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */

#pragma once

#include "pg_extension_base/extension_ids.h"

#define PG_LAKE_DUCKLAKE "pg_lake_ducklake"
#define PG_LAKE_DUCKLAKE_INTERNAL_SCHEMA "lake_ducklake"

/* cached extension IDs for pg_lake_ducklake */
extern PGDLLEXPORT CachedExtensionIds * PgLakeDucklake;

extern PGDLLEXPORT void InitializePgLakeDucklakeIdCache(void);
