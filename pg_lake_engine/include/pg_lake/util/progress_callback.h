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

/*
 * Optional progress callback for long-running operations.
 *
 * Extensions can register a function that is invoked periodically during
 * expensive loops (e.g. bulk Iceberg metadata changes at pre-commit time).
 * This allows the caller to perform housekeeping -- such as sending
 * keepalive messages on an external connection -- without pg_lake needing
 * to know the details.
 *
 * Set to NULL when no callback is needed.
 */
typedef void (*PgLakeTransactionProgressCallbackFn) (void);
extern PGDLLEXPORT PgLakeTransactionProgressCallbackFn PgLakeTransactionProgressCallback;
