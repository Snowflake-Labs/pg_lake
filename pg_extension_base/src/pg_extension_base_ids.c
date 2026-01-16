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

/*
 * Cached extension IDs for pg_extension_base.
 */
#include "postgres.h"

#include "pg_extension_base/pg_extension_base_ids.h"


/*
 * Cached extension state for pg_extension_base.
 */
CachedExtensionIds *PgExtensionBase;


/*
 * Set up extension ID caching for pg_extension_base.
 */
void
InitializePgExtensionBaseCache(void)
{
	PgExtensionBase = CreateExtensionIdsCache(PG_EXTENSION_BASE_NAME, NULL, NULL);
}
