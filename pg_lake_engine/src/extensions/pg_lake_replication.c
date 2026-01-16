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
 * Utility functions for pg_lake_replication extension OIDs.
 */
#include "postgres.h"

#include "pg_lake/extensions/pg_lake_replication.h"
#include "pg_extension_base/extension_ids.h"


/*
 * Generic extension state.
 */
CachedExtensionIds *PgLakeReplication;

/*
 * Set up extension ID caching for pg_lake_replication.
 */
void
InitializePgLakeReplicationIdCache(void)
{
	PgLakeReplication = CreateExtensionIdsCache(PG_LAKE_REPLICATION, NULL, NULL);
}
