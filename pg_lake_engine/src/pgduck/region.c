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

#include "postgres.h"

#include "pg_lake/pgduck/keywords.h"
#include "pg_lake/pgduck/client.h"
#include "pg_lake/pgduck/region.h"
#include "utils/builtins.h"


/*
 * GetBucketRegion tries to retrieve the bucket for a given path.
 */
char *
GetBucketRegion(char *path)
{
	char	   *query = psprintf("SELECT pg_lake_get_bucket_region(%s)",
								 duckdb_quote_literal(path));

	char	   *bucketRegion = GetSingleValueFromPGDuck(query);

	return bucketRegion;
}


/*
 * GetManagedStorageRegion tries to retrieve the managed storage region.
 */
char *
GetManagedStorageRegion(void)
{
	char	   *query = "SELECT pg_lake_get_managed_storage_region()";

	char	   *storageRegion = GetSingleValueFromPGDuck(query);

	return storageRegion;
}
