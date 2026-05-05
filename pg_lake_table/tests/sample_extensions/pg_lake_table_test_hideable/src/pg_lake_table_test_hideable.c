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
 * Test extension that registers itself as hideable via
 * RegisterHideableExtension().  Used by test_hide_registered_extension.py
 * to verify that externally-registered extensions have their objects
 * filtered from catalog queries when hide_objects_created_by_lake is on.
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "nodes/nodes.h"
#include "pg_extension_base/extension_ids.h"
#include "pg_lake/test/hide_lake_objects.h"

PG_MODULE_MAGIC;

static CachedExtensionIds * TestHideable;

void		_PG_init(void);

/*
 * Must be loaded via shared_preload_libraries so that
 * RegisterHideableExtension() runs before any queries are planned.
 */
void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		ereport(ERROR,
				(errmsg("pg_lake_table_test_hideable can only be loaded via shared_preload_libraries"),
				 errhint("Add pg_extension_base to shared_preload_libraries.")));
	}

	TestHideable = CreateExtensionIdsCache("pg_lake_table_test_hideable",
										   NULL, NULL);
	RegisterHideableExtension(TestHideable);
}
