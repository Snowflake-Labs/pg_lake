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

/*-------------------------------------------------------------------------
 *
 * pg_extension_base_test_oneshot -- test extension for one-shot base workers
 *
 * Provides a background worker that calls CompleteBaseWorker() inside its
 * transaction so that it runs exactly once and is never restarted.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"

#include "access/xact.h"
#include "commands/async.h"
#include "pg_extension_base/base_workers.h"
#include "pg_extension_base/spi_helpers.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pg_extension_base_test_oneshot_main_worker);


/*
 * pg_extension_base_test_oneshot_main_worker is a one-shot background worker.
 *
 * It opens a transaction, calls CompleteBaseWorker() to remove itself from the
 * catalog, and commits.  If the transaction commits the worker will never be
 * restarted.  If it aborts (e.g. server crash before commit) the catalog row
 * is preserved and the worker retries as normal.
 */
Datum
pg_extension_base_test_oneshot_main_worker(PG_FUNCTION_ARGS)
{
	START_TRANSACTION();

	DeregisterBaseWorkerSelf();

	Async_Notify("oneshot", psprintf("%d", MyBaseWorkerId));

	END_TRANSACTION();

	PG_RETURN_VOID();
}
