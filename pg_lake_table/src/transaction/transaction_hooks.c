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
#include "access/xact.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "pg_lake/transaction/track_iceberg_metadata_changes.h"
#include "pg_lake/transaction/transaction_hooks.h"


static void IcebergXactCallback(XactEvent event, void *arg);


void
IcebergRegisterCallbacks(void)
{
	RegisterXactCallback(IcebergXactCallback, NULL);
}


static void
IcebergXactCallback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_PRE_COMMIT:
		case XACT_EVENT_PARALLEL_PRE_COMMIT:

			{
				/*
				 * Postgres does not expect a snapshot to be left active at
				 * this point in the transaction. We temporarily push one, and
				 * remove in the non-error cases. If there are errors,
				 * Postgres handles Pop'ing the snapshot later on, so we don't
				 * need to worry about.
				 */
				Assert(!ActiveSnapshotSet());
				PushActiveSnapshot(GetTransactionSnapshot());

				ConsumeTrackedIcebergMetadataChanges();

				PopActiveSnapshot();

				break;
			}

		case XACT_EVENT_PARALLEL_ABORT:
		case XACT_EVENT_ABORT:
			{
				ResetTrackedIcebergMetadataOperation();
				ResetRestCatalogRequests();
				break;
			}
		case XACT_EVENT_PRE_PREPARE:
		case XACT_EVENT_PREPARE:
			{
				if (HasAnyTrackedIcebergMetadataChanges())
					ereport(ERROR,
							(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							 errmsg("cannot prepare a transaction that has Iceberg metadata changes")));
				break;
			}

		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PARALLEL_COMMIT:
			{
				PostAllRestCatalogRequests();
				ResetRestCatalogRequests();
				break;
			}
	}
}
