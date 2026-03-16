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

#include "pg_lake/pgduck/iceberg_validation.h"
#include "tcop/dest.h"

/*
 * DestReceiver wrapper that applies Iceberg out-of-range validation to
 * every tuple before forwarding it to a child receiver.
 *
 * Temporal columns (date/timestamp/timestamptz) are clamped or rejected
 * per the given policy, and bounded-numeric NaN values are handled
 * similarly.  The slot is modified in-place so that downstream consumers
 * (partition transforms, CSV serialization) see already-validated data.
 */
extern PGDLLEXPORT DestReceiver *CreateIcebergValidatingDestReceiver(DestReceiver *child,
																	 IcebergOutOfRangePolicy policy);

/*
 * Returns the child DestReceiver wrapped by an IcebergValidatingDestReceiver.
 * Use this to reach the inner receiver for operations like
 * GetPartitionedDestReceiverModifications / GetMultiDataFileDestReceiverModifications.
 */
extern PGDLLEXPORT DestReceiver *GetIcebergValidatingDestReceiverChild(DestReceiver *dest);
