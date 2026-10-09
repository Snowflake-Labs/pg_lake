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

#include "pg_extension_base/extension_ids.h"

#define PG_JOB_SCHEDULER_NAME "pg_job_scheduler"
#define JOB_SCHEDULER_SCHEMA "job_scheduler"

/* maximum number of concurrent job workers */
extern int	JobSchedulerMaxWorkers;

/* how long a finished run is kept, in seconds; negative keeps them forever */
extern int	JobSchedulerRunRetentionSec;

/* delay before retrying a failed one-shot job, doubling up to the maximum */
extern int	JobSchedulerRetryBackoffInitialMs;
extern int	JobSchedulerRetryBackoffMaxMs;

/*
 * Cached IDs for our own extension. The bookkeeping writes run as the
 * extension owner, because the command itself runs as the job's user_name,
 * who has no privileges on our tables.
 */
extern CachedExtensionIds * PgJobScheduler;

extern void InitializeJobSchedulerIdCache(void);
