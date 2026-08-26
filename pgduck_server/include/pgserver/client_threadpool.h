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
 * Definitions for the client thread pool
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef PGDUCK_CLIENT_THREAD_H
#define PGDUCK_CLIENT_THREAD_H

#include <pthread.h>
#include "pgsession/pgsession.h"

#define InvalidThreadIndex -1

extern int	MaxThreads;
extern int	MaxAllowedClients;

extern void pgclient_threadpool_init(int maxAllowedClients);
extern int	pgclient_threadpool_reserve_slot(PGClient * client);
extern void pgclient_threadpool_free_slot(int threadIndex);
#if PG_VERSION_NUM >= 180000
extern int	pgclient_threadpool_cancel_thread(int cancellationProcId, uint8 *cancellationToken,
											  size_t cancellationTokenSize);
#else
extern int	pgclient_threadpool_cancel_thread(int cancellationProcId, int32 cancellationToken);
#endif
extern void pgclient_threadpool_set_duckdb_conn(int threadIndex, duckdb_connection conn);
extern int	pgclient_threadpool_cancel_all(void);

#endif							/* PGDUCK_CLIENT_THREAD_H */
