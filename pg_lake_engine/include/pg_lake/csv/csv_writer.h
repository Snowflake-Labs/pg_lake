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

#ifndef CSV_WRITER_H
#define CSV_WRITER_H

#include "libpq-fe.h"

#include "pg_lake/copy/copy_format.h"
#include "tcop/dest.h"
#include "nodes/pg_list.h"

extern PGDLLEXPORT DestReceiver *CreateCSVDestReceiver(char *filename, List *copyOptions,
													   CopyDataFormat targetFormat);
extern PGDLLEXPORT DestReceiver *CreateCSVDestReceiverExtended(char *filename,
															   List *copyOptions,
															   CopyDataFormat targetFormat,
															   bool sessionLifetime);

/*
 * Streaming variant: bytes go to a libpq COPY-IN already opened on
 * `streamConn` (use OpenCopyInStreamToPGDuck() to get there). The caller
 * is responsible for finalizing the stream via FinishCopyInStreamToPGDuck()
 * AFTER calling rShutdown on the returned DestReceiver — rShutdown only
 * flushes the per-row buffer and emits any binary trailer; it deliberately
 * does NOT call PQputCopyEnd, so callers can still emit additional
 * CopyData (e.g. for multi-segment writes) before closing. The deferred
 * query's PGresult is returned by FinishCopyInStreamToPGDuck so callers
 * can extract row counts / column statistics.
 */
extern PGDLLEXPORT DestReceiver *CreateCSVStreamDestReceiver(PGconn *streamConn,
															 List *copyOptions,
															 CopyDataFormat targetFormat);

extern PGDLLEXPORT int GetCSVDestReceiverMaxLineSize(DestReceiver *dest);
extern PGDLLEXPORT uint64 GetCSVDestReceiverFileSize(DestReceiver *dest);

#endif
