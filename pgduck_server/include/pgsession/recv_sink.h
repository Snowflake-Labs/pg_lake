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

/*
 * Receive sink: a per-RECEIVE-query landing area on pgduck_server's local
 * filesystem.
 *
 * The RECEIVE query prefix (mirror of TRANSMIT) lets pg_lake clients stream
 * bytes via libpq COPY IN, instead of writing to a shared filesystem under
 * the Postgres backend's PGDATA. The bytes land in a sink owned by
 * pgduck_server, and the sink path is what gets passed to DuckDB's
 * read_csv() etc. This decouples pgduck_server's filesystem from the
 * client's PGDATA, which is a hard requirement when pgduck_server runs in
 * a different pod/host than the Postgres backend (e.g. on Kubernetes).
 *
 * v1 stages received bytes to a regular file in a private base directory.
 * Future versions may use a FIFO so DuckDB can consume the stream in
 * parallel with the client upload, but the file approach is simpler and
 * sufficient for typical CDC batch sizes (tens of MB).
 */
#ifndef PGDUCK_RECV_SINK_H
#define PGDUCK_RECV_SINK_H

#include "c.h"
#include <stddef.h>

typedef struct ReceiveSink ReceiveSink;

/*
 * Initialize the global receive directory. Creates it (mode 0700) if
 * missing and unlinks any stale files left over from a prior run. Must be
 * called once at startup, before any sink is created. Repeated calls
 * replace the configured path.
 *
 * Returns 0 on success, -1 on error (with errno set and the failure
 * already logged).
 */
int			recv_sink_global_init(const char *base_dir);

/*
 * Returns the configured base directory, or NULL if recv_sink has not been
 * initialized.
 */
const char *recv_sink_global_dir(void);

/*
 * Create a new sink. Allocates a unique path under the global recv
 * directory, opens it for writing (mode 0600), and returns the sink. The
 * caller owns the sink and must call recv_sink_destroy() when done.
 *
 * Returns NULL on error (with errno set).
 */
ReceiveSink *recv_sink_create(void);

/*
 * Path of the sink file. Stable from create() through destroy(). DuckDB
 * should open this path for reading.
 */
const char *recv_sink_path(const ReceiveSink * sink);

/*
 * Append bytes to the sink. Performs a full write (handles partial writes
 * and EINTR). Returns 0 on success, -1 on error.
 */
int			recv_sink_write(ReceiveSink * sink, const char *data, size_t len);

/*
 * Close the sink for writing. After this call, the sink path is readable
 * by DuckDB and reads will see EOF after all written bytes. Idempotent.
 * Returns 0 on success, -1 on error.
 */
int			recv_sink_finalize(ReceiveSink * sink);

/*
 * Destroy the sink: close the file if still open, unlink the path, and
 * free memory. Always succeeds; safe to call with NULL.
 */
void		recv_sink_destroy(ReceiveSink * sink);

#endif							/* PGDUCK_RECV_SINK_H */
