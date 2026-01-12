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
 * Main for pgduck_server.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#include <stdio.h>
#include <signal.h>

#include "c.h"
#include "postgres_fe.h"

#include "command_line/command_line.h"
#include "utils/pg_log_utils.h"
#include "utils/pgduck_log_utils.h"
#include "pgserver/pgserver.h"
#include "pgserver/client_threadpool.h"
#include "pgsession/pgsession.h"
#include "duckdb/duckdb.h"
#include "utils/pidfile.h"

int			pgduck_log_min_messages = LOG;

int
main(int argc, char *argv[])
{
	CommandLineOptions options = parse_arguments(argc, argv);

	/* user (or tests) are only interested in parsing the parameters */
	if (options.check_cli_params_only)
		return STATUS_OK;

	if (options.debug)
		pgduck_log_min_messages = DEBUG1;

	oom_is_fatal = !options.continue_on_oom;

	/* first, make sure duckdb is accessible */
	DuckDBStatus duckDbStatus = duckdb_global_init(options.duckdb_database_file_path,
												   options.cache_dir,
												   options.extensions_dir,
												   !options.no_extension_install,
												   options.memory_limit,
												   options.cache_on_write_max_size,
												   options.init_file_path);

	if (duckDbStatus != DUCKDB_SUCCESS)
	{
		/* already logged error(s) */
		return STATUS_ERROR;
	}

	if (options.pidfile_path != NULL)
	{
		create_my_pidfile(options.pidfile_path);
		atexit(cleanup_my_pidfile);
	}

	pgclient_threadpool_init(options.max_clients);

	PGServer	pgServer;

	srand(time(NULL));

	if (pgserver_init(&pgServer,
					  options.unix_socket_directory,
					  options.unix_socket_group,
					  options.unix_socket_permissions,
					  options.port) != STATUS_OK)
		return STATUS_ERROR;

	if (pgserver_run(&pgServer) != STATUS_OK)
		return STATUS_ERROR;

	if (pgserver_destroy(&pgServer) != STATUS_OK)
		return STATUS_ERROR;

	return STATUS_OK;
}
