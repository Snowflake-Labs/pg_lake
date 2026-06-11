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
 * Utility functions for command line processing.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef COMMAND_LINE_H
#define COMMAND_LINE_H

#include <stdbool.h>

typedef struct
{
	bool		check_cli_params_only;	/* mostly used for testing purposes */
	bool		verbose;
	bool		help;
	char	   *unix_socket_directory;
	char	   *unix_socket_group;
	int			unix_socket_permissions;
	unsigned int port;
	unsigned int max_clients;
	char	   *memory_limit;
	bool		continue_on_oom;
	int64_t		cache_on_write_max_size;

	char	   *duckdb_database_file_path;
	char	   *cache_dir;
	char	   *extensions_dir;
	bool		no_extension_install;
	bool		debug;
	char	   *init_file_path;
	char	   *pidfile_path;
}			CommandLineOptions;

CommandLineOptions parse_arguments(int argc, char *argv[]);

#endif							/* // COMMAND_LINE_H */
