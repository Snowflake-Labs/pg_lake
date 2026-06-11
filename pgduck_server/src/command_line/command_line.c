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
#include "postgres_fe.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>

#include "command_line/command_line.h"
#include "utils/pg_log_utils.h"
#include "utils/pgduck_log_utils.h"
#include "utils/string_utils.h"

/*
 * By default we set socket permissions to 770 (user & group only), without
 * specifying a group by default (meaning user-only is the default).
 * pgduck_server is meant as an internal component, so there is no reason for
 * other Linux users to access it by default.
 */
#define DEFAULT_UNIX_DOMAIN_PATH "/tmp"
#define DEFAULT_UNIX_DOMAIN_GROUP ""
#define DEFAULT_UNIX_DOMAIN_PERMISSIONS 0770
#define DEFAULT_PORT 5332
#define DEFAULT_DUCKDB_DATABASE_FILE_PATH "/tmp/duckdb.db"
#define DEFAULT_MAX_CLIENTS 10000
#define DEFAULT_CACHE_ON_WRITE_MAX_SIZE 1024 * 1024 * 1024 // 1GB

bool		IsOutputVerbose = false;

static void
print_usage()
{
	printf("Usage: pgduck_server [options]\n");
	printf("Options:\n");
	printf(" --unix_socket_directory <path>		Specify the unix socket directory, default is %s\n", DEFAULT_UNIX_DOMAIN_PATH);
	printf(" --unix_socket_group <group name>	Specify the unix socket group owner, default is \"%s\"\n", DEFAULT_UNIX_DOMAIN_GROUP);
	printf(" --unix_socket_permissions <mask>	Specify the unix socket (chmod) permissions, default is %o\n", DEFAULT_UNIX_DOMAIN_PERMISSIONS);
	printf(" --port <port>                 		Specify the port number, default is %d\n", DEFAULT_PORT);
	printf(" --max_clients <max_clients>		Specify the maximum allowed clients, default is %d\n", DEFAULT_MAX_CLIENTS);
	printf(" --memory_limit=<memory_limit>		Optionally specify the maximum memory of pgduck_server similar to DuckDB's memory_limit, the default is 80 percent of the system memory\n");
	printf(" --continue_on_oom                  If out of memory error occurs, continue operating\n");
	printf(" --cache_on_write_max_size=<size>   Optionally specify the maximum allowed cache size on write\n");
	printf(" --duckdb_database_file_path <path>	Specify the database file path for DuckDB, default is %s\n", DEFAULT_DUCKDB_DATABASE_FILE_PATH);
	printf(" --check_cli_params_only       		Only check the cli arguments, do not run the server\n");
	printf(" --init_file_path <path>			Execute all statements in this file on start-up\n");
	printf(" --cache_dir                    	Specify the directory to use to cache remote files (from S3)\n");
	printf(" --extensions_dir <path>			Install and load extensions in the specified directory\n");
	printf(" --pidfile <path>					Write the pid of this program to the given path\n");
	printf(" --no_extension_install             Disable extension installation\n");
	printf(" --debug                            Include debug-level log messages (including full queries) in server output\n");
	printf(" --verbose                     		Run in verbose mode\n");
	printf(" --help                        		Display this help and exit\n");
}

CommandLineOptions
parse_arguments(int argc, char *argv[])
{
	/* default values for command line options */
	CommandLineOptions options = {
		.check_cli_params_only = false,
		.verbose = false,
		.help = false,
		.unix_socket_directory = DEFAULT_UNIX_DOMAIN_PATH,
		.unix_socket_group = DEFAULT_UNIX_DOMAIN_GROUP,
		.unix_socket_permissions = DEFAULT_UNIX_DOMAIN_PERMISSIONS,
		.port = DEFAULT_PORT,
		.max_clients = DEFAULT_MAX_CLIENTS,
		.memory_limit = NULL,
		.continue_on_oom = false,
		.cache_on_write_max_size = DEFAULT_CACHE_ON_WRITE_MAX_SIZE,
		.duckdb_database_file_path = DEFAULT_DUCKDB_DATABASE_FILE_PATH,
		.init_file_path = NULL,
		.pidfile_path = NULL,
		.cache_dir = NULL,
		.extensions_dir = NULL,
		.no_extension_install = false,
		.debug = false,
	};
	int			opt;
	int			option_index = 0;

	static struct option long_options[] = {
		{"check_cli_params_only", no_argument, NULL, 'c'},
		{"verbose", no_argument, NULL, 'v'},
		{"help", no_argument, NULL, 'h'},
		{"unix_socket_directory", required_argument, NULL, 'U'},
		{"unix_socket_group", required_argument, NULL, 'G'},
		{"unix_socket_permissions", required_argument, NULL, 'm'},
		{"port", required_argument, NULL, 'P'},
		{"max_clients", required_argument, NULL, 'M'},
		{"memory_limit", required_argument, NULL, 'l'},
		{"continue_on_oom", no_argument, NULL, 'O'},
		{"cache_on_write_max_size", required_argument, NULL, 'L'},
		{"duckdb_database_file_path", required_argument, NULL, 'D'},
		{"cache_dir", required_argument, NULL, 'C'},
		{"extensions_dir", required_argument, NULL, 'E'},
		{"no_extension_install", no_argument, NULL, 'n'},
		{"init_file_path", required_argument, NULL, 'i'},
		{"pidfile", required_argument, NULL, 'p'},
		{"debug", no_argument, NULL, 'd'},
		{0, 0, 0, 0}
	};

	while ((opt = getopt_long(argc, argv, "cvhU:P:M:D:l:L:p:d", long_options, &option_index)) != -1)
	{
		switch (opt)
		{
			case 'v':
				options.verbose = true;
				break;
			case 'c':
				options.check_cli_params_only = true;
				break;
			case 'h':
				options.help = true;
				print_usage();
				exit(EXIT_SUCCESS);
			case 'U':
				options.unix_socket_directory = strdup(optarg);
				break;
			case 'G':
				options.unix_socket_group = strdup(optarg);
				break;
			case 'l':
				if (optarg)
					options.memory_limit = strdup(optarg);
				break;
			case 'O':
				options.continue_on_oom = true;
				break;
			case 'm':
				{
					int			permissions = 0;
					char		end = '\0';

					if (sscanf(optarg, "%o%c", &permissions, &end) != 1)
					{
						fprintf(stderr, "Error: permissions should be an integer\n");
						exit(EXIT_FAILURE);
					}

					if (!(permissions >= 0000 && permissions <= 0777))
					{
						fprintf(stderr, "permissions mask should be in between [0000, 0777]\n");
						exit(EXIT_FAILURE);
					}

					options.unix_socket_permissions = permissions;

					break;
				}
			case 'D':
				options.duckdb_database_file_path = strdup(optarg);
				break;
			case 'C':
				options.cache_dir = strdup(optarg);
				break;
			case 'E':
				options.extensions_dir = strdup(optarg);
				break;
			case 'n':
				options.no_extension_install = true;
				break;
			case 'P':
				{
					int			inputPort = 0;

					if (!string_to_int(optarg, &inputPort))
					{
						fprintf(stderr, "Error: Port should be an integer\n");
						exit(EXIT_FAILURE);
					}

					if (!(inputPort >= 1 && inputPort <= 65535))
					{
						fprintf(stderr, "Port should be in between [1, 65535]\n");
						exit(EXIT_FAILURE);
					}

					options.port = inputPort;

					break;
				}
			case 'M':
				{
					int			inputMaxClients = 0;

					if (!string_to_int(optarg, &inputMaxClients))
					{
						fprintf(stderr, "Error: max_clients should be an integer\n");
						exit(EXIT_FAILURE);
					}

					if (!(inputMaxClients >= 1 && inputMaxClients <= 100000))
					{
						fprintf(stderr, "max_clients should be in between [1, 100000]\n");
						exit(EXIT_FAILURE);
					}

					options.max_clients = inputMaxClients;

					break;
				}
			case 'L':
				{
					int64_t		cache_on_write_max_size = 0;

					if (!string_to_int64(optarg, &cache_on_write_max_size))
					{
						fprintf(stderr, "Error: cache_on_write_max_size should be an integer\n");
						exit(EXIT_FAILURE);
					}

					if (!(cache_on_write_max_size >= 0))
					{
						fprintf(stderr, "cache_on_write_max_size should be greater than or equal to 0\n");
						exit(EXIT_FAILURE);
					}

					options.cache_on_write_max_size = cache_on_write_max_size;

					break;
				}
			case 'i':
				options.init_file_path = strdup(optarg);
				break;
			case 'd':
				options.debug = true;
				break;
			case 'p':
				options.pidfile_path = strdup(optarg);
				break;
			case '?':
				print_usage();
				exit(EXIT_FAILURE);
			default:
				print_usage();
				exit(EXIT_FAILURE);
		}
	}

	PGDUCK_SERVER_LOG("pgduck_server is listening on unix_socket_directory: %s with port %u, max_clients allowed %d",
					  options.unix_socket_directory, options.port, options.max_clients);

	PGDUCK_SERVER_LOG("DuckDB is using database file path: %s",
					  options.duckdb_database_file_path);

	if (options.no_extension_install)
		PGDUCK_SERVER_LOG("Using local extension binaries only");

	if (options.debug)
		PGDUCK_SERVER_LOG("Debugging mode on; will log all queries");

	if (options.memory_limit)
	{
		PGDUCK_SERVER_LOG("Memory limit is set to: %s", options.memory_limit);
	}
	else
	{
		PGDUCK_SERVER_LOG("Default memory limit is used, which is 80 percent of the system memory. "
						  "To set a specific memory limit, use --memory_limit=<value>");
	}

	PGDUCK_SERVER_LOG("Cache on write max size is set to: %" PRIu64, options.cache_on_write_max_size);

	if (options.verbose)
	{
		PGDUCK_SERVER_LOG("Verbose mode enabled.");
		IsOutputVerbose = true;
	}

	return options;
}
