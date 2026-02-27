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
 * Functions for mimicking a Postgres server.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef PGDUCK_PG_SERVER_H
#define PGDUCK_PG_SERVER_H

/*
 * PGServer represents an instance of a PostgreSQL wire compatible
 * server.
 */
typedef struct PGServer
{
	int			listeningPort;
	int			listeningSocket;

	char		unixSocketDir[MAXPGPATH];
	char		unixSocketPath[MAXPGPATH];
	char		lockFilePath[MAXPGPATH];
	time_t		last_touch_time;

	/*
	 * Make it generic, for example we might use different functions for
	 * simple protocol and extended protocol etc.
	 */
	void	   *(*startFunction) (void *);
}			PGServer;

extern int	pgserver_init(PGServer * pgServer,
						  char *unixSocketPath,
						  char *unixSocketOwningGroup,
						  int unixSocketPermissions,
						  int port);
extern int	pgserver_run(PGServer * pgServer);
extern void pgserver_destroy(PGServer * pgServer);


#endif							/* PGDUCK_PG_SERVER_H */
