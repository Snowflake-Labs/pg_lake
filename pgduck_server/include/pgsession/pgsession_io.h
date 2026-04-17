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
 * Functions for mimicking a Postgres client.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef PGDUCK_PG_SESSION_IO_H
#define PGDUCK_PG_SESSION_IO_H

#include "include/pgsession/pgsession.h"

extern int	pgsession_put_message(PGSession *session, char messageType, char *buf, size_t bufferLength);
extern int	pgsession_putemptymessage(PGSession *session, char msgtype);
extern int	pgsession_get_message(PGSession *session, StringInfo message, int maxLength);
extern int	pgsession_read_startup_packet(PGSession *session);
extern int	pgsession_read_command(PGSession *session, StringInfo inputMessage);
extern int	pgsession_get_bytes(PGSession *session, char *buf, size_t len);
extern int	pgsession_get_byte(PGSession *session);
extern int	pgsession_receive(PGSession *session);
extern int	pgsession_put_bytes(PGSession *session, char *buf, size_t bufferLength);
extern int	pgsession_flush(PGSession *session);
extern char *pgduck_client_to_server(const char *s, int len);
extern void pq_sendstring(StringInfo buf, const char *str);
extern int	pgsession_send_postgres_error(PGSession *pgSession, int errSev, char *errorMessage);

#endif							/* // PGDUCK_PG_SESSION_IO_H */
