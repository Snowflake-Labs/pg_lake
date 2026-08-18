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
 * Utility functions for pgduck_server logs.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef PG_DUCK_LOG_UTILS_H
#define PG_DUCK_LOG_UTILS_H

#include <stdbool.h>
#include "utils/pg_log_utils.h"

/* we map error levels into these strings via level ## _STRING below */
#define ERROR_STRING "ERROR"
#define WARNING_STRING "WARNING"
#define LOG_STRING "LOG"
#define DEBUG1_STRING "DEBUG"

extern int	pgduck_log_min_messages;

/*
 * Print the message to stderr, prefixed by
 * verbose: <timestamp> <level string> <file name>:<line number>:<function name>
 * regular: <timestamp> <level string>
 */
#define PGDUCK_SERVER_LOG_INTERNAL(level, fmt, ...) \
	if (level >= pgduck_log_min_messages) {					\
	char currentTime[21]; \
	iso8601_timestamp(currentTime); \
	if (IsOutputVerbose) \
		fprintf(stderr, "%s %s %s:%d:%s " fmt "\n", \
				currentTime, level ## _STRING, __FILE__, __LINE__, __func__, ##__VA_ARGS__);  \
	else \
		fprintf(stderr, "%s %s " fmt "\n", \
				currentTime, level ## _STRING, ##__VA_ARGS__); \
	}

#define PGDUCK_SERVER_DEBUG(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(DEBUG1, fmt __VA_OPT__(,) __VA_ARGS__)

#define PGDUCK_SERVER_LOG(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(LOG, fmt __VA_OPT__(,) __VA_ARGS__)

#define PGDUCK_SERVER_WARN(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(WARNING, fmt __VA_OPT__(,) __VA_ARGS__)

#define PGDUCK_SERVER_ERROR(fmt, ...) \
    PGDUCK_SERVER_LOG_INTERNAL(ERROR, fmt __VA_OPT__(,) __VA_ARGS__)

extern bool IsOutputVerbose;

void		iso8601_timestamp(char *buf);

/*
 * Enough for the verb and the secret's name; the argument list that
 * follows is what QueryStringForLog leaves out.
 */
#define QUERY_LOG_BUF_SIZE 256

const char *QueryStringForLog(const char *queryString, char *buf,
							  size_t bufSize);

#endif							/* // PG_DUCK_LOG_UTILS_H */
