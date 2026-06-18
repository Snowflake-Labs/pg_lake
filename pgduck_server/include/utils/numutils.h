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
 * Helper functions for manipulating numbers.
 *
 * Copyright (c) 2025 Snowflake Computing, Inc. All rights reserved.
 */
#ifndef PGDUCK_NUM_UTILS_H
#define PGDUCK_NUM_UTILS_H

#include "c.h"

extern int	pg_itoa(int16 i, char *a);
extern int	pg_ulltoa_n(uint64 value, char *a);
extern int	pg_ultoa_n(uint32 value, char *a);
extern int	pg_ltoa(int32 value, char *a);
extern int	pg_lltoa(int64 value, char *a);
extern void pg_bool_to_text(bool val, char *buffer);
extern int	pg_ulltoa(uint64 value, char *a);
extern void pg_float4_to_text(float4 num, char *buf);
extern void pg_float8_to_text(float8 num, char *buf);

#endif							/* // PGDUCK_NUM_UTILS_H */
