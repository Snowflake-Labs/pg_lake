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

#pragma once

#include "postgres.h"

#include "utils/date.h"
#include "utils/timestamp.h"


/*
 * TimeTzGetUTCMicros converts a TimeTzADT to UTC microseconds since midnight,
 * normalized to [0, USECS_PER_DAY).
 *
 * PG stores timetz as {time (int64 usec local time), zone (int32 sec west of UTC)}.
 * UTC time = time + zone * USECS_PER_SEC  (see timetz_cmp_internal in date.c).
 *
 * The wrap-around to [0, USECS_PER_DAY) mirrors timetz_zone() in date.c which
 * uses a while-loop for negative values and a conditional modulo.  We use the
 * equivalent one-liner ((x % M) + M) % M to handle C99's negative-modulo
 * sign convention.
 */
static inline TimeADT
TimeTzGetUTCMicros(const TimeTzADT *timetz)
{
	int64		utcMicros = timetz->time + (int64) timetz->zone * USECS_PER_SEC;

	/* normalize to [0, USECS_PER_DAY) */
	utcMicros = ((utcMicros % USECS_PER_DAY) + USECS_PER_DAY) % USECS_PER_DAY;

	return (TimeADT) utcMicros;
}
