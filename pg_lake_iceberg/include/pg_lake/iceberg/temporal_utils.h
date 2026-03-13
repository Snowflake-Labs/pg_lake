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

extern PGDLLEXPORT Timestamp IcebergTimestampMsToPostgresTimestamp(Timestamp icebergTimestampMs);
extern PGDLLEXPORT Timestamp PostgresTimestampToIcebergTimestampMs(void);
extern PGDLLEXPORT Timestamp AdjustTimestampFromUnixToPostgres(Timestamp timestamp);
extern PGDLLEXPORT Timestamp AdjustTimestampFromPostgresToUnix(Timestamp timestamp);
extern PGDLLEXPORT DateADT AdjustDateFromPostgresToUnix(DateADT date);
extern PGDLLEXPORT DateADT AdjustDateFromUnixToPostgres(DateADT date);
extern PGDLLEXPORT int32_t DateYearFromUnixEpoch(DateADT date);
extern PGDLLEXPORT DateADT YearsFromEpochToDate(int32 yearsSinceEpoch);
extern PGDLLEXPORT Timestamp YearsFromEpochToTimestamp(int32 yearsSinceEpoch);
extern PGDLLEXPORT Timestamp MonthsFromUnixEpochToTimestamp(int32 monthsSinceEpoch);
extern PGDLLEXPORT DateADT MonthsFromEpochToDate(int32 monthsSinceEpoch);
extern PGDLLEXPORT DateADT DaysFromEpochToDate(int32 daysSinceEpoch);
extern PGDLLEXPORT TimeADT HoursFromUnixEpochToTime(int32 hoursSinceEpoch);
extern PGDLLEXPORT Timestamp DaysFromUnixEpochToTimestamp(int32 daysSinceEpoch);
extern PGDLLEXPORT int32_t DateMonthFromUnixEpoch(DateADT date);
extern PGDLLEXPORT int32_t DateDayFromUnixEpoch(DateADT date);
extern PGDLLEXPORT int32_t TimestampYearFromUnixEpoch(Timestamp timestamp);
extern PGDLLEXPORT int32_t TimestampMonthFromUnixEpoch(Timestamp timestamp);
extern PGDLLEXPORT int32_t TimestampDayFromUnixEpoch(Timestamp timestamp);
extern PGDLLEXPORT int32_t TimestampHourFromUnixEpoch(Timestamp timestamp);
extern PGDLLEXPORT Timestamp HoursFromUnixEpochToTimestamp(int32 hoursSinceEpoch);
extern PGDLLEXPORT int32_t TimeHourFromUnixEpoch(TimeADT time);
