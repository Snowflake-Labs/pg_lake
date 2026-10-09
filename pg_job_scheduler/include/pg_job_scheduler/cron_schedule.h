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

#include "utils/timestamp.h"

/*
 * CronSchedule is a parsed 5-field cron expression, held as one bitmap per
 * field. Bit N of a field is set when the field matches value N, so matching
 * a timestamp is a handful of bit tests.
 */
typedef struct CronSchedule
{
	uint64		minutes;		/* bits 0..59 */
	uint32		hours;			/* bits 0..23 */
	uint32		daysOfMonth;	/* bits 1..31 */
	uint16		months;			/* bits 1..12 */
	uint8		daysOfWeek;		/* bits 0..6, 0 = Sunday */

	/*
	 * Whether the day-of-month and day-of-week fields were something other
	 * than '*'. Vixie cron matches a day when *either* restricted field
	 * matches, so which of them were restricted has to survive parsing.
	 */
	bool		dayOfMonthRestricted;
	bool		dayOfWeekRestricted;
}			CronSchedule;

extern void ParseCronSchedule(const char *expression, CronSchedule * schedule);
extern TimestampTz CronScheduleNextRun(const CronSchedule * schedule,
									   TimestampTz after);
