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

/*
 * The access method name that turns a CREATE TABLE into a tiered table. It is a
 * placeholder: no relation is ever stored with it (src/ddl.c rewrites the
 * statement before PostgreSQL looks the method up), and its handler raises.
 */
#define TIMESERIES_AM "timeseries"

/* WITH options that CREATE TABLE ... USING timeseries consumes itself */
#define TIMESERIES_OPTION_TIME_COLUMN "time_column"
#define TIMESERIES_OPTION_PARTITION_INTERVAL "partition_interval"
#define TIMESERIES_OPTION_HOT_RETENTION "hot_retention"
#define TIMESERIES_OPTION_COLD_RETENTION "cold_retention"
#define TIMESERIES_OPTION_PRECREATE_AHEAD "precreate_ahead"

/* option of the Iceberg tier that is defaulted from partition_interval */
#define TIMESERIES_OPTION_PARTITION_BY "partition_by"

/* how the Iceberg tier of a tiered table is named */
#define TIMESERIES_COLD_SUFFIX "cold"

/* defaults of the options above */
#define TIMESERIES_DEFAULT_PARTITION_INTERVAL "1 day"
#define TIMESERIES_DEFAULT_HOT_RETENTION "7 days"
#define TIMESERIES_DEFAULT_PRECREATE_AHEAD 7

extern void InitializeTimeseriesDDL(void);
