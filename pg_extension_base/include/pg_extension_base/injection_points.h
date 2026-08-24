/*
 * Copyright 2026 Snowflake Inc.
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
 * IS_INJECTION_POINT_ATTACHED_COMPAT lets a test steer a branch that cannot be
 * reached by running code -- for instance a return value that only the kernel
 * refusing to fork can produce.  Only Postgres 18 can answer the question, so
 * on older versions it is constant false and the branch is unreachable; a test
 * that relies on it has to skip.  Either way it compiles to nothing unless the
 * server was built with --enable-injection-points, so it costs a regular build
 * neither code nor a check.
 */

#if PG_VERSION_NUM >= 180000

#include "utils/injection_point.h"

#define INJECTION_POINT_COMPAT(name) \
    INJECTION_POINT(name, NULL)

#define IS_INJECTION_POINT_ATTACHED_COMPAT(name) \
    IS_INJECTION_POINT_ATTACHED(name)

#elif PG_VERSION_NUM >= 170000

#include "utils/injection_point.h"

#define INJECTION_POINT_COMPAT(name) \
    INJECTION_POINT(name)

#define IS_INJECTION_POINT_ATTACHED_COMPAT(name) \
    (false)

#else

#define INJECTION_POINT_COMPAT(name) \
    ((void) name)

#define IS_INJECTION_POINT_ATTACHED_COMPAT(name) \
    (false)

#endif
