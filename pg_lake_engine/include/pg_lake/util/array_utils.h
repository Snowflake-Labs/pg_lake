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

#include "nodes/pg_list.h"
#include "utils/array.h"

extern PGDLLEXPORT List *StringArrayToList(ArrayType *array);
extern PGDLLEXPORT List *Int64ArrayToList(ArrayType *array);
extern PGDLLEXPORT ArrayType *StringListToArray(List *stringList);
extern PGDLLEXPORT ArrayType *INT16ListToArray(List *stringList);
extern PGDLLEXPORT ArrayType *OidListToArray(List *oidList);
