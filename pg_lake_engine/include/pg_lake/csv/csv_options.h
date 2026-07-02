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

#include "commands/copy.h"
#include "nodes/pg_list.h"

#if PG_VERSION_NUM >= 190000
/*
 * PG19 dropped the CopyHeaderChoice enum and stores the header choice as a
 * plain int holding COPY_HEADER_FALSE / COPY_HEADER_TRUE / COPY_HEADER_MATCH.
 * Reintroduce the typedef here so we keep a stable signature.
 */
typedef int CopyHeaderChoice;
#endif

extern PGDLLEXPORT List *InternalCSVOptions(bool includeHeader);
extern PGDLLEXPORT List *NormalizedExternalCSVOptions(List *inputOptions);
extern PGDLLEXPORT CopyHeaderChoice GetCopyHeaderChoice(DefElem *def, bool is_from);
extern PGDLLEXPORT bool HasAutoDetect(List *options);
