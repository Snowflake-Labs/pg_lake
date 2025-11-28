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

#include "utils/uuid.h"

#define PG_LAKE_STRINGIFY(x) #x
#define PG_LAKE_TOSTRING(x) PG_LAKE_STRINGIFY(x)

extern PGDLLEXPORT char *PgLakeReplaceText(const char *source,
										   const char *fromSubstitute,
										   const char *toSubstitute);
extern PGDLLEXPORT char *GenerateUUID(void);
extern PGDLLEXPORT pg_uuid_t *GeneratePGUUID(void);
extern PGDLLEXPORT char *StripFromChar(char *input, char ch);
extern PGDLLEXPORT char *ToLowerCase(char *input);
extern PGDLLEXPORT char *EscapedStringLiteral(const char *val);
extern PGDLLEXPORT char *ReverseStringSearch(const char *haystack, const char *needle);
extern PGDLLEXPORT int32_t AdjustAnyCharTypmod(int32_t typmod, int32_t newLength);
extern PGDLLEXPORT int32_t GetAnyCharLengthFrom(int32_t typmod);
extern PGDLLEXPORT bool PgStrcasecmpNullable(const char *a, const char *b);

#define RangeVarQuoteIdentifier(rv) \
	(((rv)->schemaname != NULL) ?							  \
	 quote_qualified_identifier((rv)->schemaname, (rv)->relname) :	\
	 quote_identifier((rv)->relname))
