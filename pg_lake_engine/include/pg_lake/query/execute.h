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

#include "executor/executor.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"


extern PGDLLEXPORT uint64 ExecuteInternalCommand(const char *queryString);
extern PGDLLEXPORT uint64 ExecuteQueryStringToDestReceiver(const char *queryString,
														   DestReceiver *dest);
extern PGDLLEXPORT uint64 ExecuteQueryToDestReceiver(Query *query,
													 const char *queryString,
													 ParamListInfo params,
													 DestReceiver *dest);
extern PGDLLEXPORT uint64 ExecutePlanToDestReceiver(PlannedStmt *plan,
													const char *queryString,
													ParamListInfo params,
													DestReceiver *dest);
extern PGDLLEXPORT void ExecuteInternalDDL(Node *parseTree, char *queryString);
extern PGDLLEXPORT int64 WriteQueryResultToCSV(char *query, char *destFile,
											   int *maxLineSize);

/* doesn't really belong here... */
extern PGDLLEXPORT Node *SimpleParseUtility(char *queryString);
