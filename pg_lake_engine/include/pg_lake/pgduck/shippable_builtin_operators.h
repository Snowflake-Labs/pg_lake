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

/*-------------------------------------------------------------------------
 *
 * shippable_builtin_operators.h
 *
 * This file contains the list of supported operators for the pg_lake
 * FDW.  This list is used to determine if a query can be pushed down
 * to the remote server.
 *-------------------------------------------------------------------------
 */
#ifndef PG_LAKE_SUPPORTED_OPERATORS_H
#define PG_LAKE_SUPPORTED_OPERATORS_H

#include "postgres.h"
#include "nodes/nodes.h"
#include "pg_lake/pgduck/shippable_builtin_functions.h"

typedef struct
{
	char	   *oprname;
	char	   *oprnamespace;
	char	   *oprcode;
	int			oprcodeargcount;
	char	   *oprcodeargtypes[2];
	IsShippableFunction isShippable;
} PGDuckShippableOperator;

typedef struct
{
	char	   *typename;
	int			oprsLen;
	const PGDuckShippableOperator *oprs;
} PGDuckShippableOperatorsByType;

#define ARRAY_SIZE(arr) (sizeof(arr) / sizeof((arr)[0]))

extern PGDLLEXPORT const PGDuckShippableOperatorsByType *GetPGDuckShippableOperatorsByType(int *sizePointer);

#endif							/* PG_LAKE_SUPPORTED_OPERATORS_H */
