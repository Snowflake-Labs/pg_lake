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
#include "utils/hsearch.h"

extern bool EnableStrictPushdown;

typedef enum NotShippableReason
{
	NOT_SHIPPABLE_UNKNOWN = 0,
	NOT_SHIPPABLE_TABLE,
	NOT_SHIPPABLE_TYPE,
	NOT_SHIPPABLE_FUNCTION,
	NOT_SHIPPABLE_SQL_VALUE_FUNCTION,
	NOT_SHIPPABLE_OPERATOR,
	NOT_SHIPPABLE_COLLATION,
	NOT_SHIPPABLE_SYSTEM_COLUMN,
	NOT_SHIPPABLE_NAMEDTUPLESTORE,
	NOT_SHIPPABLE_TABLEFUNC,
	NOT_SHIPPABLE_MULTIPLE_FUNCTION_TABLE,
	NOT_SHIPPABLE_SQL_FOR_UPDATE,
	NOT_SHIPPABLE_SQL_LIMIT_WITH_TIES,
	NOT_SHIPPABLE_SQL_EMPTY_TARGET_LIST,
	NOT_SHIPPABLE_SQL_WITH_ORDINALITY,
	NOT_SHIPPABLE_SQL_JOIN_MERGED_COLUMNS_ALIAS,
	NOT_SHIPPABLE_SQL_UNNEST_GROUP_BY_OR_WINDOW,
	NOT_SHIPPABLE_INFINITE_CONST_VALUE
}			NotShippableReason;


typedef struct NotShippableObject
{
	Oid			classId;		/* Class OID identifying the type of object */
	Oid			objectId;		/* Specific object OID (if applicable) */
	NotShippableReason reason;	/* Categorizes why the object isn't shippable */
}			NotShippableObject;

extern bool is_builtin(Oid objectId);
extern bool is_shippable(Oid objectId, Oid classId, Node *expr);
extern bool is_non_shippable_udt_context(Node *node);
extern const char *GetNotShippableDescription(NotShippableReason reason, Oid classId, Oid objectId);
extern HTAB *CollectNotShippableObjects(Node *node);
