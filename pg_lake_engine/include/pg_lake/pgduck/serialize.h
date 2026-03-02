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
#include "fmgr.h"

#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/type.h"

#define BYTEA_OUT_OID 31

extern PGDLLEXPORT char *PGDuckSerialize(FmgrInfo *flinfo, Oid typeOid, Datum value,
										 CopyDataFormat format);
extern PGDLLEXPORT char *PGDuckOnlySerialize(Oid typeOid, Datum value);
extern PGDLLEXPORT bool IsPGDuckSerializeRequired(PGType postgresType);
extern PGDLLEXPORT char *IntervalOutForPGDuck(Datum value);
extern PGDLLEXPORT char *IntervalArrayOutForPGDuck(Datum value);
extern bool IsContainerType(Oid postgresType);

/*
 * IsSerializedAsContainer returns whether a type will be serialized as a
 * container (struct/array/map) for the given format. Iceberg intervals are
 * serialized as struct(months, days, microseconds), so they count as
 * containers in that context.
 */
static inline bool
IsSerializedAsContainer(Oid typeId, CopyDataFormat format)
{
	return IsContainerType(typeId) ||
		(typeId == INTERVALOID && format == DATA_FORMAT_ICEBERG);
}
