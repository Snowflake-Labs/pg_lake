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

#ifndef PGDUCK_TYPE_H
#define PGDUCK_TYPE_H

#include "pg_lake/copy/copy_format.h"
#include "pg_lake/util/string_utils.h"

typedef enum DuckDBType
{
	DUCKDB_TYPE_INVALID = 0,
	/* bool */
	DUCKDB_TYPE_BOOLEAN,
	/* int8_t */
	DUCKDB_TYPE_TINYINT,
	/* int16_t */
	DUCKDB_TYPE_SMALLINT,
	/* int32_t */
	DUCKDB_TYPE_INTEGER,
	/* int64_t */
	DUCKDB_TYPE_BIGINT,
	/* uint8_t */
	DUCKDB_TYPE_UTINYINT,
	/* uint16_t */
	DUCKDB_TYPE_USMALLINT,
	/* uint32_t */
	DUCKDB_TYPE_UINTEGER,
	/* uint64_t */
	DUCKDB_TYPE_UBIGINT,
	/* float */
	DUCKDB_TYPE_FLOAT,
	/* double */
	DUCKDB_TYPE_DOUBLE,
	/* duckdb_timestamp, in microseconds */
	DUCKDB_TYPE_TIMESTAMP,
	/* duckdb_date */
	DUCKDB_TYPE_DATE,
	/* duckdb_time */
	DUCKDB_TYPE_TIME,
	/* duckdb_interval */
	DUCKDB_TYPE_INTERVAL,
	/* duckdb_hugeint */
	DUCKDB_TYPE_HUGEINT,
	/* const char* */
	DUCKDB_TYPE_VARCHAR,
	/* duckdb_blob */
	DUCKDB_TYPE_BLOB,
	/* decimal */
	DUCKDB_TYPE_DECIMAL,
	/* duckdb_timestamp, in seconds */
	DUCKDB_TYPE_TIMESTAMP_S,
	/* duckdb_timestamp, in milliseconds */
	DUCKDB_TYPE_TIMESTAMP_MS,
	/* duckdb_timestamp, in nanoseconds */
	DUCKDB_TYPE_TIMESTAMP_NS,
	/* enum type, only useful as logical type */
	DUCKDB_TYPE_ENUM,
	/* list type, only useful as logical type */
	DUCKDB_TYPE_LIST,
	/* struct type, only useful as logical type */
	DUCKDB_TYPE_STRUCT,
	/* map type, only useful as logical type */
	DUCKDB_TYPE_MAP,
	/* duckdb_hugeint */
	DUCKDB_TYPE_UUID,
	/* union type, only useful as logical type */
	DUCKDB_TYPE_UNION,
	/* duckdb_bit */
	DUCKDB_TYPE_BIT,
	/* duckdb_time_tz */
	DUCKDB_TYPE_TIME_TZ,
	/* duckdb_timestamp */
	DUCKDB_TYPE_TIMESTAMP_TZ,
	/* json extension */
	DUCKDB_TYPE_JSON,
	/* spatial extension */
	DUCKDB_TYPE_GEOMETRY
}			DuckDBType;

/*
 * DuckDBTypeInfo holds a DuckDB type with its full name in string form.
 */
typedef struct DuckDBTypeInfo
{
	DuckDBType	typeId;
	bool		isArrayType;
	char	   *typeName;
}			DuckDBTypeInfo;

/* Store information about a given postgres type */
typedef struct PGType
{
	Oid			postgresTypeOid;
	int32		postgresTypeMod;
}			PGType;

#define MakePGTypeOid(oid) ((PGType){.postgresTypeOid = oid, .postgresTypeMod = -1})
#define MakePGType(oid,mod) ((PGType){.postgresTypeOid = oid, .postgresTypeMod = mod})

extern PGDLLEXPORT const char *GetDuckDBTypeName(DuckDBType duckType);
extern PGDLLEXPORT const char *GetFullDuckDBTypeNameForPGType(PGType postgresType,
															  CopyDataFormat format);
extern PGDLLEXPORT DuckDBType GetDuckDBTypeForPGType(PGType postgresType);
extern PGDLLEXPORT DuckDBType GetDuckDBTypeByName(const char *name);
extern PGDLLEXPORT Oid GetOrCreatePGTypeForDuckDBTypeName(const char *name, int *typeMod);
extern PGDLLEXPORT bool IsStructType(const char *typeName);
extern PGDLLEXPORT bool IsArrayType(const char *typeName);
extern PGDLLEXPORT bool IsMapType(const char *typeName);
extern PGDLLEXPORT const char *QuoteDuckDBFieldName(char *fieldName);
extern PGDLLEXPORT PGType GetAttributePGType(Oid relationId, AttrNumber attrNo);

/* The schema to install our types */
#define STRUCT_TYPES_SCHEMA "lake_struct"

/* Some constants */
#define DUCKDB_STRUCT_TYPE_PREFIX "STRUCT"
#define DUCKDB_MAP_TYPE_PREFIX "MAP"

/* Mark as review candidate, but simplify code transition */
#define TYPMOD_TODO_DEFAULT -1

#endif
