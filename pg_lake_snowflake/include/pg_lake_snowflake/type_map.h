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

/*
 * type_map.h
 * Snowflake result types, their Postgres equivalents, and the conversion of
 * the values the SQL API sends over the wire.
 */
#pragma once

#include "postgres.h"

#include "fmgr.h"

/*
 * The type codes the SQL API reports in resultSetMetaData.rowType[].type.
 * These are the "logical" Snowflake types, lower case, and they are a much
 * smaller set than the SQL type names.
 */
typedef enum SnowflakeTypeCode
{
	SNOWFLAKE_TYPE_UNKNOWN = 0,
	SNOWFLAKE_TYPE_FIXED,
	SNOWFLAKE_TYPE_REAL,
	SNOWFLAKE_TYPE_TEXT,
	SNOWFLAKE_TYPE_BOOLEAN,
	SNOWFLAKE_TYPE_DATE,
	SNOWFLAKE_TYPE_TIME,
	SNOWFLAKE_TYPE_TIMESTAMP_NTZ,
	SNOWFLAKE_TYPE_TIMESTAMP_LTZ,
	SNOWFLAKE_TYPE_TIMESTAMP_TZ,
	SNOWFLAKE_TYPE_BINARY,
	SNOWFLAKE_TYPE_VARIANT,
	SNOWFLAKE_TYPE_OBJECT,
	SNOWFLAKE_TYPE_ARRAY,
	SNOWFLAKE_TYPE_MAP,
	SNOWFLAKE_TYPE_GEOGRAPHY,
	SNOWFLAKE_TYPE_GEOMETRY,
	SNOWFLAKE_TYPE_VECTOR
}			SnowflakeTypeCode;

/* one entry of resultSetMetaData.rowType */
typedef struct SnowflakeResultColumn
{
	char	   *name;
	char	   *typeName;		/* as reported, for error messages */
	SnowflakeTypeCode typeCode;
	int32		precision;
	int32		scale;
	int64		length;
	bool		nullable;
}			SnowflakeResultColumn;

/*
 * SnowflakeTypeConversion caches what it takes to turn the text of one result
 * column into a Datum of the Postgres type the column is declared as.
 */
typedef struct SnowflakeTypeConversion
{
	Oid			typeId;
	int32		typeMod;
	Oid			inputParameterType;
	FmgrInfo	inputFunction;
}			SnowflakeTypeConversion;

extern SnowflakeTypeCode SnowflakeTypeCodeFromName(const char *typeName);
extern void SnowflakeColumnPostgresType(SnowflakeResultColumn * column,
										Oid *typeId, int32 *typeMod);
extern void SnowflakeInitTypeConversion(SnowflakeTypeConversion * conversion,
										Oid typeId, int32 typeMod);
extern Datum SnowflakeValueToDatum(const char *value, SnowflakeResultColumn * column,
								   SnowflakeTypeConversion * conversion);
extern bool SnowflakeTypeIsPushdownSafe(Oid typeId);
extern char *SnowflakeFormatLiteral(Datum value, Oid typeId);
