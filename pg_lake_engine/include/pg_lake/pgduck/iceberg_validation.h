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
 * Behavior for out-of-range or unsupported values during writes to Iceberg
 * data files.
 *
 * Applies to:
 *   - Temporal columns (date, timestamp, timestamptz): values beyond
 *     the Iceberg-supported range are clamped or rejected.
 *   - Bounded numeric columns: NaN values are clamped to NULL or rejected.
 *   - Array columns: multidimensional arrays are clamped to NULL or
 *     rejected (error), since PostgreSQL's single array type (e.g. int[])
 *     maps to a flat LIST(T) in DuckDB/Iceberg.
 *
 * Controlled by the out_of_range_values table option (default: error).
 *
 * CLAMP silently adjusts values (e.g. year 10000 becomes 9999-12-31,
 * NaN becomes NULL, multidimensional arrays become NULL).
 * ERROR raises an error instead.
 * NONE skips validation entirely (used for non-Iceberg tables).
 */
typedef enum IcebergOutOfRangePolicy
{
	ICEBERG_OOR_NONE = 0,
	ICEBERG_OOR_ERROR = 1,
	ICEBERG_OOR_CLAMP = 2,
}			IcebergOutOfRangePolicy;

/*
 * GetIcebergOutOfRangePolicyForTable returns the IcebergOutOfRangePolicy
 * for the given relation.  Returns NONE when the table is not Iceberg.
 */
extern PGDLLEXPORT IcebergOutOfRangePolicy GetIcebergOutOfRangePolicyForTable(Oid relationId);
extern PGDLLEXPORT bool IsTemporalType(Oid typeOid);

/*
 * TypeNeedsIcebergValidation recursively checks whether a type contains
 * any component that needs Iceberg write validation, including inside
 * arrays, composites, maps, and domains.
 *
 * typmod is used to distinguish bounded numerics (Iceberg decimal) from
 * unbounded ones (mapped to float8).  Only bounded numerics need NaN
 * validation.
 *
 * Validation covers: temporal boundaries (date/timestamp/timestamptz),
 * multidimensional array rejection (any array type), and bounded
 * numeric NaN (non-pushdown only, since numeric blocks pushdown).
 */
extern PGDLLEXPORT bool TypeNeedsIcebergValidation(Oid typeOid, int32 typmod,
												   bool isPushdown);

/* Temporal boundary year constants shared by datum and query-level validation */
#define TEMPORAL_DATE_MIN_YEAR		(-4712)
#define TEMPORAL_TIMESTAMP_MIN_YEAR	1
#define TEMPORAL_MAX_YEAR			9999

/*
 * Snowflake's per-column byte ceilings, applied when an Iceberg table is
 * declared with compatibility_mode='snowflake'.  Values writing to such a
 * table run through IcebergSizeClampDatum / IcebergWrapQueryWithSizeClampChecks
 * and are clamped or rejected (per out_of_range_values) once they exceed the
 * matching cap.
 *
 * The cap matches Snowflake's narrowest default column ceiling for that
 * category:
 *   STRING        : 16 MiB (default VARCHAR/STRING width)
 *   BINARY        :  8 MiB (default BINARY width)
 *   OBJECT/ARRAY/
 *   VARIANT       : 128 MiB (semi-structured column ceiling)
 *
 * Tables in any other compatibility_mode pass through unchanged: the call
 * sites only enter the clamp paths when compatibility_mode='snowflake'.
 */
#define ICEBERG_SNOWFLAKE_MAX_STRING_BYTES		(16 * 1024 * 1024)
#define ICEBERG_SNOWFLAKE_MAX_BINARY_BYTES		(8 * 1024 * 1024)
#define ICEBERG_SNOWFLAKE_MAX_NESTED_TYPE_BYTES	(128 * 1024 * 1024)

/*
 * TypeNeedsIcebergSizeClamping returns true if a Datum of typeOid (or any
 * lossless string / structured-string / bytea component nested within it)
 * could potentially be size-clamped by IcebergSizeClampDatum.  Recurses
 * through arrays, composites, maps, and domains.
 */
extern PGDLLEXPORT bool TypeNeedsIcebergSizeClamping(Oid typeOid);

/*
 * IcebergScalarStorageIsStringOrBinary returns true when typeOid is a scalar
 * leaf that Iceberg stores as string or binary but that has no type-specific
 * truncation (i.e. not text/varchar/bpchar/bytea/jsonb/json) -- hstore,
 * citext, PostGIS geometry, and other types that fall back to string/binary
 * serialization.  Such values are NULLed when oversize.  *isBinary, when
 * non-NULL, is set true for the binary storage class and false for string.
 */
extern PGDLLEXPORT bool IcebergScalarStorageIsStringOrBinary(Oid typeOid,
															 bool *isBinary);
