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

#ifndef PG_LAKE_ICEBERG_ICEBERG_REPRESENTATION_H
#define PG_LAKE_ICEBERG_ICEBERG_REPRESENTATION_H

#include "postgres.h"

#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/pgduck/compatibility_mode.h"

/*
 * SameIcebergRepresentation - true when oldType and newType are stored with an
 * identical Iceberg representation, after applying `convert` at every scalar
 * leaf of each type.  It is true for pairs that differ only in ways Iceberg
 * does not model (text length, which text-family member, field ids, defaults):
 * varchar length changes and text/varchar/char (all `string`), smallint vs
 * integer (both `int`), time vs timetz (both `time`), and so on.
 *
 * This is the low-level primitive: it applies only the given leaf conversion.
 * `convert` may be NULL to compare the raw derivations.  When it reports that
 * either type has no faithful stored representation (see
 * IcebergLeafConversionFn), the two are treated as different so a caller can
 * never over-allow.  Most callers want SameIcebergStoredRepresentation, which
 * layers in the create path's compatibility storage mapping as well.
 *
 * It answers only the representation question; it does not decide whether a
 * type change is otherwise permitted (casts, USING clauses, engine policy).
 */
extern PGDLLEXPORT bool SameIcebergRepresentation(PGType oldType, PGType newType,
												  IcebergLeafConversionFn convert,
												  void *context);

/*
 * IcebergCreatePathContext captures the two settings that shape how the Iceberg
 * create path physically stores a column, so a comparison can reproduce the
 * stored schema faithfully:
 *
 *   unsupportedNumericAsDouble - the pg_lake_iceberg.unsupported_numeric_as_double
 *       value.  On: an unsupported numeric (unbounded, or precision/scale > 38)
 *       is stored as double at every nesting level.  Off: CREATE errors on such
 *       a numeric at any level, so it has no stored form.
 *   compatibilityMode - the table's compatibility_mode.  ICEBERG_COMPAT_SNOWFLAKE
 *       stores a uuid nested inside an array/composite as string (a top-level
 *       uuid stays native); ICEBERG_COMPAT_AUTO applies no such mapping.
 *
 * Both are captured, not read from live state, because the GUC is PGC_USERSET
 * and the mode is per-table: a comparison must use the values in effect when
 * the target table was created, which may differ now.  Prefer
 * InitIcebergCreatePathContext with the create-time values;
 * InitIcebergCreatePathContextFromGUC (live GUC, AUTO mode) is only correct when
 * neither has changed since.
 */
typedef struct IcebergCreatePathContext
{
	bool		unsupportedNumericAsDouble;
	IcebergCompatibilityMode compatibilityMode;
}			IcebergCreatePathContext;

extern PGDLLEXPORT void InitIcebergCreatePathContext(IcebergCreatePathContext * context,
													 bool unsupportedNumericAsDouble,
													 IcebergCompatibilityMode compatibilityMode);
extern PGDLLEXPORT void InitIcebergCreatePathContextFromGUC(IcebergCreatePathContext * context);

/*
 * Stock leaf conversion mirroring the create path's unsupported-numeric
 * handling.  Depth-independent (an unsupported numeric is stored as double, or
 * unrepresentable, at every level); the IcebergTypePosition is part of the
 * callback contract for callers whose leaf rules do vary by nesting.  The
 * compatibility storage mapping is applied separately, as a Field-tree pass, by
 * SameIcebergStoredRepresentation -- see ApplyCompatibilityStorageMapping.
 */
extern PGDLLEXPORT PGType IcebergCreatePathLeafConversion(PGType leafType,
														  IcebergTypePosition pos,
														  void *context);

/*
 * SameIcebergStoredRepresentation - true when oldType and newType are stored
 * identically by the Iceberg create path under `context`.  It reproduces the
 * full stored schema: the unsupported-numeric leaf conversion during
 * derivation, then the compatibility storage mapping over the derived field
 * tree.  This is what an ALTER-time caller (e.g. pg_lake_replication deciding
 * whether an ALTER COLUMN TYPE leaves the changelog schema unchanged) should
 * use.
 */
extern PGDLLEXPORT bool SameIcebergStoredRepresentation(PGType oldType, PGType newType,
														const IcebergCreatePathContext * context);

#endif							/* PG_LAKE_ICEBERG_ICEBERG_REPRESENTATION_H */
