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

#ifndef PG_LAKE_ICEBERG_ICEBERG_REPRESENTATION_H
#define PG_LAKE_ICEBERG_ICEBERG_REPRESENTATION_H

#include "postgres.h"

#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/pgduck/compatibility_mode.h"

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
 * DeriveIcebergStoredField - the Iceberg field the create path stores for
 * `type` under `context`: the unsupported-numeric leaf conversion applied
 * during derivation, then the compatibility storage mapping over the derived
 * tree.  Returns NULL when the type has no faithful stored form (an unsupported
 * numeric with the GUC off), so a comparison can never over-allow.
 *
 * This is the single-side primitive.  A caller that already holds the
 * actually-stored field for a column -- e.g. from field_id_mappings of an
 * existing Iceberg table -- should derive only the candidate type here and
 * compare it against that persisted field with IcebergFieldsEquivalent.  That
 * is immune to the GUC/derivation drift a two-sided re-derivation has, because
 * only the new side is recomputed.
 */
extern PGDLLEXPORT Field * DeriveIcebergStoredField(PGType type,
													const IcebergCreatePathContext * context);

/*
 * IcebergFieldsEquivalent - true when two derived Iceberg field trees are the
 * same stored representation, ignoring field ids and defaults (which are
 * per-derivation).  varchar length / text-family members collapse to `string`,
 * smallint and integer to `int`, and so on.
 */
extern PGDLLEXPORT bool IcebergFieldsEquivalent(Field * a, Field * b);

/*
 * SameIcebergStoredRepresentation - true when oldType and newType are stored
 * identically by the Iceberg create path under `context`.  A convenience over
 * DeriveIcebergStoredField + IcebergFieldsEquivalent that re-derives both
 * sides; prefer comparing against a persisted field (see
 * DeriveIcebergStoredField) when one is available.
 */
extern PGDLLEXPORT bool SameIcebergStoredRepresentation(PGType oldType, PGType newType,
														const IcebergCreatePathContext * context);

#endif							/* PG_LAKE_ICEBERG_ICEBERG_REPRESENTATION_H */
