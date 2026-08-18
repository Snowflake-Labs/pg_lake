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
 * iceberg_representation.h
 *	 Primitives for answering "what does the Iceberg create path store for this
 *	 Postgres type, and does it match what a table already stores?"
 *
 * A consumer that must decide whether a type change is invisible to an existing
 * Iceberg table (e.g. ALTER COLUMN ... TYPE) derives the stored field for the
 * NEW type only, and compares it against the field the table already persists
 * in lake_table.field_id_mappings.  Deriving BOTH sides answers a different and
 * weaker question -- "would a fresh create of old and new agree under today's
 * settings" -- and fails in the unsafe direction: a transform this module does
 * not model cancels out on both sides, yielding a spurious "equal".  Against a
 * persisted field the same gap makes the two differ, so the caller blocks.
 *
 * That matters because the transform set is not closed.  pg_lake shapes storage
 * with the unsupported-numeric rewrite and the compatibility mapping, but a
 * caller may add its own before the table is created.  Only the persisted field
 * reflects all of them.
 */

#pragma once

#include "postgres.h"

#include "pg_lake/iceberg/compatibility_mode.h"
#include "pg_lake/iceberg/iceberg_field.h"
#include "pg_lake/pgduck/type.h"

/*
 * IcebergStoredPostgresType - the Postgres type the create path actually stores
 * for a declared column type, i.e. the declared type after the unsupported
 * numeric -> float8 rewrite, gated on pg_lake_iceberg.unsupported_numeric_as_double
 * exactly as MaybeConvertUnsupportedNumericColumnsToDouble gates it.  Returns
 * `type` unchanged when nothing is rewritten (including when the GUC is off, in
 * which case CREATE rejects such a column outright rather than storing it).
 *
 * Only a numeric column and an array of numeric are rewritten.  A leaf below a
 * composite type or a map is left as it is, exactly as the create path leaves
 * it, because rewriting it would mean declaring a composite or map type the
 * user never wrote.  Such a leaf is stored as an Iceberg double instead, which
 * TypeHasUnrepresentableLeaf(type, true) reports on.
 */
extern PGDLLEXPORT PGType IcebergStoredPostgresType(PGType type);

/*
 * IcebergStorageFieldForColumnType - the Iceberg field tree the create path
 * stores for a declared column type under a table's compatibility mode: the
 * structural derivation followed by the compatibility storage mapping.
 *
 * This IS the create path's shaping, not a model of it: registration
 * (CreatePostgresColumnMappingsForColumnDefs) calls this, so a comparison
 * against a persisted field cannot drift from what produced that field.
 *
 * `declaredType` is expected to have been through IcebergStoredPostgresType
 * already, as the create path's callers have.  `surfaceFieldOut`, when non-NULL,
 * receives the pre-mapping tree, which registration needs to record the
 * per-leaf surface->storage divergences.
 */
extern PGDLLEXPORT Field * IcebergStorageFieldForColumnType(
															PGType declaredType, IcebergCompatibilityMode mode, bool forAddColumn,
															int *subFieldIndex, Field * *surfaceFieldOut);

/*
 * IcebergFieldsEquivalent - true when two Iceberg field trees are the same
 * stored representation, ignoring field ids and defaults (assigned per
 * derivation, irrelevant to storage).  So varchar length / text-family members
 * collapse to `string`, smallint and integer to `int`, and so on.
 *
 * Tolerates NULL on either side (NULL equals only NULL), so a caller can pass a
 * persisted field it could not resolve without a separate check.
 */
extern PGDLLEXPORT bool IcebergFieldsEquivalent(Field * a, Field * b);

/*
 * TypeHasUnrepresentableLeaf - true when the type tree has a leaf Iceberg
 * cannot hold natively (currently: a numeric that is unbounded or whose
 * precision/scale exceeds DUCKDB_MAX_NUMERIC_PRECISION).  `nestedOnly`
 * restricts the answer to leaves below the top level, array elements included.
 * Of those, an array element is rewritten to float8, while a leaf below a
 * composite type or a map keeps its declared type and is stored as an Iceberg
 * double.
 *
 * Shares ConvertTypeTree's traversal, so it cannot drift out of coverage from
 * the rewrite it predicts.
 */
extern PGDLLEXPORT bool TypeHasUnrepresentableLeaf(PGType type,
												   bool nestedOnly);
