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

#include "pg_lake/parquet/field.h"
#include "pg_lake/pgduck/type.h"
#include "pg_lake/parquet/leaf_field.h"

extern PGDLLEXPORT PGType IcebergFieldToPostgresType(Field * field);
extern PGDLLEXPORT Field * PostgresTypeToIcebergField(PGType pgType,
													  bool forAddColumn,
													  int *subFieldIndex);

/*
 * A leaf conversion lets iceberg_representation.c reproduce the storage
 * transforms the Iceberg-table create path applies (e.g. an unsupported numeric
 * stored as double) while it derives the in-memory Iceberg field tree, without
 * baking that create-path policy into the structural derivation here.  It is
 * invoked at every scalar leaf.
 *
 * Given a scalar Postgres type, it returns the Postgres type whose Iceberg
 * representation is actually stored there: the input unchanged to leave it
 * alone, or a PGType with an invalid Oid to signal that the create path has no
 * faithful stored representation for this leaf -- derivation then yields a NULL
 * field so a comparison can never over-allow.  It must be free of catalog side
 * effects (it runs while building in-memory Field structs, not real Postgres
 * types).
 */
typedef PGType(*IcebergLeafConversionFn) (PGType leafType, void *context);

/*
 * Like PostgresTypeToIcebergField, but applies `convert` (with `context`) at
 * every scalar leaf as it recurses.  `convert` may be NULL, in which case this
 * behaves exactly like PostgresTypeToIcebergField.  Returns NULL when `convert`
 * reports an unrepresentable leaf.
 */
extern PGDLLEXPORT Field * PostgresTypeToIcebergFieldConverted(PGType pgType,
															   bool forAddColumn,
															   int *subFieldIndex,
															   IcebergLeafConversionFn convert,
															   void *context);
extern PGDLLEXPORT void EnsureIcebergField(Field * field);
extern PGDLLEXPORT const char *IcebergTypeNameToDuckdbTypeName(const char *icebergTypeName);
extern PGDLLEXPORT DataFileSchema * CreatePositionDeleteDataFileSchema(void);
extern PGDLLEXPORT const char *GetIcebergJsonSerializedDefaultExpr(TupleDesc tupdesc, AttrNumber attnum,
																   FieldStructElement * structElementField);
