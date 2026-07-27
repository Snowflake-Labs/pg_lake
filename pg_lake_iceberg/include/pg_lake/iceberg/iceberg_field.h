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
 * SameIcebergRepresentation - Return true when two Postgres column types map to
 * the same Iceberg representation, i.e. PostgresTypeToIcebergField derives an
 * identical Iceberg type for both (ignoring field ids and default values).
 *
 * This is true for pairs that differ only in a way Iceberg does not model:
 * varchar length changes and text/varchar/char (all Iceberg `string`),
 * smallint vs integer (both `int`), time vs timetz (both `time`), bytea vs
 * geometry (both `binary`), and so on.  A top-level unsupported numeric is
 * normalized the way the create path stores it (double when
 * pg_lake_iceberg.unsupported_numeric_as_double is on, otherwise the "string"
 * fallback), so a large numeric is not mistaken for text.
 *
 * A caller can use this to decide whether an ALTER COLUMN ... TYPE leaves the
 * stored Iceberg schema unchanged.  It does not decide whether such a change is
 * otherwise permitted (casts, USING clauses, engine policy) -- that is the
 * caller's concern.
 */
extern PGDLLEXPORT bool SameIcebergRepresentation(Oid oldTypeOid,
												  int32 oldTypeMod,
												  Oid newTypeOid,
												  int32 newTypeMod);
extern PGDLLEXPORT void EnsureIcebergField(Field * field);
extern PGDLLEXPORT const char *IcebergTypeNameToDuckdbTypeName(const char *icebergTypeName);
extern PGDLLEXPORT DataFileSchema * CreatePositionDeleteDataFileSchema(void);
extern PGDLLEXPORT const char *GetIcebergJsonSerializedDefaultExpr(TupleDesc tupdesc, AttrNumber attnum,
																   FieldStructElement * structElementField);
