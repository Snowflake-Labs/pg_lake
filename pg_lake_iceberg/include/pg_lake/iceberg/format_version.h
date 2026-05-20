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

/*
 * iceberg/format_version.h - Iceberg format-version typing and capability
 * predicates.
 *
 * This header centralises every place pg_lake code wants to know which
 * Iceberg spec revision a table was written under. The plain int32 the
 * spec stores in `metadata.json` carries no static type information; by
 * funnelling all reads and writes of it through `IcebergFormatVersion` we
 * get:
 *
 *   - a compile-time exhaustiveness check on switch statements,
 *   - one source of truth for "which spec revision does feature X
 *     belong to?", and
 *   - a single choke point where reader / writer code can enforce
 *     version bounds before doing version-dependent work.
 *
 * The spec lives at https://iceberg.apache.org/spec/. v1 was retired
 * before pg_lake was written; v2 is the long-standing default; v3 is
 * the in-flight target.
 *
 * This header is callable from anywhere in the extension (writer, reader,
 * DDL, REST catalog, tests). It deliberately has no Iceberg-data-model
 * includes so it can be pulled into the bottom of the dependency graph.
 */

#pragma once

#include "postgres.h"

#include <stdbool.h>
#include <stdint.h>


typedef enum IcebergFormatVersion
{
	ICEBERG_FORMAT_VERSION_V2 = 2,
	ICEBERG_FORMAT_VERSION_V3 = 3,
}			IcebergFormatVersion;


/* ------------------------------------------------------------------------
 * int <-> enum conversion
 *
 * The on-disk wire format always carries plain integers. These helpers
 * are the only sanctioned path between the wire integer and the enum.
 * `IcebergFormatVersionFromInt()` errors via `ereport(ERROR, ...)` on
 * unknown / unsupported integers — callers can rely on a successful
 * return value being a valid enum member.
 * ----------------------------------------------------------------------*/

extern IcebergFormatVersion IcebergFormatVersionFromInt(int32_t version);
extern int32_t IcebergFormatVersionToInt(IcebergFormatVersion v);

/*
 * Lowercase short name suitable for log lines, GUC values, and
 * user-facing error messages: "v2", "v3". Backed by static strings;
 * callers must not free the result.
 */
extern const char *IcebergFormatVersionName(IcebergFormatVersion v);


/* ------------------------------------------------------------------------
 * Read / write enforcement choke points
 *
 * `EnsureIcebergFormatVersionForRead` is called by the reader as it
 * peels open a metadata.json — it errors if the file references a
 * version this build does not know how to read.
 *
 * `EnsureIcebergFormatVersionForWrite` is called by the writer before
 * any version-bound write path commits. While the v3 implementation is
 * in flight it errors for `ICEBERG_FORMAT_VERSION_V3`; later stages of
 * the effort relax this once each missing v3 feature lands.
 *
 * The two enforcement points are separate on purpose: pg_lake can
 * safely read v3 metadata (structurally) long before it can safely
 * write a v3 table. Conflating them would force the choice between
 * "no v3 support" and "tell users we wrote a v3 table when in fact we
 * silently dropped fields".
 *
 * Both helpers are no-callers in this commit. The next commit wires
 * them into the read / write paths.
 * ----------------------------------------------------------------------*/

extern void EnsureIcebergFormatVersionForRead(IcebergFormatVersion v);
extern void EnsureIcebergFormatVersionForWrite(IcebergFormatVersion v);


/* ------------------------------------------------------------------------
 * Capability predicates
 *
 * Each predicate returns `true` if format-version `v` includes the
 * named feature in the Iceberg spec. They are intentionally small: one
 * predicate per spec feature, no compound booleans. Call sites read
 * cleanly ("if (IcebergFormatVersionSupportsRowLineage(...)) ...") and
 * grep cleanly ("find every site that branches on deletion-vector
 * support").
 *
 * The mapping below is the v3 changelog distilled. Keep this set in
 * sync with the spec when a v4 lands; do not delete predicates when a
 * feature graduates — that would require touching every call site.
 * ----------------------------------------------------------------------*/

/* v3 §schema: per-field `initial-default` / `write-default`. */
extern bool IcebergFormatVersionSupportsColumnDefaults(IcebergFormatVersion v);

/*
 * v3 §row-lineage: table `next-row-id`, snapshot `first-row-id` /
 * `added-rows`, data-file `first_row_id`.
 */
extern bool IcebergFormatVersionSupportsRowLineage(IcebergFormatVersion v);

/* v3 §deletes: Puffin-encoded deletion vectors replace v2 PDFs. */
extern bool IcebergFormatVersionSupportsDeletionVectors(IcebergFormatVersion v);

/* v3 §types: `timestamp_ns` / `timestamptz_ns` (nanosecond precision). */
extern bool IcebergFormatVersionSupportsNanoTimestamp(IcebergFormatVersion v);

/* v3 §types: `variant` (self-describing semi-structured). */
extern bool IcebergFormatVersionSupportsVariantType(IcebergFormatVersion v);

/* v3 §types: `unknown` (null-only placeholder). */
extern bool IcebergFormatVersionSupportsUnknownType(IcebergFormatVersion v);

/* v3 §types: `geometry` / `geography`. */
extern bool IcebergFormatVersionSupportsGeospatialTypes(IcebergFormatVersion v);

/* v3 §partitioning / sorting: `source-ids` (plural). */
extern bool IcebergFormatVersionSupportsMultiArgTransforms(IcebergFormatVersion v);

/* v3 §encryption: table `encryption-keys`, snapshot `key-id`. */
extern bool IcebergFormatVersionSupportsEncryption(IcebergFormatVersion v);

/*
 * v3 §metadata: empty tables write JSON `null` for `current-snapshot-id`
 * instead of the v2 sentinel `-1`.
 */
extern bool IcebergFormatVersionSupportsNullCurrentSnapshotId(IcebergFormatVersion v);

/*
 * v3 §evolution: readers MUST ignore unknown partition / sort
 * transforms (forward-compatibility hook); v2 hard-errored.
 */
extern bool IcebergFormatVersionIgnoresUnknownTransforms(IcebergFormatVersion v);
