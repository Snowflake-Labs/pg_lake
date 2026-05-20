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
 * iceberg/format_version.c - IcebergFormatVersion enum implementation.
 *
 * Companion to `iceberg/format_version.h`. The capability predicates are
 * deliberately one-liners; the cost of typing them out is the price of
 * grep-ability ("which callers branch on row-lineage support?") and of
 * having one well-named location to flip when a v4 spec lands.
 */

#include "postgres.h"

#include "pg_lake/iceberg/format_version.h"


IcebergFormatVersion
IcebergFormatVersionFromInt(int32_t version)
{
	switch (version)
	{
		case 2:
			return ICEBERG_FORMAT_VERSION_V2;
		case 3:
			return ICEBERG_FORMAT_VERSION_V3;
		default:
			ereport(ERROR,
					(errmsg("unsupported iceberg format version %d", version),
					 errhint("pg_lake supports Iceberg format-version 2 and 3.")));
	}

	pg_unreachable();
}


int32_t
IcebergFormatVersionToInt(IcebergFormatVersion v)
{
	return (int32_t) v;
}


const char *
IcebergFormatVersionName(IcebergFormatVersion v)
{
	switch (v)
	{
		case ICEBERG_FORMAT_VERSION_V2:
			return "v2";
		case ICEBERG_FORMAT_VERSION_V3:
			return "v3";
	}

	/*
	 * Unreachable: enum exhaustiveness is enforced by -Wswitch when this file
	 * is compiled, and IcebergFormatVersionFromInt() is the only sanctioned
	 * way to obtain a value. The default branch exists so the compiler does
	 * not complain about a missing return on platforms with stricter
	 * analyses.
	 */
	return "<invalid>";
}


void
EnsureIcebergFormatVersionForRead(IcebergFormatVersion v)
{
	/*
	 * Every value listed in the enum is by construction structurally
	 * readable: callers reached the enum via IcebergFormatVersionFromInt,
	 * which errored on anything we cannot parse. Feature-level gating
	 * (deletion vectors, new types, ...) is the job of the per-feature
	 * predicates at their own choke points.
	 */
	(void) v;
}


void
EnsureIcebergFormatVersionForWrite(IcebergFormatVersion v)
{
	if (v == ICEBERG_FORMAT_VERSION_V3)
		ereport(ERROR,
				(errmsg("writing Iceberg format-version 3 tables is not yet supported"),
				 errhint("Use format-version 2 (the current default) until v3 write paths land.")));
}


/* ------------------------------------------------------------------------
 * Capability predicates
 *
 * Predicates are written as `v >= ICEBERG_FORMAT_VERSION_V3` rather than
 * `v == ICEBERG_FORMAT_VERSION_V3` so a future v4 inherits v3 features
 * by default. When a feature is removed or replaced in v4, the matching
 * predicate becomes `v == ICEBERG_FORMAT_VERSION_V3` at that site.
 * ----------------------------------------------------------------------*/

bool
IcebergFormatVersionSupportsColumnDefaults(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsRowLineage(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsDeletionVectors(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsNanoTimestamp(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsVariantType(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsUnknownType(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsGeospatialTypes(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsMultiArgTransforms(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsEncryption(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionSupportsNullCurrentSnapshotId(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}


bool
IcebergFormatVersionIgnoresUnknownTransforms(IcebergFormatVersion v)
{
	return v >= ICEBERG_FORMAT_VERSION_V3;
}
