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
 * test/test_format_version.c - SQL-callable wrappers around the
 * IcebergFormatVersion API, used only by pytest.
 *
 * The functions are exposed via the `create_format_version_test_functions`
 * fixture in `test_common/helpers/iceberg.py` and dropped at teardown.
 * They take the wire integer (not the C enum) so that tests can probe
 * the full int → enum boundary, including the error path for unknown
 * versions.
 */

#include "postgres.h"
#include "fmgr.h"

#include "pg_lake/iceberg/format_version.h"

#include "utils/builtins.h"

#include <string.h>


PG_FUNCTION_INFO_V1(iceberg_format_version_name);
PG_FUNCTION_INFO_V1(iceberg_format_version_supports);


/*
 * lake_iceberg.iceberg_format_version_name(version_int int) RETURNS text
 *
 * Maps the wire integer to its short name ("v2", "v3"). Errors on
 * unknown values via the IcebergFormatVersionFromInt() ereport path.
 *
 * Used by tests to assert that the C enum agrees with what callers
 * expect to receive on the wire.
 */
Datum
iceberg_format_version_name(PG_FUNCTION_ARGS)
{
	int32		raw = PG_GETARG_INT32(0);
	IcebergFormatVersion v = IcebergFormatVersionFromInt((int32_t) raw);

	PG_RETURN_TEXT_P(cstring_to_text(IcebergFormatVersionName(v)));
}


/*
 * lake_iceberg.iceberg_format_version_supports(version_int int,
 *                                              feature text) RETURNS bool
 *
 * Dispatches to the matching capability predicate in format_version.c.
 * The `feature` strings are the spec-aligned names from
 * format_version.h; keeping them as strings lets pytest parametrize
 * across the full grid without generating one UDF per predicate.
 *
 * Unknown feature strings raise so a typo in a test surfaces loudly
 * instead of silently returning false.
 */
Datum
iceberg_format_version_supports(PG_FUNCTION_ARGS)
{
	int32		raw = PG_GETARG_INT32(0);
	text	   *featureText = PG_GETARG_TEXT_P(1);
	char	   *feature = text_to_cstring(featureText);

	IcebergFormatVersion v = IcebergFormatVersionFromInt((int32_t) raw);

	bool		result;

	if (strcmp(feature, "column_defaults") == 0)
		result = IcebergFormatVersionSupportsColumnDefaults(v);
	else if (strcmp(feature, "row_lineage") == 0)
		result = IcebergFormatVersionSupportsRowLineage(v);
	else if (strcmp(feature, "deletion_vectors") == 0)
		result = IcebergFormatVersionSupportsDeletionVectors(v);
	else if (strcmp(feature, "nanosecond_timestamp") == 0)
		result = IcebergFormatVersionSupportsNanoTimestamp(v);
	else if (strcmp(feature, "variant_type") == 0)
		result = IcebergFormatVersionSupportsVariantType(v);
	else if (strcmp(feature, "unknown_type") == 0)
		result = IcebergFormatVersionSupportsUnknownType(v);
	else if (strcmp(feature, "geospatial_types") == 0)
		result = IcebergFormatVersionSupportsGeospatialTypes(v);
	else if (strcmp(feature, "multi_arg_transforms") == 0)
		result = IcebergFormatVersionSupportsMultiArgTransforms(v);
	else if (strcmp(feature, "encryption") == 0)
		result = IcebergFormatVersionSupportsEncryption(v);
	else if (strcmp(feature, "null_current_snapshot_id") == 0)
		result = IcebergFormatVersionSupportsNullCurrentSnapshotId(v);
	else if (strcmp(feature, "ignore_unknown_transforms") == 0)
		result = IcebergFormatVersionIgnoresUnknownTransforms(v);
	else
		ereport(ERROR,
				(errmsg("unknown iceberg format-version feature: %s", feature),
				 errhint("Feature must be one of: column_defaults, row_lineage, "
						 "deletion_vectors, nanosecond_timestamp, variant_type, "
						 "unknown_type, geospatial_types, multi_arg_transforms, "
						 "encryption, null_current_snapshot_id, "
						 "ignore_unknown_transforms.")));

	PG_RETURN_BOOL(result);
}
