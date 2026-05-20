"""Unit tests for the :c:type:`IcebergFormatVersion` enum + predicates.

The C implementation lives in
``pg_lake_iceberg/src/iceberg/format_version.{h,c}`` and is exposed to
SQL by two test-only UDFs declared in
``pg_lake_iceberg/src/test/test_format_version.c``::

    lake_iceberg.iceberg_format_version_name(version_int int) -> text
    lake_iceberg.iceberg_format_version_supports(version_int int, feature text)
        -> bool

These wrappers take the **wire integer** (not the C enum) so this test
file can also exercise the int → enum boundary, including the error
path for unknown versions.

Coverage map (every assertion below maps to exactly one C call site):

1. ``iceberg_format_version_name`` covers
   :c:func:`IcebergFormatVersionFromInt` (happy + error paths) and
   :c:func:`IcebergFormatVersionName` (every enum value).

2. ``iceberg_format_version_supports`` covers each of the 11 capability
   predicates declared in ``format_version.h`` against {V2, V3}. v1 is
   not a supported enum value so it is exercised only as an error.

3. An unknown feature string raises (typo-in-test detection).
"""

import pytest
from utils_pytest import *

# All capability predicates exported from format_version.h. Keep this in
# sync with the header — if a predicate is added/removed there, this list
# changes and every test that uses it gets exercised across all versions.
ALL_FEATURES = [
    "column_defaults",
    "row_lineage",
    "deletion_vectors",
    "nanosecond_timestamp",
    "variant_type",
    "unknown_type",
    "geospatial_types",
    "multi_arg_transforms",
    "encryption",
    "null_current_snapshot_id",
    "ignore_unknown_transforms",
]


# ---------------------------------------------------------------------------
# Name mapping (covers IcebergFormatVersionFromInt + IcebergFormatVersionName)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "version_int,expected_name",
    [(2, "v2"), (3, "v3")],
    ids=["v2", "v3"],
)
def test_iceberg_format_version_name(
    create_format_version_test_functions,
    superuser_conn,
    version_int,
    expected_name,
):
    rows = run_query(
        f"SELECT lake_iceberg.iceberg_format_version_name({version_int})",
        superuser_conn,
    )
    assert rows == [
        [expected_name]
    ], f"format-version {version_int} should map to {expected_name!r}, got {rows!r}"


@pytest.mark.parametrize(
    "version_int",
    [0, 1, 4, 99, -1],
    ids=["zero", "v1-retired", "v4-future", "garbage-99", "negative"],
)
def test_iceberg_format_version_name_errors_on_unsupported(
    create_format_version_test_functions, superuser_conn, version_int
):
    try:
        with pytest.raises(Exception, match="unsupported iceberg format version"):
            run_query(
                f"SELECT lake_iceberg.iceberg_format_version_name({version_int})",
                superuser_conn,
            )
    finally:
        # The shared superuser_conn is now in a failed-tx state; reset so
        # subsequent parametrizations / tests can reuse it without tripping
        # `InFailedSqlTransaction`.
        superuser_conn.rollback()


# ---------------------------------------------------------------------------
# Capability predicates
#
# Every predicate in format_version.h is v3-only today. v2 should return
# false for every feature; v3 should return true for every feature. When
# a future format-version flips this matrix, update both the C header and
# this test simultaneously.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("feature", ALL_FEATURES, ids=ALL_FEATURES)
def test_v2_supports_no_v3_features(
    create_format_version_test_functions, superuser_conn, feature
):
    rows = run_query(
        f"SELECT lake_iceberg.iceberg_format_version_supports(2, '{feature}')",
        superuser_conn,
    )
    assert rows == [[False]], f"v2 must not advertise {feature}, got {rows!r}"


@pytest.mark.parametrize("feature", ALL_FEATURES, ids=ALL_FEATURES)
def test_v3_supports_every_v3_feature(
    create_format_version_test_functions, superuser_conn, feature
):
    rows = run_query(
        f"SELECT lake_iceberg.iceberg_format_version_supports(3, '{feature}')",
        superuser_conn,
    )
    assert rows == [[True]], f"v3 must advertise {feature}, got {rows!r}"


def test_unknown_feature_errors(create_format_version_test_functions, superuser_conn):
    try:
        with pytest.raises(Exception, match="unknown iceberg format-version feature"):
            run_query(
                "SELECT lake_iceberg.iceberg_format_version_supports(3, 'not_a_real_feature')",
                superuser_conn,
            )
    finally:
        superuser_conn.rollback()


def test_supports_errors_on_unsupported_version(
    create_format_version_test_functions, superuser_conn
):
    try:
        with pytest.raises(Exception, match="unsupported iceberg format version"):
            run_query(
                "SELECT lake_iceberg.iceberg_format_version_supports(1, 'row_lineage')",
                superuser_conn,
            )
    finally:
        superuser_conn.rollback()
