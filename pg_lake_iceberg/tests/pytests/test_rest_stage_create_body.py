"""
Stage 6 (Iceberg v3 rollout): pin the JSON body shape pg_lake POSTs to the
REST catalog at stage-create.

The body MUST place ``format-version`` inside the ``properties`` map and
encode it as a *string*. That is the only place Iceberg's REST
``CreateTableRequest`` reads it from: Polaris hands ``properties`` to
``TableMetadata.newTableMetadata``, which in turn calls
``PropertyUtil.propertyAsInt(Map<String,String>, "format-version",
DEFAULT_TABLE_FORMAT_VERSION)`` -- i.e. ``Integer.parseInt`` on a string
value inside the map.

Stage 14 of the rollout exercises this end-to-end against Polaris in
``pg_lake_table/tests/pytests/test_polaris_v3_table.py``; this file pins
the contract at the unit-test layer so a future refactor cannot silently
move the field back to the body root (where Jackson drops it as an
unknown top-level key and the table is created at the implicit default
v2) or change its JSON type to integer (which fails or coerces against
``Map<String, String>``).

These tests exercise only the body-construction helper
(``BuildStageCreateBody``); they don't require Polaris to be running.
"""

import json

import pytest

from utils_pytest import *


def _build_body(pg_conn, relation_name: str, format_version: int) -> dict:
    rows = run_query(
        f"""
        SELECT lake_iceberg.build_stage_create_body('{relation_name}', {format_version});
        """,
        pg_conn,
    )
    return json.loads(rows[0][0])


def _assert_format_version_in_properties(body: dict, expected_version: int) -> None:
    """The format-version invariant Stage 6 locks down.

    Asserted as four separate predicates so a regression report points at
    the exact part of the shape that drifted:
      * ``properties`` exists and is a JSON object;
      * ``properties.format-version`` exists;
      * its JSON type is *string* (Iceberg's ``Map<String, String>``);
      * its string value parses back to ``expected_version``.
    And one negative assertion: ``format-version`` MUST NOT also be at
    the top level of the body -- that was the original Stage 6 mistake
    and the reason Polaris silently downgraded every v3 table to v2.
    """
    assert "properties" in body, body
    assert isinstance(body["properties"], dict), body["properties"]
    assert "format-version" in body["properties"], body["properties"]
    fv = body["properties"]["format-version"]
    assert isinstance(fv, str), (
        f"Iceberg's CreateTableRequest deserialises properties as "
        f"Map<String, String>; format-version must be a JSON string, "
        f"got {type(fv).__name__} ({fv!r})"
    )
    assert int(fv) == expected_version, (fv, expected_version)
    assert "format-version" not in body, (
        "format-version at the body root is silently dropped by Jackson "
        "(unknown top-level key on CreateTableRequest), causing Polaris "
        "to fall back to format-version 2. The value MUST live inside "
        "the properties map only."
    )


def test_stage_create_body_v2_uses_properties_format_version(
    pg_conn, create_http_helper_functions
):
    body = _build_body(pg_conn, "my_table", 2)

    # Required top-level keys for the REST stage-create call.
    assert body["name"] == "my_table"
    assert body["stage-create"] == "true"

    # format-version invariants (v2 lane).
    _assert_format_version_in_properties(body, 2)

    # Empty schema is intentional; full schema is sent later in
    # FinishStageRestCatalogIcebergTableCreateRestRequest.
    assert body["schema"]["type"] == "struct"
    assert body["schema"]["fields"] == []


def test_stage_create_body_v3_uses_properties_format_version_3(
    pg_conn, create_http_helper_functions
):
    """Pin the v3 lane that Stage 14 exercises end-to-end via Polaris.

    The body still has to produce a correctly-shaped CreateTableRequest
    even though the writer-side gate currently sits in front of v3
    writes elsewhere in the stack -- the body builder is the seam Stage
    14 flips, so its shape contract must be honoured now.
    """
    body = _build_body(pg_conn, "t", 3)
    _assert_format_version_in_properties(body, 3)


def test_stage_create_body_does_not_pollute_top_level(
    pg_conn, create_http_helper_functions
):
    """A standalone explicit guard against re-introducing the original
    Stage 6 bug. ``format-version`` at the body root is silently dropped
    by Jackson on the Polaris side because it is not a field on the
    Iceberg REST ``CreateTableRequest`` DTO (the only top-level fields
    are ``name``, ``location``, ``schema``, ``partition-spec``,
    ``write-order``, ``properties``, ``stage-create``). Locking this
    here makes the regression message point at the right place even if
    the assertion in ``_assert_format_version_in_properties`` were ever
    softened.
    """
    body = _build_body(pg_conn, "t", 3)
    assert "format-version" not in body, body
    top_level_keys = set(body.keys())
    allowed = {
        "name",
        "location",
        "schema",
        "partition-spec",
        "write-order",
        "properties",
        "stage-create",
    }
    assert top_level_keys <= allowed, (
        f"unexpected top-level keys on CreateTableRequest body: "
        f"{top_level_keys - allowed}; the Iceberg REST DTO only "
        f"recognises {allowed}, anything else is silently dropped."
    )


@pytest.mark.parametrize("bad_version", [0, 1, 4, 99, -1])
def test_stage_create_body_rejects_unknown_versions(
    pg_conn, create_http_helper_functions, bad_version
):
    with pytest.raises(Exception, match="unsupported iceberg format version"):
        run_query(
            f"SELECT lake_iceberg.build_stage_create_body('t', {bad_version});",
            pg_conn,
        )
    pg_conn.rollback()
