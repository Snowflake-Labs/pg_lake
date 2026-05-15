"""
Regression tests for JSON escaping in REST catalog request bodies.

The REST catalog request bodies sent by pg_lake_table /
pg_lake_iceberg are constructed by concatenating identifier and path
strings into a JSON document. If those strings contain JSON
meta-characters (`"`, `\\`, control characters), the body must escape
them rather than emitting the raw bytes -- otherwise the request
either becomes malformed JSON or, worse, allows a user with
CREATE-on-schema privileges to inject sibling fields into the commit
body.

These tests exercise the JSON-building helpers directly (via test-only
SQL functions) so they do not require a live REST catalog server.
"""

import json

import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def rest_catalog_json_helpers(superuser_conn, app_user):
    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS test_rest_catalog_json_escape_sc;

        CREATE OR REPLACE FUNCTION
            test_rest_catalog_json_escape_sc.identifier_json(namespace_flat text,
                                                             table_name text)
        RETURNS text
        LANGUAGE C IMMUTABLE STRICT
        AS 'pg_lake_table', $function$test_rest_catalog_identifier_json$function$;

        CREATE OR REPLACE FUNCTION
            test_rest_catalog_json_escape_sc.add_snapshot_body(manifest_list text)
        RETURNS text
        LANGUAGE C IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$test_rest_catalog_add_snapshot_body$function$;

        GRANT USAGE ON SCHEMA test_rest_catalog_json_escape_sc TO {app_user};
        GRANT EXECUTE ON FUNCTION
            test_rest_catalog_json_escape_sc.identifier_json(text, text) TO {app_user};
        GRANT EXECUTE ON FUNCTION
            test_rest_catalog_json_escape_sc.add_snapshot_body(text) TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    yield
    run_command(
        "DROP SCHEMA IF EXISTS test_rest_catalog_json_escape_sc CASCADE",
        superuser_conn,
    )
    superuser_conn.commit()


HOSTILE_INPUTS = [
    pytest.param("plain", "plain", id="plain"),
    pytest.param('quote"middle', "table", id="namespace-double-quote"),
    pytest.param("namespace", 'name"with"quotes', id="table-double-quote"),
    pytest.param("back\\slash", "name", id="namespace-backslash"),
    pytest.param("ns", "tab\tname", id="table-tab"),
    pytest.param("ns", "newline\nname", id="table-newline"),
    pytest.param('a"b\\c', 'd"e\\f', id="quotes-and-backslashes"),
    pytest.param('"; DROP CATALOG; "', '"injected', id="injection-attempt"),
]


@pytest.mark.parametrize("namespace_flat,table_name", HOSTILE_INPUTS)
def test_identifier_json_escapes_hostile_names(
    pg_conn, rest_catalog_json_helpers, namespace_flat, table_name
):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT test_rest_catalog_json_escape_sc.identifier_json(%s, %s)",
        (namespace_flat, table_name),
    )
    body = cur.fetchone()[0]
    cur.close()
    parsed = json.loads(body)
    assert parsed["namespace"] == [
        namespace_flat
    ], f"namespace round-trip failed: {body!r} -> {parsed!r}"
    assert (
        parsed["name"] == table_name
    ), f"name round-trip failed: {body!r} -> {parsed!r}"
    assert set(parsed.keys()) == {
        "namespace",
        "name",
    }, "no extra fields should be injected"


@pytest.mark.parametrize(
    "manifest_list",
    [
        "s3://bucket/snap-1-1-uuid.avro",
        's3://bucket/snap-1-1-uuid.avro?evil=",x":1,"y":"',
        "s3://bucket/path with spaces/snap-1-1-uuid.avro",
        "s3://bucket/back\\slash/snap-1-1-uuid.avro",
        's3://bucket/quote"in"path/snap-1-1-uuid.avro',
    ],
    ids=["clean", "json-injection", "spaces", "backslash", "double-quote"],
)
def test_add_snapshot_body_escapes_manifest_list(
    pg_conn, rest_catalog_json_helpers, manifest_list
):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT test_rest_catalog_json_escape_sc.add_snapshot_body(%s)",
        (manifest_list,),
    )
    body = cur.fetchone()[0]
    cur.close()
    # Body is two top-level objects separated by ", " (REST batch entries);
    # wrap them in an array to make it JSON-parseable.
    parsed = json.loads(f"[{body}]")
    add_snapshot = parsed[0]
    assert add_snapshot["action"] == "add-snapshot"
    assert (
        add_snapshot["snapshot"]["manifest-list"] == manifest_list
    ), f"manifest-list round-trip failed: {body!r}"
