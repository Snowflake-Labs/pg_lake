"""
Regression test for JSON escaping in REST catalog request bodies.

Pre-fix, ``IdentifierJson()`` (in track_iceberg_metadata_changes.c) and the
``manifest-list`` field in ``GetAddSnapshotCatalogRequest()`` interpolated
raw schema / table / path bytes into the REST catalog commit body via
``%s`` without JSON-escaping. A user with CREATE-on-schema could pick a
name containing ``"`` or ``\\``, on commit pg_lake would emit malformed or
attacker-influenced JSON to the REST catalog, and the request would either
be rejected or silently mutated.

This test creates a writable REST-catalog iceberg table using a PostgreSQL
schema and table whose names contain JSON meta-characters. INSERT triggers
the add-snapshot REST request, exercising both fix sites end-to-end. With
the fix in place the request body is well-formed JSON and the round-trip
succeeds; without the fix the COMMIT fails on malformed catalog payload.
"""

from utils_pytest import *
from helpers.polaris import *


def test_rest_catalog_json_escape_hostile_identifier(
    pg_conn,
    s3,
    polaris_session,
    set_polaris_gucs,
    with_default_location,
    installcheck,
    create_http_helper_functions,
):
    if installcheck:
        return

    # Backslash is a JSON meta-character that is valid in PostgreSQL
    # quoted identifiers and S3 object keys. Pre-fix, this byte would
    # land unescaped in the REST commit body and break JSON parsing.
    # The Polaris namespace validator rejects backslashes, so we put
    # the hostile bytes only in the *table* name -- which still flows
    # through IdentifierJson() and appears literally in the
    # auto-generated manifest-list S3 path embedded in the
    # add-snapshot request body.
    schema = "json_escape_sc"
    quoted_table = r'"evil\name"'

    run_command(f"CREATE SCHEMA {schema}", pg_conn)
    pg_conn.commit()
    try:
        run_command(
            f"CREATE TABLE {schema}.{quoted_table} "
            f"USING iceberg WITH (catalog='REST') AS SELECT 1 AS a",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            f"INSERT INTO {schema}.{quoted_table} VALUES (2), (3)",
            pg_conn,
        )
        pg_conn.commit()

        rows = run_query(
            f"SELECT a FROM {schema}.{quoted_table} ORDER BY a",
            pg_conn,
        )
        assert [r[0] for r in rows] == [1, 2, 3]
    finally:
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()
