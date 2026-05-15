"""
Outbound (pg_lake -> Polaris) sync tests.

These exercise the trigger on lake_iceberg.tables_internal. Rather than
go through pg_lake's CREATE TABLE flow (which requires pgduck_server +
S3), we directly INSERT/UPDATE/DELETE rows in tables_internal — the
trigger fires the same way regardless of how the row arrived.
"""

import pytest
from utils_pytest import *

POLARIS_CATALOG_ID = 1000
POLARIS_REALM = "POLARIS"
TYPE_CODE_NAMESPACE = 6
TYPE_CODE_TABLE_LIKE = 7


def _create_dummy_table(conn, schema, name):
    """Create a regular table to use as a regclass placeholder."""
    run_command(f"CREATE TABLE {schema}.{name} (a int)", conn)


def _drop_dummy_table(conn, schema, name):
    run_command(f"DROP TABLE IF EXISTS {schema}.{name}", conn)


def test_outbound_insert_creates_namespace_and_table(polaris_extension, superuser_conn):
    """
    INSERT into tables_internal must create both a namespace entity (if
    missing) and a table entity in polaris_schema.entities.
    """
    _create_dummy_table(superuser_conn, "public", "out_ins")
    superuser_conn.commit()

    try:
        run_command(
            """
            INSERT INTO lake_iceberg.tables_internal
                (table_name, metadata_location, has_custom_location)
            VALUES
                ('public.out_ins'::regclass,
                 's3://localbucket/out_ins/metadata/v1.metadata.json',
                 true);
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        rows = run_query(
            f"""
            SELECT name, type_code, sub_type_code,
                   internal_properties::jsonb->>'metadata-location'
            FROM polaris_schema.entities
            WHERE catalog_id = {POLARIS_CATALOG_ID}
              AND realm_id = '{POLARIS_REALM}'
              AND id NOT IN (0, {POLARIS_CATALOG_ID})
            ORDER BY type_code, name;
            """,
            superuser_conn,
        )
        # one namespace + one table
        assert len(rows) == 2
        assert rows[0][:2] == ["public", TYPE_CODE_NAMESPACE]
        assert rows[1][:3] == ["out_ins", TYPE_CODE_TABLE_LIKE, 2]
        assert rows[1][3] == "s3://localbucket/out_ins/metadata/v1.metadata.json"

        link = run_query(
            """
            SELECT pg_table_oid::text, last_seen_metadata_location
            FROM lake_polaris.entity_link;
            """,
            superuser_conn,
        )
        assert link == [
            [
                "out_ins",
                "s3://localbucket/out_ins/metadata/v1.metadata.json",
            ]
        ]
    finally:
        run_command(
            "DELETE FROM lake_iceberg.tables_internal "
            "WHERE table_name = 'public.out_ins'::regclass",
            superuser_conn,
        )
        _drop_dummy_table(superuser_conn, "public", "out_ins")
        superuser_conn.commit()


def test_outbound_update_bumps_entity_version(polaris_extension, superuser_conn):
    """
    Updating metadata_location must increment entity_version exactly once
    per outbound write — regression check for the round-trip loop.
    """
    _create_dummy_table(superuser_conn, "public", "out_upd")
    superuser_conn.commit()

    try:
        run_command(
            """
            INSERT INTO lake_iceberg.tables_internal
                (table_name, metadata_location, has_custom_location)
            VALUES
                ('public.out_upd'::regclass,
                 's3://localbucket/out_upd/metadata/v1.metadata.json',
                 true);
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        v1 = run_query(
            "SELECT entity_version FROM polaris_schema.entities "
            "WHERE name = 'out_upd'",
            superuser_conn,
        )[0][0]
        assert v1 == 1

        run_command(
            """
            UPDATE lake_iceberg.tables_internal
               SET metadata_location =
                   's3://localbucket/out_upd/metadata/v2.metadata.json'
             WHERE table_name = 'public.out_upd'::regclass;
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        row = run_query(
            "SELECT entity_version, "
            "       internal_properties::jsonb->>'metadata-location' "
            "FROM polaris_schema.entities WHERE name = 'out_upd'",
            superuser_conn,
        )[0]
        assert row == [
            2,
            "s3://localbucket/out_upd/metadata/v2.metadata.json",
        ]
    finally:
        run_command(
            "DELETE FROM lake_iceberg.tables_internal "
            "WHERE table_name = 'public.out_upd'::regclass",
            superuser_conn,
        )
        _drop_dummy_table(superuser_conn, "public", "out_upd")
        superuser_conn.commit()


def test_outbound_delete_removes_entity_keeps_namespace(
    polaris_extension, superuser_conn
):
    """
    DELETE on tables_internal removes the table entity and entity_link
    row, but leaves the namespace entity in place — namespaces may host
    other tables out-of-band.
    """
    _create_dummy_table(superuser_conn, "public", "out_del")
    superuser_conn.commit()

    try:
        run_command(
            """
            INSERT INTO lake_iceberg.tables_internal
                (table_name, metadata_location, has_custom_location)
            VALUES
                ('public.out_del'::regclass,
                 's3://localbucket/out_del/metadata/v1.metadata.json',
                 true);
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        run_command(
            """
            DELETE FROM lake_iceberg.tables_internal
             WHERE table_name = 'public.out_del'::regclass;
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        # link gone
        link_count = run_query(
            "SELECT count(*) FROM lake_polaris.entity_link", superuser_conn
        )[0][0]
        assert link_count == 0

        # table entity gone
        table_count = run_query(
            f"""
            SELECT count(*) FROM polaris_schema.entities
            WHERE catalog_id = {POLARIS_CATALOG_ID}
              AND type_code = {TYPE_CODE_TABLE_LIKE}
              AND id NOT IN (0, {POLARIS_CATALOG_ID});
            """,
            superuser_conn,
        )[0][0]
        assert table_count == 0

        # namespace entity stays
        ns_count = run_query(
            f"""
            SELECT count(*) FROM polaris_schema.entities
            WHERE catalog_id = {POLARIS_CATALOG_ID}
              AND type_code = {TYPE_CODE_NAMESPACE};
            """,
            superuser_conn,
        )[0][0]
        assert ns_count == 1
    finally:
        _drop_dummy_table(superuser_conn, "public", "out_del")
        superuser_conn.commit()


def test_outbound_no_metadata_location_is_skipped(polaris_extension, superuser_conn):
    """
    Inserting a tables_internal row with NULL metadata_location (the
    state right after CREATE TABLE before the first metadata write) must
    not produce a Polaris entity yet — the trigger waits for the first
    metadata write.
    """
    _create_dummy_table(superuser_conn, "public", "out_null")
    superuser_conn.commit()

    try:
        run_command(
            """
            INSERT INTO lake_iceberg.tables_internal
                (table_name, has_custom_location)
            VALUES
                ('public.out_null'::regclass, true);
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        link_count = run_query(
            "SELECT count(*) FROM lake_polaris.entity_link", superuser_conn
        )[0][0]
        assert link_count == 0

        entity_count = run_query(
            f"""
            SELECT count(*) FROM polaris_schema.entities
            WHERE catalog_id = {POLARIS_CATALOG_ID}
              AND id NOT IN (0, {POLARIS_CATALOG_ID})
              AND type_code = {TYPE_CODE_TABLE_LIKE};
            """,
            superuser_conn,
        )[0][0]
        assert entity_count == 0

        # Now write a metadata location — should fire outbound
        run_command(
            """
            UPDATE lake_iceberg.tables_internal
               SET metadata_location =
                   's3://localbucket/out_null/metadata/v1.metadata.json'
             WHERE table_name = 'public.out_null'::regclass;
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        link_count = run_query(
            "SELECT count(*) FROM lake_polaris.entity_link", superuser_conn
        )[0][0]
        assert link_count == 1
    finally:
        run_command(
            "DELETE FROM lake_iceberg.tables_internal "
            "WHERE table_name = 'public.out_null'::regclass",
            superuser_conn,
        )
        _drop_dummy_table(superuser_conn, "public", "out_null")
        superuser_conn.commit()
