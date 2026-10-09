"""
Inbound (Polaris -> pg_lake) sync tests.

These simulate Polaris-side commits by directly modifying
polaris_schema.entities and assert that the pg_lake catalog state catches
up. The INSERT path (synthesizing CREATE FOREIGN TABLE for a table that
appears in Polaris first) requires reading a real Iceberg metadata file,
so the deeper coverage there belongs in an e2e test against PyIceberg —
here we only validate the dispatch path.
"""

import pytest
from utils_pytest import *

POLARIS_CATALOG_ID = 1000
POLARIS_REALM = "POLARIS"


def _seed_namespace(conn, ns_id, name="public"):
    run_command(
        f"""
        INSERT INTO polaris_schema.entities VALUES
            ('{POLARIS_REALM}', {POLARIS_CATALOG_ID}, {ns_id},
             {POLARIS_CATALOG_ID}, '{name}', 1, 6, 0,
             0, 0, 0, 0, 0, '{{}}', '{{}}', 1)
        ON CONFLICT DO NOTHING;
        """,
        conn,
    )


def test_inbound_delete_drops_linked_table(polaris_extension, superuser_conn):
    """
    Deleting a Polaris-side table entity must DROP the corresponding
    pg_lake table and remove the entity_link row.
    """
    run_command("CREATE TABLE public.in_del (a int)", superuser_conn)
    superuser_conn.commit()

    _seed_namespace(superuser_conn, ns_id=2000)

    run_command(
        f"""
        INSERT INTO lake_polaris.entity_link
            (pg_table_oid, polaris_entity_id, polaris_namespace_id,
             last_seen_metadata_location)
        VALUES
            ('public.in_del'::regclass, 4001, 2000,
             's3://localbucket/in_del/metadata/v1.metadata.json');

        INSERT INTO polaris_schema.entities VALUES
            ('{POLARIS_REALM}', {POLARIS_CATALOG_ID}, 4001, 2000,
             'in_del', 1, 7, 2,
             0, 0, 0, 0, 0, '{{}}', '{{}}', 1);
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    # simulate Polaris-side DELETE
    run_command(
        "DELETE FROM polaris_schema.entities WHERE id = 4001",
        superuser_conn,
    )
    superuser_conn.commit()

    link_count = run_query(
        "SELECT count(*) FROM lake_polaris.entity_link",
        superuser_conn,
    )[0][0]
    assert link_count == 0

    rows = run_query(
        "SELECT count(*) FROM pg_class WHERE relname = 'in_del'",
        superuser_conn,
    )
    assert rows[0][0] == 0


def test_inbound_update_no_change_is_noop(polaris_extension, superuser_conn):
    """
    If an UPDATE on a Polaris entity leaves the metadata-location equal
    to last_seen_metadata_location (e.g., a property-only edit, or our
    own outbound write that round-tripped), the inbound trigger must NOT
    re-call sync_iceberg_metadata_from_external_write — that would be a
    no-op at best and a partial loop at worst.
    """
    run_command("CREATE TABLE public.in_noop (a int)", superuser_conn)
    superuser_conn.commit()

    _seed_namespace(superuser_conn, ns_id=2100)

    run_command(
        f"""
        INSERT INTO lake_polaris.entity_link
            (pg_table_oid, polaris_entity_id, polaris_namespace_id,
             last_seen_metadata_location)
        VALUES
            ('public.in_noop'::regclass, 4100, 2100,
             's3://localbucket/in_noop/metadata/v1.metadata.json');

        INSERT INTO polaris_schema.entities VALUES
            ('{POLARIS_REALM}', {POLARIS_CATALOG_ID}, 4100, 2100,
             'in_noop', 1, 7, 2,
             0, 0, 0, 0, 0, '{{}}',
             '{{"parent-namespace":"public",
                "metadata-location":"s3://localbucket/in_noop/metadata/v1.metadata.json"}}',
             1);
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    # Touch the entity but keep the same metadata-location. Should not
    # call sync_iceberg_metadata_from_external_write (which would error
    # because the metadata file doesn't actually exist).
    run_command(
        """
        UPDATE polaris_schema.entities
           SET entity_version = entity_version + 1
         WHERE id = 4100;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    last_seen = run_query(
        "SELECT last_seen_metadata_location FROM lake_polaris.entity_link "
        "WHERE polaris_entity_id = 4100",
        superuser_conn,
    )[0][0]
    assert last_seen == "s3://localbucket/in_noop/metadata/v1.metadata.json"


def test_inbound_insert_without_metadata_location_is_skipped(
    polaris_extension, superuser_conn
):
    """
    A new TABLE_LIKE entity with no `metadata-location` in
    internal_properties must not produce a foreign table — Polaris hasn't
    finished registering the table yet.
    """
    _seed_namespace(superuser_conn, ns_id=2200)

    run_command(
        f"""
        INSERT INTO polaris_schema.entities VALUES
            ('{POLARIS_REALM}', {POLARIS_CATALOG_ID}, 4200, 2200,
             'in_pending', 1, 7, 2,
             0, 0, 0, 0, 0, '{{}}', '{{}}', 1);
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    rows = run_query(
        """
        SELECT count(*) FROM pg_class WHERE relname = 'in_pending';
        """,
        superuser_conn,
    )
    assert rows[0][0] == 0

    link_count = run_query(
        "SELECT count(*) FROM lake_polaris.entity_link",
        superuser_conn,
    )[0][0]
    assert link_count == 0


def test_outbound_inbound_no_loop(polaris_extension, superuser_conn):
    """
    A pg_lake-originated metadata change must be reflected in Polaris
    exactly once — entity_version goes 1 -> 2, not 1 -> 3 — proving the
    outbound write doesn't re-enter via the inbound trigger.
    """
    run_command("CREATE TABLE public.in_loop (a int)", superuser_conn)
    superuser_conn.commit()

    try:
        run_command(
            """
            INSERT INTO lake_iceberg.tables_internal
                (table_name, metadata_location, has_custom_location)
            VALUES
                ('public.in_loop'::regclass,
                 's3://localbucket/in_loop/metadata/v1.metadata.json',
                 true);
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        v1 = run_query(
            "SELECT entity_version FROM polaris_schema.entities "
            "WHERE name = 'in_loop'",
            superuser_conn,
        )[0][0]
        assert v1 == 1

        run_command(
            """
            UPDATE lake_iceberg.tables_internal
               SET metadata_location =
                   's3://localbucket/in_loop/metadata/v2.metadata.json'
             WHERE table_name = 'public.in_loop'::regclass;
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        v2 = run_query(
            "SELECT entity_version FROM polaris_schema.entities "
            "WHERE name = 'in_loop'",
            superuser_conn,
        )[0][0]
        assert v2 == 2, (
            f"entity_version went 1 -> {v2}; expected exactly 2. "
            "If higher, the inbound trigger is firing on the outbound "
            "write — the suppress flag isn't being honored."
        )
    finally:
        run_command(
            """
            DELETE FROM lake_iceberg.tables_internal
             WHERE table_name = 'public.in_loop'::regclass;
            DROP TABLE IF EXISTS public.in_loop;
            """,
            superuser_conn,
        )
        superuser_conn.commit()
