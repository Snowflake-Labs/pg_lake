"""
End-to-end tests against real Iceberg metadata files.

These exercise outbound through pg_lake's full CREATE TABLE / INSERT /
DROP path (so real metadata files are written to the storage backend,
the trigger flow on real regclass values), and exercise inbound UPDATE
by pointing the Polaris entity at a metadata file that pg_lake itself
wrote earlier. That simulates an external Polaris-routed commit without
needing a Polaris REST server.
"""

import pytest

from utils_pytest import *


POLARIS_CATALOG_ID = 1000
POLARIS_REALM = "POLARIS"
TYPE_CODE_NAMESPACE = 6
TYPE_CODE_TABLE_LIKE = 7


def test_outbound_e2e_create_and_insert(
    polaris_extension,
    pg_conn,
    superuser_conn,
    with_default_location,
    s3,
):
    """
    Create an Iceberg table through pg_lake's normal DDL path, INSERT
    rows, and verify the Polaris side stays in sync — namespace appears,
    table entity appears with the right metadata-location, entity_version
    bumps once per metadata write.
    """
    tbl = "public.e2e_outbound"

    run_command(f"CREATE TABLE {tbl} (a int, b text) USING iceberg", pg_conn)
    pg_conn.commit()

    try:
        run_command(f"INSERT INTO {tbl} VALUES (1, 'one'), (2, 'two')", pg_conn)
        pg_conn.commit()

        # one namespace + one table entity in Polaris for our catalog
        rows = run_query(
            f"""
            SELECT name, type_code, sub_type_code,
                   (internal_properties::jsonb)->>'metadata-location'
            FROM polaris_schema.entities
            WHERE catalog_id = {POLARIS_CATALOG_ID}
              AND id NOT IN (0, {POLARIS_CATALOG_ID})
            ORDER BY type_code, name
            """,
            superuser_conn,
        )
        assert len(rows) == 2
        assert rows[0][:2] == ["public", TYPE_CODE_NAMESPACE]
        assert rows[1][:3] == ["e2e_outbound", TYPE_CODE_TABLE_LIKE, 2]
        assert rows[1][3].endswith(".metadata.json")

        link = run_query(
            f"""
            SELECT pg_table_oid::text, last_seen_metadata_location
            FROM lake_polaris.entity_link
            WHERE pg_table_oid = '{tbl}'::regclass
            """,
            superuser_conn,
        )
        assert len(link) == 1
        assert link[0][0] == "e2e_outbound"
        assert link[0][1] == rows[1][3]

        v_before = run_query(
            "SELECT entity_version FROM polaris_schema.entities "
            "WHERE name = 'e2e_outbound'",
            superuser_conn,
        )[0][0]

        run_command(f"INSERT INTO {tbl} VALUES (3, 'three')", pg_conn)
        pg_conn.commit()

        v_after = run_query(
            "SELECT entity_version FROM polaris_schema.entities "
            "WHERE name = 'e2e_outbound'",
            superuser_conn,
        )[0][0]
        assert v_after == v_before + 1, (
            f"entity_version went {v_before} -> {v_after}; expected exactly "
            "+1. If +2 or more, the inbound trigger is re-firing on outbound."
        )
    finally:
        run_command(f"DROP TABLE IF EXISTS {tbl}", pg_conn)
        pg_conn.commit()


def test_outbound_drop_removes_entity(
    polaris_extension,
    pg_conn,
    superuser_conn,
    with_default_location,
    s3,
):
    """DROP TABLE on the pg_lake side removes the Polaris entity row."""
    tbl = "public.e2e_drop"
    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    cnt = run_query(
        f"""
        SELECT count(*) FROM polaris_schema.entities
        WHERE catalog_id = {POLARIS_CATALOG_ID}
          AND type_code = {TYPE_CODE_TABLE_LIKE}
          AND name = 'e2e_drop'
        """,
        superuser_conn,
    )[0][0]
    assert cnt == 1

    run_command(f"DROP TABLE {tbl}", pg_conn)
    pg_conn.commit()

    cnt = run_query(
        f"""
        SELECT count(*) FROM polaris_schema.entities
        WHERE catalog_id = {POLARIS_CATALOG_ID}
          AND type_code = {TYPE_CODE_TABLE_LIKE}
          AND name = 'e2e_drop'
        """,
        superuser_conn,
    )[0][0]
    assert cnt == 0

    link_cnt = run_query(
        "SELECT count(*) FROM lake_polaris.entity_link "
        "WHERE last_seen_metadata_location LIKE '%e2e_drop%'",
        superuser_conn,
    )[0][0]
    assert link_cnt == 0


# Note: an end-to-end test of the inbound INSERT path
# (synthesizing a foreign table from a metadata file an external client
# wrote) is intentionally not provided here. Two issues block it under
# v0.1:
#
#   1. There is no way for tests to write a real Iceberg metadata file
#      without going through pg_lake itself, since the e2e harness lacks
#      pyiceberg-core (required for non-identity transforms / writes
#      through PyIceberg's SqlCatalog). PyIceberg's SqlCatalog also
#      can't write to the current database's iceberg_tables (PR #247
#      blocks INSERT for the internal catalog).
#
#   2. Reusing a metadata file pg_lake already wrote leads to
#      pg_lake-internal deletion_queue conflicts when both the original
#      table and the synthesized foreign table reference the same
#      metadata files. That's a pg_lake limitation about two iceberg
#      tables sharing storage, not a bug in the polaris extension.
#
# The dispatch tests in test_polaris_inbound.py validate the trigger
# flow, and the type-translation logic in inbound_sync_insert is small
# enough to read by inspection. A real Polaris+Spark integration test
# is the right place to lock down the full inbound INSERT path; that
# belongs in a separate harness, post-v0.1.


def test_inbound_update_e2e_syncs_data_files(
    polaris_extension,
    pg_conn,
    superuser_conn,
    with_default_location,
    s3,
):
    """
    A Polaris commit that moves the table to different metadata must
    resync pg_lake's data files. Point the entity back at the metadata
    from the first INSERT, as an external rollback would, and expect the
    second INSERT's row to disappear.
    """
    tbl = "public.e2e_inbound"

    run_command(f"CREATE TABLE {tbl} (a int) USING iceberg", pg_conn)
    run_command(f"INSERT INTO {tbl} VALUES (1)", pg_conn)
    pg_conn.commit()

    try:
        first_location = run_query(
            "SELECT last_seen_metadata_location FROM lake_polaris.entity_link "
            f"WHERE pg_table_oid = '{tbl}'::regclass",
            superuser_conn,
        )[0][0]

        run_command(f"INSERT INTO {tbl} VALUES (2)", pg_conn)
        pg_conn.commit()

        version_before = run_query(
            "SELECT entity_version FROM polaris_schema.entities "
            "WHERE name = 'e2e_inbound'",
            superuser_conn,
        )[0][0]

        run_command(
            f"""
            UPDATE polaris_schema.entities
               SET internal_properties = jsonb_set(
                       internal_properties, '{{metadata-location}}',
                       to_jsonb('{first_location}'::text)),
                   entity_version = entity_version + 1
             WHERE name = 'e2e_inbound'
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        assert run_query(f"SELECT a FROM {tbl} ORDER BY a", pg_conn) == [[1]]

        catalog_location = run_query(
            "SELECT metadata_location FROM lake_iceberg.tables_internal "
            f"WHERE table_name = '{tbl}'::regclass",
            superuser_conn,
        )[0][0]
        assert catalog_location == first_location

        # applying the commit must not echo a write back out to Polaris
        version_after = run_query(
            "SELECT entity_version FROM polaris_schema.entities "
            "WHERE name = 'e2e_inbound'",
            superuser_conn,
        )[0][0]
        assert version_after == version_before + 1
    finally:
        pg_conn.rollback()
        run_command(f"DROP TABLE IF EXISTS {tbl}", pg_conn)
        pg_conn.commit()
