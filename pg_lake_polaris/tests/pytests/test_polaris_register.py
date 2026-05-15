"""
register_catalog / unregister_catalog / bootstrap_existing_tables tests.

These cover the lifecycle of binding a Postgres database to a Polaris
catalog and the one-shot backfill of pre-existing pg_lake tables.
"""

import pytest
from utils_pytest import *

POLARIS_CATALOG_ID = 1000
POLARIS_REALM = "POLARIS"


@pytest.fixture(scope="function")
def polaris_no_register(polaris_schema, extension, superuser_conn, app_user):
    """
    Create the extension but do NOT call register_catalog. Lets the test
    drive registration itself.
    """
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_polaris;
        GRANT USAGE ON SCHEMA lake_polaris TO {app_user};
        GRANT SELECT, INSERT, UPDATE, DELETE
            ON ALL TABLES IN SCHEMA lake_polaris TO {app_user};
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA lake_polaris TO {app_user};
        GRANT INSERT, UPDATE, DELETE ON polaris_schema.entities TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        DELETE FROM polaris_schema.entities
            WHERE id NOT IN (0, {POLARIS_CATALOG_ID});
        DROP EXTENSION pg_lake_polaris CASCADE;
        """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_register_unknown_catalog_errors(polaris_no_register, superuser_conn):
    """register_catalog must raise if the named catalog isn't in Polaris."""
    error_raised = False
    try:
        run_command(
            "SELECT lake_polaris.register_catalog('does_not_exist')",
            superuser_conn,
        )
    except Exception as e:
        error_raised = True
        assert 'catalog "does_not_exist" not found' in str(e)
        superuser_conn.rollback()
    assert error_raised


def test_register_pins_catalog_id_and_realm(polaris_no_register, superuser_conn):
    """
    register_catalog records the Polaris catalog_id and the current
    realm_id GUC value into catalog_mapping.
    """
    run_command(
        "SELECT lake_polaris.register_catalog('mycat')",
        superuser_conn,
    )
    superuser_conn.commit()

    rows = run_query(
        """
        SELECT polaris_catalog_name, polaris_catalog_id,
               polaris_realm_id, enabled
        FROM lake_polaris.catalog_mapping
        WHERE pg_database = current_database()
        """,
        superuser_conn,
    )
    assert rows == [["mycat", POLARIS_CATALOG_ID, POLARIS_REALM, True]]


def test_unregister_disables_outbound(polaris_extension, superuser_conn):
    """
    After unregister_catalog, INSERT into tables_internal must NOT
    produce a Polaris entity. Re-registering re-enables.
    """
    run_command("CREATE TABLE public.unreg_t (a int)", superuser_conn)
    superuser_conn.commit()

    try:
        run_command("SELECT lake_polaris.unregister_catalog()", superuser_conn)
        superuser_conn.commit()

        run_command(
            """
            INSERT INTO lake_iceberg.tables_internal
                (table_name, metadata_location, has_custom_location)
            VALUES
                ('public.unreg_t'::regclass,
                 's3://localbucket/unreg_t/metadata/v1.metadata.json',
                 true);
            """,
            superuser_conn,
        )
        superuser_conn.commit()

        link_count = run_query(
            "SELECT count(*) FROM lake_polaris.entity_link",
            superuser_conn,
        )[0][0]
        assert link_count == 0

        # re-register and run the bootstrap to pick the table up
        run_command(
            "SELECT lake_polaris.register_catalog('mycat')",
            superuser_conn,
        )
        superuser_conn.commit()

        link_count = run_query(
            "SELECT count(*) FROM lake_polaris.entity_link",
            superuser_conn,
        )[0][0]
        assert link_count == 1
    finally:
        run_command(
            """
            DELETE FROM lake_iceberg.tables_internal
             WHERE table_name = 'public.unreg_t'::regclass;
            DROP TABLE IF EXISTS public.unreg_t;
            """,
            superuser_conn,
        )
        superuser_conn.commit()


def test_bootstrap_backfills_preexisting_tables(polaris_no_register, superuser_conn):
    """
    register_catalog calls bootstrap_existing_tables() which must mirror
    every pre-existing tables_internal row with a metadata_location into
    Polaris.
    """
    run_command(
        """
        CREATE TABLE public.bs1 (a int);
        CREATE TABLE public.bs2 (a int);
        INSERT INTO lake_iceberg.tables_internal
            (table_name, metadata_location, has_custom_location)
        VALUES
            ('public.bs1'::regclass,
             's3://localbucket/bs1/metadata/v1.metadata.json', true),
            ('public.bs2'::regclass,
             's3://localbucket/bs2/metadata/v1.metadata.json', true);
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    try:
        run_command(
            "SELECT lake_polaris.register_catalog('mycat')",
            superuser_conn,
        )
        superuser_conn.commit()

        names = sorted(
            row[0]
            for row in run_query(
                f"""
                SELECT name FROM polaris_schema.entities
                WHERE catalog_id = {POLARIS_CATALOG_ID}
                  AND type_code = 7
                """,
                superuser_conn,
            )
        )
        assert names == ["bs1", "bs2"]

        link_count = run_query(
            "SELECT count(*) FROM lake_polaris.entity_link",
            superuser_conn,
        )[0][0]
        assert link_count == 2
    finally:
        run_command(
            """
            DELETE FROM lake_iceberg.tables_internal
             WHERE table_name IN
                ('public.bs1'::regclass, 'public.bs2'::regclass);
            DROP TABLE IF EXISTS public.bs1;
            DROP TABLE IF EXISTS public.bs2;
            """,
            superuser_conn,
        )
        superuser_conn.commit()
