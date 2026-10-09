import psycopg2
import pytest
from utils_pytest import *


POLARIS_REALM = "POLARIS"
POLARIS_CATALOG_NAME = "mycat"
POLARIS_CATALOG_ID = 1000


@pytest.fixture(scope="module")
def polaris_schema(superuser_conn):
    """
    Create a minimal port of Apache Polaris's relational-jdbc schema. Just
    enough to exercise the lake_polaris triggers. Mirrors
    test_common/rest_catalog/polaris/persistence/relational-jdbc/src/main/
    resources/h2/schema-v1.sql, ported to Postgres syntax.

    The schema is module-scoped so multiple tests can share the catalog
    row; each test cleans up its own table/namespace entities.
    """
    run_command(
        """
        CREATE SCHEMA IF NOT EXISTS polaris_schema;

        CREATE TABLE IF NOT EXISTS polaris_schema.entities (
            realm_id text NOT NULL,
            catalog_id bigint NOT NULL,
            id bigint NOT NULL,
            parent_id bigint NOT NULL,
            name text NOT NULL,
            entity_version int NOT NULL,
            type_code int NOT NULL,
            sub_type_code int NOT NULL,
            create_timestamp bigint NOT NULL,
            drop_timestamp bigint NOT NULL,
            purge_timestamp bigint NOT NULL,
            to_purge_timestamp bigint NOT NULL,
            last_update_timestamp bigint NOT NULL,
            properties text NOT NULL DEFAULT '{}',
            internal_properties text NOT NULL DEFAULT '{}',
            grant_records_version int NOT NULL,
            PRIMARY KEY (realm_id, id),
            CONSTRAINT entities_unique UNIQUE
                (realm_id, catalog_id, parent_id, type_code, name)
        );
        """,
        superuser_conn,
    )

    # ROOT entity (shared across realms in real Polaris; we just need it
    # to exist so catalog rows can parent at it).
    run_command(
        f"""
        INSERT INTO polaris_schema.entities VALUES
            ('{POLARIS_REALM}', 0, 0, 0, 'ROOT', 1, 1, 0,
             0, 0, 0, 0, 0, '{{}}', '{{}}', 1)
        ON CONFLICT DO NOTHING;
        """,
        superuser_conn,
    )

    run_command(
        f"""
        INSERT INTO polaris_schema.entities VALUES
            ('{POLARIS_REALM}', 0, {POLARIS_CATALOG_ID}, 0,
             '{POLARIS_CATALOG_NAME}', 1, 4, 0,
             0, 0, 0, 0, 0, '{{}}', '{{}}', 1)
        ON CONFLICT DO NOTHING;
        """,
        superuser_conn,
    )

    superuser_conn.commit()
    yield
    # Leave schema and root rows in place so other tests in the same
    # session can reuse them. Per-test cleanup is in the function-scoped
    # fixture below.


@pytest.fixture(scope="function")
def polaris_extension(polaris_schema, extension, superuser_conn, app_user):
    """
    Create the pg_lake_polaris extension and register the test catalog.
    Cleans up entity rows + extension on teardown so each test starts
    fresh.
    """
    run_command(
        f"""
        CREATE EXTENSION IF NOT EXISTS pg_lake_polaris;
        GRANT USAGE ON SCHEMA lake_polaris TO {app_user};
        GRANT SELECT, INSERT, UPDATE, DELETE
            ON ALL TABLES IN SCHEMA lake_polaris TO {app_user};
        GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA lake_polaris TO {app_user};
        GRANT INSERT, UPDATE, DELETE ON polaris_schema.entities TO {app_user};
        SELECT lake_polaris.register_catalog('{POLARIS_CATALOG_NAME}');
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        DROP EXTENSION pg_lake_polaris CASCADE;
        DELETE FROM polaris_schema.entities
            WHERE id NOT IN (0, {POLARIS_CATALOG_ID});
        """,
        superuser_conn,
    )
    superuser_conn.commit()
