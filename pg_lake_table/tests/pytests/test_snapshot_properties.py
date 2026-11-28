import pytest
import json
from pathlib import Path
from utils_pytest import *
from helpers.polaris import *


def test_snapshot_properties_multiple_writes(
    pg_conn, extension, s3, with_default_location, snapshot_property_function
):
    run_command(
        "CREATE TABLE snapprops_multi (a int) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()

    # several writes and properties in one transaction create one snapshot,
    # and setting a key again keeps the last value
    run_command(
        """
        INSERT INTO snapprops_multi VALUES (1);
        SELECT lake_iceberg.set_snapshot_property('snapprops_multi', 'test.lsn', '0/1');
        INSERT INTO snapprops_multi VALUES (2);
        SELECT lake_iceberg.set_snapshot_property('snapprops_multi', 'test.batch', '7');
        SELECT lake_iceberg.set_snapshot_property('snapprops_multi', 'test.lsn', '0/2');
        """,
        pg_conn,
    )
    pg_conn.commit()

    snapshots = get_snapshots(s3, pg_conn, "snapprops_multi")
    assert len(snapshots) == 1
    assert snapshots[-1]["summary"] == {
        "operation": "append",
        "test.batch": "7",
        "test.lsn": "0/2",
    }

    # the next transaction starts without properties
    run_command("DELETE FROM snapprops_multi WHERE a = 1", pg_conn)
    pg_conn.commit()

    snapshots = get_snapshots(s3, pg_conn, "snapprops_multi")
    assert len(snapshots) == 2
    assert snapshots[-1]["summary"] == {"operation": "delete"}

    # properties go along with the operation of the transaction
    run_command(
        """
        DELETE FROM snapprops_multi;
        SELECT lake_iceberg.set_snapshot_property('snapprops_multi', 'test.lsn', '0/3');
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert get_snapshots(s3, pg_conn, "snapprops_multi")[-1]["summary"] == {
        "operation": "delete",
        "test.lsn": "0/3",
    }

    run_command("DROP TABLE snapprops_multi", pg_conn)
    pg_conn.commit()


def test_snapshot_properties_rollback(
    pg_conn, extension, s3, with_default_location, snapshot_property_function
):
    run_command(
        """
        CREATE TABLE snapprops_rollback (a int) USING iceberg;
        INSERT INTO snapprops_rollback VALUES (1);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # aborted transaction leaves no trace
    run_command(
        """
        INSERT INTO snapprops_rollback VALUES (2);
        SELECT lake_iceberg.set_snapshot_property('snapprops_rollback', 'test.lsn', '0/1');
        """,
        pg_conn,
    )
    pg_conn.rollback()

    snapshots = get_snapshots(s3, pg_conn, "snapprops_rollback")
    assert len(snapshots) == 1
    assert snapshots[-1]["summary"] == {"operation": "append"}

    # a rolled back savepoint undoes the property, including a replaced
    # value and a property set in a nested savepoint that was released
    run_command(
        """
        SELECT lake_iceberg.set_snapshot_property('snapprops_rollback', 'test.lsn', '0/2');
        SELECT lake_iceberg.set_snapshot_property('snapprops_rollback', 'test.keep', 'yes');
        SAVEPOINT s1;
        SELECT lake_iceberg.set_snapshot_property('snapprops_rollback', 'test.lsn', '0/3');
        SAVEPOINT s2;
        SELECT lake_iceberg.set_snapshot_property('snapprops_rollback', 'test.inner', 'yes');
        RELEASE SAVEPOINT s2;
        INSERT INTO snapprops_rollback VALUES (3);
        ROLLBACK TO SAVEPOINT s1;
        SAVEPOINT s3;
        SELECT lake_iceberg.set_snapshot_property('snapprops_rollback', 'test.released', 'yes');
        RELEASE SAVEPOINT s3;
        INSERT INTO snapprops_rollback VALUES (4);
        """,
        pg_conn,
    )
    pg_conn.commit()

    snapshots = get_snapshots(s3, pg_conn, "snapprops_rollback")
    assert len(snapshots) == 2
    assert snapshots[-1]["summary"] == {
        "operation": "append",
        "test.lsn": "0/2",
        "test.keep": "yes",
        "test.released": "yes",
    }
    assert run_query("SELECT a FROM snapprops_rollback ORDER BY a", pg_conn) == [
        [1],
        [4],
    ]

    # properties only set in a rolled back savepoint do not create a snapshot
    run_command(
        """
        SAVEPOINT s1;
        SELECT lake_iceberg.set_snapshot_property('snapprops_rollback', 'test.lsn', '0/4');
        ROLLBACK TO SAVEPOINT s1;
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert len(get_snapshots(s3, pg_conn, "snapprops_rollback")) == 2

    run_command("DROP TABLE snapprops_rollback", pg_conn)
    pg_conn.commit()


def test_snapshot_properties_without_data_change(
    pg_conn, extension, s3, with_default_location, snapshot_property_function
):
    # properties on a table created in the same transaction
    run_command(
        """
        CREATE TABLE snapprops_nodata (a int) USING iceberg;
        SELECT lake_iceberg.set_snapshot_property('snapprops_nodata', 'test.lsn', '0/1');
        """,
        pg_conn,
    )
    pg_conn.commit()

    snapshots = get_snapshots(s3, pg_conn, "snapprops_nodata")
    assert len(snapshots) == 1
    assert snapshots[-1]["summary"] == {"operation": "append", "test.lsn": "0/1"}

    run_command("INSERT INTO snapprops_nodata VALUES (1)", pg_conn)
    pg_conn.commit()

    # a transaction that only sets properties creates a snapshot with the
    # same data
    run_command(
        "SELECT lake_iceberg.set_snapshot_property('snapprops_nodata', 'test.lsn', '0/2')",
        pg_conn,
    )
    pg_conn.commit()

    snapshots = get_snapshots(s3, pg_conn, "snapprops_nodata")
    assert len(snapshots) == 3
    assert snapshots[-1]["summary"] == {"operation": "append", "test.lsn": "0/2"}
    assert snapshots[-1]["parent-snapshot-id"] == snapshots[-2]["snapshot-id"]
    assert run_query("SELECT a FROM snapprops_nodata", pg_conn) == [[1]]

    run_command("DROP TABLE snapprops_nodata", pg_conn)
    pg_conn.commit()


def test_snapshot_properties_multiple_tables(
    pg_conn, extension, s3, with_default_location, snapshot_property_function
):
    run_command(
        """
        CREATE TABLE snapprops_t1 (a int) USING iceberg;
        CREATE TABLE snapprops_t2 (a int) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO snapprops_t1 VALUES (1);
        INSERT INTO snapprops_t2 VALUES (1);
        SELECT lake_iceberg.set_snapshot_property('snapprops_t1', 'test.table', 't1');
        SELECT lake_iceberg.set_snapshot_property('snapprops_t2', 'test.table', 't2');
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert get_snapshots(s3, pg_conn, "snapprops_t1")[-1]["summary"] == {
        "operation": "append",
        "test.table": "t1",
    }
    assert get_snapshots(s3, pg_conn, "snapprops_t2")[-1]["summary"] == {
        "operation": "append",
        "test.table": "t2",
    }

    run_command("DROP TABLE snapprops_t1, snapprops_t2", pg_conn)
    pg_conn.commit()


def test_snapshot_properties_errors(
    pg_conn, extension, s3, with_default_location, snapshot_property_function
):
    run_command(
        """
        CREATE TABLE snapprops_errors (a int) USING iceberg;
        CREATE TABLE snapprops_heap (a int);
        """,
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        "SELECT lake_iceberg.set_snapshot_property('snapprops_errors', 'operation', 'delete')",
        pg_conn,
        raise_error=False,
    )
    assert 'snapshot property "operation" is reserved' in error
    pg_conn.rollback()

    error = run_command(
        "SELECT lake_iceberg.set_snapshot_property('snapprops_errors', '', 'x')",
        pg_conn,
        raise_error=False,
    )
    assert "must not be empty" in error
    pg_conn.rollback()

    error = run_command(
        "SELECT lake_iceberg.set_snapshot_property('snapprops_heap', 'test.lsn', '0/1')",
        pg_conn,
        raise_error=False,
    )
    assert "is not an Iceberg table" in error
    pg_conn.rollback()

    run_command("DROP TABLE snapprops_errors, snapprops_heap", pg_conn)
    pg_conn.commit()


def test_snapshot_properties_rest_catalog(
    installcheck,
    superuser_conn,
    pg_conn,
    s3,
    extension,
    with_default_location,
    polaris_session,
    snapshot_property_function,
):
    if installcheck:
        pytest.skip("Requires an isolated test cluster")

    SERVER_NAME = "rest_snapprops"
    SCHEMA_NAME = "snapprops_rest_ns"

    creds = json.loads(Path(server_params.POLARIS_PRINCIPAL_CREDS_FILE).read_text())
    client_id = creds["credentials"]["clientId"]
    client_secret = creds["credentials"]["clientSecret"]
    endpoint = f"http://localhost:{server_params.POLARIS_PORT}/api/catalog"

    run_command(
        f"""
        CREATE SERVER {SERVER_NAME} TYPE 'rest'
            FOREIGN DATA WRAPPER iceberg_catalog
            OPTIONS (rest_endpoint '{endpoint}',
                     catalog_name '{server_params.PG_DATABASE}',
                     location_prefix 's3://{TEST_BUCKET}');
        CREATE USER MAPPING FOR PUBLIC SERVER {SERVER_NAME}
            OPTIONS (client_id '{client_id}', client_secret '{client_secret}');
        GRANT USAGE ON FOREIGN SERVER {SERVER_NAME} TO PUBLIC;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        f"""
        CREATE SCHEMA {SCHEMA_NAME};
        CREATE TABLE {SCHEMA_NAME}.tbl (a int) USING iceberg WITH (catalog='{SERVER_NAME}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {SCHEMA_NAME}.tbl VALUES (1), (2);
        SELECT lake_iceberg.set_snapshot_property('{SCHEMA_NAME}.tbl', 'test.lsn', '0/1');
        SAVEPOINT s1;
        SELECT lake_iceberg.set_snapshot_property('{SCHEMA_NAME}.tbl', 'test.lsn', '0/2');
        ROLLBACK TO SAVEPOINT s1;
        """,
        pg_conn,
    )
    pg_conn.commit()

    url = (
        f"http://{server_params.POLARIS_HOSTNAME}:{server_params.POLARIS_PORT}"
        f"/api/catalog/v1/{server_params.PG_DATABASE}"
        f"/namespaces/{SCHEMA_NAME}/tables/tbl"
    )

    def rest_current_snapshot():
        resp = polaris_session.get(url, timeout=5)
        assert resp.status_code == 200, resp.text
        metadata = resp.json()["metadata"]
        return [
            snapshot
            for snapshot in metadata["snapshots"]
            if snapshot["snapshot-id"] == metadata["current-snapshot-id"]
        ][0]

    assert rest_current_snapshot()["summary"] == {
        "operation": "append",
        "test.lsn": "0/1",
    }

    # REST catalog tables report the actual operation, not always append
    run_command(
        f"""
        DELETE FROM {SCHEMA_NAME}.tbl;
        SELECT lake_iceberg.set_snapshot_property('{SCHEMA_NAME}.tbl', 'test.lsn', '0/3');
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert rest_current_snapshot()["summary"] == {
        "operation": "delete",
        "test.lsn": "0/3",
    }

    run_command(f"DROP SCHEMA {SCHEMA_NAME} CASCADE", pg_conn)
    pg_conn.commit()
    run_command(f"DROP SERVER {SERVER_NAME} CASCADE", superuser_conn)
    superuser_conn.commit()


def get_snapshots(s3, pg_conn, table_name):
    """Return the snapshots of the table, oldest first."""
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'",
        pg_conn,
    )[0][0]
    metadata = json.loads(read_s3_operations(s3, metadata_location))
    return sorted(
        metadata["snapshots"], key=lambda snapshot: snapshot["sequence-number"]
    )


@pytest.fixture(scope="module")
def snapshot_property_function(superuser_conn, app_user):
    # the C function is not exposed by the extension, so define it here
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.set_snapshot_property(
            relation regclass, key text, value text)
        RETURNS void
        LANGUAGE C STRICT
        AS 'pg_lake_table', $function$set_iceberg_snapshot_property$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.set_snapshot_property(regclass, text, text)
            TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command("DROP FUNCTION lake_iceberg.set_snapshot_property", superuser_conn)
    superuser_conn.commit()
