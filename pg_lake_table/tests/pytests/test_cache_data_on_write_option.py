import psycopg2
from utils_pytest import *

from test_writable_iceberg_common import *


def test_cache_data_on_write_rejects_non_boolean(pg_conn, extension):
    """cache_data_on_write only accepts a Boolean value."""
    cur = pg_conn.cursor()
    try:
        cur.execute(
            "CREATE TABLE test_cache_bad (id int) USING iceberg "
            "WITH (cache_data_on_write = 'maybe')"
        )
        assert False
    except psycopg2.Error as e:
        assert "cache_data_on_write requires a Boolean value" in str(e)
        cur.close()
        pg_conn.rollback()


def test_cache_data_on_write_false_roundtrip(
    s3, pg_conn, extension, with_default_location, allow_iceberg_guc_perms
):
    """With cache_data_on_write=false the write path prefixes data files with
    the pgduck "nocache" marker and strips it back off before persisting, so
    inserts and deletes still round-trip to the correct object-store paths."""
    pg_conn.autocommit = True

    run_command(
        """
        CREATE TABLE test_cache_off (id int) USING iceberg
            WITH (autovacuum_enabled = false, cache_data_on_write = false);
        """,
        pg_conn,
    )

    run_command("INSERT INTO test_cache_off SELECT generate_series(1, 100)", pg_conn)
    assert run_query("SELECT count(*) FROM test_cache_off", pg_conn) == [[100]]

    # a delete triggers a rewrite whose returned path also carries the prefix
    run_command("DELETE FROM test_cache_off WHERE id <= 50", pg_conn)
    result = run_query("SELECT id FROM test_cache_off ORDER BY id LIMIT 3", pg_conn)
    assert result == [[51], [52], [53]]
    assert run_query("SELECT count(*) FROM test_cache_off", pg_conn) == [[50]]

    # every persisted data file path must be a real object-store path
    paths = run_query(
        "SELECT file_path FROM lake_iceberg.files("
        "  (SELECT metadata_location FROM iceberg_tables"
        "     WHERE table_name = 'test_cache_off'))",
        pg_conn,
    )
    assert paths
    assert all(not p[0].startswith("nocache") for p in paths)

    run_command("DROP TABLE test_cache_off", pg_conn)
    pg_conn.autocommit = False
