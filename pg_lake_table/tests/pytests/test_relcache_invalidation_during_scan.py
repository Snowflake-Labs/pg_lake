from utils_pytest import *
import pytest


# Building the DuckDB read query for an Iceberg scan drops its relcache pin
# before it is done deparsing the relation's tuple descriptor, so a relcache
# invalidation arriving in that window used to leave the column names (and the
# field names in the read_parquet() schema map) pointing into freed memory:
# either a garbled query DuckDB rejects, or a segfault while walking the
# descriptor.  debug_discard_caches turns that race into a certainty.
def test_scan_survives_relcache_invalidation(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    # debug_discard_caches only accepts a non-zero value on an
    # assertion-enabled build; elsewhere its range is 0..0.  Nothing else lands
    # an invalidation inside the window this test targets: a same-session one is
    # drained at command end before the executor starts, and one that is already
    # pending is consumed during planning, before the descriptor is taken.
    discard_max = run_query(
        "SELECT max_val FROM pg_settings WHERE name = 'debug_discard_caches'",
        superuser_conn,
    )
    superuser_conn.rollback()
    if not discard_max or discard_max[0][0] == "0":
        pytest.skip("debug_discard_caches requires an assertion-enabled build")

    run_command("DROP SCHEMA IF EXISTS relcache_inval CASCADE", pg_conn)
    run_command("CREATE SCHEMA relcache_inval", pg_conn)
    run_command(
        """
        CREATE TABLE relcache_inval.tbl (
            id bigint,
            kind text,
            description text
        ) USING iceberg
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "INSERT INTO relcache_inval.tbl VALUES (1, 'INSERT', 'desc')",
        pg_conn,
    )
    pg_conn.commit()

    run_command("SET debug_discard_caches = 1", superuser_conn)
    try:
        rows = run_query("SELECT * FROM relcache_inval.tbl", superuser_conn)
        assert rows == [[1, "INSERT", "desc"]]
    finally:
        run_command("RESET debug_discard_caches", superuser_conn)
        superuser_conn.rollback()

    run_command("DROP SCHEMA relcache_inval CASCADE", pg_conn)
    pg_conn.commit()
