from utils_pytest import *
import pytest


def _require_discard_caches(superuser_conn):
    # debug_discard_caches only accepts a non-zero value on an
    # assertion-enabled build; elsewhere its range is 0..0.
    discard_max = run_query(
        "SELECT max_val FROM pg_settings WHERE name = 'debug_discard_caches'",
        superuser_conn,
    )
    superuser_conn.rollback()
    if not discard_max or discard_max[0][0] == "0":
        pytest.skip("debug_discard_caches requires an assertion-enabled build")


# RelationColumnsSuitableForPushdown() walks RelationGetDescr while calling
# get_typtype / TypeContainsUnsuitableForPushdown, which process invalidations.
# BeginForeignModify() then keeps that descriptor across GetForeignTable.
# debug_discard_caches turns those windows into a certainty.
def test_insert_select_star_survives_relcache_invalidation(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    _require_discard_caches(superuser_conn)

    run_command("DROP SCHEMA IF EXISTS relcache_inval_pd CASCADE", pg_conn)
    run_command("CREATE SCHEMA relcache_inval_pd", pg_conn)
    run_command(
        """
        CREATE TABLE relcache_inval_pd.src (
            id bigint,
            kind text
        ) USING iceberg;
        CREATE TABLE relcache_inval_pd.tgt (
            id bigint,
            kind text
        ) USING iceberg;
        INSERT INTO relcache_inval_pd.src VALUES (1, 'INSERT');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("SET debug_discard_caches = 1", superuser_conn)
    try:
        run_command(
            "INSERT INTO relcache_inval_pd.tgt SELECT * FROM relcache_inval_pd.src",
            superuser_conn,
        )
        rows = run_query("SELECT id, kind FROM relcache_inval_pd.tgt", superuser_conn)
        assert rows == [[1, "INSERT"]]
    finally:
        superuser_conn.rollback()
        run_command("RESET debug_discard_caches", superuser_conn)
        superuser_conn.rollback()

    run_command("DROP SCHEMA relcache_inval_pd CASCADE", pg_conn)
    pg_conn.commit()


def test_insert_values_survives_relcache_invalidation(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    _require_discard_caches(superuser_conn)

    run_command("DROP SCHEMA IF EXISTS relcache_inval_ins CASCADE", pg_conn)
    run_command("CREATE SCHEMA relcache_inval_ins", pg_conn)
    run_command(
        """
        CREATE TABLE relcache_inval_ins.tbl (
            id bigint,
            kind text
        ) USING iceberg
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("SET debug_discard_caches = 1", superuser_conn)
    try:
        run_command(
            "INSERT INTO relcache_inval_ins.tbl VALUES (1, 'INSERT')",
            superuser_conn,
        )
        rows = run_query("SELECT id, kind FROM relcache_inval_ins.tbl", superuser_conn)
        assert rows == [[1, "INSERT"]]
    finally:
        superuser_conn.rollback()
        run_command("RESET debug_discard_caches", superuser_conn)
        superuser_conn.rollback()

    run_command("DROP SCHEMA relcache_inval_ins CASCADE", pg_conn)
    pg_conn.commit()
