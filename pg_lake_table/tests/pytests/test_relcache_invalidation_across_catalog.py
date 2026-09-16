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


# TransformPushdownableInsertSelect() fills omitted INSERT columns with a
# NULL TargetEntry whose resname is NameStr() into the relcache descriptor,
# then table_close()s.  Later deparse (and any catalog lookup) can process a
# relcache invalidation and free that name: the DuckDB COPY then aliases the
# omitted column as 0x7F filler and FIELD_IDS cannot find it.
# debug_discard_caches turns that race into a certainty.
def test_insert_select_survives_relcache_invalidation(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    _require_discard_caches(superuser_conn)

    run_command("DROP SCHEMA IF EXISTS relcache_inval_is CASCADE", pg_conn)
    run_command("CREATE SCHEMA relcache_inval_is", pg_conn)
    run_command(
        """
        CREATE TABLE relcache_inval_is.src (
            id bigint,
            kind text
        ) USING iceberg;
        CREATE TABLE relcache_inval_is.tgt (
            id bigint,
            kind text,
            extra text
        ) USING iceberg;
        INSERT INTO relcache_inval_is.src VALUES (1, 'INSERT');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("SET debug_discard_caches = 1", superuser_conn)
    try:
        run_command(
            "INSERT INTO relcache_inval_is.tgt (id, kind) SELECT id, kind FROM relcache_inval_is.src",
            superuser_conn,
        )
        rows = run_query(
            "SELECT id, kind, extra FROM relcache_inval_is.tgt", superuser_conn
        )
        assert rows == [[1, "INSERT", None]]
    finally:
        superuser_conn.rollback()
        run_command("RESET debug_discard_caches", superuser_conn)
        superuser_conn.rollback()

    run_command("DROP SCHEMA relcache_inval_is CASCADE", pg_conn)
    pg_conn.commit()
