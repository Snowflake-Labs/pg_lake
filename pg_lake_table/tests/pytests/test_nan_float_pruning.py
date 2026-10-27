"""Pruning on a pg_lake table whose float column contains a NaN.

DuckDB's min and max leave NaN out, and Postgres orders it above every other
value. A file that has one must not be pruned on the min and max of its other
values. A file that does not have one still gets bounds.
"""

import pytest
from utils_pytest import fetch_data_files_used, run_command, run_query


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


def scanned_files(pg_conn, where, table):
    """(data files scanned, rows returned), with and without pushdown.

    The rows are checked against a read behind OFFSET 0, which keeps the filter
    out of the scan, so a wrongly pruned file fails here even when the expected
    count in the test is wrong too.
    """
    results = set()
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown={enabled}", pg_conn
        )
        plan = run_query(
            f"EXPLAIN (analyze, verbose, format json) SELECT * FROM {table} WHERE {where}",
            pg_conn,
        )
        count = run_query(f"SELECT count(*) FROM {table} WHERE {where}", pg_conn)[0][0]
        unpruned = run_query(
            f"SELECT count(*) FROM (SELECT * FROM {table} OFFSET 0) t WHERE {where}",
            pg_conn,
        )[0][0]
        assert count == unpruned, f"{where}: {count} rows, {unpruned} unpruned"
        results.add((fetch_data_files_used(plan), count))
    assert len(results) == 1
    return results.pop()


@pytest.mark.parametrize("column_type", ["double precision", "real"])
def test_nan_in_pg_lake_table(pg_conn, extension, with_default_location, column_type):
    schema = f"nan_pruning_{column_type.split()[0]}"
    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, f {column_type}) USING iceberg
            WITH (autovacuum_enabled = 'false');
        INSERT INTO {schema}.t VALUES (1, 1.0), (2, 'NaN');
        INSERT INTO {schema}.t VALUES (3, 1.0), (4, 2.0);
        """,
        pg_conn,
    )
    pg_conn.commit()
    try:
        table = f"{schema}.t"
        # only the file with the NaN can match
        assert scanned_files(pg_conn, "f > 5", table) == ("1", 1)
        assert scanned_files(pg_conn, "f = 'NaN'", table) == ("1", 1)
        # neither file can be pruned: [1, 2] holds values other than 1 and 2
        assert scanned_files(pg_conn, "f NOT IN (1, 2)", table) == ("2", 1)
        # 2 is only in the file without NaN, and the other cannot be pruned
        assert scanned_files(pg_conn, "f = 2", table) == ("2", 1)
        assert scanned_files(pg_conn, "f < 0", table) == ("1", 0)

        metadata = run_query(
            f"SELECT metadata_location FROM iceberg_tables "
            f"WHERE table_namespace = '{schema}' AND table_name = 't'",
            pg_conn,
        )[0][0]
        bounds = run_query(
            "SELECT lower_bounds, upper_bounds "
            f"FROM lake_iceberg.data_file_stats('{metadata}')",
            pg_conn,
        )
        # The file whose id starts at 1 holds the NaN, so its float column has
        # no bounds. The other file still has them.
        by_id = {lower["1"]: (lower, upper) for lower, upper in bounds}
        nan_lower, nan_upper = by_id[1]
        assert "2" not in nan_lower and "2" not in nan_upper
        clean_lower, clean_upper = by_id[3]
        assert "2" in clean_lower and "2" in clean_upper
    finally:
        pg_conn.rollback()
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()
