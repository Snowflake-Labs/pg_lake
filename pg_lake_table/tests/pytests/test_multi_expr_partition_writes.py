# tests/pytests/test_multi_expr_partition_writes.py

import uuid
from utils_pytest import *

SCHEMA = "test_part_writes_multi_expr"
PART_A_WIDTH = 10  # truncate(10 , a)
PART_B_WIDTH = 2  # truncate( 2 , b)


# ─── helpers ────────────────────────────────────────────────────────────────
def _rand_table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _setup_schema(pg_conn):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", pg_conn)


# ─── test ───────────────────────────────────────────────────────────────────
def test_multi_expression_partition_write(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
):
    """
    Partition spec:  truncate(10,a) , truncate(2,b) , c
    • Eight partitions
    • Two rows per partition  →  16 rows
    • Expect 8 data-files (1 per partition) and 2 rows in each file.
    """
    _setup_schema(pg_conn)
    tbl = f"{SCHEMA}.{_rand_table('multi_expr')}"

    # 1) table
    run_command(
        f"""
        SET pg_lake_table.enable_insert_select_pushdown TO false;

        CREATE TABLE {tbl}(a INT, b TEXT, c BIGINT)
        USING iceberg
        WITH (partition_by =
              'truncate({PART_A_WIDTH}, a), truncate({PART_B_WIDTH}, b), c',
              autovacuum_enabled = false);
        """,
        pg_conn,
    )

    # 2) 16 rows = 2 per partition
    run_command(
        f"""
        INSERT INTO {tbl}(a,b,c) VALUES
            (  1,'aa',1),(  1,'aa',1),
            (  2,'aa',2),(  2,'aa',2),
            ( 11,'ab',1),( 11,'ab',1),
            ( 12,'ab',2),( 12,'ab',2),
            ( 21,'ba',1),( 21,'ba',1),
            ( 22,'ba',2),( 22,'ba',2),
            ( 31,'bb',1),( 31,'bb',1),
            ( 32,'bb',2),( 32,'bb',2);
        """,
        pg_conn,
    )

    # 3) metadata sanity – one file per partition, all rows recorded
    file_cnt, total_rows = run_query(
        f"""
        SELECT count(*), sum(row_count)
        FROM   lake_table.files
        WHERE  table_name = '{tbl}'::regclass;
        """,
        pg_conn,
    )[0]
    assert total_rows == 16
    assert file_cnt == 8

    # 4) per-file validation: exactly one key, exactly two rows
    files = run_query(
        f"""
        SELECT path, row_count
        FROM   lake_table.files
        WHERE  table_name = '{tbl}'::regclass;
        """,
        pg_conn,
    )

    for path, row_count in files:
        # 4a) total rows in Parquet file == metadata row_count == 2
        rows_in_file = run_query(f"SELECT count(*) FROM '{path}'", pgduck_conn)[0][0]
        assert int(rows_in_file) == int(row_count) == 2

        # 4b) file covers exactly ONE truncated-key tuple
        part_key_cnt = run_query(
            f"""
            SELECT count(*)
            FROM (
                SELECT DISTINCT
                       floor(a / {PART_A_WIDTH}) * {PART_A_WIDTH}     AS a_trunc,
                       substr(b, 1, {PART_B_WIDTH})                  AS b_trunc,
                       c
                FROM '{path}'
            ) t;
            """,
            pgduck_conn,
        )[0][0]
        assert part_key_cnt == "1"

    # 5) tidy up session GUC
    run_command("RESET pg_lake_table.enable_insert_select_pushdown;", pg_conn)
    pg_conn.rollback()
