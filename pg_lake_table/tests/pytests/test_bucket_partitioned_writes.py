# test_part_writes_bucket.py
import uuid as uuid_mod
import random
import pytest
from utils_pytest import *
from spark_utils import *

N_ROWS = 64
SCHEMA = "test_part_writes_bucket"
PARTITION_COL = "v"


# ---------- helper: random table name ---------- #
def _rand_table(prefix):  # e.g. myprefix_d1f2a3b4
    return f"{prefix}_{uuid_mod.uuid4().hex[:8]}"


def _setup_schema(pg_conn):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", pg_conn)


# ---------- INSERT SELECT with duplicates ---------- #
def _insert_select(col_type: str) -> str:
    """
    Generate N_ROWS values with 2‑4 identical rows per *distinct* value,
    so multiple rows land in the same bucket.
    """
    dup = random.randint(2, 4)

    if col_type in {"SMALLINT", "INTEGER", "BIGINT", "NUMERIC"}:
        return f"SELECT (i/{dup})::{col_type} FROM generate_series(1,{N_ROWS}) AS i"

    if col_type == "TEXT":
        return f"SELECT 'txt_'||(i/{dup}) FROM generate_series(1,{N_ROWS}) AS i"

    if col_type == "BYTEA":
        return f"SELECT repeat(chr(65 + ((i/{dup})::int2 % 26)), 4)::bytea FROM generate_series(1,{N_ROWS}) AS i"

    if col_type == "DATE":
        return f"SELECT '1970-01-01'::date + (i/{dup})::int2 FROM generate_series(1,{N_ROWS}) AS i"

    if col_type == "TIME":
        return f"SELECT (time '00:00' + ((i/{dup})*interval '15 minute'))::time FROM generate_series(1,{N_ROWS}) AS i"

    if col_type == "TIMESTAMP":
        return f"SELECT (timestamp '1970-01-01 00:00' + ((i/{dup})*interval '1 hour')) FROM generate_series(1,{N_ROWS}) AS i"

    if col_type == "TIMESTAMPTZ":
        return f"SELECT (timestamp '1970-01-01 00:00+00' + ((i/{dup})*interval '1 hour')) FROM generate_series(1,{N_ROWS}) AS i"

    if col_type == "UUID":
        return f"SELECT gen_random_uuid() FROM generate_series(1,{N_ROWS})"

    raise ValueError(col_type)


# ---------- data types to cover ---------- #
CASES = [
    ("smallint", "SMALLINT", True, False),
    ("integer", "INTEGER", True, False),
    ("bigint", "BIGINT", True, False),
    ("numeric", "NUMERIC", True, False),
    ("text", "TEXT", True, True),
    ("bytea", "BYTEA", False, True),
    ("date", "DATE", True, True),
    ("time", "TIME", False, True),  # no time in spark
    ("timestamp", "TIMESTAMP", True, True),
    ("timestamptz", "TIMESTAMPTZ", True, True),
    ("uuid", "UUID", False, True),  # no uuid in spark
]


# ---------- the test itself ---------- #
@pytest.mark.parametrize("case_id,col_type,compare_with_spark, quote_filter", CASES)
def test_bucket_partition_write(
    extension,
    installcheck,
    s3,
    with_default_location,
    case_id,
    col_type,
    compare_with_spark,
    quote_filter,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
    spark_session,
):
    bucket_cnt = random.randint(3, 12)  # 3–12 buckets
    _setup_schema(pg_conn)
    tbl_name = f"{_rand_table(f'{case_id}_b{bucket_cnt}')}"
    tbl = f"{SCHEMA}.{tbl_name}"
    run_command(
        f"""
        CREATE TABLE {tbl}({PARTITION_COL} {col_type})
        USING iceberg
        WITH (partition_by='bucket({bucket_cnt}, {PARTITION_COL})',
              autovacuum_enabled=false);
        """,
        pg_conn,
    )

    # 1 · insert rows
    run_command(f"INSERT INTO {tbl} {_insert_select(col_type)};", pg_conn)

    pg_conn.commit()

    # 2 · total rows recorded in metadata == N_ROWS
    total_rows = run_query(
        f"SELECT coalesce(sum(row_count),0)"
        f"  FROM lake_table.files"
        f" WHERE table_name = '{tbl}'::regclass;",
        pg_conn,
    )[0][0]
    assert total_rows == N_ROWS

    # 3 · every data file's row_count matches its actual content,
    #     and no data file is empty
    files = run_query(
        f"SELECT path, id, row_count"
        f"  FROM lake_table.files"
        f" WHERE table_name = '{tbl}'::regclass;",
        pg_conn,
    )

    assert len(files) > 0  # at least one file written

    for path, id, row_count in files:
        rows_in_file = run_query(
            f"SELECT count(*) FROM '{path}'",
            pgduck_conn,
        )[
            0
        ][0]

        assert int(rows_in_file) == row_count  # metadata ↔ file agree
        assert int(rows_in_file) > 0  # no empty files

        partition_stats = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id='{id}'",
            pg_conn,
        )
        assert int(partition_stats[0][0]) < bucket_cnt

    res = run_query(f"SELECT count(*) FROM {tbl}", pg_conn)
    assert res[0][0] == N_ROWS

    # register the same table to spark
    # and make sure it can understand our partitioning
    if not installcheck and compare_with_spark:
        metadata_location = run_query(
            f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{tbl_name}' and table_namespace='{SCHEMA}'",
            pg_conn,
        )[0][0]
        # now make sure spark can read the data files with partitioning
        spark_register_table(
            installcheck, spark_session, f"{tbl_name}", f"{SCHEMA}", metadata_location
        )

        # pick a random value from the table
        res = run_query(
            f"SELECT {PARTITION_COL}, count(*) FROM {tbl} GROUP BY {PARTITION_COL} ORDER BY random() LIMIT 1",
            pg_conn,
        )
        test_filter = res[0][0]
        test_cnt = res[0][1]

        query_filter = f"'{test_filter}'" if quote_filter else f"{test_filter}"
        spark_query = (
            f"SELECT count(*) FROM {tbl} WHERE {PARTITION_COL} = {query_filter}"
        )

        spark_result = spark_session.sql(spark_query).collect()
        assert spark_result[0][0] == test_cnt

        spark_unregister_table(installcheck, spark_session, f"{tbl_name}", f"{SCHEMA}")

    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()
