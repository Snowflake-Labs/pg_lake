import uuid
import pytest
import random
from utils_pytest import *
from helpers.spark import *

N_ROWS = 30
SCHEMA = "test_part_writes_calendar"
PARTITION_COL = "v"

# ---------------- helpers ---------------- #


def _rand_table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _setup_schema(pg_conn):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", pg_conn)


# SQL that recreates Iceberg’s calendar‑based partition keys
def _part_expr(transform: str, col_type: str) -> str:
    v = PARTITION_COL
    if transform == "year":
        return f"(extract(year  from {v})::int - 1970)"  # years since 1970‑01‑01
    if transform == "month":
        return (
            f"((extract(year  from {v})::int - 1970) * 12 + "
            f"(extract(month from {v})::int - 1))"
        )  # months since 1970‑01‑01
    if transform == "day":
        # days since 1970‑01‑01 (works in both PG & DuckDB)
        return f"floor(extract(epoch from ({v}::timestamp)) / 86400)::bigint"
    if transform == "hour":
        return f"floor(extract(epoch from {v}) / 3600)::bigint"  # hours since epoch
    raise ValueError(transform)


# build an INSERT … SELECT for N_ROWS values spread across partitions
def _insert_select(col_type: str, transform: str) -> str:
    """
    Build an INSERT‑SELECT that puts **dup** rows in the same partition
    (so we test multi‑row partitions).  dup is picked randomly (2‑4).
    """
    dup = random.randint(2, 4)

    step = {"year": "1 year", "month": "1 month", "day": "1 day", "hour": "1 hour"}[
        transform
    ]

    # tiny “jitter” that keeps the value inside the same partition
    jitter = {
        "year": "10 day",  # < 1 year
        "month": "2 day",  # < 1 month
        "day": "3 hour",  # < 1 day
        "hour": "15 minute",
    }[
        transform
    ]  # < 1 hour

    start = {
        "DATE": "'1970-01-01'::date",
        "TIMESTAMP": "'1970-01-01 00:00:00'::timestamp",
        "TIMESTAMPTZ": "'1970-01-01 00:00:00+00'::timestamptz",
    }[col_type]

    return f"""
        WITH gen AS (
            SELECT i,
                   floor(random() * {dup})::int AS j      -- random jitter multiplier
            FROM generate_series(0,{N_ROWS - 1}) AS i
        )
        SELECT ({start}
                + ((i / {dup})::int * interval '{step}')  -- partition step
                + (j * interval '{jitter}')               -- stay inside partition
               )::{col_type}
        FROM gen
    """


# ---------------- parametrisation ---------------- #

TYPES = [
    ("date", "DATE"),
    ("timestamp", "TIMESTAMP"),
    ("timestamptz", "TIMESTAMPTZ"),
]

TRANSFORMS = ["year", "month", "day", "hour"]


@pytest.mark.parametrize("case_id, col_type", TYPES)
@pytest.mark.parametrize("transform", TRANSFORMS)
def test_calendar_partition_write(
    extension,
    installcheck,
    s3,
    with_default_location,
    case_id,
    col_type,
    transform,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
    spark_session,
):
    # skip hour(DATE) – Iceberg spec limits hour() to timestamps
    if col_type == "DATE" and transform == "hour":
        # hour() transform not valid for DATE columns
        return

    _setup_schema(pg_conn)
    tbl_name = f"{_rand_table(f'{case_id}_{transform}')}"
    tbl = f"{SCHEMA}.{tbl_name}"
    part_expr = _part_expr(transform, col_type)

    # create partitioned table
    run_command(
        f"""
        CREATE TABLE {tbl}({PARTITION_COL} {col_type})
        USING iceberg
        WITH (partition_by = '{transform}({PARTITION_COL})',
              autovacuum_enabled = false);
        """,
        pg_conn,
    )

    # insert rows
    insert_sql = _insert_select(col_type, transform)
    run_command(f"INSERT INTO {tbl} {insert_sql};", pg_conn)
    pg_conn.commit()

    # total rows written
    total_rows = run_query(
        f"SELECT sum(row_count) FROM lake_table.files "
        f"WHERE table_name = '{tbl}'::regclass;",
        pg_conn,
    )[0][0]
    assert total_rows == N_ROWS

    # expected distinct partition count
    exp_part_cnt = run_query(
        f"SELECT count(DISTINCT {part_expr}) FROM {tbl};",
        pg_conn,
    )[0][0]

    # data‑files count equals distinct partitions
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files "
        f"WHERE table_name = '{tbl}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == exp_part_cnt

    # every file contains rows of exactly one partition
    files = run_query(
        f"SELECT path, id, row_count FROM lake_table.files "
        f"WHERE table_name = '{tbl}'::regclass;",
        pg_conn,
    )
    for path, id, row_count in files:
        res = run_query(
            f"SELECT count(DISTINCT {part_expr}), count(*) FROM '{path}'",
            pgduck_conn,
        )
        assert res[0][0] == "1"
        assert int(res[0][1]) == row_count

        res = run_query(
            f"SELECT DISTINCT {part_expr} FROM '{path}'",
            pgduck_conn,
        )

        res_partition = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id='{id}'",
            pg_conn,
        )

        assert res_partition[0][0] == res[0][0]

    # register the same table to spark
    # and make sure it can understand our partitioning
    if not installcheck:
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

        spark_query = (
            f"SELECT count(*) FROM {tbl} WHERE {PARTITION_COL} = '{test_filter}'"
        )

        spark_result = spark_session.sql(spark_query).collect()
        assert spark_result[0][0] == test_cnt

        spark_unregister_table(installcheck, spark_session, f"{tbl_name}", f"{SCHEMA}")

    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()
