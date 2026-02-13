import uuid
import random
import pytest
from utils_pytest import *
from spark_utils import *

N_ROWS = 30
SCHEMA = "test_part_writes_truncate"
PARTITION_COL = "v"

CASES = [
    # (case_id, SQL type, insert-template)
    (
        "smallint",
        "SMALLINT",
        "SELECT i::smallint FROM generate_series(1,{n}) AS i",
        1,
        True,
    ),
    (
        "integer",
        "INTEGER",
        "SELECT i          FROM generate_series(1,{n}) AS i",
        1,
        True,
    ),
    ("bigint", "BIGINT", "SELECT i::bigint  FROM generate_series(1,{n}) AS i", 1, True),
    (
        "text",
        "TEXT",
        "SELECT 'txt_'||i  FROM generate_series(1,{n}) AS i",
        "'txt_1'",
        True,
    ),
    (
        "varchar",
        "VARCHAR",
        "SELECT 'val_'||i  FROM generate_series(1,{n}) AS i",
        "'val_1'",
        True,
    ),
    (
        "char",
        "CHAR(3)",
        "SELECT lpad(i::text,3,'0')::char(3) FROM generate_series(1,{n}) AS i",
        "'001'",
        True,
    ),
    # TODO: improve bytea test, the expressions may fail
    # ("bytea",    "BYTEA",
    # "SELECT decode(lpad(i::text,8,'0'),'hex') FROM generate_series(1,{n}) AS i"),
]


def _rand_table(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _setup_schema(pg_conn):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", pg_conn)


def _part_expr(col_type: str, width: int) -> str:
    """
    SQL snippet that reproduces truncate(width, v) for all supported types
    in *both* Postgres and DuckDB.
    """
    if col_type in {"SMALLINT", "INTEGER", "BIGINT"}:
        # Iceberg stores floor(v/width) * width  (same sign as v).
        # Works in both engines:
        return f"(floor(({PARTITION_COL}::float8) / {width}) * {width})::bigint"

    elif col_type == "BYTEA":
        # First <width> bytes → 2*width hex characters (skip the leading '\x')
        return f"substr(CAST({PARTITION_COL} AS VARCHAR), 3, {width * 2})"

    else:  # TEXT / VARCHAR / CHAR
        # First <width> characters
        return f"substr({PARTITION_COL}, 1, {width})"


@pytest.mark.parametrize(
    "case_id, col_type, insert_tmpl, test_filter, compare_with_spark", CASES
)
def test_truncate_partition_write(
    installcheck,
    spark_session,
    extension,
    s3,
    with_default_location,
    case_id,
    col_type,
    insert_tmpl,
    test_filter,
    compare_with_spark,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """
    1. create table partitioned by truncate(width, v)
    2. insert N_ROWS rows
    3. expect one data file per *truncated* partition value
    """
    _setup_schema(pg_conn)
    tbl_name = f"{_rand_table(case_id)}"
    tbl = f"{SCHEMA}.{tbl_name}"

    width = random.randint(1, 10)
    run_command(
        f"""
        CREATE TABLE {tbl}({PARTITION_COL} {col_type})
        USING iceberg
        WITH (partition_by = 'truncate({width}, {PARTITION_COL})',
              autovacuum_enabled = false);
        """,
        pg_conn,
    )

    # insert rows
    run_command(f"INSERT INTO {tbl} {insert_tmpl.format(n=N_ROWS)};", pg_conn)
    pg_conn.commit()

    res = run_query(f"SELECT * FROM {tbl}", pg_conn)
    # files written + rows recorded
    res = run_query(
        f"""
        SELECT count(*), sum(row_count)
        FROM   lake_table.files
        WHERE  table_name = '{tbl}'::regclass;
        """,
        pg_conn,
    )
    file_cnt, total_rows = res[0]
    assert total_rows == N_ROWS

    # how many distinct partitions should we have?
    part_expr = _part_expr(col_type, width)
    exp_part_cnt = run_query(
        f"SELECT count(DISTINCT {part_expr}) FROM {tbl};", pg_conn
    )[0][0]

    assert file_cnt == exp_part_cnt

    # every data-file must only contain rows of *one* partition
    files = run_query(
        f"SELECT path, id, row_count FROM lake_table.files "
        f"WHERE table_name = '{tbl}'::regclass",
        pg_conn,
    )
    assert len(files) == file_cnt

    for path, id, row_count in files:
        res = run_query(
            f"SELECT count(DISTINCT {part_expr}), count(*) FROM '{path}'", pgduck_conn
        )
        assert res[0][0] == "1"
        assert int(res[0][1]) == row_count

        res = run_query(f"SELECT DISTINCT {part_expr} FROM '{path}'", pgduck_conn)

        res_partition = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id='{id}'",
            pg_conn,
        )
        assert res_partition[0][0].strip() == res[0][0].strip()

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

        spark_query = (
            f"SELECT count(*) FROM {tbl} WHERE {PARTITION_COL} = {test_filter}"
        )

        spark_result = spark_session.sql(spark_query).collect()
        assert spark_result[0][0] == 1

        spark_unregister_table(installcheck, spark_session, f"{tbl_name}", f"{SCHEMA}")
    pg_conn.rollback()
