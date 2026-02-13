import uuid
import pytest
import datetime
from utils_pytest import *
from spark_utils import *


N_ROWS = 5  # how many rows we want to insert
SCHEMA = "test_part_writes"  # one throw-away schema for all
PARTITION_COL = "v"  # every table has this column


CASES = [
    (
        "boolean",
        "BOOLEAN",
        "SELECT (i % 2 = 0) FROM generate_series(1, {n}) AS i",
        2,
        True,
        False,
    ),
    (
        "bytea",
        "BYTEA",
        "SELECT decode(lpad(i::text, 8, '0'), 'hex') FROM generate_series(1,{n}) AS i",
        N_ROWS,
        "X'00000001'",
        True,
    ),
    (
        "smallint",
        "SMALLINT",
        "SELECT i::smallint FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "1",
        True,
    ),
    (
        "integer",
        "INTEGER",
        "SELECT i FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "1",
        False,
    ),
    (
        "bigint",
        "BIGINT",
        "SELECT i::bigint FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "1",
        True,
    ),
    (
        "real",
        "REAL",
        "SELECT i::real / 10 FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "0.1",
        False,  # float comparison makes the test harder to automate
    ),
    (
        "double",
        "DOUBLE PRECISION",
        "SELECT i::double precision / 10 FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "0.1",
        True,
    ),
    (
        "varchar",
        "VARCHAR",
        "SELECT 'val_' || i FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "'val_1'",
        True,
    ),
    (
        "numeric",
        "NUMERIC(20,0)",
        "SELECT i::numeric FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "1",
        True,
    ),
    (
        "numeric_10_3",
        "NUMERIC(10,3)",
        "SELECT i::numeric FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "1",
        True,
    ),
    (
        "uuid",
        "UUID",
        "SELECT gen_random_uuid() FROM generate_series(1, {n})",
        N_ROWS,
        None,
        False,  # there is no uuid in spark
    ),
    (
        "timestamp",
        "TIMESTAMP",
        "SELECT '2025-01-01'::timestamp + (i||' minutes')::interval "
        "FROM generate_series(1,{n}) AS i",
        N_ROWS,
        "'2025-01-01 00:01:00'",
        True,
    ),
    (
        "timestamptz",
        "TIMESTAMPTZ",
        "SELECT '2025-01-01'::timestamp + ((i-50)||' minutes')::interval "
        "FROM generate_series(1,{n}) AS i",
        N_ROWS,
        "'2024-12-31 23:11:00'",
        True,
    ),
    (
        "date",
        "DATE",
        "SELECT '2025-01-01'::date + (i-1) " "FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "'2025-01-01'",
        True,
    ),
    (
        "text",
        "TEXT",
        "SELECT 'txt_' || i FROM generate_series(1, {n}) AS i",
        N_ROWS,
        "'txt_1'",
        True,
    ),
    (
        "time",
        "TIME",
        "SELECT ('00:00:00'::time + (i||' second')::interval) "
        "FROM generate_series(1,{n}) AS i",
        N_ROWS,
        "'00:00:01'",
        False,  # spark does not support time
    ),
    (
        "char",
        "CHAR(3)",
        "SELECT lpad(i::text,3,'0')::char(3) FROM generate_series(1,{n}) AS i",
        N_ROWS,
        "'001'",
        True,
    ),
    (
        "float4",
        "FLOAT4",
        "SELECT i::float4 / 10 FROM generate_series(1,{n}) AS i",
        N_ROWS,
        "0.1",
        False,  # float comparison makes the test harder to automate
    ),
    (
        "float8",
        "FLOAT8",
        "SELECT i::float8 / 10 FROM generate_series(1,{n}) AS i",
        N_ROWS,
        "0.1",
        True,
    ),
]


@pytest.mark.parametrize(
    "case_id, col_type, insert_tmpl, distinct_expected, test_filter, compare_with_spark",
    CASES,
)
def test_identity_partition_write(
    extension,
    s3,
    with_default_location,
    case_id,
    col_type,
    insert_tmpl,
    distinct_expected,
    test_filter,
    compare_with_spark,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
    installcheck,
    spark_session,
):
    """
    1. create a throw-away table with a single column `v <type>`
    2. insert N_ROWS values
    3. check lake_table.files
    4. tidy up
    """
    _setup_schema(pg_conn)
    tbl_name = f"{_rand_table(case_id)}"
    tbl = f"{SCHEMA}.{tbl_name}"

    run_command(
        f"""
        
        -- we currently do not support partitioned writes for pushdown
        SET pg_lake_table.enable_insert_select_pushdown TO false;

        CREATE TABLE {tbl}({PARTITION_COL} {col_type})
        USING iceberg
        WITH (partition_by = '{PARTITION_COL}', autovacuum_enabled = false);
        """,
        pg_conn,
    )

    # insert data
    run_command(
        f"INSERT INTO {tbl} {insert_tmpl.format(n=N_ROWS)};",
        pg_conn,
    )
    pg_conn.commit()

    # check that the Iceberg writer produced the right number of files
    res = run_query(
        f"""
        SELECT count(*),               -- number of data-file rows
               sum(row_count)          -- total logical rows in those files
        FROM   lake_table.files
        WHERE  table_name = '{tbl}'::regclass;
        """,
        pg_conn,
    )
    file_cnt, total_rows = res[0]

    # 1. every logical row must be present
    assert total_rows == N_ROWS

    # 2. the writer must create one file per *distinct* partition value.
    #    (boolean → 2 partitions, everything else → N_ROWS because all values differ)
    assert file_cnt == distinct_expected

    # 3. Make sure the files are written correctly
    files = run_query(
        f"SELECT path, id, row_count FROM lake_table.files WHERE table_name = '{tbl}'::regclass",
        pg_conn,
    )
    for data_file in files:

        file_path = data_file[0]
        id = data_file[1]
        row_count = data_file[2]
        res = run_query(
            f"SELECT DISTINCT {PARTITION_COL} FROM '{file_path}'", pgduck_conn
        )

        # each file should have a distinct value
        assert len(res) == 1

        res_partition = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id='{id}'",
            pg_conn,
        )

        if col_type == "BOOLEAN":
            assert bool(res_partition[0][0]) == bool(res[0][0])
        elif col_type in ("TIMESTAMP", "TIMESTAMPTZ"):
            expected = datetime.datetime.fromisoformat(res_partition[0][0])
            actual = datetime.datetime.fromisoformat(res[0][0])

            if actual.tzinfo is not None:
                expected = expected.replace(tzinfo=datetime.timezone.utc)
            assert expected == actual
        elif col_type == "DATE":
            expected = datetime.date.fromisoformat(res_partition[0][0])
            actual = datetime.date.fromisoformat(res[0][0])
            assert expected == actual
        else:
            assert res_partition[0][0] == res[0][0]

        res = run_query(f"SELECT count(*) FROM '{file_path}'", pgduck_conn)
        # each file should have a distinct value
        assert int(res[0][0]) == int(row_count)

        # register the same table to spark
        # and make sure it can understand our partitioning
        if not installcheck and compare_with_spark:
            metadata_location = run_query(
                f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{tbl_name}' and table_namespace='{SCHEMA}'",
                pg_conn,
            )[0][0]
            # now make sure spark can read the data files with partitioning
            spark_register_table(
                installcheck,
                spark_session,
                f"{tbl_name}",
                f"{SCHEMA}",
                metadata_location,
            )

            spark_query = (
                f"SELECT count(*) FROM {tbl} WHERE {PARTITION_COL} = {test_filter}"
            )

            spark_result = spark_session.sql(spark_query).collect()
            assert spark_result[0][0] == 1
            spark_unregister_table(
                installcheck, spark_session, f"{tbl_name}", f"{SCHEMA}"
            )

    # sanity check: make sure we can read the table correctly
    res = run_query(
        f"SELECT count(*), count(DISTINCT {PARTITION_COL}) FROM {tbl}", pg_conn
    )
    assert len(res) == 1

    assert res[0][0] == N_ROWS
    assert res[0][1] == distinct_expected

    run_command("RESET pg_lake_table.enable_insert_select_pushdown;", pg_conn)
    pg_conn.rollback()


def test_two_column_partition_write(
    pg_conn, s3, with_default_location, extension, pgduck_conn
):
    """
    - creates a table partitioned by *two* columns (a,b)
    - inserts 100 rows that cover 20 distinct (a,b) pairs
    - expects one data-file per pair and all 100 logical rows recorded
    """
    schema = "test_part_writes_multi"
    run_command(f"CREATE SCHEMA IF NOT EXISTS {schema};", pg_conn)

    tbl = _rand_table("test_two_column_partition_write")

    # 1) table
    run_command(
        f"""
        CREATE TABLE {tbl}(a INT, b INT)
        USING iceberg
        WITH (partition_by = 'a,b', autovacuum_enabled = false);
        """,
        pg_conn,
    )

    # 2) data ─ five copies of every (a,b) pair → 5 × 5 × 4 = 100 rows, 20 pairs
    run_command(
        f"""
        -- we currently do not support partitioned writes for pushdown
        SET pg_lake_table.enable_insert_select_pushdown TO false;

        INSERT INTO {tbl}(a,b)
        SELECT a, b
        FROM generate_series(0,4) AS a(a)          -- 5 distinct a-values
        CROSS JOIN generate_series(0,3) AS b(b)    -- 4 distinct b-values
        CROSS JOIN generate_series(1,5) AS _;      -- duplicate each pair 5×
        """,
        pg_conn,
    )

    # 3) verify Iceberg wrote what we expect
    res = run_query(
        f"""
        SELECT count(*),            -- number of physical data files
               sum(row_count)       -- total logical rows across those files
        FROM   lake_table.files
        WHERE  table_name = '{tbl}'::regclass;
        """,
        pg_conn,
    )
    file_cnt, total_rows = res[0]

    assert total_rows == 100  # every row made it
    assert file_cnt == 20  # one file per distinct (a,b) pair

    # 3. Make sure the files are written correctly
    files = run_query(
        f"SELECT path FROM lake_table.files WHERE table_name = '{tbl}'::regclass",
        pg_conn,
    )
    for data_file in files:

        file_path = data_file[0]

        res = run_query(f"SELECT DISTINCT a,b FROM '{file_path}'", pgduck_conn)

        # each file should have a distinct value
        assert len(res) == 1

    run_command("RESET pg_lake_table.enable_insert_select_pushdown;", pg_conn)
    pg_conn.rollback()


def test_string_edge_values(
    installcheck,
    pg_conn,
    spark_session,
    s3,
    with_default_location,
    extension,
    grant_access_to_data_file_partition,
):
    if installcheck:
        return

    tbl_name = "test_string_edge_values"

    run_command(
        f"""
                create table {tbl_name}(a text) using iceberg WITH (partition_by='a');
                insert into {tbl_name} values (null);
                insert into {tbl_name} values ('');
                insert into {tbl_name} values ('hello');
                """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{tbl_name}' and table_namespace='public'",
        pg_conn,
    )[0][0]

    # register table to spark
    spark_register_table(
        installcheck, spark_session, tbl_name, SCHEMA, metadata_location
    )

    # null filter
    spark_query = f"select * from {SCHEMA}.{tbl_name} where a is null"
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] is None

    # empty string filter
    spark_query = f"select * from {SCHEMA}.{tbl_name} where a = ''"
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == ""

    # hello filter
    spark_query = f"select * from {SCHEMA}.{tbl_name} where a = 'hello'"
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == "hello"

    # count
    spark_query = f"select count(*) from {SCHEMA}.{tbl_name}"
    spark_result = spark_session.sql(spark_query).collect()
    assert spark_result[0][0] == 3

    # unregister table from spark
    spark_unregister_table(installcheck, spark_session, tbl_name, SCHEMA)

    pg_conn.rollback()


def _rand_table(prefix: str) -> str:
    """Create a unique table name each time the test runs."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _setup_schema(pg_conn):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", pg_conn)
