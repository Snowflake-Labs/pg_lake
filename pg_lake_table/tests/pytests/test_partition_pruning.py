import pytest
from datetime import date
from utils_polaris import *
from utils_pytest import *
from spark_utils import *
import re

# Parameterized data: needs_quote, table_name, sample data and partition_by
pruning_data = [
    (False, "smallint", "prune_smallint", [(1, 1), (1, 11), (11, 1), (11, 11)]),
    (False, "int", "prune_int", [(1, 1), (1, 11), (11, 1), (11, 11)]),
    (False, "bigint", "prune_bigint", [(1, 1), (1, 11), (11, 1), (11, 11)]),
    (False, "float", "prune_float", [(1.1, 1.1), (1.1, 2.1), (2.1, 1.1), (2.1, 2.1)]),
    (False, "float8", "prune_float8", [(1, 1), (1, 2), (2, 1), (2, 2)]),
    (
        False,
        "numeric",
        "prune_numeric",
        [(1.1, 1.1), (1.1, 1.2), (1.2, 1.1), (1.2, 1.2)],
    ),
    (
        False,
        "numeric(10,2)",
        "prune_numeric_10_2",
        [(1.1, 1.1), (1.1, 1.2), (1.2, 1.1), (1.2, 1.2)],
    ),
    (
        True,
        "text",
        "prune_text",
        [
            ("aaaaaaaaaaa", "aaaaaaaaaaa"),
            ("aaaaaaaaaaa", "bbbbbbbbbbb"),
            ("bbbbbbbbbbb", "aaaaaaaaaaa"),
            ("bbbbbbbbbbb", "bbbbbbbbbbb"),
        ],
    ),
    (
        True,
        "text",
        "prune_text_short",
        [("a", "a"), ("a", "b"), ("b", "a"), ("b", "b")],
    ),
    (
        True,
        "varchar",
        "prune_varchar",
        [("aaaa", "aaaa"), ("aaaa", "bbbb"), ("bbbb", "aaaa"), ("bbbb", "bbbb")],
    ),
    (
        True,
        "varchar(1)",
        "prune_varchar_1",
        [("a", "a"), ("a", "b"), ("b", "a"), ("b", "b")],
    ),
    (
        True,
        "date",
        "prune_date",
        [
            ("2023-01-01", "2023-01-01"),
            ("2023-01-01", "2024-01-02"),
            ("2024-01-02", "2023-01-01"),
            ("2024-01-02", "2024-01-02"),
        ],
    ),
    (
        True,
        "timestamp",
        "prune_timestamp",
        [
            ("2023-01-01 00:00:00", "2023-01-01 00:00:00"),
            ("2023-01-01 00:00:00", "2024-01-02 00:00:00"),
            ("2024-01-02 00:00:00", "2023-01-01 00:00:00"),
            ("2024-01-02 00:00:00", "2024-01-02 00:00:00"),
        ],
    ),
    (
        True,
        "timestamptz",
        "prune_timestamptz",
        [
            ("2023-01-01 00:00:00+00", "2023-01-01 00:00:00+00"),
            ("2023-01-01 00:00:00+00", "2024-01-02 00:00:00+00"),
            ("2024-01-02 00:00:00+00", "2023-01-01 00:00:00+00"),
            ("2024-01-02 00:00:00+00", "2024-01-02 00:00:00+00"),
        ],
    ),
    (
        True,
        "time",
        "prune_time",
        [
            ("18:36:06.928348", "18:36:06.928348"),
            ("18:36:06.928348", "19:36:07.928348"),
            ("19:36:07.928348", "18:36:06.928348"),
            ("19:36:07.928348", "19:36:07.928348"),
        ],
    ),
    (
        True,
        "bytea",
        "prune_bytea",
        [
            ("\x336538", "\x336538"),
            ("\x336538", "\x336539"),
            ("\x336539", "\x336538"),
            ("\x336539", "\x336539"),
        ],
    ),
    (
        True,
        "bytea",
        "prune_bytea_long",
        [
            ("\x336538336538336538336538323232", "\x336538336538336538336538323232"),
            ("\x336538336538336538336538323232", "\x336539336539336539336539323232"),
            ("\x336539336539336539336539323232", "\x336538336538336538336538323232"),
            ("\x336539336539336539336539323232", "\x336539336539336539336539323232"),
        ],
    ),
    (
        True,
        "uuid",
        "prune_uuid",
        [
            (
                "550e8400-e29b-41d4-a716-446655440000",
                "550e8400-e29b-41d4-a716-446655440000",
            ),
            (
                "550e8400-e29b-41d4-a716-446655440000",
                "123e4567-e89b-12d3-a456-426614174000",
            ),
            (
                "123e4567-e89b-12d3-a456-426614174000",
                "550e8400-e29b-41d4-a716-446655440000",
            ),
            (
                "123e4567-e89b-12d3-a456-426614174000",
                "123e4567-e89b-12d3-a456-426614174000",
            ),
        ],
    ),
]


partition_by = [
    ("identity", "col1, col2"),
    ("truncate", "truncate(10, col1), truncate(10, col2)"),
    ("year", "year(col1), year(col2)"),
    ("month", "month(col1), month(col2)"),
    ("day", "day(col1), day(col2)"),
    ("hour", "hour(col1), hour(col2)"),
    ("bucket", "bucket(10,col1), bucket(30, col2)"),
    ("bucket_truncate", "bucket(10,col1), truncate(10, col2)"),
    ("identity_truncate", "col1, truncate(10, col2)"),
    ("bucket_identity", "bucket(22, col1), col2"),
]


@pytest.mark.parametrize("partition_type, partition_by", partition_by)
@pytest.mark.parametrize("needs_quote, column_type, table_name, rows", pruning_data)
def test_simple_data_pruning_for_data_types(
    installcheck,
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    adjust_object_store_settings,
    polaris_session,
    set_polaris_gucs,
    partition_type,
    partition_by,
    needs_quote,
    column_type,
    table_name,
    rows,
):

    if "identity" in partition_type:
        # we support all types for identity columns
        pass

    if "truncate" in partition_type:
        # supported types for truncate
        if column_type not in (
            "smallint",
            "int",
            "bigint",
            "text",
            "varchar",
            "varchar(1)",
            "bytea",
        ):
            return

    if partition_type in ("year", "month", "day"):
        if column_type not in ("date", "timestamp", "timestamptz"):
            return

    if partition_type in ("hour"):
        if column_type not in ("time", "timestamp", "timestamptz"):
            return

    if "bucket" in partition_type:
        # supports all but not float/float8
        if column_type not in (
            "smallint",
            "int",
            "bigint",
            "numeric",
            "text",
            "varchar",
            "varchar(1)",
            "bytea",
            "time",
            "date",
            "timestamp",
            "timestamptz",
            "uuid",
        ):
            return

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_data_file_pruning;

        CREATE TABLE test_data_file_pruning.{table_name} (
            col1 {column_type},
            col2 {column_type}
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

        CREATE TABLE test_data_file_pruning.{table_name}_object_store (
            col1 {column_type},
            col2 {column_type}
        ) USING iceberg WITH (catalog='object_store', autovacuum_enabled='False', partition_by='{partition_by}');
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # rows is a list/tuple of values like "(1,'a')" etc.
    values_sql = ",".join(str(row) for row in rows)  # comma between each row
    run_command(
        f"""INSERT INTO test_data_file_pruning.{table_name} VALUES {values_sql};
            INSERT INTO test_data_file_pruning.{table_name}_object_store VALUES {values_sql};""",
        pg_conn,
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_data_file_pruning", f"{table_name}_object_store"
    )

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = 'test_data_file_pruning';",
        pg_conn,
    )[0][0]

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    run_command(
        f"""CREATE FOREIGN TABLE test_data_file_pruning.{table_name}_external ()
            SERVER pg_lake OPTIONS (path '{metadata_location}');
                    
            CREATE TABLE test_data_file_pruning.{table_name}_object_store_read_only ()
            USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='{table_name}_object_store');
            """,
        pg_conn,
    )

    table_list = [
        f"{table_name}",
        f"{table_name}_external",
        f"{table_name}_object_store",
        f"{table_name}_object_store_read_only",
    ]

    # polaris is not setup at installcheck tests AND
    # time column is not supported for polaris rest catalog
    if not installcheck and column_type != "time":
        run_command(
            f"""CREATE TABLE test_data_file_pruning.{table_name}_rest (
                    col1 {column_type},
                    col2 {column_type}
                ) USING iceberg WITH (catalog='rest', autovacuum_enabled='False', partition_by='{partition_by}');""",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            f"INSERT INTO test_data_file_pruning.{table_name}_rest VALUES {values_sql};",
            pg_conn,
        )
        pg_conn.commit()

        run_command(
            f"""CREATE TABLE test_data_file_pruning.{table_name}_rest_read_only ()
                USING iceberg WITH (catalog='rest', read_only=True, catalog_table_name='{table_name}_rest');
                """,
            pg_conn,
        )
        pg_conn.commit()

        table_list.extend(
            [
                f"{table_name}_rest",
                f"{table_name}_rest_read_only",
            ]
        )

    # this should hit two files, prune two files
    value = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{tbl_name} WHERE col1 = {value}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    # this should hit one file, prune 3 files
    value_1 = f"'{rows[1][0]}'" if needs_quote else f"{rows[1][0]}"
    value_2 = f"'{rows[1][1]}'" if needs_quote else f"{rows[1][1]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{table_name} WHERE col1 = {value_1} AND col2 = {value_2}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "1"

    # this shouldn't prune any files
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    value_2 = f"'{rows[2][0]}'" if needs_quote else f"{rows[2][0]}"
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{tbl_name} WHERE col1 = {value_1} OR col1 = {value_2}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # this should prune two files
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    value_2 = value_1
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{tbl_name} WHERE col1 = {value_1} OR col1 = {value_2}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    # we don't prune based on NULL values
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{table_name} WHERE col1 IS NULL and col2 IS NULL",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # we don't prune based on NULL values
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{table_name} WHERE col1 IS NOT NULL and col2 IS NOT NULL",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # bucket partitioning only supported for
    # equality operators, so the following tests
    # are not suitable for that
    if "bucket" in partition_type:
        run_command("DROP SCHEMA test_data_file_pruning CASCADE", pg_conn)
        pg_conn.commit()
        return

    # we are effectively having col1=value1, so should prune 2 files
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 >= {value_1} and col1 <= {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    # Normally, you might expect to see we prune 2 data files
    # however our implementation doesn't work with IS DISTINCT FROM
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 IS DISTINCT FROM {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # we should not prune any files given we cover all values for the col1
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    value_2 = f"'{rows[2][0]}'" if needs_quote else f"{rows[2][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 IN ({value_1}, {value_2})",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # we should not prune two files given we only cover one value
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 = ANY(ARRAY[{value_1}]::{column_type}[])",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    # should prune 2 files given we only pick 1 value
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 BETWEEN {value_1} and {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    if "identity" in partition_type:
        # should prune two files that hits col1=value_1
        value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

        for tbl_name in table_list:
            results = run_query(
                f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 NOT BETWEEN {value_1} and {value_1}",
                pg_conn,
            )
            assert fetch_data_files_used(results) == "2"

        # Normally, you might expect to see we prune all data files
        # however our implementation doesn't work with NOT IN
        value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
        value_1 = f"'{rows[2][0]}'" if needs_quote else f"{rows[2][0]}"
        for tbl_name in table_list:
            results = run_query(
                f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 NOT IN ({value_1}, {value_2})",
                pg_conn,
            )
            assert fetch_data_files_used(results) == "2"

        # we are effectively having WHERE false, so should prune all 4 files
        value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
        for tbl_name in table_list:
            results = run_query(
                f"{explain_prefix} SELECT *FROM test_data_file_pruning.{table_name} WHERE col1 < {value_1} and col1 > {value_1}",
                pg_conn,
            )
            assert fetch_data_files_used(results) == "0"

    run_command("DROP SCHEMA test_data_file_pruning CASCADE", pg_conn)
    pg_conn.commit()


col_types = ["date", "timestamp", "timestamptz"]
partition_types = ["year", "month", "day"]


@pytest.mark.parametrize("partition_by", partition_types)
@pytest.mark.parametrize("col_type", col_types)
def test_datetime_partition_pruning_simple(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    col_type,
    partition_by,
):

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # 1. schema + table (partitioned by year(col_date))
    run_command(
        f"""
        CREATE SCHEMA test_year_partition_simple;
        CREATE TABLE  test_year_partition_simple.tbl (
            col_date {col_type}
        ) USING iceberg
        WITH (
            autovacuum_enabled = 'False',
            partition_by       = '{partition_by}(col_date)'
        );
        SET TIME ZONE 'UTC';
    """,
        pg_conn,
    )

    # 2. four rows (one per year → one data-file per year)
    years = [
        "1950-01-01",
        "1956-05-05",
        "1970-01-01",
        "2018-01-01",
        "2019-01-01",
        "2020-01-01",
        "2021-01-01",
    ]
    values_sql = ",".join(f"('{d}')" for d in years)  # no back-slash here
    run_command(
        f"INSERT INTO test_year_partition_simple.tbl VALUES {values_sql};",
        pg_conn,
    )

    # 3. (predicate, expected rows, expected data-files)
    filter_tests = [
        ("col_date = '2020-01-01'", 1, 1),
        ("col_date >= '2020-01-01'", 2, 2),
        (
            "col_date > '2019-12-31'",
            2,
            3 if partition_by == "year" else 2,
        ),  # needs to scan 2019 file for year partitioning
        ("col_date < '2020-01-01'", 5, 5),
        ("col_date BETWEEN '2019-01-01' AND '2020-12-31'", 2, 2),
        ("col_date = '2025-01-01'", 0, 0),  # no 2025 partition
        ("col_date BETWEEN '1819-01-01' AND '1950-01-01'", 1, 1),
        ("col_date BETWEEN '1819-01-01' AND '1951-01-01'", 1, 1),
        ("col_date BETWEEN '1819-01-01' AND '1969-12-31'", 2, 2),
        ("col_date BETWEEN '1819-01-01' AND '1979-12-31'", 3, 3),
        ("col_date >= '1950-01-01'", 7, 7),
        ("col_date > '1950-01-01'", 6, 7),
        ("col_date <= '1950-01-01'", 1, 1),
        ("col_date <= '2018-01-01'", 4, 4),
        ("col_date <  '2018-01-01'", 3, 3),
        ("col_date >  '2021-01-01'", 0, 1),
        ("col_date >= '2018-01-01'", 4, 4),
        (
            "col_date >  '2018-06-01'",
            3,
            4 if partition_by == "year" else 3,
        ),
        ("col_date BETWEEN '2018-01-01' AND '2018-12-31'", 1, 1),
    ]

    # 4. run and assert
    for sql, expected_rows, expected_files in filter_tests:
        plan = run_query(
            f"{explain_prefix} SELECT * FROM test_year_partition_simple.tbl WHERE {sql}",
            pg_conn,
        )
        print(plan)
        assert fetch_data_files_used(plan) == str(expected_files), f"files: {sql}"

        rows = run_query(
            f"SELECT * FROM test_year_partition_simple.tbl WHERE {sql};",
            pg_conn,
        )
        assert len(rows) == expected_rows, f"rows: {sql}"
    run_command("RESET TIME ZONE;", pg_conn)
    pg_conn.rollback()


partition_by_results = [
    ("year", "52"),
    ("month", "635"),
    ("day", "19357"),
    ("hour", "464591"),
]


@pytest.mark.parametrize("partition_by, expected_partition_value", partition_by_results)
def test_timestamptz_partition_pruning(
    pg_conn,
    disable_data_file_pruning,
    with_default_location,
    partition_by,
    expected_partition_value,
    grant_access_to_data_file_partition,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table with partition_by = year(t)
    run_command(
        f"""
        CREATE SCHEMA timestamptz_sc;
        CREATE TABLE timestamptz_sc.t_year(t timestamptz)
        USING iceberg
        WITH (partition_by = '{partition_by}(t)', autovacuum_enabled='False');
    """,
        pg_conn,
    )

    # Insert same UTC time using different session time zones
    run_command("SET TIME ZONE 'UTC'", pg_conn)
    run_command(
        "INSERT INTO timestamptz_sc.t_year VALUES (TIMESTAMPTZ '2022-12-31 23:00:00')",
        pg_conn,
    )

    run_command("SET TIME ZONE 'Europe/Istanbul'", pg_conn)
    run_command(
        "INSERT INTO timestamptz_sc.t_year VALUES (TIMESTAMPTZ '2023-01-01 02:00:00')",
        pg_conn,
    )

    run_command("SET TIME ZONE 'America/New_York'", pg_conn)
    run_command(
        "INSERT INTO timestamptz_sc.t_year VALUES (TIMESTAMPTZ '2022-12-31 18:00:00')",
        pg_conn,
    )

    # all values are stored based on UTC
    res = run_query(
        "SELECT DISTINCT value FROM lake_table.data_file_partition_values WHERE table_name = 'timestamptz_sc.t_year'::regclass",
        pg_conn,
    )
    assert res[0][0] == expected_partition_value

    # Run filter queries and assert which partition years are read
    run_command("SET TIME ZONE 'UTC'", pg_conn)

    # 1. Should scan all the files
    plan = run_query(
        f"{explain_prefix} SELECT * FROM timestamptz_sc.t_year WHERE t < TIMESTAMPTZ '2023-01-01 00:00:00'",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "3"

    results = run_query(
        f"SELECT count(*) FROM timestamptz_sc.t_year WHERE t < TIMESTAMPTZ '2023-01-01 00:00:00'",
        pg_conn,
    )
    assert results[0][0] == 3

    plan = run_query(
        f"{explain_prefix} SELECT * FROM timestamptz_sc.t_year WHERE t >= TIMESTAMPTZ '2023-01-01 00:00:00'",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "0"

    results = run_query(
        f"SELECT count(*) FROM timestamptz_sc.t_year WHERE t >= TIMESTAMPTZ '2023-01-01 00:00:00'",
        pg_conn,
    )
    assert results[0][0] == 0

    plan = run_query(
        f"{explain_prefix} SELECT * FROM timestamptz_sc.t_year WHERE t = TIMESTAMPTZ '2022-12-31 23:00:00+00'",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "3"

    results = run_query(
        f"SELECT count(*) FROM timestamptz_sc.t_year WHERE t = TIMESTAMPTZ '2022-12-31 23:00:00+00'",
        pg_conn,
    )
    assert results[0][0] == 3

    plan = run_query(
        f"{explain_prefix} SELECT * FROM timestamptz_sc.t_year WHERE EXTRACT(YEAR FROM t) = 2022",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "3"  # expected even if pruning not triggered

    results = run_query(
        f"SELECT count(*) FROM timestamptz_sc.t_year WHERE EXTRACT(YEAR FROM t) = 2022",
        pg_conn,
    )
    assert results[0][0] == 3

    plan = run_query(
        f"""{explain_prefix} SELECT * FROM timestamptz_sc.t_year
            WHERE t >= TIMESTAMPTZ '2022-12-01 00:00:00'
            AND t < TIMESTAMPTZ '2023-01-01 00:00:00'""",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "3"

    results = run_query(
        f"""SELECT count(*) FROM timestamptz_sc.t_year
            WHERE t >= TIMESTAMPTZ '2022-12-01 00:00:00'
            AND t < TIMESTAMPTZ '2023-01-01 00:00:00'""",
        pg_conn,
    )
    assert results[0][0] == 3


# it is hard to extend more generic functions
# to have time column, so we have a separate test
def test_pruning_hour_partition_on_time(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    grant_access_to_data_file_partition,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    run_command(
        """
                CREATE SCHEMA hour_on_time;
                CREATE TABLE hour_on_time.tbl (col time) USING iceberg WITH (partition_by = 'hour(col)', autovacuum_enabled='False');

                -- fill in all hours
                INSERT INTO hour_on_time.tbl SELECT generate_series('2000-01-01 00:00:00'::timestamp, '2000-01-31 23:00:00'::timestamp, '1 hour')::time;

        """,
        pg_conn,
    )

    plan = run_query(
        f"""{explain_prefix} SELECT * FROM hour_on_time.tbl
            WHERE col >= '00:00:00'
            AND col < '01:00:00'""",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "1"

    plan = run_query(
        f"""{explain_prefix} SELECT * FROM hour_on_time.tbl
            WHERE col >= '00:00:00'""",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "24"

    plan = run_query(
        f"""{explain_prefix} SELECT * FROM hour_on_time.tbl
            WHERE col >= '20:00:00'""",
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "4"

    plan = run_query(
        f"""{explain_prefix} SELECT * FROM hour_on_time.tbl
            WHERE col IN ('20:00:00', '20:15:00') """,
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "1"

    plan = run_query(
        f"""{explain_prefix} SELECT * FROM hour_on_time.tbl
            WHERE col IN ('20:00:00', '22:15:00') """,
        pg_conn,
    )
    assert fetch_data_files_used(plan) == "2"


# TODO: truncate fails due to overflow
# following https://github.com/apache/iceberg/pull/12969

partition_bys = [("identity", "col1"), ("bucket", "bucket(32, col1)")]


@pytest.mark.parametrize("partition_type, partition_by", partition_bys)
def test_pruning_for_edge_cases(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_edge_cases;
        CREATE TABLE test_pruning_for_edge_cases.edge_case (
            col1 BIGINT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    edge_case_integers = [
        -(2**63),  # Minimum int64 value
        -(2**31),  # Minimum int32 value
        -(2**15),  # Minimum int16 value
        -(2**7),  # Minimum int8 value
        -1,  # Negative one
        0,  # Zero
        1,  # Positive one
        2**7 - 1,  # Maximum int8 value
        2**15 - 1,  # Maximum int16 value
        2**31 - 1,  # Maximum int32 value
        2**63 - 1,  # Maximum int64 value
    ]

    values_sql = ",".join(
        str("(" + str(row) + ")") for row in edge_case_integers
    )  # comma between each row
    insert_command = (
        f"INSERT INTO test_pruning_for_edge_cases.edge_case VALUES {values_sql};"
    )
    run_command(insert_command, pg_conn)

    for row in edge_case_integers:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_edge_cases.edge_case WHERE col1 = {row}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "1", row

    pg_conn.rollback()


def test_pruning_for_null_values(
    s3, disable_data_file_pruning, pg_conn, extension, with_default_location
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_null_values;
        CREATE TABLE test_pruning_for_null_values.nulls (
            col1 BIGINT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by=col1);

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    run_command(
        f"INSERT INTO test_pruning_for_null_values.nulls (col1) VALUES (NULL)",
        pg_conn,
    )

    run_command(
        f"INSERT INTO test_pruning_for_null_values.nulls (col1) VALUES (100)",
        pg_conn,
    )

    run_command(
        f"INSERT INTO test_pruning_for_null_values.nulls (col1) VALUES (100), (NULL)",
        pg_conn,
    )

    # hit partition files with NULLs
    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.nulls WHERE col1 = 1",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "0"

    # now, hit all as NULLs are always added, and we have 2 files with 100
    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.nulls WHERE col1 = 100",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "2"

    # IS NULL does not have impact on the pruning
    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.nulls WHERE col1 IS NULL",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "4"

    # IS [NOT] NULL does have impact on the pruning
    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.nulls WHERE col1 IS NOT NULL",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "2"

    pg_conn.rollback()


partition_by_3_cols = [
    ("identity", "col1, col2, col3"),
    ("truncate", "truncate(10, col1), truncate(10,col2), truncate(10,col3)"),
]


@pytest.mark.parametrize("partition_type, partition_by", partition_by_3_cols)
def test_pruning_for_complex_filters(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_complex_filters;
        CREATE TABLE test_pruning_for_complex_filters.tbl (
            col1 INT, col2 INT, col3 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    # Define the possible values for each column
    possible_values = [10, 20, 30]
    sql = f"INSERT INTO test_pruning_for_complex_filters.tbl (col1, col2, col3) VALUES "

    # Loop over all combinations of the three columns
    for col1 in possible_values:
        for col2 in possible_values:
            for col3 in possible_values:

                # Build and execute the INSERT command
                sql = sql + f"({str(col1)}, {str(col2)}, {str(col3)})"

                if [col1, col2, col3] != [30, 30, 30]:
                    sql = sql + ","
                else:
                    sql = sql + ";"

    run_command(sql, pg_conn)

    # verify results on a local table
    run_command(
        "CREATE TABLE test_pruning_for_complex_filters.heap AS SELECT * FROM test_pruning_for_complex_filters.tbl",
        pg_conn,
    )

    params = [
        # Single row match
        ("(col1, col2, col3) IN ((10,10,10))", 1),
        # Full table scan (no pruning)
        ("col3 IN (10,20,30)", 27),
        # Combination of AND and OR
        ("col1 = 10 AND (col2 = 20 OR col3 = 20)", 5),
        ("col1 = 10 OR col2 = 20 OR col3 = 30", 19),
        ("col1 = 10 OR (col2 = 20 AND col3 = 30)", 11),
        # All rows with col3 = 3 (regardless of col1 and col2)
        ("col3 = 30", 9),
        # Exact match with two possible tuples
        ("(col1, col2) IN ((10,20), (30,30))", 6),
        # No matching rows
        ("col1 = 40", 0),  # 4 is outside the dataset
        # No matching rows
        ("col1 > 40", 0),  # 4 is outside the dataset
        # No matching rows
        ("col1 < 0 OR col2 > 40", 0),  # 4 is outside the dataset
        # Full range check (ensures all are selected)
        (
            "col1 BETWEEN 10 AND 30 AND col2 BETWEEN 10 AND 30 AND col3 BETWEEN 10 AND 30",
            27,
        ),
        # A range combined with OR
        ("col1 = 10 OR col2 BETWEEN 10 AND 20", 21),
        # Checking for exact middle row case
        ("col1 = 20 AND col2 = 20 AND col3 = 20", 1),
        # Exact match with multiple values (testing IN with more than two)
        ("(col1, col2, col3) IN ((10,20,20), (20,30,10), (30,10,30))", 3),
        # OR condition combining two different equality checks
        ("(col1 = 10 AND col2 = 10) OR (col2 = 30 AND col3 = 20)", 6),
        # AND conditions testing multiple independent column constraints
        (
            "col1 = 10 AND col2 = 20 AND col3 BETWEEN 10 AND 30",
            3,
        ),  # (1,2,1), (1,2,2), (1,2,3)
        (
            "col1 = 20 AND col2 IN (10,30) AND col3 IN (20,30)",
            4,
        ),  # (2,1,2), (2,1,3), (2,3,2), (2,3,3)
        # Using range conditions
        (
            "col1 BETWEEN 10 AND 20 AND col2 BETWEEN 20 AND 30",
            12,
        ),  # Covers (1,2,X), (1,3,X), (2,2,X), (2,3,X)
        (
            "col1 BETWEEN 20 AND 30 AND col3 BETWEEN 10 AND 20",
            12,
        ),  # Covers (2,X,1), (2,X,2), (3,X,1), (3,X,2)
        # Complex OR with multiple columns involved
        (
            "(col1 = 10 AND col2 BETWEEN 20 AND 30) OR (col2 = 10 AND col3 BETWEEN 20 AND 30)",
            12,
        ),
        # Checking a case where two specific values are forced
        ("col1 IN (10,30) AND col2 IN (20,30)", 12),
    ]

    for query_pushdown in ["on", "off"]:

        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {query_pushdown}",
            pg_conn,
        )
        for param in params:
            filter_to_add, expected_result = param[0], param[1]

            results = run_query(
                f"{explain_prefix} SELECT * FROM test_pruning_for_complex_filters.tbl WHERE {filter_to_add}",
                pg_conn,
            )
            assert int(fetch_data_files_used(results)) == expected_result

            results = run_query(
                f"SELECT count(*) FROM test_pruning_for_complex_filters.tbl WHERE {filter_to_add}",
                pg_conn,
            )
            assert results[0][0] == expected_result

            results = run_query(
                f"SELECT count(*) FROM test_pruning_for_complex_filters.heap WHERE {filter_to_add}",
                pg_conn,
            )
            assert results[0][0] == expected_result

    pg_conn.rollback()


partition_by_1_col = [
    ("identity", "col1"),
    ("truncate", "truncate(2, col1)"),
    ("bucket", "bucket(22, col1)"),
]


@pytest.mark.parametrize("partition_type, partition_by", partition_by_1_col)
def test_pruning_for_non_ascii_chars(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_non_ascii_chars;
        CREATE TABLE test_pruning_for_non_ascii_chars.tbl (
            col1 text
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    rows = [
        ("Альфа"),  # Cyrillic (A)
        ("Бета"),  # Cyrillic (B)
        ("Γάμμα"),  # Greek (G)
        ("Δέλτα"),  # Greek (D)
        ("ألفا"),  # Arabic (A)
        ("بيتا"),  # Arabic (B)
        ("中文"),  # Chinese (C)
        ("漢字"),  # Chinese (H)
        ("😀"),  # Emoji (smiley)
        ("🚀"),
    ]

    values_sql = ",".join(
        str("('" + row + "')") for row in rows
    )  # comma between each row
    insert_command = (
        f"INSERT INTO test_pruning_for_non_ascii_chars.tbl VALUES {values_sql};"
    )
    run_command(insert_command, pg_conn)

    for row in rows:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_non_ascii_chars.tbl WHERE col1 = '{row}'",
            pg_conn,
        )

        if partition_type == "truncate":
            # we cannot do truncate partitioning on non-ascii chars
            assert int(fetch_data_files_used(results)) == 10
        else:
            assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


partition_by_1_drop_col = [
    ("identity", "col1"),
    ("truncate", "truncate(10,col1)"),
    ("bucket", "bucket(100,col1)"),
]


@pytest.mark.parametrize("partition_type, partition_by", partition_by_1_drop_col)
def test_pruning_with_add_drop_column(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_with_add_drop_column;
        CREATE TABLE test_pruning_with_add_drop_column.tbl (
            drop_col INT,
            col1 text
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

        -- insert some rows
        INSERT INTO test_pruning_with_add_drop_column.tbl VALUES (1,'1'), (2,'2');
        
        ALTER TABLE test_pruning_with_add_drop_column.tbl DROP COLUMN drop_col;
        ALTER TABLE test_pruning_with_add_drop_column.tbl ADD COLUMN col2 TEXT;

        -- insert some rows
        INSERT INTO test_pruning_with_add_drop_column.tbl VALUES ('2','2'),('3','3');
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_with_add_drop_column.tbl WHERE col1 = '1'",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1

    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_with_add_drop_column.tbl WHERE col1 = '1' OR col1 = '2'",
        pg_conn,
    )
    # bucket partitioning is not effective when the same column used twice with OR
    assert int(fetch_data_files_used(results)) == 4 if partition_type == "bucket" else 3

    pg_conn.rollback()


partition_by_1_prepared = [
    ("identity", "col1"),
    ("truncate", "truncate(10, col1)"),
    ("bucket", "bucket(20, col1)"),
]


@pytest.mark.parametrize("partition_type, partition_by", partition_by_1_prepared)
def test_pruning_for_prepared_statement(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_prepared_statement;
        CREATE TABLE test_pruning_for_prepared_statement.tbl (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

        INSERT INTO test_pruning_for_prepared_statement.tbl VALUES (1), (2), (3);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    for query_pushdown in ["on", "off"]:

        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {query_pushdown}",
            pg_conn,
        )
        run_command(
            f"PREPARE test_param_{query_pushdown}_tbl_{partition_type}(int) AS SELECT * FROM test_pruning_for_prepared_statement.tbl WHERE col1 = $1;",
            pg_conn,
        )
        for i in range(0, 10):

            results = run_query(
                f"{explain_prefix} EXECUTE test_param_{query_pushdown}_tbl_{partition_type}({i%3+1})",
                pg_conn,
            )
            assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


partition_by_1_deletion_file = [
    ("identity", "col1"),
    ("truncate", "truncate(100, col1)"),
    ("bucket", "bucket(103, col1)"),
]


@pytest.mark.parametrize("partition_type, partition_by", partition_by_1_deletion_file)
def test_pruning_for_deletion_files(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_deletion_files;
        CREATE TABLE test_pruning_for_deletion_files.tbl (
            col1 INT,
            col2 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

        -- create 3 partitions of data
        INSERT INTO test_pruning_for_deletion_files.tbl
        SELECT 0 AS batch, i FROM generate_series(0,  99) AS g(i)
        UNION ALL
        SELECT 100,            i FROM generate_series(100,199) AS g(i)
        UNION ALL
        SELECT 200,            i FROM generate_series(200,200) AS g(i);

        -- now, delete 2 rows from the first batch
        DELETE FROM test_pruning_for_deletion_files.tbl WHERE col2 IN (0);
        DELETE FROM test_pruning_for_deletion_files.tbl WHERE col2 IN (75);
        

        -- delete 1 row from the second batch
        DELETE FROM test_pruning_for_deletion_files.tbl WHERE col2 IN (150);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    # scan all the data
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 3
    assert int(fetch_delete_files_used(results)) == 3

    # scan only the first batch of the data
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl WHERE col1 = 0",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_delete_files_used(results)) == 2

    # bucket partitioning does not work with non-equality operators
    if partition_type == "bucket":
        return

    # scan only the second batch of the data
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl WHERE col1 >= 100 and col1 < 200",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_delete_files_used(results)) == 1

    # scan only the third batch of the data
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl WHERE col1 >= 200 and col1 < 300",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_delete_files_used(results)) == 0

    # scan only the first two batches of the data together
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl WHERE col1 < 200",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 2
    assert int(fetch_delete_files_used(results)) == 3

    pg_conn.rollback()


def test_pruning_deletes(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_deletes;
        CREATE TABLE test_pruning_deletes.tbl_id (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='col1');

        CREATE TABLE test_pruning_deletes.tbl_trunc (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='truncate(100, col1)');

        -- insert into different partitions
        INSERT INTO test_pruning_deletes.tbl_id VALUES (13), (33), (84);
        INSERT INTO test_pruning_deletes.tbl_trunc VALUES (13), (33), (84), (115), (150), (150), (200), (201);
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    # Check number of files skipped
    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl_id where col1 = 13",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 0
    assert int(fetch_data_files_skipped(results)) == 1

    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl_id where col1 < 50",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 0
    assert int(fetch_data_files_skipped(results)) == 2

    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl_trunc where col1 < 200",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 0
    assert int(fetch_data_files_skipped(results)) == 2

    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl_trunc where col1 <= 200",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_data_files_skipped(results)) == 2

    # Now do actual deletes as a sanity check
    run_command(f"delete from test_pruning_deletes.tbl_id where col1 = 13", pg_conn)

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl_id",
        pg_conn,
    )
    assert results[0][0] == 33

    run_command(f"delete from test_pruning_deletes.tbl_id where col1 < 50", pg_conn)

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl_id",
        pg_conn,
    )
    assert results[0][0] == 84

    run_command(
        f"delete from test_pruning_deletes.tbl_trunc where col1 <= 115", pg_conn
    )

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl_trunc",
        pg_conn,
    )
    assert results[0][0] == 150

    run_command(f"delete from test_pruning_deletes.tbl_trunc where col1 < 200", pg_conn)

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl_trunc",
        pg_conn,
    )
    assert results[0][0] == 200

    pg_conn.rollback()


def test_pruning_deletes_year_month(
    s3, pg_conn, extension, with_default_location, disable_data_file_pruning
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    run_command(
        f"""
		-- Create table with 'ts' column partitioned by year
		CREATE TABLE t_evolve(a int, ts timestamptz) USING iceberg WITH (partition_by='year(ts)', autovacuum_enabled='False');

		-- Insert data for multiple years, creating distinct year partitions
		-- so total of 2 files: 2022 partition and 2023 partition
		INSERT INTO t_evolve VALUES (1, '2022-01-15 10:00:00+00'), (2, '2022-06-20 12:30:00+00');
		INSERT INTO t_evolve VALUES (3, '2023-03-10 08:00:00+00'), (4, '2023-09-05 14:00:00+00');

		-- Evolve Partition Spec to Months Only
		ALTER FOREIGN TABLE t_evolve OPTIONS (SET partition_by 'month(ts)');

		-- Insert new data; this data will now be partitioned by months(ts)
		INSERT INTO t_evolve VALUES (5, '2023-01-02 09:00:00+00'), (6, '2023-01-16 11:00:00+00');
		INSERT INTO t_evolve VALUES (7, '2024-01-25 13:00:00+00'), (8, '2022-02-28 16:00:00+00');
    """,
        pg_conn,
    )

    # will partially match year 2023 and month 2023-01
    # will fully match year 2022 and month 2022-02
    results = run_query(
        f"{explain_prefix} DELETE from t_evolve WHERE ts < '2023-01-15 00:00:00'",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 2
    assert int(fetch_data_files_skipped(results)) == 2


partition_by_1_joins = [("identity", "col1"), ("truncate", "truncate(100, col1)")]


@pytest.mark.parametrize("partition_type, partition_by", partition_by_1_joins)
def test_pruning_for_joins(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_joins;
        CREATE TABLE test_pruning_for_joins.tbl_1 (
            col1 INT, col2 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

        CREATE TABLE test_pruning_for_joins.tbl_2 (
            col1 INT, col2 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');


        INSERT INTO test_pruning_for_joins.tbl_1
        SELECT 0 AS batch, i FROM generate_series(0,  99) AS g(i)
        UNION ALL
        SELECT 100,            i FROM generate_series(100,199) AS g(i);

        INSERT INTO test_pruning_for_joins.tbl_2 SELECT * FROM test_pruning_for_joins.tbl_1;
        
        DELETE FROM test_pruning_for_joins.tbl_1 WHERE col2 IN (0);
        DELETE FROM test_pruning_for_joins.tbl_2 WHERE col2 IN (0);
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    # scan all the data
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_joins.tbl_1 JOIN test_pruning_for_joins.tbl_2 USING (col1)",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 4
    assert int(fetch_delete_files_used(results)) == 2

    # we filter out tbl_2 via filter pushdown on join
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_joins.tbl_1 as tbl_1 JOIN test_pruning_for_joins.tbl_2 as tbl_2 on (tbl_1.col1 = tbl_2.col1) WHERE tbl_1.col1 = 0",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 2
    assert int(fetch_delete_files_used(results)) == 2

    # we filter out tbl_2 via filter pushdown on join
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_joins.tbl_1 as tbl_1 JOIN test_pruning_for_joins.tbl_2 as tbl_2  on (tbl_1.col1 = tbl_2.col1) WHERE tbl_1.col1 = 100",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 2
    assert int(fetch_delete_files_used(results)) == 0

    pg_conn.rollback()


partition_by_1_insert_select_pushdown = [
    ("identity", "col1"),
    ("truncate", "truncate(100, col1)"),
]


@pytest.mark.parametrize(
    "partition_type, partition_by", partition_by_1_insert_select_pushdown
)
def test_pruning_for_insert_select_pushdown(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_insert_select_pushdown;
        CREATE TABLE test_pruning_for_insert_select_pushdown.tbl_1 (
            col1 INT, col2 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

        CREATE TABLE test_pruning_for_insert_select_pushdown.tbl_2 (
            col1 INT, col2 INT
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');

        INSERT INTO test_pruning_for_insert_select_pushdown.tbl_1
        SELECT 0 AS batch, i FROM generate_series(0,  99) AS g(i)
        UNION ALL
        SELECT 100,            i FROM generate_series(100,199) AS g(i);
        
        DELETE FROM test_pruning_for_insert_select_pushdown.tbl_1 WHERE col2 IN (0);
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    # scan all the data
    results = run_query(
        f"{explain_prefix} INSERT INTO test_pruning_for_insert_select_pushdown.tbl_2 SELECT * FROM test_pruning_for_insert_select_pushdown.tbl_1",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 2
    assert int(fetch_delete_files_used(results)) == 1

    results = run_query(
        f"{explain_prefix} INSERT INTO test_pruning_for_insert_select_pushdown.tbl_2 SELECT * FROM test_pruning_for_insert_select_pushdown.tbl_1 WHERE col1 = 100",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_delete_files_used(results)) == 0

    pg_conn.rollback()


def test_pruning_for_serial(
    s3, disable_data_file_pruning, pg_conn, extension, with_default_location
):

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_serial;
        CREATE TABLE test_pruning_for_serial.tbl (
            col1 serial 
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='bucket(10,col1)');

        INSERT INTO test_pruning_for_serial.tbl VALUES (DEFAULT), (DEFAULT), (DEFAULT), (DEFAULT);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    for const_val in ["1", "2", "3", "4"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_serial.tbl WHERE col1 = {const_val}",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


def test_pruning_for_default_values(
    s3, disable_data_file_pruning, pg_conn, extension, with_default_location
):

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_default_values;
        CREATE TABLE test_pruning_for_default_values.tbl (
            col1 text DEFAULT 'pgl' 
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='bucket(32,col1)');

        INSERT INTO test_pruning_for_default_values.tbl VALUES (DEFAULT), (DEFAULT), (DEFAULT), (DEFAULT);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_for_default_values.tbl WHERE col1 = 'pgl'",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


def test_pruning_for_sequence(
    s3, disable_data_file_pruning, pg_conn, extension, with_default_location
):

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_sequence;
        CREATE SEQUENCE test_pruning_for_sequence.sq;
        CREATE TABLE test_pruning_for_sequence.tbl (
            col1 bigint DEFAULT nextval('test_pruning_for_sequence.sq') 
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='bucket(32,col1)');

        INSERT INTO test_pruning_for_sequence.tbl VALUES (DEFAULT), (DEFAULT), (DEFAULT), (DEFAULT);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    for const_val in ["1", "2", "3", "4"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_sequence.tbl WHERE col1 = {const_val}",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


def test_pruning_for_generated_cols(
    s3, disable_data_file_pruning, pg_conn, extension, with_default_location
):

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_generated_cols;
        CREATE SEQUENCE test_pruning_for_generated_cols.sq;
        CREATE TABLE test_pruning_for_generated_cols.tbl (
            col1 bigint DEFAULT nextval('test_pruning_for_generated_cols.sq'),
            col2 bigint  GENERATED ALWAYS AS (col1 * 2) STORED
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='bucket(32,col2)');

        INSERT INTO test_pruning_for_generated_cols.tbl VALUES (DEFAULT), (DEFAULT), (DEFAULT), (DEFAULT);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    for const_val in ["2", "4", "6", "8"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_generated_cols.tbl WHERE col2 = {const_val}",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


column_types = ["int", "smallint", "numeric", "numeric(10,1)"]
partition_by_1_coercions = [
    ("truncate", "truncate(10, col1)"),
    ("bucket", "bucket(20, col1)"),
    ("identity", "col1"),
]


@pytest.mark.parametrize("col_type", column_types)
@pytest.mark.parametrize("partition_type, partition_by", partition_by_1_coercions)
def test_pruning_for_coercions_numeric(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    col_type,
    partition_type,
    partition_by,
):

    # truncate does not support numeric columns
    if partition_type == "truncate" and col_type in ("numeric", "numeric(10,1)"):
        return

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_coercions;
        CREATE TABLE test_pruning_for_coercions.tbl (
            col1 {col_type}
        ) USING iceberg WITH (autovacuum_enabled='False', partition_by='{partition_by}');
        INSERT INTO test_pruning_for_coercions.tbl VALUES (1), (2), (3);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    for const_val in ["1::int", "1::smallint", "1::numeric", "1::numeric(10,1)"]:

        if (
            partition_type == "identity"
            and col_type in ("int", "smallint")
            and "numeric" in const_val
        ):
            # predicate_refuted_by cannot handle this combination
            continue

        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_coercions.tbl WHERE col1 = {const_val}",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


# Time-types to exercise
column_types_temporal = ["date", "timestamp"]

# Basic transforms that are valid for every one of the above types
partition_by_temporal = [
    ("bucket", "bucket(32, col1)"),
    ("year", "year(col1)"),
    ("month", "month(col1)"),
    ("day", "day(col1)"),
]


@pytest.mark.parametrize("col_type", column_types_temporal)
@pytest.mark.parametrize("partition_type, partition_by", partition_by_temporal)
def test_pruning_for_coercions_temporal(
    s3,
    disable_data_file_pruning,
    pg_conn,
    extension,
    with_default_location,
    col_type,
    partition_type,
    partition_by,
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Fix session TZ so casts behave the same everywhere
    run_command("SET TIME ZONE 'UTC';", pg_conn)

    # Fresh schema/table for every parameter combination
    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS test_pruning_for_coercions_ts;
        DROP TABLE     IF EXISTS test_pruning_for_coercions_ts.tbl;
        CREATE TABLE test_pruning_for_coercions_ts.tbl (
            col1 {col_type}
        ) USING iceberg WITH (
            autovacuum_enabled='False',
            partition_by='{partition_by}'
        );
        """,
        pg_conn,
    )

    # Three rows that land in different partitions for month/day
    if col_type == "date":
        values = [
            "DATE '2023-01-01'",
            "DATE '2023-06-01'",
            "DATE '2025-01-01'",
        ]
    elif col_type == "timestamp":
        values = [
            "TIMESTAMP '2023-01-01 00:00:00'",
            "TIMESTAMP '2023-06-01 00:00:00'",
            "TIMESTAMP '2025-01-01 00:00:00'",
        ]

    run_command(
        f"""
        INSERT INTO test_pruning_for_coercions_ts.tbl VALUES
        ({values[0]}), ({values[1]}), ({values[2]});
        """,
        pg_conn,
    )

    # Same literal expressed as each temporal type; ensures cast-to-column works
    const_vals = [
        "DATE '2023-01-01'",
        "TIMESTAMP '2023-01-01 00:00:00'",
    ]

    for const_val in const_vals:
        results = run_query(
            f"{explain_prefix}SELECT * FROM test_pruning_for_coercions_ts.tbl "
            f"WHERE col1 = {const_val}",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

    pg_conn.rollback()


# this test file aims to ensure partition pruning works
@pytest.fixture(scope="module")
def disable_data_file_pruning(superuser_conn):

    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_table.enable_data_file_pruning TO off;",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_table.enable_data_file_pruning",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )
    superuser_conn.commit()


def test_pruning_external_table_with_dropped_partition_field(
    installcheck,
    s3,
    pg_conn,
    extension,
    spark_session,
    spark_table_metadata_location,
    disable_data_file_pruning,
):

    if installcheck:
        return

    # create external table pointing to the spark table metadata location
    run_command(
        f"""
        CREATE FOREIGN TABLE test_pruning_external_table_with_dropped_partition_field ()
        SERVER pg_lake
        OPTIONS (path '{spark_table_metadata_location}', format 'iceberg');
    """,
        pg_conn,
    )

    explain_prefix = "EXPLAIN (verbose, format json) "

    # should prune amogst new files because only the new files is partitioned by name (4 new files are pruned)
    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_external_table_with_dropped_partition_field WHERE name = 'a4'",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 3

    # should prune amongst the old files because only the old files is partitioned by a (1 old file is pruned)
    results = run_query(
        f"{explain_prefix} SELECT * FROM test_pruning_external_table_with_dropped_partition_field WHERE a > 1",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 6

    pg_conn.rollback()


@pytest.fixture(scope="module")
def spark_table_metadata_location(installcheck, spark_session):
    if installcheck:
        yield
        return

    SPARK_TABLE_NAME = "test_pg_lake_iceberg_external_table_dropped_partition_field"
    SPARK_TABLE_NAMESPACE = "public"

    table_name = f"{SPARK_TABLE_NAMESPACE}.{SPARK_TABLE_NAME}"

    spark_session.sql(
        f"CREATE TABLE {table_name} (a int) USING iceberg PARTITIONED BY (a)"
    )
    spark_session.sql(f"INSERT INTO {table_name} SELECT 1 AS a")
    spark_session.sql(f"INSERT INTO {table_name} SELECT 2 AS a")
    spark_session.sql(f"ALTER TABLE {table_name} ADD COLUMN name STRING")
    spark_session.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD a")
    spark_session.sql(f"ALTER TABLE {table_name} ADD PARTITION FIELD truncate(name, 2)")
    spark_session.sql(f"INSERT INTO {table_name} SELECT 0 AS a, ('a' || 10) AS name")
    spark_session.sql(f"INSERT INTO {table_name} SELECT 0 AS a, ('a' || 20) AS name")
    spark_session.sql(f"INSERT INTO {table_name} SELECT 0 AS a, ('a' || 30) AS name")
    spark_session.sql(f"INSERT INTO {table_name} SELECT 0 AS a, ('a' || 40) AS name")
    spark_session.sql(f"INSERT INTO {table_name} SELECT 0 AS a, ('a' || 50) AS name")

    spark_table_metadata_location = (
        spark_session.sql(
            f"SELECT timestamp, file FROM {table_name}.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )

    yield spark_table_metadata_location

    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
