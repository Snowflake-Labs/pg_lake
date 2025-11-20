import pytest
import server_params
from utils_pytest import *
import re

# Parameterized data: needs_quote, table_name and rows
pruning_data = [
    (False, "int", "prune_int", [(1, 1), (1, 2), (2, 1), (2, 2)]),
    (True, "text", "prune_text", [("1", "1"), ("1", "2"), ("2", "1"), ("2", "2")]),
    (False, "smallint", "prune_smallint", [(1, 1), (1, 2), (2, 1), (2, 2)]),
    (False, "bigint", "prune_bigint", [(1, 1), (1, 2), (2, 1), (2, 2)]),
    (
        False,
        "numeric",
        "prune_numeric",
        [(1.1, 1.1), (1.1, 1.2), (1.2, 1.1), (1.2, 1.2)],
    ),
    (
        True,
        "varchar",
        "prune_varchar",
        [("a", "a"), ("a", "b"), ("b", "a"), ("b", "b")],
    ),
    (
        True,
        "date",
        "prune_date",
        [
            ("2023-01-01", "2023-01-01"),
            ("2023-01-01", "2023-01-02"),
            ("2023-01-02", "2023-01-01"),
            ("2023-01-02", "2023-01-02"),
        ],
    ),
    (
        True,
        "timestamp",
        "prune_timestamp",
        [
            ("2023-01-01 00:00:00", "2023-01-01 00:00:00"),
            ("2023-01-01 00:00:00", "2023-01-02 00:00:00"),
            ("2023-01-02 00:00:00", "2023-01-01 00:00:00"),
            ("2023-01-02 00:00:00", "2023-01-02 00:00:00"),
        ],
    ),
    (
        True,
        "timestamptz",
        "prune_timestamptz",
        [
            ("2023-01-01 00:00:00+00", "2023-01-01 00:00:00+00"),
            ("2023-01-01 00:00:00+00", "2023-01-02 00:00:00+00"),
            ("2023-01-02 00:00:00+00", "2023-01-01 00:00:00+00"),
            ("2023-01-02 00:00:00+00", "2023-01-02 00:00:00+00"),
        ],
    ),
    # we currently do not store statistics for UUID, once we do, uncomment
    # (True, "uuid", "prune_uuid", [('550e8400-e29b-41d4-a716-446655440000', '550e8400-e29b-41d4-a716-446655440000'), ('550e8400-e29b-41d4-a716-446655440000', '123e4567-e89b-12d3-a456-426614174000'), ('123e4567-e89b-12d3-a456-426614174000', '550e8400-e29b-41d4-a716-446655440000'), ('123e4567-e89b-12d3-a456-426614174000', '123e4567-e89b-12d3-a456-426614174000')]),
]


# this test aims to ensure some common operators like =,>,<,>=,<=,IN,ANY,BETWEEN etc
# is supported for different data types
@pytest.mark.parametrize("needs_quote, column_type, table_name, rows", pruning_data)
def test_simple_data_pruning_for_data_types(
    installcheck,
    s3,
    pg_conn,
    extension,
    with_default_location,
    adjust_object_store_settings,
    polaris_session,
    set_polaris_gucs,
    needs_quote,
    column_type,
    table_name,
    rows,
    create_helper_functions,
):

    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_data_file_pruning;
    
        CREATE TABLE test_data_file_pruning.{table_name} (
            col1 {column_type},
            col2 {column_type}
        ) USING iceberg WITH (autovacuum_enabled='False');

        CREATE TABLE test_data_file_pruning.{table_name}_object_store (
            col1 {column_type},
            col2 {column_type}
        ) USING iceberg WITH (catalog='object_store', autovacuum_enabled='False');

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    for row in rows:
        run_command(
            f"""INSERT INTO test_data_file_pruning.{table_name} (col1, col2) VALUES {row};
                INSERT INTO test_data_file_pruning.{table_name}_object_store (col1, col2) VALUES {row};""",
            pg_conn,
        )
        pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_data_file_pruning", f"{table_name}_object_store"
    )

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    run_command(
        f"""SELECT create_external_iceberg_table('{table_name}', '{table_name}_external', 'test_data_file_pruning', 'test_data_file_pruning');
                    
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

    # polaris is not setup at installcheck tests
    if not installcheck:
        run_command(
            f"""CREATE TABLE test_data_file_pruning.{table_name}_rest (
                    col1 {column_type},
                    col2 {column_type}
                ) USING iceberg WITH (catalog='rest', autovacuum_enabled='False');""",
            pg_conn,
        )
        pg_conn.commit()

        for row in rows:
            run_command(
                f"INSERT INTO test_data_file_pruning.{table_name}_rest (col1, col2) VALUES {row};",
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
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{tbl_name} WHERE col1 = {value_1} AND col2 = {value_2}",
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
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{tbl_name} WHERE col1 IS NULL and col2 IS NULL",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # we don't prune based on NULL values
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_data_file_pruning.{tbl_name} WHERE col1 IS NOT NULL and col2 IS NOT NULL",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # we are effectively having col1=value1, so should prune 2 files
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 >= {value_1} and col1 <= {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    # we are effectively having WHERE false, so should prune all 4 files
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 < {value_1} and col1 > {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "0"

    # we should not prune any files given we cover all values for the col1
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    value_2 = f"'{rows[2][0]}'" if needs_quote else f"{rows[2][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 IN ({value_1}, {value_2})",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    # we should not prune two files given we only cover one value
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 = ANY(ARRAY[{value_1}]::{column_type}[])",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    # should prune 2 files given we only pick 1 value
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 BETWEEN {value_1} and {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    # should prune all files
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 NOT BETWEEN {value_1} and {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "2"

    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    value_2 = f"'{rows[2][0]}'" if needs_quote else f"{rows[2][0]}"
    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 NOT IN ({value_1}, {value_2})",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "0"

    # Normally, you might expect to see we prune 2 data files
    # however our implementation doesn't work with IS DISTINCT FROM
    value_1 = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"

    for tbl_name in table_list:
        results = run_query(
            f"{explain_prefix} SELECT *FROM test_data_file_pruning.{tbl_name} WHERE col1 IS DISTINCT FROM {value_1}",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "4"

    run_command("DROP SCHEMA test_data_file_pruning CASCADE", pg_conn)
    pg_conn.commit()


# Parameterized data: needs_quote, table_name and rows
no_pruning_data = [
    (
        True,
        "jsonb",
        "prune_jsonb",
        [
            ('{"key":"value"}', '{"key":"value"}'),
            ('{"key":"value"}', '{"key":"another"}'),
            ('{"key":"another"}', '{"key":"value"}'),
            ('{"key":"another"}', '{"key":"another"}'),
        ],
    ),
    (
        True,
        "bit(4)",
        "prune_bit",
        [("1010", "1010"), ("1010", "1111"), ("1111", "1010"), ("1111", "1111")],
    ),  #
    (
        True,
        "bit varying(3)",
        "prune_bit_varying",
        [("101", "101"), ("101", "111"), ("111", "101"), ("111", "111")],
    ),
    (
        True,
        "macaddr",
        "prune_macaddr",
        [
            ("08:00:2b:01:02:03", "08:00:2b:01:02:03"),
            ("08:00:2b:01:02:03", "08:00:2b:04:05:06"),
            ("08:00:2b:04:05:06", "08:00:2b:01:02:03"),
            ("08:00:2b:04:05:06", "08:00:2b:04:05:06"),
        ],
    ),
    (
        True,
        "inet",
        "prune_inet",
        [
            ("192.168.1.1", "192.168.1.1"),
            ("192.168.1.1", "192.168.1.2"),
            ("192.168.1.2", "192.168.1.1"),
            ("192.168.1.2", "192.168.1.2"),
        ],
    ),
    (
        True,
        "cidr",
        "prune_cidr",
        [
            ("192.168.1.0/24", "192.168.1.0/24"),
            ("192.168.1.0/24", "192.168.2.0/24"),
            ("192.168.2.0/24", "192.168.1.0/24"),
            ("192.168.2.0/24", "192.168.2.0/24"),
        ],
    ),
    (
        True,
        "tsvector",
        "prune_tsvector",
        [("a:1", "a:1"), ("a:1", "b:2"), ("b:2", "a:1"), ("b:2", "b:2")],
    ),
]


# this test aims to ensure some common data types that are not supported for pruning
# but still works fine in general
@pytest.mark.parametrize("needs_quote, column_type, table_name, rows", no_pruning_data)
def test_simple_data_no_pruning_for_data_types(
    s3,
    pg_conn,
    extension,
    with_default_location,
    needs_quote,
    column_type,
    table_name,
    rows,
    create_helper_functions,
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_data_file_pruning;
        CREATE TABLE test_data_file_pruning.{table_name} (
            col1 {column_type},
            col2 {column_type}
        ) USING iceberg WITH (autovacuum_enabled='False');

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    for row in rows:
        run_command(
            f"INSERT INTO test_data_file_pruning.{table_name} (col1, col2) VALUES {row}",
            pg_conn,
        )
        pg_conn.commit()

    # this should hit two files, prune two files
    value = f"'{rows[0][0]}'" if needs_quote else f"{rows[0][0]}"
    results = run_query(
        f"{explain_prefix} SELECT * FROM test_data_file_pruning.{table_name} WHERE col1 = {value}",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "4"

    run_command("DROP SCHEMA test_data_file_pruning CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_edge_cases(
    s3, pg_conn, extension, with_default_location, create_helper_functions
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_edge_cases;
        CREATE TABLE test_pruning_for_edge_cases.edge_case (
            col1 BIGINT
        ) USING iceberg WITH (autovacuum_enabled='False');

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

    for row in edge_case_integers:
        run_command(
            f"INSERT INTO test_pruning_for_edge_cases.edge_case (col1) VALUES ({row})",
            pg_conn,
        )
        pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('edge_case', 'edge_case_external', 'test_pruning_for_edge_cases', 'test_pruning_for_edge_cases')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for row in edge_case_integers:
        for tbl_name in ["edge_case", "edge_case_external"]:
            results = run_query(
                f"{explain_prefix} SELECT * FROM test_pruning_for_edge_cases.{tbl_name} WHERE col1 = {row}",
                pg_conn,
            )
            assert fetch_data_files_used(results) == "1"

    run_command("DROP SCHEMA test_pruning_for_edge_cases CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_null_values(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_null_values;
        CREATE TABLE test_pruning_for_null_values.nulls (
            col1 BIGINT
        ) USING iceberg WITH (autovacuum_enabled='False');

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    run_command(
        f"INSERT INTO test_pruning_for_null_values.nulls (col1) VALUES (NULL)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO test_pruning_for_null_values.nulls (col1) VALUES (100)",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"INSERT INTO test_pruning_for_null_values.nulls (col1) VALUES (100), (NULL)",
        pg_conn,
    )
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('nulls', 'nulls_external', 'test_pruning_for_null_values', 'test_pruning_for_null_values')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    # when a file only consists of NULL values, we do not keep stats for that
    # so we read that file in any case
    for tbl_name in ["nulls", "nulls_external"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.{tbl_name} WHERE col1 = 1",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "1"

    # now, similar to the above, read all files because this time the filter
    # matches all
    for tbl_name in ["nulls", "nulls_external"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.{tbl_name} WHERE col1 = 100",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "3"

    # IS [NOT] NULL doesn't have impact on the pruning
    for tbl_name in ["nulls", "nulls_external"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.{tbl_name} WHERE col1 IS NULL",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "3"

    # IS [NOT] NULL doesn't have impact on the pruning
    for tbl_name in ["nulls", "nulls_external"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_null_values.{tbl_name} WHERE col1 IS NOT NULL",
            pg_conn,
        )
        assert fetch_data_files_used(results) == "3"

    run_command("DROP SCHEMA test_pruning_for_null_values CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_complex_filters(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_complex_filters;
        CREATE TABLE test_pruning_for_complex_filters.tbl (
            col1 INT, col2 INT, col3 INT
        ) USING iceberg WITH (autovacuum_enabled='False');

    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)

    # Define the possible values for each column
    possible_values = [1, 2, 3]

    # Loop over all combinations of the three columns
    for col1 in possible_values:
        for col2 in possible_values:
            for col3 in possible_values:

                # Build and execute the INSERT command
                sql = (
                    f"INSERT INTO test_pruning_for_complex_filters.tbl (col1, col2, col3) "
                    f"VALUES ({str(col1)}, {str(col2)}, {str(col3)});"
                )
                run_command(sql, pg_conn)
                pg_conn.commit()

    # verify results on a local table
    run_command(
        "CREATE TABLE test_pruning_for_complex_filters.heap AS SELECT * FROM test_pruning_for_complex_filters.tbl",
        pg_conn,
    )
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('tbl', 'tbl_external', 'test_pruning_for_complex_filters', 'test_pruning_for_complex_filters')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    params = [
        # Single row match
        ("(col1, col2, col3) IN ((1,1,1))", 1),
        # Full table scan (no pruning)
        ("col3 IN (1,2,3)", 27),
        # Combination of AND and OR
        ("col1 = 1 AND (col2 = 2 OR col3 = 2)", 5),
        ("col1 = 1 OR col2 = 2 OR col3 = 3", 19),
        ("col1 = 1 OR (col2 = 2 AND col3 = 3)", 11),
        # All rows with col3 = 3 (regardless of col1 and col2)
        ("col3 = 3", 9),
        # Exact match with two possible tuples
        ("(col1, col2) IN ((1,2), (3,3))", 6),
        # No matching rows
        ("col1 = 4", 0),  # 4 is outside the dataset
        # No matching rows
        ("col1 > 4", 0),  # 4 is outside the dataset
        # No matching rows
        ("col1 < 0 OR col2 > 4", 0),  # 4 is outside the dataset
        # Full range check (ensures all are selected)
        ("col1 BETWEEN 1 AND 3 AND col2 BETWEEN 1 AND 3 AND col3 BETWEEN 1 AND 3", 27),
        # A range combined with OR
        ("col1 = 1 OR col2 BETWEEN 1 AND 2", 21),
        # Checking for exact middle row case
        ("col1 = 2 AND col2 = 2 AND col3 = 2", 1),
        # Exact match with multiple values (testing IN with more than two)
        ("(col1, col2, col3) IN ((1,2,2), (2,3,1), (3,1,3))", 3),
        # OR condition combining two different equality checks
        ("(col1 = 1 AND col2 = 1) OR (col2 = 3 AND col3 = 2)", 6),
        # AND conditions testing multiple independent column constraints
        (
            "col1 = 1 AND col2 = 2 AND col3 BETWEEN 1 AND 3",
            3,
        ),  # (1,2,1), (1,2,2), (1,2,3)
        (
            "col1 = 2 AND col2 IN (1,3) AND col3 IN (2,3)",
            4,
        ),  # (2,1,2), (2,1,3), (2,3,2), (2,3,3)
        # Using range conditions
        (
            "col1 BETWEEN 1 AND 2 AND col2 BETWEEN 2 AND 3",
            12,
        ),  # Covers (1,2,X), (1,3,X), (2,2,X), (2,3,X)
        (
            "col1 BETWEEN 2 AND 3 AND col3 BETWEEN 1 AND 2",
            12,
        ),  # Covers (2,X,1), (2,X,2), (3,X,1), (3,X,2)
        # Complex OR with multiple columns involved
        (
            "(col1 = 1 AND col2 BETWEEN 2 AND 3) OR (col2 = 1 AND col3 BETWEEN 2 AND 3)",
            12,
        ),
        # Checking a case where two specific values are forced
        ("col1 IN (1,3) AND col2 IN (2,3)", 12),
    ]

    for query_pushdown in ["on", "off"]:

        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {query_pushdown}",
            pg_conn,
        )
        for param in params:
            filter_to_add, expected_result = param[0], param[1]

            # specific file with a unique filter so only hit a single file
            for tbl_name in ["tbl", "tbl_external"]:
                results = run_query(
                    f"{explain_prefix} SELECT * FROM test_pruning_for_complex_filters.{tbl_name} WHERE {filter_to_add}",
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

    run_command("DROP SCHEMA test_pruning_for_complex_filters CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_non_ascii_chars(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_non_ascii_chars;
        CREATE TABLE test_pruning_for_non_ascii_chars.tbl (
            col1 text
        ) USING iceberg WITH (autovacuum_enabled='False');

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

    for row in rows:
        run_command(
            f"INSERT INTO test_pruning_for_non_ascii_chars.tbl VALUES ('{row}')",
            pg_conn,
        )
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('tbl', 'tbl_external', 'test_pruning_for_non_ascii_chars', 'test_pruning_for_non_ascii_chars')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for row in rows:
        # specific file with a unique filter so only hit a single file
        for tbl_name in ["tbl", "tbl_external"]:
            results = run_query(
                f"{explain_prefix} SELECT * FROM test_pruning_for_non_ascii_chars.{tbl_name} WHERE col1 = '{row}'",
                pg_conn,
            )
            assert int(fetch_data_files_used(results)) == 1

    run_command("DROP SCHEMA test_pruning_for_non_ascii_chars CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_with_add_drop_column(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_with_add_drop_column;
        CREATE TABLE test_pruning_with_add_drop_column.tbl (
            drop_col INT,
            col1 text
        ) USING iceberg WITH (autovacuum_enabled='False');

        -- insert some rows
        INSERT INTO test_pruning_with_add_drop_column.tbl VALUES (1,'1');
        
        ALTER TABLE test_pruning_with_add_drop_column.tbl DROP COLUMN drop_col;
        ALTER TABLE test_pruning_with_add_drop_column.tbl ADD COLUMN col2 TEXT;

        -- insert some rows
        INSERT INTO test_pruning_with_add_drop_column.tbl VALUES ('2','2');
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('tbl', 'tbl_external', 'test_pruning_with_add_drop_column', 'test_pruning_with_add_drop_column')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for tbl_name in ["tbl", "tbl_external"]:
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_with_add_drop_column.{tbl_name} WHERE col1 = '1'",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

        # we cannot prune the first file for col2 only, but we can prune the second file given the range not match
        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_with_add_drop_column.{tbl_name} WHERE col2 = '1'",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_with_add_drop_column.{tbl_name} WHERE col1 = '1' OR col1 = '2'",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 2

    run_command("DROP SCHEMA test_pruning_with_add_drop_column CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_prepared_statement(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_prepared_statement;
        CREATE TABLE test_pruning_for_prepared_statement.tbl (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');

        INSERT INTO test_pruning_for_prepared_statement.tbl VALUES (1);
        INSERT INTO test_pruning_for_prepared_statement.tbl VALUES (2);
        INSERT INTO test_pruning_for_prepared_statement.tbl VALUES (3);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('tbl', 'tbl_external', 'test_pruning_for_prepared_statement', 'test_pruning_for_prepared_statement')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for tbl_name in ["tbl", "tbl_external"]:
        for query_pushdown in ["on", "off"]:

            run_command(
                f"SET LOCAL pg_lake_table.enable_full_query_pushdown TO {query_pushdown}",
                pg_conn,
            )
            run_command(
                f"PREPARE test_param_{query_pushdown}_{tbl_name}(int) AS SELECT * FROM test_pruning_for_prepared_statement.{tbl_name} WHERE col1 = $1;",
                pg_conn,
            )
            for i in range(0, 10):

                results = run_query(
                    f"{explain_prefix} EXECUTE test_param_{query_pushdown}_{tbl_name}({i%3+1})",
                    pg_conn,
                )
                assert int(fetch_data_files_used(results)) == 1

    run_command("DROP SCHEMA test_pruning_for_prepared_statement CASCADE", pg_conn)
    pg_conn.commit()


# we cannot prune deletion files for external iceberg tables
# so this test shouldn't run for external iceberg tables
def test_pruning_for_deletion_files(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_deletion_files;
        CREATE TABLE test_pruning_for_deletion_files.tbl (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');

        -- create 3 batches of data
        INSERT INTO test_pruning_for_deletion_files.tbl SELECT i FROM generate_series(0, 99)i;
        INSERT INTO test_pruning_for_deletion_files.tbl SELECT i FROM generate_series(100, 199)i;
        INSERT INTO test_pruning_for_deletion_files.tbl SELECT i FROM generate_series(200, 200)i;
        
        -- now, delete 2 rows from the first batch
        DELETE FROM test_pruning_for_deletion_files.tbl WHERE col1 IN (0);
        DELETE FROM test_pruning_for_deletion_files.tbl WHERE col1 IN (75);
        

        -- delete 1 row from the second batch
        DELETE FROM test_pruning_for_deletion_files.tbl WHERE col1 IN (150);
        
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # scan all the data
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 3
    assert int(fetch_delete_files_used(results)) == 3

    # scan only the first batch of the data
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl WHERE col1 < 95",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_delete_files_used(results)) == 2

    # scan only the first batch of the data, even even only cover one
    # deletion file with the filter, but that pruning cannot know hence
    # still needs 2 deletion files
    results = run_query(
        f"{explain_prefix} SELECT count(*) FROM test_pruning_for_deletion_files.tbl WHERE col1 < 50",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_delete_files_used(results)) == 2

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

    run_command("DROP SCHEMA test_pruning_for_deletion_files CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_deletes(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_deletes;
        CREATE TABLE test_pruning_deletes.tbl (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');

        -- range of values
        INSERT INTO test_pruning_deletes.tbl VALUES (1), (3);

        -- overlapping ranges
        INSERT INTO test_pruning_deletes.tbl VALUES (4), (5);
        INSERT INTO test_pruning_deletes.tbl VALUES (5), (6);

        -- single value range
        INSERT INTO test_pruning_deletes.tbl VALUES (0), (0);

        -- larger tange
        INSERT INTO test_pruning_deletes.tbl SELECT s FROM generate_series(101, 120) s;
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # Check number of files skipped
    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where col1 = 0",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 0
    assert int(fetch_data_files_skipped(results)) == 1

    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where col1 < 4",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 0
    assert int(fetch_data_files_skipped(results)) == 2

    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where col1 <= 4",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_data_files_skipped(results)) == 2

    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where col1 = 5",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 2
    assert int(fetch_data_files_skipped(results)) == 0

    # No filters
    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 0
    assert int(fetch_data_files_skipped(results)) == 5

    # Redundant filters
    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where true or random() > 0.5",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 0
    assert int(fetch_data_files_skipped(results)) == 5

    # No table filters
    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where random() > 0.5",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 5
    assert int(fetch_data_files_skipped(results)) == 0

    # Joins break scan pruning
    results = run_query(
        f"""
        {explain_prefix}
        delete from test_pruning_deletes.tbl
        using (select s from generate_series(1,3) s)
        where col1 = s and col1 = 0
        """,
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert fetch_data_files_skipped(results) is None

    # Returning breaks scan pruning
    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where col1 = 0 returning *",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert fetch_data_files_skipped(results) is None

    # Now do actual deletes
    run_command(
        f"delete from test_pruning_deletes.tbl where col1 = 0 or col1 = 101", pg_conn
    )
    pg_conn.commit()

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl",
        pg_conn,
    )
    assert results[0][0] == 1
    assert data_file_count(pg_conn, "test_pruning_deletes.tbl") == 5

    # Range delete
    run_command(f"delete from test_pruning_deletes.tbl where col1 < 4", pg_conn)

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl",
        pg_conn,
    )
    assert results[0][0] == 4
    assert data_file_count(pg_conn, "test_pruning_deletes.tbl") == 4

    # Partial delete (file is replaced)
    run_command(f"delete from test_pruning_deletes.tbl where col1 <= 4", pg_conn)

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl",
        pg_conn,
    )
    assert results[0][0] == 5
    assert data_file_count(pg_conn, "test_pruning_deletes.tbl") == 4

    run_command(f"delete from test_pruning_deletes.tbl where col1 = 5", pg_conn)

    results = run_query(
        f"select min(col1) from test_pruning_deletes.tbl",
        pg_conn,
    )
    assert results[0][0] == 6
    assert data_file_count(pg_conn, "test_pruning_deletes.tbl") == 3

    run_command(f"delete from test_pruning_deletes.tbl", pg_conn)

    results = run_query(
        f"select count(*) from test_pruning_deletes.tbl",
        pg_conn,
    )
    assert results[0][0] == 0

    results = run_query(
        f"select count(*) from lake_table.files where table_name = 'test_pruning_deletes.tbl'::regclass",
        pg_conn,
    )
    assert results[0][0] == 0

    run_command("DROP SCHEMA test_pruning_deletes CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_deletes_trigger(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (verbose, format json, analyze) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_deletes;
        CREATE TABLE test_pruning_deletes.tbl (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');

        INSERT INTO test_pruning_deletes.tbl VALUES (1);

        CREATE TABLE test_pruning_deletes.audit_log (
            log_id SERIAL PRIMARY KEY,
            original_value int
        );

        CREATE OR REPLACE FUNCTION log_delete()
        RETURNS TRIGGER AS $$
        BEGIN
            INSERT INTO test_pruning_deletes.audit_log (original_value)
            VALUES (OLD.col1);
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_after_delete
        AFTER DELETE ON test_pruning_deletes.tbl
        FOR EACH ROW EXECUTE FUNCTION log_delete();
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # Check number of files skipped
    results = run_query(
        f"{explain_prefix} delete from test_pruning_deletes.tbl where col1 = 1",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert fetch_data_files_skipped(results) is None

    results = run_query(
        f"select original_value from test_pruning_deletes.audit_log",
        pg_conn,
    )
    assert len(results) == 1
    assert results[0][0] == 1

    run_command("DROP SCHEMA test_pruning_deletes CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_joins(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_joins;
        CREATE TABLE test_pruning_for_joins.tbl_1 (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');

        CREATE TABLE test_pruning_for_joins.tbl_2 (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');


        INSERT INTO test_pruning_for_joins.tbl_1 SELECT i FROM generate_series(0, 99)i;
        INSERT INTO test_pruning_for_joins.tbl_1 SELECT i FROM generate_series(100, 200)i;

        INSERT INTO test_pruning_for_joins.tbl_2 SELECT i FROM generate_series(0, 99)i;
        INSERT INTO test_pruning_for_joins.tbl_2 SELECT i FROM generate_series(100, 200)i;

        
        DELETE FROM test_pruning_for_joins.tbl_1 WHERE col1 IN (0);
        DELETE FROM test_pruning_for_joins.tbl_2 WHERE col1 IN (0);
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('tbl_1', 'tbl_1_external', 'test_pruning_for_joins', 'test_pruning_for_joins')"
    run_command(create_external_iceberg_table_cmd, pg_conn)
    create_external_iceberg_table_cmd = f"SELECT create_external_iceberg_table('tbl_2', 'tbl_2_external', 'test_pruning_for_joins', 'test_pruning_for_joins')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for tbl_postfix in ["", "_external"]:
        # scan all the data
        results = run_query(
            f"{explain_prefix} SELECT count(*) FROM test_pruning_for_joins.tbl_1{tbl_postfix} JOIN test_pruning_for_joins.tbl_2{tbl_postfix} USING (col1)",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 4

        # we cannot prune deletion files for external iceberg tables
        if tbl_postfix != "_external":
            assert int(fetch_delete_files_used(results)) == 2

        # we filter out tbl_2 via filter pushdown on join
        results = run_query(
            f"{explain_prefix} SELECT count(*) FROM test_pruning_for_joins.tbl_1{tbl_postfix} as tbl_1 JOIN test_pruning_for_joins.tbl_2{tbl_postfix} as tbl_2 on (tbl_1.col1 = tbl_2.col1) WHERE tbl_1.col1 = 0",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 2

        # we cannot prune deletion files for external iceberg tables
        if tbl_postfix != "_external":
            assert int(fetch_delete_files_used(results)) == 2

        # we filter out tbl_2 via filter pushdown on join
        results = run_query(
            f"{explain_prefix} SELECT count(*) FROM test_pruning_for_joins.tbl_1{tbl_postfix} as tbl_1 JOIN test_pruning_for_joins.tbl_2{tbl_postfix} as tbl_2  on (tbl_1.col1 = tbl_2.col1) WHERE tbl_1.col1 = 150",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 2

        # we cannot prune deletion files for external iceberg tables
        if tbl_postfix != "_external":
            assert int(fetch_delete_files_used(results)) == 0

    run_command("DROP SCHEMA test_pruning_for_joins CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_insert_select_pushdown(
    s3, pg_conn, extension, with_default_location
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_insert_select_pushdown;
        CREATE TABLE test_pruning_for_insert_select_pushdown.tbl_1 (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');

        CREATE TABLE test_pruning_for_insert_select_pushdown.tbl_2 (
            col1 INT
        ) USING iceberg WITH (autovacuum_enabled='False');


        INSERT INTO test_pruning_for_insert_select_pushdown.tbl_1 SELECT i FROM generate_series(0, 99)i;
        INSERT INTO test_pruning_for_insert_select_pushdown.tbl_1 SELECT i FROM generate_series(100, 200)i;
        
        DELETE FROM test_pruning_for_insert_select_pushdown.tbl_1 WHERE col1 IN (0);
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # scan all the data
    results = run_query(
        f"{explain_prefix} INSERT INTO test_pruning_for_insert_select_pushdown.tbl_2 SELECT * FROM test_pruning_for_insert_select_pushdown.tbl_1",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 2
    assert int(fetch_delete_files_used(results)) == 1

    # scan all the data
    results = run_query(
        f"{explain_prefix} INSERT INTO test_pruning_for_insert_select_pushdown.tbl_2 SELECT * FROM test_pruning_for_insert_select_pushdown.tbl_1 WHERE col1 = 150",
        pg_conn,
    )
    assert int(fetch_data_files_used(results)) == 1
    assert int(fetch_delete_files_used(results)) == 0

    run_command("DROP SCHEMA test_pruning_for_insert_select_pushdown CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_coercions(
    s3, pg_conn, extension, with_default_location, create_helper_functions
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_coercions;
        SET search_path TO test_pruning_for_coercions;

        CREATE TABLE cast_test (
            small_int_col SMALLINT DEFAULT 100,
            int_col INTEGER DEFAULT 100,
            big_int_col BIGINT DEFAULT 100,
            real_col REAL DEFAULT 100,
            double_col DOUBLE PRECISION DEFAULT 100,
            numeric_col NUMERIC(10,2) DEFAULT 100,
            text_col TEXT DEFAULT '100',
            varchar_col VARCHAR(50) DEFAULT '100',
            char_col CHAR(5) DEFAULT '100',
            date_col DATE DEFAULT '2022-01-01',
            timestamp_col TIMESTAMP DEFAULT '2022-01-01',
            uuid_col UUID DEFAULT 'c58c16fc-5bd4-4202-bcb7-3e893416d02d'
        )USING iceberg WITH (autovacuum_enabled='False');

       
    """
    # Run the SQL command using the connection
    run_command(create_table_sql, pg_conn)
    # List of INSERT statements with constant values
    insert_statements = [
        "INSERT INTO cast_test (small_int_col) VALUES (1);",
        "INSERT INTO cast_test (int_col) VALUES (1);",
        "INSERT INTO cast_test (big_int_col) VALUES (1);",
        "INSERT INTO cast_test (real_col) VALUES (1.0);",
        "INSERT INTO cast_test (double_col) VALUES (1.0);",
        "INSERT INTO cast_test (numeric_col) VALUES (1.00);",
        "INSERT INTO cast_test (text_col) VALUES ('1');",
        "INSERT INTO cast_test (varchar_col) VALUES ('1');",
        "INSERT INTO cast_test (char_col) VALUES ('1');",
        "INSERT INTO cast_test (date_col) VALUES ('2024-01-01');",
        "INSERT INTO cast_test (timestamp_col) VALUES ('2024-01-01 00:00:00');",
        "INSERT INTO cast_test (uuid_col) VALUES ('550e8400-e29b-41d4-a716-446655440000');",
    ]
    for insert_stmt in insert_statements:
        run_command(insert_stmt, pg_conn)
        pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT public.create_external_iceberg_table('cast_test', 'cast_test_external', 'test_pruning_for_coercions', 'test_pruning_for_coercions');"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    select_statements = [
        # int conversions
        "SELECT * FROM cast_test WHERE small_int_col = 1::bigint;",
        "SELECT * FROM cast_test WHERE int_col = 1::bigint;",
        "SELECT * FROM cast_test WHERE big_int_col = 1::int;",
        "SELECT * FROM cast_test WHERE big_int_col = CAST(1 AS BIGINT);",
        "SELECT * FROM cast_test WHERE big_int_col = CAST(1 AS INT);",
        "SELECT * FROM cast_test WHERE numeric_col = 1;",
        "SELECT * FROM cast_test WHERE numeric_col = CAST(1 AS SMALLINT);",
        "SELECT * FROM cast_test WHERE real_col = CAST(1.0 AS DOUBLE PRECISION);",
        "SELECT * FROM cast_test WHERE real_col = CAST(1.0 AS NUMERIC);",
        "SELECT * FROM cast_test WHERE numeric_col = CAST(1.0 AS NUMERIC(20,15));",
        "SELECT * FROM cast_test WHERE text_col = '1'::char;",
        "SELECT * FROM cast_test WHERE text_col = '1'::varchar;",
        "SELECT * FROM cast_test WHERE text_col = '1'::varchar(10);",
        "SELECT * FROM cast_test WHERE varchar_col = '1'::text;",
        "SELECT * FROM cast_test WHERE varchar_col = '1';",
        "SELECT * FROM cast_test WHERE varchar_col = CAST('1' AS text);",
        "SELECT * FROM cast_test WHERE date_col = '2024-01-01 00:00:00';",
        "SELECT * FROM cast_test WHERE timestamp_col = '2024-01-01'::DATE;",
        "SELECT * FROM cast_test WHERE date_col >= '2024-01-01'::TIMESTAMP;",
        # "SELECT * FROM cast_test WHERE uuid_col = '550e8400-e29b-41d4-a716-446655440000';",  # Explicit UUID → text
        # "SELECT * FROM cast_test WHERE uuid_col = '550e8400-e29b-41d4-a716-446655440000'::TEXT;",  # Explicit UUID → text
    ]

    for stmt in select_statements:
        results = run_query(
            f"{explain_prefix} {stmt}",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

        # now, run on the external table
        stmt = stmt.replace("cast_test", "cast_test_external")
        results = run_query(
            f"{explain_prefix} {stmt}",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 1

    run_command("DROP SCHEMA test_pruning_for_coercions CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_collations(
    s3, pg_conn, extension, with_default_location, create_helper_functions
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_collations;
        SET search_path TO test_pruning_for_collations;
        CREATE COLLATION s_coll (LOCALE = 'C');

        CREATE TABLE test_table (
            col_default TEXT,       -- Default collation (typically database default)
            col_s_coll TEXT COLLATE s_coll -- Using the custom collation
        ) USING iceberg WITH (autovacuum_enabled='False');

        INSERT INTO test_table (col_default, col_s_coll) VALUES 
            ('hello', 'hello');
        INSERT INTO test_table (col_default, col_s_coll) VALUES 
            ('world', 'world');
       
    """
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT public.create_external_iceberg_table('test_table', 'test_table_external', 'test_pruning_for_collations', 'test_pruning_for_collations')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for tbl_name in ["test_table", "test_table_external"]:

        results = run_query(
            f"{explain_prefix}  SELECT * FROM {tbl_name} WHERE col_default = 'hello'",
            pg_conn,
        )

        # with default collation, apply pruning
        assert int(fetch_data_files_used(results)) == 1

        # with default collation, apply pruning
        results = run_query(
            f"{explain_prefix}  SELECT * FROM {tbl_name} WHERE col_s_coll = 'hello'",
            pg_conn,
        )

        # with non-default collation, do not apply pruning on internal iceberg tables
        # external iceberg tables are unaware of the collations
        if tbl_name == "test_table":
            assert int(fetch_data_files_used(results)) == 2
        else:
            assert int(fetch_data_files_used(results)) == 1

    run_command("DROP SCHEMA test_pruning_for_collations CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_for_domains(
    s3, pg_conn, extension, with_default_location, create_helper_functions
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_domains;
        SET search_path TO test_pruning_for_domains;

        CREATE DOMAIN positive_int AS INT CHECK (VALUE > 0);

        -- Step 2: Create a table using the domain
        CREATE TABLE users (
            age positive_int
        ) USING iceberg WITH (autovacuum_enabled='False');

        INSERT INTO users (age) VALUES (25);
        INSERT INTO users (age) VALUES (30);

               
    """
    run_command(create_table_sql, pg_conn)

    results = run_query(
        f"{explain_prefix}  SELECT * FROM users WHERE age = 25",
        pg_conn,
    )

    # we cannot apply pruning on domain types
    assert int(fetch_data_files_used(results)) == 2

    # with explicit cast, do not apply pruning
    results = run_query(
        f"{explain_prefix}  SELECT * FROM users WHERE age = 25::int",
        pg_conn,
    )

    assert int(fetch_data_files_used(results)) == 2

    pg_conn.rollback()


# we currently do not support pruning based on some types
# still have this behavior in the test
def test_not_pruning_for_types(
    s3, pg_conn, extension, with_default_location, create_helper_functions
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    map_type = create_map_type("int", "int")

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_not_pruning_for_nested_types;
        SET search_path TO test_not_pruning_for_nested_types;

        CREATE TYPE comp_type AS (key int, value int);

        CREATE TABLE users (
            id int,
            m {map_type},
            a int[],
            c comp_type,
            j json,
            jb jsonb

        ) USING iceberg WITH (autovacuum_enabled='False');

        INSERT INTO users VALUES (1, '{{"(1,1)"}}'::{map_type}, ARRAY[1,1,1], (1,1), '{{"1": "1"}}'::JSON, '{{"1": "1"}}'::JSONB);
        INSERT INTO users VALUES (2, '{{"(2,2)"}}'::{map_type}, ARRAY[2,2,2], (2,2), '{{"2": "2"}}'::JSON, '{{"2": "2"}}'::JSONB);

    """
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT public.create_external_iceberg_table('users', 'users_external', 'test_not_pruning_for_nested_types', 'test_not_pruning_for_nested_types')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for tbl_name in ["users", "users_external"]:

        results = run_query(
            f"{explain_prefix}  SELECT * FROM {tbl_name} WHERE m = '{{\"(1,1)\"}}'::{map_type}",
            pg_conn,
        )

        # we can't apply pruning on map types
        assert int(fetch_data_files_used(results)) == 2

        results = run_query(
            f"{explain_prefix}  SELECT * FROM {tbl_name} WHERE a = ARRAY[2,2,2]",
            pg_conn,
        )

        # we can't apply pruning on array types
        assert int(fetch_data_files_used(results)) == 2

        results = run_query(
            f"{explain_prefix}  SELECT * FROM {tbl_name} WHERE c = (2,2)",
            pg_conn,
        )

        # we can't apply pruning on composite types
        assert int(fetch_data_files_used(results)) == 2

        # json types are written as text, so external iceberg tables
        # cannot have json type
        if tbl_name == "users":
            results = run_query(
                f"""{explain_prefix}  SELECT * FROM {tbl_name} WHERE j->>'1' = '1'""",
                pg_conn,
            )

            # we can't apply pruning on json types
            assert int(fetch_data_files_used(results)) == 2

            results = run_query(
                f"""{explain_prefix}  SELECT * FROM {tbl_name} WHERE jb->>'1' = '1'""",
                pg_conn,
            )

            # we can't apply pruning on jsonb operators
            assert int(fetch_data_files_used(results)) == 2

    run_command("DROP SCHEMA test_not_pruning_for_nested_types CASCADE", pg_conn)
    pg_conn.commit()


def test_pruning_with_full_stats_mode(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_with_full_stats_mode;
        SET search_path TO test_pruning_with_full_stats_mode;

        CREATE TABLE pruning_stats_mode_test (
            a text,
            b bytea

        ) USING iceberg WITH (autovacuum_enabled='False', column_stats_mode='full');

        INSERT INTO pruning_stats_mode_test VALUES ('aaaaaa', '\\x0101010101');
        INSERT INTO pruning_stats_mode_test VALUES ('bbbbb', '\\x0202020202');

    """
    run_command(create_table_sql, pg_conn)

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE a = 'aaaaaaaaaaaaaaaa'",
        pg_conn,
    )

    assert int(fetch_data_files_used(results)) == 0

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE b = '\\x0101010101010101'",
        pg_conn,
    )

    assert int(fetch_data_files_used(results)) == 2

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE a = 'bbbbb'",
        pg_conn,
    )

    assert int(fetch_data_files_used(results)) == 1

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE b = '\\x0202020202'",
        pg_conn,
    )

    assert int(fetch_data_files_used(results)) == 2

    pg_conn.rollback()


def test_pruning_with_truncate_stats_mode(
    s3, pg_conn, extension, with_default_location
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_with_truncate_stats_mode;
        SET search_path TO test_pruning_with_truncate_stats_mode;

        CREATE TABLE pruning_stats_mode_test (
            a text,
            b bytea

        ) USING iceberg WITH (autovacuum_enabled='False', column_stats_mode='truncate(2)');

        INSERT INTO pruning_stats_mode_test VALUES ('aaaaaa', '\\x0101010101');
        INSERT INTO pruning_stats_mode_test VALUES ('bbbbb', '\\x0202020202');

    """
    run_command(create_table_sql, pg_conn)

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE a = 'aaaaaaaaaaaaaaaa'",
        pg_conn,
    )

    # 1 file pruned even with truncate
    assert int(fetch_data_files_used(results)) == 1

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE b = '\\x0101010101010101'",
        pg_conn,
    )

    assert int(fetch_data_files_used(results)) == 2

    pg_conn.rollback()


def test_pruning_with_none_stats_mode(s3, pg_conn, extension, with_default_location):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_with_none_stats_mode;
        SET search_path TO test_pruning_with_none_stats_mode;

        CREATE TABLE pruning_stats_mode_test (
            a text,
            b bytea

        ) USING iceberg WITH (autovacuum_enabled='False', column_stats_mode='none');

        INSERT INTO pruning_stats_mode_test VALUES ('aaaaaa', '\\x0101010101');
        INSERT INTO pruning_stats_mode_test VALUES ('bbbbb', '\\x0202020202');

    """
    run_command(create_table_sql, pg_conn)

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE a = 'aaaaaaaaaaaaaaaa'",
        pg_conn,
    )

    # no files pruned as there is no stats collected
    assert int(fetch_data_files_used(results)) == 2

    results = run_query(
        f"{explain_prefix}  SELECT * FROM pruning_stats_mode_test WHERE b = '\\x0101010101010101'",
        pg_conn,
    )

    # no files pruned as there is no stats collected
    assert int(fetch_data_files_used(results)) == 2

    pg_conn.rollback()


def test_pruning_for_inlined_functions(
    s3, pg_conn, extension, with_default_location, create_helper_functions
):
    explain_prefix = "EXPLAIN (verbose, format json) "

    # Create table and insert rows
    create_table_sql = f"""
        CREATE SCHEMA test_pruning_for_inlined_functions;
        SET search_path TO test_pruning_for_inlined_functions;
        CREATE TABLE test_table (
            a int
        ) USING iceberg WITH (autovacuum_enabled='False');

        INSERT INTO test_table VALUES (1);
        INSERT INTO test_table VALUES (2);
        INSERT INTO test_table VALUES (3);


        """
    run_command(create_table_sql, pg_conn)
    pg_conn.commit()

    # now, create the same iceberg table using pg_lake
    # we'd like to make sure pruning works in the same way for
    # pg_lake tables as well
    create_external_iceberg_table_cmd = f"SELECT public.create_external_iceberg_table('test_table', 'test_table_external', 'test_pruning_for_inlined_functions', 'test_pruning_for_inlined_functions')"
    run_command(create_external_iceberg_table_cmd, pg_conn)

    for tbl_name in ["test_table", "test_table_external"]:
        create_function_sql = f"""


            CREATE FUNCTION test_pruning_for_inlined_functions.inline_table_func_{tbl_name}()
            RETURNS SETOF int
            STABLE LANGUAGE SQL AS $$
              SELECT a FROM {tbl_name}
            $$;

            CREATE FUNCTION test_pruning_for_inlined_functions.inline_table_func_filtered_{tbl_name}()
            RETURNS SETOF int
            STABLE LANGUAGE SQL AS $$
              SELECT a FROM {tbl_name} WHERE a > 1
            $$;
        """
        run_command(create_function_sql, pg_conn)

    for tbl_name in ["test_table", "test_table_external"]:

        # should be able to query without filters
        results = run_query(
            f"SELECT sum(a) FROM test_pruning_for_inlined_functions.inline_table_func_{tbl_name}() a",
            pg_conn,
        )

        # should prune based on internal filter
        assert results[0]["sum"] == 6

        results = run_query(
            f"{explain_prefix} SELECT sum(a) FROM test_pruning_for_inlined_functions.inline_table_func_{tbl_name}() a",
            pg_conn,
        )

        # should prune based on internal filter
        assert int(fetch_data_files_used(results)) == 3

        results = run_query(
            f"{explain_prefix}  SELECT * FROM test_pruning_for_inlined_functions.inline_table_func_filtered_{tbl_name}()",
            pg_conn,
        )

        # should prune based on internal filter
        assert int(fetch_data_files_used(results)) == 2

        results = run_query(
            f"{explain_prefix} SELECT * FROM test_pruning_for_inlined_functions.inline_table_func_filtered_{tbl_name}() a WHERE a < 3",
            pg_conn,
        )

        # should prune more based on external filter as well
        assert int(fetch_data_files_used(results)) == 1

    run_command("DROP SCHEMA test_pruning_for_inlined_functions CASCADE", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn):

    run_command(
        f"""

          CREATE OR REPLACE FUNCTION create_external_iceberg_table(
            internal_table_name TEXT,
            external_table_name TEXT,
            source_schema_name TEXT DEFAULT NULL,
            target_schema_name TEXT DEFAULT NULL

        ) RETURNS VOID AS $$
        DECLARE
            effective_source_schema TEXT;
            effective_target_schema TEXT;
            metadata_location_c TEXT;
        BEGIN
            -- Determine the effective source schema
            IF source_schema_name IS NULL OR trim(source_schema_name) = '' THEN
                SELECT current_schema() INTO effective_source_schema;
            ELSE
                effective_source_schema := source_schema_name;
            END IF;

            -- Determine the effective target schema
            IF target_schema_name IS NULL OR trim(target_schema_name) = '' THEN
                SELECT current_schema() INTO effective_target_schema;
            ELSE
                effective_target_schema := target_schema_name;
            END IF;

            -- Fetch metadata location from lake_iceberg.tables
            SELECT metadata_location INTO metadata_location_c
            FROM iceberg_tables
            WHERE table_name = internal_table_name
              AND table_namespace = effective_source_schema;

            -- Check if metadata location is found
            IF metadata_location_c IS NULL THEN
                RAISE EXCEPTION 'Metadata location not found for table %.%', effective_source_schema, internal_table_name;
            END IF;

            -- Create external Iceberg table with the fetched metadata location
            EXECUTE format(
                'CREATE FOREIGN TABLE %I.%I () SERVER pg_lake OPTIONS (path %L)',
                effective_target_schema, external_table_name, metadata_location_c
            );
        END;
        $$ LANGUAGE plpgsql;

""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION create_external_iceberg_table;
""",
        superuser_conn,
    )
    superuser_conn.commit()
