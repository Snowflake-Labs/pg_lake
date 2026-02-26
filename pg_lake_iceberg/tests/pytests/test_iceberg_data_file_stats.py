import pytest
import json
import random
import string
import sys
from utils_pytest import *
from helpers.spark import *

SPARK_TABLE_NAME = "test_pg_lake_iceberg_file_stats"
SPARK_TABLE_NAMESPACE = "public"
SPARK_TABLE_COLUMNS = [
    ["a", "string"],
    ["b", "float"],
    ["c", "double"],
    ["d", "integer"],
    ["e", "long"],
    ["f", "boolean"],
    ["g", "date"],
    ["h", "timestamp"],
    ["j", "numeric(10,2)"],
    ["k", "short"],
    ["l", "timestamp_ntz"],
    ["m", "char(1)"],
    ["n", "varchar(10)"],
    ["p", "array<integer>"],
    ["r", "struct<id: int>"],
    ["s", "map<int,int>"],
]

PG_LAKE_TABLE_NAME = "test_pg_lake_iceberg_file_stats"
PG_LAKE_TABLE_NAMESPACE = "public"
PG_LAKE_TABLE_COLUMNS = [
    ["a", "text"],
    ["b", "float4"],
    ["c", "float8"],
    ["d", "int"],
    ["e", "bigint"],
    ["f", "bool"],
    ["g", "date"],
    ["h", "timestamptz"],
    ["j", "numeric(10,2)"],
    ["k", "smallint"],
    ["l", "timestamp"],
    ["m", '"char"'],
    ["n", "varchar(10)"],
    ["p", "int[]"],
    ["r", "simple_composite"],
    ["s", "map_type.key_int_val_int"],
]


def test_pg_lake_iceberg_table_read_data_file_stats_from_metadata(
    pg_lake_table_metadata_location,
    pg_conn,
    iceberg_extension,
    s3,
    create_helper_functions,
):
    pg_query = f"SELECT lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats('{pg_lake_table_metadata_location}');"

    result = run_query(pg_query, pg_conn)

    assert result == [
        [
            {
                "1": "Amsterdam",
                "2": 37.77397,
                "3": -122.431297,
                "4": 1,
                "5": 2,
                "6": False,
                "7": "2021-01-01",
                "8": "2021-01-01T04:00:00+00:00",
                "9": -6403.01,
                "10": 12,
                "11": "2021-01-01T00:00:00",
                "12": "a",
                "13": "abc",
                # "15": 1,
                # "17": 1,
                # "19": 1,
                # "20": 2,
            },
            {
                "1": "San Francisco",
                "2": 53.11254,
                "3": 6.0989,
                "4": 7,
                "5": 8,
                "6": True,
                "7": "2021-01-04",
                "8": "2021-01-04T04:00:00+00:00",
                "9": 123.01,
                "10": 15,
                "11": "2021-01-04T00:00:00",
                "12": "d",
                "13": "jkl",
                # "15": 12,
                # "17": 4,
                # "19": 7,
                # "20": 8,
            },
        ]
    ]

    pg_conn.rollback()


def test_pg_lake_iceberg_table_reserialize_data_file_stats_from_metadata(
    pg_lake_table_metadata_location,
    pg_conn,
    iceberg_extension,
    s3,
    create_helper_functions,
):
    reserialized_file_stats = run_query(
        f"SELECT lower_bounds, upper_bounds FROM lake_iceberg.reserialize_data_file_stats('{pg_lake_table_metadata_location}');",
        pg_conn,
    )

    assert reserialized_file_stats == [
        [
            {
                "1": "\\x416d7374657264616d",
                "2": "\\x8c181742",
                "3": "\\x3a77bb5e9a9b5ec0",
                "4": "\\x01000000",
                "5": "\\x0200000000000000",
                "6": "\\x00",
                "7": "\\xc4480000",
                "8": "\\x001034c6ceb70500",
                "9": "\\xf63ad3",
                "10": "\\x0c000000",
                "11": "\\x0080e56bcbb70500",
                "12": "\\x61",
                "13": "\\x616263",
                # "15": "\\x01000000",
                # "17": "\\x01000000",
                # "19": "\\x01000000",
                # "20": "\\x02000000",
            },
            {
                "1": "\\x53616e204672616e636973636f",
                "2": "\\x3e735442",
                "3": "\\x304ca60a46651840",
                "4": "\\x07000000",
                "5": "\\x0800000000000000",
                "6": "\\x01",
                "7": "\\xc7480000",
                "8": "\\x0030ba1f0bb80500",
                "9": "\\x300d",
                "10": "\\x0f000000",
                "11": "\\x00a06bc507b80500",
                "12": "\\x64",
                "13": "\\x6a6b6c",
                # "15": "\\x0c000000",
                # "17": "\\x04000000",
                # "19": "\\x07000000",
                # "20": "\\x08000000",
            },
        ]
    ]


def test_pg_lake_iceberg_table_read_data_file_stats_from_catalog(
    pg_lake_table_metadata_location,
    pg_conn,
    extension,
    s3,
    create_helper_functions,
):
    stats_catalog_query = f"""SELECT d.id as file_id, field_id, lower_bound, upper_bound FROM lake_table.files d
                                LEFT JOIN (lake_table.data_file_column_stats s
                                            JOIN lake_table.field_id_mappings m USING(table_name, field_id)
                                            JOIN pg_attribute a ON (m.table_name = a.attrelid AND m.pg_attnum = a.attnum AND NOT a.attisdropped)
                                           ) sma USING(table_name, path)
                              WHERE table_name = '{PG_LAKE_TABLE_NAME}'::regclass 
                              ORDER BY file_id, field_id;"""

    result = run_query(stats_catalog_query, pg_conn)
    assert result == [
        [3, 1, "Amsterdam", "San Francisco"],
        [3, 2, "37.77397", "53.11254"],
        [3, 3, "-122.431297", "6.0989"],
        [3, 4, "1", "7"],
        [3, 5, "2", "8"],
        [3, 6, "0", "1"],
        [3, 7, "2021-01-01", "2021-01-04"],
        [3, 8, "2021-01-01 04:00:00+00", "2021-01-04 04:00:00+00"],
        [3, 9, "-6403.01", "123.01"],
        [3, 10, "12", "15"],
        [3, 11, "2021-01-01 00:00:00", "2021-01-04 00:00:00"],
        [3, 12, "a", "d"],
        [3, 13, "abc", "jkl"],
        # [3, 15, "1", "12"],
        # [3, 17, "1", "4"],
        # [3, 19, "1", "7"],
        # [3, 20, "2", "8"],
    ]

    table_name = f"{PG_LAKE_TABLE_NAMESPACE}.{PG_LAKE_TABLE_NAME}"

    # drop first and the last column and insert some data
    run_command(f"ALTER TABLE {table_name} DROP COLUMN a, DROP COLUMN s;", pg_conn)
    run_command(f"INSERT INTO {table_name}(b) VALUES (37.77397), (53.11254);", pg_conn)

    result = run_query(stats_catalog_query, pg_conn)
    assert result == [
        [3, 2, "37.77397", "53.11254"],
        [3, 3, "-122.431297", "6.0989"],
        [3, 4, "1", "7"],
        [3, 5, "2", "8"],
        [3, 6, "0", "1"],
        [3, 7, "2021-01-01", "2021-01-04"],
        [3, 8, "2021-01-01 04:00:00+00", "2021-01-04 04:00:00+00"],
        [3, 9, "-6403.01", "123.01"],
        [3, 10, "12", "15"],
        [3, 11, "2021-01-01 00:00:00", "2021-01-04 00:00:00"],
        [3, 12, "a", "d"],
        [3, 13, "abc", "jkl"],
        # [3, 15, "1", "12"],
        # [3, 17, "1", "4"],
        [4, 2, "37.77397", "53.11254"],
    ]

    # let vacuum see the new data
    pg_conn.commit()

    # vacuum to compact the files
    run_command_outside_tx(
        [
            "SET pg_lake_table.vacuum_compact_min_input_files = 1;",
            f"VACUUM {table_name}",
        ],
        pg_conn,
    )

    result = run_query(stats_catalog_query, pg_conn)
    assert result == [
        [5, 2, "37.77397", "53.11254"],
        [5, 3, "-122.431297", "6.0989"],
        [5, 4, "1", "7"],
        [5, 5, "2", "8"],
        [5, 6, "0", "1"],
        [5, 7, "2021-01-01", "2021-01-04"],
        [5, 8, "2021-01-01 04:00:00+00", "2021-01-04 04:00:00+00"],
        [5, 9, "-6403.01", "123.01"],
        [5, 10, "12", "15"],
        [5, 11, "2021-01-01 00:00:00", "2021-01-04 00:00:00"],
        [5, 12, "a", "d"],
        [5, 13, "abc", "jkl"],
        # [5, 15, "1", "12"],
        # [5, 17, "1", "4"],
    ]

    # remove data files. then stats should be empty due to foreign key constraint
    run_command(
        f"DELETE FROM lake_table.files WHERE table_name = '{PG_LAKE_TABLE_NAME}'::regclass;",
        pg_conn,
    )

    result = run_query(stats_catalog_query, pg_conn)
    assert result == []

    pg_conn.commit()


def test_spark_iceberg_table_read_data_file_stats(
    installcheck,
    spark_table_metadata_location,
    pg_conn,
    spark_session,
    iceberg_extension,
    s3,
    create_helper_functions,
):
    if installcheck:
        return

    # spark catalog does not return nested fields' stats, so, we only read the top-level fields' stats
    pg_query = f"SELECT lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats('{spark_table_metadata_location}');"

    result = run_query(pg_query, pg_conn)

    assert result == [
        [
            {
                "1": "New York",
                "4": 3,
                "5": 4,
                "6": False,
                "7": "2021-01-02",
                "8": "2021-01-02T04:00:00+00:00",
                "9": 0.0,
                "11": "2021-01-02T00:00:00",
                "12": "g",
                "13": "stu",
                "18": 7,
            },
            {
                "1": "New York",
                "4": 3,
                "5": 4,
                "6": False,
                "7": "2021-01-02",
                "8": "2021-01-02T04:00:00+00:00",
                "9": 0.0,
                "11": "2021-01-02T00:00:00",
                "12": "g",
                "13": "stu",
                "18": 7,
            },
        ],
        [{}, {}],
        [
            {
                "1": "New York",
                "2": 40.7128,
                "3": -74.006,
                "4": 3,
                "5": 4,
                "6": False,
                "7": "2021-01-02",
                "8": "2021-01-02T04:00:00+00:00",
                "9": 0.01,
                "10": 17,
                "11": "2021-01-02T00:00:00",
                "12": "f",
                "13": "pqr",
                "18": 6,
            },
            {
                "1": "New York",
                "2": 40.7128,
                "3": -74.006,
                "4": 3,
                "5": 4,
                "6": False,
                "7": "2021-01-02",
                "8": "2021-01-02T04:00:00+00:00",
                "9": 0.01,
                "10": 17,
                "11": "2021-01-02T00:00:00",
                "12": "f",
                "13": "pqr",
                "18": 6,
            },
        ],
        [
            {
                "1": "Berlin",
                "2": 52.52,
                "3": 13.405,
                "4": 1,
                "5": 2,
                "6": True,
                "7": "2021-01-01",
                "8": "2021-01-01T04:00:00+00:00",
                "9": 0.1,
                "10": 16,
                "11": "2021-01-01T00:00:00",
                "12": "e",
                "13": "mno",
                "18": 5,
            },
            {
                "1": "Berlin",
                "2": 52.52,
                "3": 13.405,
                "4": 1,
                "5": 2,
                "6": True,
                "7": "2021-01-01",
                "8": "2021-01-01T04:00:00+00:00",
                "9": 0.1,
                "10": 16,
                "11": "2021-01-01T00:00:00",
                "12": "e",
                "13": "mno",
                "18": 5,
            },
        ],
        [
            {
                "1": "Amsterdam",
                "2": 37.77397,
                "3": -122.431297,
                "4": 1,
                "5": 2,
                "6": False,
                "7": "2021-01-01",
                "8": "2021-01-01T04:00:00+00:00",
                "9": -6403.01,
                "10": 12,
                "11": "2021-01-01T00:00:00",
                "12": "a",
                "13": "abc",
                "18": 1,
            },
            {
                "1": "San Francisco",
                "2": 53.11254,
                "3": 6.0989,
                "4": 7,
                "5": 8,
                "6": True,
                "7": "2021-01-04",
                "8": "2021-01-04T04:00:00+00:00",
                "9": 123.01,
                "10": 15,
                "11": "2021-01-04T00:00:00",
                "12": "d",
                "13": "jkl",
                "18": 4,
            },
        ],
        [{}, {}],
    ]

    pg_conn.rollback()


def test_spark_iceberg_table_reserialize_data_file_stats(
    installcheck,
    spark_table_metadata_location,
    pg_conn,
    spark_session,
    iceberg_extension,
    s3,
    create_helper_functions,
):
    if installcheck:
        return

    spark_query = f"SELECT lower_bounds, upper_bounds FROM {SPARK_TABLE_NAMESPACE}.{SPARK_TABLE_NAME}.files order by file_path;"
    spark_result = spark_session.sql(spark_query).collect()

    # spark catalog does not return nested fields' stats, so, we only read the top-level fields' stats
    pg_query = f"SELECT lower_bounds, upper_bounds FROM lake_iceberg.reserialize_data_file_stats('{spark_table_metadata_location}') order by path;"
    pg_result = run_query(pg_query, pg_conn)

    assert len(spark_result) > 0
    assert len(spark_result) == len(pg_result)

    row_len = len(spark_result)

    for row_id in range(0, row_len):
        for field_id in range(1, len(SPARK_TABLE_COLUMNS)):
            print(field_id)
            # lower bound
            if field_id in spark_result[row_id]["lower_bounds"]:
                assert spark_result[row_id]["lower_bounds"][field_id] == bytes.fromhex(
                    pg_result[row_id]["lower_bounds"][str(field_id)].replace("\\x", "")
                )
            else:
                # field is not present for null, nan values or nested types
                assert str(field_id) not in pg_result[row_id]["lower_bounds"]

            # upper bound
            if field_id in spark_result[row_id]["upper_bounds"]:
                assert spark_result[row_id]["upper_bounds"][field_id] == bytes.fromhex(
                    pg_result[row_id]["upper_bounds"][str(field_id)].replace("\\x", "")
                )
            else:
                # field is not present for null, nan values or nested types
                assert str(field_id) not in pg_result[row_id]["upper_bounds"]

    pg_conn.rollback()


def test_pg_lake_iceberg_table_skip_min_max(
    pg_lake_table_metadata_location,
    pg_conn,
    iceberg_extension,
    s3,
    create_helper_functions,
    with_default_location,
):
    run_command(
        "CREATE TABLE skip_min_max(col1 text, col2 float, col3 text) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    #  all columns missing min/max
    run_command("INSERT INTO skip_min_max VALUES (NULL, NULL, NULL);", pg_conn)
    pg_conn.commit()

    # some columns missing min/max
    run_command(
        "INSERT INTO skip_min_max VALUES ('1', NULL, NULL), (NULL, 2, NULL), (NULL, NULL, '3');",
        pg_conn,
    )
    pg_conn.commit()

    # float missing min/max due to NaN
    run_command(
        "INSERT INTO skip_min_max VALUES ('4', 'NaN', '5'), ('6', 'Inf', '7');", pg_conn
    )
    pg_conn.commit()

    stats = run_query(
        "SELECT sequence_number, lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='skip_min_max')) ORDER BY sequence_number;",
        pg_conn,
    )

    assert stats == [
        [
            1,
            {},
            {},
        ],
        [
            2,
            {"1": "1", "2": 2, "3": "3"},
            {"1": "1", "2": 2, "3": "3"},
        ],
        [
            3,
            {"1": "4", "3": "5"},
            {"1": "6", "3": "7"},
        ],
    ]

    run_command("DROP TABLE skip_min_max", pg_conn)
    pg_conn.commit()


def test_pg_lake_iceberg_table_special_numerics(
    pg_conn,
    iceberg_extension,
    s3,
    create_helper_functions,
    with_default_location,
):
    run_command(
        "CREATE TABLE special_numerics(col1 numeric, col2 float4, col3 float8) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    # +0.0 and -0.0
    run_command(
        "INSERT INTO special_numerics VALUES (+0.0, +0.0, +0.0), (-0.0, -0.0, -0.0);",
        pg_conn,
    )
    pg_conn.commit()

    # +inf and -inf. (numeric(38, 9) does not support infinity)
    run_command(
        "INSERT INTO special_numerics VALUES (null, '+Infinity', 'inf'), (null, '-Infinity', '-inf');",
        pg_conn,
    )
    pg_conn.commit()

    # NaN (we throw error at csv writer for NaN)
    run_command(
        "INSERT INTO special_numerics VALUES (null, 'NaN', 'NaN'), (null, 'nan', 'nan');",
        pg_conn,
    )
    pg_conn.commit()

    stats = run_query(
        "SELECT sequence_number, lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='special_numerics')) ORDER BY sequence_number;",
        pg_conn,
    )

    assert stats == [
        [
            1,
            {"1": 0, "2": 0, "3": 0},
            {"1": 0, "2": 0, "3": 0},
        ],
        [
            2,
            {},
            {},
        ],
        [
            3,
            {},
            {},
        ],
    ]

    run_command("DROP TABLE special_numerics", pg_conn)
    pg_conn.commit()


def test_pg_lake_iceberg_table_add_drop_columns(
    pg_conn,
    extension,
    s3,
    create_helper_functions,
    with_default_location,
    enable_stats_for_nested_types,
):
    map_type = create_map_type("int", "int")

    run_command(
        f"CREATE TABLE test_ddls(col1 text, col2 int, col3 {map_type}) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    # insert some data
    run_command(
        """INSERT INTO test_ddls VALUES ('abc', 12, array[(1,2)]::map_type.key_int_val_int),
                                        ('def', 13, array[(3,4)]::map_type.key_int_val_int);""",
        pg_conn,
    )
    pg_conn.commit()

    # drop the first column
    run_command("ALTER TABLE test_ddls DROP COLUMN col1;", pg_conn)
    pg_conn.commit()

    # insert some data
    run_command(
        """INSERT INTO test_ddls VALUES (14, array[(5,6)]::map_type.key_int_val_int),
                                        (15, array[(7,8)]::map_type.key_int_val_int);""",
        pg_conn,
    )
    pg_conn.commit()

    # drop the last column
    run_command("ALTER TABLE test_ddls DROP COLUMN col3;", pg_conn)
    pg_conn.commit()

    # insert some data
    run_command(
        "INSERT INTO test_ddls VALUES (16), (17);",
        pg_conn,
    )
    pg_conn.commit()

    # add a column
    run_command("ALTER TABLE test_ddls ADD COLUMN col4 numeric(5,2);", pg_conn)
    pg_conn.commit()

    # insert some data
    run_command(
        "INSERT INTO test_ddls VALUES (18, 167.01), (19, 170.4);",
        pg_conn,
    )
    pg_conn.commit()

    stats = run_query(
        "SELECT sequence_number, lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='test_ddls')) ORDER BY sequence_number;",
        pg_conn,
    )

    # We do not show dropped columns in stats
    # since we are not guaranteed to know about historical snapshots
    assert stats == [
        [
            1,
            {"2": 12},
            {"2": 13},
        ],
        # drop col1
        [3, {"2": 14}, {"2": 15}],
        # drop col3
        [5, {"2": 16}, {"2": 17}],
        # add col4
        [7, {"2": 18, "6": 167.01}, {"2": 19, "6": 170.4}],
    ]

    run_command("DROP TABLE test_ddls", pg_conn)
    pg_conn.commit()


def test_pg_lake_iceberg_table_uuid_column(
    pg_conn,
    extension,
    s3,
    create_helper_functions,
    with_default_location,
):
    table_name = "test_pg_lake_iceberg_table_uuid_column"
    run_command(f"CREATE TABLE {table_name}(col1 uuid) USING iceberg;", pg_conn)
    pg_conn.commit()

    value = "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
    run_command(f"INSERT INTO {table_name} VALUES ('{value}');", pg_conn)
    pg_conn.commit()

    stats = run_query(
        f"SELECT sequence_number, lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='{table_name}')) ORDER BY sequence_number;",
        pg_conn,
    )

    # duckdb query returns null stats for uuid type
    assert stats == [
        [
            1,
            {},
            {},
        ]
    ]

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()


def test_pg_lake_iceberg_table_bytea_column(
    pg_conn,
    extension,
    s3,
    create_helper_functions,
    with_default_location,
):
    table_name = "test_pg_lake_iceberg_table_bytea_column"
    run_command(f"CREATE TABLE {table_name}(col1 bytea) USING iceberg;", pg_conn)
    pg_conn.commit()

    value = "\\x010203"
    run_command(f"INSERT INTO {table_name} VALUES ('{value}');", pg_conn)
    pg_conn.commit()

    stats = run_query(
        f"SELECT sequence_number, lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='{table_name}')) ORDER BY sequence_number;",
        pg_conn,
    )

    # we do not store stats for "bytea" when we use duckdb to fetch the column stats
    # see issue Old repo: issues/957
    assert stats == [
        [
            1,
            {},
            {},
        ]
    ]

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()


def test_pg_lake_iceberg_table_serial_column(
    pg_conn,
    extension,
    s3,
    create_helper_functions,
    with_default_location,
):
    table_name = "test_pg_lake_iceberg_table_serial_column"
    run_command(f"CREATE TABLE {table_name}(col1 serial) USING iceberg;", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {table_name} VALUES (100), (200);", pg_conn)
    pg_conn.commit()

    stats = run_query(
        f"SELECT sequence_number, lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='{table_name}')) ORDER BY sequence_number;",
        pg_conn,
    )

    assert stats == [
        [
            1,
            {"1": 100},
            {"1": 200},
        ]
    ]

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()


def test_pg_lake_iceberg_table_random_values(
    pg_conn,
    extension,
    s3,
    create_helper_functions,
    with_default_location,
):
    table_name = "test_pg_lake_iceberg_table_random_values"
    columns = [
        [
            "a",
            "smallint",
        ],
        ["b", "int"],
        ["c", "bigint"],
        ["d", "float4"],
        ["e", "float8"],
        ["g", "text"],
        ["h", "date"],
        ["i", "timestamp"],
        ["j", "timestamptz"],
        ["k", "time"],
        ["m", "numeric(10,2)"],
        ["n", "numeric(5,-2)"],
        ["o", "numeric(5,8)"],
        ["p", "numeric(15,15)"],
        ["r", "numeric(38,9)"],
        ["s", "varchar(14)"],
        ["u", "boolean"],
        ["v", '"char"'],
    ]

    run_command(
        f"CREATE TABLE {table_name}({', '.join([f'{col[0]} {col[1]}' for col in columns])}) USING iceberg WITH (autovacuum_enabled='False');",
        pg_conn,
    )

    # insert random rows
    for _row_id in range(0, 20):
        random_col_vals = []
        col_names = []

        for [col_name, col_type] in columns:
            random_col_val, _ = generate_random_value(col_type)
            random_col_vals.append(random_col_val)
            col_names.append(col_name)

        run_command(
            f"INSERT INTO {table_name}({', '.join(col_names)}) VALUES ({', '.join(random_col_vals)});",
            pg_conn,
        )
        pg_conn.commit()

    # assert column stats before compaction
    for field_id, [col_name, col_type] in enumerate(columns, start=1):
        # below types do not have min/max functions so we cast to text
        type_name = (
            "text"
            if col_type in ["bytea", "boolean", '"char"', "json", "jsonb"]
            else col_type
        )

        result = run_query(
            f"""SELECT min((lower_bounds->>'{field_id}')::{type_name}) = (SELECT min({col_name}::{type_name}) FROM {table_name}),
                    max((upper_bounds->>'{field_id}')::{type_name}) = (SELECT max({col_name}::{type_name}) FROM {table_name})
                FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'));""",
            pg_conn,
        )
        assert result == [[True, True]]

    # vacuum to compact the files
    run_command_outside_tx([f"VACUUM {table_name}"], pg_conn)

    # assert column stats after compaction
    for field_id, [col_name, col_type] in enumerate(columns, start=1):
        # below types do not have min/max functions so we cast to text
        type_name = (
            "text"
            if col_type in ["bytea", "boolean", '"char"', "json", "jsonb"]
            else col_type
        )

        result = run_query(
            f"""SELECT min((lower_bounds->>'{field_id}')::{type_name}) = (SELECT min({col_name}::{type_name}) FROM {table_name}),
                    max((upper_bounds->>'{field_id}')::{type_name}) = (SELECT max({col_name}::{type_name}) FROM {table_name})
                FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}'));""",
            pg_conn,
        )
        assert result == [[True, True]]

    run_command(f"DROP TABLE {table_name}", pg_conn)
    pg_conn.commit()


def skippedtest_pg_lake_iceberg_table_complex_values(
    superuser_conn,
    enable_stats_for_nested_types,
    extension,
    s3,
    create_helper_functions,
    with_default_location,
):
    run_command(
        """CREATE SCHEMA min_max_stats;
           CREATE TYPE min_max_stats.composite_type AS (key int, value text);
           SELECT map_type.create('int','text');
           CREATE TYPE min_max_stats.nested_composite_type AS (key int, value min_max_stats.composite_type, other_value map_type.key_int_val_text);
           SELECT map_type.create('text','min_max_stats.nested_composite_type');""",
        superuser_conn,
    )

    run_command(
        """CREATE TABLE min_max_stats.test_table (int_col int,
                                                  array_int_col int[],
                                                  composite_col min_max_stats.composite_type,
                                                  nested_composite_col min_max_stats.nested_composite_type,
                                                  map_col map_type.key_int_val_text) USING iceberg;""",
        superuser_conn,
    )

    run_command(
        """INSERT INTO min_max_stats.test_table (int_col, array_int_col, composite_col, nested_composite_col , map_col)
                    SELECT  i, 
                            ARRAY[i, i+1, i+2],
                            ROW(i, 'some_text')::min_max_stats.composite_type,
                            ROW(
                                i,
                                ROW(i, 'nested_value')::min_max_stats.composite_type,
                                ARRAY[(i, 'some_text'), (i + 1, 'another_text')]::map_type.key_int_val_text
                            )::min_max_stats.nested_composite_type,
                            ARRAY[(i, 'text_for_key10'), (i + 12, 'text_for_key20')]::map_type.key_int_val_text
                    FROM generate_series(0, 100) i;""",
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query(
        "SELECT lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='test_table' and table_namespace = 'min_max_stats')) order by sequence_number;",
        superuser_conn,
    )

    assert result == [
        [
            {
                "1": 0,
                "3": 0,
                "5": 0,
                "6": "some_text",
                "8": 0,
                "10": 0,
                "11": "nested_value",
                "13": 0,
                "14": "another_text",
                "16": 0,
                "17": "text_for_key10",
            },
            {
                "1": 100,
                "3": 102,
                "5": 100,
                "6": "some_text",
                "8": 100,
                "10": 100,
                "11": "nested_value",
                "13": 101,
                "14": "some_text",
                "16": 112,
                "17": "text_for_key20",
            },
        ]
    ]

    run_command(
        """UPDATE min_max_stats.test_table SET 
                    int_col = -1,
                    array_int_col = ARRAY[-1,-1,-1],
                    composite_col =  ROW(-1, 'some_text')::min_max_stats.composite_type,
                    nested_composite_col = ROW(
                                                -1,
                                                ROW(-1, 'nested_value')::min_max_stats.composite_type,
                                                ARRAY[
                                                (-1, 'some_text'), 
                                                (-2, 'another_text')
                                                ]::map_type.key_int_val_text
                                            )::min_max_stats.nested_composite_type,
                    map_col =   ARRAY[
                                        (-1, 'text_for_key10'),
                                        (-2, 'text_for_key20')
                                    ]::map_type.key_int_val_text
                    WHERE int_col = 1;""",
        superuser_conn,
    )
    superuser_conn.commit()

    result = run_query(
        "SELECT lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='test_table' and table_namespace = 'min_max_stats')) order by sequence_number;",
        superuser_conn,
    )
    assert result == [
        [
            {
                "1": 0,
                "3": 0,
                "5": 0,
                "6": "some_text",
                "8": 0,
                "10": 0,
                "11": "nested_value",
                "13": 0,
                "14": "another_text",
                "16": 0,
                "17": "text_for_key10",
            },
            {
                "1": 100,
                "3": 102,
                "5": 100,
                "6": "some_text",
                "8": 100,
                "10": 100,
                "11": "nested_value",
                "13": 101,
                "14": "some_text",
                "16": 112,
                "17": "text_for_key20",
            },
        ],
        [
            {
                "1": -1,
                "3": -1,
                "5": -1,
                "6": "some_text",
                "8": -1,
                "10": -1,
                "11": "nested_value",
                "13": -2,
                "14": "another_text",
                "16": -2,
                "17": "text_for_key10",
            },
            {
                "1": -1,
                "3": -1,
                "5": -1,
                "6": "some_text",
                "8": -1,
                "10": -1,
                "11": "nested_value",
                "13": -1,
                "14": "some_text",
                "16": -1,
                "17": "text_for_key20",
            },
        ],
        [{}, {}],
    ]

    superuser_conn.commit()

    run_command_outside_tx(
        [
            "SET pg_lake_table.vacuum_compact_min_input_files = 1;",
            f"VACUUM min_max_stats.test_table",
        ],
        superuser_conn,
    )

    result = run_query(
        "SELECT lower_bounds, upper_bounds FROM lake_iceberg.data_file_stats((SELECT metadata_location FROM iceberg_tables where table_name ='test_table' and table_namespace = 'min_max_stats')) order by sequence_number DESC LIMIT 1;",
        superuser_conn,
    )

    assert result == [
        [
            {
                "1": -1,
                "3": -1,
                "5": -1,
                "6": "some_text",
                "8": -1,
                "10": -1,
                "11": "nested_value",
                "13": -2,
                "14": "another_text",
                "16": -2,
                "17": "text_for_key10",
            },
            {
                "1": 100,
                "3": 102,
                "5": 100,
                "6": "some_text",
                "8": 100,
                "10": 100,
                "11": "nested_value",
                "13": 101,
                "14": "some_text",
                "16": 112,
                "17": "text_for_key20",
            },
        ]
    ]

    run_command("DROP SCHEMA min_max_stats CASCADE;", superuser_conn)

    superuser_conn.commit()


@pytest.fixture(scope="module")
def spark_table_metadata_location(installcheck, spark_session):
    if installcheck:
        yield
        return

    table_name = f"{SPARK_TABLE_NAMESPACE}.{SPARK_TABLE_NAME}"

    spark_session.sql(
        f"""CREATE TABLE tmp_table(
            {', '.join([f'{col[0]} {col[1]}' for col in SPARK_TABLE_COLUMNS])}
            ) USING iceberg
                    """
    )

    spark_session.sql(
        """INSERT INTO tmp_table VALUES ('Amsterdam', 52.371807, 4.896029, 1, 2, true,
                                         date '2021-01-01', timestamp '2021-01-01 00:00:00-04:00',
                                         -6403.01, 12, timestamp_ntz '2021-01-01 00:00:00',
                                         'a', 'abc', array(1, 2, 3), named_struct('id', 1), map(1, 2)),
                                        ('San Francisco', 37.773972, -122.431297, 3, 4, false,
                                         date '2021-01-02', timestamp '2021-01-02 00:00:00-04:00',
                                         123.01, 13, timestamp_ntz '2021-01-02 00:00:00',
                                         'b', 'def', array(4, 5, 6), named_struct('id', 2), map(3, 4)),
                                        ('Drachten', 53.11254, 6.0989, 5, 6, true,
                                         date '2021-01-03', timestamp '2021-01-03 00:00:00',
                                         1, 14, timestamp_ntz '2021-01-03 00:00:00',
                                         'c', 'ghi', array(7, 8, 9), named_struct('id', 3), map(5, 6)),
                                        ('Paris', 48.864716, 2.349014, 7, 8, false,
                                         date '2021-01-04', timestamp '2021-01-04 00:00:00-04:00',
                                         7.0, 15, timestamp_ntz '2021-01-04 00:00:00',
                                         'd', 'jkl', array(10, 11, 12), named_struct('id', 4), map(7, 8))"""
    )

    spark_session.sql(
        f"""CREATE TABLE {table_name}(
            {', '.join([f'{col[0]} {col[1]}' for col in SPARK_TABLE_COLUMNS])}
            ) USING iceberg TBLPROPERTIES('write.delete.mode' = 'merge-on-read')"""
    )

    spark_session.sql(f"INSERT INTO {table_name} SELECT * FROM tmp_table")

    spark_session.sql("DROP TABLE IF EXISTS tmp_table")

    spark_session.sql(
        f"""INSERT INTO {table_name} VALUES ('Berlin', 52.5200, 13.4050, 1, 2, true,
                                             date '2021-01-01', timestamp '2021-01-01 00:00:00-04:00',
                                             0.10, 16, timestamp_ntz '2021-01-01 00:00:00',
                                             'e', 'mno', array(13, 14, 15), named_struct('id', 5),
                                             map(1, 2))"""
    )

    spark_session.sql(
        f"""INSERT INTO {table_name} VALUES ('New York', 40.7128, -74.0060, 3, 4, false,
                                             date '2021-01-02', timestamp '2021-01-02 00:00:00-04:00',
                                             0.01, 17, timestamp_ntz '2021-01-02 00:00:00',
                                             'f', 'pqr', array(16, 17, 18), named_struct('id', 6),
                                             map(3, 4))"""
    )

    spark_session.sql(f"DELETE FROM {table_name} WHERE a = 'Amsterdam'")

    # insert all nulls
    spark_session.sql(
        f"INSERT INTO {table_name} VALUES (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)"
    )

    # insert nans and nulls
    spark_session.sql(
        f"""INSERT INTO {table_name} VALUES ('New York', CAST('NaN' as float), CAST('NaN' as double), 3, 4, false,
                                             date '2021-01-02', timestamp '2021-01-02 00:00:00-04:00',
                                             0.0, null, timestamp_ntz '2021-01-02 00:00:00',
                                             'g', 'stu', array(19, 20, 21), named_struct('id', 7), map(5, 6))"""
    )

    spark_table_metadata_location = (
        spark_session.sql(
            f"SELECT timestamp, file FROM {table_name}.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )

    yield spark_table_metadata_location

    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture(scope="function")
def enable_stats_for_nested_types(
    installcheck, extension, app_user, pg_conn, superuser_conn, with_default_location
):
    run_command_outside_tx(
        [
            # we have the ability to collect stats for the nested types
            # but there are complications with using it. So, we enable
            # this on the tests to make sure we don't break it, but currently
            # disabled in the production systems
            "ALTER SYSTEM SET pg_lake_iceberg.enable_stats_collection_for_nested_types TO on;",
            "SELECT pg_reload_conf();",
        ],
        superuser_conn,
    )

    yield

    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_iceberg.enable_stats_collection_for_nested_types;",
            "SELECT pg_reload_conf();",
        ],
        superuser_conn,
    )


@pytest.fixture(scope="function")
def pg_lake_table_metadata_location(
    installcheck,
    extension,
    app_user,
    pg_conn,
    superuser_conn,
    with_default_location,
    enable_stats_for_nested_types,
):
    table_name = f"{PG_LAKE_TABLE_NAMESPACE}.{PG_LAKE_TABLE_NAME}"

    run_command(
        f"""GRANT SELECT ON lake_iceberg.tables TO {app_user};
            GRANT SELECT ON lake_table.field_id_mappings TO {app_user};
            GRANT SELECT, DELETE ON lake_table.files TO {app_user};
            GRANT SELECT ON lake_table.data_file_column_stats TO {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()

    run_command("CREATE EXTENSION IF NOT EXISTS pg_map", pg_conn)

    run_command("CREATE TYPE simple_composite AS (id int)", pg_conn)

    map_typename = create_map_type("int", "int")
    assert map_typename == "map_type.key_int_val_int"

    run_command(
        f"""CREATE TABLE {table_name}(
            {', '.join([f'{col[0]} {col[1]}' for col in PG_LAKE_TABLE_COLUMNS])}
            ) USING iceberg""",
        pg_conn,
    )

    run_command(
        f"""INSERT INTO {table_name} VALUES ('Amsterdam', 52.371807, 4.896029, 1, 2, true,
                                         '2021-01-01', '2021-01-01 00:00:00-04:00',
                                         -6403.01, 12, '2021-01-01 00:00:00',
                                         'a', 'abc', array[1, 2, 3], row(1), array[(1,2)]::map_type.key_int_val_int),
                                        ('San Francisco', 37.773972, -122.431297, 3, 4, false,
                                         '2021-01-02', '2021-01-02 00:00:00-04:00',
                                         123.01, 13, '2021-01-02 00:00:00',
                                         'b', 'def', array[4, 5, 6], row(2), array[(3,4)]::map_type.key_int_val_int),
                                        ('Drachten', 53.11254, 6.0989, 5, 6, true,
                                         '2021-01-03', '2021-01-03 00:00:00',
                                         1, 14, '2021-01-03 00:00:00',
                                         'c', 'ghi', array[7, 8, 9], row(3), array[(5,6)]::map_type.key_int_val_int),
                                        ('Paris', 48.864716, 2.349014, 7, 8, false,
                                         '2021-01-04', '2021-01-04 00:00:00-04:00',
                                         7.0, 15, '2021-01-04 00:00:00',
                                         'd', 'jkl', array[10, 11, 12], row(4), array[(7,8)]::map_type.key_int_val_int)""",
        pg_conn,
    )
    pg_conn.commit()

    pg_lake_table_metadata_location = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = '{PG_LAKE_TABLE_NAME}'",
        pg_conn,
    )[0][0]

    yield pg_lake_table_metadata_location

    run_command(f"DROP TABLE IF EXISTS {table_name}", pg_conn)
    run_command("DROP TYPE IF EXISTS simple_composite", pg_conn)
    pg_conn.commit()

    run_command(
        f"""REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
            REVOKE SELECT ON lake_table.field_id_mappings FROM {app_user};
            REVOKE SELECT, DELETE ON lake_table.files FROM {app_user};
            REVOKE SELECT ON lake_table.data_file_column_stats FROM {app_user};""",
        superuser_conn,
    )
    superuser_conn.commit()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn, s3, iceberg_extension):

    run_command(
        f"""
        
        CREATE OR REPLACE FUNCTION lake_iceberg.reserialize_data_file_stats(metadataUri text)
        RETURNS TABLE(path text, sequence_number bigint, lower_bounds json, upper_bounds json)
        LANGUAGE C STRICT
        AS 'pg_lake_iceberg', $$pg_lake_reserialize_data_file_stats$$;
        
        CREATE OR REPLACE FUNCTION lake_iceberg.serde_value(value anyelement, iceberg_scalar_type text)
        RETURNS anyelement
        LANGUAGE C STRICT
        AS 'pg_lake_iceberg', $$pg_lake_serde_value$$;
""",
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    superuser_conn.rollback()

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION IF EXISTS lake_iceberg.reserialize_data_file_stats;
        DROP FUNCTION IF EXISTS lake_iceberg.serde_value
""",
        superuser_conn,
    )

    superuser_conn.commit()
