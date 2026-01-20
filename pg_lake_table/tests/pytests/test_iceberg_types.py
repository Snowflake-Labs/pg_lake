import pytest
import psycopg2
from utils_pytest import *
import json
import textwrap


def test_iceberg_base_types(pg_conn, s3, extension, create_helper_functions):
    run_command(
        """
                CREATE SCHEMA test_iceberg_base_types;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_iceberg_base_types"
    # should be able to create a writable table with location
    run_command(
        f"""
            CREATE FOREIGN TABLE test_iceberg_base_types."iceberg base types" (
                    "boolean col" BOOLEAN DEFAULT true,
                    blob_col BYTEA DEFAULT decode('00001234', 'hex'),
                    tiny_int_col SMALLINT,
                    smallint_col SMALLINT DEFAULT 1+1,
                    int_col INTEGER NOT NULL DEFAULT 1,
                    bigint_col BIGINT DEFAULT 100,
                    utinyint_col SMALLINT,
                    usmallint_col INTEGER,
                    uinteger_col BIGINT,
                    uint64_col NUMERIC(20, 0) DEFAULT NULL,
                    float4_col REAL DEFAULT 3.14,
                    float8_col DOUBLE PRECISION DEFAULT 2.718281828,
                    varchar_col VARCHAR DEFAULT 'test' || 'as',
                    decimal_col NUMERIC DEFAULT random(),
                    uuid_col UUID DEFAULT '123e4567-e89b-12d3-a456-426614174000',
                    hugeint_col NUMERIC(38, 0),
                    uhugeint_col NUMERIC(38, 0),
                    timestamp_ns_col TIMESTAMP,
                    timestamp_ms_col TIMESTAMP,
                    timestamp_s_col TIMESTAMP,
                    timestamp_col TIMESTAMP DEFAULT '2024-01-01',
                    timestamp_tz_col TIMESTAMPTZ DEFAULT now(),
                    time_col TIME DEFAULT '12:34:56',
                    date_col DATE DEFAULT '2024-01-01',
                    escaping_varchar_col VARCHAR DEFAULT 'test"  \\ "dummy_A!"@#$%^&* <>?`~'
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}');


    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'iceberg base types' and table_namespace = 'test_iceberg_base_types'",
        pg_conn,
    )
    assert len(results) == 1
    print(results)
    metadata_path = results[0][0]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    # Define the expected fields
    expected_fields = [
        {
            "id": 1,
            "name": "boolean col",
            "required": False,
            "type": "boolean",
            "write-default": True,
        },
        {
            "id": 2,
            "name": "blob_col",
            "required": False,
            "type": "binary",
            "write-default": "00001234",
        },
        {"id": 3, "name": "tiny_int_col", "required": False, "type": "int"},
        {
            "id": 4,
            "name": "smallint_col",
            "required": False,
            "type": "int",
            "write-default": 2,
        },
        {
            "id": 5,
            "name": "int_col",
            "required": True,
            "type": "int",
            "write-default": 1,
        },
        {
            "id": 6,
            "name": "bigint_col",
            "required": False,
            "type": "long",
            "write-default": 100,
        },
        {"id": 7, "name": "utinyint_col", "required": False, "type": "int"},
        {"id": 8, "name": "usmallint_col", "required": False, "type": "int"},
        {"id": 9, "name": "uinteger_col", "required": False, "type": "long"},
        {"id": 10, "name": "uint64_col", "required": False, "type": "decimal(20,0)"},
        {
            "id": 11,
            "name": "float4_col",
            "required": False,
            "type": "float",
            "write-default": 3.14,
        },
        {
            "id": 12,
            "name": "float8_col",
            "required": False,
            "type": "double",
            "write-default": 2.718281828,
        },
        {
            "id": 13,
            "name": "varchar_col",
            "required": False,
            "type": "string",
            "write-default": "testas",
        },
        {"id": 14, "name": "decimal_col", "required": False, "type": "decimal(38,9)"},
        {
            "id": 15,
            "name": "uuid_col",
            "required": False,
            "type": "uuid",
            "write-default": "123e4567-e89b-12d3-a456-426614174000",
        },
        {"id": 16, "name": "hugeint_col", "required": False, "type": "decimal(38,0)"},
        {"id": 17, "name": "uhugeint_col", "required": False, "type": "decimal(38,0)"},
        {"id": 18, "name": "timestamp_ns_col", "required": False, "type": "timestamp"},
        {"id": 19, "name": "timestamp_ms_col", "required": False, "type": "timestamp"},
        {"id": 20, "name": "timestamp_s_col", "required": False, "type": "timestamp"},
        {
            "id": 21,
            "name": "timestamp_col",
            "required": False,
            "type": "timestamp",
            "write-default": "2024-01-01T00:00:00",
        },
        {
            "id": 22,
            "name": "timestamp_tz_col",
            "required": False,
            "type": "timestamptz",
        },
        {
            "id": 23,
            "name": "time_col",
            "required": False,
            "type": "time",
            "write-default": "12:34:56",
        },
        {
            "id": 24,
            "name": "date_col",
            "required": False,
            "type": "date",
            "write-default": "2024-01-01",
        },
        {
            "id": 25,
            "name": "escaping_varchar_col",
            "required": False,
            "type": "string",
            "write-default": 'test"  \\ "dummy_A!"@#$%^&* <>?`~',
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = parsed_data["schemas"][0]["fields"]

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"
        compare_fields(expected["type"], actual["type"])

        if expected.get("doc") is not None:
            assert (
                expected["doc"] == actual["doc"]
            ), f"Type mismatch: expected {expected['doc']} but got {actual['doc']}"
        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_base_types.\"iceberg base types\"'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = {
        "boolean col": 1,
        "blob_col": 2,
        "tiny_int_col": 3,
        "smallint_col": 4,
        "int_col": 5,
        "bigint_col": 6,
        "utinyint_col": 7,
        "usmallint_col": 8,
        "uinteger_col": 9,
        "uint64_col": 10,
        "float4_col": 11,
        "float8_col": 12,
        "varchar_col": 13,
        "decimal_col": 14,
        "uuid_col": 15,
        "hugeint_col": 16,
        "uhugeint_col": 17,
        "timestamp_ns_col": 18,
        "timestamp_ms_col": 19,
        "timestamp_s_col": 20,
        "timestamp_col": 21,
        "timestamp_tz_col": 22,
        "time_col": 23,
        "date_col": 24,
        "escaping_varchar_col": 25,
    }
    assert data == str(expected_field_ids)

    # load and read data back
    run_command(
        """
        INSERT INTO test_iceberg_base_types."iceberg base types" VALUES (
                DEFAULT,                -- boolean_col uses the default (true)
                DEFAULT,                -- blob_col uses the default ('00001234')
                10,                     -- tiny_int_col (SMALLINT)
                DEFAULT,                -- smallint_col uses the default (1 + 1)
                DEFAULT,                -- int_col uses the default (1)
                DEFAULT,                -- bigint_col
                20,                     -- utinyint_col (SMALLINT)
                30000,                  -- usmallint_col (INTEGER)
                100000000,              -- uinteger_col (BIGINT)
                NULL,                   -- uint64_col (NUMERIC(20, 0))
                DEFAULT,                -- float4_col uses the default (3.14)
                DEFAULT,                -- float8_col uses the default (2.718281828)
                DEFAULT,                -- varchar_col uses default ('testas')
                DEFAULT,                -- decimal_col uses default (random())
                DEFAULT,                -- uuid_col uses the default ('123e4567-e89b-12d3-a456-426614174000')
                12345678901234567890123456789012345678, -- hugeint_col (NUMERIC(38, 0))
                98765432109876543210987654321098765432, -- uhugeint_col (NUMERIC(38, 0))
                '2024-01-01 12:00:00',  -- timestamp_ns_col (TIMESTAMP)
                '2024-01-01 12:00:00',  -- timestamp_ms_col (TIMESTAMP)
                '2024-01-01 12:00:00',  -- timestamp_s_col (TIMESTAMP)
                DEFAULT,                -- timestamp_col uses the default ('2024-01-01')
                DEFAULT,                -- timestamp_tz_col uses the default (now())
                DEFAULT,                -- time_col (TIME) uses the default ('12:34:56')
                DEFAULT,                -- date_col (DATE) uses the default ('2024-01-01')
                DEFAULT                 -- escaping_varchar_col uses default ('test"  \\ "dummy_A!"@#$%^&* <>?`~')
            );
""",
        pg_conn,
    )

    results = run_query(
        'SELECT * FROM test_iceberg_base_types."iceberg base types"', pg_conn
    )
    assert len(results) == 1
    assert len(results[0]) == 25
    assert results[0][24] == 'test"  \\ "dummy_A!"@#$%^&* <>?`~'

    run_command("DROP SCHEMA test_iceberg_base_types CASCADE", pg_conn)
    pg_conn.commit()

    # dropping the table removes the table from pg_lake_iceberg
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_namespace = 'test_iceberg_base_types'",
        pg_conn,
    )
    assert len(results) == 0


def test_iceberg_types_array(pg_conn, create_helper_functions, s3, extension):
    run_command(
        """
                CREATE SCHEMA test_iceberg_array_types;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_iceberg_array_types"
    # Create a writable table with array columns
    run_command(
        f"""
            CREATE FOREIGN TABLE test_iceberg_array_types.iceberg_types_arr_3 (
                "boolean col" BOOLEAN[],
                blob_col BYTEA[],
                tiny_int_col SMALLINT[],
                smallint_col SMALLINT[],
                int_col INTEGER[],
                bigint_col BIGINT[],
                utinyint_col SMALLINT[],
                usmallint_col INTEGER[],
                uinteger_col BIGINT[],
                uint64_col NUMERIC(20, 0)[],
                float4_col REAL[],
                float8_col DOUBLE PRECISION[],
                varchar_col VARCHAR[],
                decimal_col NUMERIC[],
                uuid_col UUID[],
                hugeint_col NUMERIC(38, 0)[],
                uhugeint_col NUMERIC(38, 0)[],
                timestamp_ns_col TIMESTAMP[],
                timestamp_ms_col TIMESTAMP[],
                timestamp_s_col TIMESTAMP[],
                timestamp_col TIMESTAMP[],
                timestamp_tz_col TIMESTAMPTZ[],
                time_col TIME[],
                date_col DATE[]
            ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'iceberg_types_arr_3' and table_namespace = 'test_iceberg_array_types'",
        pg_conn,
    )
    assert len(results) == 1
    print(results)
    metadata_path = results[0][0]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {
            "id": 1,
            "name": "boolean col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 2,
                "element": "boolean",
                "element-required": False,
            },
        },
        {
            "id": 3,
            "name": "blob_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 4,
                "element": "binary",
                "element-required": False,
            },
        },
        {
            "id": 5,
            "name": "tiny_int_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 6,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 7,
            "name": "smallint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 8,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 9,
            "name": "int_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 10,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 11,
            "name": "bigint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 12,
                "element": "long",
                "element-required": False,
            },
        },
        {
            "id": 13,
            "name": "utinyint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 14,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 15,
            "name": "usmallint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 16,
                "element": "int",
                "element-required": False,
            },
        },
        {
            "id": 17,
            "name": "uinteger_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 18,
                "element": "long",
                "element-required": False,
            },
        },
        {
            "id": 19,
            "name": "uint64_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 20,
                "element": "decimal(20,0)",
                "element-required": False,
            },
        },
        {
            "id": 21,
            "name": "float4_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 22,
                "element": "float",
                "element-required": False,
            },
        },
        {
            "id": 23,
            "name": "float8_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 24,
                "element": "double",
                "element-required": False,
            },
        },
        {
            "id": 25,
            "name": "varchar_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 26,
                "element": "string",
                "element-required": False,
            },
        },
        {
            "id": 27,
            "name": "decimal_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 28,
                "element": "decimal(38,9)",
                "element-required": False,
            },
        },
        {
            "id": 29,
            "name": "uuid_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 30,
                "element": "uuid",
                "element-required": False,
            },
        },
        {
            "id": 31,
            "name": "hugeint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 32,
                "element": "decimal(38,0)",
                "element-required": False,
            },
        },
        {
            "id": 33,
            "name": "uhugeint_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 34,
                "element": "decimal(38,0)",
                "element-required": False,
            },
        },
        {
            "id": 35,
            "name": "timestamp_ns_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 36,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 37,
            "name": "timestamp_ms_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 38,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 39,
            "name": "timestamp_s_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 40,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 41,
            "name": "timestamp_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 42,
                "element": "timestamp",
                "element-required": False,
            },
        },
        {
            "id": 43,
            "name": "timestamp_tz_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 44,
                "element": "timestamptz",
                "element-required": False,
            },
        },
        {
            "id": 45,
            "name": "time_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 46,
                "element": "time",
                "element-required": False,
            },
        },
        {
            "id": 47,
            "name": "date_col",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 48,
                "element": "date",
                "element-required": False,
            },
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"
        compare_fields(expected["type"], actual["type"])

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_array_types.iceberg_types_arr_3'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = (
        "{'boolean col': {__duckdb_field_id: 1, element: 2}, "
        "'blob_col': {__duckdb_field_id: 3, element: 4}, "
        "'tiny_int_col': {__duckdb_field_id: 5, element: 6}, "
        "'smallint_col': {__duckdb_field_id: 7, element: 8}, "
        "'int_col': {__duckdb_field_id: 9, element: 10}, "
        "'bigint_col': {__duckdb_field_id: 11, element: 12}, "
        "'utinyint_col': {__duckdb_field_id: 13, element: 14}, "
        "'usmallint_col': {__duckdb_field_id: 15, element: 16}, "
        "'uinteger_col': {__duckdb_field_id: 17, element: 18}, "
        "'uint64_col': {__duckdb_field_id: 19, element: 20}, "
        "'float4_col': {__duckdb_field_id: 21, element: 22}, "
        "'float8_col': {__duckdb_field_id: 23, element: 24}, "
        "'varchar_col': {__duckdb_field_id: 25, element: 26}, "
        "'decimal_col': {__duckdb_field_id: 27, element: 28}, "
        "'uuid_col': {__duckdb_field_id: 29, element: 30}, "
        "'hugeint_col': {__duckdb_field_id: 31, element: 32}, "
        "'uhugeint_col': {__duckdb_field_id: 33, element: 34}, "
        "'timestamp_ns_col': {__duckdb_field_id: 35, element: 36}, "
        "'timestamp_ms_col': {__duckdb_field_id: 37, element: 38}, "
        "'timestamp_s_col': {__duckdb_field_id: 39, element: 40}, "
        "'timestamp_col': {__duckdb_field_id: 41, element: 42}, "
        "'timestamp_tz_col': {__duckdb_field_id: 43, element: 44}, "
        "'time_col': {__duckdb_field_id: 45, element: 46}, "
        "'date_col': {__duckdb_field_id: 47, element: 48}}"
    )

    assert data == expected_field_ids

    run_command(
        """
        INSERT INTO test_iceberg_array_types.iceberg_types_arr_3 VALUES (
            ARRAY[TRUE, FALSE, TRUE],                                      -- boolean_col
            ARRAY[decode('1234', 'hex'), decode('5678', 'hex')],            -- blob_col
            ARRAY[10, 20, 30],                                              -- tiny_int_col (SMALLINT)
            ARRAY[1, 2, 3],                                                 -- smallint_col (SMALLINT)
            ARRAY[100, 200, 300],                                           -- int_col (INTEGER)
            ARRAY[1000000000, 2000000000, 3000000000],                      -- bigint_col (BIGINT)
            ARRAY[10, 20],                                                  -- utinyint_col (SMALLINT)
            ARRAY[30000, 40000],                                            -- usmallint_col (INTEGER)
            ARRAY[100000000, 200000000],                                    -- uinteger_col (BIGINT)
            ARRAY[NULL, 12345678901234567890],                              -- uint64_col (NUMERIC(20, 0))
            ARRAY[3.14, 2.71],                                              -- float4_col (REAL)
            ARRAY[2.718281828, 3.1415926535],                               -- float8_col (DOUBLE PRECISION)
            ARRAY['hello', 'world'],                                        -- varchar_col (VARCHAR)
            ARRAY[random(), random()],                                      -- decimal_col (NUMERIC)
            ARRAY['123e4567-e89b-12d3-a456-426614174000'::uuid, '321e6547-e89b-12d3-a456-426614174000'::uuid], -- uuid_col (UUID)
            ARRAY[12345678901234567890123456789012345678, 98765432109876543210987654321098765432], -- hugeint_col (NUMERIC(38, 0))
            ARRAY[11111111111111111111111111111111111111, 22222222222222222222222222222222222222], -- uhugeint_col (NUMERIC(38, 0))
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_ns_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_ms_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_s_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00'::TIMESTAMP, '2025-01-01 12:00:00'::TIMESTAMP],            -- timestamp_col (TIMESTAMP)
            ARRAY['2024-01-01 12:00:00+00'::TIMESTAMPTZ, '2025-01-01 12:00:00+00'::TIMESTAMPTZ],      -- timestamp_tz_col (TIMESTAMPTZ)
            ARRAY['12:34:56'::TIME, '23:45:01'::TIME],                                  -- time_col (TIME)
            ARRAY['2024-01-01'::DATE, '2025-01-01'::DATE]                               -- date_col (DATE)
        );

        """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_array_types.iceberg_types_arr_3", pg_conn
    )
    assert len(results) == 1
    assert len(results[0]) == 24

    run_command("DROP SCHEMA test_iceberg_array_types CASCADE", pg_conn)
    pg_conn.commit()

    # dropping the table removes the table from pg_lake_iceberg
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_namespace = 'test_iceberg_array_types'",
        pg_conn,
    )
    assert len(results) == 0


def test_iceberg_types_composite(pg_conn, create_helper_functions, s3, extension):
    run_command(
        """
                CREATE SCHEMA test_iceberg_composite_types;
                """,
        pg_conn,
    )
    pg_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_iceberg_types_composite"

    # Create composite types and foreign table
    run_command(
        """
        CREATE TYPE test_iceberg_composite_types."t type" AS ("a with escape" int, "b with escape" int);
        CREATE TYPE test_iceberg_composite_types.t_type_2 AS ("a with escape" int, "b with escape" int, c test_iceberg_composite_types."t type");

        CREATE FOREIGN TABLE test_iceberg_composite_types.iceberg_types_composite (
            "a with escape" int,
            "b with escape" test_iceberg_composite_types."t type" DEFAULT ROW(1, NULL),
            c test_iceberg_composite_types.t_type_2,
            d test_iceberg_composite_types.t_type_2[] DEFAULT ARRAY[ROW(1, 2, ROW(3, 4))::test_iceberg_composite_types.t_type_2, NULL]
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """.format(
            location=location
        ),
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'iceberg_types_composite' and table_namespace = 'test_iceberg_composite_types'",
        pg_conn,
    )
    assert len(results) == 1
    print(results)
    metadata_path = results[0][0]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "a with escape", "required": False, "type": "int"},
        {
            "id": 2,
            "name": "b with escape",
            "required": False,
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "id": 3,
                        "name": "a with escape",
                        "required": False,
                        "type": "int",
                    },
                    {
                        "id": 4,
                        "name": "b with escape",
                        "required": False,
                        "type": "int",
                    },
                ],
            },
            "write-default": {"3": 1, "4": None},
        },
        {
            "id": 5,
            "name": "c",
            "required": False,
            "type": {
                "type": "struct",
                "fields": [
                    {
                        "id": 6,
                        "name": "a with escape",
                        "required": False,
                        "type": "int",
                    },
                    {
                        "id": 7,
                        "name": "b with escape",
                        "required": False,
                        "type": "int",
                    },
                    {
                        "id": 8,
                        "name": "c",
                        "required": False,
                        "type": {
                            "type": "struct",
                            "fields": [
                                {
                                    "id": 9,
                                    "name": "a with escape",
                                    "required": False,
                                    "type": "int",
                                },
                                {
                                    "id": 10,
                                    "name": "b with escape",
                                    "required": False,
                                    "type": "int",
                                },
                            ],
                        },
                    },
                ],
            },
        },
        {
            "id": 11,
            "name": "d",
            "required": False,
            "type": {
                "type": "list",
                "element-id": 12,
                "element": {
                    "type": "struct",
                    "fields": [
                        {
                            "id": 13,
                            "name": "a with escape",
                            "required": False,
                            "type": "int",
                        },
                        {
                            "id": 14,
                            "name": "b with escape",
                            "required": False,
                            "type": "int",
                        },
                        {
                            "id": 15,
                            "name": "c",
                            "required": False,
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "id": 16,
                                        "name": "a with escape",
                                        "required": False,
                                        "type": "int",
                                    },
                                    {
                                        "id": 17,
                                        "name": "b with escape",
                                        "required": False,
                                        "type": "int",
                                    },
                                ],
                            },
                        },
                    ],
                },
                "element-required": False,
            },
            "write-default": [{"13": 1, "14": 2, "15": {"16": 3, "17": 4}}, None],
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_composite_types.iceberg_types_composite'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = (
        "{'a with escape': 1, "
        "'b with escape': {__duckdb_field_id: 2, 'a with escape': 3, 'b with escape': 4}, "
        "'c': {__duckdb_field_id: 5, 'a with escape': 6, 'b with escape': 7, 'c': {__duckdb_field_id: 8, 'a with escape': 9, 'b with escape': 10}}, "
        "'d': {__duckdb_field_id: 11, element: {__duckdb_field_id: 12, 'a with escape': 13, 'b with escape': 14, 'c': {__duckdb_field_id: 15, 'a with escape': 16, 'b with escape': 17}}}}"
    )

    assert data == expected_field_ids

    # load some data and read it
    run_command(
        """
            INSERT INTO test_iceberg_composite_types.iceberg_types_composite
            VALUES (
                1,
                ROW(10, 20),
                ROW(30, 40, ROW(50, 60)),
                ARRAY[
                    '(70,80,"(90,100)")'::test_iceberg_composite_types.t_type_2,
                    '(110,120,"(130,140)")'::test_iceberg_composite_types.t_type_2
                ]
            ),
                (
                2,
                ROW(11, 21),
                ROW(31, 41, ROW(51, 61)),
                NULL
            ),
            (
                3,
                ROW(12, 22),
                NULL,
                NULL
            ),
            (
                4,
                NULL,
                NULL,
                NULL
            ),
            (
                5,
                DEFAULT,
                ROW(44, 55, ROW(55, 55)),
                DEFAULT
            );

        """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_composite_types.iceberg_types_composite ORDER BY 1",
        pg_conn,
    )
    print(results)
    assert results == [
        [
            1,
            "(10,20)",
            '(30,40,"(50,60)")',
            '{"(70,80,\\"(90,100)\\")","(110,120,\\"(130,140)\\")"}',
        ],
        [2, "(11,21)", '(31,41,"(51,61)")', None],
        [3, "(12,22)", None, None],
        [4, None, None, None],
        [5, "(1,)", '(44,55,"(55,55)")', '{"(1,2,\\"(3,4)\\")",NULL}'],
    ]

    run_command("DROP SCHEMA test_iceberg_composite_types CASCADE", pg_conn)
    pg_conn.commit()

    # dropping the table removes the table from pg_lake_iceberg
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_namespace = 'test_iceberg_composite_types'",
        pg_conn,
    )
    assert len(results) == 0


def test_iceberg_types_converted_to_string(
    pg_conn, create_helper_functions, s3, extension
):
    run_command("CREATE EXTENSION IF NOT EXISTS hstore;", pg_conn)

    run_command(
        """
                CREATE SCHEMA test_iceberg_types_converted_to_string;
                """,
        pg_conn,
    )

    location = "s3://" + TEST_BUCKET + "/test_iceberg_types_converted_to_string"

    run_command(
        """
        CREATE FOREIGN TABLE test_iceberg_types_converted_to_string.iceberg_types_converted_to_string (
                id INT,
                hstore_col hstore DEFAULT '"a"=>"1", "b"=>"2"',
                json_col json DEFAULT '{{"a": 1, "b": 2}}',
                jsonb_col json DEFAULT '{{"hello": [3,4]}}'
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """.format(
            location=location
        ),
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'iceberg_types_converted_to_string' and table_namespace = 'test_iceberg_types_converted_to_string'",
        pg_conn,
    )
    assert len(results) == 1
    print(results)
    metadata_path = results[0][0]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "id", "required": False, "type": "int"},
        {
            "id": 2,
            "name": "hstore_col",
            "required": False,
            "type": "string",
            "write-default": '"a"=>"1", "b"=>"2"',
        },
        {
            "id": 3,
            "name": "json_col",
            "required": False,
            "type": "string",
            "write-default": '{"a": 1, "b": 2}',
        },
        {
            "id": 4,
            "name": "jsonb_col",
            "required": False,
            "type": "string",
            "write-default": '{"hello": [3,4]}',
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_types_converted_to_string.iceberg_types_converted_to_string'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = "{'id': 1, 'hstore_col': 2, 'json_col': 3, 'jsonb_col': 4}"

    assert data == expected_field_ids

    # load some data and read it
    run_command(
        """
            INSERT INTO test_iceberg_types_converted_to_string.iceberg_types_converted_to_string
            VALUES (
                1,
                '"a"=>"1", "b"=>"2"',
                '{"a": 1, "b": 2}',
                '{"hello": [3,4]}'
            ),
            (
                2,
                DEFAULT,
                DEFAULT,
                DEFAULT
            );

        """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_types_converted_to_string.iceberg_types_converted_to_string ORDER BY 1",
        pg_conn,
    )
    assert results == [
        [1, '"a"=>"1", "b"=>"2"', {"a": 1, "b": 2}, {"hello": [3, 4]}],
        [2, '"a"=>"1", "b"=>"2"', {"a": 1, "b": 2}, {"hello": [3, 4]}],
    ]

    run_command("DROP SCHEMA test_iceberg_types_converted_to_string CASCADE", pg_conn)
    pg_conn.commit()


def test_iceberg_map_type(pg_conn, create_helper_functions, s3, extension):
    create_map_type("int", "text")
    run_command(
        """
                CREATE SCHEMA test_iceberg_map_type;
                CREATE TYPE test_iceberg_map_type."test type 10" AS (field1 text, "field 2" int, field3 numeric(20,0));
                """,
        pg_conn,
    )

    pg_conn.commit()
    complex_map_type_name = create_map_type(
        "text", 'test_iceberg_map_type."test type 10"[]'
    )

    location = "s3://" + TEST_BUCKET + "/test_iceberg_map_type"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_iceberg_map_type.simple_map_pg
        (
            id INT,
            simple_map map_type.key_int_val_text DEFAULT ARRAY[(2, 'me'), (3, 'myself'), (4, 'i')]::map_type.key_int_val_text,
            "complex map" {complex_map_type_name}

       ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """.format(
            location=location, complex_map_type_name=complex_map_type_name
        ),
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'simple_map_pg' and table_namespace = 'test_iceberg_map_type'",
        pg_conn,
    )
    assert len(results) == 1
    print(results)
    metadata_path = results[0][0]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    expected_fields = [
        {"id": 1, "name": "id", "required": False, "type": "int"},
        {
            "id": 2,
            "name": "simple_map",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 3,
                "key": "int",
                "value-id": 4,
                "value": "string",
                "value-required": False,
            },
            "write-default": {"keys": [2, 3, 4], "values": ["me", "myself", "i"]},
        },
        {
            "id": 5,
            "name": "complex map",
            "required": False,
            "type": {
                "type": "map",
                "key-id": 6,
                "key": "string",
                "value-id": 7,
                "value": {
                    "type": "list",
                    "element-id": 8,
                    "element": {
                        "type": "struct",
                        "fields": [
                            {
                                "id": 9,
                                "name": "field1",
                                "required": False,
                                "type": "string",
                            },
                            {
                                "id": 10,
                                "name": "field 2",
                                "required": False,
                                "type": "int",
                            },
                            {
                                "id": 11,
                                "name": "field3",
                                "required": False,
                                "type": "decimal(20,0)",
                            },
                        ],
                    },
                    "element-required": False,
                },
                "value-required": False,
            },
        },
    ]

    # Extract the actual fields from the parsed JSON data
    actual_fields = fields

    # Verify that the actual fields match the expected fields
    assert len(actual_fields) == len(expected_fields), "Field count mismatch"

    for expected, actual in zip(expected_fields, actual_fields):
        assert (
            expected["id"] == actual["id"]
        ), f"ID mismatch: expected {expected['id']} but got {actual['id']}"
        assert (
            expected["name"] == actual["name"]
        ), f"Name mismatch: expected {expected['name']} but got {actual['name']}"
        assert (
            expected["required"] == actual["required"]
        ), f"Required mismatch: expected {expected['required']} but got {actual['required']}"

        # Recursively compare the 'type' field
        compare_fields(expected["type"], actual["type"])

        if expected.get("write-default") is not None:
            assert (
                expected["write-default"] == actual["write-default"]
            ), f"write-default mismatch: expected {expected['write-default']} but got {actual['write-default']}"
        elif actual.get("write-default") is not None:
            assert False, "unexpected write-default"

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_iceberg_map_type.simple_map_pg'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = (
        "{'id': 1, "
        "'simple_map': {__duckdb_field_id: 2, key: 3, value: 4}, "
        "'complex map': {__duckdb_field_id: 5, key: 6, value: {__duckdb_field_id: 7, element: {__duckdb_field_id: 8, 'field1': 9, 'field 2': 10, 'field3': 11}}}}"
    )
    assert data == str(expected_field_ids)

    # lets ingest some data and read back
    run_command(
        f"""
        INSERT INTO test_iceberg_map_type.simple_map_pg
        VALUES (
            1,
            array[(1, 'me'), (2, 'myself'), (3, 'i')]::map_type.key_int_val_text,
            array[('1', ARRAY['(''me'', 1, 1.1)'])]::{complex_map_type_name}
        ),
        (
            2,
            DEFAULT,
            NULL
        ),
        (
            3,
            NULL,
            NULL
        ),
        (
            4,
            array[]::map_type.key_int_val_text,
            array[]::{complex_map_type_name}
        )
    """,
        pg_conn,
    )

    results = run_query(
        "SELECT * FROM test_iceberg_map_type.simple_map_pg ORDER BY 1", pg_conn
    )
    print(results)
    assert results == [
        [
            1,
            '{"(1,me)","(2,myself)","(3,i)"}',
            '{"(1,\\"{\\"\\"(\'me\',1,1)\\"\\"}\\")"}',
        ],
        [2, '{"(2,me)","(3,myself)","(4,i)"}', None],
        [3, None, None],
        [4, "{}", "{}"],
    ]

    run_command("DROP SCHEMA test_iceberg_map_type CASCADE", pg_conn)
    pg_conn.commit()

    # dropping the table removes the table from pg_lake_iceberg
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_namespace = 'test_iceberg_map_type'",
        pg_conn,
    )
    assert len(results) == 0


# fallback to string
def test_iceberg_unsupported_type(pg_conn, create_helper_functions, s3, extension):

    location = "s3://" + TEST_BUCKET + "/unsupported_type"
    # Create a writable table with array columns
    run_command(
        f"""
                CREATE SCHEMA unsupported_type;
            CREATE FOREIGN TABLE unsupported_type.unsupported_type(
            a int, regclass_col regclass
    ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

    """.format(
            location=location
        ),
        pg_conn,
    )
    pg_conn.commit()

    # get the metadata location from the catalog
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'unsupported_type' and table_namespace = 'unsupported_type'",
        pg_conn,
    )
    assert len(results) == 1
    print(results)
    metadata_path = results[0][0]

    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    regclass_field = fields[1]

    # make sure we mark it as string
    assert regclass_field["name"] == "regclass_col"
    assert regclass_field["type"] == "string"

    run_command(
        """
               DROP SCHEMA unsupported_type CASCADE;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # dropping the table removes the table from pg_lake_iceberg
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_namespace = 'unsupported_type'",
        pg_conn,
    )
    assert len(results) == 0


# comment can be added after create table
def test_comment_on_column(
    pg_conn, create_helper_functions, s3, extension, with_default_location
):

    # Create a writable table with array columns
    run_command(
        """
                CREATE SCHEMA comment_table;
                CREATE TABLE comment_table.comment_table(
                    int_col int
                ) USING pg_lake_iceberg;
                COMMENT ON COLUMN comment_table.comment_table.int_col IS 'test comment escape''d ';
                """,
        pg_conn,
    )

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.initial_metadata_for_table('comment_table.comment_table'::regclass)",
        pg_conn,
    )[0][0]

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    comment_field = fields[0]

    # make sure we mark it as string
    assert comment_field["name"] == "int_col"
    assert comment_field["type"] == "int"
    assert comment_field["doc"] == "test comment escape'd "

    run_command(
        """
               DROP SCHEMA comment_table CASCADE;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # dropping the table removes the table from pg_lake_iceberg
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_namespace = 'comment_table'",
        pg_conn,
    )
    assert len(results) == 0


def test_drop_column_with_subfields(
    pg_conn, create_helper_functions, s3, extension, with_default_location
):

    run_command(
        """
                CREATE SCHEMA test_drop_column_with_subfields;
                CREATE TYPE test_drop_column_with_subfields.t_type AS (a int, b int, c int);
                CREATE TABLE test_drop_column_with_subfields.t_table(drop_col_1 int, col_1 int, col_2 test_drop_column_with_subfields.t_type, drop_col_2 test_drop_column_with_subfields.t_type) USING iceberg;

                INSERT INTO test_drop_column_with_subfields.t_table VALUES (1, 2, '(3, 4, 5)', '(6, 7, 8)');

                ALTER TABLE test_drop_column_with_subfields.t_table DROP COLUMN drop_col_1, DROP COLUMN drop_col_2, ADD COLUMN col_3 INT;

    """,
        pg_conn,
    )

    res = run_query("SELECT * FROM test_drop_column_with_subfields.t_table", pg_conn)
    assert res == [[2, "(3,4,5)", None]]

    run_command("UPDATE test_drop_column_with_subfields.t_table SET col_3=7", pg_conn)
    res = run_query("SELECT * FROM test_drop_column_with_subfields.t_table", pg_conn)
    assert res == [[2, "(3,4,5)", 7]]

    data = run_query(
        f"SELECT test_iceberg_base_types_sc.iceberg_table_fieldids('test_drop_column_with_subfields.t_table'::regclass)",
        pg_conn,
    )[0][0]
    expected_field_ids = "{'col_1': 2, 'col_2': {__duckdb_field_id: 3, 'a': 4, 'b': 5, 'c': 6}, 'col_3': 11}"
    assert data == str(expected_field_ids)

    pg_conn.rollback()


# TODO: When we support query arguments for pg_lake_iceberg, enable this test
# also add some insert/delete statements Old repo: pull/643
def no_test_query_arguments(pg_conn, create_helper_functions, s3, extension):

    location = (
        "s3://"
        + TEST_BUCKET
        + "/test_query_arguments?s3_region="
        + TEST_AWS_REGION
        + "&s3_use_ssl=false"
    )
    # Create a writable table with array columns
    run_command(
        f"""
                CREATE SCHEMA "test escape @@! !~*'();/?:@&=+$,#";
            CREATE FOREIGN TABLE "test escape @@! !~*'();/?:@&=+$,#"."test escape @@! !~*'();/?:@&=+$,#"(
            a int
    ) SERVER pg_lake_iceberg OPTIONS (location '{location}');

    """.format(
            location=location
        ),
        pg_conn,
    )

    # get the metadata location from the catalog
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'test escape @@! !~*''();/?:@&=+$,#'",
        pg_conn,
    )
    assert len(results) == 1
    metadata_path = results[0][0]
    print(metadata_path)
    # make sure we write the version properly to the metadata.json
    assert metadata_path.split("/")[-1].startswith("00000-")

    data = read_s3_operations(s3, metadata_path)

    # Parse the JSON data
    parsed_data = json.loads(data)
    # Access specific fields
    fields = parsed_data["schemas"][0]["fields"]

    field = fields[0]

    assert field["name"] == "a"
    assert field["type"] == "int"

    run_command(
        f"""
               DROP SCHEMA "test escape @@! !~*'();/?:@&=+$,#" CASCADE;
    """.format(),
        pg_conn,
    )

    pg_conn.commit()

    # dropping the table removes the table from pg_lake_iceberg
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables WHERE table_namespace = 'test escape @@! !~*''();/?:@&=+$,#'",
        pg_conn,
    )
    assert len(results) == 0


def test_drop_table_removes_field_id_mappings(
    superuser_conn, s3, extension, create_helper_functions, with_default_location
):
    run_command(
        """
                CREATE SCHEMA test_drop_table_removes_field_id_mappings;
                """,
        superuser_conn,
    )
    superuser_conn.commit()

    location = "s3://" + TEST_BUCKET + "/test_drop_table_removes_field_id_mappings"
    # should be able to create a writable table with location
    run_command(
        f"""
            CREATE TABLE test_drop_table_removes_field_id_mappings."iceberg base types" (
                    "boolean col" BOOLEAN DEFAULT true,
                    int_col INT
                ) USING iceberg;

    """,
        superuser_conn,
    )
    superuser_conn.commit()

    before_drop_results = run_query(
        f"SELECT count(*), table_name::oid FROM lake_table.field_id_mappings group by table_name",
        superuser_conn,
    )

    # there are 2 columns
    assert before_drop_results[0][0] == 2

    run_command(
        f"""DROP SCHEMA test_drop_table_removes_field_id_mappings CASCADE;""",
        superuser_conn,
    )
    superuser_conn.commit()

    # show that there is one row per-column
    after_drop_results = run_query(
        f"SELECT * FROM lake_table.field_id_mappings WHERE table_name::oid = {before_drop_results[0][1]}",
        superuser_conn,
    )

    # drop removes the field attributes
    assert len(after_drop_results) == 0


def test_initial_and_write_defaults_composite_keys(
    pg_conn, superuser_conn, extension, with_default_location
):
    run_command(
        f"""
            CREATE SCHEMA test_initial_and_write_defaults;
            SET search_path TO test_initial_and_write_defaults;

            CREATE TYPE nested_type AS (
                subfield1 INT,
                subfield2 TEXT
            );

            CREATE TYPE main_type AS (
                id INT,
                details nested_type,
                description TEXT
            );

            CREATE TABLE example_table (
                create_table_col_with_default main_type DEFAULT (ROW(1, ROW(0, 'default_text'), 'default_description')::main_type),
                create_table_col_without_default main_type
            ) USING iceberg;

            ALTER TABLE example_table ADD COLUMN add_column_with_default main_type DEFAULT (ROW(2, ROW(1, 'another_default'), 'additional_description')::main_type);
            ALTER TABLE example_table ADD COLUMN add_column_without_default main_type;

            ALTER TABLE example_table ADD COLUMN modify_column_for_defaults main_type;

            ALTER TABLE example_table ALTER COLUMN modify_column_for_defaults SET DEFAULT (ROW(3, ROW(2, 'temporary_default'), 'temporary_description')::main_type);
        """,
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [
            1,
            1,
            None,
            '{"2": 1, "3": {"4": 0, "5": "default_text"}, "6": "default_description"}',
        ],
        [
            3,
            13,
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}',
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}',
        ],
        [
            5,
            25,
            None,
            '{"26": 3, "27": {"28": 2, "29": "temporary_default"}, "30": "temporary_description"}',
        ],
    ]

    # also, verify from the
    run_command(
        """
                CREATE TEMPORARY TABLE tmp_field_ids as
                with ice as (select lake_iceberg.metadata(metadata_location) metadata from iceberg_tables where table_name = 'example_table')   
                    select jsonb_pretty(metadata->'schemas'->(metadata->>'current-schema-id')::int) from ice;
    """,
        pg_conn,
    )

    res = run_query(
        """
            SELECT jsonb_pretty(field)::jsonb->>'write-default' FROM tmp_field_ids, jsonb_array_elements(jsonb_pretty::jsonb->'fields') AS field 
            WHERE field->>'write-default' is not null;
        """,
        pg_conn,
    )

    assert res == [
        ['{"2": 1, "3": {"4": 0, "5": "default_text"}, "6": "default_description"}'],
        [
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}'
        ],
        [
            '{"26": 3, "27": {"28": 2, "29": "temporary_default"}, "30": "temporary_description"}'
        ],
    ]

    res = run_query(
        """
            SELECT jsonb_pretty(field)::jsonb->>'initial-default' FROM tmp_field_ids, jsonb_array_elements(jsonb_pretty::jsonb->'fields') AS field 
            WHERE  field->>'initial-default' is not null;
        """,
        pg_conn,
    )
    assert res == [
        [
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}'
        ]
    ]

    run_command(
        "ALTER TABLE example_table ALTER COLUMN modify_column_for_defaults DROP DEFAULT;",
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [
            1,
            1,
            None,
            '{"2": 1, "3": {"4": 0, "5": "default_text"}, "6": "default_description"}',
        ],
        [
            3,
            13,
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}',
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}',
        ],
    ]
    # sanity check
    res = run_query("SELECT * FROM example_table", pg_conn)
    assert len(res) == 0

    run_command(
        """
        INSERT INTO example_table (create_table_col_without_default, add_column_without_default, modify_column_for_defaults)
        VALUES (
            ROW(4, ROW(3, 'no_default_provided'), 'custom_description'),
            ROW(5, ROW(4, 'another_no_default'), 'another_custom_description'),
            ROW(6, ROW(5, 'set_default_removed'), 'final_description')
        );
""",
        pg_conn,
    )

    res = run_query("SELECT * FROM example_table", pg_conn)
    assert res == [
        [
            '(1,"(0,default_text)",default_description)',
            '(4,"(3,no_default_provided)",custom_description)',
            '(2,"(1,another_default)",additional_description)',
            '(5,"(4,another_no_default)",another_custom_description)',
            '(6,"(5,set_default_removed)",final_description)',
        ]
    ]

    # finally, do SET DEFAULT, which should update the write-default
    run_command(
        "ALTER TABLE example_table ALTER COLUMN create_table_col_with_default SET DEFAULT (ROW(30, ROW(20, 'temporary_default'), 'temporary_description')::main_type);;",
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [
            1,
            1,
            None,
            '{"2": 30, "3": {"4": 20, "5": "temporary_default"}, "6": "temporary_description"}',
        ],
        [
            3,
            13,
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}',
            '{"14": 2, "15": {"16": 1, "17": "another_default"}, "18": "additional_description"}',
        ],
    ]

    # sanity check
    run_command(
        "SELECT * FROM example_table;",
        pg_conn,
    )

    assert_vacuum_re_register_field_ids(
        pg_conn, superuser_conn, "test_initial_and_write_defaults.example_table"
    )

    run_command(
        "DROP SCHEMA test_initial_and_write_defaults CASCADE",
        pg_conn,
    )
    pg_conn.commit()


def test_complex_ddl_history_re_register(
    pg_conn, superuser_conn, with_default_location
):

    run_command(
        """
                CREATE SCHEMA test_complex_ddl_history_re_register;
                CREATE TABLE test_complex_ddl_history_re_register.tb1(a int, b text DEFAULT 'test') USING iceberg;
                INSERT INTO test_complex_ddl_history_re_register.tb1 VALUES (1), (2);

                ALTER TABLE test_complex_ddl_history_re_register.tb1 DROP COLUMN a, ADD COLUMN a_new INT DEFAULT 100;

                INSERT INTO test_complex_ddl_history_re_register.tb1 VALUES ('non-default-text-2');

                ALTER TABLE test_complex_ddl_history_re_register.tb1 RENAME COLUMN a_new TO a_new_new;

        """,
        pg_conn,
    )
    pg_conn.commit()

    pre_result = run_query(
        "SELECT * FROM test_complex_ddl_history_re_register.tb1 ORDER BY 1,2", pg_conn
    )

    assert_vacuum_re_register_field_ids(
        pg_conn, superuser_conn, "test_complex_ddl_history_re_register.tb1"
    )
    assert_vacuum_re_register_field_ids(
        pg_conn, superuser_conn, "test_complex_ddl_history_re_register.tb1"
    )

    post_result = run_query(
        "SELECT * FROM test_complex_ddl_history_re_register.tb1 ORDER BY 1,2", pg_conn
    )

    assert pre_result == post_result


def test_initial_and_write_defaults_arrays(
    pg_conn, superuser_conn, extension, with_default_location
):
    run_command(
        f"""
            CREATE SCHEMA test_initial_and_write_defaults_arrays;
            SET search_path TO test_initial_and_write_defaults_arrays;

            CREATE TABLE example_table (
                create_table_col_with_default numeric[] DEFAULT ARRAY[1.0, 2.1, 3.2],
                create_table_col_without_default text[]
            ) USING iceberg;

            ALTER TABLE example_table ADD COLUMN add_column_with_default numeric[] DEFAULT ARRAY[10.0, 20.1, 30.2];
            ALTER TABLE example_table ADD COLUMN add_column_without_default numeric[];

            ALTER TABLE example_table ADD COLUMN modify_column_for_defaults numeric[];

            ALTER TABLE example_table ALTER COLUMN modify_column_for_defaults SET DEFAULT ARRAY[100.0, 200.1, 300.2];
        """,
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults_arrays.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [1, 1, None, "[1.0, 2.1, 3.2]"],
        [3, 5, "[10.0, 20.1, 30.2]", "[10.0, 20.1, 30.2]"],
        [5, 9, None, "[100.0, 200.1, 300.2]"],
    ]

    # also, verify from the
    run_command(
        """
                CREATE TEMPORARY TABLE tmp_field_ids_arrays as
                with ice as (select lake_iceberg.metadata(metadata_location) metadata from iceberg_tables where table_name = 'example_table')
                    select jsonb_pretty(metadata->'schemas'->(metadata->>'current-schema-id')::int) from ice;
    """,
        pg_conn,
    )

    res = run_query(
        """
            SELECT jsonb_pretty(field)::jsonb->>'write-default' FROM tmp_field_ids_arrays, jsonb_array_elements(jsonb_pretty::jsonb->'fields') AS field
            WHERE field->>'write-default' is not null;
        """,
        pg_conn,
    )

    assert res == [
        ["[1.0, 2.1, 3.2]"],
        ["[10.0, 20.1, 30.2]"],
        ["[100.0, 200.1, 300.2]"],
    ]

    res = run_query(
        """
            SELECT jsonb_pretty(field)::jsonb->>'initial-default' FROM tmp_field_ids_arrays, jsonb_array_elements(jsonb_pretty::jsonb->'fields') AS field
            WHERE  field->>'initial-default' is not null;
        """,
        pg_conn,
    )

    assert res == [["[10.0, 20.1, 30.2]"]]

    run_command(
        "ALTER TABLE example_table ALTER COLUMN modify_column_for_defaults DROP DEFAULT;",
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults_arrays.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [1, 1, None, "[1.0, 2.1, 3.2]"],
        [3, 5, "[10.0, 20.1, 30.2]", "[10.0, 20.1, 30.2]"],
    ]

    # sanity check
    res = run_query("SELECT * FROM example_table", pg_conn)
    assert len(res) == 0

    # finally, do SET DEFAULT, which should update the write-default
    run_command(
        "ALTER TABLE example_table ALTER COLUMN create_table_col_with_default SET DEFAULT ARRAY[500, 501];",
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults_arrays.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [1, 1, None, "[500, 501]"],
        [3, 5, "[10.0, 20.1, 30.2]", "[10.0, 20.1, 30.2]"],
    ]

    # sanity check
    run_command(
        "SELECT * FROM example_table;",
        pg_conn,
    )

    assert_vacuum_re_register_field_ids(
        pg_conn, superuser_conn, "test_initial_and_write_defaults_arrays.example_table"
    )

    run_command(
        "DROP SCHEMA test_initial_and_write_defaults_arrays CASCADE",
        pg_conn,
    )
    pg_conn.commit()


def test_initial_and_write_defaults_maps(
    pg_conn, superuser_conn, extension, with_default_location
):

    # first, create the map
    map_type_name = create_map_type("int", "text")

    run_command(
        f"""
            CREATE SCHEMA test_initial_and_write_defaults_maps;
            SET search_path TO test_initial_and_write_defaults_maps;

            CREATE TABLE example_table (
                create_table_col_with_default {map_type_name} DEFAULT ARRAY[(1,'onder'), (2, 'marco'), (3, 'aykut')]::{map_type_name},
                create_table_col_without_default {map_type_name}
            ) USING iceberg;

            ALTER TABLE example_table ADD COLUMN add_column_with_default {map_type_name} DEFAULT ARRAY[(4,'utku'), (5, 'david')]::{map_type_name};
            ALTER TABLE example_table ADD COLUMN add_column_without_default {map_type_name};

            ALTER TABLE example_table ADD COLUMN modify_column_for_defaults {map_type_name};

            ALTER TABLE example_table ALTER COLUMN modify_column_for_defaults SET DEFAULT ARRAY[(6,'craig')]::{map_type_name};
        """,
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults_maps.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [1, 1, None, '{"keys": [1, 2, 3], "values": ["onder", "marco", "aykut"]}'],
        [
            3,
            7,
            '{"keys": [4, 5], "values": ["utku", "david"]}',
            '{"keys": [4, 5], "values": ["utku", "david"]}',
        ],
        [5, 13, None, '{"keys": [6], "values": ["craig"]}'],
    ]

    # also, verify from the
    run_command(
        """
                CREATE TEMPORARY TABLE tmp_field_ids_maps as
                with ice as (select lake_iceberg.metadata(metadata_location) metadata from iceberg_tables where table_name = 'example_table')   
                    select jsonb_pretty(metadata->'schemas'->(metadata->>'current-schema-id')::int) from ice;
    """,
        pg_conn,
    )

    res = run_query(
        """
            SELECT jsonb_pretty(field)::jsonb->>'write-default' FROM tmp_field_ids_maps, jsonb_array_elements(jsonb_pretty::jsonb->'fields') AS field 
            WHERE field->>'write-default' is not null;
        """,
        pg_conn,
    )

    assert res == [
        ['{"keys": [1, 2, 3], "values": ["onder", "marco", "aykut"]}'],
        ['{"keys": [4, 5], "values": ["utku", "david"]}'],
        ['{"keys": [6], "values": ["craig"]}'],
    ]

    res = run_query(
        """
            SELECT jsonb_pretty(field)::jsonb->>'initial-default' FROM tmp_field_ids_maps, jsonb_array_elements(jsonb_pretty::jsonb->'fields') AS field 
            WHERE  field->>'initial-default' is not null;
        """,
        pg_conn,
    )

    assert res == [['{"keys": [4, 5], "values": ["utku", "david"]}']]

    run_command(
        "ALTER TABLE example_table ALTER COLUMN modify_column_for_defaults DROP DEFAULT;",
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults_maps.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [1, 1, None, '{"keys": [1, 2, 3], "values": ["onder", "marco", "aykut"]}'],
        [
            3,
            7,
            '{"keys": [4, 5], "values": ["utku", "david"]}',
            '{"keys": [4, 5], "values": ["utku", "david"]}',
        ],
    ]

    # sanity check
    res = run_query("SELECT * FROM example_table", pg_conn)
    assert len(res) == 0

    # finally, do SET DEFAULT, which should update the write-default
    run_command(
        f"ALTER TABLE example_table ALTER COLUMN create_table_col_with_default SET DEFAULT ARRAY[(7,'paul')]::{map_type_name};",
        pg_conn,
    )
    pg_conn.commit()

    field_id_mappings = run_query(
        """SELECT pg_attnum, field_id, initial_default, write_default FROM lake_table.field_id_mappings
           WHERE table_name = 'test_initial_and_write_defaults_maps.example_table'::regclass
            AND (initial_default IS NOT NULL or write_default IS NOT NULL)
           ORDER BY 1,2""",
        superuser_conn,
    )

    assert field_id_mappings == [
        [1, 1, None, '{"keys": [7], "values": ["paul"]}'],
        [
            3,
            7,
            '{"keys": [4, 5], "values": ["utku", "david"]}',
            '{"keys": [4, 5], "values": ["utku", "david"]}',
        ],
    ]
    # sanity check
    run_command(
        "SELECT * FROM example_table;",
        pg_conn,
    )

    assert_vacuum_re_register_field_ids(
        pg_conn, superuser_conn, "test_initial_and_write_defaults_maps.example_table"
    )

    run_command(
        "DROP SCHEMA test_initial_and_write_defaults_maps CASCADE",
        pg_conn,
    )
    pg_conn.commit()


def test_field_id_mappings(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    create_helper_functions,
    with_default_location,
):
    get_subfields_query = f"""SELECT
                    field_id, pg_attnum, field_pg_type, field_pg_typemod
            FROM
                lake_table.field_id_mappings
                    WHERE table_name = 'verify_field_id_mapping.test_table'::regclass AND
                          parent_field_id=%d
            ORDER BY field_id ASC;"""

    create_map_type("int", "text")

    # should be able to create a writable table with location
    run_command(
        f"""
            CREATE SCHEMA verify_field_id_mapping;
            CREATE TYPE verify_field_id_mapping.composite_type AS (key int, value text);
            CREATE TYPE verify_field_id_mapping.nested_composite_type AS (key int, value verify_field_id_mapping.composite_type, other_value map_type.key_int_val_text);

            CREATE TABLE verify_field_id_mapping.test_table (int_col int, array_int_col int[],
                                                             numeric_col numeric, numeric_typmod_col numeric(20, 10),
                                                             composite_col verify_field_id_mapping.composite_type,
                                                             nested_composite_col verify_field_id_mapping.nested_composite_type,
                                                             map_col map_type.key_int_val_text,
                                                             array_of_map_col map_type.key_int_val_text[]) USING iceberg;

    """,
        pg_conn,
    )
    pg_conn.commit()

    # first, make sure we get the top level columns properly
    top_level_mappings = run_query(
        f"""SELECT
                    table_name, field_id, attname, field_pg_type, field_pg_typemod
            FROM
                lake_table.field_id_mappings JOIN pg_catalog.pg_attribute
                    ON (table_name = attrelid AND attnum = pg_attnum)
                    WHERE table_name = 'verify_field_id_mapping.test_table'::regclass AND
                          parent_field_id IS NULL
            ORDER BY field_id ASC;""",
        superuser_conn,
    )

    assert top_level_mappings == [
        ["verify_field_id_mapping.test_table", 1, "int_col", "integer", -1],
        ["verify_field_id_mapping.test_table", 2, "array_int_col", "integer[]", -1],
        ["verify_field_id_mapping.test_table", 4, "numeric_col", "numeric", 2490381],
        [
            "verify_field_id_mapping.test_table",
            5,
            "numeric_typmod_col",
            "numeric",
            1310734,
        ],
        [
            "verify_field_id_mapping.test_table",
            6,
            "composite_col",
            "verify_field_id_mapping.composite_type",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            9,
            "nested_composite_col",
            "verify_field_id_mapping.nested_composite_type",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            17,
            "map_col",
            "map_type.key_int_val_text",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            20,
            "array_of_map_col",
            "map_type.key_int_val_text[]",
            -1,
        ],
    ]

    # Transform into a list of dictionaries
    field_name_to_id_map = {
        entry["attname"]: {"id": entry["field_id"]} for entry in top_level_mappings
    }

    field_id = field_name_to_id_map["int_col"]["id"]
    subfield_mapping_for_int = run_query(
        get_subfields_query % (field_id),
        superuser_conn,
    )
    assert len(subfield_mapping_for_int) == 0

    field_id = field_name_to_id_map["array_int_col"]["id"]
    subfield_mapping_for_int_array = run_query(
        get_subfields_query % (field_id),
        superuser_conn,
    )
    assert subfield_mapping_for_int_array == [[3, 2, "integer", -1]]

    field_id = field_name_to_id_map["composite_col"]["id"]
    subfield_mapping_for_composite_key = run_query(
        get_subfields_query % (field_id),
        superuser_conn,
    )
    assert subfield_mapping_for_composite_key == [
        [7, 5, "integer", -1],
        [8, 5, "text", -1],
    ]

    field_id = field_name_to_id_map["nested_composite_col"]["id"]
    subfield_mapping_for_nested_composite_key = run_query(
        get_subfields_query % (field_id),
        superuser_conn,
    )
    assert subfield_mapping_for_nested_composite_key == [
        [10, 6, "integer", -1],
        [11, 6, "verify_field_id_mapping.composite_type", -1],
        [14, 6, "map_type.key_int_val_text", -1],
    ]

    # now, for sub-fields of nested parts
    # 11 represents value verify_field_id_mapping.composite_type
    subfield_mapping_for_nested_composite_key_1 = run_query(
        get_subfields_query % (11),
        superuser_conn,
    )
    assert subfield_mapping_for_nested_composite_key_1 == [
        [12, 6, "integer", -1],
        [13, 6, "text", -1],
    ]

    # now, for sub-fields of nested parts
    # 14 represents value verify_field_id_mapping.other_value
    subfield_mapping_for_nested_composite_key_1 = run_query(
        get_subfields_query % (14),
        superuser_conn,
    )
    assert subfield_mapping_for_nested_composite_key_1 == [
        [15, 6, "integer", -1],
        [16, 6, "text", -1],
    ]

    field_id = field_name_to_id_map["map_col"]["id"]
    subfield_mapping_for_map_type = run_query(
        get_subfields_query % (field_id),
        superuser_conn,
    )
    assert subfield_mapping_for_map_type == [
        [18, 7, "integer", -1],
        [19, 7, "text", -1],
    ]

    field_id = field_name_to_id_map["array_of_map_col"]["id"]
    subfield_mapping_for_map_arr_type = run_query(
        get_subfields_query % (field_id),
        superuser_conn,
    )
    assert subfield_mapping_for_map_arr_type == [
        [21, 8, "map_type.key_int_val_text", -1]
    ]

    # now, for sub-fields of nested parts
    # 21 represents map_type.key_int_val_text
    subfield_mapping_for_nested_composite_key_1 = run_query(
        get_subfields_query % (21),
        superuser_conn,
    )
    assert subfield_mapping_for_nested_composite_key_1 == [
        [22, 8, "integer", -1],
        [23, 8, "text", -1],
    ]

    # let's drop/add some columns and check it again
    run_command(
        "ALTER TABLE verify_field_id_mapping.test_table DROP COLUMN nested_composite_col",
        pg_conn,
    )
    run_command(
        "ALTER TABLE verify_field_id_mapping.test_table ADD COLUMN nested_composite_col_new verify_field_id_mapping.nested_composite_type",
        pg_conn,
    )
    pg_conn.commit()

    # first, make sure we get the top level columns properly
    mappings_for_new_col = run_query(
        f"""WITH fields AS (SELECT
                    table_name, field_id, attname, field_pg_type, field_pg_typemod
            FROM
                lake_table.field_id_mappings JOIN pg_catalog.pg_attribute
                    ON (table_name = attrelid AND attnum = pg_attnum)
                    WHERE table_name = 'verify_field_id_mapping.test_table'::regclass
            ORDER BY pg_attnum DESC FETCH FIRST 1 ROWS WITH TIES)
            SELECT * FROM fields ORDER BY field_id ASC;""",
        superuser_conn,
    )

    assert mappings_for_new_col == [
        [
            "verify_field_id_mapping.test_table",
            24,
            "nested_composite_col_new",
            "verify_field_id_mapping.nested_composite_type",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            25,
            "nested_composite_col_new",
            "integer",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            26,
            "nested_composite_col_new",
            "verify_field_id_mapping.composite_type",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            27,
            "nested_composite_col_new",
            "integer",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            28,
            "nested_composite_col_new",
            "text",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            29,
            "nested_composite_col_new",
            "map_type.key_int_val_text",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            30,
            "nested_composite_col_new",
            "integer",
            -1,
        ],
        [
            "verify_field_id_mapping.test_table",
            31,
            "nested_composite_col_new",
            "text",
            -1,
        ],
    ]

    assert_vacuum_re_register_field_ids(
        pg_conn, superuser_conn, "verify_field_id_mapping.test_table"
    )

    run_command(
        f"""DROP SCHEMA verify_field_id_mapping CASCADE;""",
        superuser_conn,
    )

    pg_conn.commit()


def assert_vacuum_re_register_field_ids(pg_conn, superuser_conn, tbl_name):

    # now, get all the field_id mappings and then remove the entries
    # to show that VACUUM can re-insert properly
    pre_field_ids = run_query(
        f"SELECT f.* FROM lake_table.field_id_mappings f JOIN pg_catalog.pg_attribute p ON (p.attrelid = table_name AND p.attnum = pg_attnum) WHERE table_name = '{tbl_name}'::regclass and NOT p.attisdropped ORDER BY field_id",
        superuser_conn,
    )
    superuser_conn.commit()

    # now, remove the entries for this table
    run_command(
        f"DELETE FROM lake_table.field_id_mappings WHERE table_name = '{tbl_name}'::regclass",
        superuser_conn,
    )
    superuser_conn.commit()

    # make sure we can do that after VACUUM as well
    run_command_outside_tx([f"VACUUM {tbl_name}"], superuser_conn)

    post_field_ids = run_query(
        f"SELECT f.* FROM lake_table.field_id_mappings f JOIN pg_catalog.pg_attribute p ON (p.attrelid = table_name AND p.attnum = pg_attnum) WHERE table_name = '{tbl_name}'::regclass and NOT p.attisdropped ORDER BY field_id",
        superuser_conn,
    )
    assert pre_field_ids == post_field_ids

    # a second time VACUUM should not impact fieldId mappings
    run_command_outside_tx([f"VACUUM {tbl_name}"], superuser_conn)
    post_field_ids = run_query(
        f"SELECT f.* FROM lake_table.field_id_mappings f JOIN pg_catalog.pg_attribute p ON (p.attrelid = table_name AND p.attnum = pg_attnum) WHERE table_name = '{tbl_name}'::regclass and NOT p.attisdropped ORDER BY field_id",
        superuser_conn,
    )

    assert pre_field_ids == post_field_ids


# make sure we write parquet with the correct data type
# we use DATE/INT and TEXT types as representative type, this applies to all
# data types
def test_iceberg_parquet_file_types(
    extension, pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    location = "s3://" + TEST_BUCKET + "/test_iceberg_parquet_file_types"
    run_command(
        f"""
            CREATE SCHEMA test_iceberg_parquet_file_types;
            CREATE TABLE test_iceberg_parquet_file_types.date_type (
                    date_col DATE,
                    int_col INT,
                    text_col TEXT,
                    bigint_col BIGINT)
                    USING iceberg WITH (autovacuum_enabled=False);
    
        COPY (SELECT '2022-01-01' as c1, 1 as c2, 'test1' as c3, 1 as c4) TO '{location}/d1.csv';
        COPY (SELECT '2023-01-01' as c1, 2 as c2, 'test2' as c3, 2^32+1 as c4) TO '{location}/d2.csv' WITH (header);
        COPY (SELECT '2023-01-01' as c1, 2 as c2, 'test2' as c3, 2^32+1 as c4) TO '{location}/d1.parquet';
        COPY (SELECT '2023-01-01'::DATE as c1, 2::INT as c2, 'test2'::text as c3, 50 as c4) TO '{location}/d2.parquet';
        COPY (SELECT '2023-01-01' as c1, 2 as c2, 'test2' as c3, 123 as c4) TO '{location}/d1.json';

        -- copy pushdown
        COPY test_iceberg_parquet_file_types.date_type FROM '{location}/d1.csv';
        COPY test_iceberg_parquet_file_types.date_type FROM '{location}/d2.csv' WITH(header);
        COPY test_iceberg_parquet_file_types.date_type FROM '{location}/d2.csv' WITH (auto_detect);
        
        -- copy pushdown with parquet and json
        COPY test_iceberg_parquet_file_types.date_type FROM '{location}/d1.parquet';
        COPY test_iceberg_parquet_file_types.date_type FROM '{location}/d2.parquet';
        COPY test_iceberg_parquet_file_types.date_type FROM '{location}/d1.json';

        -- copy non-pushdown
        COPY test_iceberg_parquet_file_types.date_type FROM '{location}/d1.csv' WHERE date_col = '2022-01-01';
        
        -- regular INSERT
        INSERT INTO test_iceberg_parquet_file_types.date_type VALUES ('2024-01-01', 3, 'test3', 2^32+1 );
        
        -- INSERT .. SELECT pushdown
        INSERT INTO test_iceberg_parquet_file_types.date_type SELECT * FROM test_iceberg_parquet_file_types.date_type;

        -- INSERT .. SELECT with generate series
        INSERT INTO test_iceberg_parquet_file_types.date_type SELECT '2024-01-01', i, i::text, 2^32+1 FROM generate_series(0,10)i;

        -- INSERT .. SELECT with generate series bigint
        INSERT INTO test_iceberg_parquet_file_types.date_type SELECT '2024-01-01', 10, i::text, i FROM generate_series((2^32+1)::bigint,(2^32+10)::bigint)i;

        -- INSERT .. SELECT with generate series bigint
        INSERT INTO test_iceberg_parquet_file_types.date_type(date_col, int_col, text_col, bigint_col) SELECT date_col, bigint_col, text_col, int_col FROM test_iceberg_parquet_file_types.date_type WHERE bigint_col < 100;

        -- INSERT .. SELECT pull
        SET pg_lake_table.enable_insert_select_pushdown TO off;
        INSERT INTO test_iceberg_parquet_file_types.date_type SELECT * FROM test_iceberg_parquet_file_types.date_type;
        INSERT INTO test_iceberg_parquet_file_types.date_type SELECT '2024-01-01', i, i::text, 2^32+1 FROM generate_series(0,10)i;
        INSERT INTO test_iceberg_parquet_file_types.date_type SELECT '2024-01-01', 10, i::text, i FROM generate_series((2^32+1)::bigint,(2^32+10)::bigint)i;
        INSERT INTO test_iceberg_parquet_file_types.date_type(date_col, int_col, text_col, bigint_col) SELECT date_col, bigint_col, text_col, int_col FROM test_iceberg_parquet_file_types.date_type WHERE bigint_col < 100;

        RESET pg_lake_table.enable_insert_select_pushdown;
        """,
        pg_conn,
    )

    pg_conn.commit()

    files = run_query(
        "SELECT path FROM lake_table.files WHERE table_name = 'test_iceberg_parquet_file_types.date_type'::regclass",
        superuser_conn,
    )
    assert len(files) == 16
    for file in files:
        columns = run_query(
            f"DESCRIBE SELECT * FROM read_parquet('{file[0]}')", pgduck_conn
        )
        # column_type
        assert columns[0][1] == "DATE"
        assert columns[1][1] == "INTEGER"
        assert columns[2][1] == "VARCHAR"
        assert columns[3][1] == "BIGINT"

    # make sure VACUUM doesn't break this
    run_command_outside_tx(
        [f"VACUUM (FULL) test_iceberg_parquet_file_types.date_type"], superuser_conn
    )

    files = run_query(
        "SELECT path FROM lake_table.files WHERE table_name = 'test_iceberg_parquet_file_types.date_type'::regclass",
        superuser_conn,
    )
    for file in files:
        columns = run_query(
            f"DESCRIBE SELECT * FROM read_parquet('{file[0]}')", pgduck_conn
        )
        # column_type
        assert columns[0][1] == "DATE"

    run_command("DROP SCHEMA test_iceberg_parquet_file_types CASCADE", pg_conn)
    pg_conn.commit()


def compare_fields(expected, actual):
    """Recursively compare fields in nested dictionaries."""
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in expected:
            assert key in actual, f"Key {key} missing in actual"
            if isinstance(expected[key], dict) or isinstance(expected[key], list):
                compare_fields(expected[key], actual[key])
            else:
                assert (
                    expected[key] == actual[key]
                ), f"Value mismatch for key {key}: expected {expected[key]} but got {actual[key]}"
    elif isinstance(expected, list) and isinstance(actual, list):
        assert len(expected) == len(
            actual
        ), f"List length mismatch: expected {len(expected)} but got {len(actual)}"
        for exp_item, act_item in zip(expected, actual):
            compare_fields(exp_item, act_item)
    else:
        assert (
            expected == actual
        ), f"Value mismatch: expected {expected} but got {actual}"


def test_iceberg_large_value(pg_conn, s3, extension, with_default_location):
    location = f"s3://{TEST_BUCKET}/test_iceberg_large_value"

    # Create a table with a text column
    run_command(
        f"""
            CREATE SCHEMA test_iceberg_large_value;
            CREATE FOREIGN TABLE test_iceberg_large_value.large_value_test (
                id INTEGER,
                large_text TEXT
            ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
        """,
        pg_conn,
    )

    # Create a large string (over 32,000,000 characters)
    large_string_size = 33000000
    # Use repeat to generate a large string efficiently
    run_command(
        f"""
            INSERT INTO test_iceberg_large_value.large_value_test
            VALUES (1, repeat('a', {large_string_size}));
        """,
        pg_conn,
    )

    for execution_type in ["vectorize", "pushdown"]:
        if execution_type == "vectorize":
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO true;",
                pg_conn,
            )
        else:
            run_command(
                "SET pg_lake_table.enable_full_query_pushdown TO false;",
                pg_conn,
            )

        # Verify the data was inserted and can be read back
        results = run_query(
            "SELECT id, length(large_text) FROM test_iceberg_large_value.large_value_test",
            pg_conn,
        )
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == large_string_size

        # Verify we can retrieve the actual data (just check first and last chars)
        results = run_query(
            "SELECT substring(large_text, 1, 1), substring(large_text, length(large_text), 1) FROM test_iceberg_large_value.large_value_test",
            pg_conn,
        )
        assert results[0][0] == "a"
        assert results[0][1] == "a"

    pg_conn.rollback()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn, app_user):

    run_command(
        f"""
        CREATE SCHEMA test_iceberg_base_types_sc;
        CREATE OR REPLACE FUNCTION test_iceberg_base_types_sc.initial_metadata_for_table(tableOid Oid)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_table', $function$initial_metadata_for_table$function$;

        CREATE OR REPLACE FUNCTION test_iceberg_base_types_sc.iceberg_table_fieldids(tableOid Oid)
        RETURNS text
         LANGUAGE C
         IMMUTABLE STRICT
        AS 'pg_lake_table', $function$iceberg_table_fieldids$function$;

        GRANT USAGE ON SCHEMA test_iceberg_base_types_sc TO {app_user};
        GRANT SELECT ON lake_iceberg.tables TO {app_user};
        GRANT EXECUTE ON FUNCTION test_iceberg_base_types_sc.initial_metadata_for_table(oid) TO {app_user};
        GRANT EXECUTE ON FUNCTION test_iceberg_base_types_sc.iceberg_table_fieldids(oid) TO {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP SCHEMA test_iceberg_base_types_sc CASCADE;
        REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()
