import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *


def test_create_iceberg_table_no_stats(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_no_stats (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'none');
    """,
        pg_conn,
    )

    text_val = "a" * 17
    binary_val = "\\x" + "01" * 17

    # insert data
    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_no_stats VALUES (1, '{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    # there should be no stats
    result = run_query(
        "SELECT count(*) FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_no_stats'::regclass",
        pg_conn,
    )
    assert result[0][0] == 0

    pg_conn.rollback()


# column_stats_mode=full is equivalent to truncate(256)
def test_create_iceberg_table_full_stats(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_full_stats (a int, b text, c bytea, d text) USING iceberg WITH (column_stats_mode = 'full');
    """,
        pg_conn,
    )

    text_val = "a" * 17
    binary_val = "\\x" + "01" * 17
    longtext_val = "a" * 257

    # insert data
    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_full_stats VALUES (1, '{text_val}', '{binary_val}', '{longtext_val}');
        """,
        pg_conn,
    )

    # there should be stats in full text
    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_full_stats'::regclass order by field_id",
        pg_conn,
    )
    assert result == [
        [1, "1", "1"],
        [2, "aaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaa"],
        # [
        #     3,
        #     "\\x0101010101010101010101010101010101",
        #     "\\x0101010101010101010101010101010101",
        # ],
        [
            4,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab",
        ],
    ]

    pg_conn.rollback()


def test_create_iceberg_table_truncated_stats(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_truncated_stats (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'truncate(4)');
    """,
        pg_conn,
    )

    text_val = "a" * 17
    binary_val = "\\x" + "01" * 17

    # insert data
    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_truncated_stats VALUES (1, '{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    # verify there should be stats in truncated form and upper bound's last byte is incremented
    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_truncated_stats'::regclass order by field_id",
        pg_conn,
    )
    assert result == [
        [1, "1", "1"],
        [2, "aaaa", "aaab"],
        # [
        #     3,
        #     "\\x01010101",
        #     "\\x01010102",
        # ],
    ]

    assert (
        len(result[1][1]) == 4
        and len(result[1][2]) == 4
        # and len(bytes.fromhex(result[2][1].strip("\\x"))) == 4
        # and len(bytes.fromhex(result[2][2].strip("\\x"))) == 4
    )

    # insert data with lower bound less than truncate length
    run_command(
        f"INSERT INTO test_create_iceberg_table_truncated_stats VALUES (0, 'a', '\\x01'), (100, '{text_val}', '{binary_val}');",
        pg_conn,
    )

    # verify lower bound is the same as original value
    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_truncated_stats'::regclass order by 1,2,3",
        pg_conn,
    )
    assert result == [
        [1, "0", "100"],
        [1, "1", "1"],
        [2, "a", "aaab"],
        [2, "aaaa", "aaab"],
        # [3, "\\x01", "\\x01010102"],
        # [3, "\\x01010101", "\\x01010102"],
    ]

    # insert data with upper bound less than truncate length
    run_command(
        f"INSERT INTO test_create_iceberg_table_truncated_stats VALUES (1000, '{text_val}', '{binary_val}'), (10000, 'b', '\\x02');",
        pg_conn,
    )

    # verify upper bound's last byte is not incremented
    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_truncated_stats'::regclass order by 1,2,3",
        pg_conn,
    )
    assert result == [
        [1, "0", "100"],
        [1, "1", "1"],
        [1, "1000", "10000"],
        [2, "a", "aaab"],
        [2, "aaaa", "aaab"],
        [2, "aaaa", "b"],
        # [3, "\\x01", "\\x01010102"],
        # [3, "\\x01010101", "\\x01010102"],
        # [3, "\\x01010101", "\\x02"],
    ]

    pg_conn.rollback()


def test_create_iceberg_table_default_stats_mode(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b varchar, c bytea) USING iceberg;
    """,
        pg_conn,
    )

    text_val = "a" * 17
    binary_val = "\\x" + "01" * 17

    # insert data
    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_default_stats_mode VALUES (1, '{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    # default stats mode is truncate(16)
    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_default_stats_mode'::regclass order by field_id",
        pg_conn,
    )
    assert result == [
        [1, "1", "1"],
        [2, "aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaab"],
        # [
        #     3,
        #     "\\x01010101010101010101010101010101",
        #     "\\x01010101010101010101010101010102",
        # ],
    ]

    assert (
        len(result[1][1]) == 16
        and len(result[1][2]) == 16
        # and len(bytes.fromhex(result[2][1].strip("\\x"))) == 16
        # and len(bytes.fromhex(result[2][2].strip("\\x"))) == 16
    )

    pg_conn.rollback()


def test_create_iceberg_table_default_stats_escape(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_escape (a varchar) USING iceberg  WITH (column_stats_mode = 'full');
    """,
        pg_conn,
    )

    text_val = """This is a ''test'' string that испанч боб новости дейская dsmpeople.ru/real-estate=0&state=201307051938 includes "double quotes" and ''single quotes'' to check how escape characters are handled. It also includes a newline \n, a carriage return \r, and a tab \t ∑, ∞, ∇, ∂, ∫, ≈, ℵ 🂡 (playing card) \u200b (zero-width space), \u2066 (left-to-right isolate), \u2067 (right-to-left isolate) ``` character.Additionally, let''s include some Unicode characters: ✓, ©, ™, and emojis 😊, 🚀 to check UTF-8 compatibility."""

    # insert data
    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_default_stats_escape VALUES ('{text_val}');
        """,
        pg_conn,
    )

    # full stats mode is truncate(256)
    result = run_query(
        "SELECT field_id, lower_bound, upper_bound, length(lower_bound), length(upper_bound) FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_default_stats_escape'::regclass order by field_id",
        pg_conn,
    )

    normalized_text_val = text_val.replace("''", "'")
    # use the first 256 bytes of the string
    encoded_string = (normalized_text_val.encode("utf-8"))[:256]

    # make sure it's an ascii char or we need to fix the test data here
    assert encoded_string[255] < 127

    computed_min = encoded_string.decode("utf-8")
    # increment our byte rather than construct a new bytearray for a trivial change
    computed_max = (encoded_string[:255] + bytes([encoded_string[255] + 1])).decode(
        "utf-8"
    )

    assert computed_min == result[0][1]
    assert computed_max == result[0][2]

    pg_conn.rollback()


def test_create_iceberg_table_stats_numeric(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE numeric_test_ice (
            drop_col INT,
            num_38_9 NUMERIC(38,9),
            num_30_5 NUMERIC(30,5),
            num_25_10 NUMERIC(25,10),
            num_20_2 NUMERIC(20,2),
            num_19_5 NUMERIC(19,5),
            num_15_8 NUMERIC(15,8),
            num_12_4 NUMERIC(12,4),
            num_10_3 NUMERIC(10,3),
            num_8_2 NUMERIC(8,2),
            num_5_1 NUMERIC(5,1)
        ) USING iceberg;
    """,
        pg_conn,
    )

    # insert data
    run_command(
        f"""
            ALTER TABLE numeric_test_ice DROP COLUMN drop_col;

            INSERT INTO numeric_test_ice (
                num_38_9, num_30_5, num_25_10, num_20_2, num_19_5,
                num_15_8, num_12_4, num_10_3, num_8_2, num_5_1
            ) VALUES (
                -99999999999999999999999999999.999999999, -- NUMERIC(38,9)
                -9999999999999999999999999.99999,         -- NUMERIC(30,5)
                -999999999999999.9999999999,              -- NUMERIC(25,10)
                -999999999999999999.99,                   -- NUMERIC(20,2)
                -99999999999999.99999,                    -- NUMERIC(19,5)
                -9999999.99999999,                        -- NUMERIC(15,8)
                -99999999.9999,                           -- NUMERIC(12,4)
                -9999999.999,                             -- NUMERIC(10,3)
                -999999.99,                               -- NUMERIC(8,2)
                -9999.9                                   -- NUMERIC(5,1)
            ), (
                99999999999999999999999999999.999999999, -- NUMERIC(38,9)
                9999999999999999999999999.99999,         -- NUMERIC(30,5)
                999999999999999.9999999999,              -- NUMERIC(25,10)
                999999999999999999.99,                   -- NUMERIC(20,2)
                99999999999999.99999,                    -- NUMERIC(19,5)
                9999999.99999999,                        -- NUMERIC(15,8)
                99999999.9999,                           -- NUMERIC(12,4)
                9999999.999,                             -- NUMERIC(10,3)
                999999.99,                               -- NUMERIC(8,2)
                9999.9                                   -- NUMERIC(5,1)
            );
        """,
        pg_conn,
    )

    # default stats mode is truncate(16)
    result = run_query(
        "SELECT lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'numeric_test_ice'::regclass order by field_id",
        pg_conn,
    )

    assert result[0][0] == "-99999999999999999999999999999.999999999"
    assert result[0][1] == "99999999999999999999999999999.999999999"

    assert result[1][0] == "-9999999999999999999999999.99999"
    assert result[1][1] == "9999999999999999999999999.99999"

    assert result[2][0] == "-999999999999999.9999999999"
    assert result[2][1] == "999999999999999.9999999999"

    assert result[3][0] == "-999999999999999999.99"
    assert result[3][1] == "999999999999999999.99"

    assert result[4][0] == "-99999999999999.99999"
    assert result[4][1] == "99999999999999.99999"

    assert result[5][0] == "-9999999.99999999"
    assert result[5][1] == "9999999.99999999"

    assert result[6][0] == "-99999999.9999"
    assert result[6][1] == "99999999.9999"

    assert result[7][0] == "-9999999.999"
    assert result[7][1] == "9999999.999"

    assert result[8][0] == "-999999.99"
    assert result[8][1] == "999999.99"

    assert result[9][0] == "-9999.9"
    assert result[9][1] == "9999.9"

    pg_conn.rollback()


def test_create_iceberg_table_truncated_stats_with_overflow(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_truncated_stats_with_overflow (a varchar(32), b bytea) USING iceberg WITH (column_stats_mode = 'tRunCate(3)');
    """,
        pg_conn,
    )

    text_val = chr(127) * 17
    binary_val = "\\x" + ("FF" * 17)

    # insert data
    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_truncated_stats_with_overflow VALUES ('{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    # due to overflow, upper bound should be set to null
    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_truncated_stats_with_overflow'::regclass order by field_id",
        pg_conn,
    )
    assert result == [
        [1, "\x7f\x7f\x7f", None],
        # [2, "\\xffffff", None]
    ]

    assert (
        len(result[0][1])
        == 3
        # and len(bytes.fromhex(result[1][1].strip("\\x"))) == 3
    )

    run_command(
        "TRUNCATE test_create_iceberg_table_truncated_stats_with_overflow;", pg_conn
    )

    # last byte overflows but previous does not
    text_val = chr(126) * 2 + chr(127) * 15
    binary_val = "\\x" + ("FE" * 2) + "FF" * 15

    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_truncated_stats_with_overflow VALUES ('{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_truncated_stats_with_overflow'::regclass order by field_id",
        pg_conn,
    )
    assert result == [
        [1, "~~\x7f", "~\x7f\x7f"],
        # [2, "\\xfefeff", "\\xfeffff"],
    ]

    assert (
        len(result[0][1]) == 3
        and len(result[0][2]) == 3
        # and len(bytes.fromhex(result[1][1].strip("\\x"))) == 3
        # and len(bytes.fromhex(result[1][2].strip("\\x"))) == 3
    )

    pg_conn.rollback()


def test_create_iceberg_table_invalid_stats_mode(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'invalid');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "invalid column_stats_mode" in error

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'truncate(0)');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "truncate length must be greater than 0" in error

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'truncate(-1)');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "truncate length must be greater than 0" in error

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'truncate(12aasdsa)');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "column_stats_mode truncate length must be a valid integer" in error

    pg_conn.rollback()

    int64_max = 9223372036854775807

    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'truncate({int64_max + 1})');
    """,
        pg_conn,
        raise_error=False,
    )

    assert f"column_stats_mode truncate length must be at most {int64_max}" in error

    pg_conn.rollback()

    # try with max truncate limit
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b text, c bytea) USING iceberg WITH (column_stats_mode = 'truncate({int64_max})');
    """,
        pg_conn,
    )

    pg_conn.rollback()


def test_create_iceberg_table_alter_stats_mode(
    pg_conn, extension, app_user, s3, with_default_location, stats_catalog_permission
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_default_stats_mode (a int, b varchar, c bytea) USING iceberg WITH (column_stats_mode = 'full');
    """,
        pg_conn,
    )

    text_val = "a" * 17
    binary_val = "\\x" + "01" * 17

    # insert data
    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_default_stats_mode VALUES (1, '{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_default_stats_mode'::regclass order by field_id",
        pg_conn,
    )
    assert result == [
        [1, "1", "1"],
        [2, "aaaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaaa"],
        # [
        #     3,
        #     "\\x0101010101010101010101010101010101",
        #     "\\x0101010101010101010101010101010101",
        # ],
    ]

    assert (
        len(result[1][1]) == 17
        and len(result[1][2]) == 17
        # and len(bytes.fromhex(result[2][1].strip("\\x"))) == 17
        # and len(bytes.fromhex(result[2][2].strip("\\x"))) == 17
    )

    run_command("truncate test_create_iceberg_table_default_stats_mode;", pg_conn)

    # alter stats mode to truncate(4)
    run_command(
        "alter foreign table test_create_iceberg_table_default_stats_mode OPTIONS (SET column_stats_mode  'truncate(4)');",
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_default_stats_mode VALUES (1, '{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_default_stats_mode'::regclass order by 1,2,3",
        pg_conn,
    )
    assert result == [
        [1, "1", "1"],
        [2, "aaaa", "aaab"],
        # [
        #     3,
        #     "\\x01010101",
        #     "\\x01010102",
        # ],
    ]

    assert (
        len(result[1][1]) == 4
        and len(result[1][2]) == 4
        # and len(bytes.fromhex(result[2][1].strip("\\x"))) == 4
        # and len(bytes.fromhex(result[2][2].strip("\\x"))) == 4
    )

    run_command("truncate test_create_iceberg_table_default_stats_mode;", pg_conn)

    # reset to default stats mode
    run_command(
        "alter foreign table test_create_iceberg_table_default_stats_mode OPTIONS (DROP column_stats_mode);",
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_create_iceberg_table_default_stats_mode VALUES (1, '{text_val}', '{binary_val}');
        """,
        pg_conn,
    )

    result = run_query(
        "SELECT field_id, lower_bound, upper_bound FROM lake_table.data_file_column_stats where table_name = 'test_create_iceberg_table_default_stats_mode'::regclass order by 1,2,3",
        pg_conn,
    )
    assert result == [
        [1, "1", "1"],
        [2, "aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaab"],
        # [
        #     3,
        #     "\\x01010101010101010101010101010101",
        #     "\\x01010101010101010101010101010102",
        # ],
    ]

    assert (
        len(result[1][1]) == 16
        and len(result[1][2]) == 16
        # and len(bytes.fromhex(result[2][1].strip("\\x"))) == 16
        # and len(bytes.fromhex(result[2][2].strip("\\x"))) == 16
    )

    pg_conn.rollback()


@pytest.fixture(scope="module")
def stats_catalog_permission(app_user, superuser_conn, extension, s3):
    run_command(
        f"GRANT SELECT ON lake_table.data_file_column_stats TO {app_user};",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"REVOKE SELECT ON lake_table.data_file_column_stats FROM {app_user};",
        superuser_conn,
    )
    superuser_conn.commit()
