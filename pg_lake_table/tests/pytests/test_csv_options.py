import pytest
import psycopg2
from utils_pytest import *


def test_fdw_table_header_option_boolean(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_header_boolean_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with non-boolean header option
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_header_boolean_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', header 'yes', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.SyntaxError as e:
        assert "header requires a Boolean value" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_header_boolean_sc CASCADE", pg_conn)


def test_fdw_table_delimiter_single_byte(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_delimiter_single_byte_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with multiple character delimiter
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_delimiter_single_byte_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', delimiter ';;', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert "delimiter must be a single one-byte character" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_delimiter_single_byte_sc CASCADE", pg_conn)


def test_fdw_table_delimiter_no_newline_or_cr(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_delimiter_no_newline_or_cr_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with newline as delimiter
    cur = pg_conn.cursor()
    try:
        cur.execute(
            r"""  CREATE FOREIGN TABLE test_delimiter_no_newline_or_cr_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', delimiter E'\n', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert "delimiter cannot be newline or carriage return" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_delimiter_no_newline_or_cr_sc CASCADE", pg_conn)


def test_fdw_table_quote_single_byte(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_quote_single_byte_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with multiple character quote
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_quote_single_byte_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', quote '""', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert "quote must be a single one-byte character" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_quote_single_byte_sc CASCADE", pg_conn)


def test_fdw_table_escape_single_byte(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_escape_single_byte_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with multiple character escape
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_escape_single_byte_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', escape '\\\\', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert "escape must be a single one-byte character" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_escape_single_byte_sc CASCADE", pg_conn)


def test_fdw_table_null_no_newline_or_cr(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_null_no_newline_or_cr_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with newline in null option
    cur = pg_conn.cursor()
    try:
        cur.execute(
            r"""  CREATE FOREIGN TABLE test_null_no_newline_or_cr_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', null E'\n', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert "null cannot be newline or carriage return" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_null_no_newline_or_cr_sc CASCADE", pg_conn)


def test_fdw_table_csv_options_for_csv_format_only(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_csv_options_for_csv_only_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with CSV options but json format
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_csv_options_for_csv_only_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'json', header 'true', delimiter ',', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.SyntaxError as e:
        assert (
            '"header", "delimiter", "quote", "escape", "new_line", "null" and "null_padding" options are only supported for csv format tables'
            in str(e)
        )
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_csv_options_for_csv_only_sc CASCADE", pg_conn)


def test_fdw_table_csv_delimiter_not_in_null(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_csv_delim_not_in_null_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with delimiter appearing in null string
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_csv_delim_not_in_null_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', delimiter '|', null '|', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert (
            "CSV delimiter character must not appear in the NULL specification"
            in str(e)
        )
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_csv_delim_not_in_null_sc CASCADE", pg_conn)


def test_fdw_table_csv_quote_not_in_null(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_csv_quote_not_in_null_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with quote appearing in null string
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_csv_quote_not_in_null_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', quote '"', null '"', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert "CSV quote character must not appear in the NULL specification" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_csv_quote_not_in_null_sc CASCADE", pg_conn)


def test_fdw_table_csv_delimiter_quote_different(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_csv_delim_quote_different_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with same character for delimiter and quote
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_csv_delim_quote_different_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', delimiter ',', quote ',', path 's3://');"""
        )

        # can't get here, already failed
        assert False

    except psycopg2.errors.InvalidParameterValue as e:
        assert "CSV delimiter and quote must be different" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_csv_delim_quote_different_sc CASCADE", pg_conn)


def test_fdw_table_with_invalid_data_format(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_invalid_data_format_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with invalid format option
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_invalid_data_format_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'xml', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.FeatureNotSupported as e:
        assert (
            "only csv, json, gdal, and parquet formats are currently supported"
            in str(e)
        )
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_invalid_data_format_sc CASCADE", pg_conn)


def test_fdw_table_escape_valid_invalid_characters(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_escape_valid_invalid_chars_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with valid escape character
    cur = pg_conn.cursor()
    cur.execute(
        """  CREATE FOREIGN TABLE test_escape_valid_invalid_chars_sc.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'csv', escape '\\', path 's3://');"""
    )

    # table with invalid escape character (more than one byte)
    try:
        cur = pg_conn.cursor()
        cur.execute(
            """  CREATE FOREIGN TABLE test_escape_valid_invalid_chars_sc.ft2 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', escape 'ab', path 's3://');"""
        )

        # can't get here, already failed
        assert False
    except psycopg2.errors.InvalidParameterValue as e:
        assert "escape must be a single one-byte character" in str(e)
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_escape_valid_invalid_chars_sc CASCADE", pg_conn)


def test_fdw_table_csv_options_correct_values(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_csv_options_correct_values_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with correct CSV options
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_csv_options_correct_values_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', header 'true', delimiter ',', quote '"', escape '\\', null 'N/A', path 's3://');"""
        )
        assert True  # If we reach here, the command was successful
        cur.close()
    except psycopg2.Error as e:
        assert False, f"Unexpected error with correct CSV options: {e}"

    # cleanup
    run_command("DROP SCHEMA test_csv_options_correct_values_sc CASCADE", pg_conn)


def test_fdw_table_non_csv_with_csv_options(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_non_csv_with_csv_options_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with JSON format but including CSV-specific options
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_non_csv_with_csv_options_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'json', header 'true', delimiter ',', path 's3://');"""
        )
        # can't get here, already failed
        assert False
    except psycopg2.errors.SyntaxError as e:
        assert (
            '"header", "delimiter", "quote", "escape", "new_line", "null" and "null_padding" options are only supported for csv format tables'
            in str(e)
        )
        cur.close()
        pg_conn.commit()

    # cleanup
    run_command("DROP SCHEMA test_non_csv_with_csv_options_sc CASCADE", pg_conn)


def test_fdw_table_null_delimiter_with_valid_null_option(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_null_delimiter_valid_null_option_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table without delimiter but with a valid null option
    cur = pg_conn.cursor()

    cur.execute(
        """  CREATE FOREIGN TABLE test_null_delimiter_valid_null_option_sc.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'csv', null 'N/A', path 's3://');"""
    )
    # Expecting not to fail due to missing delimiter but having a null option
    assert True
    cur.close()

    # cleanup
    run_command("DROP SCHEMA test_null_delimiter_valid_null_option_sc CASCADE", pg_conn)


def test_fdw_table_null_string_no_quote_option(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_null_string_no_quote_option_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with a null option but no quote option
    cur = pg_conn.cursor()
    cur.execute(
        """  CREATE FOREIGN TABLE test_null_string_no_quote_option_sc.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'csv', null '""', path 's3://');"""
    )
    # Expecting not to fail due to missing quote but having a null option
    assert True
    cur.close()

    # cleanup
    run_command("DROP SCHEMA test_null_string_no_quote_option_sc CASCADE", pg_conn)


def test_fdw_table_no_escape_with_other_options(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_no_escape_with_other_options_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table without escape option but with other specified options
    cur = pg_conn.cursor()
    cur.execute(
        """  CREATE FOREIGN TABLE test_no_escape_with_other_options_sc.ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'csv', delimiter ',', quote '"', null 'N/A', path 's3://');"""
    )
    # Expecting not to fail due to missing or null escape option
    assert True
    cur.close()

    # cleanup
    run_command("DROP SCHEMA test_no_escape_with_other_options_sc CASCADE", pg_conn)


def test_fdw_table_option_provided_multiple_times(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_option_provided_multiple_times_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with an option provided multiple times
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_option_provided_multiple_times_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', null 'N/A', null 'NULL', path 's3://');"""
        )
        # Expecting to fail due to option provided multiple times
        assert (
            False
        ), "Expected failure for options provided multiple times but succeeded."
    except psycopg2.Error as e:
        assert 'option "null" provided more than once' in str(
            e
        ), "Expected error for multiple options not found."
        cur.close()
        pg_conn.rollback()

    # cleanup
    run_command("DROP SCHEMA test_option_provided_multiple_times_sc CASCADE", pg_conn)


def test_fdw_table_valid_options_with_unusual_characters(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_valid_options_with_unusual_chars_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with a null string option that contains unusual but valid characters
    cur = pg_conn.cursor()
    try:
        cur.execute(
            r"""  CREATE FOREIGN TABLE test_valid_options_with_unusual_chars_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', null ' \t\n', path 's3://');"""
        )
        # Expecting not to fail due to valid yet unusual characters in options
        assert True
    except Exception as e:
        assert False, f"Failed unexpectedly with error: {e}"

    # cleanup
    run_command("DROP SCHEMA test_valid_options_with_unusual_chars_sc CASCADE", pg_conn)


def test_fdw_table_single_char_options_boundary(pg_conn, extension):
    # setup
    run_command(
        """
                CREATE SCHEMA test_single_char_options_boundary_sc;
                """,
        pg_conn,
    )

    pg_conn.commit()

    # table with boundary condition character for delimiter
    cur = pg_conn.cursor()
    try:
        # Using ASCII control character as a delimiter (e.g., Horizontal Tab - \x09)
        cur.execute(
            r"""  CREATE FOREIGN TABLE test_single_char_options_boundary_sc.ft1 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', delimiter E'\x09', path 's3://');"""
        )
        # Expecting to pass if the implementation correctly handles control characters
        assert True
    except Exception as e:
        assert False, f"Failed unexpectedly with error: {e}"

    # cleanup
    run_command("DROP SCHEMA test_single_char_options_boundary_sc CASCADE", pg_conn)


def test_fdw_table_null_padding_option_boolean(pg_conn, extension):
    # table with valid boolean null_padding option
    cur = pg_conn.cursor()
    cur.execute(
        """  CREATE FOREIGN TABLE test_null_padding_ft1 (
                    id int,
                    value text
                ) SERVER pg_lake OPTIONS (format 'csv', null_padding 'true', path 's3://');"""
    )
    cur.close()

    # table with non-boolean null_padding option
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_null_padding_ft2 (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'csv', null_padding 'yes', path 's3://');"""
        )
        assert False
    except psycopg2.errors.SyntaxError as e:
        assert "null_padding requires a Boolean value" in str(e)
        cur.close()

    pg_conn.rollback()


def test_fdw_table_null_padding_csv_only(pg_conn, extension):
    # table with null_padding option but json format
    cur = pg_conn.cursor()
    try:
        cur.execute(
            """  CREATE FOREIGN TABLE test_null_padding_json (
                        id int,
                        value text
                    ) SERVER pg_lake OPTIONS (format 'json', null_padding 'true', path 's3://');"""
        )
        assert False
    except psycopg2.errors.SyntaxError as e:
        assert '"null_padding" options are only supported for csv format tables' in str(
            e
        )
        cur.close()

    pg_conn.rollback()
