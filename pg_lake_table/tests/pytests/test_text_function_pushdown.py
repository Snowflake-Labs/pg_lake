import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *


# including an ID as part of each parameter set
# where id is oprcode for the given operator
# this id shows up in the pytest output
test_cases = [
    # Text operators
    ("substr_t_i", "WHERE substr(col_text, 2) = 'est'", "substring_pg", True),
    ("substr_t_i_i", "WHERE substr(col_text, 2, 2) = 'es'", "substring_pg", True),
    ("substring_t_i", "WHERE substring(col_text, 2) = 'est'", "substring_pg", True),
    ("substring_t_i_i", "WHERE substring(col_text, 2, 2) = 'es'", "substring_pg", True),
    ("substring_t_i", "WHERE substring(col_varchar, 2) = 'est'", "substring_pg", True),
    (
        "substring_t_i_i",
        "WHERE substring(col_varchar, 2, 2) = 'es'",
        "substring_pg",
        True,
    ),
    # different syntax
    (
        "s_substring_t_i",
        "WHERE substring(col_text from 2) = 'est'",
        "substring_pg",
        True,
    ),
    (
        "s_substring_t_i_i",
        "WHERE substring(col_text from 2 for 2) = 'es'",
        "substring_pg",
        True,
    ),
    (
        "s_substring_t_i",
        "WHERE substring(col_varchar from 2) = 'est'",
        "substring_pg",
        True,
    ),
    (
        "s_substring_t_i_i",
        "WHERE substring(col_varchar from 2 for 2) = 'es'",
        "substring_pg",
        True,
    ),
    # Postgres and DuckDB diverges in negative values in substring, but we re-write
    # the parameters of the functions on duckdb_pglake to match with Postgres
    ("substring_t_ni", "WHERE substring(col_text, -2) != 'est'", "substring_pg", True),
    (
        "substring_t_ni",
        "WHERE substring(col_varchar, -2) != 'est'",
        "substring_pg",
        True,
    ),
    # different syntax
    (
        "s_substring_t_ni",
        "WHERE substring(col_text from -2) != 'est'",
        "substring_pg",
        True,
    ),
    (
        "_substring_t_ni",
        "WHERE substring(col_varchar from -2) != 'est'",
        "substring_pg",
        True,
    ),
    # DuckDB doesn't support substring(text, text) and substring(text, text, text)
    # so we don't pushdown
    ("substring_t_t", "WHERE substring(col_text, 't') != 'est'", "substring", False),
    (
        "substring_t_t_t",
        "WHERE substring(col_text, '%t%', 'e') != 't'",
        "substring",
        False,
    ),
    ("upper_text", "WHERE upper(col_text) = 'TEST'", "upper", True),
    ("lower_text", "WHERE lower(col_text) = lower('TEST')", "lower", True),
    ("upper_varchar", "WHERE upper(col_varchar) = 'TEST'", "upper", True),
    ("lower_varchar", "WHERE lower(col_varchar) = lower('TEST')", "lower", True),
    # these are cast to text
    ("upper_char", "WHERE upper(col_char) = 'E'", "upper", True),
    ("lower_char", "WHERE lower(col_char) = lower('E')", "lower", True),
    # we can pushdown concat with certain data types
    (
        "concat",
        "WHERE concat(col_text, col_varchar, 1, 'test', 1.1, 1::bigint, 11.1::numeric, 2::float4, 8::float8, 'c'::char, 'test'::varchar, '6ecd8c99-4036-403d-bf84-cf8400f67836'::uuid) != 'onder'",
        "concat",
        True,
    ),
    (
        "||",
        "WHERE (col_text || true || col_varchar || 1 || 'test' || 1.1 || 1::bigint || 11.1::numeric || 2::float4 || 8::float8 || 'c'::char || 'test'::varchar) != 'marco'",
        "||",
        True,
    ),
    (
        "||",
        "WHERE (col_text || '6ecd8c99-4036-403d-bf84-cf8400f67836'::uuid || '2042-12-31'::date) <> 'marco'",
        "||",
        True,
    ),
    (
        "||",
        "WHERE (col_varchar || '2042-12-31 15:31:16+00'::timestamptz || '2000-01-01'::timestamp) != 'marco'",
        "||",
        True,
    ),
    (
        "||",
        "WHERE ('15:31:16+00'::timetz || col_text || '00:00'::time || interval '3 days') != 'marco'",
        "||",
        True,
    ),
    # for some, we cannot pushdown
    ("concat", "WHERE concat(col_text, ARRAY['1', '2']) != 'onder'", "concat", False),
    ("concat", "WHERE concat(col_text, true) != 'onder'", "concat", False),
    ("||-struct", "WHERE (col_text || (2,4)) != 'marco'", "||", False),
    (
        "||-cast",
        "WHERE (col_text || bpchar(col_varchar,1,true)) != 'marco'",
        "||",
        False,
    ),
    # Add some extra cast tests
    ("bool_cast", "WHERE (col_text is not null)::text = 'true'", "=", True),
    ("bpchar_cast", "WHERE substring(col_text, 1, 1)::bpchar::text = 't'", "=", True),
    ("char_cast", "WHERE substring(col_text, 1, 1)::char::text = 't'", "=", True),
    # Additional functions that should be pushed down
    ("ascii", "WHERE ascii(col_text) <> 10", "ascii", True),
    ("bit_length", "WHERE bit_length(col_text) > 2", "bit_length", True),
    ("btrim_1_arg", "WHERE btrim(col_text) = col_text", "trim", True),
    ("btrim_2_arg", "WHERE btrim(col_text, 'a') = col_text", "trim", True),
    ("btrim_trim", "WHERE trim(both 'a' from col_text) = col_text", "trim", True),
    ("chr", "WHERE chr(9) <> col_text", "chr", True),
    ("concat_ws", "WHERE length(concat_ws(' ', col_text)) > 2", "concat_ws", True),
    ("left", "WHERE left(col_text, 3) = 'moo'", "left", True),
    ("lpad_2_arg", "WHERE length(lpad(col_text, 20)) = 20", "lpad", True),
    ("lpad_3_arg", "WHERE length(lpad(col_text, 20, ' ')) = 20", "lpad", True),
    ("ltrim_1_arg", "WHERE ltrim(col_text) = col_text", "ltrim", True),
    ("ltrim_2_arg", "WHERE ltrim(col_text, 'abc') = col_text", "ltrim", True),
    (
        "ltrim_trim",
        "WHERE trim(leading 'abc' from col_text) = col_text",
        "TRIM(LEADING",
        True,
    ),
    ("md5", "WHERE md5(col_text) <> 'abc'", "md5", True),
    (
        "position",
        "WHERE POSITION('abc' in col_text) <> 1",
        "POSITION",
        True,
    ),  # gets uppercased by deparse
    ("repeat", "WHERE repeat(col_text, 1) = col_text", "repeat", True),
    ("replace", "WHERE replace(col_text, 'a', 'a') = col_text", "replace", True),
    ("reverse", "WHERE reverse(col_text) <> col_text", "reverse", True),
    ("right", "WHERE right(col_text, 1) = 'o'", "right", True),
    ("rpad_2_arg", "WHERE rpad(right(col_text,1),3) = 'o  '", "rpad", True),
    ("rpad_3_arg", "WHERE rpad(right(col_text,1),3, 'a') = 'oaa'", "rpad", True),
    ("rtrim_1_arg", "WHERE rtrim(col_text) = col_text", "rtrim", True),
    ("rtrim_2_arg", "WHERE rtrim(col_text, 'a') = col_text", "rtrim", True),
    (
        "rtrim_trim",
        "WHERE trim(trailing 'a' from col_text) = col_text",
        "TRIM(TRAILING",
        True,
    ),
    ("split_part", "WHERE split_part(col_text, 'o', 1) = 'm'", "split_part", True),
    ("starts_with", "WHERE starts_with(col_text, 'mo')", "starts_with", True),
    ("strpos", "WHERE strpos(col_text, 'a') IS NULL", "strpos", True),
    # initcap
    ("initcap_text", "WHERE initcap(col_text) = 'Test'", "initcap_pg", True),
    ("initcap_varchar", "WHERE initcap(col_varchar) = 'Test'", "initcap_pg", True),
    # translate
    (
        "translate_text",
        "WHERE translate(col_text, 'lo', 'LO') <> col_text",
        "translate",
        True,
    ),
    (
        "translate_varchar",
        "WHERE translate(col_varchar, 'lo', 'LO') <> col_varchar",
        "translate",
        True,
    ),
    # char_length / character_length. A varchar argument resolves to the same
    # text overload (Postgres plans it as char_length(col_varchar::text)), so
    # both are pushed down.
    ("char_length_text", "WHERE char_length(col_text) >= 0", "char_length", True),
    (
        "char_length_varchar",
        "WHERE char_length(col_varchar) >= 0",
        "char_length",
        True,
    ),
    (
        "character_length_text",
        "WHERE character_length(col_text) >= 0",
        "character_length",
        True,
    ),
    (
        "character_length_varchar",
        "WHERE character_length(col_varchar) >= 0",
        "character_length",
        True,
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression, assert_pushdown",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_text_function_operator_pushdown(
    create_test_text_function_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
    assert_pushdown,
):
    query = (
        "SELECT * FROM test_text_function_operator_pushdown.tbl " + operator_expression
    )

    if assert_pushdown:
        assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    else:
        assert_remote_query_not_contains_expression(query, expected_expression, pg_conn)

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_text_function_operator_pushdown.tbl"],
        ["test_text_function_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_text_function_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_test_text_function_operator_pushdown_table/data.parquet"
    run_command(
        f"""
            COPY (
                    SELECT NULL::text as col_text, NULL::name as col_name, NULL::varchar as col_varchar, NULL::char
                        UNION ALL
                    SELECT ''::text as col_text, ''::name as col_int4, ''::varchar as col_varchar, ''::char as col_char
                        UNION ALL
                    SELECT 'test'::text as col_text, 'test'::name as col_int4, 'test'::varchar as col_varchar, 's'::char
                        UNION ALL
                    SELECT 'moo'::text as col_text, 'moo'::name as col_int4, 'moo'::varchar as col_varchar, 'e'::char
                ) TO '{url}' WITH (FORMAT 'parquet');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
                CREATE SCHEMA test_text_function_operator_pushdown;
                CREATE FOREIGN TABLE test_text_function_operator_pushdown.tbl
                (
                    col_text text,
                    col_name name,
                    col_varchar varchar,
                    col_char char
                ) SERVER pg_lake OPTIONS (format 'parquet', path '{}');
                """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
                CREATE TABLE test_text_function_operator_pushdown.heap_tbl
                (
                    col_text text,
                    col_name name,
                    col_varchar varchar,
                    col_char char
                );
                COPY test_text_function_operator_pushdown.heap_tbl FROM '{}';
                """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield
    pg_conn.rollback()

    run_command("DROP SCHEMA test_text_function_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()


def test_substring_on_pg_vs_duck(pg_conn, pgduck_conn):

    test_queries = [
        (
            "SELECT substring('Hello World', 1, 5)",
            "Hello",
        ),  # Basic positive start and length
        ("SELECT substring('Hello World', 7, 5)", "World"),  # End of string
        (
            "SELECT substring('Hello World', 12, 5)",
            "",
        ),  # Start position beyond string length
        (
            "SELECT substring('Hello World', -5, 3)",
            "",
        ),  # Negative start, treated as 1 but starting at the wrong point results in empty
        (
            "SELECT substring('Hello World', -5, 7)",
            "H",
        ),  # Negative start, treated as 1 and starting from the first char
        (
            "SELECT substring('Hello World', 1, 50)",
            "Hello World",
        ),  # Length longer than the string itself
        (
            "SELECT substring('Hello World', 1, 11)",
            "Hello World",
        ),  # Start and length that cover exactly the string length
        (
            "SELECT substring('Hello World', 3, 0)",
            "",
        ),  # Zero length, expecting empty string
        (
            "SELECT substring('Hello World', 0, 5)",
            "Hell",
        ),  # Start at zero, treated as 1
        (
            "SELECT substring('Hello World', -3)",
            "Hello World",
        ),  # Negative start with no length, treated as start 1 but no specified end
        (
            "SELECT substring('Hello World', 7, 5)",
            "World",
        ),  # Length that would end exactly at the last character
        (
            "SELECT substring('Hello World', -100, 5)",
            "",
        ),  # Negative start much larger than the string length results in empty
        (
            "SELECT substring('Hello World', 100, 5)",
            "",
        ),  # Very large start position results in empty string
        (
            "SELECT substring('Hello World', 0, 0)",
            "",
        ),  # Combination of zero start and zero length
        (
            "SELECT substring('Hello World', 2, NULL)",
            None,
        ),  # Length as NULL results in NULL
        ("SELECT substring(NULL, 2, 3)", None),  # Text as NULL results in NULL
        (
            "SELECT substring('Hello World', 3)",
            "llo World",
        ),  # Start with positive no length, expects rest of string from position 3
        (
            "SELECT substring('Hello World', -3, 5)",
            "H",
        ),  # Negative start treated as 1, but incorrect start calculation
        (
            "SELECT substring('Hello World', 3, 15)",
            "llo World",
        ),  # Length exceeds the string length from position 3
    ]

    for test_query in test_queries:
        query = test_query[0]
        expected_result = test_query[1]
        pg_results = run_query(query, pg_conn)

        duck_query = query.replace("substring", "substring_pg")
        duck_results = run_query(duck_query, pgduck_conn)
        assert expected_result == pg_results[0][0] == duck_results[0][0]

    # this should error
    pg_query = "SELECT substring('Hello World', 1, -1)"
    res = run_query(pg_query, pg_conn, raise_error=False)
    print(res)
    assert "negative substring length not allowed" in res, False
    pg_conn.rollback()

    duck_query = "SELECT substring_pg('Hello World', 1, -1)"
    res = run_query(duck_query, pgduck_conn, raise_error=False)
    assert "negative substring length not allowed" in res, False
    pgduck_conn.rollback()


initcap_test_cases = [
    # Basic word capitalization
    "hello world",
    "HELLO WORLD",
    "hElLo WoRlD",
    # Various separators / punctuation as word boundaries
    "hello-world",
    "hello_world",
    "hello.world",
    "hello,world",
    "hello;world",
    "hello:world",
    "hello/world",
    "hello+world",
    "hello=world",
    "hello@world",
    "hello#world",
    "hello!world",
    "hello?world",
    # Parentheses, brackets, braces
    "(hello) world",
    "[hello] world",
    "{hello} world",
    # Apostrophe as word boundary
    "it's a test",
    "don't stop",
    "o'brien",
    # Whitespace variations
    "hello  world",
    "  hello world  ",
    "   ",
    "hello\tworld",
    "hello\nworld",
    "hello\r\nworld",
    # Digits and alphanumeric boundaries
    "123abc",
    "abc123def",
    "abc 123def",
    "abc 123 def",
    "12345",
    "1a2b3c",
    "a1b2c3",
    "abc-123-def",
    "abc-1a-def",
    "100-200-300",
    # Single characters
    "a",
    "A",
    "z",
    "1",
    " ",
    "-",
    # All uppercase / all lowercase
    "ALL CAPS HERE",
    "all lowercase here",
    "aaaa",
    "AAAA",
    # Leading and trailing punctuation
    "...hello...",
    "---hello---",
    "***hello***",
    "!!hello!!",
    # Only special characters
    "!@#$%",
    "---",
    # Multiple mixed separators
    "a-b_c.d",
    "one.two.three",
    "test_value",
    # Repeated separators
    "hello--world",
    "hello__world",
    "hello..world",
    # Quoted strings / longer sentences
    'she said "hello"',
    "the quick brown fox jumps over the lazy dog",
    "THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG",
]


@pytest.fixture(scope="module")
def create_initcap_edge_case_tables(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/initcap_pushdown/data.parquet"

    run_command(
        """
        CREATE SCHEMA initcap_pushdown;
        CREATE TABLE initcap_pushdown.src (val text);
        """,
        pg_conn,
    )

    cur = pg_conn.cursor()
    for input_val in initcap_test_cases:
        cur.execute("INSERT INTO initcap_pushdown.src VALUES (%s)", (input_val,))
    cur.close()
    pg_conn.commit()

    run_command(
        f"COPY initcap_pushdown.src TO '{url}' WITH (FORMAT 'parquet')",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE FOREIGN TABLE initcap_pushdown.tbl (val text)
            SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        CREATE TABLE initcap_pushdown.heap_tbl (val text);
        COPY initcap_pushdown.heap_tbl FROM '{url}';
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("DROP TABLE initcap_pushdown.src", pg_conn)
    pg_conn.commit()

    yield
    pg_conn.rollback()
    run_command("DROP SCHEMA initcap_pushdown CASCADE", pg_conn)
    pg_conn.commit()


def test_initcap_on_pg_vs_duck(create_initcap_edge_case_tables, pg_conn):
    """
    Comprehensive edge case tests for initcap pushdown.
    Runs initcap() on a foreign table (DuckDB) and a heap table (Postgres)
    and verifies they produce identical results for every edge case row.
    """
    query = "SELECT val, initcap(val) FROM initcap_pushdown.tbl ORDER BY val"

    assert_query_results_on_tables(
        query,
        pg_conn,
        ["initcap_pushdown.tbl"],
        ["initcap_pushdown.heap_tbl"],
    )


# Values chosen for the equivalence check below: NULL, the empty string, and
# strings where a naive implementation diverges -- trailing blanks (which
# count for text but not for bpchar), multi-byte UTF-8, an emoji plus skin
# tone modifier and a ZWJ sequence (multiple codepoints that render as one
# grapheme), and a combining accent. Plain "abc" is here for the translate
# cases below, where it keeps the expected output readable.
TEXT_EDGE_VALUES = [
    None,
    "",
    "     ",
    "abc",
    "abc  ",
    "  abc",
    "h\u00e9llo",
    "he\u0301llo",
    "\u65e5\u672c\u8a9e",
    "\U0001f44d",
    "\U0001f44d\U0001f3fd",
    "\U0001f468\u200d\U0001f469\u200d\U0001f467",
]


@pytest.fixture(scope="module")
def create_text_edge_values_table(pg_conn, s3, extension):
    """A table whose text, varchar and bpchar columns all hold TEXT_EDGE_VALUES.

    The bpchar column is char(10) so Postgres blank-pads every value, which is
    what makes the char_length(bpchar) negative test below meaningful.
    """
    url = f"s3://{TEST_BUCKET}/text_edge_values_test/data.parquet"
    rows = " UNION ALL ".join(
        "SELECT {v}::text AS col_text, {v}::varchar AS col_varchar, "
        "{v}::char(10) AS col_bpchar".format(
            v="NULL" if value is None else "'" + value + "'"
        )
        for value in TEXT_EDGE_VALUES
    )
    run_command(
        f"COPY ({rows}) TO '{url}' WITH (FORMAT 'parquet');",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE SCHEMA text_edge_vals;
        CREATE FOREIGN TABLE text_edge_vals.fdw_tbl
            (col_text text, col_varchar varchar, col_bpchar char(10))
        SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        CREATE TABLE text_edge_vals.heap_tbl
            (col_text text, col_varchar varchar, col_bpchar char(10));
        COPY text_edge_vals.heap_tbl FROM '{url}';
        """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA text_edge_vals CASCADE", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize("func", ["char_length", "character_length"])
@pytest.mark.parametrize("col", ["col_text", "col_varchar"])
def test_length_specific_values(create_text_edge_values_table, pg_conn, func, col):
    """char_length/character_length pushdown agrees with Postgres on NULL, the
    empty string, trailing blanks and multi-codepoint UTF-8 sequences."""
    query = f"SELECT {func}({col}) FROM text_edge_vals.fdw_tbl"
    assert_remote_query_contains_expression(query, func, pg_conn)
    assert_table_contents_match(
        pg_conn,
        f"(SELECT {func}({col}) FROM text_edge_vals.fdw_tbl) fdw",
        f"(SELECT {func}({col}) FROM text_edge_vals.heap_tbl) heap",
    )


@pytest.mark.parametrize("func", ["char_length", "character_length"])
def test_length_bpchar_is_not_pushed_down(create_text_edge_values_table, pg_conn, func):
    """char_length(bpchar) is a different function (bpcharlen) and has to stay
    local. Postgres ignores the blank padding, DuckDB counts it:

        SELECT char_length('abc'::char(10));  -- pg: 3, DuckDB: 10

    Only the text overload is on the shippable list, so this is already what
    happens. Pin it here, so that adding a bpchar entry later cannot silently
    start returning the padded length.
    """
    query = f"SELECT {func}(col_bpchar) FROM text_edge_vals.fdw_tbl"
    assert_remote_query_not_contains_expression(query, func, pg_conn)
    assert_table_contents_match(
        pg_conn,
        f"(SELECT {func}(col_bpchar) FROM text_edge_vals.fdw_tbl) fdw",
        f"(SELECT {func}(col_bpchar) FROM text_edge_vals.heap_tbl) heap",
    )


# from/to pairs where a naive translate() diverges: deletion when from is
# longer than to, extra to characters ignored, duplicate from characters
# (first wins), no cascading replacement, and multi-byte characters on
# either side.
#
# no_cascade_overlap is the important one: from and to overlap, so a
# character produced by the mapping is itself in from.
#
#     SELECT translate('abc', 'ab', 'bc');  -- bcc, not ccc
#
# Both engines scan the input once, so the "b" that came from "a" is not
# translated again. A second pass over the output would give "ccc".
translate_cases = [
    ("delete_all", "abc", ""),
    ("delete_extra", "abcd", "AB"),
    ("to_longer_than_from", "ab", "ABCDEF"),
    ("duplicate_in_from", "aa", "XY"),
    ("no_cascade", "abc", "cba"),
    ("no_cascade_overlap", "ab", "bc"),
    ("empty_from", "", "XY"),
    ("multibyte_from", "\u00e9\u65e5", "eX"),
    ("multibyte_to", "ab", "\u00e9\u65e5"),
    ("blanks", " ", ""),
]


@pytest.mark.parametrize("col", ["col_text", "col_varchar"])
@pytest.mark.parametrize(
    "test_id, from_chars, to_chars",
    translate_cases,
    ids=[case[0] for case in translate_cases],
)
def test_translate_specific_values(
    create_text_edge_values_table, pg_conn, col, test_id, from_chars, to_chars
):
    """translate() pushdown agrees with Postgres for the character-set edge
    cases, over NULL and multi-byte input."""
    expr = f"translate({col}, '{from_chars}', '{to_chars}')"
    query = f"SELECT {expr} FROM text_edge_vals.fdw_tbl"
    assert_remote_query_contains_expression(query, "translate", pg_conn)
    assert_table_contents_match(
        pg_conn,
        f"(SELECT {expr} FROM text_edge_vals.fdw_tbl) fdw",
        f"(SELECT {expr} FROM text_edge_vals.heap_tbl) heap",
    )
