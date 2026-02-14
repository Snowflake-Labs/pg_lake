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
    ("initcap_moo", "WHERE initcap(col_text) = 'Moo'", "initcap_pg", True),
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


def test_initcap_on_pg_vs_duck(pg_conn, pgduck_conn):
    """
    Comprehensive edge case tests for initcap pushdown.
    Verifies that Postgres initcap() and DuckDB initcap_pg() produce
    identical results across a wide range of inputs.
    """

    test_queries = [
        # --- Basic word capitalization ---
        ("SELECT initcap('hello world')", "Hello World"),
        ("SELECT initcap('HELLO WORLD')", "Hello World"),
        ("SELECT initcap('hElLo WoRlD')", "Hello World"),
        # --- Various separators / punctuation as word boundaries ---
        ("SELECT initcap('hello-world')", "Hello-World"),
        ("SELECT initcap('hello_world')", "Hello_World"),
        ("SELECT initcap('hello.world')", "Hello.World"),
        ("SELECT initcap('hello,world')", "Hello,World"),
        ("SELECT initcap('hello;world')", "Hello;World"),
        ("SELECT initcap('hello:world')", "Hello:World"),
        ("SELECT initcap('hello/world')", "Hello/World"),
        ("SELECT initcap('hello+world')", "Hello+World"),
        ("SELECT initcap('hello=world')", "Hello=World"),
        ("SELECT initcap('hello@world')", "Hello@World"),
        ("SELECT initcap('hello#world')", "Hello#World"),
        ("SELECT initcap('hello!world')", "Hello!World"),
        ("SELECT initcap('hello?world')", "Hello?World"),
        # --- Parentheses, brackets, braces ---
        ("SELECT initcap('(hello) world')", "(Hello) World"),
        ("SELECT initcap('[hello] world')", "[Hello] World"),
        ("SELECT initcap('{hello} world')", "{Hello} World"),
        # --- Apostrophe as word boundary ---
        # Apostrophe is non-alphanumeric, so it breaks words
        ("SELECT initcap('it''s a test')", "It'S A Test"),
        ("SELECT initcap('don''t stop')", "Don'T Stop"),
        ("SELECT initcap('o''brien')", "O'Brien"),
        # --- Whitespace variations ---
        ("SELECT initcap('hello  world')", "Hello  World"),
        ("SELECT initcap('  hello world  ')", "  Hello World  "),
        ("SELECT initcap('   ')", "   "),
        ("SELECT initcap(E'hello\\tworld')", "Hello\tWorld"),
        ("SELECT initcap(E'hello\\nworld')", "Hello\nWorld"),
        ("SELECT initcap(E'hello\\r\\nworld')", "Hello\r\nWorld"),
        # --- Digits and alphanumeric boundaries ---
        # Digits ARE alphanumeric: they continue a word, not start a new one
        ("SELECT initcap('123abc')", "123abc"),
        ("SELECT initcap('abc123def')", "Abc123def"),
        ("SELECT initcap('abc 123def')", "Abc 123def"),
        ("SELECT initcap('abc 123 def')", "Abc 123 Def"),
        ("SELECT initcap('12345')", "12345"),
        ("SELECT initcap('1a2b3c')", "1a2b3c"),
        ("SELECT initcap('a1b2c3')", "A1b2c3"),
        # Digits after separators: digit starts the word (but digit itself
        # is not a letter, so no case change; next letter is lowercased)
        ("SELECT initcap('abc-123-def')", "Abc-123-Def"),
        ("SELECT initcap('abc-1a-def')", "Abc-1a-Def"),
        ("SELECT initcap('100-200-300')", "100-200-300"),
        # --- Empty and NULL ---
        ("SELECT initcap('')", ""),
        ("SELECT initcap(NULL)", None),
        # --- Single characters ---
        ("SELECT initcap('a')", "A"),
        ("SELECT initcap('A')", "A"),
        ("SELECT initcap('z')", "Z"),
        ("SELECT initcap('1')", "1"),
        ("SELECT initcap(' ')", " "),
        ("SELECT initcap('-')", "-"),
        # --- All uppercase / all lowercase ---
        ("SELECT initcap('ALL CAPS HERE')", "All Caps Here"),
        ("SELECT initcap('all lowercase here')", "All Lowercase Here"),
        ("SELECT initcap('aaaa')", "Aaaa"),
        ("SELECT initcap('AAAA')", "Aaaa"),
        # --- Leading and trailing punctuation ---
        ("SELECT initcap('...hello...')", "...Hello..."),
        ("SELECT initcap('---hello---')", "---Hello---"),
        ("SELECT initcap('***hello***')", "***Hello***"),
        ("SELECT initcap('!!hello!!')", "!!Hello!!"),
        # --- Only special characters (no letters/digits) ---
        ("SELECT initcap('!@#$%')", "!@#$%"),
        ("SELECT initcap('---')", "---"),
        ("SELECT initcap('...')", "..."),
        # --- Multiple mixed separators ---
        ("SELECT initcap('a-b_c.d')", "A-B_C.D"),
        ("SELECT initcap('one.two.three')", "One.Two.Three"),
        ("SELECT initcap('test_value')", "Test_Value"),
        # --- Repeated separators ---
        ("SELECT initcap('hello--world')", "Hello--World"),
        ("SELECT initcap('hello__world')", "Hello__World"),
        ("SELECT initcap('hello..world')", "Hello..World"),
        # --- Quoted strings inside ---
        ("SELECT initcap('she said \"hello\"')", 'She Said "Hello"'),
        # --- Longer sentence ---
        (
            "SELECT initcap('the quick brown fox jumps over the lazy dog')",
            "The Quick Brown Fox Jumps Over The Lazy Dog",
        ),
        (
            "SELECT initcap('THE QUICK BROWN FOX JUMPS OVER THE LAZY DOG')",
            "The Quick Brown Fox Jumps Over The Lazy Dog",
        ),
    ]

    for test_query in test_queries:
        query = test_query[0]
        expected_result = test_query[1]
        pg_results = run_query(query, pg_conn)

        duck_query = query.replace("initcap(", "initcap_pg(")
        duck_results = run_query(duck_query, pgduck_conn)

        pg_val = pg_results[0][0]
        duck_val = duck_results[0][0]

        # Primary assertion: Postgres and DuckDB must produce identical results
        assert (
            pg_val == duck_val
        ), f"PG vs DuckDB mismatch for {query}: pg={pg_val!r}, duck={duck_val!r}"
        # Secondary assertion: both match the expected result
        assert (
            expected_result == pg_val
        ), f"Expected value mismatch for {query}: expected={expected_result!r}, pg={pg_val!r}"
