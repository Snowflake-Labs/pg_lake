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
    ("text", "WHERE col_text = text(true)", 'WHERE ("col_text" = (true)::"text")'),
    (
        "text",
        "WHERE starts_with(col_text, text('t'::\"char\"))",
        'WHERE "starts_with"("col_text", (\'t\'::"char")::"text")',
    ),
    (
        "text",
        "WHERE col_text = text(col_varchar)",
        'WHERE ("col_text" = ("col_varchar")::"text")',
    ),
    # Text operators
    ("text_lt", "WHERE col_text < 'abc'", 'WHERE ("col_text" < \'abc\'::"text")'),
    ("text_le", "WHERE col_text <= 'def'", 'WHERE ("col_text" <= \'def\'::"text")'),
    ("text_gt", "WHERE col_text > 'ghi'", 'WHERE ("col_text" > \'ghi\'::"text")'),
    ("text_ge", "WHERE col_text >= 'jkl'", 'WHERE ("col_text" >= \'jkl\'::"text")'),
    ("texteq", "WHERE col_text = 'moo'", 'WHERE ("col_text" = \'moo\'::"text")'),
    ("textne", "WHERE col_text <> 'pqr'", 'WHERE ("col_text" <> \'pqr\'::"text")'),
    ("textlike", "WHERE col_text LIKE col_text", "~~"),
    ("textlike", "WHERE col_text LIKE '%m%'", "~~"),
    ("textnlike", "WHERE col_text NOT LIKE '%q%'", "!~~"),
    ("texticlike", "WHERE col_text ILIKE '%M%'", "~~*"),
    ("texticnlike", "WHERE col_text NOT ILIKE '%Q%'", "!~~*"),
    (
        "textrematch",
        "WHERE col_text ~ '.'",
        'WHERE "lake_regexp_matches"("col_text", \'.\'::"text", false)',
    ),
    (
        "textrenmatch",
        "WHERE col_text !~ '.'",
        'WHERE (NOT "lake_regexp_matches"("col_text", \'.\'::"text", false))',
    ),
    (
        "textreicmatch",
        "WHERE col_text ~* '.'",
        'WHERE "lake_regexp_matches"("col_text", \'.\'::"text", true)',
    ),
    (
        "textreicnmatch",
        "WHERE col_text !~* '.'",
        'WHERE (NOT "lake_regexp_matches"("col_text", \'.\'::"text", true))',
    ),
    ("textcat", "WHERE col_text || 'se' = 'moose'", "||"),
    ("starts_with", "WHERE col_text ^@ 'mo'", "^@"),
    # Text functions
    ("length", "WHERE length(col_text) = 4", 'WHERE ("length"("col_text") = 4)'),
    (
        "regexp_replace",
        "WHERE regexp_replace(col_text, 'o', '$') = 'm$o'",
        "regexp_replace",
    ),
    (
        "regexp_replace",
        "WHERE regexp_replace(col_text, '.o[a-z]', '$') = '$'",
        "regexp_replace",
    ),
    (
        "regexp_replace",
        "WHERE regexp_replace(col_text, '^mo', 'bo') = 'boo'",
        "regexp_replace",
    ),
    (
        "regexp_replace",
        "WHERE regexp_replace(col_text, 'o', '$', 'g') = 'm$$'",
        "regexp_replace",
    ),
    # Unsupported cases should still work, but no pushdown phrase
    ("regexp_replace", "WHERE regexp_replace(col_text, 'o', '$', 3) = 'mo$'", ""),
    (
        "regexp_replace",
        "WHERE regexp_replace(col_text, 'o', '$', 1, 1, 'g') = 'm$o'",
        "",
    ),
    (
        "varchar_lt",
        "WHERE col_varchar < 'abc'::varchar",
        'WHERE (("col_varchar")::"text" < (\'abc\'::character varying)::"text")',
    ),
    (
        "varchar_le",
        "WHERE col_varchar <= 'def'::varchar",
        'WHERE (("col_varchar")::"text" <= (\'def\'::character varying)::"text")',
    ),
    (
        "varchar_gt",
        "WHERE col_varchar > 'ghi'::varchar",
        'WHERE (("col_varchar")::"text" > (\'ghi\'::character varying)::"text")',
    ),
    (
        "varchar_ge",
        "WHERE col_varchar >= 'jkl'::varchar",
        'WHERE (("col_varchar")::"text" >= (\'jkl\'::character varying)::"text")',
    ),
    (
        "varchareq",
        "WHERE col_varchar = 'moo'::varchar",
        'WHERE (("col_varchar")::"text" = (\'moo\'::character varying)::"text")',
    ),
    (
        "varcharne",
        "WHERE col_varchar <> 'pqr'::varchar",
        'WHERE (("col_varchar")::"text" <> (\'pqr\'::character varying)::"text")',
    ),
    ("varcharlike", "WHERE col_varchar LIKE '%m%'", "~~"),
    ("varcharnlike", "WHERE col_varchar NOT LIKE '%q%'", "!~~"),
    ("varchariclike", "WHERE col_varchar ILIKE '%M%'", "~~*"),
    ("varcharicnlike", "WHERE col_varchar NOT ILIKE '%Q%'", "!~~*"),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_text_comparison_operator_pushdown(
    create_test_text_comparison_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = (
        "SELECT * FROM test_text_comparison_operator_pushdown.tbl "
        + operator_expression
    )
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["test_text_comparison_operator_pushdown.tbl"],
        ["test_text_comparison_operator_pushdown.heap_tbl"],
    )


def test_name_column_query_pushdown(
    create_test_text_comparison_operator_pushdown_table, pg_conn
):
    query = "SELECT col_name FROM test_text_comparison_operator_pushdown.tbl"
    explain = "EXPLAIN (ANALYZE, VERBOSE, format " "JSON" ") " + query
    explain_result = perform_query_on_cursor(explain, pg_conn)[0]
    print(explain_result)
    assert f"Custom Plan Provider" in str(explain_result)
    assert f"Query Pushdown" in str(explain_result)


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_test_text_comparison_operator_pushdown_table(pg_conn, s3, extension):

    url = f"s3://{TEST_BUCKET}/create_test_text_comparison_operator_pushdown_table/data.parquet"
    run_command(
        f"""
			COPY (
					SELECT NULL::text as col_text, NULL::name as col_name, NULL::varchar as col_varchar, NULL::char
						UNION ALL
                    SELECT ''::text as col_text, ''::name as col_name, ''::varchar as col_varchar, ''::char as col_char
					 	UNION ALL
                    SELECT 'test'::text as col_text, 'test'::name as col_name, 'test'::varchar as col_varchar, 's'::char
                        UNION ALL
                    SELECT 'moo'::text as col_text, 'moo'::name as col_name, 'moo'::varchar as col_varchar, 'e'::char
                        UNION ALL
                    SELECT 'true'::text as col_text, 'true'::name as col_name, 'true'::varchar as col_varchar, 't'::char
				) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE SCHEMA test_text_comparison_operator_pushdown;
	            CREATE FOREIGN TABLE test_text_comparison_operator_pushdown.tbl
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
	            CREATE TABLE test_text_comparison_operator_pushdown.heap_tbl
				(
                    col_text text,
                    col_name name,
                    col_varchar varchar,
                    col_char char
	            );
	            COPY test_text_comparison_operator_pushdown.heap_tbl FROM '{}';
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA test_text_comparison_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()
