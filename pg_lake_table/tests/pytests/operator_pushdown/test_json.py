import pytest
import json
from utils_pytest import *

test_cases = [
    # JSON functions and operators
    (
        "json_array_length",
        "WHERE col_is_array = 1 and json_array_length(col_json) > 1",
        "json_array_length",
    ),
    (
        "col_json_scalar",
        "WHERE col_json->>'hello' = 'world'",
        "->> 'hello'",
    ),
    (
        "col_json_nested",
        "WHERE col_json->'atts'->>'k1' = 'v1'",
        "-> 'atts'",
    ),
    (
        "col_json_array_scalar",
        "WHERE col_json->>0 = '1'",
        "->> 0",
    ),
    (
        "col_json_array_nested",
        "WHERE col_json->'items'->0->>'a' = '1'",
        "-> 0",
    ),
    (
        "col_jsonb_literal",
        "WHERE col_jsonb = '123'::jsonb",
        "123",
    ),
    # JSONB functions and operators
    (
        "jsonb_array_length",
        "WHERE col_is_array = 1 and jsonb_array_length(col_jsonb) > 1",
        "json_array_length",
    ),
    (
        "col_jsonb_text",
        "WHERE col_jsonb->>'hello' = 'world'",
        "->> 'hello'",
    ),
    (
        "col_jsonb_nested",
        "WHERE col_jsonb->'atts'->>'k1' = 'v1'",
        "-> 'atts'",
    ),
    (
        "col_jsonb_array_scalar",
        "WHERE col_jsonb->>0 = '2'",
        "->> 0",
    ),
    (
        "col_jsonb_array_nested",
        "WHERE col_jsonb->'items'->0->>'a' = '1'",
        "-> 0",
    ),
    (
        "col_jsonb_literal",
        "WHERE col_jsonb = '123'::jsonb",
        "123",
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_cases[0] for test_cases in test_cases],
)
def test_json_operator_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT * FROM json_operator_pushdown.tbl " + operator_expression
    if test_id == "col_jsonb_literal":
        # An external Parquet jsonb column may be physically VARIANT. Its
        # rendered text is not safe for PostgreSQL jsonb comparisons.
        assert_remote_query_not_contains_expression(query, expected_expression, pg_conn)
    else:
        assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["json_operator_pushdown.tbl"],
        ["json_operator_pushdown.heap_tbl"],
    )


# create the table on both Postgres
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, request, extension):

    run_command(
        """
	            CREATE SCHEMA json_operator_pushdown;

	            """,
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"

    data_query = """
		SELECT NULL::json as col_json, NULL::jsonb as col_jsonb, 0
			UNION ALL
		SELECT '{"hello":"world", "atts":{"k1":"v1", "k2":"v2"}}'::json as col_json, '{"hello":"world", "atts":{"k1":"v1", "k2":"v2"}}'::jsonb as col_jsonb, 0
			UNION ALL
		SELECT '{"items":[{"a":1},{"b":1}]}'::json as col_json, '{"items":[{"a":1},{"b":1}]}'::jsonb as col_jsonb, 0
			UNION ALL
		SELECT '[1, 3]'::json as col_json, '[2, 4]'::jsonb as col_jsonb, 1
			UNION ALL
		SELECT '{}'::json as col_json, '{}'::jsonb as col_jsonb, 0
			UNION ALL
		SELECT '123'::json as col_json, '123'::jsonb as col_jsonb, 0
        """

    run_command(
        f"""
			COPY ({data_query}) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    # Create a table with 2 columns on the fdw
    run_command(
        """
	            CREATE FOREIGN TABLE json_operator_pushdown.tbl
	            (
					col_json json,
					col_jsonb jsonb,
					col_is_array int
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
	            CREATE TABLE json_operator_pushdown.heap_tbl AS
				SELECT * FROM json_operator_pushdown.tbl;
	            """.format(
            url
        ),
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command("DROP SCHEMA json_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()
