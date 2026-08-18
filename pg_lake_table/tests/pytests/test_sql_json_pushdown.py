from utils_pytest import *


# SQL/JSON expressions are deparsed back into the standard SQL/JSON syntax,
# which the DuckDB parser does not understand, so none of these may be pushed
# down. Each entry is the minimum server_version_num that supports it.
json_expressions = [
    (160000, "col IS JSON"),
    (160000, "col IS NOT JSON"),
    (160000, "col IS JSON VALUE"),
    (160000, "col IS JSON OBJECT"),
    (160000, "col IS JSON ARRAY"),
    (160000, "col IS JSON SCALAR"),
    (160000, "col IS JSON WITH UNIQUE KEYS"),
    (160000, "col IS JSON WITHOUT UNIQUE KEYS"),
    (160000, "JSON(col)"),
    (160000, "JSON_SCALAR(col)"),
    (160000, "JSON_SERIALIZE(col::json)"),
    (160000, "JSON_OBJECT('key' VALUE col)"),
    (160000, "JSON_ARRAY(col)"),
    (170000, "JSON_EXISTS(col::json, '$.key')"),
    (170000, "JSON_VALUE(col::json, '$.key')"),
    (170000, "JSON_QUERY(col::json, '$.key')"),
]

# Aggregates need their own query shape, so they are listed separately.
json_aggregates = [
    (160000, "JSON_OBJECTAGG(col VALUE col)"),
    (160000, "JSON_ARRAYAGG(col)"),
]


def test_sql_json_pushdown(s3, pg_conn, extension, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_sql_json_pushdown;
        SET search_path TO test_sql_json_pushdown;

        CREATE TABLE json_docs(col text) USING iceberg;

        INSERT INTO json_docs VALUES ('{"key": "value"}'), ('[1, 2]'), ('42');
        """,
        pg_conn,
    )

    pg_version_num = get_pg_version_num(pg_conn)

    for min_version_num, expression in json_expressions:
        if pg_version_num < min_version_num:
            continue

        # both in the target list and in a qual, since the deparsed query is
        # rejected by DuckDB wherever the expression ends up
        for query in [
            f"SELECT {expression} FROM json_docs ORDER BY col",
            f"SELECT col FROM json_docs WHERE ({expression})::text = 'x'",
        ]:
            assert_query_not_pushdownable(query, pg_conn)

            # the query still has to give an answer via the FDW path
            run_query(query, pg_conn)

    for min_version_num, expression in json_aggregates:
        if pg_version_num < min_version_num:
            continue

        query = f"SELECT {expression} FROM json_docs"

        assert_query_not_pushdownable(query, pg_conn)
        run_query(query, pg_conn)

    # the query from the original report, which used to fail with
    # "Parser Error: syntax error at or near \"JSON\""
    results = run_query("SELECT col FROM json_docs WHERE col IS JSON OBJECT", pg_conn)
    assert [row["col"] for row in results] == ['{"key": "value"}']

    pg_conn.rollback()
