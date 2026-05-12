import pytest
from utils_pytest import *

queries = [
    # OPERATOR is deparsed as =
    ["WHERE", "SELECT DISTINCT * FROM Users_f WHERE (id OPERATOR(pg_catalog.=) 6);"],
    # cast to text is deparsed as ::"text"
    [
        '::"text"',
        "SELECT DISTINCT * FROM (select cast(id as text) from Users_f GROUP BY id)",
    ],
    # cannot pushdown as the type Users_f is not pushable, still useful to have coverage
    ['SELECT "id"', "SELECT Users_f FROM Users_f"],
    # cannot pushdown row / record functions
    # ["JOIN", "SELECT count(distinct bar.col1) FROM (SELECT row(id) FROM Users_f) as foo(col1) JOIN (SELECT row(id) FROM Events_f) bar(col1) ON (true)"],
    # Anti-Join
    [
        "NOT (EXISTS",
        "SELECT DISTINCT u.* FROM Users_f u WHERE NOT EXISTS (SELECT 1 FROM Events_f e WHERE e.id > u.id)",
    ],
    # JOIN LATERAL
    [
        "LATERAL",
        "SELECT DISTINCT * FROM Users_f t1 JOIN LATERAL (SELECT DISTINCT metric2 FROM Events_f t2 WHERE t1.id = t2.id) ON (true);",
    ],
    # LATERAL subquery
    [
        "LATERAL",
        "SELECT DISTINCT e.* FROM Users_f u, LATERAL (SELECT DISTINCT metric2 FROM Events_f e WHERE e.id = u.id) e;",
    ],
    [
        '"count"(DISTINCT "array_metric"[1])',
        "SELECT COUNT(DISTINCT array_metric[1]) FILTER (WHERE e.id = 1000000) FROM Events_f e",
    ],
    [
        'DISTINCT ARRAY["max"("id"), "min"("metric1")',
        "SELECT DISTINCT ARRAY[max(id), min(metric1)] from Users_f",
    ],
]


def test_expression_pushdown(create_pushdown_tables, pg_conn):

    for ops in queries:

        exprs = ops[0]
        query = ops[1]

        # first, compare the query results
        assert_query_results_on_tables(
            query, pg_conn, ["Users_f", "Events_f"], ["Users", "Events"]
        )

        # then make sure we can pushdown the relevant expressions
        assert_remote_query_contains_expression(query, exprs, pg_conn), query
