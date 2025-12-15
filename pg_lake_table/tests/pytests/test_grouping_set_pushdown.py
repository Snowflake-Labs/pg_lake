import pytest
from utils_pytest import *


queries = [
    # Simple ROLLUP
    "SELECT id, GROUPING(id, event_type), COUNT(*), SUM(metric2) FROM Events_f GROUP BY ROLLUP(id, event_type) ORDER BY 1,2,3,4 LIMIT 5;",
    # Using ROLLUP with a JOIN
    "SELECT u.id, e.event_type, GROUPING(u.id, e.event_type), SUM(u.metric1) FROM Users_f u JOIN Events_f e ON u.id = e.id GROUP BY ROLLUP(u.id, e.event_type);",
    # Multiple grouping sets
    "SELECT id, GROUPING(id), SUM(metric2) FROM Users_f GROUP BY GROUPING SETS ((id), ());",
    # Grouping sets with multiple groups
    "SELECT e.id, event_type, GROUPING(e.id, event_type), SUM(u.metric1), COUNT(e.metric2) FROM Users_f u JOIN Events_f e ON(e.id=u.id) GROUP BY GROUPING SETS ((e.id, event_type), (u.id), (event_type), ());",
    # CUBE with aggregates
    "SELECT u.id, event_type, GROUPING(u.id, event_type), AVG(u.float_value), COUNT(*) FROM Users_f u JOIN Events_f e ON(e.id=u.id) GROUP BY CUBE(u.id, event_type);",
    # Nested grouping sets
    "SELECT id, GROUPING(id), SUM(metric1) FROM Users_f GROUP BY GROUPING SETS (id, ());",
    # Complex grouping with order by
    "SELECT id, GROUPING(id), SUM(metric1) FROM Users_f GROUP BY GROUPING SETS ((id)) ORDER BY GROUPING(id), SUM(metric1) DESC;",
    # Aggregates with GROUPING SETS
    "SELECT id, COUNT(*), SUM(metric1) FROM Users_f GROUP BY GROUPING SETS ((id), ());",
    # More complex CUBE example
    "SELECT id, event_type, GROUPING(id, event_type), SUM(metric2) FROM Events_f GROUP BY CUBE(id, event_type);",
    # Rollup with different ordering
    "SELECT id, event_type, GROUPING(id, event_type), COUNT(*), MAX(metric2) FROM Events_f GROUP BY ROLLUP(id, event_type) ORDER BY id DESC, event_type;",
    # Cube with complex order
    "SELECT id, event_type, GROUPING(id, event_type), COUNT(*), SUM(metric2) FROM Events_f GROUP BY CUBE(id, event_type) ORDER BY COUNT(*) DESC;",
    # Grouping sets with explicit ordering
    "SELECT id, GROUPING(id), COUNT(*), SUM(metric1) FROM Users_f GROUP BY GROUPING SETS ((id)) ORDER BY SUM(metric1) DESC;",
    # Complex grouping set with multiple levels
    "SELECT id, event_type, GROUPING(id, event_type), SUM(metric2), COUNT(*) FROM Events_f GROUP BY GROUPING SETS ((id), (event_type), (id, event_type));",
    # Grouping with all possible combinations
    "SELECT e.id, event_type, GROUPING(e.id, event_type), COUNT(*), AVG(metric1) FROM Users_f u JOIN Events_f e ON(e.id=u.id) GROUP BY GROUPING SETS ((e.id, event_type), (e.id), (event_type), ());",
    # Nested GROUPING SETS
    "SELECT sum(id) FROM Users_f GROUP BY GROUPING SETS((), GROUPING SETS((), GROUPING SETS(()))) ORDER BY 1 DESC;",
    # Complex GROUPING SETS with CUBE and ROLLUP
    "SELECT id, event_type, SUM(metric2) FROM Events_f GROUP BY GROUPING SETS(GROUPING SETS(ROLLUP(id), GROUPING SETS(CUBE(event_type)))) ORDER BY 1 DESC;",
    # Grouping with empty sets and multiple levels
    "SELECT id, event_type, SUM(metric2) FROM Events_f GROUP BY GROUPING SETS((), GROUPING SETS((), GROUPING SETS((id, event_type)))) ORDER BY 1 DESC;",
    # Use of GROUPING() with joins
    "SELECT u.id, e.event_type, GROUPING(u.id, e.event_type), SUM(u.metric1), MAX(e.metric2) FROM Users_f u JOIN Events_f e ON(e.id=u.id) GROUP BY GROUPING SETS ((u.id, e.event_type), (u.id)), ();",
    # Nested GROUPING SETS with joins and multiple group levels
    "SELECT u.id, e.event_type, GROUPING(u.id, e.event_type), SUM(u.metric1) FROM Users_f u JOIN Events_f e ON(e.id=u.id) GROUP BY GROUPING SETS(GROUPING SETS(ROLLUP(u.id, e.event_type), CUBE(u.id))), ();",
    # Multiple GROUPING SETS with complex aggregates
    "SELECT id, GROUPING(id), SUM(metric1), COUNT(*), MAX(metric2) FROM Users_f GROUP BY GROUPING SETS((id), ());",
    # Complex nesting with window functions over GROUPING SETS
    "SELECT id, SUM(metric1) AS total_metric1, SUM(SUM(metric1)) OVER (ORDER BY id) AS cumulative_sum FROM Users_f GROUP BY ROLLUP(id) ORDER BY cumulative_sum, id;",
    # Advanced ROLLUP with functional dependencies
    "SELECT id, event_type, GROUPING(id, event_type), SUM(metric2) FROM Events_f GROUP BY ROLLUP(id, event_type);",
    # Nested CUBE with multiple grouping
    "SELECT id, event_type, GROUPING(id, event_type), SUM(metric2), COUNT(*) FROM Events_f GROUP BY CUBE(id, event_type);",
    # Rollup with more complex ordering
    "SELECT id, event_type, GROUPING(id, event_type), COUNT(*), MAX(metric2) FROM Events_f GROUP BY ROLLUP(id, event_type) ORDER BY COUNT(*) DESC, MAX(metric2);",
    # Grouping sets with explicit ordering and window function
    "SELECT id, GROUPING(id), COUNT(*), SUM(metric1) FROM Users_f GROUP BY GROUPING SETS ((id)) ORDER BY SUM(metric1) DESC, COUNT(*) OVER (ORDER BY SUM(metric1) DESC);",
    # Multi-level GROUPING SETS with conditional aggregation
    "SELECT id, event_type, GROUPING(id, event_type), COUNT(*), AVG(CASE WHEN metric2 > 10 THEN metric2 ELSE NULL END) FROM Events_f GROUP BY GROUPING SETS ((id, event_type), (id), (event_type), ());",
    # Nesting with window functions
    "SELECT id, SUM(metric1), SUM(SUM(metric1)) OVER (ORDER BY id, metric1) AS running_total FROM Users_f GROUP BY ROLLUP(id, metric1) ORDER BY running_total, id;",
]


def test_grouping_sets_pushdown(create_pushdown_tables, pg_conn):

    for query in queries:
        print(query)
        # first, compare the query results
        assert_query_results_on_tables(
            query, pg_conn, ["Users_f", "Events_f"], ["Users", "Events"]
        )

        # some query have only OVER, PARTITION BY is not always required
        if "GROUPING" in query:
            assert_remote_query_contains_expression(query, "GROUPING", pg_conn), query
        elif "ROLLUP" in query:
            assert_remote_query_contains_expression(query, "ROLLUP", pg_conn), query
        else:
            assert False


pg_queries = [
    #  test handling of subqueries in grouping sets
    """select grouping((select t1.v from gstest5 t2 where id = t1.id)),
        (select t1.v from gstest5 t2 where id = t1.id) as s
    from gstest5 t1
    group by grouping sets(v, s)
    order by case when grouping((select t1.v from gstest5 t2 where id = t1.id)) = 0
                then (select t1.v from gstest5 t2 where id = t1.id)
                else null end
            nulls first;""",
    """select grouping((select t1.v from gstest5 t2 where id = t1.id)),
        (select t1.v from gstest5 t2 where id = t1.id) as s
    from gstest5 t1
    group by grouping sets(v, s)
    order by case when grouping((select t1.v from gstest5 t2 where id = t1.id)) = 0
                then (select t1.v from gstest5 t2 where id = t1.id)
                else null end
            nulls first;""",
    """select grouping((select t1.v from gstest5 t2 where id = t1.id)),
        (select t1.v from gstest5 t2 where id = t1.id) as s,
        case when grouping((select t1.v from gstest5 t2 where id = t1.id)) = 0
                then (select t1.v from gstest5 t2 where id = t1.id)
                else null end as o
    from gstest5 t1
    group by grouping sets(v, s)
    order by o nulls first;""",
    """select grouping((select t1.v from gstest5 t2 where id = t1.id)),
        (select t1.v from gstest5 t2 where id = t1.id) as s,
        case when grouping((select t1.v from gstest5 t2 where id = t1.id)) = 0
                then (select t1.v from gstest5 t2 where id = t1.id)
                else null end as o
    from gstest5 t1
    group by grouping sets(v, s)
    order by o nulls first;""",
    #  test handling of expressions that should match lower target items
    """select a < b and b < 3 from (values (1, 2)) t(a, b) group by rollup(a < b and b < 3) having a < b and b < 3;"""
    """select a < b and b < 3 from (values (1, 2)) t(a, b) group by rollup(a < b and b < 3) having a < b and b < 3;"""
    """select not a from (values(true)) t(a) group by rollup(not a) having not not a;"""
    """select not a from (values(true)) t(a) group by rollup(not a) having not not a;""",
]


# queries are taken from postgres commit https://github.com/postgres/postgres/commit/247dea8
def test_grouping_sets_pushdown2(pg_conn, with_default_location):
    run_command(
        """create table gstest5(id integer, v integer) USING iceberg;
                   insert into gstest5 select i, i from generate_series(1,5)i;

                   create table gstest5_pg(id integer, v integer);
                   insert into gstest5_pg select i, i from generate_series(1,5)i;""",
        pg_conn,
    )

    for query in queries:
        print(query)
        # first, compare the query results
        assert_query_results_on_tables(
            query, pg_conn, ["gstest5", "gstest5"], ["gstest5_pg", "gstest5_pg"]
        )

        if "GROUPING" in query:
            assert_remote_query_contains_expression(query, "GROUPING", pg_conn), query
        elif "ROLLUP" in query:
            assert_remote_query_contains_expression(query, "ROLLUP", pg_conn), query
        else:
            assert False


def test_grouping_sets_pushdown_bool(pg_conn, with_default_location):
    run_command(
        """
        CREATE TABLE test_grouping_issue USING iceberg as
        SELECT
              gs,
              (random() < 0.8) AS success
              FROM generate_series(1, 1000) gs
        """,
        pg_conn,
    )

    query = """
    select true as tool_boolean, sum(gs), avg(gs), grouping(true)
    from test_grouping_issue
    group by rollup(1)
    """

    assert_query_results_on_tables(
        query, pg_conn, ["test_grouping_issue", "test_grouping_issue"]
    )

    pg_conn.rollback()
