import pytest
from utils_pytest import *

queries = [
    [
        "UNION",
        "SELECT DISTINCT id, metric2 FROM Users_f UNION SELECT id, metric2 FROM Events_f;",
    ],
    [
        "UNION ALL",
        "SELECT DISTINCT id, float_value FROM Users_f UNION ALL SELECT id, float_value FROM Events_f;",
    ],
    ["INTERSECT", "SELECT DISTINCT id FROM Users_f INTERSECT SELECT id FROM Events_f;"],
    [
        "EXCEPT",
        "SELECT DISTINCT metric2 FROM Users_f EXCEPT SELECT metric2 FROM Events_f UNION SELECT 1;",
    ],
    [
        "UNION",
        "SELECT DISTINCT * FROM (SELECT id, metric1 FROM Users_f WHERE metric1 < 100) AS Subquery1 UNION SELECT * FROM (SELECT id, metric2 FROM Events_f WHERE metric2 > 100) AS Subquery2;",
    ],
    [
        "INTERSECT",
        "SELECT AVG(float_value) AS average_value FROM (SELECT float_value FROM Users_f INTERSECT SELECT float_value FROM Events_f) AS Subquery;",
    ],
    [
        "UNION",
        "WITH UserData AS (SELECT id, metric1 FROM Users_f), EventData AS (SELECT id, event_type FROM Events_f) SELECT * FROM UserData UNION SELECT * FROM EventData;",
    ],
    [
        "UNION",
        "SELECT U.id, E.event_type FROM Users_f U JOIN Events_f E ON U.id = E.id UNION SELECT U.id, E.metric2 FROM Users_f U JOIN Events_f E ON U.big_metric = E.big_metric;",
    ],
    [
        "UNION",
        "SELECT U.id, E.event_type FROM Users_f U, Events_f E WHERE U.id = E.id UNION SELECT U.id, E.metric2 FROM Users_f U, Events_f E WHERE U.metric2 = E.metric2;",
    ],
    [
        "UNION",
        "WITH FilteredUsers_f AS (SELECT * FROM Users_f WHERE metric1 > 50) SELECT id FROM FilteredUsers_f UNION SELECT id FROM Events_f INTERSECT SELECT id FROM Users_f WHERE metric2 < 30;",
    ],
    [
        "INTERSECT",
        "SELECT U.id, U.metric2 FROM Users_f U JOIN Events_f E ON U.id = E.id INTERSECT SELECT id, metric2 FROM Users_f WHERE float_value < 50.0;",
    ],
    [
        "UNION",
        "SELECT id, (SELECT AVG(metric1) FROM Users_f) AS avg_metric FROM Users_f UNION SELECT id, metric2 FROM Events_f WHERE metric2 > 10;",
    ],
    [
        "UNION ALL",
        "SELECT * FROM (SELECT id, metric1 FROM Users_f ORDER BY id DESC, metric1 DESC LIMIT 10) AS A UNION ALL SELECT * FROM (SELECT id, metric2 FROM Events_f ORDER BY id DESC, metric2 DESC LIMIT 5) AS B;",
    ],
    [
        "EXCEPT",
        "SELECT id, SUM(metric1) AS total_metric FROM Users_f GROUP BY id EXCEPT SELECT id, SUM(metric2) AS total_metric FROM Events_f GROUP BY id;",
    ],
    [
        "UNION",
        "SELECT id, (SELECT * FROM (SELECT MIN(metric1) FROM Users_f UNION SELECT MAX(metric2) FROM Events_f) ORDER BY 1 LIMIT 1) AS metric FROM Users_f WHERE id = ANY(SELECT max(id) FROM Events_f);",
    ],
    [
        "INTERSECT",
        "SELECT * FROM Users_f WHERE metric1 IN (SELECT metric1 FROM Users_f INTERSECT SELECT metric2 FROM Events_f);",
    ],
    [
        "EXCEPT",
        "WITH UserMetrics AS (SELECT id, metric1 FROM Users_f), EventMetrics AS (SELECT id, metric2 FROM Events_f) SELECT UM.id, (SELECT metric1 FROM UserMetrics UM2 WHERE UM2.id = UM.id EXCEPT SELECT metric2 FROM EventMetrics EM WHERE EM.id = UM.id) AS UniqueMetrics FROM UserMetrics UM;",
    ],
    [
        "UNION ALL",
        "SELECT id, metric1 FROM Users_f WHERE id IN (SELECT id FROM Users_f UNION ALL SELECT id FROM Events_f WHERE event_type > 2);",
    ],
    [
        "INTERSECT",
        "SELECT DISTINCT U.id, U.metric1 FROM Users_f U WHERE EXISTS (SELECT E.id FROM Events_f E WHERE U.id = E.id INTERSECT SELECT id FROM Users_f WHERE metric2 < 100);",
    ],
    [
        "UNION",
        "SELECT id, metric1 FROM Users_f WHERE metric1 IN (SELECT metric1 FROM Users_f WHERE metric1 < 100 UNION SELECT metric2 FROM Events_f WHERE metric2 < 100)",
    ],
    [
        "INTERSECT",
        "SELECT DISTINCT U.id, (SELECT SUM(metric1) FROM Users_f WHERE id = U.id INTERSECT SELECT SUM(metric2) FROM Events_f WHERE id = U.id) AS IntersectionSum FROM Users_f U;",
    ],
    [
        "UNION",
        "SELECT DISTINCT id FROM (SELECT U.id FROM Users_f U JOIN Events_f E ON U.metric2 = E.metric2 UNION SELECT E.id FROM Events_f E JOIN Users_f U ON E.big_metric = U.big_metric) AS JoinedData;",
    ],
    [
        "EXCEPT",
        "SELECT DISTINCT id, (SELECT metric1 FROM Users_f U WHERE U.id = Users_f.id AND metric1 > 100 EXCEPT SELECT metric2 FROM Events_f E WHERE E.id = Users_f.id) AS DiffMetric FROM Users_f WHERE id IN (SELECT id FROM Events_f WHERE event_type = 3);",
    ],
]


def test_set_operation_pushdown(create_pushdown_tables, pg_conn):

    for query_data in queries:

        query = query_data[1]

        # first, compare the query results
        assert_query_results_on_tables(
            query, pg_conn, ["Users_f", "Events_f"], ["Users", "Events"]
        )

        # then replace table names for ensuring SET OPERATION pushdown
        expected_exprs = query_data[0]
        assert_remote_query_contains_expression(query, expected_exprs, pg_conn)
