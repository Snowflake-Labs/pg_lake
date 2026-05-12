import pytest
import psycopg2
import time
import duckdb
import math
import datetime
import json
from decimal import *
from utils_pytest import *

test_cases = [
    (
        "simple_unnest",
        """
            SELECT unnest(fruits)
            FROM untest
            ORDER BY unnest;
        """,
        True,
    ),
    (
        "sublink_unnest",
        """
            SELECT *
            FROM unnest((SELECT fruits FROM untest ORDER BY id LIMIT 1))
        """,
        True,
    ),
    (
        "double_unnest",
        """
            SELECT *
            FROM unnest((SELECT fruits FROM untest ORDER BY id LIMIT 1), array['Hello','World','!'])
        """,
        False,
    ),
    (
        "map_unnest",
        """
            SELECT unnest(colors)
            FROM untest
        """,
        False,
    ),
    (
        "group_by_unnest",
        """
            SELECT unnest(fruits) AS fruit_name, COUNT(*) AS fruit_count
            FROM untest
            GROUP BY fruit_name
            ORDER BY fruit_name;
        """,
        False,  # DuckDB does not allow unnest in GROUP BY
    ),
    (
        "where_clause_unnest",
        """
            SELECT id, unnest(fruits)
            FROM untest
            WHERE 'banana' = ANY(fruits)
            ORDER BY id, unnest;
        """,
        True,
    ),
    (
        "join_unnest",
        """
            SELECT t.id, f.fruit 
            FROM untest t
            JOIN LATERAL unnest(t.fruits) AS f(fruit) ON true
            WHERE f.fruit LIKE 'c%'
            ORDER BY t.id, f.fruit;
        """,
        True,
    ),
    (
        "group_by_grouping_sets",
        """
            SELECT
                CASE WHEN GROUPING(f.fruit) = 1 THEN 'ALL' ELSE f.fruit END AS fruit,
                COUNT(*) AS fruit_count
            FROM untest t
            CROSS JOIN LATERAL unnest(t.fruits) AS f(fruit)
            GROUP BY GROUPING SETS ((f.fruit), ())
            ORDER BY GROUPING(f.fruit), fruit;
        """,
        True,
    ),
    (
        "unnest_in_subquery",
        """
            SELECT sub.id, sub.fruit_name
            FROM (
                SELECT id, unnest(fruits) AS fruit_name
                FROM untest
            ) sub
            WHERE sub.fruit_name IN ('apple', 'banana')
            ORDER BY sub.id, sub.fruit_name;
        """,
        True,
    ),
    (
        "window_function_unnest",
        """
            SELECT 
                id, 
                unnest(fruits) AS fruit_name,
                row_number() OVER (
                    PARTITION BY id 
                    ORDER BY unnest(fruits)
                ) AS fruit_index
            FROM untest
            ORDER BY id, fruit_index;
        """,
        False,  # DuckDB does not allow unnest in window function
    ),
    (
        "distinct_unnest",
        """
            SELECT DISTINCT unnest(fruits) AS unique_fruit
            FROM untest
            ORDER BY unique_fruit;
        """,
        True,
    ),
    (
        "union_all_unnest",
        """
            SELECT unnest(fruits) AS fruit
            FROM untest
            WHERE id = 1
            UNION ALL
            SELECT unnest(fruits) AS fruit
            FROM untest
            WHERE id = 2
            ORDER BY fruit;
        """,
        True,
    ),
    (
        "cte_unnest",
        """
            WITH cte AS (
                SELECT id, unnest(fruits) AS fruit
                FROM untest
            )
            SELECT fruit, COUNT(*) AS total
            FROM cte
            GROUP BY fruit
            ORDER BY fruit;
        """,
        True,
    ),
    (
        "filter_aggregate_unnest",
        """
            SELECT f.fruit, 
                   COUNT(*) FILTER (WHERE f.fruit LIKE 'a%') AS starts_with_a
            FROM untest t
            CROSS JOIN LATERAL unnest(t.fruits) AS f(fruit)
            GROUP BY f.fruit
            ORDER BY f.fruit;
        """,
        True,
    ),
    (
        "multiple_cross_join",
        """
            SELECT f.fruit, c.color
            FROM untest t
            CROSS JOIN LATERAL unnest(t.fruits) AS f(fruit)
            CROSS JOIN LATERAL (
                SELECT unnest(ARRAY['red','yellow','blue']) AS color
            ) c
            ORDER BY t.id, f.fruit, c.color;
        """,
        True,
    ),
    (
        "unnest_with_ordinality",
        """
			SELECT t.id, fruit_info.fruit_name, fruit_info.idx
			FROM untest t
			CROSS JOIN LATERAL unnest(t.fruits) WITH ORDINALITY 
				AS fruit_info(fruit_name, idx)
			ORDER BY t.id, fruit_info.idx;
		""",
        False,  # WITH ORDINALITY is not implemented in DuckDB
    ),
    (
        "unnest_with_ordinality_group_by",
        """
			SELECT
				fruit_info.fruit_name,
				COUNT(*) AS total_occurrences
			FROM untest t
			CROSS JOIN LATERAL unnest(t.fruits) WITH ORDINALITY 
				AS fruit_info(fruit_name, idx)
			GROUP BY fruit_info.fruit_name
			ORDER BY fruit_info.fruit_name;
		""",
        False,  # WITH ORDINALITY is not implemented in DuckDB
    ),
]


@pytest.mark.parametrize(
    "name, query, pushdown",
    test_cases,
    ids=[test_case[0] for test_case in test_cases],
)
def test_unnest_pushdown(create_unnest_table, pg_conn, name, query, pushdown):
    if pushdown:
        assert_remote_query_contains_expression(query, '"unnest"(', pg_conn)
    else:
        assert_remote_query_not_contains_expression(query, '"unnest"(', pg_conn)

    assert_query_results_on_tables(query, pg_conn, ["untest"], ["untest_heap"])


@pytest.fixture(scope="module")
def create_unnest_table(s3, pg_conn, superuser_conn, extension):
    """
    Fixture to set up a table for UNNEST queries.
    Creates a table with an array column and inserts sample data.
    Drops the table after the tests are done.
    """

    run_command(
        """
        SELECT map_type.create('text', 'text')
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    # Create test table
    create_table_query = f"""
        CREATE TABLE untest (
            id SERIAL,
            fruits TEXT[],
            colors map_type.key_text_val_text
        )
        USING iceberg
        WITH (location = 's3://{TEST_BUCKET}/untest/');
    """
    run_command(create_table_query, pg_conn)

    # Insert sample array data
    insert_data_query = """
        INSERT INTO untest (fruits, colors)
        VALUES
            (ARRAY['apple','banana','cherry'], array[('apple','red')]::map_type.key_text_val_text),
            (ARRAY['date','elderberry','fig'], array[('fig','green')]::map_type.key_text_val_text);
    """
    run_command(insert_data_query, pg_conn)

    # Copy to a heap table
    copy_to_heap_query = """
        CREATE TABLE untest_heap AS SELECT * FROM untest;
    """
    run_command(copy_to_heap_query, pg_conn)

    pg_conn.commit()

    # Yield control back to the test(s)
    yield

    pg_conn.rollback()

    # Clean up
    drop_table_query = "DROP TABLE untest, untest_heap;"
    run_command(drop_table_query, pg_conn)
    pg_conn.commit()
