import pytest
from utils_pytest import *


def test_select_empty_table(s3, pg_conn, extension):
    """Test SELECT from empty DuckLake table"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_select_empty/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_select_empty(
            id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # SELECT from empty table should return no rows
    result = run_query(
        "SELECT * FROM test_ducklake_select_empty",
        pg_conn,
    )
    assert len(result) == 0

    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_select_empty",
        pg_conn,
    )
    assert result[0][0] == 0

    run_command("DROP FOREIGN TABLE test_ducklake_select_empty", pg_conn)
    pg_conn.rollback()


def test_select_after_insert(s3, pg_conn, extension):
    """Test SELECT retrieves inserted data correctly"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_select_basic/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_select_basic(
            id BIGINT,
            name TEXT,
            value NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert data
    run_command(
        """
        INSERT INTO test_ducklake_select_basic VALUES
        (1, 'Alice', 100.5),
        (2, 'Bob', 200.75),
        (3, 'Charlie', 300.0)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Test SELECT *
    result = run_query(
        "SELECT * FROM test_ducklake_select_basic ORDER BY id",
        pg_conn,
    )
    assert len(result) == 3
    assert result[0] == [1, "Alice", 100.5]
    assert result[2] == [3, "Charlie", 300.0]

    # Test SELECT specific columns
    result = run_query(
        "SELECT name, value FROM test_ducklake_select_basic WHERE id = 2",
        pg_conn,
    )
    assert result[0] == ["Bob", 200.75]

    # Test COUNT
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_select_basic",
        pg_conn,
    )
    assert result[0][0] == 3

    run_command("DROP FOREIGN TABLE test_ducklake_select_basic", pg_conn)
    pg_conn.rollback()


def test_select_with_filters(s3, pg_conn, extension):
    """Test SELECT with WHERE clauses"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_select_filters/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_select_filters(
            id BIGINT,
            category TEXT,
            amount NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert test data
    run_command(
        """
        INSERT INTO test_ducklake_select_filters
        SELECT i, CASE WHEN i % 2 = 0 THEN 'even' ELSE 'odd' END, i * 10.0
        FROM generate_series(1, 20) i
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Test equality filter
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_select_filters WHERE category = 'even'",
        pg_conn,
    )
    assert result[0][0] == 10

    # Test range filter
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_select_filters WHERE id BETWEEN 5 AND 15",
        pg_conn,
    )
    assert result[0][0] == 11

    # Test numeric comparison
    result = run_query(
        "SELECT id FROM test_ducklake_select_filters WHERE amount > 150 ORDER BY id LIMIT 1",
        pg_conn,
    )
    assert result[0][0] == 16

    run_command("DROP FOREIGN TABLE test_ducklake_select_filters", pg_conn)
    pg_conn.rollback()


def test_select_with_aggregations(s3, pg_conn, extension):
    """Test SELECT with aggregate functions"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_select_agg/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_select_agg(
            id BIGINT,
            category TEXT,
            value NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert test data
    run_command(
        """
        INSERT INTO test_ducklake_select_agg VALUES
        (1, 'A', 10),
        (2, 'A', 20),
        (3, 'B', 30),
        (4, 'B', 40),
        (5, 'C', 50)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Test GROUP BY with COUNT
    result = run_query(
        "SELECT category, COUNT(*) FROM test_ducklake_select_agg GROUP BY category ORDER BY category",
        pg_conn,
    )
    assert result[0] == ["A", 2]
    assert result[1] == ["B", 2]
    assert result[2] == ["C", 1]

    # Test SUM
    result = run_query(
        "SELECT category, SUM(value) FROM test_ducklake_select_agg GROUP BY category ORDER BY category",
        pg_conn,
    )
    assert result[0] == ["A", 30]
    assert result[1] == ["B", 70]

    # Test AVG
    result = run_query(
        "SELECT AVG(value) FROM test_ducklake_select_agg",
        pg_conn,
    )
    assert result[0][0] == 30

    run_command("DROP FOREIGN TABLE test_ducklake_select_agg", pg_conn)
    pg_conn.rollback()


def test_select_with_joins(s3, pg_conn, extension):
    """Test SELECT with joins between DuckLake tables"""
    location1 = f"s3://{TEST_BUCKET}/ducklake/test_join_orders/"
    location2 = f"s3://{TEST_BUCKET}/ducklake/test_join_customers/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_orders(
            order_id BIGINT,
            customer_id BIGINT,
            amount NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location1}')
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_customers(
            customer_id BIGINT,
            name TEXT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location2}')
        """,
        pg_conn,
    )

    # Insert test data
    run_command(
        """
        INSERT INTO test_ducklake_customers VALUES
        (1, 'Alice'),
        (2, 'Bob'),
        (3, 'Charlie')
        """,
        pg_conn,
    )

    run_command(
        """
        INSERT INTO test_ducklake_orders VALUES
        (101, 1, 100.0),
        (102, 1, 150.0),
        (103, 2, 200.0),
        (104, 3, 300.0)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Test INNER JOIN
    result = run_query(
        """
        SELECT c.name, COUNT(o.order_id), SUM(o.amount)
        FROM test_ducklake_customers c
        JOIN test_ducklake_orders o ON c.customer_id = o.customer_id
        GROUP BY c.name
        ORDER BY c.name
        """,
        pg_conn,
    )
    assert result[0] == ["Alice", 2, 250.0]
    assert result[1] == ["Bob", 1, 200.0]

    run_command("DROP FOREIGN TABLE test_ducklake_orders, test_ducklake_customers", pg_conn)
    pg_conn.rollback()


def test_select_with_order_by(s3, pg_conn, extension):
    """Test SELECT with ORDER BY"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_select_order/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_order(
            id BIGINT,
            name TEXT,
            score NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert unordered data
    run_command(
        """
        INSERT INTO test_ducklake_order VALUES
        (3, 'Charlie', 85.5),
        (1, 'Alice', 92.0),
        (5, 'Eve', 78.0),
        (2, 'Bob', 88.5),
        (4, 'David', 95.0)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Test ORDER BY ascending
    result = run_query(
        "SELECT name FROM test_ducklake_order ORDER BY id",
        pg_conn,
    )
    assert [r[0] for r in result] == ["Alice", "Bob", "Charlie", "David", "Eve"]

    # Test ORDER BY descending
    result = run_query(
        "SELECT name FROM test_ducklake_order ORDER BY score DESC",
        pg_conn,
    )
    assert result[0][0] == "David"
    assert result[4][0] == "Eve"

    run_command("DROP FOREIGN TABLE test_ducklake_order", pg_conn)
    pg_conn.rollback()


def test_select_with_limit(s3, pg_conn, extension):
    """Test SELECT with LIMIT and OFFSET"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_select_limit/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_limit(
            id BIGINT,
            value BIGINT
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert test data
    run_command(
        "INSERT INTO test_ducklake_limit SELECT i, i * 10 FROM generate_series(1, 50) i",
        pg_conn,
    )
    pg_conn.commit()

    # Test LIMIT
    result = run_query(
        "SELECT COUNT(*) FROM (SELECT * FROM test_ducklake_limit LIMIT 10) sub",
        pg_conn,
    )
    assert result[0][0] == 10

    # Test LIMIT with OFFSET
    result = run_query(
        "SELECT id FROM test_ducklake_limit ORDER BY id LIMIT 5 OFFSET 10",
        pg_conn,
    )
    assert len(result) == 5
    assert result[0][0] == 11
    assert result[4][0] == 15

    run_command("DROP FOREIGN TABLE test_ducklake_limit", pg_conn)
    pg_conn.rollback()


def test_select_null_handling(s3, pg_conn, extension):
    """Test SELECT with NULL value handling"""
    location = f"s3://{TEST_BUCKET}/ducklake/test_select_nulls/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_ducklake_nulls_select(
            id BIGINT,
            optional_text TEXT,
            optional_num NUMERIC
        )
        SERVER pg_lake_ducklake
        OPTIONS (location '{location}')
        """,
        pg_conn,
    )

    # Insert data with NULLs
    run_command(
        """
        INSERT INTO test_ducklake_nulls_select VALUES
        (1, 'text1', 10),
        (2, NULL, 20),
        (3, 'text3', NULL),
        (4, NULL, NULL),
        (5, 'text5', 50)
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Test IS NULL
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_nulls_select WHERE optional_text IS NULL",
        pg_conn,
    )
    assert result[0][0] == 2

    # Test IS NOT NULL
    result = run_query(
        "SELECT COUNT(*) FROM test_ducklake_nulls_select WHERE optional_num IS NOT NULL",
        pg_conn,
    )
    assert result[0][0] == 3

    # Test COALESCE
    result = run_query(
        "SELECT id, COALESCE(optional_text, 'default') FROM test_ducklake_nulls_select WHERE id = 2",
        pg_conn,
    )
    assert result[0] == [2, "default"]

    run_command("DROP FOREIGN TABLE test_ducklake_nulls_select", pg_conn)
    pg_conn.rollback()
