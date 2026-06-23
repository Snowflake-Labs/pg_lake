"""
CREATE TABLE ... USING ducklake AS SELECT/TABLE/VALUES tests.

Mirrors the corresponding pg_lake_iceberg path: the ProcessUtility hook
rewrites the CTAS into CREATE FOREIGN TABLE + INSERT INTO ... SELECT, so
the resulting table is a regular DuckLake foreign table backed by parquet
files in the configured location.
"""

import pytest


def test_ctas_select_basic(pg_cursor, s3):
    pg_cursor.execute(
        """
        CREATE TABLE ctas_basic
        USING ducklake WITH (location = 's3://testbucketcdw/ctas_basic')
        AS SELECT i, 'row-' || i::text AS label
        FROM generate_series(1, 5) AS i
        """
    )

    pg_cursor.execute("SELECT count(*), min(i), max(i) FROM ctas_basic")
    row = pg_cursor.fetchone()
    assert row == (5, 1, 5)

    pg_cursor.execute("SELECT i, label FROM ctas_basic ORDER BY i")
    assert pg_cursor.fetchall() == [
        (1, "row-1"),
        (2, "row-2"),
        (3, "row-3"),
        (4, "row-4"),
        (5, "row-5"),
    ]

    # writes registered a data_file
    pg_cursor.execute(
        "SELECT count(*) FROM lake_ducklake.data_file df "
        "JOIN lake_ducklake.table t USING (table_id) "
        "WHERE t.table_name = 'ctas_basic' AND df.end_snapshot IS NULL"
    )
    assert pg_cursor.fetchone()[0] >= 1


def test_ctas_with_no_data(pg_cursor, s3):
    pg_cursor.execute(
        """
        CREATE TABLE ctas_no_data
        USING ducklake WITH (location = 's3://testbucketcdw/ctas_no_data')
        AS SELECT i FROM generate_series(1, 3) AS i
        WITH NO DATA
        """
    )

    pg_cursor.execute("SELECT count(*) FROM ctas_no_data")
    assert pg_cursor.fetchone()[0] == 0


def test_ctas_with_values(pg_cursor, s3):
    pg_cursor.execute(
        """
        CREATE TABLE ctas_values
        USING ducklake WITH (location = 's3://testbucketcdw/ctas_values')
        AS VALUES (1, 'a'), (2, 'b'), (3, 'c')
        """
    )

    pg_cursor.execute("SELECT * FROM ctas_values ORDER BY column1")
    assert pg_cursor.fetchall() == [(1, "a"), (2, "b"), (3, "c")]


def test_ctas_with_table(pg_cursor, s3):
    pg_cursor.execute("CREATE TABLE ctas_src (k int, v text)")
    pg_cursor.execute("INSERT INTO ctas_src VALUES (1, 'x'), (2, 'y')")

    pg_cursor.execute(
        """
        CREATE TABLE ctas_from_table
        USING ducklake WITH (location = 's3://testbucketcdw/ctas_from_table')
        AS TABLE ctas_src
        """
    )

    pg_cursor.execute("SELECT k, v FROM ctas_from_table ORDER BY k")
    assert pg_cursor.fetchall() == [(1, "x"), (2, "y")]


def test_ctas_default_location_prefix(pg_cursor, s3):
    pg_cursor.execute(
        "SET pg_lake_ducklake.default_location_prefix = 's3://testbucketcdw/ctas_prefix'"
    )

    pg_cursor.execute(
        """
        CREATE TABLE ctas_prefixed USING ducklake AS
        SELECT i FROM generate_series(1, 2) AS i
        """
    )

    pg_cursor.execute(
        "SELECT path FROM lake_ducklake.table WHERE table_name = 'ctas_prefixed' AND end_snapshot IS NULL"
    )
    path = pg_cursor.fetchone()[0]
    assert "ctas_prefixed" in path

    pg_cursor.execute("SELECT count(*) FROM ctas_prefixed")
    assert pg_cursor.fetchone()[0] == 2


def test_ctas_if_not_exists_skips_existing(pg_cursor, s3):
    pg_cursor.execute(
        """
        CREATE TABLE ctas_if_not_exists USING ducklake
        WITH (location = 's3://testbucketcdw/ctas_if_not_exists')
        AS SELECT 1 AS x
        """
    )

    # Second CTAS with IF NOT EXISTS should be a no-op
    pg_cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS ctas_if_not_exists USING ducklake
        WITH (location = 's3://testbucketcdw/ctas_if_not_exists')
        AS SELECT 999 AS x
        """
    )

    pg_cursor.execute("SELECT x FROM ctas_if_not_exists")
    assert pg_cursor.fetchone() == (1,)


def test_ctas_temp_rejected(pg_cursor, s3):
    with pytest.raises(Exception, match="temporary tables are not allowed"):
        pg_cursor.execute(
            """
            CREATE TEMP TABLE ctas_temp USING ducklake
            WITH (location = 's3://testbucketcdw/ctas_temp')
            AS SELECT 1
            """
        )
    pg_cursor.connection.rollback()


def test_ctas_unlogged_rejected(pg_cursor, s3):
    with pytest.raises(Exception, match="unlogged tables are not allowed"):
        pg_cursor.execute(
            """
            CREATE UNLOGGED TABLE ctas_unlogged USING ducklake
            WITH (location = 's3://testbucketcdw/ctas_unlogged')
            AS SELECT 1
            """
        )
    pg_cursor.connection.rollback()
