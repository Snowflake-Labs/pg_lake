import pytest
from utils_pytest import *

url = f"s3://{TEST_BUCKET}/test_spatial_basics/data.parquet"


def test_no_extension(
    user_conn,
    superuser_conn,
    postgis_extension,
    pg_lake_extension,
    basic_geometry_file,
):
    # Drop the existing extension
    run_command(
        f"""
        DROP EXTENSION IF EXISTS pg_lake_spatial;
    """,
        superuser_conn,
    )

    superuser_conn.commit()

    # test creating a pg_lake table with a geometry column without pg_lake_spatial
    error = run_command(
        f"""
        CREATE FOREIGN TABLE test_no_extension (id text, g geometry)
        SERVER pg_lake OPTIONS (path '{url}');
    """,
        user_conn,
        raise_error=False,
    )
    assert "pg_lake_spatial extension is required" in error

    user_conn.rollback()

    # test creating a pg_lake_iceberg table with a geometry column without pg_lake_spatial
    error = run_command(
        f"""
        CREATE FOREIGN TABLE test_no_extension_iceberg (id text, g geometry)
        SERVER pg_lake_iceberg OPTIONS (location 's3://{TEST_BUCKET}/test_spatial_basics/iceberg/');
    """,
        user_conn,
        raise_error=False,
    )
    assert "pg_lake_spatial extension is required" in error

    user_conn.rollback()

    # test creating a table with a bytea column (fine), and then running a spatial query without pg_lake_spatial
    error = run_command(
        f"""
        CREATE FOREIGN TABLE test_no_extension (id text, g bytea)
        SERVER pg_lake OPTIONS (path '{url}');
    """,
        user_conn,
    )

    error = run_command(
        f"""
        SELECT ST_AsText(ST_GeomFromWKB(g)) FROM test_no_extension;
    """,
        user_conn,
        raise_error=False,
    )
    assert "pg_lake_spatial extension is required" in error

    user_conn.rollback()

    # Restore extension
    run_command(
        f"""
        CREATE EXTENSION pg_lake_spatial;
    """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_copy_from(
    user_conn,
    spatial_analytics_extension,
    pg_lake_extension,
    basic_geometry_file,
):
    # Create a regular table and load in data using COPY .. FROM
    run_command(
        f"""
        CREATE TABLE test_copy_from (id text, g geometry);
        COPY test_copy_from FROM '{url}';
    """,
        user_conn,
    )

    result = run_query(
        """
        SELECT ST_AsText(g) geom FROM test_copy_from ORDER BY 1
    """,
        user_conn,
    )
    assert result[0]["geom"] == "POINT(1 1)"

    user_conn.rollback()


def test_wkb(
    user_conn,
    spatial_analytics_extension,
    pg_lake_extension,
    basic_geometry_file,
):

    # Table will have a bytea column with WKB contents
    run_command(
        f"""
        CREATE FOREIGN TABLE test_wkb () SERVER pg_lake OPTIONS (path '{url}');
    """,
        user_conn,
    )

    result = run_query(
        """
        SELECT ST_AsText(ST_GeomFromWKB(g)) geom FROM test_wkb ORDER BY 1
    """,
        user_conn,
    )

    # Warning: DuckDB adds an extra space after POINT, while PostGIS does not
    assert result[0]["geom"] == "POINT (1 1)"

    user_conn.rollback()


def test_geometry(
    user_conn,
    spatial_analytics_extension,
    pg_lake_extension,
    basic_geometry_file,
):

    # Define a table with a geometry column, WKB BLOB will get converted to geometry
    run_command(
        f"""
        CREATE FOREIGN TABLE test_wkb (id text, g geometry) SERVER pg_lake OPTIONS (path '{url}');
    """,
        user_conn,
    )

    result = run_query(
        """
        SELECT ST_AsText(g) geom FROM test_wkb ORDER BY 1
    """,
        user_conn,
    )

    # Warning: DuckDB adds an extra space after POINT, while PostGIS does not
    assert result[0]["geom"] == "POINT (1 1)"

    user_conn.rollback()


def test_iceberg_geometry(user_conn, spatial_analytics_extension, pg_lake_extension):
    # we're going to commit, rollback any existing state
    user_conn.rollback()

    run_command(
        f"""
        CREATE FOREIGN TABLE test_iceberg (id text, g geometry)
        SERVER pg_lake_iceberg OPTIONS (location 's3://{TEST_BUCKET}/test_spatial_basics/iceberg/');
    """,
        user_conn,
    )

    run_command("insert into test_iceberg values ('1', ST_Point(1,2))", user_conn)
    run_command("insert into test_iceberg values ('1', ST_Point(3,4))", user_conn)

    res = run_query("select id, g from test_iceberg", user_conn)
    assert len(res) == 2

    user_conn.commit()

    ## This section of the test evaluates whether we are properly using column
    ## aliases for geometry-based expressions in BuildColumnProjection() (as
    ## demonstrated in #767).

    # switch to mode that we can run vacuum
    user_conn.autocommit = True
    user_conn.notices.clear()

    run_command("VACUUM (ICEBERG, VERBOSE)", user_conn, raise_error=False)
    res = run_query("SELECT id, g FROM test_iceberg", user_conn)

    assert len(res) == 2

    # verify we used a column alias for the data column; if not, we would get a Binder error
    assert ("Binder" not in notice for notice in user_conn.notices)

    user_conn.autocommit = False

    run_command("drop foreign table test_iceberg", user_conn)
    user_conn.commit()


def test_writable_parquet(user_conn, spatial_analytics_extension, pg_lake_extension):
    location = f"s3://{TEST_BUCKET}/test_spatial_basics/test_parquet/"

    # Define a table with a geometry column
    run_command(
        f"""
        CREATE FOREIGN TABLE test_parquet (id text, g geometry)
        SERVER pg_lake
        OPTIONS (location '{location}', format 'parquet', writable 'true');

        INSERT INTO test_parquet VALUES (1, 'POINT (3.5 7.0)');
    """,
        user_conn,
    )

    result = run_query(
        """
        SELECT ST_AsText(g) geom FROM test_parquet ORDER BY 1
    """,
        user_conn,
    )

    # Warning: DuckDB adds an extra space after POINT, while PostGIS does not
    assert result[0]["geom"] == "POINT (3.5 7)"

    run_command(
        f"PREPARE point_lookup_parquet(geometry) AS SELECT * FROM test_parquet WHERE g = $1",
        user_conn,
    )

    for s in range(1, 7):
        results = run_query(
            f"EXECUTE point_lookup_parquet('POINT(3.5 7.0)')", user_conn
        )
        assert results[0]["id"] == "1"

    user_conn.rollback()


def test_writable_csv(user_conn, spatial_analytics_extension, pg_lake_extension):
    location = f"s3://{TEST_BUCKET}/test_spatial_basics/test_csv/"

    # Define a table with a geometry column
    run_command(
        f"""
        CREATE FOREIGN TABLE test_csv (id text, g geometry)
        SERVER pg_lake
        OPTIONS (location '{location}', format 'csv', writable 'true');

        INSERT INTO test_csv VALUES (1, 'POINT (3.5 0.0)');
    """,
        user_conn,
    )

    result = run_query(
        """
        SELECT ST_AsText(g) geom FROM test_csv ORDER BY 1
    """,
        user_conn,
    )

    # Warning: DuckDB adds an extra space after POINT, while PostGIS does not
    assert result[0]["geom"] == "POINT (3.5 0)"

    run_command(
        f"PREPARE point_lookup_csv(geometry) AS SELECT * FROM test_csv WHERE g = $1",
        user_conn,
    )

    for s in range(1, 7):
        results = run_query(f"EXECUTE point_lookup_csv('POINT(3.5 0.0)')", user_conn)
        assert results[0]["id"] == "1"

    user_conn.rollback()


def test_writable_json(user_conn, spatial_analytics_extension, pg_lake_extension):
    location = f"s3://{TEST_BUCKET}/test_spatial_basics/test_json/"

    # Define a table with a geometry column
    run_command(
        f"""
        CREATE FOREIGN TABLE test_json (id text, g geometry)
        SERVER pg_lake
        OPTIONS (location '{location}', format 'json', writable 'true');

        INSERT INTO test_json VALUES (1, 'POINT (7.9 1.1)');
    """,
        user_conn,
    )

    result = run_query(
        """
        SELECT ST_AsText(g) geom FROM test_json ORDER BY 1
    """,
        user_conn,
    )

    # Warning: DuckDB adds an extra space after POINT, while PostGIS does not
    assert result[0]["geom"] == "POINT (7.9 1.1)"

    run_command(
        f"PREPARE point_lookup_json(geometry) AS SELECT * FROM test_json WHERE g = $1",
        user_conn,
    )

    for s in range(1, 7):
        results = run_query(f"EXECUTE point_lookup_json('POINT(7.9 1.1)')", user_conn)
        assert results[0]["id"] == "1"

    user_conn.rollback()


@pytest.fixture(scope="module")
def basic_geometry_file(s3, user_conn, postgis_extension, spatial_analytics_extension):
    run_command(
        f"""
        COPY (SELECT 'key-' || s id, format('point(%s %s)', s, s)::geometry g FROM generate_series(1,10) s) to '{url}';
    """,
        user_conn,
    )
