"""String bounds are ordered by bytes, so data file pruning must not compare
them in a linguistic collation."""

import pytest
from utils_pytest import *

DATABASE = "string_pruning_icu"


def test_linguistic_default_collation(icu_database):
    conn = icu_database

    # the collation disagrees with byte order, which puts 'B' first
    assert run_query("SELECT 'a' < 'B'", conn) == [[True]]

    run_command(
        f"""CREATE TABLE t (v text) USING iceberg
            WITH (location='s3://{TEST_BUCKET}/{DATABASE}/')""",
        conn,
    )
    run_command("INSERT INTO t VALUES ('B'), ('a')", conn)
    conn.commit()

    assert run_query("SELECT v FROM t WHERE v = 'a'", conn) == [["a"]]
    assert run_query("SELECT v FROM t WHERE v = 'B'", conn) == [["B"]]
    conn.rollback()


def test_equality_prunes_under_linguistic_default_collation(icu_database):
    """Equality constraints compare exact values, so identity partitions
    still prune when the bounds cannot be ordered."""
    conn = icu_database

    run_command(
        f"""CREATE TABLE p (v text) USING iceberg
            WITH (location='s3://{TEST_BUCKET}/{DATABASE}/p/', partition_by='v')""",
        conn,
    )
    run_command("INSERT INTO p VALUES ('B'), ('a'), ('c')", conn)
    conn.commit()

    for value in ["a", "B", "c"]:
        clause = f"v = '{value}'"
        assert run_query(f"SELECT v FROM p WHERE {clause}", conn) == [[value]]
        explain = run_query(
            f"EXPLAIN (verbose, format json) SELECT v FROM p WHERE {clause}", conn
        )
        assert int(fetch_data_files_used(explain)) == 1
    conn.rollback()


def test_bucket_under_nondeterministic_collation(
    pg_conn, s3, extension, with_default_location
):
    """A bucket holds a hash of the value's bytes, so a comparison that
    ignores case matches rows in other buckets."""
    error = run_command(
        "CREATE COLLATION string_pruning_ci "
        "(provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        pg_conn,
        raise_error=False,
    )
    if error:
        pg_conn.rollback()
        pytest.skip(f"needs ICU: {error}")

    run_command(
        "CREATE TABLE string_pruning_bucket (id int, v text) USING iceberg "
        "WITH (partition_by = 'bucket(8, v)')",
        pg_conn,
    )
    run_command("INSERT INTO string_pruning_bucket VALUES (1, 'a'), (2, 'A')", pg_conn)

    assert run_query(
        "SELECT id FROM string_pruning_bucket "
        "WHERE v COLLATE string_pruning_ci = 'a' ORDER BY id",
        pg_conn,
    ) == [[1], [2]]
    pg_conn.rollback()


@pytest.fixture
def icu_database(installcheck, superuser_conn, s3, extension):
    if installcheck:
        pytest.skip("creates a database")

    superuser_conn.autocommit = True
    error = run_command(
        f"CREATE DATABASE {DATABASE} TEMPLATE template0 "
        "LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LOCALE 'C'",
        superuser_conn,
        raise_error=False,
    )
    superuser_conn.autocommit = False
    if error:
        pytest.skip(f"needs ICU: {error}")

    conn = open_pg_conn_to_db(DATABASE)
    run_command("CREATE EXTENSION pg_lake_table CASCADE", conn)
    conn.commit()

    yield conn

    conn.close()
    superuser_conn.autocommit = True
    run_command(f"DROP DATABASE {DATABASE} WITH (FORCE)", superuser_conn)
    superuser_conn.autocommit = False
