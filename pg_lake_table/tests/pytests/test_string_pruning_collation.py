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
