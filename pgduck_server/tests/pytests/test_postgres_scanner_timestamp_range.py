"""
Verify postgres_scan rejects PostgreSQL timestamps outside DuckDB's range.

PostgreSQL accepts a slightly wider timestamp range than DuckDB. Without an
explicit bounds check, postgres_scan wraps values near PostgreSQL's upper bound
to an unrelated BC timestamp and can persist the corrupted value to Parquet.
"""

import psycopg2
import pytest
from utils_pytest import *


def _connstr():
    return (
        f"host={server_params.PG_HOST} "
        f"port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} "
        f"user={server_params.PG_USER} "
        f"password={server_params.PG_PASSWORD}"
    )


def _scan(table="scanner_timestamp_range_tbl"):
    return f"postgres_scan('{_connstr()}', 'public', '{table}')"


@pytest.fixture(scope="module")
def pg_timestamp_range_table(postgres):
    """Create the issue reproduction and supported controls in PostgreSQL."""
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS scanner_timestamp_range_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_timestamp_control_tbl")
    cur.execute(
        "CREATE TABLE scanner_timestamp_range_tbl ("
        "  id integer PRIMARY KEY,"
        "  ts timestamp,"
        "  tstz timestamptz"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_timestamp_range_tbl VALUES "
        "(1, '294276-12-31 23:59:59', '294276-12-31 23:59:59+00')"
    )
    # This vendored scanner applies the outer filter after decoding the source
    # batch, so keep valid controls separate from the deliberately invalid row.
    cur.execute(
        "CREATE TABLE scanner_timestamp_control_tbl ("
        "  id integer PRIMARY KEY,"
        "  ts timestamp,"
        "  tstz timestamptz"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_timestamp_control_tbl VALUES "
        "(2, '10000-01-01 00:00:00',  '10000-01-01 00:00:00+00'),"
        "(3, '2026-01-15 12:00:00',   '2026-01-15 12:00:00+00'),"
        "(4, '4713-01-01 BC',         '4713-01-01 BC')"
    )
    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS scanner_timestamp_range_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_timestamp_control_tbl")
    cur.close()
    conn.close()


@pytest.fixture
def timestamp_scanner_conn(pgduck_server):
    conn = psycopg2.connect(
        host=server_params.PGDUCK_UNIX_DOMAIN_PATH,
        port=server_params.PGDUCK_PORT,
    )
    conn.autocommit = True
    yield conn
    conn.close()


def test_postgres_scan_supported_timestamp_controls(
    pg_timestamp_range_table, timestamp_scanner_conn
):
    rows = run_query(
        f"""
        SELECT id, CAST(ts AS VARCHAR), CAST(tstz AS VARCHAR)
        FROM {_scan("scanner_timestamp_control_tbl")}
        ORDER BY id
        """,
        timestamp_scanner_conn,
    )
    assert [tuple(row) for row in rows] == [
        (2, "10000-01-01 00:00:00", "10000-01-01 00:00:00+00"),
        (3, "2026-01-15 12:00:00", "2026-01-15 12:00:00+00"),
        (4, "4713-01-01 (BC) 00:00:00", "4713-01-01 (BC) 00:00:00+00"),
    ]


@pytest.mark.parametrize("column", ["ts", "tstz"])
def test_postgres_scan_rejects_out_of_range_timestamp(
    pg_timestamp_range_table, timestamp_scanner_conn, column
):
    with pytest.raises(psycopg2.Error, match="timestamp out of range"):
        run_query(
            f"SELECT {column} FROM {_scan()} WHERE id = 1",
            timestamp_scanner_conn,
        )


def test_postgres_scan_copy_rejects_out_of_range_timestamp(
    pg_timestamp_range_table, timestamp_scanner_conn, tmp_path
):
    output_path = str(tmp_path / "timestamp_range.parquet").replace("'", "''")
    with pytest.raises(psycopg2.Error, match="timestamp out of range"):
        run_command(
            f"""
            COPY (
                SELECT ts, tstz
                FROM {_scan()}
                WHERE id = 1
            ) TO '{output_path}' (FORMAT PARQUET)
            """,
            timestamp_scanner_conn,
        )


def test_postgres_binary_writer_rejects_out_of_range_timestamp(
    timestamp_scanner_conn, tmp_path
):
    output_path = str(tmp_path / "timestamp_range.bin").replace("'", "''")
    with pytest.raises(psycopg2.Error, match="out of range for Postgres"):
        run_command(
            f"""
            COPY (
                SELECT TIMESTAMP '290309-12-22 (BC)'
            ) TO '{output_path}' (FORMAT postgres_binary)
            """,
            timestamp_scanner_conn,
        )
