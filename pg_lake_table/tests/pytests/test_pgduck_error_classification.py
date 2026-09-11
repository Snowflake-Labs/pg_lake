"""Coverage for the classified, PII-free LOG line pg_lake_engine emits on pgduck errors."""

import psycopg2
import pytest
from utils_pytest import *


def test_pgduck_engine_error_log_has_class_not_detail(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/test_pgduck_engine_error_log/missing.parquet"
    run_command(
        f"""
        CREATE SCHEMA IF NOT EXISTS test_pgduck_engine_error_log;
        CREATE FOREIGN TABLE test_pgduck_engine_error_log.t (id int)
        SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("SET client_min_messages TO log", pg_conn)
    del pg_conn.notices[:]

    with pytest.raises(psycopg2.Error):
        run_query("SELECT * FROM test_pgduck_engine_error_log.t", pg_conn)

    class_lines = [n for n in pg_conn.notices if "pgduck_engine_error:" in n]
    assert class_lines, f"expected classified LOG in notices, got {pg_conn.notices!r}"
    line = class_lines[0]
    assert (
        "pgduck_engine_error: io_error" in line or "pgduck_engine_error: other" in line
    )
    assert url not in line
    pg_conn.rollback()
