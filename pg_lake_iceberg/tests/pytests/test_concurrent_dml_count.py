"""
Concurrent UPDATE + INSERT + DELETE row-count test for iceberg tables.

Exists to determine whether the count-divergence we see for DuckLake
(see pg_lake_ducklake/tests/pytests/test_concurrency_pg_vs_duckdb.py::
test_pg_concurrent_dml_no_orphan_files) is shared by Iceberg or
DuckLake-specific.

The shared write path is in pg_lake_table/src/fdw/writable_table.c and
data_files_catalog.c, so if Iceberg also fails we know the bug is in
shared code.
"""

import threading

import psycopg2
import pytest

from utils_pytest import *


def _new_pg_conn():
    return psycopg2.connect(
        host=server_params.PG_HOST,
        port=server_params.PG_PORT,
        dbname=server_params.PG_DATABASE,
        user=server_params.PG_USER,
    )


def test_iceberg_concurrent_dml_no_orphan_files(
    pg_conn, s3, extension, with_default_location
):
    """
    Same scenario as the DuckLake test, against an Iceberg table:
    50 rows preloaded; concurrent UPDATE first-25, DELETE 26-35,
    INSERT 20 new rows. Expected final count = 50 + 20 - 10 = 60.
    """
    cur = pg_conn.cursor()
    cur.execute("DROP TABLE IF EXISTS iceberg_conc_dml")
    cur.execute(
        f"""
        CREATE TABLE iceberg_conc_dml (id INT, value INT)
        USING iceberg WITH (location = 's3://{TEST_BUCKET}/iceberg_conc_dml')
        """
    )
    cur.execute(
        "INSERT INTO iceberg_conc_dml SELECT i, i FROM generate_series(1, 50) i"
    )
    pg_conn.commit()

    errors = []

    def inserter():
        try:
            conn = _new_pg_conn()
            c = conn.cursor()
            c.execute(
                "INSERT INTO iceberg_conc_dml "
                "SELECT i, i FROM generate_series(100, 119) i"
            )
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append(("insert", e))

    def updater():
        try:
            conn = _new_pg_conn()
            c = conn.cursor()
            c.execute("UPDATE iceberg_conc_dml SET value = value + 1000 WHERE id <= 25")
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append(("update", e))

    def deleter():
        try:
            conn = _new_pg_conn()
            c = conn.cursor()
            c.execute("DELETE FROM iceberg_conc_dml WHERE id BETWEEN 26 AND 35")
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append(("delete", e))

    threads = [threading.Thread(target=fn) for fn in (inserter, updater, deleter)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"thread errors: {errors}"

    cur.execute("SELECT count(*) FROM iceberg_conc_dml")
    final_count = cur.fetchone()[0]
    assert final_count == 60, f"expected 60 rows, got {final_count}"

    cur.execute("SELECT count(*) FROM iceberg_conc_dml WHERE value > 1000 AND id <= 25")
    updated = cur.fetchone()[0]
    assert updated == 25, f"expected all 25 rows updated, got {updated}"

    cur.execute("DROP TABLE iceberg_conc_dml")
    pg_conn.commit()
