"""Data file pruning on float columns that hold NaN.

Parquet and Iceberg min/max statistics leave out NaN, while Postgres orders NaN
above every other value, so a file's min and max do not bound a float column
that has one.
"""

import io
import json

import fastavro
import pytest
from utils_pytest import (
    fetch_data_files_used,
    read_s3_operations,
    run_command,
    run_query,
)


@pytest.fixture(autouse=True)
def rollback_after_test(pg_conn):
    yield
    pg_conn.rollback()


def scanned_files(pg_conn, table, where):
    """(data files scanned, rows returned) for a filter, with and without pushdown.

    The rows are checked against a read behind OFFSET 0, which keeps the filter
    out of the scan, so a wrongly pruned file fails here even when the expected
    count in the test is wrong too.
    """
    results = set()
    for enabled in ("on", "off"):
        run_command(
            f"SET LOCAL pg_lake_table.enable_full_query_pushdown={enabled}", pg_conn
        )
        plan = run_query(
            f"EXPLAIN (analyze, verbose, format json) SELECT * FROM {table} WHERE {where}",
            pg_conn,
        )
        count = run_query(f"SELECT count(*) FROM {table} WHERE {where}", pg_conn)[0][0]
        unpruned = run_query(
            f"SELECT count(*) FROM (SELECT * FROM {table} OFFSET 0) t WHERE {where}",
            pg_conn,
        )[0][0]
        assert count == unpruned, f"{where}: {count} rows, {unpruned} unpruned"
        results.add((fetch_data_files_used(plan), count))
    assert len(results) == 1
    return results.pop()


@pytest.mark.parametrize("column_type", ["double precision", "real"])
def test_nan_in_pg_lake_table(pg_conn, extension, with_default_location, column_type):
    """
    A file with a NaN must not be pruned on the min and max of its other
    values. A file without one is still pruned.
    """
    schema = f"nan_pruning_{column_type.split()[0]}"
    run_command(
        f"""
        CREATE SCHEMA {schema};
        CREATE TABLE {schema}.t (id int, f {column_type}) USING iceberg;
        INSERT INTO {schema}.t VALUES (1, 1.0), (2, 'NaN');
        INSERT INTO {schema}.t VALUES (3, 1.0), (4, 2.0);
        """,
        pg_conn,
    )
    pg_conn.commit()
    try:
        # only the file with the NaN can match
        assert scanned_files(pg_conn, f"{schema}.t", "f > 5") == ("1", 1)
        assert scanned_files(pg_conn, f"{schema}.t", "f = 'NaN'") == ("1", 1)
        # neither file can be pruned: [1, 2] holds values other than 1 and 2
        assert scanned_files(pg_conn, f"{schema}.t", "f NOT IN (1, 2)") == ("2", 1)
        # 2 is only in the file without NaN, and the other cannot be pruned
        assert scanned_files(pg_conn, f"{schema}.t", "f = 2") == ("2", 1)
        assert scanned_files(pg_conn, f"{schema}.t", "f < 0") == ("1", 0)
    finally:
        pg_conn.rollback()
        run_command(f"DROP SCHEMA {schema} CASCADE", pg_conn)
        pg_conn.commit()


def test_nan_counts_in_manifest(pg_conn, s3, extension, with_default_location):
    """
    A float column gets bounds only in a file without NaN, and then a NaN count
    of zero, which other readers need to trust Iceberg bounds that leave out
    NaN. A file with a NaN has neither.
    """
    run_command(
        """
        CREATE SCHEMA nan_counts;
        CREATE TABLE nan_counts.t (id int, f double precision, r real) USING iceberg;
        INSERT INTO nan_counts.t VALUES (1, 1.0, 1.0), (2, 'NaN', 2.0);
        INSERT INTO nan_counts.t VALUES (3, 1.0, 'NaN'), (4, 2.0, 3.0);
        """,
        pg_conn,
    )
    pg_conn.commit()
    try:
        metadata_location = run_query(
            "SELECT metadata_location FROM iceberg_tables "
            "WHERE table_namespace = 'nan_counts' AND table_name = 't'",
            pg_conn,
        )[0][0]
        metadata = json.loads(read_s3_operations(s3, metadata_location))
        snapshot = next(
            s
            for s in metadata["snapshots"]
            if s["snapshot-id"] == metadata["current-snapshot-id"]
        )
        manifest_list = read_s3_operations(s3, snapshot["manifest-list"], is_text=False)

        files = []
        for manifest in fastavro.reader(io.BytesIO(manifest_list)):
            body = read_s3_operations(s3, manifest["manifest_path"], is_text=False)
            for entry in fastavro.reader(io.BytesIO(body)):
                data_file = entry["data_file"]
                files.append(
                    (
                        data_file["record_count"],
                        sorted(b["key"] for b in data_file["lower_bounds"]),
                        sorted(
                            (c["key"], c["value"])
                            for c in data_file["nan_value_counts"] or []
                        ),
                    )
                )

        # field ids: id 1, f 2, r 3. Each file has a NaN in one float column,
        # and bounds and a NaN count of zero for the other.
        assert sorted(files, key=lambda f: f[1]) == [
            (2, [1, 2], [(2, 0)]),
            (2, [1, 3], [(3, 0)]),
        ]
    finally:
        pg_conn.rollback()
        run_command("DROP SCHEMA nan_counts CASCADE", pg_conn)
        pg_conn.commit()
