"""Direct trigger for duckdb_pglake/patches/duckdb/parquet-virtual-column-stats.patch.

The patch is new in PR 300 and the PR ships no test for it. It replaces two
D_ASSERT calls in ParquetPartitionRowGroup with runtime guards for the case where a
StorageIndex points past the file's real columns, which is what a virtual
column (_filename) does. D_ASSERT compiles away in a release build, so without
the guard this is an out-of-bounds read of root_schema->children, not an
assertion failure -- it can return junk stats or crash rather than fail loudly.

Filtering on the virtual column is what makes the optimizer ask for its
statistics, so every query here puts _filename in a WHERE clause. Row groups
are only pruned when there is more than one, hence the row counts.
"""

import pytest

from utils_pytest import *

ROWS = 300000  # > 2 default row groups (122880), so stats are actually consulted


def test_filter_on_virtual_filename_column(s3, pg_conn, extension):
    base = f"s3://{TEST_BUCKET}/pr300_vcol/"
    urls = [f"{base}part{i}.parquet" for i in range(3)]

    for i, url in enumerate(urls):
        run_command(
            f"""
            COPY (SELECT s AS id, (s % 97) AS grp
                  FROM generate_series({i * ROWS + 1}, {(i + 1) * ROWS}) s)
            TO '{url}';
            """,
            pg_conn,
        )

    run_command(
        f"""
        CREATE FOREIGN TABLE pr300_vcol () SERVER pg_lake
            OPTIONS (path '{base}*.parquet', filename 'true');
        """,
        pg_conn,
    )

    # equality on the virtual column: one file's worth of rows
    res = run_query(
        f"SELECT count(*) AS c FROM pr300_vcol WHERE _filename = '{urls[1]}'", pg_conn
    )
    assert res[0]["c"] == ROWS

    # a value no file has: must prune to nothing rather than read junk stats
    res = run_query(
        f"SELECT count(*) AS c FROM pr300_vcol WHERE _filename = '{base}nope.parquet'",
        pg_conn,
    )
    assert res[0]["c"] == 0

    # inequality + IN, still on the virtual column
    res = run_query(
        f"SELECT count(*) AS c FROM pr300_vcol WHERE _filename <> '{urls[0]}'", pg_conn
    )
    assert res[0]["c"] == 2 * ROWS

    res = run_query(
        f"SELECT count(*) AS c FROM pr300_vcol "
        f"WHERE _filename IN ('{urls[0]}', '{urls[2]}')",
        pg_conn,
    )
    assert res[0]["c"] == 2 * ROWS

    # virtual column combined with a real-column predicate, which is where a
    # bogus stats object for the virtual index would corrupt pruning
    res = run_query(
        f"SELECT count(*) AS c FROM pr300_vcol "
        f"WHERE _filename = '{urls[2]}' AND grp = 5",
        pg_conn,
    )
    expected = run_query(
        f"SELECT count(*) AS c FROM pr300_vcol " f"WHERE id > {2 * ROWS} AND grp = 5",
        pg_conn,
    )
    assert res[0]["c"] == expected[0]["c"] > 0

    # min/max over a real column under a virtual-column filter: exercises
    # MinMaxIsExact, the second guard in the patch
    res = run_query(
        f"SELECT min(id) AS lo, max(id) AS hi FROM pr300_vcol "
        f"WHERE _filename = '{urls[1]}'",
        pg_conn,
    )
    assert res[0]["lo"] == ROWS + 1
    assert res[0]["hi"] == 2 * ROWS

    run_command("DROP FOREIGN TABLE pr300_vcol", pg_conn)
    pg_conn.commit()
