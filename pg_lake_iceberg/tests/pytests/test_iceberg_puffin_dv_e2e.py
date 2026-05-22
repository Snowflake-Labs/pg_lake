"""
End-to-end test for the Iceberg Puffin/DV primitives exposed by the patch in
``duckdb_pglake/patches/duckdb-iceberg/expose_puffin_dv_functions.patch``.

These tests deliberately do NOT touch ``pg_lake_iceberg``'s manifest write
path or scan-rewrite. They issue, directly through ``pgduck_conn``, the
exact SQL shapes the future ``pg_lake_iceberg`` V3 integration will emit:

* The future writer in ``pg_lake_table/src/fdw/position_delete_dest.c`` will
  call ``iceberg_write_dvs(...)`` to materialize a deletion vector and
  return ``(referenced_data_file, content_offset, content_size_in_bytes,
  cardinality, file_size)`` straight into ``DataFile`` fields the manifest
  writer in ``pg_lake_iceberg/src/iceberg/write_table_metadata.c`` persists
  into manifest entry fields 143/144/145/103/104.

* The future reader in ``pg_lake_engine/src/pgduck/read_data.c`` will
  substitute a single ``iceberg_dv_positions(paths[], offsets[], sizes[],
  referenced_data_files[]) -> (file_path, pos)`` call for the current
  ``read_parquet(<positional-delete-files>)`` in the anti-join. One TVF
  invocation covers the whole scan: a query that touches N data files
  (each with its own DV) does NOT need an N-leg UNION ALL. The TVF
  itself stamps every output row's ``file_path`` from the matching
  element of the ``referenced_data_files`` array.

The test transits the (path, offset, size, ref) 4-tuples through Python
locals to emulate the C-level ``DataFile[]`` array hop the integration
will take in memory; the manifest leg is intentionally not exercised
here.
"""

import pytest
from utils_pytest import *


@pytest.fixture(scope="module", autouse=True)
def _load_iceberg(pgduck_conn):
    run_command("LOAD iceberg;", pgduck_conn)


def _path(name):
    return f"s3://{TEST_BUCKET}/test_iceberg_puffin_dv_e2e/{name}"


def _write_data_parquet(pgduck_conn, data_path, n_rows):
    """Produce a parquet data file with ``id`` and ``val`` columns; row N
    has ``id=N`` so positional deletes line up 1:1 with ``id``."""
    # In DuckDB, ``generate_series(1, N) i`` is a table-returning function:
    # the column inside ``i`` is literally named ``generate_series``. We
    # qualify it explicitly so we get scalar BIGINT/VARCHAR columns on
    # disk instead of structs that would confuse the downstream
    # ``read_parquet`` consumers.
    run_command(
        f"""
        COPY (
            SELECT i.generate_series AS id,
                   'row-' || i.generate_series AS val
            FROM generate_series(1, {n_rows}) i
        ) TO '{data_path}'
        """,
        pgduck_conn,
    )


def _write_dv(pgduck_conn, dv_path, data_path, deleted_positions):
    """Write a single-DV Puffin file pointed at ``data_path`` and return the
    (content_offset, content_size_in_bytes) tuple captured at write time
    (the same way the future TableMetadataOperation will capture it)."""
    arr_sql = "ARRAY[" + ",".join(f"{p}::BIGINT" for p in deleted_positions) + "]"
    row = run_query(
        f"""
        SELECT content_offset, content_size_in_bytes, cardinality
        FROM iceberg_write_dvs('{dv_path}', '{data_path}', {arr_sql})
        """,
        pgduck_conn,
    )
    assert len(row) == 1
    return int(row[0][0]), int(row[0][1]), int(row[0][2])


def _anti_join(pgduck_conn, data_path, dv_path, content_offset, content_size, ref):
    """Run the exact anti-join shape that the future V3 scan-rewrite in
    ``pg_lake_engine/src/pgduck/read_data.c`` will emit (single data file).
    Returns the live rows (not deleted) ordered by id."""
    rows = run_query(
        f"""
        SELECT id
        FROM read_parquet('{data_path}', filename=true, file_row_number=true) res
        WHERE (filename, file_row_number) NOT IN (
            SELECT file_path, pos FROM iceberg_dv_positions(
                ['{dv_path}'],
                [{content_offset}::BIGINT],
                [{content_size}::BIGINT],
                ['{ref}']
            )
        )
        ORDER BY id
        """,
        pgduck_conn,
    )
    return [int(r[0]) for r in rows]


# ----------------------------------------------------------------------------
# Scenarios
# ----------------------------------------------------------------------------


def test_e2e_delete_none(pgduck_conn, s3):
    """Cardinality 0 DV is a degenerate case: the anti-join must return all
    rows. We cannot encode an empty DV via ``iceberg_dv_encode`` (the spec
    requires at least one bitmap), so we exercise the equivalent shape by
    pointing the DV at a single bogus position outside the data file's
    range and asserting no real rows are filtered out."""
    data_path = _path("none/data.parquet")
    dv_path = _path("none/deletes.puffin")
    _write_data_parquet(pgduck_conn, data_path, 100)

    # Use a sentinel position past the end of the file. Since DV positions
    # are interpreted as file_row_number (0-indexed), 999 has no match in a
    # 100-row file; all 100 rows survive the anti-join.
    co, cs, _ = _write_dv(pgduck_conn, dv_path, data_path, [999])
    live = _anti_join(pgduck_conn, data_path, dv_path, co, cs, data_path)
    assert live == list(range(1, 101))


def test_e2e_delete_single(pgduck_conn, s3):
    """Delete row 7 only: 99 rows survive. ``file_row_number`` is 0-indexed,
    so 'delete row 7' on a 1..100 ``id`` column means dropping the row
    with file_row_number=6 -> id=7."""
    data_path = _path("single/data.parquet")
    dv_path = _path("single/deletes.puffin")
    _write_data_parquet(pgduck_conn, data_path, 100)

    co, cs, _ = _write_dv(pgduck_conn, dv_path, data_path, [6])
    live = _anti_join(pgduck_conn, data_path, dv_path, co, cs, data_path)
    assert live == [i for i in range(1, 101) if i != 7]


def test_e2e_delete_many_sparse(pgduck_conn, s3):
    """Sparse delete set: 100 positions spread across 1M rows. Verify
    exact set difference and that the DV blob stays tiny (Roaring should
    pack the run-length-encoded bitmap into well under 10 KB)."""
    data_path = _path("many/data.parquet")
    dv_path = _path("many/deletes.puffin")
    n_rows = 1_000_000
    _write_data_parquet(pgduck_conn, data_path, n_rows)

    # Delete every 10_000th row (positions 0, 10_000, ..., 990_000)
    deleted = list(range(0, n_rows, 10_000))
    co, cs, card = _write_dv(pgduck_conn, dv_path, data_path, deleted)
    assert card == len(deleted)
    # 100 sparse positions in a Roaring bitmap is essentially a single
    # 200-byte container per high-32 block, so the blob payload should be tiny.
    assert cs < 10_000

    live_count = run_query(
        f"""
        SELECT count(*)
        FROM read_parquet('{data_path}', filename=true, file_row_number=true) res
        WHERE (filename, file_row_number) NOT IN (
            SELECT file_path, pos FROM iceberg_dv_positions(
                ['{dv_path}'], [{co}::BIGINT], [{cs}::BIGINT], ['{data_path}']
            )
        )
        """,
        pgduck_conn,
    )
    assert int(live_count[0][0]) == n_rows - len(deleted)


def test_e2e_delete_all(pgduck_conn, s3):
    """Delete every row in a small data file."""
    data_path = _path("all/data.parquet")
    dv_path = _path("all/deletes.puffin")
    _write_data_parquet(pgduck_conn, data_path, 50)
    deleted = list(range(0, 50))

    co, cs, _ = _write_dv(pgduck_conn, dv_path, data_path, deleted)
    live = _anti_join(pgduck_conn, data_path, dv_path, co, cs, data_path)
    assert live == []


def test_e2e_footer_matches_write_dvs_output(pgduck_conn, s3):
    """Spec invariant from the Iceberg V3 ``Data File Fields`` table:
    ``content_offset`` and ``content_size_in_bytes`` returned by the
    writer MUST equal the matching ``offset`` and ``length`` in the
    Puffin footer. This catches the most common Puffin-writer bug class
    (manifest offsets that disagree with the on-disk footer)."""
    data_path = _path("footer_invariant/data.parquet")
    dv_path = _path("footer_invariant/deletes.puffin")
    _write_data_parquet(pgduck_conn, data_path, 10)

    co, cs, _ = _write_dv(pgduck_conn, dv_path, data_path, [0, 1, 2])
    footer = run_query(
        f"""
        SELECT blob_offset, blob_length, blob_type
        FROM iceberg_puffin_read_metadata('{dv_path}')
        """,
        pgduck_conn,
    )
    assert len(footer) == 1, "iceberg_write_dvs must produce exactly one blob"
    foffset, flength, ftype = footer[0]
    assert int(foffset) == co
    assert int(flength) == cs
    assert ftype == "deletion-vector-v1"


def test_e2e_two_data_files_two_dvs(pgduck_conn, s3):
    """Multi-data-file scan shape: two parquet files, two Puffin files (one
    per data file) each containing a single DV blob, anti-join via a SINGLE
    ``iceberg_dv_positions`` call with parallel arrays. This mirrors what
    the future multi-file scan in ``pg_lake_engine/src/pgduck/read_data.c``
    will emit - no UNION ALL, one TVF invocation regardless of N."""
    data_a = _path("twofile/data-A.parquet")
    data_b = _path("twofile/data-B.parquet")
    dv_a = _path("twofile/deletes-A.puffin")
    dv_b = _path("twofile/deletes-B.puffin")

    _write_data_parquet(pgduck_conn, data_a, 50)
    _write_data_parquet(pgduck_conn, data_b, 50)
    co_a, cs_a, _ = _write_dv(pgduck_conn, dv_a, data_a, [4, 9])  # drop id 5, 10 of A
    co_b, cs_b, _ = _write_dv(pgduck_conn, dv_b, data_b, [0, 1])  # drop id 1, 2 of B

    rows = run_query(
        f"""
        SELECT filename, id
        FROM read_parquet(['{data_a}', '{data_b}'], filename=true, file_row_number=true) res
        WHERE (filename, file_row_number) NOT IN (
            SELECT file_path, pos FROM iceberg_dv_positions(
                ['{dv_a}',  '{dv_b}'],
                [{co_a}::BIGINT, {co_b}::BIGINT],
                [{cs_a}::BIGINT, {cs_b}::BIGINT],
                ['{data_a}', '{data_b}']
            )
        )
        ORDER BY filename, id
        """,
        pgduck_conn,
    )
    by_file = {}
    for r in rows:
        by_file.setdefault(r[0], []).append(int(r[1]))
    assert sorted(by_file[data_a]) == [i for i in range(1, 51) if i not in (5, 10)]
    assert sorted(by_file[data_b]) == [i for i in range(1, 51) if i not in (1, 2)]


def test_e2e_many_data_files_one_tvf_call(pgduck_conn, s3):
    """Stress the array-form API: a 50-data-file scan whose anti-join is a
    single iceberg_dv_positions call with 50-element parallel arrays. The
    SQL string size grows linearly in N (as it would for a UNION ALL too),
    but the plan stays a single-leg TVF instead of a 50-leg union, which
    is the whole point of the array signature."""
    n_files = 50
    rows_per_file = 20
    data_paths = [_path(f"many/data-{i:02d}.parquet") for i in range(n_files)]
    dv_paths = [_path(f"many/deletes-{i:02d}.puffin") for i in range(n_files)]
    cos, css = [], []
    for i in range(n_files):
        _write_data_parquet(pgduck_conn, data_paths[i], rows_per_file)
        # Delete the file_row_number-0 of each file (i.e. id=1).
        co, cs, _ = _write_dv(pgduck_conn, dv_paths[i], data_paths[i], [0])
        cos.append(co)
        css.append(cs)

    paths_sql = "[" + ",".join(f"'{p}'" for p in dv_paths) + "]"
    offs_sql = "[" + ",".join(f"{c}::BIGINT" for c in cos) + "]"
    sizes_sql = "[" + ",".join(f"{c}::BIGINT" for c in css) + "]"
    refs_sql = "[" + ",".join(f"'{p}'" for p in data_paths) + "]"
    parquet_list = "[" + ",".join(f"'{p}'" for p in data_paths) + "]"

    live_count = run_query(
        f"""
        SELECT count(*)
        FROM read_parquet({parquet_list}, filename=true, file_row_number=true) res
        WHERE (filename, file_row_number) NOT IN (
            SELECT file_path, pos FROM iceberg_dv_positions(
                {paths_sql}, {offs_sql}, {sizes_sql}, {refs_sql}
            )
        )
        """,
        pgduck_conn,
    )
    # n_files data files, rows_per_file rows each, exactly 1 deleted row each.
    assert int(live_count[0][0]) == n_files * (rows_per_file - 1)


def test_e2e_supersede_dv(pgduck_conn, s3):
    """Writing a new DV for the same data file overwrites the old (the
    future integration will mark the old delete-file entry as DELETED in
    the manifest; for this stand-alone shape we just ignore DV1)."""
    data_path = _path("supersede/data.parquet")
    dv1 = _path("supersede/deletes-v1.puffin")
    dv2 = _path("supersede/deletes-v2.puffin")
    _write_data_parquet(pgduck_conn, data_path, 20)

    _write_dv(pgduck_conn, dv1, data_path, [0, 1, 2])  # initial deletes
    co2, cs2, _ = _write_dv(
        pgduck_conn, dv2, data_path, [0, 1, 2, 3, 4]
    )  # superseding DV

    live = _anti_join(pgduck_conn, data_path, dv2, co2, cs2, data_path)
    # Only DV2 applies -> rows 1..5 dropped, 6..20 survive.
    assert live == list(range(6, 21))
