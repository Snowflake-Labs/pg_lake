from utils_pytest import *


def test_parquet_column_chunk_file_offset_multi_row_group(pgduck_conn, tmp_path):
    """ColumnChunk.file_offset must point at the start of each column chunk.

    file_offset is a required Parquet field but is otherwise left at its
    default of 0. Readers that use it to locate a column chunk then misread
    files with more than one row group, because every row group resolves to
    offset 0 and the first row group is returned repeatedly. Write a file with
    several row groups and assert every column chunk's file_offset matches its
    start offset (the dictionary page if present, else the first data page).
    """
    path = str(tmp_path / "multi_rg.parquet")

    # range(8192) with ROW_GROUP_SIZE 2048 => 4 row groups, 2 columns each.
    copy = f"""
        COPY (
            SELECT i AS id, repeat('x', 16) AS payload
            FROM range(8192) t(i)
        ) TO '{path}' (FORMAT PARQUET, ROW_GROUP_SIZE 2048)
    """
    run_command(copy, pgduck_conn)

    rows = run_query(
        f"""
        SELECT row_group_id,
               column_id,
               file_offset,
               coalesce(dictionary_page_offset, data_page_offset) AS chunk_start
        FROM parquet_metadata('{path}')
        ORDER BY row_group_id, column_id
        """,
        pgduck_conn,
    )

    assert len(rows) > 0
    # The test is only meaningful with more than one row group.
    assert max(r["row_group_id"] for r in rows) >= 1

    for r in rows:
        # file_offset must equal the start of the column chunk, and therefore
        # be non-zero (offset 0 is the file's "PAR1" magic, never a chunk).
        assert r["file_offset"] == r["chunk_start"], r
        assert r["file_offset"] > 0, r
