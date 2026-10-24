import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from utils_pytest import *

ROW_ID_FIELD_ID = 2147483540
LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID = 2147483539

# Predicates the rewritten file must still answer exactly. Each would either
# prune the file or fail to cast a neighbour's bounds if the footer stats were
# matched to the wrong column.
PREDICATES = [
    "a = 1",
    "a = 3",
    "b = 'three'",
    "c",
    "d > '2026-01-01'",
    "e = 30",
]


def rewrite_with_row_lineage_in_the_middle(s3, data_file_path):
    """
    Rewrite a data file the way another engine's compaction does: the row
    lineage columns are materialized and _row_id is no longer the last leaf.
    """
    bucket, key = parse_s3_path(data_file_path)
    original = pq.read_table(
        io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    )
    row_count = original.num_rows

    def lineage_field(name, field_id):
        return pa.field(
            name, pa.int64(), metadata={b"PARQUET:field_id": str(field_id).encode()}
        )

    columns = original.columns
    fields = list(original.schema)
    row_id = lineage_field("_row_id", ROW_ID_FIELD_ID)
    sequence_number = lineage_field(
        "_last_updated_sequence_number", LAST_UPDATED_SEQUENCE_NUMBER_FIELD_ID
    )

    # _row_id goes right after the first column; its values (0..n-1) cannot be
    # mistaken for any of the user columns' bounds.
    rewritten = pa.Table.from_arrays(
        columns[:1]
        + [pa.array(range(row_count), pa.int64())]
        + columns[1:]
        + [pa.array([1] * row_count, pa.int64())],
        schema=pa.schema(fields[:1] + [row_id] + fields[1:] + [sequence_number]),
    )

    buffer = io.BytesIO()
    pq.write_table(rewritten, buffer)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())


def test_pruning_external_iceberg_with_row_id_in_the_middle(
    s3, superuser_conn, extension, with_default_location
):
    # The pruning settings are superuser-only.
    pg_conn = superuser_conn
    run_command(
        """
        create schema rowid_middle;
        create table rowid_middle.src (a int, b text, c bool, d timestamptz, e int)
          using iceberg;
        insert into rowid_middle.src values
          (1, 'one',   false, '2026-01-02', 10),
          (2, 'two',   true,  '2026-01-03', 20),
          (3, 'three', true,  '2026-01-04', 30);
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "select metadata_location from iceberg_tables where table_name = 'src' and table_namespace = 'rowid_middle'",
        pg_conn,
    )[0][0]
    data_files = run_query(
        "select path from lake_table.files where table_name = 'rowid_middle.src'::regclass",
        pg_conn,
    )
    assert len(data_files) == 1
    rewrite_with_row_lineage_in_the_middle(s3, data_files[0][0])

    # The write populated the local file cache; read the rewritten object.
    run_query(f"select lake_file_cache.remove('{data_files[0][0]}')", pg_conn)

    run_command(
        f"""
        create foreign table rowid_middle.ext () server pg_lake
          options (path '{metadata_location}');
        """,
        pg_conn,
    )

    for predicate in PREDICATES:
        query = f"select count(*) from rowid_middle.ext where {predicate}"

        run_command("set pg_lake_table.enable_data_file_pruning to off", pg_conn)
        run_command("set pg_lake_table.enable_partition_pruning to off", pg_conn)
        expected = run_query(query, pg_conn)
        assert expected[0][0] > 0, predicate

        run_command("reset pg_lake_table.enable_data_file_pruning", pg_conn)
        run_command("reset pg_lake_table.enable_partition_pruning", pg_conn)
        assert run_query(query, pg_conn) == expected, predicate

    pg_conn.rollback()
    run_command("drop schema rowid_middle cascade", pg_conn)
    pg_conn.commit()
