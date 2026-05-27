"""
Tests for DuckLake partitioning interop.

Covers:
- PG-side CREATE TABLE with partition_by writes partition_info + partition_column
- PG-side INSERT routes rows by partition value, sets data_file.partition_id,
  and writes file_partition_value rows
- DuckDB-CREATE-PARTITIONED-TABLE through the public.ducklake_partition_info
  view (i.e. that the INSTEAD OF INSERT trigger uses real columns)
"""

import pytest
from utils_pytest import TEST_BUCKET, server_params

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


def test_partition_info_view_trigger_columns(pg_cursor):
    """
    The INSTEAD OF INSERT trigger on public.ducklake_partition_info must
    write the actual columns of lake_ducklake.partition_info. A regression
    that references a non-existent column (e.g. schema_version) breaks
    DuckDB-side CREATE TABLE ... PARTITION BY at the moment DuckDB INSERTs
    via the public view.
    """
    pg_cursor.execute(
        """
        INSERT INTO lake_ducklake.schema
        (schema_id, schema_uuid, begin_snapshot, schema_name)
        VALUES (501, gen_random_uuid(), 0, 's_part_trigger');
        INSERT INTO lake_ducklake.table
        (table_id, table_uuid, begin_snapshot, schema_id, table_name, path)
        VALUES (501, gen_random_uuid(), 0, 501, 't_part_trigger', '/tmp/p');
    """
    )

    pg_cursor.execute(
        """
        INSERT INTO main.ducklake_partition_info
            (partition_id, table_id, begin_snapshot, end_snapshot)
        VALUES (501, 501, 0, NULL)
    """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        "SELECT partition_id, table_id, begin_snapshot, end_snapshot "
        "FROM lake_ducklake.partition_info WHERE partition_id = 501"
    )
    row = pg_cursor.fetchone()
    assert row == (501, 501, 0, None), row


def test_create_table_with_partition_by_writes_spec(pg_cursor):
    """
    PG-side CREATE TABLE with partition_by must populate partition_info
    plus one partition_column row per transform.
    """
    location = f"s3://{TEST_BUCKET}/p_create"
    pg_cursor.execute(
        f"""
        CREATE TABLE p_create (
            id INTEGER,
            region TEXT
        ) USING ducklake
        WITH (location = '{location}', partition_by = 'region')
    """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT pi.partition_id, pi.end_snapshot
          FROM lake_ducklake.partition_info pi
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'p_create' AND t.end_snapshot IS NULL
    """
    )
    pi_rows = pg_cursor.fetchall()
    assert len(pi_rows) == 1, f"expected one live partition_info row, got {pi_rows}"
    assert pi_rows[0][1] is None, "partition_info should be live"

    pg_cursor.execute(
        """
        SELECT pc.transform, c.column_name
          FROM lake_ducklake.partition_column pc
          JOIN lake_ducklake.column c
            ON c.column_id = pc.column_id AND c.end_snapshot IS NULL
          JOIN lake_ducklake.table t ON t.table_id = c.table_id
         WHERE t.table_name = 'p_create' AND t.end_snapshot IS NULL
         ORDER BY pc.partition_key_index
    """
    )
    pc_rows = pg_cursor.fetchall()
    assert pc_rows == [("identity", "region")], pc_rows


def test_create_table_with_bucket_transform(pg_cursor):
    """
    bucket(N) transforms should be stored with parens (DuckLake style),
    not square brackets (Iceberg style).
    """
    location = f"s3://{TEST_BUCKET}/p_bucket"
    pg_cursor.execute(
        f"""
        CREATE TABLE p_bucket (
            id INTEGER,
            region TEXT
        ) USING ducklake
        WITH (location = '{location}', partition_by = 'bucket(4, region)')
    """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT pc.transform
          FROM lake_ducklake.partition_column pc
          JOIN lake_ducklake.column c
            ON c.column_id = pc.column_id AND c.end_snapshot IS NULL
          JOIN lake_ducklake.table t ON t.table_id = c.table_id
         WHERE t.table_name = 'p_bucket' AND t.end_snapshot IS NULL
    """
    )
    transforms = [r[0] for r in pg_cursor.fetchall()]
    assert transforms == ["bucket(4)"], transforms


def test_insert_routes_to_per_partition_files(pg_cursor, s3):
    """
    INSERT three rows with two distinct partition values and verify:
    - one data_file row per distinct partition value
    - data_file.partition_id is set (non-NULL) on every routed file
    - file_partition_value rows match the partition values
    - SELECT round-trips all rows
    """
    location = f"s3://{TEST_BUCKET}/p_insert"
    pg_cursor.execute(
        f"""
        CREATE TABLE p_insert (
            id INTEGER,
            region TEXT
        ) USING ducklake
        WITH (location = '{location}', partition_by = 'region')
    """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        INSERT INTO p_insert VALUES
            (1, 'us'),
            (2, 'us'),
            (3, 'eu')
    """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute(
        """
        SELECT df.partition_id, df.record_count
          FROM lake_ducklake.data_file df
          JOIN lake_ducklake.table t USING (table_id)
         WHERE t.table_name = 'p_insert' AND df.end_snapshot IS NULL
         ORDER BY df.record_count, df.data_file_id
    """
    )
    df_rows = pg_cursor.fetchall()
    assert len(df_rows) == 2, f"expected 2 data files, got {df_rows}"
    for partition_id, record_count in df_rows:
        assert partition_id is not None, df_rows
    assert sorted(r[1] for r in df_rows) == [1, 2], df_rows

    pg_cursor.execute(
        """
        SELECT fpv.partition_value
          FROM lake_ducklake.file_partition_value fpv
          JOIN lake_ducklake.data_file df USING (data_file_id)
          JOIN lake_ducklake.table t ON t.table_id = df.table_id
         WHERE t.table_name = 'p_insert'
         ORDER BY fpv.partition_value
    """
    )
    values = [r[0] for r in pg_cursor.fetchall()]
    assert values == ["eu", "us"], values

    pg_cursor.execute("SELECT id, region FROM p_insert ORDER BY id")
    rows = pg_cursor.fetchall()
    assert rows == [(1, "us"), (2, "us"), (3, "eu")], rows


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_duckdb_reads_partitioned_pg_table(pg_cursor, s3):
    """
    DuckDB ATTACH should be able to read a partitioned table created and
    populated by PostgreSQL.
    """
    location = f"s3://{TEST_BUCKET}/p_pg2duck"
    pg_cursor.execute(
        f"""
        CREATE TABLE p_pg2duck (
            id INTEGER,
            region TEXT
        ) USING ducklake
        WITH (location = '{location}', partition_by = 'region')
    """
    )
    pg_cursor.execute(
        """
        INSERT INTO p_pg2duck VALUES (1, 'us'), (2, 'us'), (3, 'eu')
    """
    )
    pg_cursor.connection.commit()

    duck = duckdb.connect()
    duck.execute("INSTALL postgres")
    duck.execute("LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly")
    duck.execute("LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    conn = (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )
    duck.execute(
        f"ATTACH 'postgres:{conn}' AS dl " f"(TYPE DUCKLAKE, METADATA_SCHEMA 'main')"
    )

    rows = duck.execute(
        "SELECT id, region FROM dl.public.p_pg2duck ORDER BY id"
    ).fetchall()
    assert rows == [(1, "us"), (2, "us"), (3, "eu")], rows


def test_partition_pruning_filter_correctness(pg_cursor, s3):
    """
    With per-file partition values attached to TableDataFile, PruneDataFiles
    must drop files whose partition value can't satisfy the WHERE clause but
    keep the rest. Verifies correctness end-to-end: SELECTs that target a
    single partition return only that partition's rows; SELECTs with no
    filter still return everything.
    """
    location = f"s3://{TEST_BUCKET}/p_prune"
    pg_cursor.execute(
        f"""
        CREATE TABLE p_prune (
            id INTEGER,
            region TEXT
        ) USING ducklake
        WITH (location = '{location}', partition_by = 'region')
    """
    )
    pg_cursor.execute(
        """
        INSERT INTO p_prune VALUES
            (1, 'us'), (2, 'us'),
            (3, 'eu'), (4, 'eu'),
            (5, 'apac')
    """
    )
    pg_cursor.connection.commit()

    pg_cursor.execute("SELECT id FROM p_prune WHERE region = 'us' ORDER BY id")
    assert [r[0] for r in pg_cursor.fetchall()] == [1, 2]

    pg_cursor.execute("SELECT id FROM p_prune WHERE region = 'eu' ORDER BY id")
    assert [r[0] for r in pg_cursor.fetchall()] == [3, 4]

    pg_cursor.execute(
        "SELECT id FROM p_prune WHERE region IN ('us','apac') ORDER BY id"
    )
    assert [r[0] for r in pg_cursor.fetchall()] == [1, 2, 5]

    pg_cursor.execute("SELECT count(*) FROM p_prune")
    assert pg_cursor.fetchone()[0] == 5

    pg_cursor.execute("SELECT id FROM p_prune WHERE region = 'nowhere' ORDER BY id")
    assert pg_cursor.fetchall() == []


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
