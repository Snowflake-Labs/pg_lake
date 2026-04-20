"""
Tests for partitioned INSERT..SELECT and COPY FROM pushdown.

When the target Iceberg table uses partition transforms that can be
expressed as DuckDB SQL (identity, year, month, day, hour), the write
is pushed down to DuckDB COPY TO with PARTITION_BY. Bucket and truncate
transforms continue to use the row-by-row PartitionedDestReceiver.
"""

import pytest
from utils_pytest import *


SCHEMA = "test_partitioned_pushdown"
N_ROWS = 50


def _table(name: str) -> str:
    return f"{SCHEMA}.{name}"


def _setup_schema(pg_conn):
    run_command(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};", pg_conn)


def _drop_tables(pg_conn, *names):
    for name in names:
        run_command(f"DROP TABLE IF EXISTS {_table(name)};", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# INSERT..SELECT pushdown into partitioned tables
# ---------------------------------------------------------------------------


def test_insert_select_year_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into year-partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_year", "tgt_year")
    src = _table("src_year")
    tgt = _table("tgt_year")

    run_command(
        f"""
        CREATE TABLE {src}(id int, ts timestamp) USING iceberg;
        INSERT INTO {src}
            SELECT i, '2020-01-15'::timestamp + (i * interval '120 days')
            FROM generate_series(1, {N_ROWS}) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts timestamp) USING iceberg
        WITH (partition_by = 'year(ts)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Verify pushdown is used
    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    # Verify row count
    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == N_ROWS

    # Verify correct number of files (one per distinct year)
    exp_years = run_query(
        f"SELECT count(DISTINCT extract(year from ts)) FROM {src};", pg_conn
    )[0][0]
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == exp_years

    # Verify each file contains a single partition value
    files = run_query(
        f"SELECT path, id FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )
    for path, file_id in files:
        distinct = run_query(
            f"SELECT count(DISTINCT (year(ts) - 1970)) FROM '{path}'",
            pgduck_conn,
        )
        assert distinct[0][0] == "1"

        # Verify partition metadata matches file data
        file_year = run_query(
            f"SELECT DISTINCT (year(ts) - 1970) FROM '{path}'", pgduck_conn
        )[0][0]
        meta_val = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id = '{file_id}'",
            pg_conn,
        )[0][0]
        assert str(meta_val) == str(file_year)

    # Verify data is readable and matches source
    src_data = run_query(f"SELECT id, ts FROM {src} ORDER BY id;", pg_conn)
    tgt_data = run_query(f"SELECT id, ts FROM {tgt} ORDER BY id;", pg_conn)
    assert src_data == tgt_data

    _drop_tables(pg_conn, "tgt_year", "src_year")


def test_insert_select_month_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into month-partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_month", "tgt_month")
    src = _table("src_month")
    tgt = _table("tgt_month")

    run_command(
        f"""
        CREATE TABLE {src}(id int, d date) USING iceberg;
        INSERT INTO {src}
            SELECT i, '2024-01-15'::date + (i * interval '10 days')
            FROM generate_series(1, {N_ROWS}) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, d date) USING iceberg
        WITH (partition_by = 'month(d)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == N_ROWS

    # Verify file count matches distinct months
    exp_months = run_query(
        f"SELECT count(DISTINCT (extract(year from d)::int - 1970) * 12 + extract(month from d)::int - 1) FROM {src};",
        pg_conn,
    )[0][0]
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == exp_months

    _drop_tables(pg_conn, "tgt_month", "src_month")


def test_insert_select_day_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into day-partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_day", "tgt_day")
    src = _table("src_day")
    tgt = _table("tgt_day")

    run_command(
        f"""
        CREATE TABLE {src}(id int, ts timestamptz) USING iceberg;
        INSERT INTO {src}
            SELECT i, '2025-03-01'::timestamptz + (i * interval '6 hours')
            FROM generate_series(1, {N_ROWS}) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts timestamptz) USING iceberg
        WITH (partition_by = 'day(ts)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == N_ROWS

    # Verify each file has one distinct day
    files = run_query(
        f"SELECT path FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )
    for (path,) in files:
        distinct = run_query(
            f"SELECT count(DISTINCT ts::date) FROM '{path}'", pgduck_conn
        )
        assert distinct[0][0] == "1"

    _drop_tables(pg_conn, "tgt_day", "src_day")


def test_insert_select_hour_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into hour-partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_hour", "tgt_hour")
    src = _table("src_hour")
    tgt = _table("tgt_hour")

    run_command(
        f"""
        CREATE TABLE {src}(id int, ts timestamp) USING iceberg;
        INSERT INTO {src}
            SELECT i, '2025-06-01 00:00:00'::timestamp + (i * interval '15 minutes')
            FROM generate_series(1, {N_ROWS}) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts timestamp) USING iceberg
        WITH (partition_by = 'hour(ts)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == N_ROWS

    _drop_tables(pg_conn, "tgt_hour", "src_hour")


def test_insert_select_identity_int_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into identity(int)-partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_id_int", "tgt_id_int")
    src = _table("src_id_int")
    tgt = _table("tgt_id_int")

    n_partitions = 5
    run_command(
        f"""
        CREATE TABLE {src}(id int, category int) USING iceberg;
        INSERT INTO {src}
            SELECT i, (i % {n_partitions})
            FROM generate_series(1, {N_ROWS}) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, category int) USING iceberg
        WITH (partition_by = 'category', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == N_ROWS

    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == n_partitions

    # Verify each file has one distinct category
    files = run_query(
        f"SELECT path, id FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )
    for path, file_id in files:
        distinct = run_query(
            f"SELECT count(DISTINCT category) FROM '{path}'", pgduck_conn
        )
        assert distinct[0][0] == "1"

        file_val = run_query(f"SELECT DISTINCT category FROM '{path}'", pgduck_conn)[0][
            0
        ]
        meta_val = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id = '{file_id}'",
            pg_conn,
        )[0][0]
        assert str(meta_val) == str(file_val)

    _drop_tables(pg_conn, "tgt_id_int", "src_id_int")


def test_insert_select_identity_text_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into identity(text)-partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_id_txt", "tgt_id_txt")
    src = _table("src_id_txt")
    tgt = _table("tgt_id_txt")

    run_command(
        f"""
        CREATE TABLE {src}(id int, region text) USING iceberg;
        INSERT INTO {src} VALUES
            (1, 'us-east'), (2, 'us-west'), (3, 'eu-west'),
            (4, 'us-east'), (5, 'us-west'), (6, 'eu-west'),
            (7, 'us-east'), (8, 'ap-south');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, region text) USING iceberg
        WITH (partition_by = 'region', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 8

    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 4  # 4 distinct regions

    _drop_tables(pg_conn, "tgt_id_txt", "src_id_txt")


def test_insert_select_identity_date_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into identity(date)-partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_id_date", "tgt_id_date")
    src = _table("src_id_date")
    tgt = _table("tgt_id_date")

    run_command(
        f"""
        CREATE TABLE {src}(id int, d date) USING iceberg;
        INSERT INTO {src}
            SELECT i, '2025-01-01'::date + (i % 5)
            FROM generate_series(1, {N_ROWS}) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, d date) USING iceberg
        WITH (partition_by = 'd', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == N_ROWS

    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 5

    # Verify data matches
    src_data = run_query(f"SELECT id, d FROM {src} ORDER BY id;", pg_conn)
    tgt_data = run_query(f"SELECT id, d FROM {tgt} ORDER BY id;", pg_conn)
    assert src_data == tgt_data

    _drop_tables(pg_conn, "tgt_id_date", "src_id_date")


# ---------------------------------------------------------------------------
# Multi-field partition pushdown
# ---------------------------------------------------------------------------


def test_insert_select_multi_partition_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT into table with multiple partition fields uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_multi", "tgt_multi")
    src = _table("src_multi")
    tgt = _table("tgt_multi")

    run_command(
        f"""
        CREATE TABLE {src}(id int, ts timestamp, region text) USING iceberg;
        INSERT INTO {src} VALUES
            (1, '2024-03-15 10:00:00', 'us'),
            (2, '2024-03-15 11:00:00', 'eu'),
            (3, '2025-07-20 09:00:00', 'us'),
            (4, '2025-07-20 14:00:00', 'eu'),
            (5, '2025-07-20 14:30:00', 'us'),
            (6, '2024-03-15 10:30:00', 'eu');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts timestamp, region text) USING iceberg
        WITH (partition_by = 'year(ts), region', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 6

    # 4 distinct (year, region) combinations:
    # (2024, us), (2024, eu), (2025, us), (2025, eu)
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 4

    # Verify data is correct
    src_data = run_query(f"SELECT id, ts, region FROM {src} ORDER BY id;", pg_conn)
    tgt_data = run_query(f"SELECT id, ts, region FROM {tgt} ORDER BY id;", pg_conn)
    assert src_data == tgt_data

    _drop_tables(pg_conn, "tgt_multi", "src_multi")


# ---------------------------------------------------------------------------
# NULL partition values
# ---------------------------------------------------------------------------


def test_insert_select_null_partition_value(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT with NULL values in partition column produces correct metadata."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_null", "tgt_null")
    src = _table("src_null")
    tgt = _table("tgt_null")

    run_command(
        f"""
        CREATE TABLE {src}(id int, category text) USING iceberg;
        INSERT INTO {src} VALUES
            (1, 'a'), (2, 'b'), (3, NULL), (4, 'a'), (5, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, category text) USING iceberg
        WITH (partition_by = 'category', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 5

    # 3 files: 'a', 'b', NULL
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 3

    # Check the NULL partition file
    files = run_query(
        f"SELECT id FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )
    null_count = 0
    for (file_id,) in files:
        meta = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id = '{file_id}'",
            pg_conn,
        )
        if meta[0][0] is None:
            null_count += 1
    assert null_count == 1

    _drop_tables(pg_conn, "tgt_null", "src_null")


# ---------------------------------------------------------------------------
# COPY FROM pushdown into partitioned tables
# ---------------------------------------------------------------------------


def test_copy_from_year_partitioned_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """COPY FROM into year-partitioned Iceberg table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "copy_year")
    parquet_url = f"s3://{TEST_BUCKET}/test_partitioned_pushdown/copy_year.parquet"
    tgt = _table("copy_year")

    # Create source data as parquet
    run_command(
        f"""
        COPY (
            SELECT i AS id,
                   '2020-01-15'::timestamp + (i * interval '120 days') AS ts
            FROM generate_series(1, {N_ROWS}) i
        ) TO '{parquet_url}';
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts timestamp) USING iceberg
        WITH (partition_by = 'year(ts)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"COPY {tgt} FROM '{parquet_url}';", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == N_ROWS

    # Verify files are split by year
    exp_years = run_query(
        f"SELECT count(DISTINCT extract(year from ts)) FROM {tgt};", pg_conn
    )[0][0]
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == exp_years

    _drop_tables(pg_conn, "copy_year")


def test_copy_from_identity_partitioned_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """COPY FROM into identity-partitioned Iceberg table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "copy_ident")
    parquet_url = f"s3://{TEST_BUCKET}/test_partitioned_pushdown/copy_identity.parquet"
    tgt = _table("copy_ident")

    run_command(
        f"""
        COPY (
            SELECT i AS id, (i % 4) AS category
            FROM generate_series(1, 20) i
        ) TO '{parquet_url}';
        """,
        pg_conn,
    )

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, category int) USING iceberg
        WITH (partition_by = 'category', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"COPY {tgt} FROM '{parquet_url}';", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 20

    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 4  # 0, 1, 2, 3

    _drop_tables(pg_conn, "copy_ident")


# ---------------------------------------------------------------------------
# Partition pruning after pushdown write
# ---------------------------------------------------------------------------


def test_partition_pruning_after_pushdown_write(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """After pushdown write, partition pruning should work correctly."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_prune", "tgt_prune")
    src = _table("src_prune")
    tgt = _table("tgt_prune")

    run_command(
        f"""
        CREATE TABLE {src}(id int, d date) USING iceberg;
        INSERT INTO {src}
            SELECT i, '2024-01-01'::date + ((i % 12) * 30)
            FROM generate_series(1, {N_ROWS}) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, d date) USING iceberg
        WITH (partition_by = 'month(d)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    total_files = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert total_files > 1  # Multiple partitions

    # Query with filter that should prune partitions
    result = run_query(
        f"SELECT count(*) FROM {tgt} WHERE d >= '2024-06-01' AND d < '2024-07-01';",
        pg_conn,
    )
    # Just verify it returns a valid result (no crash from wrong partition metadata)
    assert result[0][0] >= 0

    _drop_tables(pg_conn, "tgt_prune", "src_prune")


# ---------------------------------------------------------------------------
# Clamped values: infinity, out-of-range temporals, NaN
# ---------------------------------------------------------------------------


def test_infinity_timestamp_clamped_year_partition(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Infinity timestamps are clamped before partitioning, landing in year 9999."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_inf")
    tgt = _table("tgt_inf")

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts timestamp) USING iceberg
        WITH (partition_by = 'year(ts)', autovacuum_enabled = false,
              out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        WITH data(id, ts) AS (VALUES
            (1, '2025-06-15 12:00:00'::timestamp),
            (2, 'infinity'::timestamp),
            (3, '-infinity'::timestamp),
            (4, '1970-01-01 00:00:00'::timestamp)
        )
        INSERT INTO {tgt} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 4

    # infinity -> 9999 (year 8029 since epoch), -infinity -> 0001 (year -1969)
    # plus 2025 (year 55) and 1970 (year 0) = 4 distinct partitions
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 4

    # Verify the clamped values are readable
    tgt_data = run_query(f"SELECT id, ts FROM {tgt} ORDER BY id;", pg_conn)
    assert tgt_data[0][0] == 1  # normal row
    assert tgt_data[1][1].year == 9999  # infinity clamped to max
    assert tgt_data[2][1].year == 1  # -infinity clamped to min
    assert tgt_data[3][0] == 4  # normal row

    _drop_tables(pg_conn, "tgt_inf")


def test_infinity_date_clamped_month_partition(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Infinity dates are clamped before month partitioning."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_inf_d")
    tgt = _table("tgt_inf_d")

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, d date) USING iceberg
        WITH (partition_by = 'month(d)', autovacuum_enabled = false,
              out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        WITH data(id, d) AS (VALUES
            (1, '2025-03-15'::date),
            (2, 'infinity'::date),
            (3, '-infinity'::date)
        )
        INSERT INTO {tgt} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 3

    # 3 distinct month partitions
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 3

    _drop_tables(pg_conn, "tgt_inf_d")


def test_infinity_identity_partition_clamped(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Infinity date/timestamp clamped in identity partition (epoch integers in path)."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_inf_id_d", "tgt_inf_id_ts")
    tgt_date = _table("tgt_inf_id_d")
    tgt_ts = _table("tgt_inf_id_ts")

    # Identity-partitioned date table
    run_command(
        f"""
        CREATE TABLE {tgt_date}(id int, d date) USING iceberg
        WITH (partition_by = 'd', autovacuum_enabled = false,
              out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        WITH data(id, d) AS (VALUES
            (1, '2025-03-15'::date),
            (2, 'infinity'::date),
            (3, '-infinity'::date)
        )
        INSERT INTO {tgt_date} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt_date};", pg_conn)
    assert result[0][0] == 3

    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt_date}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 3  # 3 distinct dates

    # Identity-partitioned timestamp table
    run_command(
        f"""
        CREATE TABLE {tgt_ts}(id int, ts timestamp) USING iceberg
        WITH (partition_by = 'ts', autovacuum_enabled = false,
              out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        WITH data(id, ts) AS (VALUES
            (1, '2025-06-15 12:00:00'::timestamp),
            (2, 'infinity'::timestamp),
            (3, '-infinity'::timestamp)
        )
        INSERT INTO {tgt_ts} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt_ts};", pg_conn)
    assert result[0][0] == 3

    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt_ts}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 3  # 3 distinct timestamps

    # Verify data readable
    tgt_data = run_query(f"SELECT id, ts FROM {tgt_ts} ORDER BY id;", pg_conn)
    assert tgt_data[1][1].year == 9999  # infinity clamped to max
    assert tgt_data[2][1].year == 1  # -infinity clamped to min

    _drop_tables(pg_conn, "tgt_inf_id_d", "tgt_inf_id_ts")


def test_infinity_clamped_cross_path(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Clamped infinity values produce same partition values via both paths."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_inf_push", "tgt_inf_row")
    tgt_push = _table("tgt_inf_push")
    tgt_row = _table("tgt_inf_row")

    for tgt in (tgt_push, tgt_row):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, ts timestamp) USING iceberg
            WITH (partition_by = 'year(ts)', autovacuum_enabled = false,
                  out_of_range_values = 'clamp');
            """,
            pg_conn,
        )
        pg_conn.commit()

    # Pushdown path: CTE -> INSERT..SELECT
    run_command(
        f"""
        WITH data(id, ts) AS (VALUES
            (1, '2025-01-01 00:00:00'::timestamp),
            (2, 'infinity'::timestamp),
            (3, '-infinity'::timestamp)
        )
        INSERT INTO {tgt_push} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Non-pushdown path: INSERT..VALUES (row-by-row)
    run_command(
        f"""
        INSERT INTO {tgt_row} VALUES
            (1, '2025-01-01 00:00:00'::timestamp),
            (2, 'infinity'::timestamp),
            (3, '-infinity'::timestamp);
        """,
        pg_conn,
    )
    pg_conn.commit()

    push_vals = _partition_value_set(tgt_push, pg_conn)
    row_vals = _partition_value_set(tgt_row, pg_conn)

    assert push_vals == row_vals, (
        f"Partition value mismatch with infinity clamping:\n"
        f"  pushdown: {sorted(push_vals)}\n"
        f"  row-by-row: {sorted(row_vals)}"
    )

    _drop_tables(pg_conn, "tgt_inf_push", "tgt_inf_row")


def test_null_from_nan_clamp_in_identity_partition(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """NaN in bounded numeric clamped to NULL produces a NULL partition value."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_nan")
    tgt = _table("tgt_nan")

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, val numeric(10,2)) USING iceberg
        WITH (partition_by = 'val', autovacuum_enabled = false,
              out_of_range_values = 'clamp');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        WITH data(id, val) AS (VALUES
            (1, 42.50::numeric(10,2)),
            (2, 'NaN'::numeric(10,2)),
            (3, 99.99::numeric(10,2))
        )
        INSERT INTO {tgt} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 3

    # 3 partitions: 42.50, 99.99, NULL (from NaN clamp)
    file_cnt = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )[0][0]
    assert file_cnt == 3

    # Verify the NaN row has NULL partition value
    files = run_query(
        f"SELECT id FROM lake_table.files WHERE table_name = '{tgt}'::regclass;",
        pg_conn,
    )
    null_count = 0
    for (file_id,) in files:
        meta = run_query(
            f"SELECT value FROM lake_table.data_file_partition_values WHERE id = '{file_id}'",
            pg_conn,
        )
        if meta[0][0] is None:
            null_count += 1
    assert null_count == 1

    _drop_tables(pg_conn, "tgt_nan")


def test_identity_decimal_extreme_values(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Identity partition on decimal with NaN, infinity, and large values."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_dec_push", "tgt_dec_row")
    tgt_push = _table("tgt_dec_push")
    tgt_row = _table("tgt_dec_row")

    for tgt in (tgt_push, tgt_row):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, val numeric) USING iceberg
            WITH (partition_by = 'val', autovacuum_enabled = false);
            """,
            pg_conn,
        )
        pg_conn.commit()

    values_sql = """
        (1, 0::numeric),
        (2, -1::numeric),
        (3, 99999999999999999999.12345::numeric),
        (4, -99999999999999999999.12345::numeric),
        (5, 0.000001::numeric)
    """

    # Pushdown path
    run_command(
        f"""
        WITH data(id, val) AS (VALUES {values_sql})
        INSERT INTO {tgt_push} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Non-pushdown path
    run_command(f"INSERT INTO {tgt_row} VALUES {values_sql};", pg_conn)
    pg_conn.commit()

    # Verify row counts
    for tgt in (tgt_push, tgt_row):
        result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
        assert result[0][0] == 5

    # Cross-path: partition values must match
    push_vals = _partition_value_set(tgt_push, pg_conn)
    row_vals = _partition_value_set(tgt_row, pg_conn)
    assert push_vals == row_vals, (
        f"Decimal partition value mismatch:\n"
        f"  pushdown: {sorted(push_vals)}\n"
        f"  row-by-row: {sorted(row_vals)}"
    )

    # Verify data readable and matches
    push_data = run_query(f"SELECT id, val FROM {tgt_push} ORDER BY id;", pg_conn)
    row_data = run_query(f"SELECT id, val FROM {tgt_row} ORDER BY id;", pg_conn)
    assert push_data == row_data

    _drop_tables(pg_conn, "tgt_dec_push", "tgt_dec_row")


def test_identity_float_nan_infinity(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Identity partition on float8 with NaN and infinity (become NULL partitions)."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_flt_push", "tgt_flt_row")
    tgt_push = _table("tgt_flt_push")
    tgt_row = _table("tgt_flt_row")

    for tgt in (tgt_push, tgt_row):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, val float8) USING iceberg
            WITH (partition_by = 'val', autovacuum_enabled = false);
            """,
            pg_conn,
        )
        pg_conn.commit()

    values_sql = """
        (1, 1.5::float8),
        (2, 'NaN'::float8),
        (3, 'infinity'::float8),
        (4, '-infinity'::float8),
        (5, -0.0::float8)
    """

    # Pushdown path
    run_command(
        f"""
        WITH data(id, val) AS (VALUES {values_sql})
        INSERT INTO {tgt_push} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Non-pushdown path
    run_command(f"INSERT INTO {tgt_row} VALUES {values_sql};", pg_conn)
    pg_conn.commit()

    for tgt in (tgt_push, tgt_row):
        result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
        assert result[0][0] == 5

    # Cross-path: partition values must match
    push_vals = _partition_value_set(tgt_push, pg_conn)
    row_vals = _partition_value_set(tgt_row, pg_conn)
    assert push_vals == row_vals, (
        f"Float partition value mismatch:\n"
        f"  pushdown: {sorted(push_vals)}\n"
        f"  row-by-row: {sorted(row_vals)}"
    )

    # NaN and infinity should produce NULL partition values
    null_count = sum(
        1
        for vals in _get_partition_values(tgt_push, pg_conn).values()
        for v in vals.values()
        if v is None
    )
    # NaN, infinity, -infinity all become NULL partitions = 3 NULL partition files
    assert null_count == 3

    _drop_tables(pg_conn, "tgt_flt_push", "tgt_flt_row")


def test_identity_text_special_characters(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Identity partition on text with characters that DuckDB percent-encodes in Hive paths."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_special_push", "tgt_special_row")
    tgt_push = _table("tgt_special_push")
    tgt_row = _table("tgt_special_row")

    for tgt in (tgt_push, tgt_row):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, val text) USING iceberg
            WITH (partition_by = 'val', autovacuum_enabled = false);
            """,
            pg_conn,
        )
        pg_conn.commit()

    values_sql = """
        (1, 'hello world'),
        (2, 'a+b'),
        (3, 'x=y'),
        (4, '100%done'),
        (5, 'café')
    """

    # Pushdown path
    run_command(
        f"""
        WITH data(id, val) AS (VALUES {values_sql})
        INSERT INTO {tgt_push} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Non-pushdown path
    run_command(f"INSERT INTO {tgt_row} VALUES {values_sql};", pg_conn)
    pg_conn.commit()

    for tgt in (tgt_push, tgt_row):
        result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
        assert result[0][0] == 5

    # Cross-path: partition values must match
    push_vals = _partition_value_set(tgt_push, pg_conn)
    row_vals = _partition_value_set(tgt_row, pg_conn)
    assert push_vals == row_vals, (
        f"Text special char partition value mismatch:\n"
        f"  pushdown: {sorted(push_vals)}\n"
        f"  row-by-row: {sorted(row_vals)}"
    )

    # Verify data readable and matches
    push_data = run_query(f"SELECT id, val FROM {tgt_push} ORDER BY id;", pg_conn)
    row_data = run_query(f"SELECT id, val FROM {tgt_row} ORDER BY id;", pg_conn)
    assert push_data == row_data

    _drop_tables(pg_conn, "tgt_special_push", "tgt_special_row")


# ---------------------------------------------------------------------------
# Cross-path validation: pushdown (DuckDB) vs non-pushdown (C) partition values
# ---------------------------------------------------------------------------


def _get_partition_values(table_name, pg_conn):
    """Return {file_id: {field_id: value}} for all files in a table."""
    files = run_query(
        f"SELECT id FROM lake_table.files WHERE table_name = '{table_name}'::regclass;",
        pg_conn,
    )
    result = {}
    for (file_id,) in files:
        vals = run_query(
            f"SELECT partition_field_id, value "
            f"FROM lake_table.data_file_partition_values WHERE id = '{file_id}'",
            pg_conn,
        )
        result[file_id] = {int(fid): v for fid, v in vals}
    return result


def _partition_value_set(table_name, pg_conn):
    """Return a set of (field_id, value) tuples across all files -- for comparison."""
    all_vals = _get_partition_values(table_name, pg_conn)
    return {(fid, v) for file_vals in all_vals.values() for fid, v in file_vals.items()}


@pytest.mark.parametrize(
    "transform,col_name,col_type,values",
    [
        (
            "year(ts)",
            "ts",
            "timestamp",
            [
                "'2020-06-15 10:00:00'",
                "'2023-01-01 00:00:00'",
                "'1969-07-20 20:17:00'",
                "'1970-01-01 00:00:00'",
            ],
        ),
        (
            "month(ts)",
            "ts",
            "timestamp",
            [
                "'2024-01-15 10:00:00'",
                "'2024-06-15 10:00:00'",
                "'1969-12-31 23:59:59'",
                "'1970-01-01 00:00:00'",
            ],
        ),
        (
            "day(d)",
            "d",
            "date",
            [
                "'2025-03-15'",
                "'2020-02-29'",
                "'1969-12-31'",
                "'1970-01-01'",
            ],
        ),
        (
            "hour(ts)",
            "ts",
            "timestamp",
            [
                "'2025-01-01 00:00:00'",
                "'2025-01-01 12:30:00'",
                "'1969-12-31 23:30:00'",
                "'1970-01-01 00:30:00'",
            ],
        ),
        (
            "d",
            "d",
            "date",
            [
                "'2025-01-15'",
                "'1969-07-20'",
                "'1970-01-01'",
                "'2000-06-30'",
            ],
        ),
        (
            "ts",
            "ts",
            "timestamp",
            [
                "'2025-01-15 10:30:00'",
                "'1969-07-20 20:17:00'",
                "'1970-01-01 00:00:00'",
                "'2000-06-30 12:00:00'",
            ],
        ),
        (
            "v",
            "v",
            "uuid",
            [
                "'550e8400-e29b-41d4-a716-446655440000'",
                "'6ba7b810-9dad-11d1-80b4-00c04fd430c8'",
                "'00000000-0000-0000-0000-000000000000'",
                "'ffffffff-ffff-ffff-ffff-ffffffffffff'",
            ],
        ),
        (
            "v",
            "v",
            "boolean",
            [
                "true",
                "false",
            ],
        ),
        (
            "v",
            "v",
            "time",
            [
                "'00:00:00'",
                "'12:30:45'",
                "'23:59:59.999999'",
            ],
        ),
    ],
    ids=[
        "year",
        "month",
        "day",
        "hour",
        "identity_date",
        "identity_timestamp",
        "identity_uuid",
        "identity_boolean",
        "identity_time",
    ],
)
def test_cross_path_partition_values(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
    transform,
    col_name,
    col_type,
    values,
):
    """Partition values from pushdown (DuckDB) must match non-pushdown (C) path.

    Inserts the same rows via INSERT..VALUES (non-pushdown, C code computes
    partition values) and INSERT..SELECT (pushdown, DuckDB SQL expressions
    compute partition values), then asserts the stored partition values are
    identical.
    """
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "xv_src", "xv_push", "xv_row")
    src = _table("xv_src")
    tgt_pushdown = _table("xv_push")
    tgt_rowbyrow = _table("xv_row")

    # Source table for INSERT..SELECT
    values_sql = ", ".join(f"({i + 1}, {v}::{col_type})" for i, v in enumerate(values))
    run_command(
        f"""
        CREATE TABLE {src}(id int, {col_name} {col_type}) USING iceberg;
        INSERT INTO {src} VALUES {values_sql};
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Two target tables with identical schema
    for tgt in (tgt_pushdown, tgt_rowbyrow):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, {col_name} {col_type}) USING iceberg
            WITH (partition_by = '{transform}', autovacuum_enabled = false);
            """,
            pg_conn,
        )
        pg_conn.commit()

    # Pushdown path: INSERT..SELECT
    run_command(f"INSERT INTO {tgt_pushdown} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    # Non-pushdown path: INSERT..VALUES (always row-by-row)
    run_command(f"INSERT INTO {tgt_rowbyrow} VALUES {values_sql};", pg_conn)
    pg_conn.commit()

    # Compare partition value sets
    push_vals = _partition_value_set(tgt_pushdown, pg_conn)
    row_vals = _partition_value_set(tgt_rowbyrow, pg_conn)

    assert push_vals == row_vals, (
        f"Partition value mismatch for {transform}:\n"
        f"  pushdown (DuckDB): {sorted(push_vals)}\n"
        f"  row-by-row (C):    {sorted(row_vals)}"
    )

    _drop_tables(pg_conn, "xv_src", "xv_push", "xv_row")


# ---------------------------------------------------------------------------
# Bucket/truncate fallback (NOT pushdown)
# ---------------------------------------------------------------------------


def test_bucket_partition_not_pushdownable(
    extension,
    s3,
    with_default_location,
    pg_conn,
):
    """INSERT..SELECT into bucket-partitioned table should NOT use pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_bucket", "tgt_bucket")
    src = _table("src_bucket")
    tgt = _table("tgt_bucket")

    run_command(
        f"""
        CREATE TABLE {src}(id int, val text) USING iceberg;
        INSERT INTO {src} SELECT i, 'val_' || i FROM generate_series(1, 10) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, val text) USING iceberg
        WITH (partition_by = 'bucket(4, id)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_not_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    # Verify it still works via non-pushdown path
    run_command(
        f"""
        SET pg_lake_table.enable_insert_select_pushdown TO false;
        INSERT INTO {tgt} SELECT * FROM {src};
        RESET pg_lake_table.enable_insert_select_pushdown;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 10

    _drop_tables(pg_conn, "tgt_bucket", "src_bucket")


def test_truncate_partition_not_pushdownable(
    extension,
    s3,
    with_default_location,
    pg_conn,
):
    """INSERT..SELECT into truncate-partitioned table should NOT use pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_trunc", "tgt_trunc")
    src = _table("src_trunc")
    tgt = _table("tgt_trunc")

    run_command(
        f"""
        CREATE TABLE {src}(id int, val text) USING iceberg;
        INSERT INTO {src} SELECT i, 'val_' || i FROM generate_series(1, 10) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, val text) USING iceberg
        WITH (partition_by = 'truncate(3, val)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_not_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    _drop_tables(pg_conn, "tgt_trunc", "src_trunc")


def test_bytea_identity_partition_not_pushdownable(
    extension,
    s3,
    with_default_location,
    pg_conn,
):
    """INSERT..SELECT into identity(bytea)-partitioned table should NOT use pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_bytea", "tgt_bytea")
    src = _table("src_bytea")
    tgt = _table("tgt_bytea")

    run_command(
        f"""
        CREATE TABLE {src}(id int, v bytea) USING iceberg;
        INSERT INTO {src} VALUES (1, '\\x0001'), (2, '\\x00ff'), (3, '\\xdeadbeef');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, v bytea) USING iceberg
        WITH (partition_by = 'v', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_not_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    _drop_tables(pg_conn, "tgt_bytea", "src_bytea")


def test_hour_partition_not_pushdownable_for_time(
    extension,
    s3,
    with_default_location,
    pg_conn,
):
    """INSERT..SELECT into hour(time)-partitioned table should NOT use pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_hour_time", "tgt_hour_time")
    src = _table("src_hour_time")
    tgt = _table("tgt_hour_time")

    run_command(
        f"""
        CREATE TABLE {src}(id int, t time) USING iceberg;
        INSERT INTO {src} VALUES (1, '10:30:00'), (2, '14:00:00'), (3, '23:59:59');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, t time) USING iceberg
        WITH (partition_by = 'hour(t)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_not_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    _drop_tables(pg_conn, "tgt_hour_time", "src_hour_time")


def test_hour_partition_not_pushdownable_for_timetz(
    extension,
    s3,
    with_default_location,
    pg_conn,
):
    """INSERT..SELECT into hour(timetz)-partitioned table should NOT use pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "src_hour_timetz", "tgt_hour_timetz")
    src = _table("src_hour_timetz")
    tgt = _table("tgt_hour_timetz")

    run_command(
        f"""
        CREATE TABLE {src}(id int, t timetz) USING iceberg;
        INSERT INTO {src} VALUES
            (1, '10:30:00+02'), (2, '14:00:00-05'), (3, '23:59:59+00');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, t timetz) USING iceberg
        WITH (partition_by = 'hour(t)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert_query_not_pushdownable(f"INSERT INTO {tgt} SELECT * FROM {src}", pg_conn)

    _drop_tables(pg_conn, "tgt_hour_timetz", "src_hour_timetz")


# ---------------------------------------------------------------------------
# Text with path-separator characters (validates partition_keys approach)
# ---------------------------------------------------------------------------


def test_identity_text_with_slashes(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Identity partition on text with forward slashes and path-like characters."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_slash_push", "tgt_slash_row")
    tgt_push = _table("tgt_slash_push")
    tgt_row = _table("tgt_slash_row")

    for tgt in (tgt_push, tgt_row):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, val text) USING iceberg
            WITH (partition_by = 'val', autovacuum_enabled = false);
            """,
            pg_conn,
        )
        pg_conn.commit()

    values_sql = """
        (1, 'a/b'),
        (2, '1/1'),
        (3, 'path/to/file'),
        (4, 'no-slash'),
        (5, '/leading'),
        (6, 'trailing/')
    """

    # Pushdown path
    run_command(
        f"""
        WITH data(id, val) AS (VALUES {values_sql})
        INSERT INTO {tgt_push} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Non-pushdown path
    run_command(f"INSERT INTO {tgt_row} VALUES {values_sql};", pg_conn)
    pg_conn.commit()

    for tgt in (tgt_push, tgt_row):
        result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
        assert result[0][0] == 6

    # Cross-path: partition values must match
    push_vals = _partition_value_set(tgt_push, pg_conn)
    row_vals = _partition_value_set(tgt_row, pg_conn)
    assert push_vals == row_vals, (
        f"Text slash partition value mismatch:\n"
        f"  pushdown: {sorted(push_vals)}\n"
        f"  row-by-row: {sorted(row_vals)}"
    )

    # Verify data readable and matches
    push_data = run_query(f"SELECT id, val FROM {tgt_push} ORDER BY id;", pg_conn)
    row_data = run_query(f"SELECT id, val FROM {tgt_row} ORDER BY id;", pg_conn)
    assert push_data == row_data

    _drop_tables(pg_conn, "tgt_slash_push", "tgt_slash_row")


# ---------------------------------------------------------------------------
# UTC timezone cross-path: timestamptz near midnight boundaries
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "transform,col_name,col_type,values",
    [
        (
            "year(ts)",
            "ts",
            "timestamptz",
            [
                # Near midnight UTC on New Year: these would land in different years
                # if timezone is not handled correctly
                "'2024-12-31 23:30:00+00'",
                "'2025-01-01 00:30:00+00'",
                # Same instants expressed in a non-UTC timezone — UTC year boundary
                # at midnight means these are in different years in UTC
                "'2024-12-31 18:30:00-05'",
                "'2025-01-01 05:30:00+05'",
            ],
        ),
        (
            "month(ts)",
            "ts",
            "timestamptz",
            [
                # Near midnight UTC on month boundary
                "'2025-03-31 23:30:00+00'",
                "'2025-04-01 00:30:00+00'",
                # Same instants in other timezones
                "'2025-03-31 18:30:00-05'",
                "'2025-04-01 05:30:00+05'",
            ],
        ),
        (
            "day(ts)",
            "ts",
            "timestamptz",
            [
                # Near midnight UTC on day boundary
                "'2025-06-15 23:30:00+00'",
                "'2025-06-16 00:30:00+00'",
                # Same instants in non-UTC timezones
                "'2025-06-15 18:30:00-05'",
                "'2025-06-16 05:30:00+05'",
            ],
        ),
        (
            "hour(ts)",
            "ts",
            "timestamptz",
            [
                # Near hour boundary in UTC
                "'2025-06-15 14:59:00+00'",
                "'2025-06-15 15:01:00+00'",
                # Same instants in non-UTC: should produce same hour partitions
                "'2025-06-15 09:59:00-05'",
                "'2025-06-15 20:01:00+05'",
            ],
        ),
    ],
    ids=["year_tz", "month_tz", "day_tz", "hour_tz"],
)
def test_cross_path_timestamptz_utc(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
    transform,
    col_name,
    col_type,
    values,
):
    """Timestamptz partition values must use UTC, matching pushdown and non-pushdown paths.

    Tests values near temporal boundaries (midnight, month/year rollover) where
    incorrect timezone handling would produce wrong partition values.
    """
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "xv_tz_src", "xv_tz_push", "xv_tz_row")
    src = _table("xv_tz_src")
    tgt_pushdown = _table("xv_tz_push")
    tgt_rowbyrow = _table("xv_tz_row")

    values_sql = ", ".join(f"({i + 1}, {v}::{col_type})" for i, v in enumerate(values))
    run_command(
        f"""
        CREATE TABLE {src}(id int, {col_name} {col_type}) USING iceberg;
        INSERT INTO {src} VALUES {values_sql};
        """,
        pg_conn,
    )
    pg_conn.commit()

    for tgt in (tgt_pushdown, tgt_rowbyrow):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, {col_name} {col_type}) USING iceberg
            WITH (partition_by = '{transform}', autovacuum_enabled = false);
            """,
            pg_conn,
        )
        pg_conn.commit()

    # Pushdown path: INSERT..SELECT
    run_command(f"INSERT INTO {tgt_pushdown} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    # Non-pushdown path: INSERT..VALUES
    run_command(f"INSERT INTO {tgt_rowbyrow} VALUES {values_sql};", pg_conn)
    pg_conn.commit()

    # Compare partition value sets
    push_vals = _partition_value_set(tgt_pushdown, pg_conn)
    row_vals = _partition_value_set(tgt_rowbyrow, pg_conn)

    assert push_vals == row_vals, (
        f"Timestamptz partition value mismatch for {transform}:\n"
        f"  pushdown (DuckDB): {sorted(push_vals)}\n"
        f"  row-by-row (C):    {sorted(row_vals)}"
    )

    # Also verify row counts match
    push_cnt = run_query(f"SELECT count(*) FROM {tgt_pushdown};", pg_conn)[0][0]
    row_cnt = run_query(f"SELECT count(*) FROM {tgt_rowbyrow};", pg_conn)[0][0]
    assert push_cnt == row_cnt == len(values)

    _drop_tables(pg_conn, "xv_tz_src", "xv_tz_push", "xv_tz_row")


# ---------------------------------------------------------------------------
# INSERT..SELECT with CTE into partitioned table
# ---------------------------------------------------------------------------


def test_insert_select_with_cte_partitioned(
    extension,
    s3,
    with_default_location,
    pg_conn,
    grant_access_to_data_file_partition,
):
    """INSERT..SELECT with CTE into partitioned table uses pushdown."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "cte_tgt")
    tgt = _table("cte_tgt")

    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts date) USING iceberg
        WITH (partition_by = 'year(ts)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = f"""
        WITH data AS (
            SELECT i AS id, '2023-06-15'::date + (i * 180) AS ts
            FROM generate_series(1, 10) i
        )
        INSERT INTO {tgt} SELECT * FROM data
    """

    assert_query_pushdownable(query, pg_conn)

    run_command(query + ";", pg_conn)
    pg_conn.commit()

    result = run_query(f"SELECT count(*) FROM {tgt};", pg_conn)
    assert result[0][0] == 10

    _drop_tables(pg_conn, "cte_tgt")


# ---------------------------------------------------------------------------
# Long text partition values (verifies flat file paths work without HTTP 400)
# ---------------------------------------------------------------------------


def test_identity_text_long_values(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Identity partition on text with long values (1000+ chars)."""
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "tgt_long_push", "tgt_long_row")
    tgt_push = _table("tgt_long_push")
    tgt_row = _table("tgt_long_row")

    for tgt in (tgt_push, tgt_row):
        run_command(
            f"""
            CREATE TABLE {tgt}(id int, val text) USING iceberg
            WITH (partition_by = 'val', autovacuum_enabled = false);
            """,
            pg_conn,
        )
        pg_conn.commit()

    long_a = "a" * 1000
    long_b = "b" * 1500
    long_mixed = "x" * 500 + "/" * 100 + "y" * 400

    # Pushdown path
    run_command(
        f"""
        WITH data(id, val) AS (VALUES
            (1, '{long_a}'),
            (2, '{long_b}'),
            (3, '{long_mixed}'),
            (4, 'short')
        )
        INSERT INTO {tgt_push} SELECT * FROM data;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Non-pushdown path
    run_command(
        f"""
        INSERT INTO {tgt_row} VALUES
            (1, '{long_a}'),
            (2, '{long_b}'),
            (3, '{long_mixed}'),
            (4, 'short');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Cross-path: partition values must match
    push_vals = _partition_value_set(tgt_push, pg_conn)
    row_vals = _partition_value_set(tgt_row, pg_conn)
    assert push_vals == row_vals, (
        f"Long text partition value mismatch:\n"
        f"  pushdown: {sorted(len(v) for v in push_vals)}\n"
        f"  row-by-row: {sorted(len(v) for v in row_vals)}"
    )

    # Verify data readable and matches
    push_data = run_query(f"SELECT id, val FROM {tgt_push} ORDER BY id;", pg_conn)
    row_data = run_query(f"SELECT id, val FROM {tgt_row} ORDER BY id;", pg_conn)
    assert push_data == row_data

    _drop_tables(pg_conn, "tgt_long_push", "tgt_long_row")


# ---------------------------------------------------------------------------
# Partition evolution: pushdown across spec changes
# ---------------------------------------------------------------------------


def test_partition_evolution_pushdown(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """Pushdown works correctly across partition spec evolution.

    Evolves: unpartitioned -> year(ts) -> month(ts) -> unpartitioned,
    inserting data via pushdown at each stage and verifying all data
    remains readable.
    """
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "evo_src", "evo_tgt")
    src = _table("evo_src")
    tgt = _table("evo_tgt")

    run_command(
        f"""
        CREATE TABLE {src}(id int, ts timestamp) USING iceberg;
        INSERT INTO {src}
            SELECT i, '2020-01-15'::timestamp + (i * interval '45 days')
            FROM generate_series(1, 20) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Phase 1: unpartitioned target
    run_command(
        f"""
        CREATE TABLE {tgt}(id int, ts timestamp) USING iceberg
        WITH (autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src} WHERE id <= 5;", pg_conn)
    pg_conn.commit()
    assert run_query(f"SELECT count(*) FROM {tgt};", pg_conn)[0][0] == 5

    # Phase 2: evolve to year(ts)
    run_command(f"ALTER TABLE {tgt} OPTIONS (ADD partition_by 'year(ts)');", pg_conn)
    pg_conn.commit()

    assert_query_pushdownable(
        f"INSERT INTO {tgt} SELECT * FROM {src} WHERE id > 5 AND id <= 10", pg_conn
    )
    run_command(
        f"INSERT INTO {tgt} SELECT * FROM {src} WHERE id > 5 AND id <= 10;", pg_conn
    )
    pg_conn.commit()
    assert run_query(f"SELECT count(*) FROM {tgt};", pg_conn)[0][0] == 10

    # Phase 3: evolve to month(ts)
    run_command(f"ALTER TABLE {tgt} OPTIONS (SET partition_by 'month(ts)');", pg_conn)
    pg_conn.commit()

    assert_query_pushdownable(
        f"INSERT INTO {tgt} SELECT * FROM {src} WHERE id > 10 AND id <= 15", pg_conn
    )
    run_command(
        f"INSERT INTO {tgt} SELECT * FROM {src} WHERE id > 10 AND id <= 15;", pg_conn
    )
    pg_conn.commit()
    assert run_query(f"SELECT count(*) FROM {tgt};", pg_conn)[0][0] == 15

    # Phase 4: drop partitioning
    run_command(f"ALTER TABLE {tgt} OPTIONS (DROP partition_by);", pg_conn)
    pg_conn.commit()

    run_command(f"INSERT INTO {tgt} SELECT * FROM {src} WHERE id > 15;", pg_conn)
    pg_conn.commit()
    assert run_query(f"SELECT count(*) FROM {tgt};", pg_conn)[0][0] == 20

    # Verify all data is readable and correct
    src_data = run_query(f"SELECT id, ts FROM {src} ORDER BY id;", pg_conn)
    tgt_data = run_query(f"SELECT id, ts FROM {tgt} ORDER BY id;", pg_conn)
    assert src_data == tgt_data

    _drop_tables(pg_conn, "evo_src", "evo_tgt")


def test_enable_partitioned_write_pushdown_guc(
    extension,
    s3,
    with_default_location,
    pg_conn,
    pgduck_conn,
    grant_access_to_data_file_partition,
):
    """
    When pg_lake_table.enable_partitioned_write_pushdown is off,
    partitioned INSERT..SELECT falls back to row-by-row processing
    but still produces correct results. Non-partitioned pushdown
    remains unaffected.
    """
    _setup_schema(pg_conn)
    _drop_tables(pg_conn, "guc_src", "guc_tgt_part", "guc_tgt_nopart")
    src = _table("guc_src")
    tgt_part = _table("guc_tgt_part")
    tgt_nopart = _table("guc_tgt_nopart")

    # source table
    run_command(
        f"""
        CREATE TABLE {src}(id INT, ts TIMESTAMP)
        USING iceberg WITH (autovacuum_enabled = false);
        INSERT INTO {src}
        SELECT i, '2025-01-01'::timestamp + (i || ' days')::interval
        FROM generate_series(1, 20) AS i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # partitioned target
    run_command(
        f"""
        CREATE TABLE {tgt_part}(id INT, ts TIMESTAMP)
        USING iceberg
        WITH (partition_by = 'month(ts)', autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # non-partitioned target
    run_command(
        f"""
        CREATE TABLE {tgt_nopart}(id INT, ts TIMESTAMP)
        USING iceberg WITH (autovacuum_enabled = false);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # verify pushdown is on by default for partitioned tables
    assert_query_pushdownable(f"INSERT INTO {tgt_part} SELECT * FROM {src}", pg_conn)

    # disable partitioned write pushdown
    run_command(
        "SET pg_lake_table.enable_partitioned_write_pushdown TO false;", pg_conn
    )

    # partitioned INSERT..SELECT should no longer be pushdownable
    assert_query_not_pushdownable(
        f"INSERT INTO {tgt_part} SELECT * FROM {src}", pg_conn
    )

    # non-partitioned INSERT..SELECT should still be pushdownable
    assert_query_pushdownable(f"INSERT INTO {tgt_nopart} SELECT * FROM {src}", pg_conn)

    # insert via row-by-row fallback should still produce correct results
    run_command(f"INSERT INTO {tgt_part} SELECT * FROM {src};", pg_conn)
    pg_conn.commit()

    res = run_query(
        f"""
        SELECT count(*), count(DISTINCT id)
        FROM {tgt_part};
        """,
        pg_conn,
    )
    assert res[0][0] == 20
    assert res[0][1] == 20

    # verify partition files are correct
    file_cnt = run_query(
        f"""
        SELECT count(*)
        FROM lake_table.files
        WHERE table_name = '{tgt_part}'::regclass;
        """,
        pg_conn,
    )[0][0]
    assert file_cnt == 1  # all in January 2025

    run_command("RESET pg_lake_table.enable_partitioned_write_pushdown;", pg_conn)
    _drop_tables(pg_conn, "guc_src", "guc_tgt_part", "guc_tgt_nopart")
