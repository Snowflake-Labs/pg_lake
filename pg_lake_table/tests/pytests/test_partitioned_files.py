import pytest
from utils_pytest import *


# we use superuser_conn because we access a GUC that requires
# superuser, otherwise should be fine to run with pg_conn
def test_max_open_files(superuser_conn, s3, extension, with_default_location):

    run_command(
        """

		CREATE SCHEMA test_s3_copy_to_json;
		CREATE TABLE test_s3_copy_to_json.tbl (a int) USING iceberg WITH (autovacuum_enabled=False, partition_by='a');

		-- only allow 1 file per partitioned writes
		SET pg_lake_table.max_open_files_for_partitioned_write TO 1;

		-- now, this creates 25 different files
		INSERT INTO test_s3_copy_to_json.tbl SELECT i%2 FROM generate_series(0,24)i;
		""",
        superuser_conn,
    )

    res = run_query(
        "SELECT count(*) FROM lake_table.files WHERE table_name = 'test_s3_copy_to_json.tbl'::regclass",
        superuser_conn,
    )

    # each row generate 1 file
    assert res[0][0] == 25

    # sanity check
    res = run_query(
        "SELECT count(*), count(DISTINCT a) FROM test_s3_copy_to_json.tbl",
        superuser_conn,
    )
    assert res[0] == [25, 2]

    # sanity check on the pruning
    res = run_query(
        "SELECT count(*) FROM test_s3_copy_to_json.tbl WHERE a = 1",
        superuser_conn,
    )
    assert res[0][0] == 12

    res = run_query(
        "SELECT count(*) FROM test_s3_copy_to_json.tbl WHERE a = 0",
        superuser_conn,
    )
    assert res[0][0] == 13

    superuser_conn.rollback()


def test_load_from_partition_by(pg_conn, s3, extension, with_default_location):

    url = f"s3://{TEST_BUCKET}/test_load_from_partition_by/data.parquet"
    run_command(f"COPY (SELECT i FROM generate_series(0,249)i) TO '{url}'", pg_conn)

    run_command(
        f"""
		CREATE SCHEMA test_load_from_partition_by;
		CREATE TABLE test_load_from_partition_by.tbl() USING iceberg WITH (load_from='{url}', partition_by='truncate(10, i)', autovacuum_enabled=False);

		""",
        pg_conn,
    )

    res = run_query(
        "SELECT count(*) FROM lake_table.files WHERE table_name = 'test_load_from_partition_by.tbl'::regclass",
        pg_conn,
    )

    # total 250 rows, bucket by 10, total 25 buckets
    assert res[0][0] == 25

    # sanity check
    res = run_query(
        "SELECT count(*), count(DISTINCT i) FROM test_load_from_partition_by.tbl",
        pg_conn,
    )
    assert res[0] == [250, 250]

    pg_conn.rollback()
