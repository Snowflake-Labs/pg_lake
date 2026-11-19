"""Pruning skips partition fields with a void transform, which a v1 table
leaves behind when a partition field is dropped."""

import json

from helpers.spark import *
from utils_pytest import *


def test_filter_on_column_of_void_partition_field(
    installcheck, s3, pg_conn, extension, spark_session
):
    if installcheck:
        return

    table_name = "public.test_void_partition_field"
    spark_session.sql(
        f"CREATE TABLE {table_name} (id bigint, p string) USING iceberg "
        "PARTITIONED BY (p) TBLPROPERTIES ('format-version'='1')"
    )
    try:
        spark_session.sql(f"INSERT INTO {table_name} VALUES (1, 'x'), (2, 'y')")
        spark_session.sql(f"ALTER TABLE {table_name} DROP PARTITION FIELD p")
        spark_session.sql(f"INSERT INTO {table_name} VALUES (3, 'x')")
        spark_session.sql(
            f"ALTER TABLE {table_name} SET TBLPROPERTIES ('format-version'='2')"
        )
        spark_session.sql(f"INSERT INTO {table_name} VALUES (4, 'x')")
        # keep only the v2 snapshot, whose metadata has a sequence number
        spark_session.sql(
            f"CALL pg_lake.system.expire_snapshots(table => '{table_name}', "
            "older_than => TIMESTAMP '2999-01-01 00:00:00', retain_last => 1)"
        )
        metadata_location = (
            spark_session.sql(
                f"SELECT file FROM {table_name}.metadata_log_entries "
                "ORDER BY timestamp DESC"
            )
            .collect()[0]
            .file
        )
        metadata = json.loads(read_s3_operations(s3, metadata_location))
        transforms = [
            field["transform"]
            for spec in metadata["partition-specs"]
            for field in spec["fields"]
        ]
        assert transforms == ["identity", "void"]

        run_command(
            f"""
            CREATE FOREIGN TABLE test_void_partition_field ()
            SERVER pg_lake
            OPTIONS (path '{metadata_location}', format 'iceberg')
            """,
            pg_conn,
        )

        assert run_query(
            "SELECT id FROM test_void_partition_field WHERE p = 'x' ORDER BY id",
            pg_conn,
        ) == [[1], [3], [4]]

        results = run_query(
            "EXPLAIN (verbose, format json) "
            "SELECT * FROM test_void_partition_field WHERE p = 'x'",
            pg_conn,
        )
        assert int(fetch_data_files_used(results)) == 3
    finally:
        pg_conn.rollback()
        spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
