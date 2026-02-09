import pytest
from utils_pytest import *

TABLE_NAME = "test_pg_lake_iceberg_metadata"
TABLE_NAMESPACE = "public"


def test_pg_lake_iceberg_files(
    installcheck,
    metadata_location,
    superuser_conn,
    spark_session,
    iceberg_extension,
    s3,
):
    if installcheck:
        return

    spark_query = f"""SELECT CASE WHEN content = 0 THEN 'DATA'
                                WHEN content = 1 THEN 'POSITION_DELETES'
                                WHEN content = 2 THEN 'EQUALITY_DELETES'
                            END AS content, file_path, file_format, spec_id, record_count, file_size_in_bytes
                    FROM {TABLE_NAMESPACE}.{TABLE_NAME}.files;"""

    pg_query = f"SELECT content, file_path, file_format, spec_id, record_count, file_size_in_bytes FROM lake_iceberg.files('{metadata_location}')"

    result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, superuser_conn, spark_query, pg_query
    )

    assert len(result) == 4

    assert result[3][0] == "POSITION_DELETES"

    superuser_conn.rollback()


def test_pg_lake_iceberg_table_metadata(
    installcheck, metadata_location, superuser_conn, pg_conn, iceberg_extension, s3
):
    if installcheck:
        return

    error = run_query(
        "SELECT * FROM lake_iceberg.metadata('invalid_uri')",
        pg_conn,
        raise_error=False,
    )

    assert (
        "pg_lake_iceberg: only s3://, gs://, az://, azure://, and abfss:// are supported"
        in error
    )

    pg_conn.rollback()

    pg_query = f"SELECT * FROM lake_iceberg.metadata('{metadata_location}')"

    error = run_query(pg_query, pg_conn, raise_error=False)

    assert "permission denied to read from URL" in error

    result = run_query(pg_query, superuser_conn)

    metadata = result[0][0]

    assert (
        metadata["location"]
        == "s3://testbucketcdw/public/test_pg_lake_iceberg_metadata"
    )

    snapshots = metadata["snapshots"]

    assert len(snapshots) == 4

    assert snapshots[0]["sequence-number"] == 1

    assert snapshots[1]["sequence-number"] == 2
    assert snapshots[1]["parent-snapshot-id"] == snapshots[0]["snapshot-id"]

    assert snapshots[0]["summary"]["operation"] == "append"

    assert len(metadata["snapshot-log"]) == 4

    assert metadata["snapshot-log"][3]["snapshot-id"] == snapshots[3]["snapshot-id"]


def test_pg_lake_iceberg_snapshots(
    installcheck, metadata_location, superuser_conn, duckdb_conn, iceberg_extension, s3
):
    if installcheck:
        return

    install_duckdb_extension(duckdb_conn, "iceberg")

    duckdb_query = f"SELECT * FROM iceberg_snapshots('{metadata_location}') ORDER BY 1"
    pg_query = f"SELECT * FROM lake_iceberg.snapshots('{metadata_location}') ORDER BY 1"

    assert_query_result_on_duckdb_and_pg(
        duckdb_conn, superuser_conn, duckdb_query, pg_query
    )


def test_unsupported_url(installcheck, superuser_conn, iceberg_extension, s3):
    if installcheck:
        return

    query = f"SELECT * FROM lake_iceberg.snapshots('invalid_uri')"

    error = run_query(query, superuser_conn, raise_error=False)

    assert (
        "pg_lake_iceberg: only s3://, gs://, az://, azure://, and abfss:// are supported"
        in error
    )


@pytest.fixture(scope="module")
def metadata_location(installcheck, spark_session):
    if installcheck:
        yield
        return

    table_name = f"{TABLE_NAMESPACE}.{TABLE_NAME}"

    spark_session.sql("CREATE TABLE tmp_table(city string, lat double, long double)")

    spark_session.sql(
        """INSERT INTO tmp_table VALUES ('Amsterdam', 52.371807, 4.896029),
                                                      ('San Francisco', 37.773972, -122.431297),
                                                      ('Drachten', 53.11254, 6.0989),
                                                      ('Paris', 48.864716, 2.349014)"""
    )

    spark_session.sql(
        f"""CREATE TABLE {table_name}(city string, lat double, long double) USING iceberg
                            TBLPROPERTIES('write.delete.mode' = 'merge-on-read')"""
    )

    spark_session.sql(f"INSERT INTO {table_name} SELECT * FROM tmp_table")

    spark_session.sql("DROP TABLE IF EXISTS tmp_table")

    spark_session.sql(f"INSERT INTO {table_name} VALUES ('Berlin', 52.5200, 13.4050)")

    spark_session.sql(
        f"INSERT INTO {table_name} VALUES ('New York', 40.7128, -74.0060)"
    )

    spark_session.sql(f"DELETE FROM {table_name} WHERE city = 'Amsterdam'")

    metadata_location = (
        spark_session.sql(
            f"SELECT timestamp, file FROM {table_name}.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )

    yield metadata_location

    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
