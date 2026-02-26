import pytest
import psycopg2
from utils_pytest import *
from helpers.spark import *


def test_iceberg_add_file(
    installcheck,
    pg_conn,
    duckdb_conn,
    spark_session,
    s3,
    extension,
    create_test_helper_functions,
):
    run_command(
        """
                CREATE SCHEMA test_iceberg_add_file;
                """,
        pg_conn,
    )

    # test relies on keeping some snapshots
    run_command(
        f"""
            SET pg_lake_iceberg.max_snapshot_age TO 100000;
        """,
        pg_conn,
    )

    install_duckdb_extension(duckdb_conn, "iceberg")

    location = "s3://" + TEST_BUCKET + "/test_iceberg_add_file"

    # first, generate some data files that we'll use later in the test
    # lake_table.files have pkey on table_name/file_path, so have many files
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_0.parquet'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_1.parquet'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_2.parquet'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_3.parquet'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,100)i) TO '{location}_100_rows.parquet'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,250)i) TO '{location}_250_rows.parquet'",
        pg_conn,
    )

    # should be able to create a writable table with location
    run_command(
        f"""CREATE FOREIGN TABLE test_iceberg_add_file.ft1 (
                    id int
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}', autovacuum_enabled 'False');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # lets add 10 rows data
    run_command(
        f"CALL lake_iceberg.add_files_to_table('test_iceberg_add_file.ft1'::regclass, ARRAY['{location}_10_rows_0.parquet']);",
        pg_conn,
    )
    pg_conn.commit()

    # get table's current metadata
    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_iceberg_add_file'",
        pg_conn,
    )[0][0]
    results = duckdb_conn.execute(
        f"SELECT count(*) FROM iceberg_scan('{metadata_location}')"
    ).fetchall()
    assert results[0][0] == 10

    # ensure we properly increment the sequence no
    sequence_no = metadata_location.split("metadata/")[1].split("-")[0]
    assert int(sequence_no) == 1

    # lets add 20 rows data
    run_command(
        f"CALL lake_iceberg.add_files_to_table('test_iceberg_add_file.ft1'::regclass, ARRAY['{location}_10_rows_1.parquet', '{location}_10_rows_2.parquet']);",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_iceberg_add_file'",
        pg_conn,
    )[0][0]
    results = duckdb_conn.execute(
        f"SELECT count(*) FROM iceberg_scan('{metadata_location}')"
    ).fetchall()
    assert results[0][0] == 30

    # ensure we properly increment the sequence no
    sequence_no = metadata_location.split("metadata/")[1].split("-")[0]
    assert int(sequence_no) == 2

    # lets add 10+100+250 rows data
    run_command(
        f"CALL lake_iceberg.add_files_to_table('test_iceberg_add_file.ft1'::regclass, ARRAY['{location}_10_rows_3.parquet', '{location}_100_rows.parquet', '{location}_250_rows.parquet']);",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_iceberg_add_file'",
        pg_conn,
    )[0][0]
    results = duckdb_conn.execute(
        f"SELECT count(*) FROM iceberg_scan('{metadata_location}')"
    ).fetchall()
    assert results[0][0] == 30 + 10 + 100 + 250

    # ensure we properly increment the sequence no
    sequence_no = metadata_location.split("metadata/")[1].split("-")[0]
    assert int(sequence_no) == 3

    # ensure previous metadata location as well
    previous_metadata_location = run_query(
        "SELECT previous_metadata_location FROM lake_iceberg.tables WHERE table_name = 'ft1' and table_namespace='test_iceberg_add_file'",
        pg_conn,
    )[0][0]
    previous_sequence_no = previous_metadata_location.split("metadata/")[1].split("-")[
        0
    ]
    assert int(previous_sequence_no) == 2

    # verify querying the table
    results = run_query("SELECT count(*) FROM test_iceberg_add_file.ft1", pg_conn)
    assert results[0][0] == 30 + 10 + 100 + 250

    # now compare the snapshots
    duckdb_query = f"SELECT * FROM iceberg_snapshots('{metadata_location}') ORDER BY 1"
    pg_query = f"SELECT * FROM lake_iceberg.snapshots('{metadata_location}') ORDER BY 1"
    result = assert_query_result_on_duckdb_and_pg(
        duckdb_conn, pg_conn, duckdb_query, pg_query
    )
    # we did 3 different snapshot creation
    assert len(result) == 3

    # now compare metadata/files
    spark_query = f"""SELECT CASE WHEN content = 0 THEN 'DATA'
                                WHEN content = 1 THEN 'POSITION_DELETES'
                                WHEN content = 2 THEN 'EQUALITY_DELETES'
                            END AS content, file_path, file_format, spec_id, record_count, file_size_in_bytes
                    FROM test_iceberg_add_file.ft1.files;"""

    pg_query = f"SELECT content, file_path, file_format, spec_id, record_count, file_size_in_bytes FROM lake_iceberg.files('{metadata_location}')"

    spark_register_table(
        installcheck, spark_session, "ft1", "test_iceberg_add_file", metadata_location
    )

    assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, spark_query, pg_query
    )

    spark_unregister_table(installcheck, spark_session, "ft1", "test_iceberg_add_file")

    # verify the files in the files table as well
    results = run_query(
        "SELECT count(*) FROM lake_table.files where table_name ='test_iceberg_add_file.ft1'::regclass",
        pg_conn,
    )
    assert results[0][0] == 6

    # verify the manifest metadata looks sane
    # get the latest sequence, and see that we added 3 files in the last iteration
    # and make sure we get the correct row counts
    results = run_query(
        f"SELECT added_files_count, added_rows_count FROM lake_iceberg.current_manifests('{metadata_location}') ORDER BY sequence_number DESC LIMIT 1",
        pg_conn,
    )
    print(results)
    assert results[0][0] == 3
    assert results[0][1] == 10 + 100 + 250

    run_command("DROP SCHEMA test_iceberg_add_file CASCADE", pg_conn)
    # test relies on keeping some snapshots
    run_command(
        f"""
            RESET pg_lake_iceberg.max_snapshot_age;
        """,
        pg_conn,
    )
    pg_conn.commit()


def test_iceberg_add_file_non_parquet(
    pg_conn, duckdb_conn, s3, extension, create_test_helper_functions
):
    run_command(
        """
                CREATE SCHEMA test_iceberg_add_file_non_parquet;
                """,
        pg_conn,
    )

    install_duckdb_extension(duckdb_conn, "iceberg")

    location = "s3://" + TEST_BUCKET + "/test_iceberg_add_file_non_parquet"

    # first, generate some data files that we'll use later in the test
    # lake_table.files have pkey on table_name/file_path, so have many files
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_0.parquet'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_0.csv'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_0.json'",
        pg_conn,
    )
    run_command(
        f"COPY (SELECT i FROM generate_series(1,10)i) TO '{location}_10_rows_0.noformat' WITH(format 'parquet')",
        pg_conn,
    )

    # should be able to create a writable table with location
    run_command(
        f"""CREATE FOREIGN TABLE test_iceberg_add_file_non_parquet.ft1 (
                    id int
                ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )
    pg_conn.commit()

    err = run_command(
        f"CALL lake_iceberg.add_files_to_table('test_iceberg_add_file_non_parquet.ft1'::regclass, ARRAY['{location}_10_rows_0.parquet', '{location}_10_rows_0.csv']);",
        pg_conn,
        raise_error=False,
    )
    assert "File format is not parquet:" in str(err)
    pg_conn.rollback()

    err = run_command(
        f"CALL lake_iceberg.add_files_to_table('test_iceberg_add_file_non_parquet.ft1'::regclass, ARRAY['{location}_10_rows_0.json']);",
        pg_conn,
        raise_error=False,
    )
    assert "File format is not parquet:" in str(err)
    pg_conn.rollback()

    # even if the file prefix is not parquet, we can add the file
    err = run_command(
        f"CALL lake_iceberg.add_files_to_table('test_iceberg_add_file_non_parquet.ft1'::regclass, ARRAY['{location}_10_rows_0.noformat']);",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    run_command("DROP SCHEMA test_iceberg_add_file_non_parquet CASCADE", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_test_helper_functions(superuser_conn, app_user):
    run_command(
        f"""

        CREATE OR REPLACE PROCEDURE lake_iceberg.add_files_to_table(
                table_name REGCLASS,
                file_paths TEXT[])
        LANGUAGE C
        AS 'pg_lake_table', $function$add_files_to_table$function$;
        GRANT EXECUTE ON PROCEDURE lake_iceberg.add_files_to_table(regclass, text[]) TO {app_user};

        CREATE OR REPLACE FUNCTION lake_iceberg.current_manifests(
                tableMetadataPath TEXT
        ) RETURNS TABLE(
                manifest_path TEXT,
                manifest_length BIGINT,
                partition_spec_id INT,
                manifest_content TEXT,
                sequence_number BIGINT,
                min_sequence_number BIGINT,
                added_snapshot_id BIGINT,
                added_files_count INT,
                existing_files_count INT,
                deleted_files_count INT,
                added_rows_count BIGINT,
                existing_rows_count BIGINT,
                deleted_rows_count BIGINT)
          LANGUAGE C
          IMMUTABLE STRICT
        AS 'pg_lake_iceberg', $function$current_manifests$function$;
        GRANT EXECUTE ON FUNCTION lake_iceberg.current_manifests(text) TO {app_user};
        GRANT SELECT ON lake_iceberg.tables TO {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    # Teardown: Drop the functions after the test(s) are done
    run_command(
        f"""
        DROP PROCEDURE lake_iceberg.add_files_to_table;
        DROP FUNCTION lake_iceberg.current_manifests;
        REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
""",
        superuser_conn,
    )
    superuser_conn.commit()
