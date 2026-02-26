import pytest
import psycopg2
from utils_pytest import *
from helpers.spark import *


SPARK_TABLE_NAME = "test_reserialize_via_spark"
SPARK_TABLE_NAMESPACE = "public"
SPARK_TABLE_COLUMNS = [
    ['`a test"`', "int"],
    ["col_long", "long"],
    ["col_string", "string"],
    ["col_binary", "binary"],
    ["col_date", "date"],
    ["col_ts", "timestamp_ntz"],
]


def partition_fields(superuser_conn, metadata_location):
    partition_fields = run_query(
        f"""
        SELECT * from lake_iceberg.current_partition_fields('{metadata_location}') ORDER BY partition_field_id;
""",
        superuser_conn,
    )

    return partition_fields


def test_partition_fields_udf(
    pg_conn, superuser_conn, with_default_location, create_reserialize_helper_functions
):
    run_command(
        """CREATE TABLE test_partition_fields_udf (
                    col_smallint smallint,
                    col_int int,
                    col_bigint bigint,
                    col_float float4,
                    col_double float8,
                    col_bool bool,
                    col_text text,
                    col_bytea bytea,
                    col_date date,
                    col_timestamp timestamp,
                    col_timestamptz timestamptz,
                    col_time time,
                    col_numeric numeric(4,2),
                    col_uuid uuid) USING iceberg
                   WITH (partition_by = 'col_smallint,col_int,col_float,col_double,col_bool,col_text,col_bytea,col_date,col_timestamp,col_timestamptz,col_time,col_numeric,col_uuid');
                    
                   INSERT INTO test_partition_fields_udf VALUES(1, 2, 3, 4.5, 5.5, 'true', 'hello', '\\x0102', '2020-01-01', '2020-01-01', '2020-01-01', '10:00:00', 12.21, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11');""",
        pg_conn,
    )
    pg_conn.commit()
    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_partition_fields_udf'",
        pg_conn,
    )[0][0]

    fields = partition_fields(superuser_conn, metadata_location)

    # strip file path since it can change at each run
    fields = [field[1:] for field in fields]

    assert fields == [
        [1000, "col_smallint", "int", None, "1"],
        [1001, "col_int", "int", None, "2"],
        [1002, "col_float", "float", None, "4.500000"],
        [1003, "col_double", "double", None, "5.500000"],
        [1004, "col_bool", "boolean", None, "true"],
        [1005, "col_text", "string", None, "hello"],
        [1006, "col_bytea", "bytes", None, "0102"],
        [1007, "col_date", "int", "date", "18262"],
        [1008, "col_timestamp", "long", "timestamp", "1577836800000000"],
        [1009, "col_timestamptz", "long", "timestamp", "1577836800000000"],
        [1010, "col_time", "long", "time", "36000000000"],
        [1011, "col_numeric", "bytes", "decimal", "04c5"],
        [1012, "col_uuid", "bytes", "uuid", "a0eebc999c0b4ef8bb6d6bb9bd380a11"],
    ]

    run_command("DROP TABLE test_partition_fields_udf", pg_conn)
    pg_conn.commit()


def test_reserialize_via_spark(
    installcheck,
    spark_session,
    superuser_conn,
    extension,
    s3,
    spark_table_metadata_location,
    create_reserialize_helper_functions,
):
    if installcheck:
        return

    partition_fields_before_reserialize = partition_fields(
        superuser_conn, spark_table_metadata_location
    )

    old_metadata = read_s3_operations(s3, spark_table_metadata_location)
    manifest_list_location = manifest_list_file_location(
        superuser_conn, spark_table_metadata_location
    )

    read_s3_operations(s3, manifest_list_location, is_text=False)
    manifest_locations = manifest_file_locations(superuser_conn, manifest_list_location)

    for manifest_location in manifest_locations:
        read_s3_operations(s3, manifest_location, is_text=False)

    table_name = f"{SPARK_TABLE_NAMESPACE}.{SPARK_TABLE_NAME}"
    result = spark_session.sql(
        f'select * from {table_name} order by `a test"`'
    ).collect()

    # now, we use our own logic to regenerate the metadata.json,
    # and push it to s3 instead of the original metadata.json generated via pyiceberg
    regenerate_metadata_json(superuser_conn, spark_table_metadata_location, s3)
    new_metadata = read_s3_operations(s3, spark_table_metadata_location)

    # regenerate manifest list file and push it to s3 by replacing the original one
    regenerate_manifest_list_file(superuser_conn, manifest_list_location, s3)
    read_s3_operations(s3, manifest_list_location, is_text=False)

    # regenerate manifest files and push them to s3 by replacing the original ones
    for manifest_location in manifest_locations:
        regenerate_manifest_file(superuser_conn, manifest_location, s3)
        read_s3_operations(s3, manifest_location, is_text=False)

    partition_fields_after_reserialize = partition_fields(
        superuser_conn, spark_table_metadata_location
    )

    assert len(partition_fields_before_reserialize) > 0
    assert partition_fields_before_reserialize == partition_fields_after_reserialize

    # reload the metadata, and query the table
    spark_query = f'select * from {table_name} order by `a test"`'

    result_after_reserialize = spark_session.sql(spark_query).collect()

    assert len(result) > 0
    assert result == result_after_reserialize

    assert_jsons_equivalent(old_metadata, new_metadata)

    # create a pg_lake table from path
    run_command(
        f"""
        CREATE FOREIGN TABLE {table_name} () SERVER pg_lake
        OPTIONS (path '{spark_table_metadata_location}');
        """,
        superuser_conn,
    )

    pg_query = f'select * from {table_name} order by "a test""" nulls first'

    pg_lake_result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, superuser_conn, spark_query, pg_query
    )

    assert len(pg_lake_result) > 0

    superuser_conn.rollback()


@pytest.fixture(scope="module")
def spark_table_metadata_location(installcheck, spark_session):
    if installcheck:
        yield
        return

    table_name = f"{SPARK_TABLE_NAMESPACE}.{SPARK_TABLE_NAME}"

    spark_session.sql(
        f"""CREATE TABLE {table_name}(
            {', '.join([f'{col[0]} {col[1]}' for col in SPARK_TABLE_COLUMNS])}
            ) USING iceberg PARTITIONED BY (`a test"`,
                                            truncate(456, col_string),
                                            bucket(16, col_long),
                                            truncate(12, col_binary),
                                            day(col_date),
                                            hour(col_ts))"""
    )

    spark_session.sql(
        f"""INSERT INTO {table_name} VALUES (11, 21323, 'Amsterdam', cast('Amsterdam' as binary),
                                             date '2021-01-01', timestamp '2021-01-01 00:00:00-04:00')"""
    )

    spark_session.sql(
        f'ALTER TABLE {table_name} ADD PARTITION FIELD bucket(3, `a test"`) AS `shard @ asd|"`'
    )

    spark_session.sql(
        f"""INSERT INTO {table_name} VALUES
            -- Row 1: Maximum INT, maximum BIGINT, extreme timestamps
            (
                2147483647,
                9223372036854775807,
                'max int',
                cast('max int' as binary),
                date '1970-01-01',
                timestamp '2000-01-01 00:00:00'
            ),

            -- Row 2: Zero/empty string, mid-range timestamps
            (
                0,
                0,
                '',
                cast('' as binary),
                date '1971-01-01 00:00:00',
                timestamp '2100-12-31 23:59:59'
            ),

            -- Row 3: Minimum INT, minimum BIGINT, some NULL timestamp
            (
                -2147483648,
                -9223372036854775808,
                'min int',
                cast('min int' as binary),
                date '0001-01-02',
                timestamp '1970-01-01 00:00:00'
            ),

            -- Row 4: Special characters in STRING, typical numeric values, more timestamp checks
            (
                123,
                1234,
                'specialChars!@#$%^&*()_+',
                cast('specialChars!@#$%^&*()_+' as binary),
                date '2020-02-29',
                timestamp '9999-12-31 23:59:59'
            ),

            -- Row 5: Emoji in STRING, more timestamps around the epoch boundary
            (
                404,
                808,
                'emoji: 😀',
                cast('emoji: 😀' as binary),
                date '1970-01-01',
                timestamp '1970-01-02 00:00:00'
            )"""
    )

    # insert all nulls
    spark_session.sql(
        f"INSERT INTO {table_name} VALUES (null, null, null, null, null, null)"
    )

    # alter partition
    spark_session.sql(
        f"ALTER TABLE {table_name} REPLACE PARTITION FIELD col_long_bucket WITH truncate(2, col_long)"
    )

    # insert more data
    spark_session.sql(
        f"""INSERT INTO {table_name} VALUES (1, 9999, null, null, null, null),
                                            (2, 8888, null, null, null, null),
                                            (3, 7777, null, null, null, null),
                                            (4, 6666, null, null, null, null),
                                            (5, 5555, null, null, null, null)"""
    )

    spark_table_metadata_location = (
        spark_session.sql(
            f"SELECT timestamp, file FROM {table_name}.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )

    yield spark_table_metadata_location

    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
