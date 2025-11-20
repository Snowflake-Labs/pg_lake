import pytest
import json
import random
import string
import sys
from utils_pytest import *


def test_iceberg_table_with_large_avro_allocs(
    pg_conn, superuser_conn, iceberg_extension, with_default_location, app_user
):
    # 63 characters is the maximum length of a column name in PostgreSQL
    table_name = "".join(random.choices(string.ascii_lowercase, k=63))

    # leave space for column name suffix
    col_name_prefix = "".join(random.choices(string.ascii_lowercase, k=59))

    # default value
    col_default = "".join(random.choices(string.ascii_lowercase, k=1000))

    create_table_command = f"""
    DO $$
    DECLARE
        i   INT;
        sql TEXT;
        col_name TEXT;
        partition_by TEXT := '';
    BEGIN
        sql := 'CREATE TABLE {table_name} (';

        FOR i IN 1..1600 LOOP
            col_name := '{col_name_prefix}' || i;
            sql := sql || col_name || ' TEXT DEFAULT ''{col_default}''';
            partition_by := partition_by || 'truncate(10000000, ' || col_name || ')';

            IF i < 1600 THEN
                sql := sql || ',';
                partition_by := partition_by || ',';
            END IF;
        END LOOP;

        sql := sql || ') USING iceberg WITH (column_stats_mode = ''full'');';

        RAISE NOTICE 'Creating table with SQL: %', sql;
        EXECUTE sql;
    END
    $$;
    """

    insert_command = f"""
                -- Step 2: Insert one row with random values for each of the 1600 TEXT columns.
                DO $$
                DECLARE
                    i   INT;
                    sql TEXT;
                BEGIN
                    sql := 'INSERT INTO {table_name} VALUES (';

                    FOR i IN 1..1600 LOOP
                        -- Generate a random text value.
                        -- This example uses MD5 of a random number, just as an example.
                        -- You can change this part if you want different random text.
                        sql := sql || quote_literal(repeat(md5(random()::text), 10));

                        IF i < 1600 THEN
                            sql := sql || ',';
                        END IF;
                    END LOOP;

                    sql := sql || ');';

                    EXECUTE sql;
                END
                $$;
                """

    # write is expected to fail with the default block size
    run_command(create_table_command, pg_conn)
    try:
        run_command(insert_command, pg_conn)
        run_command("COMMIT", pg_conn)
    except Exception as e:
        assert "Value too large for file block size" in str(e)
    pg_conn.rollback()

    # set the block size to 1024 KB and try again
    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_iceberg.default_avro_writer_block_size_kb = 1024;",
            "SELECT pg_reload_conf();",
            f"GRANT SELECT ON lake_table.data_file_column_stats TO {app_user};",
        ],
        superuser_conn,
    )

    run_command(create_table_command, pg_conn)
    run_command(insert_command, pg_conn)
    result = run_query(f"SELECT COUNT(*) FROM {table_name};", pg_conn)
    assert result[0][0] == 1

    # ensuring the column bounds for all columns takes long
    # time, so pick 10 random ones
    random_column_ids = random.sample(range(1, 1600), 10)

    # make sure we have the first and last entries
    # as they are sort of edge cases
    random_column_ids = random_column_ids + [1, 1600]

    for columnId in random_column_ids:
        colName = col_name_prefix + str(columnId)
        result = run_query(f"SELECT {colName} FROM {table_name};", pg_conn)[0][0]

        bounds = run_query(
            f"SELECT lower_bound, upper_bound FROM lake_table.data_file_column_stats WHERE table_name = '{table_name}'::regclass and field_id = {columnId}",
            pg_conn,
        )

        # data_file_column_stats are truncated to 256 chars, so we need to check the truncated values match the expected range
        encoded_string = (result.encode("utf-8"))[:256]
        assert encoded_string[255] < 127

        computed_min = encoded_string.decode("utf-8")
        # increment our byte rather than construct a new bytearray for a trivial change
        computed_max = (encoded_string[:255] + bytes([encoded_string[255] + 1])).decode(
            "utf-8"
        )

        assert computed_min == bounds[0][0], "lower bound is wrong!"
        assert computed_max == bounds[0][1], "upper bound is wrong!"

    # TODO: ERROR:  unable to open avro file: Cannot write 249883 bytes in memory buffer
    # partition_specs = table_partition_specs(pg_conn, table_name)
    # assert len(partition_specs) == 1
    # partition_fields = partition_specs[-1]["fields"]
    # assert len(partition_fields) == 1600
    # for field in partition_fields:
    #    assert field["transform"] == "truncate[10000000]"

    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_iceberg.default_avro_writer_block_size_kb;",
            "SELECT pg_reload_conf();",
            f"REVOKE SELECT ON lake_table.data_file_column_stats FROM {app_user};",
        ],
        superuser_conn,
    )

    pg_conn.rollback()


def test_manifest_file_with_dot_field_name(
    pg_conn, superuser_conn, s3, iceberg_extension, create_reserialize_helper_functions
):
    key = "sample_avro/dot-field-name.avro"
    local_manifest_path = sample_avro_filepath(f"dot-field-name.avro")
    s3.upload_file(local_manifest_path, TEST_BUCKET, key)

    manifest_path = f"s3://{TEST_BUCKET}/{key}"

    regenerate_manifest_file(superuser_conn, manifest_path, s3)
    read_s3_operations(s3, manifest_path, is_text=False)
