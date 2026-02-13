import pytest
import datetime
from decimal import Decimal
import random
from utils_pytest import *
from spark_utils import *


TOTAL_ROWS = 100

RANDOM_TRUNCATE_LENGTH = random.randint(1, 100)

RANDOM_BUCKET_SIZE = random.randint(2, 100)

RANDOM_NUMERIC_TYPENAME = generate_random_numeric_typename()


PG_COLUMNS = [
    # physical types
    ("col_short", "int2"),
    ("col_int", "int4"),
    ("col_bigint", "int8"),
    ("col_text", "text"),
    ("col_varchar", "varchar"),
    ("col_varchar_limit", "varchar(1)"),
    ("col_bpchar", "bpchar"),
    ("col_bpchar_limit", "char(1)"),
    ("col_binary", "bytea"),
    ("col_float", "float4"),
    ("col_double", "float8"),
    ("col_bool", "bool"),
    # logical types
    ("col_date", "date"),
    ("col_timestamp", "timestamp"),
    ("col_timestamptz", "timestamptz"),
    ("col_numeric", RANDOM_NUMERIC_TYPENAME),
]

SPARK_COLUMNS = [
    # physical types
    ("col_short", "short"),
    ("col_int", "int"),
    ("col_bigint", "long"),
    ("col_text", "string"),
    ("col_varchar", "string"),
    ("col_varchar_limit", "string"),
    ("col_bpchar", "string"),
    ("col_bpchar_limit", "char(1)"),
    ("col_binary", "binary"),
    ("col_float", "float"),
    ("col_double", "double"),
    ("col_bool", "boolean"),
    # logical types
    ("col_date", "date"),
    ("col_timestamp", "timestamp_ntz"),
    ("col_timestamptz", "timestamp"),
    ("col_numeric", RANDOM_NUMERIC_TYPENAME),
]


def test_manifest_partition_summary_via_spark(
    pg_conn,
    installcheck,
    spark_session,
    create_helper_functions,
    spark_table_metadata_location,
    transforms_for_pg_lake_test_table,
):
    if installcheck:
        return

    run_command(f"SET pg_lake_iceberg.enable_manifest_merge_on_write TO off", pg_conn)

    insert_random_rows(spark_session, pg_conn, 10)

    # we do not set contains_null or contains_nan properly yet, we do not verify them
    spark_query = f"""select lower_bound, upper_bound from ( select inline(partition_summaries) from public.test_apply_partition_transform.all_manifests ) manifest_summaries
                      order by lower_bound desc nulls first, upper_bound desc nulls first;"""

    pg_query = f"""select lower_bound, upper_bound from lake_table.partition_summary('test_apply_partition_transform'::regclass)
                     order by lower_bound COLLATE "C" desc nulls first, upper_bound COLLATE "C" desc nulls first;"""

    result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, spark_query, pg_query
    )

    assert len(result) > 0

    spark_session.sql("truncate table public.test_apply_partition_transform")
    pg_conn.rollback()


def test_data_file_partitions_via_spark(
    pg_conn,
    installcheck,
    spark_session,
    create_helper_functions,
    transforms_for_pg_and_spark,
):
    if installcheck:
        return

    order_by = """order by col_short nulls first, col_int nulls first, col_bigint nulls first,
                           col_text nulls first, col_varchar nulls first, col_varchar_limit nulls first,
                           col_bpchar nulls first, col_bpchar_limit nulls first, col_binary nulls first,
                           col_float nulls first, col_double nulls first, col_bool nulls first,
                           col_date nulls first, col_timestamp nulls first, col_timestamptz nulls first,
                           col_numeric nulls first"""

    spark_query = f"select partition.* from public.test_apply_partition_transform.files {order_by};"

    pg_query = f"""select transformed.* from test_apply_partition_transform t,
                   lateral lake_table.partition_tuple(t.*)
                    as transformed({transforms_for_pg_and_spark}) {order_by};
              """

    result = assert_query_result_on_spark_and_pg(
        installcheck, spark_session, pg_conn, spark_query, pg_query
    )

    # total tows + 1 null row + 1 min row + 1 max row + 3 edge row
    assert len(result) == TOTAL_ROWS + 6

    spark_session.sql("truncate table public.test_apply_partition_transform")
    pg_conn.rollback()


def test_manifest_partition_summary(
    pg_conn,
    superuser_conn,
    installcheck,
    create_helper_functions,
    with_default_location,
):
    if installcheck:
        return

    run_command(
        f"""
        CREATE TABLE test_manifest_partition_summary(a int, b timestamp) USING iceberg
            WITH (partition_by = 'a, truncate(10, a), bucket(3, a), b, year(b), month(b), day(b), hour(b)', autovacuum_enabled = 'False');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        SET pg_lake_iceberg.enable_manifest_merge_on_write TO off;
        
        INSERT INTO test_manifest_partition_summary
            SELECT i, ((2000 + i)::text || '-' || (i % 12 + 1)::text || '-' || (i % 28 + 1)::text || ' ' ||
                    (i % 24)::text || ':' || (i % 60)::text || ':' || (i % 60)::text)::timestamp from generate_series(1, 100) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    pg_query = f"""select * from lake_table.partition_summary('test_manifest_partition_summary'::regclass)
                     order by sequence_number, partition_field_id, lower_bound COLLATE "C" desc nulls first, upper_bound COLLATE "C" desc nulls first;"""

    result = run_query(pg_query, pg_conn)

    assert result == [
        [1, 1000, "1", "100"],
        [1, 1001, "0", "100"],
        [1, 1002, "0", "2"],
        [1, 1003, "2001-02-02T01:01:01", "2100-05-17T04:40:40"],
        [1, 1004, "2001", "2100"],
        [1, 1005, "2001-02", "2100-05"],
        [1, 1006, "2001-02-02", "2100-05-17"],
        [1, 1007, "2001-02-02-01", "2100-05-17-04"],
    ]

    # insert another batch with the same spec into a different manifest (manifest merge disabled)
    run_command(
        """
        INSERT INTO test_manifest_partition_summary
            SELECT i, ((2000 + i)::text || '-' || (i % 12 + 1)::text || '-' || (i % 28 + 1)::text || ' ' ||
                    (i % 24)::text || ':' || (i % 60)::text || ':' || (i % 60)::text)::timestamp from generate_series(101, 200) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(pg_query, pg_conn)

    assert result == [
        [1, 1000, "1", "100"],
        [1, 1001, "0", "100"],
        [1, 1002, "0", "2"],
        [1, 1003, "2001-02-02T01:01:01", "2100-05-17T04:40:40"],
        [1, 1004, "2001", "2100"],
        [1, 1005, "2001-02", "2100-05"],
        [1, 1006, "2001-02-02", "2100-05-17"],
        [1, 1007, "2001-02-02-01", "2100-05-17-04"],
        [2, 1000, "101", "200"],
        [2, 1001, "100", "200"],
        [2, 1002, "0", "2"],
        [2, 1003, "2101-06-18T05:41:41", "2200-09-05T08:20:20"],
        [2, 1004, "2101", "2200"],
        [2, 1005, "2101-06", "2200-09"],
        [2, 1006, "2101-06-18", "2200-09-05"],
        [2, 1007, "2101-06-18-05", "2200-09-05-08"],
    ]

    # insert nulls
    run_command(
        "INSERT INTO test_manifest_partition_summary VALUES(NULL, NULL);", pg_conn
    )
    pg_conn.commit()

    result = run_query(pg_query, pg_conn)

    assert result == [
        [1, 1000, "1", "100"],
        [1, 1001, "0", "100"],
        [1, 1002, "0", "2"],
        [1, 1003, "2001-02-02T01:01:01", "2100-05-17T04:40:40"],
        [1, 1004, "2001", "2100"],
        [1, 1005, "2001-02", "2100-05"],
        [1, 1006, "2001-02-02", "2100-05-17"],
        [1, 1007, "2001-02-02-01", "2100-05-17-04"],
        [2, 1000, "101", "200"],
        [2, 1001, "100", "200"],
        [2, 1002, "0", "2"],
        [2, 1003, "2101-06-18T05:41:41", "2200-09-05T08:20:20"],
        [2, 1004, "2101", "2200"],
        [2, 1005, "2101-06", "2200-09"],
        [2, 1006, "2101-06-18", "2200-09-05"],
        [2, 1007, "2101-06-18-05", "2200-09-05-08"],
        [3, 1000, None, None],
        [3, 1001, None, None],
        [3, 1002, None, None],
        [3, 1003, None, None],
        [3, 1004, None, None],
        [3, 1005, None, None],
        [3, 1006, None, None],
        [3, 1007, None, None],
    ]

    # insert after altering partition spec
    run_command(
        "ALTER TABLE test_manifest_partition_summary OPTIONS (SET partition_by 'bucket(2, a)');",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        INSERT INTO test_manifest_partition_summary
            SELECT i, null from generate_series(201, 300) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(pg_query, pg_conn)

    assert result == [
        [1, 1000, "1", "100"],
        [1, 1001, "0", "100"],
        [1, 1002, "0", "2"],
        [1, 1003, "2001-02-02T01:01:01", "2100-05-17T04:40:40"],
        [1, 1004, "2001", "2100"],
        [1, 1005, "2001-02", "2100-05"],
        [1, 1006, "2001-02-02", "2100-05-17"],
        [1, 1007, "2001-02-02-01", "2100-05-17-04"],
        [2, 1000, "101", "200"],
        [2, 1001, "100", "200"],
        [2, 1002, "0", "2"],
        [2, 1003, "2101-06-18T05:41:41", "2200-09-05T08:20:20"],
        [2, 1004, "2101", "2200"],
        [2, 1005, "2101-06", "2200-09"],
        [2, 1006, "2101-06-18", "2200-09-05"],
        [2, 1007, "2101-06-18-05", "2200-09-05-08"],
        [3, 1000, None, None],
        [3, 1001, None, None],
        [3, 1002, None, None],
        [3, 1003, None, None],
        [3, 1004, None, None],
        [3, 1005, None, None],
        [3, 1006, None, None],
        [3, 1007, None, None],
        [5, 1008, "0", "1"],
    ]

    # insert another batch
    run_command(
        """
        INSERT INTO test_manifest_partition_summary
            SELECT i, null from generate_series(201, 300) i;
        """,
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(pg_query, pg_conn)

    assert result == [
        [1, 1000, "1", "100"],
        [1, 1001, "0", "100"],
        [1, 1002, "0", "2"],
        [1, 1003, "2001-02-02T01:01:01", "2100-05-17T04:40:40"],
        [1, 1004, "2001", "2100"],
        [1, 1005, "2001-02", "2100-05"],
        [1, 1006, "2001-02-02", "2100-05-17"],
        [1, 1007, "2001-02-02-01", "2100-05-17-04"],
        [2, 1000, "101", "200"],
        [2, 1001, "100", "200"],
        [2, 1002, "0", "2"],
        [2, 1003, "2101-06-18T05:41:41", "2200-09-05T08:20:20"],
        [2, 1004, "2101", "2200"],
        [2, 1005, "2101-06", "2200-09"],
        [2, 1006, "2101-06-18", "2200-09-05"],
        [2, 1007, "2101-06-18-05", "2200-09-05-08"],
        [3, 1000, None, None],
        [3, 1001, None, None],
        [3, 1002, None, None],
        [3, 1003, None, None],
        [3, 1004, None, None],
        [3, 1005, None, None],
        [3, 1006, None, None],
        [3, 1007, None, None],
        [5, 1008, "0", "1"],
        [6, 1008, "0", "1"],
    ]

    # delete to create positional deletion file
    run_command("DELETE FROM test_manifest_partition_summary WHERE a = 205;", pg_conn)
    pg_conn.commit()

    result = run_query(pg_query, pg_conn)

    assert result == [
        [1, 1000, "1", "100"],
        [1, 1001, "0", "100"],
        [1, 1002, "0", "2"],
        [1, 1003, "2001-02-02T01:01:01", "2100-05-17T04:40:40"],
        [1, 1004, "2001", "2100"],
        [1, 1005, "2001-02", "2100-05"],
        [1, 1006, "2001-02-02", "2100-05-17"],
        [1, 1007, "2001-02-02-01", "2100-05-17-04"],
        [2, 1000, "101", "200"],
        [2, 1001, "100", "200"],
        [2, 1002, "0", "2"],
        [2, 1003, "2101-06-18T05:41:41", "2200-09-05T08:20:20"],
        [2, 1004, "2101", "2200"],
        [2, 1005, "2101-06", "2200-09"],
        [2, 1006, "2101-06-18", "2200-09-05"],
        [2, 1007, "2101-06-18-05", "2200-09-05-08"],
        [3, 1000, None, None],
        [3, 1001, None, None],
        [3, 1002, None, None],
        [3, 1003, None, None],
        [3, 1004, None, None],
        [3, 1005, None, None],
        [3, 1006, None, None],
        [3, 1007, None, None],
        [5, 1008, "0", "1"],
        [6, 1008, "0", "1"],
        [7, 1008, "1", "1"],  # from deletion file
    ]

    # trigger a manifest merge now
    pg_conn.commit()
    run_command_outside_tx(
        [
            "SET pg_lake_iceberg.enable_manifest_merge_on_write TO on;",
            "SET pg_lake_iceberg.manifest_min_count_to_merge TO 2;",
            "VACUUM FULL test_manifest_partition_summary;",
            "RESET pg_lake_iceberg.enable_manifest_merge_on_write;",
            "RESET pg_lake_iceberg.manifest_min_count_to_merge;",
        ],
        superuser_conn,
    )

    result = run_query(pg_query, pg_conn)
    assert result == [
        [8, 1000, "1", "200"],
        [8, 1001, "0", "200"],
        [8, 1002, "0", "2"],
        [8, 1003, "2001-02-02T01:01:01", "2200-09-05T08:20:20"],
        [8, 1004, "2001", "2200"],
        [8, 1005, "2001-02", "2200-09"],
        [8, 1006, "2001-02-02", "2200-09-05"],
        [8, 1007, "2001-02-02-01", "2200-09-05-08"],
        [9, 1008, "0", "1"],
    ]

    # cleanup
    run_command(
        """
        DROP TABLE test_manifest_partition_summary;
        """,
        pg_conn,
    )
    pg_conn.commit()


def test_partition_transform_udf(
    pg_conn,
    create_helper_functions,
    transforms_for_pg_lake_test_table,
):
    # number of fields mismatch
    run_command("insert into test_apply_partition_transform values (default);", pg_conn)

    pg_query = """select transformed.* from test_apply_partition_transform t,
                  lateral lake_table.partition_tuple(t.*) as transformed(a int2);
              """
    error = run_query(pg_query, pg_conn, raise_error=False)

    assert "number of fields" in error

    pg_conn.rollback()

    # type mismatch
    run_command("insert into test_apply_partition_transform values (default);", pg_conn)

    type_mismatch_cols = transforms_for_pg_lake_test_table.replace("int2", "int4")
    pg_query = f"""select transformed.* from test_apply_partition_transform t,
                  lateral lake_table.partition_tuple(t.*) as transformed({type_mismatch_cols});
              """
    error = run_query(pg_query, pg_conn, raise_error=False)

    assert "does not match" in error

    pg_conn.rollback()

    # numeric typmod mismatch
    run_command("insert into test_apply_partition_transform values (default);", pg_conn)

    typemod_mismatch_cols = transforms_for_pg_lake_test_table.replace(
        RANDOM_NUMERIC_TYPENAME, "numeric(55,33)"
    )
    pg_query = f"""select transformed.* from test_apply_partition_transform t,
                  lateral lake_table.partition_tuple(t.*) as transformed({typemod_mismatch_cols});
              """
    error = run_query(pg_query, pg_conn, raise_error=False)

    assert "does not match" in error

    pg_conn.rollback()

    # try after adding new column and setting new partition_by
    run_command("insert into test_apply_partition_transform values (default);", pg_conn)
    pg_conn.commit()

    run_command(
        """ALTER TABLE test_apply_partition_transform ADD COLUMN col_new int4;
           ALTER TABLE test_apply_partition_transform OPTIONS (SET partition_by 'col_short, col_bigint, col_new');""",
        pg_conn,
    )
    pg_conn.commit()

    pg_query = """select transformed.* from test_apply_partition_transform t,
                  lateral lake_table.partition_tuple(t.*) as transformed(col_short int2, col_bigint int8, col_new int4);
              """
    run_query(pg_query, pg_conn)

    pg_conn.rollback()

    # empty partition transforms
    location = f"s3://{TEST_BUCKET}/test_empty_partition_transform"

    run_command(
        f"""create table test_empty_partition_transform (col_short int2) using iceberg with (location = '{location}');
            insert into test_empty_partition_transform values (1);""",
        pg_conn,
    )
    pg_conn.commit()

    pg_query = """select transformed.* from test_empty_partition_transform t,
                lateral lake_table.partition_tuple(t.*) as transformed(col_short int2);
            """
    error = run_query(pg_query, pg_conn, raise_error=False)

    assert "empty partition transforms" in error

    pg_conn.rollback()


# type time does not exist in spark
def test_partition_transform_time(
    pg_conn,
    create_helper_functions,
    with_default_location,
):
    run_command(
        "create table test_partition_transform_time(a time) using iceberg with (partition_by = 'a, bucket(100, a), hour(a)');",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "insert into test_partition_transform_time values ('00:00:00'), ('23:59:59');",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "select transformed.* from test_partition_transform_time t, lateral lake_table.partition_tuple(t.*) as transformed(a time, a_bucket_100 int4, a_year int4) order by a, a_bucket_100, a_year;",
        pg_conn,
    )

    assert result == [[datetime.time(0, 0), 76, 0], [datetime.time(23, 59, 59), 78, 23]]

    run_command("DROP TABLE test_partition_transform_time", pg_conn)
    pg_conn.commit()


# type uuid does not exist in spark
def test_partition_transform_uuid(
    pg_conn,
    create_helper_functions,
    with_default_location,
):
    run_command(
        "create table test_partition_transform_uuid(a uuid) using iceberg with (partition_by = 'a, bucket(100, a)');",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        "insert into test_partition_transform_uuid values ('cc791afe-837f-44fb-80cc-9f85226989ac'), ('57654f58-7a49-4836-be9c-2f9fd12ea1d8');",
        pg_conn,
    )
    pg_conn.commit()

    result = run_query(
        "select transformed.* from test_partition_transform_uuid t, lateral lake_table.partition_tuple(t.*) as transformed(a uuid, a_bucket_100 int4) order by a, a_bucket_100;",
        pg_conn,
    )

    assert result == [
        ["57654f58-7a49-4836-be9c-2f9fd12ea1d8", 66],
        ["cc791afe-837f-44fb-80cc-9f85226989ac", 20],
    ]

    run_command("DROP TABLE test_partition_transform_uuid", pg_conn)
    pg_conn.commit()


def test_same_partition_transform(
    pg_conn,
    with_default_location,
):
    error = run_command(
        "create table test_same_partition_transform(a int) using iceberg with (partition_by = 'bucket(100, a), bucket(2, a)');",
        pg_conn,
        raise_error=False,
    )

    assert "appears multiple times" in error

    pg_conn.rollback()

    error = run_command(
        "create table test_same_partition_transform(a date) using iceberg with (partition_by = 'day(a), day(a)');",
        pg_conn,
        raise_error=False,
    )

    assert "appears multiple times" in error

    pg_conn.rollback()


def insert_random_rows(spark_session, pg_conn, n_rows):
    pg_values = ""
    spark_values = ""
    for row_id in range(0, n_rows):
        random_pg_col_vals = []
        random_spark_col_vals = []
        col_names = []

        for col_name, col_type in PG_COLUMNS:
            random_pg_col_val, random_spark_col_val = generate_random_value(col_type)
            random_pg_col_vals.append(random_pg_col_val)
            random_spark_col_vals.append(random_spark_col_val)
            col_names.append(col_name)

        pg_values += f"({', '.join(random_pg_col_vals)})"
        spark_values += f"({', '.join(random_spark_col_vals)})"

        if row_id < n_rows - 1:
            pg_values += ", "
            spark_values += ", "

    run_command(
        f"INSERT INTO test_apply_partition_transform({', '.join(col_names)}) VALUES {pg_values};",
        pg_conn,
    )
    pg_conn.commit()

    spark_session.sql(
        f"INSERT INTO public.test_apply_partition_transform({', '.join(col_names)}) VALUES {spark_values};"
    )


@pytest.fixture(scope="function")
def transforms_for_pg_and_spark(
    installcheck,
    pg_conn,
    spark_session,
    spark_table_metadata_location,
    transforms_for_pg_lake_test_table,
):
    if installcheck:
        yield
        return

    # insert random rows into pg and spark table
    insert_random_rows(spark_session, pg_conn, TOTAL_ROWS)

    # all min values
    run_command(
        """INSERT INTO test_apply_partition_transform VALUES (
            -32768, -2147483648, -9223372036854775808, '', '', '', '', '', '\\x00',
            1E-37, 1E-307, false, '0001-01-01', '0001-01-01 00:00:00', '1911-01-01 00:00:00+00', 0
        );""",
        pg_conn,
    )
    pg_conn.commit()

    spark_session.sql(
        """INSERT INTO public.test_apply_partition_transform VALUES (
            -32768, -2147483648, -9223372036854775808, '', '', '', '', ' ', X'00',
            1E-37, 1E-307, false, date '0001-01-01', timestamp_ntz '0001-01-01 00:00:00', timestamp '1911-01-01 00:00:00+00', 0
        );"""
    )

    # all max values
    run_command(
        """INSERT INTO test_apply_partition_transform VALUES (
            32767, 2147483647, 9223372036854775807, 'hello \"world', 'hello \"world', 'hello \"world'::varchar(1), 'hello \"world', 'z', '\\xae0203',
            1E+10, 1E+308, true, '9999-12-31', '9999-12-31 23:59:59', '9998-12-31 23:59:59+00', 9
        );""",
        pg_conn,
    )
    pg_conn.commit()

    spark_session.sql(
        """INSERT INTO public.test_apply_partition_transform VALUES (
            32767, 2147483647, 9223372036854775807, 'hello \"world', 'hello \"world', 'h', 'hello \"world', 'z', X'ae0203',
            1E+10, 1E+308, true, date '9999-12-31', timestamp_ntz '9999-12-31 23:59:59', timestamp '9998-12-31 23:59:59+00', 9
        );"""
    )

    # edge values
    run_command(
        """INSERT INTO test_apply_partition_transform VALUES (
            -1, NULL, NULL, '💰 stri💰ng w💰ith a 💰 surrog💰ate pair: 💰', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '2024-02-29', '2024-02-29 00:00:00', '2024-02-29 00:00:00+00', NULL
        ),
        (
            -2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '1969-12-31', '1969-12-31 00:00:00', '1969-12-31 00:00:00+00', NULL
        ),
        (
            -3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '1970-01-01', '1970-01-01 00:59:59', '1970-01-01 00:59:59+00', NULL
        );""",
        pg_conn,
    )
    pg_conn.commit()

    spark_session.sql(
        """INSERT INTO public.test_apply_partition_transform VALUES (
            -1, NULL, NULL, '💰 stri💰ng w💰ith a 💰 surrog💰ate pair: 💰', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, date '2024-02-29', timestamp_ntz '2024-02-29 00:00:00', timestamp '2024-02-29 00:00:00+00', NULL
        ),
        (
            -2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, date '1969-12-31', timestamp_ntz '1969-12-31 00:00:00', timestamp '1969-12-31 00:00:00+00', NULL
        ),
        (
            -3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, date '1970-01-01', timestamp_ntz '1970-01-01 00:59:59', timestamp '1970-01-01 00:59:59+00', NULL
        );"""
    )

    # all nulls
    run_command("insert into test_apply_partition_transform values (default);", pg_conn)
    pg_conn.commit()

    spark_session.sql(
        """INSERT INTO public.test_apply_partition_transform VALUES (
            NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
        )"""
    )

    yield transforms_for_pg_lake_test_table


@pytest.fixture(scope="function")
def transforms_for_pg_lake_test_table(pg_conn, s3, extension, with_default_location):
    transform_cols = ""
    partition_by_text = ""
    create_table_command = "CREATE TABLE test_apply_partition_transform ("

    for i, (column_name, column_type) in enumerate(PG_COLUMNS):
        create_table_command += f"{column_name} {column_type}"
        partition_by_text += f"{column_name}"
        transform_cols += f"{column_name} {column_type}"

        if i < len(PG_COLUMNS) - 1:
            create_table_command += ", "
            partition_by_text += ", "
            transform_cols += ", "

    # add truncate transforms for applicable columns
    for column_name, column_type in PG_COLUMNS:
        if column_type in [
            "int2",
            "int4",
            "int8",
            "text",
            "varchar",
            "varchar(1)",
            "bpchar",
            "char(1)",
            "bytea",
        ]:
            partition_by_text += f", truncate({RANDOM_TRUNCATE_LENGTH}, {column_name})"
            transform_cols += f", {column_name}_trunc_{RANDOM_TRUNCATE_LENGTH} {column_type if column_type != 'int2' else 'int4'}"

    # add year, month, day and hour transforms for applicable columns
    for column_name, column_type in PG_COLUMNS:
        if column_type in ["date", "timestamp", "timestamptz"]:
            partition_by_text += (
                f", year({column_name}), month({column_name}), day({column_name})"
            )
            transform_cols += f", {column_name}_year int4, {column_name}_month int4, {column_name}_day int4"

        if column_type in ["timestamp", "timestamptz"]:
            partition_by_text += f", hour({column_name})"
            transform_cols += f", {column_name}_hour int4"

    # add bucket transforms for applicable columns
    for column_name, column_type in PG_COLUMNS:
        if column_type in [
            "int2",
            "int4",
            "int8",
            "text",
            "varchar",
            "varchar(1)",
            "bpchar",
            "char(1)",
            "bytea",
            "date",
            "timestamp",
            "timestamptz",
            RANDOM_NUMERIC_TYPENAME,
        ]:
            partition_by_text += f", bucket({RANDOM_BUCKET_SIZE}, {column_name})"
            transform_cols += f", {column_name}_bucket_{RANDOM_BUCKET_SIZE} int4"

    create_table_command += (
        f") USING iceberg with (partition_by = '{partition_by_text}');"
    )

    run_command(create_table_command, pg_conn)

    pg_conn.commit()

    yield transform_cols

    run_command("DROP TABLE IF EXISTS test_apply_partition_transform;", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="function")
def spark_table_metadata_location(installcheck, spark_session):
    if installcheck:
        yield
        return

    partition_by_text = ""
    create_table_command = "CREATE TABLE public.test_apply_partition_transform ("

    for i, (column_name, column_type) in enumerate(SPARK_COLUMNS):
        create_table_command += f"{column_name} {column_type}"
        partition_by_text += f"{column_name}"

        if i < len(SPARK_COLUMNS) - 1:
            create_table_command += ", "
            partition_by_text += ", "

    create_table_command += f") USING iceberg PARTITIONED BY ({partition_by_text});"

    spark_session.sql(create_table_command)

    # add truncate transforms for applicable columns
    for column_name, column_type in SPARK_COLUMNS:
        if column_type in [
            "short",
            "int",
            "long",
            "string",
            "char(1)",
            "binary",
        ]:
            spark_session.sql(
                f"ALTER TABLE public.test_apply_partition_transform ADD PARTITION FIELD truncate({RANDOM_TRUNCATE_LENGTH}, {column_name})"
            )

    # add year, month, day and hour transforms for applicable columns
    for column_name, column_type in SPARK_COLUMNS:
        if column_type in ["date", "timestamp_ntz", "timestamp"]:
            spark_session.sql(
                f"ALTER TABLE public.test_apply_partition_transform ADD PARTITION FIELD year({column_name})"
            )
            spark_session.sql(
                f"ALTER TABLE public.test_apply_partition_transform ADD PARTITION FIELD month({column_name})"
            )
            spark_session.sql(
                f"ALTER TABLE public.test_apply_partition_transform ADD PARTITION FIELD day({column_name})"
            )

        if column_type in ["timestamp_ntz", "timestamp"]:
            spark_session.sql(
                f"ALTER TABLE public.test_apply_partition_transform ADD PARTITION FIELD hour({column_name})"
            )

    # add bucket transforms for applicable columns
    for column_name, column_type in SPARK_COLUMNS:
        if column_type in [
            "short",
            "int",
            "long",
            "string",
            "char(1)",
            "binary",
            "date",
            "timestamp_ntz",
            "timestamp",
            RANDOM_NUMERIC_TYPENAME,
        ]:
            spark_session.sql(
                f"ALTER TABLE public.test_apply_partition_transform ADD PARTITION FIELD bucket({RANDOM_BUCKET_SIZE}, {column_name})"
            )

    spark_table_metadata_location = (
        spark_session.sql(
            f"SELECT timestamp, file FROM public.test_apply_partition_transform.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )

    yield spark_table_metadata_location

    spark_session.sql(f"DROP TABLE IF EXISTS public.test_apply_partition_transform")


@pytest.fixture(scope="function")
def create_helper_functions(superuser_conn, s3, extension):

    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_table.partition_tuple(row_value RECORD)
        RETURNS SETOF RECORD
        LANGUAGE C STRICT
        AS 'pg_lake_table', $$get_partition_tuple$$;
        
        CREATE OR REPLACE FUNCTION lake_table.partition_summary(tablename regclass)
        RETURNS TABLE (
            sequence_number int,
            partition_field_id int,
            lower_bound text,
            upper_bound text
        )
        LANGUAGE C STRICT
        AS 'pg_lake_table', $$get_partition_summary$$;
""",
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    superuser_conn.rollback()

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION IF EXISTS lake_table.partition_tuple(RECORD);
        DROP FUNCTION IF EXISTS lake_table.partition_summary(regclass)
""",
        superuser_conn,
    )

    superuser_conn.commit()
