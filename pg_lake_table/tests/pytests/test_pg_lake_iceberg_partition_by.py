import pytest
from collections import namedtuple
from utils_pytest import *
from pyiceberg.table import StaticTable


SpecTransform = namedtuple(
    "SpecTransform",
    [
        "transform_name",
        "partition_field_name",
        "partition_field_id",
        "source_id",
        "source_ids",
    ],
)


COLUMN_TRANSFORMS = [
    # identity only
    ("col_short", [SpecTransform("identity", "col_short", 1000, 1, [1])]),
    ("col_int", [SpecTransform("identity", "col_int", 1000, 2, [2])]),
    ("col_bigint", [SpecTransform("identity", "col_bigint", 1000, 3, [3])]),
    ("col_text", [SpecTransform("identity", "col_text", 1000, 4, [4])]),
    ("col_binary", [SpecTransform("identity", "col_binary", 1000, 5, [5])]),
    ("col_date", [SpecTransform("identity", "col_date", 1000, 6, [6])]),
    ("col_ts", [SpecTransform("identity", "col_ts", 1000, 7, [7])]),
    ("col_time", [SpecTransform("identity", "col_time", 1000, 8, [8])]),
    ("col_bool", [SpecTransform("identity", "col_bool", 1000, 9, [9])]),
    # there are 3 generated columns in between
    ("col_float", [SpecTransform("identity", "col_float", 1000, 13, [13])]),
    ("col_double", [SpecTransform("identity", "col_double", 1000, 14, [14])]),
    ("col_uuid", [SpecTransform("identity", "col_uuid", 1000, 15, [15])]),
    ("col_varchar", [SpecTransform("identity", "col_varchar", 1000, 17, [17])]),
    ("col_bpchar", [SpecTransform("identity", "col_bpchar", 1000, 18, [18])]),
    # bucket only
    (
        "bucket(4,col_short)",
        [SpecTransform("bucket[4]", "col_short_bucket_4", 1000, 1, [1])],
    ),
    (
        "bucket(5,col_short)",
        [SpecTransform("bucket[5]", "col_short_bucket_5", 1000, 1, [1])],
    ),
    (
        "bucket(4,col_int)",
        [SpecTransform("bucket[4]", "col_int_bucket_4", 1000, 2, [2])],
    ),
    (
        "bucket(12,col_bigint)",
        [SpecTransform("bucket[12]", "col_bigint_bucket_12", 1000, 3, [3])],
    ),
    (
        "bucket(4,col_text)",
        [SpecTransform("bucket[4]", "col_text_bucket_4", 1000, 4, [4])],
    ),
    (
        "bucket(4,col_binary)",
        [SpecTransform("bucket[4]", "col_binary_bucket_4", 1000, 5, [5])],
    ),
    (
        "bucket(4,col_date)",
        [SpecTransform("bucket[4]", "col_date_bucket_4", 1000, 6, [6])],
    ),
    (
        "bucket(4,col_ts)",
        [SpecTransform("bucket[4]", "col_ts_bucket_4", 1000, 7, [7])],
    ),
    (
        "bucket(4,col_time)",
        [SpecTransform("bucket[4]", "col_time_bucket_4", 1000, 8, [8])],
    ),
    (
        "bucket(345346,col_serial)",
        [SpecTransform("bucket[345346]", "col_serial_bucket_345346", 1000, 11, [11])],
    ),
    (
        "bucket(4,col_stored)",
        [SpecTransform("bucket[4]", "col_stored_bucket_4", 1000, 12, [12])],
    ),
    (
        "bucket(4,col_uuid)",
        [SpecTransform("bucket[4]", "col_uuid_bucket_4", 1000, 15, [15])],
    ),
    # truncate only
    (
        "truncate(4,col_short)",
        [SpecTransform("truncate[4]", "col_short_trunc_4", 1000, 1, [1])],
    ),
    (
        "truncate(5,col_short)",
        [SpecTransform("truncate[5]", "col_short_trunc_5", 1000, 1, [1])],
    ),
    (
        "truncate(4,col_int)",
        [SpecTransform("truncate[4]", "col_int_trunc_4", 1000, 2, [2])],
    ),
    (
        "truncate(99999,col_bigint)",
        [SpecTransform("truncate[99999]", "col_bigint_trunc_99999", 1000, 3, [3])],
    ),
    (
        "truncate(12,col_text)",
        [SpecTransform("truncate[12]", "col_text_trunc_12", 1000, 4, [4])],
    ),
    (
        "truncate(1,col_binary)",
        [SpecTransform("truncate[1]", "col_binary_trunc_1", 1000, 5, [5])],
    ),
    # year only
    ("year(col_date)", [SpecTransform("year", "col_date_year", 1000, 6, [6])]),
    ("year(col_ts)", [SpecTransform("year", "col_ts_year", 1000, 7, [7])]),
    # month only
    ("month(col_date)", [SpecTransform("month", "col_date_month", 1000, 6, [6])]),
    ("month(col_ts)", [SpecTransform("month", "col_ts_month", 1000, 7, [7])]),
    # day only
    ("day(col_date)", [SpecTransform("day", "col_date_day", 1000, 6, [6])]),
    ("day(col_ts)", [SpecTransform("day", "col_ts_day", 1000, 7, [7])]),
    # hour only
    ("hour(col_ts)", [SpecTransform("hour", "col_ts_hour", 1000, 7, [7])]),
    ("hour(col_time)", [SpecTransform("hour", "col_time_hour", 1000, 8, [8])]),
    # combinations with different applicable transforms and whitespaces and quotes
    (
        "col_short, col_int",
        [
            SpecTransform("identity", "col_short", 1000, 1, [1]),
            SpecTransform("identity", "col_int", 1001, 2, [2]),
        ],
    ),
    (
        "col_short, hour(col_time)",
        [
            SpecTransform("identity", "col_short", 1000, 1, [1]),
            SpecTransform("hour", "col_time_hour", 1001, 8, [8]),
        ],
    ),
    (
        "col_short, day(col_date)",
        [
            SpecTransform("identity", "col_short", 1000, 1, [1]),
            SpecTransform("day", "col_date_day", 1001, 6, [6]),
        ],
    ),
    (
        'truncate(2,col_short), hour( "col_time"    )',
        [
            SpecTransform("truncate[2]", "col_short_trunc_2", 1000, 1, [1]),
            SpecTransform("hour", "col_time_hour", 1001, 8, [8]),
        ],
    ),
    (
        'bucket(13, "col_ , ""?_,,\'\'")  ,day(col_date), truncate(4,col_binary), month(col_date)',
        [
            SpecTransform("bucket[13]", "col_ , \"?_,,'_bucket_13", 1000, 10, [10]),
            SpecTransform("day", "col_date_day", 1001, 6, [6]),
            SpecTransform("truncate[4]", "col_binary_trunc_4", 1002, 5, [5]),
            SpecTransform("month", "col_date_month", 1003, 6, [6]),
        ],
    ),
]


def test_ctas_partition_by(pg_conn, extension, s3, with_default_location):

    # first CTAS with partition by
    run_command(
        "create table test_create_iceberg_table_partition_by USING iceberg WITH (partition_by = 'i, truncate(10,i), truncate(2,j)') as select i, i%100 as j from generate_series(0,200)i;",
        pg_conn,
    )
    pg_conn.commit()

    metadata = table_metadata(pg_conn)
    assert metadata["default-spec-id"] == 1

    partition_specs = table_partition_specs(pg_conn)

    # we push multiple specs if table is created with partition_by
    # first one is empty spec, the second is partition spec
    assert len(partition_specs) == 2

    empty_spec = partition_specs[0]
    assert len(empty_spec["fields"]) == 0

    # there are 3 fields
    partition_by_spec = partition_specs[1]
    assert len(partition_by_spec["fields"]) == 3

    # now, make sure pyiceberg can see the partition spec properly
    assert_partition_metadata_via_py_iceberg(pg_conn)

    run_command("DROP TABLE test_create_iceberg_table_partition_by", pg_conn)
    pg_conn.commit()


def test_add_column_partition_by(pg_conn, extension, s3, with_default_location):

    # first create a table
    run_command(
        "create table test_create_iceberg_table_partition_by(a int, b int) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    metadata = table_metadata(pg_conn)
    assert metadata["default-spec-id"] == 0

    # now, add column and partition by using that column
    run_command(
        "ALTER TABLE test_create_iceberg_table_partition_by ADD COLUMN c timestamptz, OPTIONS (ADD partition_by 'a, b, year(c)');",
        pg_conn,
    )
    pg_conn.commit()

    metadata = table_metadata(pg_conn)
    assert metadata["default-spec-id"] == 1

    partition_specs = table_partition_specs(pg_conn)

    # we push a single spec but initial metadata also has an empty spec
    # so total of 2
    assert len(partition_specs) == 2

    # we are interested in the last spec
    spec = partition_specs[-1]

    # there are 3 fields
    assert len(spec["fields"]) == 3

    # now, make sure pyiceberg can see the partition spec properly
    assert_partition_metadata_via_py_iceberg(pg_conn)

    run_command("DROP TABLE test_create_iceberg_table_partition_by", pg_conn)
    pg_conn.commit()


def test_create_iceberg_table_partition_by_nonexistent_columns(
    pg_conn, extension, s3, with_default_location
):
    non_existent_columns = [
        "c",
        "hour(c)",
        "day(c)",
        "month(c)",
        "year(c)",
        "bucket(4,c)",
        "truncate(4,c)",
    ]

    for col in non_existent_columns:
        error = run_command(
            f"""
            CREATE TABLE test_create_iceberg_table_partition_by (a int, b text) USING iceberg WITH (partition_by = 'a,{col}');
        """,
            pg_conn,
            raise_error=False,
        )

        assert "does not exist" in error

        pg_conn.rollback()


def test_create_iceberg_table_partition_by_empty(
    pg_conn, extension, s3, with_default_location
):
    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a int) USING iceberg WITH (partition_by = '');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "empty" in error

    pg_conn.rollback()

    # empty with spaces
    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a int) USING iceberg WITH (partition_by = '    ');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "empty" in error

    pg_conn.rollback()

    # invalid comma
    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a int) USING iceberg WITH (partition_by = 'a, ');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "after comma" in error

    pg_conn.rollback()


def test_create_iceberg_table_partition_by_nested_type(
    pg_conn, extension, s3, with_default_location
):
    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a int[]) USING iceberg WITH (partition_by = 'a');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "must be a scalar type" in error

    pg_conn.rollback()

    error = run_command(
        f"""
        CREATE TYPE dog as (id int);
        CREATE TABLE test_create_iceberg_table_partition_by (a dog) USING iceberg WITH (partition_by = 'a');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "must be a scalar type" in error

    pg_conn.rollback()

    map_type = create_map_type("int", "int")

    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a map_type.key_int_val_int) USING iceberg WITH (partition_by = 'a');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "must be a scalar type" in error

    pg_conn.rollback()


def test_create_iceberg_table_partition_by_dropped_columns(
    pg_conn, extension, s3, with_default_location
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a int, b int, c int, d int) USING iceberg with (partition_by = 'a,c');
        ALTER TABLE test_create_iceberg_table_partition_by DROP COLUMN b;
        """,
        pg_conn,
    )

    # transform on existing columns
    run_command(
        f"""
        ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (SET partition_by 'a,c');
    """,
        pg_conn,
    )

    pg_conn.commit()

    # transform on dropped column should not be allowed
    error = run_command(
        f"""
        ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (SET partition_by 'b');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "not exist" in error

    pg_conn.rollback()

    # drop column d
    run_command(
        f"""
        ALTER TABLE test_create_iceberg_table_partition_by DROP COLUMN d;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # transform on existing columns
    run_command(
        f"""
        ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (SET partition_by 'a,c');
    """,
        pg_conn,
    )
    pg_conn.commit()

    # drop partition_by
    run_command(
        f"""
        ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (DROP partition_by);
    """,
        pg_conn,
    )
    pg_conn.commit()

    assert_partition_metadata_via_py_iceberg(pg_conn)

    # clean up
    run_command(
        f"""
        DROP TABLE test_create_iceberg_table_partition_by;
        """,
        pg_conn,
    )
    pg_conn.commit()


def test_create_iceberg_table_partition_by_void_transform(
    pg_conn, extension, s3, with_default_location
):
    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a int) USING iceberg WITH (partition_by = 'void(a)');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "void partition transform" in error

    pg_conn.rollback()


def test_create_iceberg_table_partition_by_unknown_transform(
    pg_conn, extension, s3, with_default_location
):
    error = run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (a int) USING iceberg WITH (partition_by = 'abcd(a)');
        """,
        pg_conn,
        raise_error=False,
    )

    assert "unknown" in error

    pg_conn.rollback()


def test_create_iceberg_table_partition_by_inapplicable_transform_types(
    pg_conn, extension, s3, with_default_location
):
    imapplicable_transform_types = [
        "col_json",
        "hour(col_short)",
        "day(col_int)",
        "month(col_text)",
        "year(col_binary)",
        "bucket(4,col_bool)",
        "truncate(4,col_bool)",
        "truncate(1231232134234324324,col_short)",
        "truncate(4,col_numeric)",
    ]

    for transform in imapplicable_transform_types:
        error = run_command(
            f"""
            CREATE TABLE test_create_iceberg_table_partition_by (col_short int2, col_int int4, col_bigint int8, col_text text, col_numeric numeric,
                                                                 col_binary bytea, col_date date, col_ts timestamp, col_time time, col_bool bool, col_json json
                                                                ) USING iceberg WITH (partition_by = '{transform}');
            """,
            pg_conn,
            raise_error=False,
        )

        assert "but type is" in error or "size must be between 1 and" in error

        pg_conn.rollback()


def test_create_iceberg_table_partition_by_duplicate_column_transforms(
    pg_conn, extension, s3, with_default_location
):
    duplicate_column_transforms = [
        "a, a",
        "hour(a), hour(a)",
        "day(a), day(a)",
        "month(a), month(a)",
        "year(a), year(a)",
        "bucket(4,b), bucket(4,b)",
        "truncate(4,b), truncate(4,b)",
    ]

    for transforms in duplicate_column_transforms:
        error = run_command(
            f"""
            CREATE TABLE test_create_iceberg_table_partition_by USING iceberg WITH (partition_by = '{transforms}')
            AS SELECT 'now()'::timestamptz a, 'abc'::text b;;
        """,
            pg_conn,
            raise_error=False,
        )

        assert "multiple times" in error

        pg_conn.rollback()


def test_create_iceberg_table_partition_by(
    pg_conn, extension, s3, with_default_location
):

    for transform_text, expected_spec in COLUMN_TRANSFORMS:
        run_command(
            f"""
            CREATE TABLE test_create_iceberg_table_partition_by (col_short int2, col_int int4, col_bigint int8, col_text text,
                                                                 col_binary bytea, col_date date, col_ts timestamp, col_time time, col_bool bool,
                                                                 "col_ , ""?_,,'" text, col_serial serial, col_stored int4 generated always as (col_short + 2) stored,
                                                                 col_float float4, col_double float8, col_uuid uuid, col_json json, col_varchar varchar, col_bpchar bpchar
                                                                ) USING iceberg WITH (partition_by = '{transform_text}');
        """,
            pg_conn,
        )
        pg_conn.commit()

        metadata = table_metadata(pg_conn)
        assert metadata["default-spec-id"] == 1

        # first, verify pyieberg and our metadata.json has
        # the same view of the partitions
        assert_partition_metadata_via_py_iceberg(pg_conn)

        partition_specs = table_partition_specs(pg_conn)

        # we push multiple specs if table is created with partition_by
        assert len(partition_specs) == 2

        last_spec = partition_specs[metadata["default-spec-id"]]

        # first spec is empty spec in metadata
        assert len(last_spec["fields"]) == len(expected_spec)

        # verify the partition spec
        for i, field in enumerate(last_spec["fields"]):
            assert field["name"] == expected_spec[i].partition_field_name
            assert field["field-id"] == expected_spec[i].partition_field_id
            assert field["source-id"] == expected_spec[i].source_id
            assert field["transform"] == expected_spec[i].transform_name
            assert field["source-ids"] == expected_spec[i].source_ids

        run_command("DROP TABLE test_create_iceberg_table_partition_by", pg_conn)
        pg_conn.commit()


def test_alter_iceberg_table_partition_by(
    pg_conn, extension, s3, with_default_location
):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (col_short int2, col_int int4, col_bigint int8, col_text text, 
                                                                 col_binary bytea, col_date date, col_ts timestamp, col_time time, col_bool bool,
                                                                 "col_ , ""?_,,'" text, col_serial serial, col_stored int4 generated always as (col_short + 2) stored,
                                                                 col_float float4, col_double float8, col_uuid uuid, col_json json, col_varchar varchar, col_bpchar bpchar
                                                                ) USING iceberg;
    """,
        pg_conn,
    )
    pg_conn.commit()

    # first, verify pyieberg and our metadata.json has
    # the same view of the partitions
    assert_partition_metadata_via_py_iceberg(pg_conn)

    partition_specs = table_partition_specs(pg_conn)

    assert partition_specs == [{"fields": [], "spec-id": 0}]

    current_expected_spec_count = 1

    # defined in the code as ICEBERG_PARTITION_FIELD_ID_START
    last_partition_id = 999

    current_expected_last_partition_field_id = 1000

    for spec_id, (transform_text, expected_spec) in enumerate(COLUMN_TRANSFORMS):
        run_command(
            f"""
            ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (
                {"ADD" if current_expected_spec_count == 1 else "SET"} partition_by '{transform_text}'
            );
            """,
            pg_conn,
        )
        pg_conn.commit()

        partition_specs = table_partition_specs(pg_conn)

        # first spec is empty spec in metadata
        assert len(partition_specs) == current_expected_spec_count + 1

        #  we always append a new spec after ALTER TABLE partition_by
        metadata = table_metadata(pg_conn)
        assert metadata["default-spec-id"] == current_expected_spec_count

        current_expected_spec_count += 1

        last_partition_id += len(expected_spec)
        assert metadata["last-partition-id"] == last_partition_id

        last_spec = partition_specs[-1]

        # first spec is empty spec in metadata
        assert len(last_spec["fields"]) == len(expected_spec)

        # verify the partition spec
        for i, field in enumerate(last_spec["fields"]):
            assert field["name"] == expected_spec[i].partition_field_name
            assert field["field-id"] == current_expected_last_partition_field_id
            assert field["source-id"] == expected_spec[i].source_id
            assert field["transform"] == expected_spec[i].transform_name
            assert field["source-ids"] == expected_spec[i].source_ids

            current_expected_last_partition_field_id += 1

    # try to set a disallowed partition transform
    error = run_command(
        f"""
        ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (SET partition_by 'hour(col_date)');
    """,
        pg_conn,
        raise_error=False,
    )

    assert "but type is" in error

    pg_conn.rollback()

    run_command("DROP TABLE test_create_iceberg_table_partition_by", pg_conn)
    pg_conn.commit()


def test_create_iceberg_table_partition_by_with_quoted_cols(
    pg_conn, extension, s3, with_default_location
):
    # quoted column name with different cases, spaces and special characters
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by ("cOL_ , ""?(_,,)'2" text) USING iceberg WITH (partition_by = '"cOL_ , ""?(_,,)''2"');
    """,
        pg_conn,
    )
    pg_conn.commit()

    assert_partition_metadata_via_py_iceberg(pg_conn)

    run_command("DROP TABLE test_create_iceberg_table_partition_by", pg_conn)
    pg_conn.commit()

    # not quoted column
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (cOL_2 text) USING iceberg WITH (partition_by = 'col_2');
    """,
        pg_conn,
    )
    pg_conn.commit()

    assert_partition_metadata_via_py_iceberg(pg_conn)

    run_command("DROP TABLE test_create_iceberg_table_partition_by", pg_conn)
    pg_conn.commit()

    # not quoted column with different cases
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (cOL_2 text) USING iceberg WITH (partition_by = 'cOL_2');
    """,
        pg_conn,
    )
    pg_conn.commit()

    assert_partition_metadata_via_py_iceberg(pg_conn)

    run_command("DROP TABLE test_create_iceberg_table_partition_by", pg_conn)
    pg_conn.commit()


# we use superuser_conn instead of granting access to the partition_specs/fields
# as this test only focus on thiose
def test_drop_partitioned_table(superuser_conn, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (col_int int4, col_bigint int8) USING iceberg WITH (partition_by = 'col_int, truncate(200, col_bigint)');
    """,
        superuser_conn,
    )

    run_command(
        "ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (SET partition_by 'col_bigint, truncate(200, col_int)')",
        superuser_conn,
    )
    superuser_conn.commit()

    # record the oid of the table before we drop
    res = run_query(
        "SELECT 'test_create_iceberg_table_partition_by'::regclass::oid", superuser_conn
    )
    table_id = res[0][0]

    # make sure metadata is there
    res = run_query(
        f"SELECT * FROM lake_table.partition_specs WHERE table_name = {table_id}",
        superuser_conn,
    )
    assert len(res) == 3

    res = run_query(
        f"SELECT * FROM lake_table.partition_fields WHERE table_name = {table_id}",
        superuser_conn,
    )
    assert len(res) == 4  # each spec with 2 fields

    run_command("DROP TABLE test_create_iceberg_table_partition_by", superuser_conn)

    res = run_query(
        f"SELECT * FROM lake_table.partition_specs WHERE table_name = {table_id}",
        superuser_conn,
    )
    assert len(res) == 0

    res = run_query(
        f"SELECT * FROM lake_table.partition_fields WHERE table_name = {table_id}",
        superuser_conn,
    )
    assert len(res) == 0

    superuser_conn.commit()


# we use superuser_conn instead of granting access to the partition_specs/fields
# as this test only focus on thiose
def test_re_add_partition_by(superuser_conn, with_default_location):
    run_command(
        f"""
        CREATE TABLE test_create_iceberg_table_partition_by (col_int int4, col_bigint int8) USING iceberg WITH (partition_by = 'col_int, truncate(200, col_bigint)');
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        "ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (SET partition_by 'col_bigint, truncate(200, col_int)')",
        superuser_conn,
    )
    superuser_conn.commit()

    # sanity check
    assert_partition_metadata_via_py_iceberg(superuser_conn)

    metadata = table_metadata(superuser_conn)
    prev_partition_id = metadata["last-partition-id"]

    # now, drop the partition_by, which is equivalent to pushing
    # an empty partition spec as the initial metadata
    run_command(
        "ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (DROP partition_by)",
        superuser_conn,
    )
    superuser_conn.commit()

    # sanity check
    assert_partition_metadata_via_py_iceberg(superuser_conn)

    # make sure metadata is there
    res = run_query(
        f"SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_create_iceberg_table_partition_by'::regclass",
        superuser_conn,
    )
    assert len(res) == 1
    spec_id = res[0][0]
    assert spec_id == 0

    # for non-partitioned case, we should not add partition_values
    run_command(
        "INSERT INTO test_create_iceberg_table_partition_by VALUES (1,1)",
        superuser_conn,
    )
    superuser_conn.commit()

    res = run_query(
        f"SELECT * FROM lake_table.data_file_partition_values JOIN lake_table.partition_fields USING(partition_field_id) WHERE spec_id = {spec_id}",
        superuser_conn,
    )
    assert len(res) == 0

    # dropping partition spec should not change the last-partition-d
    metadata = table_metadata(superuser_conn)
    assert prev_partition_id == metadata["last-partition-id"]

    # we shouldn't have any partition fields for this spec
    res = run_query(
        f"SELECT * FROM lake_table.partition_fields WHERE table_name = 'test_create_iceberg_table_partition_by'::regclass and spec_id = {spec_id}",
        superuser_conn,
    )
    assert len(res) == 0

    # now, adding back one more partition_by should be fine
    run_command(
        "ALTER TABLE test_create_iceberg_table_partition_by OPTIONS (ADD partition_by 'col_int, col_bigint')",
        superuser_conn,
    )
    superuser_conn.commit()

    # sanity check
    assert_partition_metadata_via_py_iceberg(superuser_conn)

    # now that we added a new partition_by with 2 identity transforms,
    # we should see the new "last-partition-id" should be incremented accordingly
    metadata = table_metadata(superuser_conn)
    assert prev_partition_id + 2 == metadata["last-partition-id"]

    # make sure metadata is there
    res = run_query(
        f"SELECT spec_id FROM lake_table.partition_specs WHERE table_name = 'test_create_iceberg_table_partition_by'::regclass ORDER BY spec_id DESC",
        superuser_conn,
    )
    assert len(res) == 4
    spec_id = res[0][0]
    assert spec_id == 3

    # we shouldn't have any partition fields for this spec
    res = run_query(
        f"SELECT * FROM lake_table.partition_fields WHERE table_name = 'test_create_iceberg_table_partition_by'::regclass and spec_id = {spec_id}",
        superuser_conn,
    )
    assert len(res) == 2

    run_command("DROP TABLE test_create_iceberg_table_partition_by", superuser_conn)
    superuser_conn.commit()


def get_partition_spec_via_py_iceberg(pg_conn):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_create_iceberg_table_partition_by'",
        pg_conn,
    )[0][0]

    table = StaticTable.from_metadata(
        metadata_location,
        properties={
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.region": TEST_AWS_REGION,
        },
    )

    return table.spec().fields


def assert_partition_metadata_via_py_iceberg(pg_conn):
    partition_spec = table_partition_spec(pg_conn)
    pyiceberg_fields = get_partition_spec_via_py_iceberg(pg_conn)

    partition_specs_fields = partition_spec["fields"]  # assuming last spec

    assert len(partition_specs_fields) == len(pyiceberg_fields)
    for custom_field, iceberg_field in zip(partition_specs_fields, pyiceberg_fields):
        assert custom_field["name"] == iceberg_field.name
        assert custom_field["field-id"] == iceberg_field.field_id
        assert custom_field["source-id"] == iceberg_field.source_id
        assert custom_field["transform"] == iceberg_field.transform.__str__().lower()


def table_metadata(pg_conn):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = 'test_create_iceberg_table_partition_by'",
        pg_conn,
    )[0][0]

    pg_query = f"SELECT * FROM lake_iceberg.metadata('{metadata_location}')"

    metadata = run_query(pg_query, pg_conn)[0][0]

    return metadata


def table_partition_specs(pg_conn):
    metadata = table_metadata(pg_conn)
    return metadata["partition-specs"]


def table_partition_spec(pg_conn):
    metadata = table_metadata(pg_conn)
    default_spec_id = metadata["default-spec-id"]
    return metadata["partition-specs"][default_spec_id]
