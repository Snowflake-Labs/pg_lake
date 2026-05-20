import pytest
import psycopg2
import time
import duckdb
import math
from utils_pytest import *
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    BinaryType,
    FixedType,
    NestedField,
    ListType,
    StructType,
)
import pyarrow
from datetime import datetime, date, timezone


def test_iceberg_pg_lake_table(pg_conn, cities_table, extension):
    run_command(
        f"""
        create foreign table cities ()
        server pg_lake
        options (path '{cities_table.metadata_location}')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'cities'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 3
    assert result == [
        ["city", "text"],
        ["lat", "double precision"],
        ["long", "double precision"],
    ]

    result = run_query("select * from cities order by city", pg_conn)
    assert len(result) == 4
    assert result[0]["city"] == "Amsterdam"

    # Test an empty result
    result = run_query("select a.lat from cities a where city = 'Den Helder'", pg_conn)
    assert len(result) == 0

    # Test a join query
    result = run_query(
        "select a.lat + b.lat from cities a join cities b on (a.city = b.city) where a.lat + b.long > 0 order by a.city",
        pg_conn,
    )
    assert len(result) == 3

    # Test an aggregation query
    result = run_query(
        "select city, count(*) from cities group by 1 order by 2 desc", pg_conn
    )
    assert len(result) == 4

    pg_conn.rollback()


def test_iceberg_nested_types(pg_conn, bids_table, extension):
    run_command(
        f"""
        create foreign table bids ()
        server pg_lake
        options (path '{bids_table.metadata_location}', FORMAT 'ICEBERG')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'bids'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert len(result) == 7
    assert result[0] == ["datetime", "timestamp without time zone"]
    assert result[1] == ["symbol", "text"]
    assert result[2] == ["bid", "real"]
    assert result[3] == ["ask", "double precision"]
    assert result[4][0] == "details"
    assert result[4][1].startswith("lake_struct.created_by")
    assert result[5] == ["data", "bytea"]
    assert result[6] == ["fixed_data", "bytea"]

    result = run_query("select (details).created_by from bids order by bid", pg_conn)
    assert len(result) == 4
    assert result[0]["created_by"] == "joe"
    assert result[1]["created_by"] == "john"

    pg_conn.rollback()


def test_iceberg_spark_types(pg_conn, spark_types_table, extension):
    run_command(
        f"""
        create foreign table test_types ()
        server pg_lake
        options (path '{spark_types_table.metadata_location}', FORMAT 'ICEBERG')
    """,
        pg_conn,
    )

    result = run_query(
        """
        select attname column_name, atttypid::regtype type_name from pg_attribute where attrelid = 'test_types'::regclass and attnum > 0 order by attnum
    """,
        pg_conn,
    )
    assert result == [
        ["int_col", "integer"],
        ["text_col", "text"],
        ["numeric_col", "numeric"],
        ["int_array_col", "integer[]"],
        ["boolean_col", "boolean"],
        ["date_col", "date"],
        ["timestamp_col", "timestamp with time zone"],
        ["struct_col", "lake_struct.field1_field2_4be86b8b"],
        ["bigint_col", "bigint"],
        ["decimal_col", "numeric"],
        ["real_col", "real"],
        ["uuid_col", "text"],
        ["float_col", "real"],
        ["smallint_col", "integer"],
        ["double_col", "double precision"],
        ["char_col", "text"],
        ["varchar_col", "text"],
    ]

    result = run_query(
        """
        select * from test_types
    """,
        pg_conn,
    )
    assert result == [
        [
            1,
            "Example text",
            Decimal("100.50"),
            [10, 20, 30],
            True,
            date(2024, 5, 1),
            datetime(2024, 5, 1, 6, 30, tzinfo=timezone.utc),
            "(Example,5)",
            999999999999999999,
            Decimal("789012.345678"),
            23456.79,
            "a0ec1234-5678-90ab-cdef-1234567890ab",
            12.345,
            12345,
            67890.12345,
            "C",
            "Another variable length string",
        ]
    ]

    pg_conn.rollback()


@pytest.fixture(scope="module")
def cities_table(iceberg_catalog):
    schema = Schema(
        NestedField(1, "city", StringType(), required=False),
        NestedField(2, "lat", DoubleType(), required=False),
        NestedField(3, "long", DoubleType(), required=False),
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.cities",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/cities",
    )

    data = pyarrow.Table.from_pylist(
        [
            {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
            {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
            {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
            {"city": "Paris", "lat": 48.864716, "long": 2.349014},
        ],
    )

    iceberg_table.append(data)

    return iceberg_table


@pytest.fixture(scope="module")
def bids_table(iceberg_catalog):
    schema = Schema(
        NestedField(
            field_id=1, name="datetime", field_type=TimestampType(), required=False
        ),
        NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
        NestedField(field_id=3, name="bid", field_type=FloatType(), required=False),
        NestedField(field_id=5, name="ask", field_type=DoubleType(), required=False),
        NestedField(
            field_id=6,
            name="details",
            field_type=StructType(
                NestedField(
                    field_id=7,
                    name="created_by",
                    field_type=StringType(),
                    required=False,
                ),
            ),
            required=False,
        ),
        NestedField(field_id=8, name="data", field_type=BinaryType(), required=False),
        NestedField(
            field_id=9, name="fixed_data", field_type=FixedType(16), required=False
        ),
    )

    iceberg_table = iceberg_catalog.create_table(
        identifier="public.bids",
        schema=schema,
        location=f"s3://{TEST_BUCKET}/iceberg/public/bids",
    )

    arrow_schema = pyarrow.schema(
        [
            pyarrow.field("datetime", pyarrow.timestamp("us")),
            pyarrow.field("symbol", pyarrow.string()),
            pyarrow.field("bid", pyarrow.float32()),
            pyarrow.field("ask", pyarrow.float64()),
            pyarrow.field(
                "details", pyarrow.struct([("created_by", pyarrow.string())])
            ),
            pyarrow.field("data", pyarrow.binary()),
            pyarrow.field("fixed_data", pyarrow.binary(16)),
        ]
    )

    data = pyarrow.Table.from_pylist(
        [
            {
                "datetime": datetime(2024, 1, 1),
                "symbol": "EUR",
                "bid": 12.4,
                "ask": 1.0,
                "details": {"created_by": "john"},
                "data": b"1",
                "fixed_data": b"1234567890123456",
            },
            {
                "datetime": datetime(2024, 1, 2),
                "symbol": "USD",
                "bid": 0.1,
                "ask": 1.1,
                "details": {"created_by": "joe"},
                "data": b"tt5678",
                "fixed_data": b"1234567890123456",
            },
            {
                "datetime": datetime(2024, 1, 3),
                "symbol": "EUR",
                "bid": 6000,
                "ask": 1.2,
                "details": {"created_by": "jack"},
                "data": b"90f12",
                "fixed_data": b"1234567890123456",
            },
            {
                "datetime": datetime(2024, 1, 4),
                "symbol": "USD",
                "bid": 6001,
                "ask": 1.3,
                "details": {"created_by": "jim"},
                "data": b"34asd56",
                "fixed_data": b"1234567890123456",
            },
        ],
        schema=arrow_schema,
    )

    iceberg_table.append(data)

    return iceberg_table


"""
Table created in spark-sql using:

spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1 \
    --conf spark.sql.catalog.my_catalog=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.my_catalog.catalog-impl=org.apache.iceberg.jdbc.JdbcCatalog \
    --conf spark.sql.catalog.my_catalog.warehouse=s3://testbucketcdw/iceberg/ \
    --conf spark.sql.catalog.my_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.my_catalog.uri=jdbc:postgresql://localhost:1600/postgres \
    --conf spark.sql.catalog.my_catalog.jdbc.verifyServerCertificate=false \
    --conf spark.sql.catalog.my_catalog.jdbc.useSSL=true \
    --conf spark.sql.catalog.my_catalog.jdbc.user=marco \
    --conf spark.sql.catalog.my_catalog.jdbc.password=pass

CREATE TABLE my_catalog.public.test_types (
    int_col INT,
    text_col STRING,
    numeric_col DECIMAL(10, 2),
    int_array_col ARRAY<INT>,
    boolean_col BOOLEAN,
    date_col DATE,
    timestamp_col TIMESTAMP,
    struct_col STRUCT<field1: STRING, field2: INT>,
    bigint_col BIGINT,
    decimal_col DECIMAL(20, 6),
    real_col REAL,
    uuid_col STRING,
    float_col FLOAT,
    smallint_col SMALLINT,
    double_col DOUBLE,
    char_col STRING,
    varchar_col STRING
) USING iceberg;

INSERT INTO my_catalog.public.test_types
VALUES (
    1,                                         -- int_col
    'Example text',                            -- text_col
    100.50,                                    -- numeric_col
    ARRAY(10, 20, 30),                         -- int_array_col
    true,                                      -- boolean_col
    DATE '2024-05-01',                         -- date_col
    TIMESTAMP '2024-05-01 08:30:00',           -- timestamp_col
    NAMED_STRUCT('field1', 'Example', 'field2', 5), -- struct_col
    999999999999999999,                        -- bigint_col
    789012.345678,                             -- decimal_col
    23456.789,                                 -- real_col
    'a0ec1234-5678-90ab-cdef-1234567890ab',    -- uuid_col
    12.345,                                    -- float_col
    12345,                                     -- smallint_col
    67890.12345,                               -- double_col
    'C',                                       -- char_col
    'Another variable length string'           -- varchar_col
);
"""


@pytest.fixture(scope="module")
def spark_types_table(iceberg_catalog, s3):
    iceberg_prefix = f"iceberg/public/test_types"
    iceberg_url = f"s3://{TEST_BUCKET}/{iceberg_prefix}"
    iceberg_path = sampledata_filepath("iceberg/test_types")

    # Upload data files
    for root, dirs, files in os.walk(iceberg_path + "/data"):
        for filename in files:
            s3.upload_file(
                os.path.join(root, filename),
                TEST_BUCKET,
                f"{iceberg_prefix}/data/{filename}",
            )
            print(
                f"uploading {os.path.join(root,filename)} to {iceberg_prefix}/data/{filename}"
            )

    # Upload metadata files
    for root, dirs, files in os.walk(iceberg_path + "/metadata"):
        for filename in files:
            s3.upload_file(
                os.path.join(root, filename),
                TEST_BUCKET,
                f"{iceberg_prefix}/metadata/{filename}",
            )
            print(
                f"uploading {os.path.join(root,filename)} to {iceberg_prefix}/metadata/{filename}"
            )

    return iceberg_catalog.register_table(
        "public.test_types",
        f"{iceberg_url}/metadata/00001-c591dd86-3c92-4d94-8a07-3eda38167148.metadata.json",
    )
