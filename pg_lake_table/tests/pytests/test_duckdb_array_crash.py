import pytest
import psycopg2
from utils_pytest import *

# verifies the fix of https://github.com/Snowflake-Labs/pg_lake/issues/18


def test_duckdb_array_crash(extension, pg_conn, s3):
    # setup test tables
    location = f"s3://{TEST_BUCKET}/test_duckdb_array_crash/"

    run_command(
        f"""
    DROP TABLE IF EXISTS t1, t2;

    SET pg_lake_iceberg.default_location_prefix = '{location}';

    CREATE TABLE t1 (
        id SERIAL,
        name TEXT,
        shipper_account_id INT,
        created_at TIMESTAMP DEFAULT now()
    ) USING iceberg;

    CREATE TABLE t2 (
        id SERIAL,
        company_name TEXT,
        hierarchy_array TEXT[]
    ) USING iceberg;

    RESET pg_lake_iceberg.default_location_prefix;

    -- Insert a few thousand rows
    INSERT INTO t1 (name, shipper_account_id)
    SELECT
        'order_' || g,
        (random() * 5000)::INT
    FROM generate_series(1, 5000) AS g;

    INSERT INTO t2 (company_name, hierarchy_array)
    SELECT
        'shipper_' || g,
        ARRAY[ '0H' || (10000 + g)::TEXT, (10000 + g)::TEXT ]
    FROM generate_series(1, 5000) AS g;


    ALTER TABLE t1 ADD COLUMN hierarchy_array TEXT[];

    -- Update existing rows
    UPDATE t1
    SET hierarchy_array = ARRAY[
        '0H' || (10000 + id)::TEXT,
        (id % 5)::TEXT
    ];


    -- Insert new rows with different array lengths
    INSERT INTO t1 (name, shipper_account_id, hierarchy_array)
    SELECT
        'new_order_' || g,
        (random() * 5000)::INT,
        ARRAY[
            '0H' || (90000 + g)::TEXT,
            'EXTRA' || (g % 10)::TEXT,
            CASE WHEN g % 2 = 0 THEN 'ALT' ELSE NULL END
        ]
    FROM generate_series(5001, 5200) AS g;

    -- Update some random rows
    UPDATE t1
    SET hierarchy_array = ARRAY['0H11317', '1234567']
    WHERE id % 37 = 0;
    """,
        pg_conn,
    )

    res = None
    try:
        res = run_query(
            """
        SELECT COUNT(*)
        FROM t1
        WHERE hierarchy_array && ARRAY['0H11317', '999999'];
        """,
            pg_conn,
        )

    except:
        pass

    assert res is not None and res[0][0] > 0

    pg_conn.rollback()
