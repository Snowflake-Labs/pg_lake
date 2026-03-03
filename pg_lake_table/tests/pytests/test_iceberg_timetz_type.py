from utils_pytest import *
import json
import pytest
from datetime import time, timezone


def test_iceberg_timetz_as_utc_time(
    pg_conn, s3, extension, create_helper_functions, with_default_location
):
    """timetz columns are stored as Iceberg 'time' type, normalized to UTC."""

    schema_name = "test_iceberg_timetz"
    table_name = "timetz_table"
    location = f"s3://{TEST_BUCKET}/{schema_name}"

    run_command(f"CREATE SCHEMA {schema_name};", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
        CREATE TABLE {schema_name}.{table_name} (
            id INTEGER,
            t TIMETZ
        ) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # 1. Verify that the Iceberg metadata uses "time" (not "timetz")
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables "
        f"WHERE table_name = '{table_name}' AND table_namespace = '{schema_name}'",
        pg_conn,
    )
    assert len(results) == 1
    metadata_path = results[0][0]

    data = read_s3_operations(s3, metadata_path)
    parsed_data = json.loads(data)
    fields = parsed_data["schemas"][0]["fields"]

    time_field = next(f for f in fields if f["name"] == "t")
    assert (
        time_field["type"] == "time"
    ), f"Expected Iceberg type 'time' for timetz column, got '{time_field['type']}'"

    # 2. Insert rows with different timezone offsets
    run_command(
        f"""
        INSERT INTO {schema_name}.{table_name} VALUES
            (1, '12:30:00+00'),
            (2, '12:30:00+04'),
            (3, '23:30:00-02'),
            (4, '01:30:00+04'),
            (5, '00:00:00+00'),
            (6, '12:00:00.123456+00');
        """,
        pg_conn,
    )

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )

    utc = timezone.utc
    expected = [
        [1, time(12, 30, 0, tzinfo=utc)],  # 12:30:00+00 → 12:30:00+00
        [2, time(8, 30, 0, tzinfo=utc)],  # 12:30:00+04 → 08:30:00+00
        [3, time(1, 30, 0, tzinfo=utc)],  # 23:30:00-02 → 01:30:00+00
        [4, time(21, 30, 0, tzinfo=utc)],  # 01:30:00+04 → 21:30:00+00
        [5, time(0, 0, 0, tzinfo=utc)],  # 00:00:00+00 → 00:00:00+00
        [
            6,
            time(12, 0, 0, 123456, tzinfo=utc),
        ],  # 12:00:00.123456+00 → 12:00:00.123456+00
    ]

    assert results == expected

    # 3. COPY to csv file
    csv_path = f"s3://{TEST_BUCKET}/{schema_name}/timetz.csv"
    run_command(
        f"COPY (SELECT * FROM {schema_name}.{table_name}) TO '{csv_path}' WITH (header);",
        pg_conn,
    )

    run_command(f"TRUNCATE TABLE {schema_name}.{table_name};", pg_conn)

    # 4. INSERT..SELECT from another iceberg table
    run_command(
        f"CREATE TABLE {schema_name}.{table_name}_csv (id INTEGER, t TIMETZ) USING iceberg WITH (load_from = '{csv_path}');",
        pg_conn,
    )

    run_command(
        f"CREATE TABLE {schema_name}.{table_name}_copy (id INTEGER, t TIMETZ) USING iceberg;",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {schema_name}.{table_name}_copy SELECT * FROM {schema_name}.{table_name}_csv",
        pg_conn,
    )
    explain_results = run_query(
        f"EXPLAIN ANALYZE INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.{table_name}_copy",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(
        explain_results
    ), "INSERT..SELECT with TIMETZ column should be pushed down"

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )
    assert results == expected

    run_command(f"TRUNCATE TABLE {schema_name}.{table_name};", pg_conn)

    # 5. INSERT..SELECT from a pg_lake table
    run_command(
        f"CREATE FOREIGN TABLE {schema_name}.{table_name}_pg_lake (id INTEGER, t TIMETZ) SERVER pg_lake OPTIONS (format 'csv', header 'true', path '{csv_path}');",
        pg_conn,
    )

    explain_results = run_query(
        f"EXPLAIN ANALYZE INSERT INTO {schema_name}.{table_name} SELECT * FROM {schema_name}.{table_name}_pg_lake",
        pg_conn,
    )
    assert "Custom Scan (Query Pushdown)" in str(
        explain_results
    ), "INSERT..SELECT with TIMETZ column should be pushed down"

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )
    assert results == expected

    run_command(f"TRUNCATE TABLE {schema_name}.{table_name};", pg_conn)

    # 6. COPY FROM and verify data + pushdown
    run_command(
        f"COPY {schema_name}.{table_name} FROM '{csv_path}' WITH (header);",
        pg_conn,
    )

    results = run_query(
        f"SELECT id, t FROM {schema_name}.{table_name} ORDER BY id",
        pg_conn,
    )
    assert results == expected

    result = run_query(
        "SELECT pg_lake_last_copy_pushed_down_test() pushed_down",
        pg_conn,
    )
    assert result[0][
        "pushed_down"
    ], "COPY FROM with TIMETZ into Iceberg should be pushed down"

    # 7. timetz[] array round-trip: elements must come back at UTC (+00)
    array_table = f"{table_name}_array"
    run_command(
        f"""
        CREATE TABLE {schema_name}.{array_table} (
            id INTEGER,
            ts TIMETZ[]
        ) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO {schema_name}.{array_table} VALUES
            (1, ARRAY['12:30:00+00'::timetz, '12:30:00+04'::timetz]),
            (2, ARRAY['23:30:00-02'::timetz, '01:30:00+04'::timetz]),
            (3, ARRAY['00:00:00+00'::timetz, '12:00:00.123456+00'::timetz]);
        """,
        pg_conn,
    )

    results = run_query(
        f"SELECT id, ts FROM {schema_name}.{array_table} ORDER BY id",
        pg_conn,
    )

    expected_arrays = [
        [1, [time(12, 30, 0, tzinfo=utc), time(8, 30, 0, tzinfo=utc)]],
        [2, [time(1, 30, 0, tzinfo=utc), time(21, 30, 0, tzinfo=utc)]],
        [3, [time(0, 0, 0, tzinfo=utc), time(12, 0, 0, 123456, tzinfo=utc)]],
    ]

    assert (
        results == expected_arrays
    ), f"timetz[] round-trip failed: {results} != {expected_arrays}"

    # 8. DROP TABLES
    run_command(f"DROP SCHEMA {schema_name} CASCADE", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn, app_user):
    run_command(
        f"GRANT SELECT ON lake_iceberg.tables TO {app_user};",
        superuser_conn,
    )
    run_command(
        """
        CREATE OR REPLACE FUNCTION pg_lake_last_copy_pushed_down_test()
          RETURNS bool
          LANGUAGE C
        AS 'pg_lake_copy', $function$pg_lake_last_copy_pushed_down_test$function$;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        DROP FUNCTION IF EXISTS pg_lake_last_copy_pushed_down_test;
        REVOKE SELECT ON lake_iceberg.tables FROM {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()
