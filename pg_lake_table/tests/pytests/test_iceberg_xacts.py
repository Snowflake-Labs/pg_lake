import pytest
import psycopg2
from utils_pytest import *
from helpers.spark import *
import json
import re

from test_writable_iceberg_common import *


@pytest.mark.parametrize(
    "manifest_min_count_to_merge, target_manifest_size_kb, max_snapshot_age_params ",
    manifest_snapshot_settings,
)
def test_writable_iceberg_table_tx(
    installcheck,
    install_iceberg_to_duckdb,
    spark_session,
    pg_conn,
    duckdb_conn,
    s3,
    extension,
    manifest_min_count_to_merge,
    target_manifest_size_kb,
    max_snapshot_age_params,
    create_iceberg_table,
    with_default_location,
):
    run_command(
        f"""
            SET pg_lake_iceberg.manifest_min_count_to_merge TO {manifest_min_count_to_merge};
            SET pg_lake_iceberg.target_manifest_size_kb TO {target_manifest_size_kb};
            SET pg_lake_iceberg.max_snapshot_age TO {max_snapshot_age_params};

        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
                CREATE SCHEMA test_writable_iceberg_table_tx;
                """,
        pg_conn,
    )
    pg_conn.commit()

    agg_query_table_1 = """SELECT
                            COUNT(*) AS total_rows,
                            COUNT(DISTINCT name) AS distinct_names,
                            MAX(value) AS max_value,
                            MIN(value) AS min_value
                        FROM table1;"""
    agg_query_table_2 = """
                        SELECT
                            COUNT(*) AS total_rows,
                            COUNT(DISTINCT description) AS distinct_descriptions,
                            MAX(price) AS max_price,
                            MIN(price) AS min_price,
                            AVG(price) AS avg_price,
                            SUM(quantity) AS total_quantity
                        FROM table2;"""

    run_command("BEGIN;", pg_conn)
    run_command("SET search_path TO test_writable_iceberg_table_tx;", pg_conn)

    # should be able to create a writable table with location
    run_command(
        f"""CREATE TABLE table1 (
                    id INT,
                    name VARCHAR(50),
                    value DECIMAL(10, 2),
                    created_at TIMESTAMP
                ) USING pg_lake_iceberg;

                CREATE TABLE table2 (
                    id INT,
                    description TEXT,
                    quantity INT,
                    price DECIMAL(10, 2),
                    updated_at TIMESTAMP
                ) USING pg_lake_iceberg;

            -- Single Row Insert
            INSERT INTO table1 (id, name, value, created_at)
            VALUES (1, 'Item A', 100.00, NOW());

            INSERT INTO table2 (id, description, quantity, price, updated_at)
            VALUES (1, 'Description A', 10, 15.00, NOW());

            -- Multi-Row Insert
            INSERT INTO table1 (id, name, value, created_at)
            VALUES
            (2, 'Item B', 200.00, NOW()),
            (3, 'Item C', 300.00, NOW()),
            (4, 'Item D', 400.00, NOW());

            -- Generate and Insert 10,000 Rows
            INSERT INTO table1 (id, name, value, created_at)
            SELECT
                generate_series(5, 10004) AS id,
                'Item ' || generate_series(5, 10004) AS name,
                random() * 1000 AS value,
                NOW() - (generate_series(5, 10004) || ' days')::interval AS created_at;

            INSERT INTO table2 (id, description, quantity, price, updated_at)
            SELECT
                generate_series(5, 10004) AS id,
                'Description ' || generate_series(5, 10004) AS description,
                (random() * 100)::int AS quantity,
                random() * 50 AS price,
                NOW() - (generate_series(5, 10004) || ' days')::interval AS updated_at;
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    run_command(
        f"""
            -- Single Row Update
            UPDATE table1
            SET value = value + 50
            WHERE id = 1;
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    run_command(
        f"""
            -- Batch Update
            UPDATE table2
            SET price = price * 1.1
            WHERE quantity > 50;
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    run_command(
        f"""
            -- Single Row Delete
            DELETE FROM table1
            WHERE id = 2;
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    run_command(
        f"""
            -- Batch Delete
            DELETE FROM table2
            WHERE quantity < 20;
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    run_command(
        f"""
            -- Complex Update with CTE
            WITH updated_values AS (
                SELECT id, value * 1.05 AS new_value
                FROM table1
                WHERE id BETWEEN 100 AND 200
            )
            UPDATE table1
            SET value = uv.new_value
            FROM updated_values uv
            WHERE table1.id = uv.id;
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    run_command(
        f"""
            -- Complex Insert with CTE
            WITH new_data AS (
                SELECT generate_series(10005, 10014) AS id,
                       'New Item ' || generate_series(10005, 10014) AS name,
                       random() * 1000 AS value,
                       NOW() AS created_at
            )
            INSERT INTO table1 (id, name, value, created_at)
            SELECT id, name, value, created_at
            FROM new_data;
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    run_command(
        f"""
            -- Complex Delete with CTE
            WITH to_delete AS (
                SELECT id
                FROM table2
                WHERE price < 10
            )
            DELETE FROM table2
            WHERE id IN (SELECT id FROM to_delete);
    """,
        pg_conn,
    )
    pg_conn.commit()

    compare_results_with_reference_iceberg_implementations(
        installcheck,
        pg_conn,
        duckdb_conn,
        spark_session,
        "table1",
        None,
        agg_query_table_1,
    )

    # trigger snapshot retention
    vacuum_commands = [
        "SET search_path TO test_writable_iceberg_table_tx",
        f"SET pg_lake_iceberg.manifest_min_count_to_merge TO {manifest_min_count_to_merge};",
        f"SET pg_lake_iceberg.target_manifest_size_kb TO {target_manifest_size_kb}",
        f"SET pg_lake_iceberg.max_snapshot_age TO {max_snapshot_age_params};",
        "VACUUM table1, table2",
    ]
    run_command_outside_tx(vacuum_commands)

    run_command("DROP SCHEMA test_writable_iceberg_table_tx CASCADE", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
            RESET pg_lake_iceberg.manifest_min_count_to_merge;
            RESET pg_lake_iceberg.target_manifest_size_kb;
            RESET pg_lake_iceberg.max_snapshot_age;
        """,
        pg_conn,
    )
    pg_conn.commit()
