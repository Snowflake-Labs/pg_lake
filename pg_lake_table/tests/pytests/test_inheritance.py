import pytest
import json
import xml.etree.ElementTree as ET
import yaml
from utils_pytest import *


def test_inherits(s3, pg_conn, extension, with_default_location):
    url = f"s3://{TEST_BUCKET}/inherits_tables/numbers.parquet"

    run_command(
        f"""
        -- parent table with 10 rows and an Iceberg family
        create table parent using iceberg as select s x, s y from generate_series(1,10) s;
        create table child (like parent) inherits (parent) using iceberg;
        create table grandchild () inherits (child) using iceberg;
        create table empty_grandchild () inherits (child) using iceberg;

        -- add 5 rows to each child
        insert into child select s, s from generate_series(11, 15) s;
        insert into grandchild select s, s from generate_series(21, 25) s;

        -- Parquet sibling with 10 rows
        copy (select s x, s y from generate_series(21, 30) s) to '{url}';
        create foreign table child_parquet () server pg_lake options (path '{url}');
        alter table child_parquet inherit parent;
    """,
        pg_conn,
    )

    result = run_query(
        f"""
        select count(*) from parent
    """,
        pg_conn,
    )
    assert result[0]["count"] == 30

    # Should delegate into DuckDB
    result = run_query(
        f"""
        explain (format 'json') select * from parent
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert full_plan["Node Type"] == "Custom Scan"
    assert duckdb_plan["Node Type"] == "UNION"

    # Joins are fine
    result = run_query(
        f"""
        select count(*) from child join child_parquet using (x);
    """,
        pg_conn,
    )
    assert result[0]["count"] == 5

    # Should delegate into DuckDB
    result = run_query(
        f"""
        explain (format 'json') select count(*) from child join child_parquet using (x);
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert full_plan["Node Type"] == "Custom Scan"
    assert duckdb_plan["Node Type"] == "UNGROUPED_AGGREGATE"

    pg_conn.rollback()


def test_partitioning(s3, pg_conn, extension, with_default_location):
    url = f"s3://{TEST_BUCKET}/partitioning_tables/numbers.parquet"

    run_command(
        f"""
        -- parent table with 10 rows and an Iceberg family
        create table parent (x int, y int) partition by range (x);
        create table child1 partition of parent for values from (1) to (10) using iceberg;
        create table child2 partition of parent default using iceberg;

        insert into parent select s, s from generate_series(1, 20) s;
    """,
        pg_conn,
    )

    result = run_query(
        f"""
        select count(*) from parent
    """,
        pg_conn,
    )
    assert result[0]["count"] == 20

    # A partitioned table whose partitions are all lake tables holds no data of
    # its own, so it delegates as the UNION of its partitions, like an
    # inheritance tree does
    result = run_query(
        f"""
        explain (format 'json') select * from parent
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert full_plan["Node Type"] == "Custom Scan"
    assert duckdb_plan["Node Type"] == "UNION"

    # ... unless whole-query pushdown is off, in which case Postgres scans the
    # partitions one by one
    run_command("SET LOCAL pg_lake_table.enable_full_query_pushdown TO false", pg_conn)
    result = run_query(
        f"""
        explain (format 'json') select * from parent
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    assert full_plan["Node Type"] != "Custom Scan"

    pg_conn.rollback()


def test_pg_child(s3, pg_conn, extension, with_default_location):

    run_command(
        f"""
        -- parent with a regular table as child
        create table parent (x int, y int) using iceberg;
        create table child (like parent) inherits (parent);

        insert into parent values (1,1);
        insert into child values (2,2);
    """,
        pg_conn,
    )

    result = run_query(
        f"""
        select * from parent order by x
    """,
        pg_conn,
    )
    assert result[0]["x"] == 1
    assert result[1]["x"] == 2

    # Should not delegate into DuckDB
    result = run_query(
        f"""
        explain (format 'json') select * from parent
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert full_plan["Node Type"] == "Append"

    pg_conn.rollback()
