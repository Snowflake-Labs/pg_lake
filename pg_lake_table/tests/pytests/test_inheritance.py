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

    # So far we do not expect partitioned table queries to delegate
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

def test_schema_propagation(s3, pg_conn, extension, with_default_location):
    # 1. add column should propagate
    run_command(
        """
        create table parent (id bigint, value text) using iceberg;
        create table child1 (id bigint, value text) using iceberg;
        alter table child1 inherit parent;
        insert into parent values (1, 'parent');
        insert into child1 values (2, 'child1');
        alter table parent add column z int default 10;
        """,
        pg_conn,
    )

    result = run_query("select * from parent", pg_conn)
    assert len(result) == 2
    assert len(result[0]) == 3
    assert result[0]["z"] == 10

    result = run_query("select * from child1", pg_conn)
    assert len(result) == 1
    assert len(result[0]) == 3
    assert result[0]["z"] == 10

    pg_conn.rollback()


    # 2. rename column should propagate
    run_command(
        """
        create table parent (id bigint, old_name text) using iceberg;
        create table child1 (id bigint, old_name text) using iceberg;
        alter table child1 inherit parent;
        insert into parent values (1, 'parent');
        insert into child1 values (2, 'child1');
        alter table parent rename column old_name to new_name;
        """,
        pg_conn,
    )

    result = run_query("select id, new_name from parent order by id", pg_conn)
    assert len(result) == 2
    assert result[0]["new_name"] == "parent"

    result = run_query("select id, new_name from child1", pg_conn)
    assert len(result) == 1
    assert result[0]["new_name"] == "child1"

    pg_conn.rollback()
    
    
    # 3. drop column should not propagate
    run_command(
        """
        create table parent (id bigint, value text, z int) using iceberg;
        create table child1 (id bigint, value text, z int) using iceberg;
        alter table child1 inherit parent;
        insert into parent values (1, 'parent', 10);
        insert into child1 values (2, 'child1', 20);
        alter table parent drop column z;
        """,
        pg_conn,
    )

    result = run_query("select * from parent", pg_conn)
    assert len(result) == 2
    assert len(result[0]) == 2

    result = run_query("select * from child1", pg_conn)
    assert len(result) == 1
    assert len(result[0]) == 3

    pg_conn.rollback()
