import pytest
import json
import xml.etree.ElementTree as ET
import yaml
from utils_pytest import *


def test_explain_json(pg_conn, explain_table):
    # Run a CustomScan query
    result = run_query(
        f"""
        EXPLAIN (format 'json') SELECT count(*) FROM explain
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert full_plan["Node Type"] == "Custom Scan"
    assert duckdb_plan["Node Type"] == "UNGROUPED_AGGREGATE"
    assert duckdb_plan["Aggregates"] == "count_star()"


def test_explain_json_foreign(pg_conn, explain_table):
    # Run a ForeignScan query
    result = run_query(
        f"""
        EXPLAIN (format 'json') SELECT to_postgres(count(*)) FROM explain
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert full_plan["Node Type"] == "Foreign Scan"
    assert duckdb_plan["Node Type"] == "UNGROUPED_AGGREGATE"
    assert duckdb_plan["Aggregates"] == "count_star()"


def test_explain_json_analyze(pg_conn, explain_table):
    # Run with analyze
    result = run_query(
        f"""
        EXPLAIN (analyze, format 'json') SELECT count(*) FROM explain
    """,
        pg_conn,
    )
    full_plan = result[0][0][0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert duckdb_plan["Node Type"] == "UNGROUPED_AGGREGATE"


def test_explain_prepared(pg_conn, explain_table):

    for pushdown in ["on", "off"]:

        # Explain a prepared statement
        run_command(
            f"""
            SET LOCAL pg_lake_table.enable_full_query_pushdown TO {pushdown}; 
            PREPARE lookup_{pushdown}(int) AS SELECT count(*) FROM explain WHERE s = $1;
        """,
            pg_conn,
        )

        for i in range(0, 7):
            result = run_query(
                f"""
                EXPLAIN (ANALYZE, format 'json') EXECUTE lookup_{pushdown}(3);
            """,
                pg_conn,
            )
            full_plan = result[0][0][0]["Plan"]
            duckdb_plan = full_plan["Plans"][0]
            assert duckdb_plan["Node Type"] == "UNGROUPED_AGGREGATE"
            assert duckdb_plan["Aggregates"] == "count_star()"

    run_command("RESET pg_lake_table.enable_full_query_pushdown", pg_conn)


def test_explain_delete(pg_conn, explain_table):
    # Run a CustomScan query
    result = run_query(
        f"""
        EXPLAIN (format 'json') DELETE FROM explain WHERE s < 500
    """,
        pg_conn,
    )
    modify_plan = result[0][0][0]["Plan"]
    assert modify_plan["Node Type"] == "ModifyTable"

    foreign_plan = modify_plan["Plans"][0]
    assert foreign_plan["Node Type"] == "Foreign Scan"

    duckdb_plan = foreign_plan["Plans"][0]
    assert duckdb_plan["Node Type"] == "PROJECTION"


def test_explain_file_counts(pg_conn, explain_table):

    results = run_query(
        f"""
        EXPLAIN (verbose, format 'json') SELECT count(*) FROM explain;
    """,
        pg_conn,
    )
    assert fetch_data_files_used(results) == "1"
    assert fetch_delete_files_used(results) == "0"

    results = run_query(
        f"""
        EXPLAIN (verbose, format 'json') UPDATE explain SET s = 15;
    """,
        pg_conn,
    )
    assert fetch_data_files_used(results) == "1"
    assert fetch_delete_files_used(results) == "0"

    # now two tables, one file each
    results = run_query(
        f"""
        EXPLAIN (verbose, format 'json') SELECT count(*) FROM explain t1 JOIN explain t2 on(true);
    """,
        pg_conn,
    )
    assert fetch_data_files_used(results) == "2"
    assert fetch_delete_files_used(results) == "0"

    # now do an actual delete
    run_command("DELETE FROM explain WHERE s = 1", pg_conn)

    results = run_query(
        f"""
        EXPLAIN (verbose, format 'json') SELECT count(*) FROM explain;
    """,
        pg_conn,
    )
    assert fetch_data_files_used(results) == "1"
    assert fetch_delete_files_used(results) == "1"

    results = run_query(
        f"""
        EXPLAIN (verbose, format 'json') UPDATE explain SET s = 15;
    """,
        pg_conn,
    )
    assert fetch_data_files_used(results) == "1"
    assert fetch_delete_files_used(results) == "1"

    # now two tables, one file each
    results = run_query(
        f"""
        EXPLAIN (verbose, format 'json') SELECT count(*) FROM explain t1 JOIN explain t2 on(true);
    """,
        pg_conn,
    )
    assert fetch_data_files_used(results) == "2"
    assert fetch_delete_files_used(results) == "2"


def test_explain_analyze_update(pg_conn, with_default_location):
    run_command(
        f"""
        CREATE TABLE explain_update USING iceberg AS SELECT 1 x, 2 y;
    """,
        pg_conn,
    )

    # Run explain analyze to actually execute an update
    result = run_query(
        f"""
        EXPLAIN (analyze, format 'json') UPDATE explain_update SET y = 3;
    """,
        pg_conn,
    )
    modify_plan = result[0][0][0]["Plan"]
    assert modify_plan["Node Type"] == "ModifyTable"

    foreign_plan = modify_plan["Plans"][0]
    assert foreign_plan["Node Type"] == "Foreign Scan"

    duckdb_plan = foreign_plan["Plans"][0]
    assert duckdb_plan["Node Type"] == "PROJECTION"
    assert duckdb_plan["Estimated Cardinality"] == "1"

    # Check whether the explain analyze did something
    result = run_query(
        f"""
        SELECT y FROM explain_update
    """,
        pg_conn,
    )
    assert result[0][0] == 3


def test_explain_text(pg_conn, explain_table):
    # Check xml validity
    result = run_query(
        f"""
        EXPLAIN SELECT count(*) FROM explain
    """,
        pg_conn,
    )
    assert any("Estimated Cardinality" in line[0] for line in result)


def test_explain_xml(pg_conn, explain_table):
    # Check xml validity
    result = run_query(
        f"""
        EXPLAIN (format 'xml') SELECT count(*) FROM explain
    """,
        pg_conn,
    )
    print(result[0][0])
    xml_plan = ET.fromstring(str(result[0][0]))
    assert (
        xml_plan.find(".//{http://www.postgresql.org/2009/explain}Function").text
        == "READ_PARQUET"
    )


def test_explain_yaml(pg_conn, explain_table):
    # Run a CustomScan query
    result = run_query(
        f"""
        EXPLAIN (format 'yaml') SELECT count(*) FROM explain
    """,
        pg_conn,
    )
    yaml_plan = yaml.safe_load(result[0][0])
    full_plan = yaml_plan[0]["Plan"]
    duckdb_plan = full_plan["Plans"][0]
    assert full_plan["Node Type"] == "Custom Scan"
    assert duckdb_plan["Node Type"] == "UNGROUPED_AGGREGATE"
    assert duckdb_plan["Aggregates"] == "count_star()"


def test_explain_explicit_cardinality(pg_conn, s3, with_default_location):
    run_command(
        f"""
        CREATE TABLE explicit_cardinality USING iceberg AS SELECT i FROM generate_series(0,100)i;
    """,
        pg_conn,
    )

    # Run explain analyze to actually execute an update
    result = run_query(
        f"""
        EXPLAIN (analyze, format 'json') SELECT * FROM explicit_cardinality;
    """,
        pg_conn,
    )
    custom_scan = result[0][0][0]["Plan"]
    assert custom_scan["Node Type"] == "Custom Scan"

    duckdb_plan = custom_scan["Plans"][0]
    assert duckdb_plan["Node Type"].strip() == "READ_PARQUET"
    assert duckdb_plan["Estimated Cardinality"] == "101"

    # first, delete one row, then check the plan with position deletes
    run_command(
        f"""
        DELETE FROM explicit_cardinality WHERE i = 0;
    """,
        pg_conn,
    )

    # now, delete one row
    result = run_query(
        f"""
        EXPLAIN (analyze, format 'json') SELECT * FROM explicit_cardinality;
    """,
        pg_conn,
    )

    custom_scan = result[0][0][0]["Plan"]
    assert custom_scan["Node Type"] == "Custom Scan"

    duckdb_plan = custom_scan["Plans"][0]

    # we believe this is a hard-coded ratio that DuckDB picks
    assert duckdb_plan["Node Type"] == "PROJECTION"
    assert duckdb_plan["Estimated Cardinality"] == "20"

    hash_join_plan = duckdb_plan["Plans"][0]["Plans"][0]
    assert hash_join_plan["Node Type"] == "HASH_JOIN"
    assert hash_join_plan["Estimated Cardinality"] == "101"

    source_read_parquet_plan = hash_join_plan["Plans"][0]
    assert source_read_parquet_plan["Node Type"].strip() == "READ_PARQUET"
    assert source_read_parquet_plan["Estimated Cardinality"] == "101"

    position_delete_projection_plan = hash_join_plan["Plans"][1]
    assert position_delete_projection_plan["Node Type"] == "PROJECTION"
    assert position_delete_projection_plan["Projections"] == '"row"(file_path, pos)'
    assert position_delete_projection_plan["Estimated Cardinality"] == "1"

    position_delete_read_parquet_plan = position_delete_projection_plan["Plans"][0]
    assert position_delete_read_parquet_plan["Node Type"].strip() == "READ_PARQUET"
    assert position_delete_read_parquet_plan["Estimated Cardinality"] == "1"


def test_explain_ctas(pg_conn):
    # EXPLAIN ANALYZE CTAS is broken right now because it does not go through ProcessUtility hook
    error = run_command(
        f"""
        EXPLAIN ANALYZE CREATE TABLE explain_update USING iceberg AS SELECT 1 x, 2 y;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "not supported" in error

    pg_conn.rollback()


def test_explain_not_vectorized_messages(s3, pg_conn, extension, with_default_location):
    run_command(
        "create schema test_explain_not_vectorized_messages; set search_path to test_explain_not_vectorized_messages;",
        pg_conn,
    )

    run_command("create table test2(a text);", pg_conn)
    run_command("create table test(a text) using iceberg;", pg_conn)

    # not shippable table and function
    result = run_query(
        """
            explain (verbose) SELECT * FROM test join test2 using(a) WHERE regexp_replace(a, 'o', '$', 1, 1, 'g') = 'm$o';
        """,
        pg_conn,
    )

    assert any("Not Vectorized Constructs:" in line[0] for line in result)

    assert any(": \tTable" in line[0] for line in result)
    assert any(
        "Description: test_explain_not_vectorized_messages.test2" in line[0]
        for line in result
    )

    assert any(": \tFunction" in line[0] for line in result)
    assert any(
        "Description: pg_catalog.regexp_replace(pg_catalog.text,pg_catalog.text,pg_catalog.text,integer,integer,pg_catalog.text)"
        in line[0]
        for line in result
    )

    # not shippable limit with ties
    result = run_query(
        """
            explain (verbose) SELECT * FROM test join test2 using(a) ORDER BY a FETCH FIRST 2 ROWS WITH TIES;
        """,
        pg_conn,
    )

    assert any(": \tSQL Syntax" in line[0] for line in result)
    assert any("Description: LIMIT with TIES clause" in line[0] for line in result)

    assert any(": \tTable" in line[0] for line in result)
    assert any(
        "Description: test_explain_not_vectorized_messages.test2" in line[0]
        for line in result
    )

    # not shippable empty target list
    result = run_query(
        """
            explain (verbose) SELECT FROM test;
        """,
        pg_conn,
    )

    assert any("1: \tSQL Syntax" in line[0] for line in result)
    assert any("Description: Empty target list" in line[0] for line in result)

    # not shippable with ordinality
    result = run_query(
        """
            explain (verbose) SELECT * FROM test, generate_series(1,10) WITH ORDINALITY AS i(i);
        """,
        pg_conn,
    )

    assert any("1: \tSQL Syntax" in line[0] for line in result)
    assert any("Description: WITH ORDINALITY clause" in line[0] for line in result)

    # not shippable unnest group by
    result = run_query(
        """
            explain (verbose) SELECT * FROM (test t1 join test t2 using (a)) t;
        """,
        pg_conn,
    )

    assert any("1: \tSQL Syntax" in line[0] for line in result)
    assert any(
        "Description: JOIN with merged columns and alias" in line[0] for line in result
    )

    # not shippable unnest in group by
    result = run_query(
        """
            explain(verbose) SELECT COUNT(*) FROM test, unnest(array['1','2']) GROUP BY unnest(array['1','2']);
        """,
        pg_conn,
    )

    assert any("1: \tSQL Syntax" in line[0] for line in result)
    assert any(
        "Description: UNNEST with GROUP BY or window function" in line[0]
        for line in result
    )

    # not shippable collation
    result = run_query(
        """
            explain(verbose) SELECT * FROM test where a COLLATE "POSIX" = 'test';
        """,
        pg_conn,
    )

    assert any("1: \tCollation" in line[0] for line in result)
    assert any('Description: pg_catalog."POSIX"' in line[0] for line in result)

    # not shippable sql value function
    result = run_query(
        """
            explain(verbose) SELECT *, current_user FROM test;
        """,
        pg_conn,
    )

    assert any("1: \tSQL Value Function" in line[0] for line in result)

    # not shippable type
    result = run_query(
        """
            explain(verbose) SELECT *, '<test></test>'::xml current_user FROM test;
        """,
        pg_conn,
    )

    assert any("1: \tType" in line[0] for line in result)
    assert any("Description: pg_catalog.xml" in line[0] for line in result)

    # not shippable function and type
    result = run_query(
        """
            explain(verbose) SELECT *, xmlcomment('todo') current_user FROM test;
        """,
        pg_conn,
    )

    assert any(": \tFunction" in line[0] for line in result)
    assert any(
        "Description: pg_catalog.xmlcomment(pg_catalog.text)" in line[0]
        for line in result
    )

    assert any(": \tType" in line[0] for line in result)
    assert any("Description: pg_catalog.xml" in line[0] for line in result)

    # not shippable operator and type
    result = run_query(
        """
            explain(verbose) SELECT * FROM test where '(1,2)'::point << '(3,4)'::point;
        """,
        pg_conn,
    )

    assert any(": \tType" in line[0] for line in result)
    assert any("Description: pg_catalog.point" in line[0] for line in result)

    assert any(": \tOperator" in line[0] for line in result)
    assert any(
        "Description: pg_catalog.<<(pg_catalog.point,pg_catalog.point)" in line[0]
        for line in result
    )

    # not shippable objects with subqueries
    result = run_query(
        """
            explain (verbose) SELECT * FROM ( SELECT *,random() FROM test join test2 using(a) ORDER BY a FETCH FIRST 2 ROWS WITH TIES) foo;
        """,
        pg_conn,
    )

    assert any(": \tSQL Syntax" in line[0] for line in result)
    assert any("Description: LIMIT with TIES clause" in line[0] for line in result)

    assert any(": \tTable" in line[0] for line in result)
    assert any(
        "Description: test_explain_not_vectorized_messages.test2" in line[0]
        for line in result
    )

    # not shippable map_type type
    map_type = create_map_type("int", "int")
    result = run_query(
        f"""
            explain(verbose) with test_with_map_count as (select array[(1,2)]::map_type.key_int_val_int as cnt, * from test)
                             SELECT * FROM test_with_map_count;
        """,
        pg_conn,
    )

    assert any(": \tType" in line[0] for line in result)
    assert any("Description: map_type.pair_int_int" in line[0] for line in result)

    # not shippable jsonb operator
    result = run_query(
        """
            explain(verbose) SELECT * FROM test where a::jsonb @> '{}'::jsonb;
        """,
        pg_conn,
    )

    assert any("1: \tOperator" in line[0] for line in result)
    assert any(
        "Description: pg_catalog.@>(pg_catalog.jsonb,pg_catalog.jsonb)" in line[0]
        for line in result
    )

    # not shippable table in insert select
    result = run_query(
        """
            explain (verbose) insert into test select * from test2;
        """,
        pg_conn,
    )

    assert any("1: \tTable" in line[0] for line in result)
    assert any(
        "Description: test_explain_not_vectorized_messages.test2" in line[0]
        for line in result
    )

    # not shippable collate at order by
    result = run_query(
        """
            explain (verbose) SELECT * FROM test ORDER BY a COLLATE "POSIX";
        """,
        pg_conn,
    )

    assert any("1: \tCollation" in line[0] for line in result)
    assert any('Description: pg_catalog."POSIX"' in line[0] for line in result)

    # not shippable collate at group by
    result = run_query(
        """
            explain (verbose) SELECT COUNT(*) FROM test GROUP BY a COLLATE "POSIX";
        """,
        pg_conn,
    )

    assert any("1: \tCollation" in line[0] for line in result)
    assert any('Description: pg_catalog."POSIX"' in line[0] for line in result)

    # not shippable collate at distinct
    result = run_query(
        """
            explain (verbose) SELECT DISTINCT a COLLATE "POSIX" FROM test;
        """,
        pg_conn,
    )

    assert any("1: \tCollation" in line[0] for line in result)
    assert any('Description: pg_catalog."POSIX"' in line[0] for line in result)

    # not shippable collate at window function
    result = run_query(
        """
            explain (verbose) SELECT a, ROW_NUMBER() OVER (PARTITION BY (a COLLATE "POSIX")) AS rownum FROM test;
        """,
        pg_conn,
    )

    assert any("1: \tCollation" in line[0] for line in result)
    assert any('Description: pg_catalog."POSIX"' in line[0] for line in result)

    # not shippable input collate at function
    result = run_query(
        """
            explain (verbose) SELECT lower(a COLLATE "POSIX") COLLATE "C" FROM test;
        """,
        pg_conn,
    )

    assert any("1: \tCollation" in line[0] for line in result)
    assert any('Description: pg_catalog."POSIX"' in line[0] for line in result)

    # not shippable output collate at function
    result = run_query(
        """
            explain (verbose) SELECT lower(a COLLATE "C") COLLATE "POSIX" FROM test;
        """,
        pg_conn,
    )

    assert any("1: \tCollation" in line[0] for line in result)
    assert any('Description: pg_catalog."POSIX"' in line[0] for line in result)

    # all shippable
    result = run_query(
        """
            explain(verbose) SELECT * FROM test;
        """,
        pg_conn,
    )

    assert not any("Not Vectorized Constructs:" in line[0] for line in result)

    pg_conn.rollback()


@pytest.fixture(scope="module")
def explain_table(s3, pg_conn, extension):
    run_command(
        f"""
        CREATE TABLE explain
        USING pg_lake_iceberg
        WITH (location = 's3://{TEST_BUCKET}/iceberg/explain/')
        AS SELECT s FROM generate_series(1,1000) s;
    """,
        pg_conn,
    )

    yield
    pg_conn.rollback()
