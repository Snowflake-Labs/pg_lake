import pytest
from utils_pytest import *

TEST_NUMERIC_URL = f"s3://{TEST_BUCKET}/numeric.parquet"


def test_small_numeric_pushdown(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/large_numeric.parquet"

    run_command(
        f"""
        copy (select 12 as id, 123.12::numeric(5,2) as a)
        to '{uri}'
    """,
        pg_conn,
    )

    run_command(
        f"""
        create foreign table small_numeric (id int, a numeric(5,2))
            server pg_lake options (path '{uri}');
    """,
        pg_conn,
    )

    expected_expression = '"sum"("a")'
    query = "select sum(a) from small_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = '"abs"("a")'
    query = "select abs(a) from small_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = '"a" + "a"'
    query = "select a + a from small_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_large_numeric_pushdown(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/large_numeric.parquet"

    run_command(
        f"""
        copy (select 12 as id, 123456789012345678901234567890123456789.123456789012345::numeric(54,15) as a)
        to '{uri}'
    """,
        pg_conn,
    )

    run_command(
        f"""
        create foreign table large_numeric (id int, a numeric(54,15))
            server pg_lake options (path '{uri}');
    """,
        pg_conn,
    )

    run_command("SAVEPOINT sp1", pg_conn)
    error = run_query("select sum(a) from large_numeric", pg_conn, raise_error=False)
    assert (
        "No function matches the given name and argument types 'sum(VARCHAR)'" in error
    )
    run_command("ROLLBACK TO SAVEPOINT sp1", pg_conn)

    run_command("SAVEPOINT sp1", pg_conn)
    error = run_query("select abs(a) from large_numeric", pg_conn, raise_error=False)
    assert (
        "No function matches the given name and argument types 'abs(VARCHAR)'" in error
    )
    run_command("ROLLBACK TO SAVEPOINT sp1", pg_conn)

    error = run_query("select a + a from large_numeric", pg_conn, raise_error=False)
    assert (
        "No function matches the given name and argument types '+(VARCHAR, VARCHAR)'"
        in error
    )

    pg_conn.rollback()


def test_unbounded_numeric_pushdown(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/unbounded_numeric.parquet"

    run_command(
        f"""
        copy (select 12 as id, 123.12::numeric as a)
        to '{uri}'
    """,
        pg_conn,
    )

    run_command(
        f"""
        create foreign table unbounded_numeric (id int, a numeric)
            server pg_lake options (path '{uri}');
    """,
        pg_conn,
    )

    expected_expression = '"sum"("a")'
    query = "select sum(a) from unbounded_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = '"abs"("a")'
    query = "select abs(a) from unbounded_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    expected_expression = '"a" + "a"'
    query = "select a + a from unbounded_numeric"
    run_query(query, pg_conn)
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_regular_table_load_from(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create table regular_table () with (load_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'regular_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    pg_conn.rollback()


def test_regular_table_definition_from(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create table regular_table () with (definition_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'regular_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    pg_conn.rollback()


def test_regular_table_explicit(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create table regular_table (numeric_small numeric(5,3), numeric_large numeric(39,10), numeric_unbounded numeric) with (load_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'regular_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 39, 10], ["numeric", 5, 3], ["numeric", None, None]]

    pg_conn.rollback()


def test_pg_lake_table_with_no_cols(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create foreign table pg_lake_table_with_no_cols () server pg_lake options (path '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'pg_lake_table_with_no_cols' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    expected_expression = 'WHERE ("abs"("numeric_unbounded") > (1)::numeric(10,0))'
    query = f"SELECT numeric_large, numeric_small, numeric_unbounded FROM pg_lake_table_with_no_cols {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [["12.2230000000", Decimal("12.223"), Decimal("12.22300000")]]

    pg_conn.rollback()


def test_pg_lake_table_explicit(s3, pg_conn, extension, copy_numeric_to_file):
    run_command(
        f"""
        create foreign table pg_lake_table_explicit (numeric_small numeric(5,3), numeric_large numeric(39,10), numeric_unbounded numeric)
         server pg_lake options (path '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'pg_lake_table_explicit' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 39, 10], ["numeric", 5, 3], ["numeric", None, None]]

    expected_expression = 'WHERE ("abs"("numeric_unbounded") > (1)::numeric(10,0))'
    query = f"SELECT numeric_large, numeric_small, numeric_unbounded FROM pg_lake_table_explicit {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    result = run_query(query, pg_conn)
    assert result == [
        [Decimal("12.2230000000"), Decimal("12.223"), Decimal("12.22300000")]
    ]

    pg_conn.rollback()


def test_writable_pg_lake_table(s3, pg_conn, extension, copy_numeric_to_file):
    run_command("set pg_lake_table.unbounded_numeric_default_precision = 10", pg_conn)
    run_command("set pg_lake_table.unbounded_numeric_default_scale = 5", pg_conn)

    run_command(
        f"""
        create foreign table writable_pg_lake_table (numeric_small numeric(5,3), numeric_large numeric(39,10), numeric_unbounded numeric)
         server pg_lake options (location '{TEST_NUMERIC_URL}', format 'parquet', writable 'true');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'writable_pg_lake_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 39, 10], ["numeric", 5, 3], ["numeric", None, None]]

    expected_expression = 'WHERE ("abs"("numeric_unbounded") > (1)::numeric(10,0))'
    query = f"SELECT * FROM writable_pg_lake_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_iceberg_table_load_from(s3, pg_conn, extension, copy_numeric_to_file):
    location = f"s3://{TEST_BUCKET}/iceberg_table"

    run_command(
        f"""
        create table iceberg_table () using pg_lake_iceberg with (load_from = '{TEST_NUMERIC_URL}', location = '{location}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    expected_expression = 'WHERE ("abs"("numeric_unbounded") > (1)::numeric(10,0))'
    query = f"SELECT * FROM iceberg_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_iceberg_table_definition_from(
    s3, pg_conn, extension, copy_numeric_to_file, with_default_location
):
    run_command(
        f"""
        create table iceberg_table () using pg_lake_iceberg with (definition_from = '{TEST_NUMERIC_URL}');
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["text", None, None], ["numeric", 5, 3], ["numeric", 38, 9]]

    expected_expression = 'WHERE ("abs"("numeric_unbounded") > (1)::numeric(10,0))'
    query = f"SELECT * FROM iceberg_table {expected_expression}"
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)

    pg_conn.rollback()


def test_iceberg_table_explicit(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        create table iceberg_table (numeric_small numeric(5,3), numeric_unbounded numeric) using pg_lake_iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 5, 3], ["double precision", 53, None]]

    pg_conn.rollback()


def test_iceberg_create_as_select(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        create table iceberg_table using pg_lake_iceberg
         as select 12.223::numeric(5,3) as numeric_small,
                   12.223::numeric as numeric_unbounded;
    """,
        pg_conn,
    )

    result = run_query(
        "select data_type, numeric_precision, numeric_scale from information_schema.columns where table_name = 'iceberg_table' order by column_name",
        pg_conn,
    )
    assert result == [["numeric", 5, 3], ["double precision", 53, None]]

    result = run_query(
        "SELECT numeric_small, numeric_unbounded FROM iceberg_table", pg_conn
    )
    assert result == [[Decimal("12.223"), 12.223]]

    pg_conn.rollback()


def test_unbounded_numeric_params_casts_and_constants(s3, pg_conn, extension):
    uri = f"s3://{TEST_BUCKET}/unbounded_numeric_params/data.parquet"

    run_command("RESET pg_lake_table.unbounded_numeric_default_precision", pg_conn)
    run_command("RESET pg_lake_table.unbounded_numeric_default_scale", pg_conn)

    run_command(
        f"""
        create table unbounded_numeric_heap (id int, n numeric(38,20), i8 bigint, p numeric(10,2));
        insert into unbounded_numeric_heap values
            (1, 1.5, 100000000000000000, 1.50),
            (2, 7.07, 200000000000000000, 7.07),
            (3, 0.00000000000000000001, 3, 0.00),
            (4, 123456789012345678.5, 4, 1.51);
        copy unbounded_numeric_heap to '{uri}';
        create foreign table unbounded_numeric_lake (id int, n numeric(38,20), i8 bigint, p numeric(10,2))
            server pg_lake options (path '{uri}');
    """,
        pg_conn,
    )

    # DuckDB reads a bare NUMERIC as DECIMAL(18,3): 1.4999 would become 1.500
    # and drop the 1.5 row, and 123456789012345678.5 would not fit. Next to n,
    # numeric(38,9) would round n's 1e-20 to 0, and next to p, numeric(10,2)
    # would round 1.505 to a value of p.
    statements = {
        "greater": "SELECT array_agg(id ORDER BY id) FROM {t} WHERE n > $1",
        "equal": "SELECT id FROM {t} WHERE n = $1",
        "in": "SELECT array_agg(id ORDER BY id) FROM {t} WHERE n IN ($1, 7.07)",
        "distinct": "SELECT array_agg(id ORDER BY id) FROM {t} WHERE n IS DISTINCT FROM $1",
        "not_distinct": "SELECT array_agg(id ORDER BY id) FROM {t} WHERE n IS NOT DISTINCT FROM $1",
        "equal_low_scale": "SELECT array_agg(id ORDER BY id) FROM {t} WHERE p = $1",
        "param": "SELECT $1 FROM {t} WHERE id = 1",
        "coalesce": "SELECT coalesce(n, $1) FROM {t} WHERE id = 3",
        "case": "SELECT CASE WHEN id = 3 THEN n ELSE $1 END FROM {t} WHERE id = 3",
        "greatest": "SELECT greatest(n, $1) FROM {t} WHERE id = 3",
        "least": "SELECT least(n, $1) FROM {t} WHERE id = 3",
        "nullif": "SELECT nullif(n, $1) FROM {t} WHERE id = 3",
    }
    values = {
        "greater": ["1.4999", "7.0699"],
        "equal": ["123456789012345678.5", "0.00000000000000000001"],
        "in": ["0.00000000000000000001", "1.5"],
        "distinct": ["0.00000000000000000001"],
        "not_distinct": ["0.00000000000000000001"],
        "equal_low_scale": ["1.505", "1.51"],
        "param": ["1.4999", "123456789012345678.5"],
        "coalesce": ["1"],
        "case": ["1"],
        "greatest": ["0"],
        "least": ["1"],
        "nullif": ["1"],
    }
    queries = [
        "SELECT array_agg(id ORDER BY id) FROM {t} WHERE n > ('1.4999'::text)::numeric",
        "SELECT n::numeric FROM {t} WHERE id = 3",
        "SELECT coalesce(n, ('1'::text)::numeric) FROM {t} WHERE id = 3",
        "SELECT sum(i8::numeric) FROM {t}",
        "SELECT round(i8::numeric, 2) FROM {t} WHERE id = 2",
        "SELECT count(*) FROM {t} WHERE n < 100000000000000000",
        "SELECT n * 2 FROM {t} WHERE id = 4",
        "SELECT array_agg(x ORDER BY x) FROM (SELECT 2::numeric AS x FROM {t} WHERE id = 1 "
        "UNION ALL SELECT 0.000001 FROM {t} WHERE id = 1) s",
    ]

    for pushdown in ["true", "false"]:
        run_command(
            f"SET pg_lake_table.enable_full_query_pushdown TO {pushdown}", pg_conn
        )

        for query in queries:
            expected = run_query(query.format(t="unbounded_numeric_heap"), pg_conn)
            result = run_query(query.format(t="unbounded_numeric_lake"), pg_conn)
            assert result == expected, (pushdown, query)

        for plan_cache_mode in ["force_custom_plan", "force_generic_plan"]:
            run_command(f"SET plan_cache_mode TO {plan_cache_mode}", pg_conn)

            for name, query in statements.items():
                # a custom plan folds the IN list into a numeric[] constant,
                # which DuckDB still reads as DECIMAL(18,3)[] without pushdown
                if (
                    name == "in"
                    and pushdown == "false"
                    and plan_cache_mode == "force_custom_plan"
                ):
                    continue

                for table in ["heap", "lake"]:
                    run_command(
                        f"PREPARE {name}_{table}(numeric) AS "
                        + query.format(t=f"unbounded_numeric_{table}"),
                        pg_conn,
                    )

                for value in values[name]:
                    expected = run_query(f"EXECUTE {name}_heap({value})", pg_conn)
                    result = run_query(f"EXECUTE {name}_lake({value})", pg_conn)
                    assert result == expected, (pushdown, plan_cache_mode, name, value)

                run_command(f"DEALLOCATE {name}_heap; DEALLOCATE {name}_lake", pg_conn)

        if pushdown == "true":
            # next to n, the cast takes n's scale; elsewhere that of unbounded columns
            explain = run_query(
                "EXPLAIN (VERBOSE) SELECT id FROM unbounded_numeric_lake WHERE n > ('1.5'::text)::numeric",
                pg_conn,
            )
            assert "numeric(38,20)" in str(explain)

            explain = run_query(
                "EXPLAIN (VERBOSE) SELECT id FROM unbounded_numeric_lake WHERE p > ('1.5'::text)::numeric",
                pg_conn,
            )
            assert "numeric(38,9)" in str(explain)

    run_command("RESET plan_cache_mode", pg_conn)
    run_command("RESET pg_lake_table.enable_full_query_pushdown", pg_conn)
    pg_conn.rollback()


@pytest.fixture(scope="module")
def copy_numeric_to_file(pg_conn, s3, request):
    run_command(
        f"""copy (select 12.223::numeric(5,3) as numeric_small,
                                 12.223::numeric(39, 10) as numeric_large,
                                 12.223::numeric as numeric_unbounded)
                    to '{TEST_NUMERIC_URL}'
                """,
        pg_conn,
    )
