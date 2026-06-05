import pytest
import psycopg2
import array
import duckdb
import math
import json
from decimal import *
from utils_pytest import *

test_cases = [
    # Int array operators
    (
        "int_array_eq",
        "WHERE col_int_array = array[13,1]",
        'WHERE ("col_int_array" = ARRAY[13, 1])',
    ),
    (
        "int_array_in",
        "WHERE col_int_array IN (array[4,5], array[13,1])",
        'WHERE (("col_int_array" = ARRAY[4, 5]) OR ("col_int_array" = ARRAY[13, 1]))',
    ),
    (
        "int_array_lt",
        "WHERE col_int_array < array[4,5]",
        'WHERE ("col_int_array" < ARRAY[4, 5])',
    ),
    (
        "int_array_le",
        "WHERE col_int_array <= array[4,5]",
        'WHERE ("col_int_array" <= ARRAY[4, 5])',
    ),
    (
        "int_array_gt",
        "WHERE col_int_array > array[4,5]",
        'WHERE ("col_int_array" > ARRAY[4, 5])',
    ),
    (
        "int_array_ge",
        "WHERE col_int_array >= array[]::int[]",
        'WHERE ("col_int_array" >= ARRAY[]::integer[])',
    ),
    (
        "int_array_overlaps",
        "WHERE col_int_array && array[4,5]",
        'WHERE ("col_int_array" && ARRAY[4, 5])',
    ),
    (
        "int_array_concat",
        "WHERE col_int_array || array[4] = array[13,1,4]",
        'WHERE (("col_int_array" || ARRAY[4]) = ARRAY[13, 1, 4])',
    ),
    (
        "int_array_append",
        "WHERE col_int_array || 4 = array[4]",
        'WHERE ("array_append"("col_int_array", 4) = ARRAY[4])',
    ),
    # Text array operators
    (
        "text_array_eq",
        "WHERE col_text_array = array['hello', 'world']",
        'WHERE ("col_text_array" = ARRAY[\'hello\'::"text", \'world\'::"text"])',
    ),
    (
        "text_array_in",
        "WHERE col_text_array IN (array[]::text[], array['hello', 'world'])",
        'WHERE (("col_text_array" = ARRAY[]::"text"[]) OR ("col_text_array" = ARRAY[\'hello\'::"text", \'world\'::"text"]))',
    ),
    (
        "text_array_lt",
        "WHERE col_text_array < array['bye']",
        'WHERE ("col_text_array" < ARRAY[\'bye\'::"text"])',
    ),
    (
        "text_array_le",
        "WHERE col_text_array <= array['bye']",
        'WHERE ("col_text_array" <= ARRAY[\'bye\'::"text"])',
    ),
    (
        "text_array_gt",
        "WHERE col_text_array > array['hello']",
        'WHERE ("col_text_array" > ARRAY[\'hello\'::"text"])',
    ),
    (
        "text_array_ge",
        "WHERE col_text_array >= array[]::text[]",
        'WHERE ("col_text_array" >= ARRAY[]::"text"[])',
    ),
    (
        "text_array_overlaps",
        "WHERE col_text_array && array['earth','hello']",
        'WHERE ("col_text_array" && ARRAY[\'earth\'::"text", \'hello\'::"text"])',
    ),
    (
        "text_array_concat",
        "WHERE col_text_array || array['earth'] = array['hello', 'world', 'earth']",
        'WHERE (("col_text_array" || ARRAY[\'earth\'::"text"]) = ARRAY[\'hello\'::"text", \'world\'::"text", \'earth\'::"text"])',
    ),
    (
        "text_array_append",
        "WHERE col_text_array || ''::text = array['hello', 'world', '']",
        'WHERE ("array_append"("col_text_array", \'\'::"text") = ARRAY[\'hello\'::"text", \'world\'::"text", \'\'::"text"])',
    ),
]


# Use the first element of each tuple for the ids parameter by extracting it with a list comprehension
@pytest.mark.parametrize(
    "test_id, operator_expression, expected_expression",
    test_cases,
    ids=[test_cases[0] for test_cases in test_cases],
)
def test_array_operator_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    operator_expression,
    expected_expression,
):
    query = "SELECT * FROM array_operator_pushdown.tbl " + operator_expression
    assert_remote_query_contains_expression(query, expected_expression, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["array_operator_pushdown.tbl"],
        ["array_operator_pushdown.heap_tbl"],
    )


# Array containment (@> / <@) is shipped only when the contained sub-list is a
# NULL-free, one-dimensional, scalar-element constant array (see
# IsShippableContainmentConstArray in shippable_builtin_operators.c). These
# cases must be pushed down AND return the same rows as the heap table.
containment_pushdown_cases = [
    # col @> const, across element types
    ("contains_int4", "WHERE col_int_array @> array[13]", "@>"),
    ("contains_int8", "WHERE col_int8_array @> array[13]::int8[]", "@>"),
    ("contains_int2", "WHERE col_int2_array @> array[13]::int2[]", "@>"),
    ("contains_text", "WHERE col_text_array @> array['hello']", "@>"),
    ("contains_float8", "WHERE col_float8_array @> array[1.5]::float8[]", "@>"),
    ("contains_float4", "WHERE col_float4_array @> array[1.5]::float4[]", "@>"),
    (
        "contains_numeric",
        "WHERE col_numeric_array @> array[1.5]::numeric(10,2)[]",
        "@>",
    ),
    (
        "contains_uuid",
        "WHERE col_uuid_array @> array['11111111-1111-1111-1111-111111111111'::uuid]",
        "@>",
    ),
    (
        "contains_date",
        "WHERE col_date_array @> array['2020-01-01'::date]",
        "@>",
    ),
    ("contains_bool", "WHERE col_bool_array @> array[true]", "@>"),
    # multiple non-null elements
    ("contains_multi", "WHERE col_int_array @> array[13,1]", "@>"),
    # empty sub-list is vacuously true and has no NULLs, so it ships
    ("contains_empty", "WHERE col_int_array @> array[]::int[]", "@>"),
    # const <@ col : the sub-list is the constant LHS, so this commuted form
    # is also safe to push.
    ("contained_const_lhs", "WHERE array[13,1] <@ col_int_array", "<@"),
    ("contained_const_lhs_text", "WHERE array['hello'] <@ col_text_array", "<@"),
]


# A stable projection that uniquely identifies every seeded row without
# selecting the bytea column (psycopg2 returns bytea as memoryview, which does
# not sort/compare deterministically in the result-comparison helpers).
CONTAINMENT_PROJECTION = "SELECT col_int_array, col_text_array FROM "


@pytest.mark.parametrize(
    "test_id, where_clause, expected_operator",
    containment_pushdown_cases,
    ids=[case[0] for case in containment_pushdown_cases],
)
def test_array_containment_pushdown(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    where_clause,
    expected_operator,
):
    query = CONTAINMENT_PROJECTION + "array_operator_pushdown.tbl " + where_clause
    assert_remote_query_contains_expression(query, expected_operator, pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["array_operator_pushdown.tbl"],
        ["array_operator_pushdown.heap_tbl"],
    )


# Array containment that must NOT be pushed down (the contained sub-list could
# contain NULLs, is multidimensional, or is the column itself). These must fall
# back to PostgreSQL yet still return the same rows as the heap table.
containment_fallback_cases = [
    # NULL element in the constant sub-list
    ("null_element", "WHERE col_text_array @> array['hello', NULL]", "@>"),
    # fully NULL constant sub-list
    ("all_null_const", "WHERE col_text_array @> array[NULL]::text[]", "@>"),
    # const @> col : the column is the contained side
    ("const_contains_col", "WHERE array[13] @> col_int_array", "@>"),
    # col <@ const : the column is the contained side
    ("col_contained_const", "WHERE col_int_array <@ array[13,1]", "<@"),
    # multidimensional constant sub-list
    ("multidim_const", "WHERE col_int_array @> array[array[13]]", "@>"),
]


def _assert_fdw_matches_heap(query, pg_conn, tolerance=0.001):
    """Compare FDW vs heap results, allowing an empty (but equal) result set.

    Some valid fallback predicates (e.g. ``col @> array[..., NULL]``) correctly
    return zero rows in PostgreSQL, which assert_query_results_on_tables rejects.
    """
    fdw_result = sort_with_none_at_end(perform_query_on_cursor(query, pg_conn))
    heap_query = query.replace(
        "array_operator_pushdown.tbl", "array_operator_pushdown.heap_tbl"
    )
    heap_result = sort_with_none_at_end(perform_query_on_cursor(heap_query, pg_conn))

    assert len(fdw_result) == len(
        heap_result
    ), f"Result sets have different lengths: {fdw_result} vs {heap_result}"
    for row_fdw, row_heap in zip(fdw_result, heap_result):
        assert compare_rows(
            row_fdw, row_heap, tolerance
        ), f"Results do not match: {row_fdw} and {row_heap}"


@pytest.mark.parametrize(
    "test_id, where_clause, not_expected_operator",
    containment_fallback_cases,
    ids=[case[0] for case in containment_fallback_cases],
)
def test_array_containment_not_pushed(
    create_operator_pushdown_table,
    pg_conn,
    test_id,
    where_clause,
    not_expected_operator,
):
    query = CONTAINMENT_PROJECTION + "array_operator_pushdown.tbl " + where_clause
    assert_remote_query_not_contains_expression(query, not_expected_operator, pg_conn)
    _assert_fdw_matches_heap(query, pg_conn)


# create the FDW and heap tables and seed them from the same Parquet file
@pytest.fixture(scope="module")
def create_operator_pushdown_table(pg_conn, s3, request, extension):

    run_command(
        """
	            CREATE SCHEMA array_operator_pushdown;

	            """,
        pg_conn,
    )
    pg_conn.commit()

    # bytea is intentionally excluded here: psycopg2 returns it as memoryview,
    # which breaks the SELECT * based comparisons of the existing tests. bytea
    # containment is covered separately in test_array_containment_bytea.
    columns = (
        "col_int_array int[], "
        "col_text_array text[], "
        "col_int8_array int8[], "
        "col_int2_array int2[], "
        "col_float8_array float8[], "
        "col_float4_array float4[], "
        "col_numeric_array numeric(10,2)[], "
        "col_uuid_array uuid[], "
        "col_date_array date[], "
        "col_bool_array bool[]"
    )

    url = f"s3://{TEST_BUCKET}/{request.node.name}/data.parquet"
    run_command(
        f"""
			COPY (
				-- all-NULL arrays (also fixes the column types for the file)
				SELECT
					NULL::int[]            as int_array,
					NULL::text[]           as text_array,
					NULL::int8[]           as int8_array,
					NULL::int2[]           as int2_array,
					NULL::float8[]         as float8_array,
					NULL::float4[]         as float4_array,
					NULL::numeric(10,2)[]  as numeric_array,
					NULL::uuid[]           as uuid_array,
					NULL::date[]           as date_array,
					NULL::bool[]           as bool_array
				UNION ALL
				-- "match" row containing the values probed by the positive tests
				SELECT
					array[13,1],
					array['hello','world'],
					array[13,1]::int8[],
					array[13,1]::int2[],
					array[1.5,2.5]::float8[],
					array[1.5,2.5]::float4[],
					array[1.5,2.5]::numeric(10,2)[],
					array['11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222']::uuid[],
					array['2020-01-01','2020-02-02']::date[],
					array[true,false]
				UNION ALL
				-- distinct, fully-populated row
				SELECT
					array[3,4],
					array['foo','bar'],
					array[3,4]::int8[],
					array[3,4]::int2[],
					array[3.5,4.5]::float8[],
					array[3.5,4.5]::float4[],
					array[3.5]::numeric(10,2)[],
					array['33333333-3333-3333-3333-333333333333']::uuid[],
					array['2021-03-03']::date[],
					array[true]
				UNION ALL
				-- empty arrays
				SELECT
					array[]::int[],
					array[]::text[],
					array[]::int8[],
					array[]::int2[],
					array[]::float8[],
					array[]::float4[],
					array[]::numeric(10,2)[],
					array[]::uuid[],
					array[]::date[],
					array[]::bool[]
				UNION ALL
				-- arrays that contain a NULL element (exercises the fallback path)
				SELECT
					array[13,NULL]::int[],
					array['hello',NULL]::text[],
					array[13,NULL]::int8[],
					array[13,NULL]::int2[],
					array[1.5,NULL]::float8[],
					array[1.5,NULL]::float4[],
					array[1.5,NULL]::numeric(10,2)[],
					array['11111111-1111-1111-1111-111111111111',NULL]::uuid[],
					array['2020-01-01',NULL]::date[],
					array[true,NULL]::bool[]
				UNION ALL
				-- non-matching distinct row
				SELECT
					array[7,8],
					array['x','y'],
					array[7,8]::int8[],
					array[7,8]::int2[],
					array[7.5,8.5]::float8[],
					array[7.5,8.5]::float4[],
					array[7.5]::numeric(10,2)[],
					array['44444444-4444-4444-4444-444444444444']::uuid[],
					array['2022-04-04']::date[],
					array[false]
			) TO '{url}' WITH (FORMAT 'parquet');
		""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
	            CREATE FOREIGN TABLE array_operator_pushdown.tbl
	            (
					{columns}
	            ) SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
	            """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
	            CREATE TABLE array_operator_pushdown.heap_tbl
				(
					{columns}
	            );
	            COPY array_operator_pushdown.heap_tbl FROM '{url}';
	            """,
        pg_conn,
    )
    pg_conn.commit()

    yield

    run_command("DROP SCHEMA array_operator_pushdown CASCADE", pg_conn)
    pg_conn.commit()


# && (arrayoverlap) maps to DuckDB's list_has_any, which skips NULL elements on
# both the build and probe sides, matching PostgreSQL's overlap semantics
# (NULLs never count as a common element). Unlike @> / <@, it therefore needs no
# value-based guard and is pushed unconditionally. These cases verify it stays
# pushed AND correct when NULLs are involved on either side.
overlap_null_cases = [
    ("overlap_simple", "WHERE col_int_array && array[13]"),
    ("overlap_const_null_elem", "WHERE col_int_array && array[13, NULL]"),
    ("overlap_all_null_const", "WHERE col_int_array && array[NULL]::int[]"),
    ("overlap_no_match", "WHERE col_int_array && array[999]"),
    ("overlap_text_null_elem", "WHERE col_text_array && array['hello', NULL]"),
    ("overlap_text_only_null", "WHERE col_text_array && array[NULL]::text[]"),
    # commuted form: constant on the left
    ("overlap_const_lhs", "WHERE array[13, NULL] && col_int_array"),
]


@pytest.mark.parametrize(
    "test_id, where_clause",
    overlap_null_cases,
    ids=[case[0] for case in overlap_null_cases],
)
def test_array_overlap_null_semantics(
    create_operator_pushdown_table, pg_conn, test_id, where_clause
):
    query = CONTAINMENT_PROJECTION + "array_operator_pushdown.tbl " + where_clause
    assert_remote_query_contains_expression(query, "&&", pg_conn)
    _assert_fdw_matches_heap(query, pg_conn)


def test_array_containment_prepared_statement(create_operator_pushdown_table, pg_conn):
    """Containment with a parameterized (prepared) sub-list.

    In a custom plan the bound parameter is folded to a Const and (when
    NULL-free) pushed down; in a generic plan it stays a Param and the
    predicate safely falls back to PostgreSQL. Either way the result must match
    the heap table, including when the parameter array contains a NULL element.
    """
    params = ["{13}", "{13,1}", "{13,NULL}", "{}", "{99}"]

    for mode, pushed in [("force_custom_plan", True), ("force_generic_plan", False)]:
        run_command(f"SET plan_cache_mode TO {mode}", pg_conn)
        run_command("DEALLOCATE ALL", pg_conn)
        run_command(
            "PREPARE pc (int[]) AS SELECT col_int_array, col_text_array "
            "FROM array_operator_pushdown.tbl WHERE col_int_array @> $1",
            pg_conn,
        )
        run_command(
            "PREPARE ph (int[]) AS SELECT col_int_array, col_text_array "
            "FROM array_operator_pushdown.heap_tbl WHERE col_int_array @> $1",
            pg_conn,
        )

        for p in params:
            fdw = sort_with_none_at_end(
                run_query(f"EXECUTE pc ('{p}'::int[])", pg_conn)
            )
            heap = sort_with_none_at_end(
                run_query(f"EXECUTE ph ('{p}'::int[])", pg_conn)
            )
            assert len(fdw) == len(heap), f"{mode} {p}: {fdw} vs {heap}"
            for row_fdw, row_heap in zip(fdw, heap):
                assert compare_rows(
                    row_fdw, row_heap, 0.001
                ), f"{mode} {p}: {row_fdw} vs {row_heap}"

        # the plan mode should determine whether a NULL-free param is shipped
        execute_query = "EXECUTE pc ('{13}'::int[])"
        if pushed:
            assert_remote_query_contains_expression(execute_query, "@>", pg_conn)
        else:
            assert_remote_query_not_contains_expression(execute_query, "@>", pg_conn)

        run_command("DEALLOCATE ALL", pg_conn)

    run_command("RESET plan_cache_mode", pg_conn)
    pg_conn.commit()


special_float_cases = [
    ("float8_nan", "WHERE col_float8_array @> array['NaN'::float8]"),
    ("float8_inf", "WHERE col_float8_array @> array['Infinity'::float8]"),
    ("float8_neg_inf", "WHERE col_float8_array @> array['-Infinity'::float8]"),
    ("float4_nan", "WHERE col_float4_array @> array['NaN'::float4]"),
]


# NaN / +-Infinity in float arrays must still be pushed down. We project only
# the integer id column so the result comparison never has to compare NaN
# floats (which never compare equal to themselves).
@pytest.mark.parametrize(
    "test_id, where_clause",
    special_float_cases,
    ids=[case[0] for case in special_float_cases],
)
def test_array_containment_special_floats(
    pg_conn, s3, extension, test_id, where_clause
):
    url = f"s3://{TEST_BUCKET}/array_containment_special_floats/data.parquet"
    run_command(
        f"""
        DROP FOREIGN TABLE IF EXISTS array_containment_floats_fdw;
        DROP TABLE IF EXISTS array_containment_floats_heap;
        COPY (
            SELECT 1 as id, array[1.5,2.5]::float8[] as col_float8_array, array[1.5,2.5]::float4[] as col_float4_array
            UNION ALL
            SELECT 2 as id, array['NaN','Infinity','-Infinity']::float8[], array['NaN','Infinity']::float4[]
            UNION ALL
            SELECT 3 as id, array['Infinity',7.5]::float8[], array[7.5]::float4[]
            UNION ALL
            SELECT 4 as id, array['NaN']::float8[], array['NaN']::float4[]
        ) TO '{url}' WITH (FORMAT 'parquet');
        CREATE FOREIGN TABLE array_containment_floats_fdw (id int, col_float8_array float8[], col_float4_array float4[])
            SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        CREATE TABLE array_containment_floats_heap (id int, col_float8_array float8[], col_float4_array float4[]);
        COPY array_containment_floats_heap FROM '{url}';
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = (
        "SELECT id FROM array_containment_floats_fdw " + where_clause + " ORDER BY id"
    )
    assert_remote_query_contains_expression(query, "@>", pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["array_containment_floats_fdw"],
        ["array_containment_floats_heap"],
    )

    run_command(
        """
        DROP FOREIGN TABLE array_containment_floats_fdw;
        DROP TABLE array_containment_floats_heap;
        """,
        pg_conn,
    )
    pg_conn.commit()


# bytea containment is covered on its own so the comparison can project a
# stable identifier column (bytea itself comes back as memoryview).
def test_array_containment_bytea(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/array_containment_bytea/data.parquet"
    run_command(
        f"""
        COPY (
            SELECT 1 as id, array['\\xdeadbeef'::bytea, '\\xcafe'::bytea] as bytea_array
            UNION ALL
            SELECT 2 as id, array['\\xaa'::bytea] as bytea_array
            UNION ALL
            SELECT 3 as id, array['\\xdeadbeef'::bytea, NULL]::bytea[] as bytea_array
        ) TO '{url}' WITH (FORMAT 'parquet');
        CREATE FOREIGN TABLE array_containment_bytea_fdw (id int, col_bytea_array bytea[])
            SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        CREATE TABLE array_containment_bytea_heap (id int, col_bytea_array bytea[]);
        COPY array_containment_bytea_heap FROM '{url}';
        """,
        pg_conn,
    )
    pg_conn.commit()

    # NULL-free constant sub-list: pushed down, ids 1 and 3 match
    query = (
        "SELECT id FROM array_containment_bytea_fdw "
        "WHERE col_bytea_array @> array['\\xdeadbeef'::bytea] ORDER BY id"
    )
    assert_remote_query_contains_expression(query, "@>", pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["array_containment_bytea_fdw"],
        ["array_containment_bytea_heap"],
    )

    run_command(
        """
        DROP FOREIGN TABLE array_containment_bytea_fdw;
        DROP TABLE array_containment_bytea_heap;
        """,
        pg_conn,
    )
    pg_conn.commit()


# Arrays of composite types pass the column type gate (they map to a DuckDB
# LIST of STRUCT) but we conservatively do not push containment for them, since
# DuckDB struct list equality has not been validated against PostgreSQL. The
# query must therefore stay on the PG side and still return the right rows.
def test_array_containment_composite_not_pushed(pg_conn, s3, extension):
    url = f"s3://{TEST_BUCKET}/array_containment_composite/data.parquet"
    run_command(
        f"""
        CREATE TYPE array_containment_pair AS (a int, b int);
        COPY (
            SELECT array[row(1,2)::array_containment_pair, row(3,4)::array_containment_pair] as comp_array
            UNION ALL
            SELECT array[row(5,6)::array_containment_pair] as comp_array
        ) TO '{url}' WITH (FORMAT 'parquet');
        CREATE FOREIGN TABLE array_containment_composite_fdw (comp_col array_containment_pair[])
            SERVER pg_lake OPTIONS (format 'parquet', path '{url}');
        CREATE TABLE array_containment_composite_heap (comp_col array_containment_pair[]);
        COPY array_containment_composite_heap FROM '{url}';
        """,
        pg_conn,
    )
    pg_conn.commit()

    query = (
        "SELECT * FROM array_containment_composite_fdw "
        "WHERE comp_col @> array[row(1,2)::array_containment_pair]"
    )
    assert_remote_query_not_contains_expression(query, "@>", pg_conn)
    assert_query_results_on_tables(
        query,
        pg_conn,
        ["array_containment_composite_fdw"],
        ["array_containment_composite_heap"],
    )

    run_command(
        """
        DROP FOREIGN TABLE array_containment_composite_fdw;
        DROP TABLE array_containment_composite_heap;
        DROP TYPE array_containment_pair;
        """,
        pg_conn,
    )
    pg_conn.commit()
