"""Arrays with a lower bound other than 1 can be written to Iceberg tables.

PostgreSQL lets any array value carry custom bounds: '[0:1]={1,2}' fits an
int[] column, and ``UPDATE t SET a[0] = x`` extends an existing array
downwards. A list has no bounds, so the elements are written with the bounds
reset to 1. Pushing down a constant with custom bounds still errors, since
the bounds change the result of a comparison.
"""

import io

from utils_pytest import *


def test_write_array_lower_bounds(pg_conn, extension, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_array_lbound;
        CREATE DOMAIN test_array_lbound.int_list AS int[];
        CREATE TYPE test_array_lbound.tagged AS (tag text, vals int[]);

        CREATE TABLE test_array_lbound.ice (
            id int,
            plain int[],
            texts text[],
            via_domain test_array_lbound.int_list,
            in_composite test_array_lbound.tagged
        ) USING iceberg;

        CREATE TABLE test_array_lbound.heap (LIKE test_array_lbound.ice);
        INSERT INTO test_array_lbound.heap VALUES
            (2, '{1,2}', '{c}', '{11}', ROW('select', '{12}'));
        UPDATE test_array_lbound.heap SET plain[0] = 0;

        INSERT INTO test_array_lbound.ice VALUES
            (1, '[0:1]={3,4}', '[-2:-1]={"b",NULL}', '[5:5]={7}',
             ROW('values', '[3:4]={9,10}'));
        INSERT INTO test_array_lbound.ice SELECT * FROM test_array_lbound.heap;
        """,
        pg_conn,
    )

    with pg_conn.cursor() as cursor:
        cursor.copy_expert(
            "COPY test_array_lbound.ice FROM STDIN",
            io.StringIO("3\t[0:0]={8}\t{d}\t[2:3]={5,6}\t(copy,[0:0]={13})\n"),
        )

    run_command(
        """
        CREATE TABLE test_array_lbound.ctas USING iceberg
        AS SELECT * FROM test_array_lbound.heap;
        """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT id, plain::text, texts::text, via_domain::text,
               (in_composite).tag, (in_composite).vals::text
        FROM test_array_lbound.ice ORDER BY id
        """,
        pg_conn,
    )
    assert [list(row) for row in result] == [
        [1, "{3,4}", "{b,NULL}", "{7}", "values", "{9,10}"],
        [2, "{0,1,2}", "{c}", "{11}", "select", "{12}"],
        [3, "{8}", "{d}", "{5,6}", "copy", "{13}"],
    ]

    result = run_query("SELECT plain FROM test_array_lbound.ctas", pg_conn)
    assert [list(row) for row in result] == [[[0, 1, 2]]]

    error = run_query(
        "SELECT id FROM test_array_lbound.ice WHERE plain = '[0:1]={3,4}'",
        pg_conn,
        raise_error=False,
    )
    assert "cannot push down array with custom lower bounds" in error

    pg_conn.rollback()
