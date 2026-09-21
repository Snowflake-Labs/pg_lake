"""
Verify postgres_scan reads PostgreSQL's builtin geometric types as text.

Upstream postgres_scanner maps point to a struct and line, lseg, box, path,
polygon and circle to a list of doubles. pg_lake has no Iceberg mapping for
these types and stores their Postgres text form as a string, so the scanner is
patched to read them as VARCHAR -- server-side casts make the scanned value
exactly what Postgres itself prints.

Without that patch a scan returns a struct or a list, and Parquet written from
it does not match the string column of the Iceberg table.
"""

import psycopg2
import pytest

from utils_pytest import *


def _connstr():
    return (
        f"host={server_params.PG_HOST} "
        f"port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} "
        f"user={server_params.PG_USER} "
        f"password={server_params.PG_PASSWORD}"
    )


def _scan(table, schema="public"):
    return f"postgres_scan('{_connstr()}', '{schema}', '{table}')"


@pytest.fixture(scope="module")
def pg_geometric_tables(postgres):
    """Create tables covering every builtin geometric type."""
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS scanner_geometric_tbl")
    cur.execute(
        "CREATE TABLE scanner_geometric_tbl ("
        "  id int PRIMARY KEY,"
        "  pt point,"
        "  ln line,"
        "  ls lseg,"
        "  bx box,"
        "  pth path,"
        "  ply polygon,"
        "  cr circle"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_geometric_tbl VALUES "
        "(1, '(1,2)', '{1,2,3}', '((0,0),(1,1))', '((0,0),(1,1))', "
        "    '((0,0),(1,1),(2,0))', '((0,0),(1,1),(2,0))', '<(0,0),5>'),"
        "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    )

    cur.execute("DROP TABLE IF EXISTS scanner_geometric_array_tbl")
    cur.execute(
        "CREATE TABLE scanner_geometric_array_tbl (id int PRIMARY KEY, pts point[])"
    )
    cur.execute(
        "INSERT INTO scanner_geometric_array_tbl VALUES "
        "(1, ARRAY['(1,2)', '(3,4)']::point[]),"
        "(2, NULL)"
    )

    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS scanner_geometric_tbl")
    cur.execute("DROP TABLE IF EXISTS scanner_geometric_array_tbl")
    cur.close()
    conn.close()


GEOMETRIC_COLUMNS = ["pt", "ln", "ls", "bx", "pth", "ply", "cr"]


def test_geometric_types_are_varchar(pg_geometric_tables, pgduck_conn):
    """Every geometric column is scanned as VARCHAR, not a struct or a list."""
    scan = _scan("scanner_geometric_tbl")
    rows = perform_query_on_cursor(
        f"SELECT {', '.join(f'typeof({c})' for c in GEOMETRIC_COLUMNS)} "
        f"FROM {scan} WHERE id = 1",
        pgduck_conn,
    )
    assert rows == [tuple(["VARCHAR"] * len(GEOMETRIC_COLUMNS))]


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_geometric_values_match_postgres_text(
    pg_geometric_tables, pgduck_conn, use_text_protocol
):
    """Scanned values are Postgres' own text output (box normalizes its corners)."""
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        scan = _scan("scanner_geometric_tbl")
        rows = perform_query_on_cursor(
            f"SELECT {', '.join(GEOMETRIC_COLUMNS)} FROM {scan} ORDER BY id",
            pgduck_conn,
        )
        assert rows == [
            (
                "(1,2)",
                "{1,2,3}",
                "[(0,0),(1,1)]",
                "(1,1),(0,0)",
                "((0,0),(1,1),(2,0))",
                "((0,0),(1,1),(2,0))",
                "<(0,0),5>",
            ),
            (None, None, None, None, None, None, None),
        ]
    finally:
        if use_text_protocol:
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


def test_geometric_array_is_varchar_list(pg_geometric_tables, pgduck_conn):
    """An array of a geometric type is scanned as a list of text values."""
    scan = _scan("scanner_geometric_array_tbl")
    rows = perform_query_on_cursor(
        f"SELECT typeof(pts), pts[1], pts[2] FROM {scan} WHERE id = 1",
        pgduck_conn,
    )
    assert rows == [("VARCHAR[]", "(1,2)", "(3,4)")]


def test_geometric_array_null(pg_geometric_tables, pgduck_conn):
    """NULL arrays stay NULL."""
    scan = _scan("scanner_geometric_array_tbl")
    rows = perform_query_on_cursor(f"SELECT pts FROM {scan} WHERE id = 2", pgduck_conn)
    assert rows == [(None,)]


@pytest.fixture(scope="module")
def pg_user_type_named_point(postgres):
    """A user-defined composite type that happens to be called "point"."""
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP SCHEMA IF EXISTS scanner_geom_collision CASCADE")
    cur.execute("CREATE SCHEMA scanner_geom_collision")
    cur.execute("CREATE TYPE scanner_geom_collision.point AS (a int, b text)")
    cur.execute(
        "CREATE TABLE scanner_geom_collision.t "
        "(id int PRIMARY KEY, p scanner_geom_collision.point)"
    )
    cur.execute(
        "INSERT INTO scanner_geom_collision.t VALUES "
        "(1, ROW(7, 'seven')::scanner_geom_collision.point),"
        "(2, NULL)"
    )

    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS scanner_geom_collision CASCADE")
    cur.close()
    conn.close()


def test_user_type_named_point_is_not_read_as_text(
    pg_user_type_named_point, pgduck_conn
):
    """The varchar cast keys off the pg_catalog OID, not the type name.

    A composite type of the user's own named "point" has to keep resolving as
    that composite; reading it as text would write a Parquet primitive into a
    struct column.
    """
    scan = _scan("t", schema="scanner_geom_collision")
    rows = perform_query_on_cursor(
        f"SELECT typeof(p), p.a, p.b FROM {scan} WHERE id = 1", pgduck_conn
    )
    assert rows[0][0].startswith("STRUCT("), rows[0][0]
    assert rows[0][1:] == (7, "seven")


@pytest.fixture(scope="module")
def pg_geom_composite(postgres):
    """Composites, nested composites and arrays of composites with a point field."""
    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("DROP SCHEMA IF EXISTS scanner_geom_composite CASCADE")
    cur.execute("CREATE SCHEMA scanner_geom_composite")
    cur.execute("CREATE TYPE scanner_geom_composite.inner_t AS (n int, g point)")
    cur.execute(
        "CREATE TYPE scanner_geom_composite.outer_t AS "
        "(label text, inr scanner_geom_composite.inner_t)"
    )
    cur.execute(
        "CREATE TABLE scanner_geom_composite.t ("
        "  id int PRIMARY KEY,"
        "  c scanner_geom_composite.inner_t,"
        "  o scanner_geom_composite.outer_t,"
        "  ca scanner_geom_composite.inner_t[]"
        ")"
    )
    cur.execute(
        "INSERT INTO scanner_geom_composite.t VALUES "
        "(1, ROW(5, '(1,2)'), ROW('lbl', ROW(6, '(3,4)')),"
        "    ARRAY[ROW(7, '(5,6)')::scanner_geom_composite.inner_t]),"
        "(2, NULL, NULL, NULL)"
    )

    # A composite whose fields are all NULL prints as "(,)", which the text
    # protocol's struct parser drops empty fields from and then rejects.  That
    # is independent of the varchar cast, so keep the row out of the table the
    # text-protocol test scans.
    cur.execute(
        "CREATE TABLE scanner_geom_composite.t_null_fields "
        "(id int PRIMARY KEY, c scanner_geom_composite.inner_t)"
    )
    cur.execute(
        "INSERT INTO scanner_geom_composite.t_null_fields VALUES (1, ROW(NULL, NULL))"
    )

    cur.close()
    conn.close()

    yield

    conn = open_pg_conn()
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("DROP SCHEMA IF EXISTS scanner_geom_composite CASCADE")
    cur.close()
    conn.close()


@pytest.mark.parametrize("use_text_protocol", [False, True], ids=["binary", "text"])
def test_geometric_field_inside_composite(
    pg_geom_composite, pgduck_conn, use_text_protocol
):
    """A point nested in a composite is a VARCHAR field of the struct.

    The composite itself cannot be cast, so the scanner rebuilds it field by
    field server-side and casts only the leaves that are read as text.
    """
    if use_text_protocol:
        perform_query_on_cursor("SET pg_use_text_protocol = true", pgduck_conn)
    try:
        scan = _scan("t", schema="scanner_geom_composite")
        rows = perform_query_on_cursor(
            f"SELECT typeof(c), c.n, c.g FROM {scan} WHERE id = 1", pgduck_conn
        )
        assert rows[0][0].startswith("STRUCT("), rows[0][0]
        assert "g VARCHAR" in rows[0][0], rows[0][0]
        assert rows[0][1:] == (5, "(1,2)")
    finally:
        if use_text_protocol:
            pgduck_conn.rollback()
            perform_query_on_cursor("SET pg_use_text_protocol = false", pgduck_conn)


def test_geometric_field_inside_nested_composite(pg_geom_composite, pgduck_conn):
    """The rebuild recurses, so a point two composites deep is still text."""
    scan = _scan("t", schema="scanner_geom_composite")
    rows = perform_query_on_cursor(
        f"SELECT typeof(o), o.label, o.inr.n, o.inr.g FROM {scan} WHERE id = 1",
        pgduck_conn,
    )
    assert "inr STRUCT(n INTEGER, g VARCHAR)" in rows[0][0], rows[0][0]
    assert rows[0][1:] == ("lbl", 6, "(3,4)")


def test_null_composite_stays_null(pg_geom_composite, pgduck_conn):
    """A NULL composite is NULL, not a struct of NULLs."""
    scan = _scan("t", schema="scanner_geom_composite")
    rows = perform_query_on_cursor(
        f"SELECT c IS NULL, o IS NULL FROM {scan} WHERE id = 2", pgduck_conn
    )
    assert rows == [(True, True)]


def test_composite_of_nulls_is_not_null(pg_geom_composite, pgduck_conn):
    """A composite whose fields are all NULL is itself not NULL."""
    scan = _scan("t_null_fields", schema="scanner_geom_composite")
    rows = perform_query_on_cursor(
        f"SELECT c IS NULL, c.n, c.g FROM {scan} WHERE id = 1", pgduck_conn
    )
    assert rows == [(False, None, None)]


def test_array_of_composite_with_geometric_field_is_rejected(
    pg_geom_composite, pgduck_conn
):
    """The cast is not pushed into array elements, so such a column errors out."""
    scan = _scan("t", schema="scanner_geom_composite")
    cur = pgduck_conn.cursor()
    try:
        with pytest.raises(psycopg2.Error) as exc_info:
            cur.execute(f"SELECT ca FROM {scan} WHERE id = 1")
        assert "cast to varchar not implemented" in str(exc_info.value)
    finally:
        cur.close()
        # the failed scan left the transaction aborted
        pgduck_conn.rollback()
