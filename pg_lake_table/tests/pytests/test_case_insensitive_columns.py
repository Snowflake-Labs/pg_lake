import pytest

from utils_pytest import *


def test_create_iceberg_table_case_duplicate_columns(
    pg_conn, extension, with_default_location
):
    error = run_command(
        """CREATE TABLE case_dup ("a" int, "A" int) USING iceberg""",
        pg_conn,
        raise_error=False,
    )
    assert 'column name "A" conflicts with column "a"' in str(error)
    pg_conn.rollback()

    error = run_command(
        """CREATE TABLE case_dup_multi ("x" int, "y" int, "X" int) USING iceberg""",
        pg_conn,
        raise_error=False,
    )
    assert 'column name "X" conflicts with column "x"' in str(error)
    pg_conn.rollback()

    error = run_command(
        """CREATE TABLE case_dup_mixed ("FooBar" int, "foobar" int) USING iceberg""",
        pg_conn,
        raise_error=False,
    )
    assert 'column name "foobar" conflicts with column "FooBar"' in str(error)
    pg_conn.rollback()


def test_create_iceberg_nested_composite_case_duplicate(
    pg_conn, superuser_conn, extension, with_default_location
):
    run_command(
        """
        CREATE TYPE case_comp_dup AS ("a" int, "A" int);
        CREATE TYPE case_outer AS (inner_f case_comp_dup);
        """,
        superuser_conn,
    )
    map_type = run_query(
        "SELECT map_type.create('text', 'case_comp_dup')", superuser_conn
    )[0][0]
    superuser_conn.commit()

    for column_type in ["case_comp_dup", "case_comp_dup[]", "case_outer", map_type]:
        error = run_command(
            f"""CREATE TABLE case_nested (c {column_type}) USING iceberg""",
            pg_conn,
            raise_error=False,
        )
        assert 'field name "A" conflicts with field "a" in type case_comp_dup' in str(
            error
        )
        pg_conn.rollback()

    run_command("DROP TYPE case_comp_dup CASCADE", superuser_conn)
    superuser_conn.commit()


def test_create_iceberg_domain_over_colliding_composite(
    pg_conn, extension, with_default_location
):
    run_command("""CREATE TYPE case_dom_comp AS ("a" int, "A" int)""", pg_conn)
    run_command("""CREATE DOMAIN case_dom AS case_dom_comp""", pg_conn)

    error = run_command(
        """CREATE TABLE case_domain (d case_dom) USING iceberg""",
        pg_conn,
        raise_error=False,
    )
    assert 'field name "A" conflicts with field "a"' in str(error)
    pg_conn.rollback()


def test_create_iceberg_non_ascii_case_columns_allowed(
    pg_conn, extension, with_default_location
):
    # Non-ASCII case pairs stay distinct, same as DuckDB.
    run_command(
        """
        CREATE TABLE case_unicode (
            "é" int, "É" int,
            "ä" int, "Ä" int,
            "Σ" int, "σ" int, "ς" int
        ) USING iceberg
        """,
        pg_conn,
    )
    run_command(
        """INSERT INTO case_unicode VALUES (1, 2, 3, 4, 5, 6, 7)""",
        pg_conn,
    )
    result = run_query(
        """SELECT "é", "É", "ä", "Ä", "Σ", "σ", "ς" FROM case_unicode""",
        pg_conn,
    )
    assert result == [[1, 2, 3, 4, 5, 6, 7]]
    pg_conn.rollback()

    error = run_command(
        """CREATE TABLE case_mixed_ascii ("éa" int, "éA" int) USING iceberg""",
        pg_conn,
        raise_error=False,
    )
    assert 'column name "éA" conflicts with column "éa"' in str(error)
    pg_conn.rollback()


def test_create_iceberg_cross_level_names_allowed(
    pg_conn, extension, with_default_location
):
    # "A" and its field "a" are not siblings.
    run_command("""CREATE TYPE case_ok AS ("a" int, "B" int)""", pg_conn)
    run_command(
        """CREATE TABLE case_cross ("A" case_ok, "b" int) USING iceberg""",
        pg_conn,
    )
    run_command(
        """INSERT INTO case_cross VALUES (ROW(1, 2), 3)""",
        pg_conn,
    )
    result = run_query(
        """SELECT ("A")."a", ("A")."B", "b" FROM case_cross""",
        pg_conn,
    )
    assert result == [[1, 2, 3]]
    pg_conn.rollback()


def test_create_iceberg_dropped_composite_attribute_ignored(
    pg_conn, extension, with_default_location
):
    run_command("""CREATE TYPE case_dropped AS ("a" int, "A" int)""", pg_conn)
    run_command("""ALTER TYPE case_dropped DROP ATTRIBUTE "A" """, pg_conn)
    run_command(
        """CREATE TABLE case_dropped_tbl (c case_dropped) USING iceberg""",
        pg_conn,
    )
    run_command("""INSERT INTO case_dropped_tbl VALUES (ROW(1))""", pg_conn)
    assert run_query("""SELECT (c)."a" FROM case_dropped_tbl""", pg_conn) == [[1]]
    pg_conn.rollback()


def test_alter_table_add_column_case_duplicate(
    pg_conn, extension, with_default_location
):
    run_command("""CREATE TABLE case_add ("a" int) USING iceberg""", pg_conn)
    pg_conn.commit()

    error = run_command(
        """ALTER TABLE case_add ADD COLUMN "A" int""",
        pg_conn,
        raise_error=False,
    )
    assert 'column name "A" conflicts with column "a"' in str(error)
    pg_conn.rollback()

    run_command("""CREATE TYPE case_add_comp AS ("x" int, "X" int)""", pg_conn)
    error = run_command(
        """ALTER TABLE case_add ADD COLUMN c case_add_comp""",
        pg_conn,
        raise_error=False,
    )
    assert 'field name "X" conflicts with field "x"' in str(error)
    pg_conn.rollback()

    run_command("""ALTER TABLE case_add DROP COLUMN "a" """, pg_conn)
    run_command("""ALTER TABLE case_add ADD COLUMN "A" int""", pg_conn)
    run_command("""INSERT INTO case_add VALUES (42)""", pg_conn)
    assert run_query("""SELECT "A" FROM case_add""", pg_conn) == [[42]]
    pg_conn.rollback()

    run_command("DROP TABLE case_add", pg_conn)
    pg_conn.commit()


def test_alter_table_rename_column_case_duplicate(
    pg_conn, extension, with_default_location
):
    run_command(
        """CREATE TABLE case_rename ("a" int, "b" int) USING iceberg""",
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        """ALTER TABLE case_rename RENAME COLUMN "b" TO "A" """,
        pg_conn,
        raise_error=False,
    )
    assert 'column name "A" conflicts with column "a"' in str(error)
    pg_conn.rollback()

    # The old name is gone, so the other case is free.
    run_command(
        """ALTER TABLE case_rename RENAME COLUMN "a" TO "A" """,
        pg_conn,
    )
    run_command("""INSERT INTO case_rename VALUES (1, 2)""", pg_conn)
    assert run_query("""SELECT "A", "b" FROM case_rename""", pg_conn) == [[1, 2]]
    pg_conn.rollback()

    run_command("DROP TABLE case_rename", pg_conn)
    pg_conn.commit()


def test_create_table_like_case_duplicate(pg_conn, extension, with_default_location):
    run_command(
        """CREATE TABLE case_heap_src ("a" int, "A" int)""",
        pg_conn,
    )
    error = run_command(
        """CREATE TABLE case_like (LIKE case_heap_src) USING iceberg""",
        pg_conn,
        raise_error=False,
    )
    assert 'column name "A" conflicts with column "a"' in str(error)
    pg_conn.rollback()


def test_create_table_as_case_duplicate(pg_conn, extension, with_default_location):
    error = run_command(
        """
        CREATE TABLE case_ctas USING iceberg AS
        SELECT 1 AS "a", 2 AS "A"
        """,
        pg_conn,
        raise_error=False,
    )
    assert 'column name "A" conflicts with column "a"' in str(error)
    pg_conn.rollback()


def test_create_lake_table_case_duplicate(pg_conn, extension, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_case_insensitive_columns/writable/"
    error = run_command(
        f"""
        CREATE FOREIGN TABLE case_writable ("a" int, "A" int) SERVER pg_lake
        OPTIONS (writable 'true', location '{url}', format 'parquet')
        """,
        pg_conn,
        raise_error=False,
    )
    assert 'column name "A" conflicts with column "a"' in str(error)
    pg_conn.rollback()

    run_command("""CREATE TABLE case_source ("a" int, b int) USING iceberg""", pg_conn)
    pg_conn.commit()
    path = run_query(
        """
        SELECT metadata_location FROM iceberg_tables
        WHERE table_name = 'case_source'
        """,
        pg_conn,
    )[0][0]

    error = run_command(
        f"""
        CREATE FOREIGN TABLE case_external ("a" int, "A" int) SERVER pg_lake
        OPTIONS (path '{path}')
        """,
        pg_conn,
        raise_error=False,
    )
    assert 'column name "A" conflicts with column "a"' in str(error)
    pg_conn.rollback()

    run_command("DROP TABLE case_source", pg_conn)
    pg_conn.commit()


def test_legacy_table_repair_operations(
    pg_conn, superuser_conn, extension, with_default_location
):
    """A table that already has the clash can still be renamed or dropped."""
    run_command(
        """CREATE TABLE case_legacy ("a" int, "b" int) USING iceberg""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        UPDATE pg_attribute
        SET attname = 'A'
        WHERE attrelid = 'case_legacy'::regclass AND attname = 'b'
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(
        """ALTER TABLE case_legacy RENAME TO case_legacy_renamed""",
        pg_conn,
    )
    run_command(
        """ALTER TABLE case_legacy_renamed RENAME COLUMN "A" TO a_fixed""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        """
        UPDATE pg_attribute
        SET attname = 'A'
        WHERE attrelid = 'case_legacy_renamed'::regclass AND attname = 'a_fixed'
        """,
        superuser_conn,
    )
    superuser_conn.commit()
    run_command(
        """ALTER TABLE case_legacy_renamed DROP COLUMN "A" """,
        pg_conn,
    )
    run_command("""DROP TABLE case_legacy_renamed""", pg_conn)
    pg_conn.commit()
