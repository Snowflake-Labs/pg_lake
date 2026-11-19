"""A read-only table reads a column added with a default as the default in the
files that predate it."""

from utils_pytest import *


def test_column_added_with_default(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    run_command("CREATE SCHEMA read_only_defaults", pg_conn)
    run_command(
        "CREATE TABLE read_only_defaults.wrt_tbl (a int) USING iceberg "
        "WITH (catalog='object_store')",
        pg_conn,
    )
    run_command("INSERT INTO read_only_defaults.wrt_tbl VALUES (1)", pg_conn)
    run_command(
        "ALTER TABLE read_only_defaults.wrt_tbl ADD COLUMN b int DEFAULT 5", pg_conn
    )
    run_command("INSERT INTO read_only_defaults.wrt_tbl VALUES (2, 6)", pg_conn)
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "read_only_defaults", "wrt_tbl"
    )

    run_command(
        "CREATE TABLE read_only_defaults.read_tbl () USING iceberg "
        "WITH (catalog='object_store', read_only=True, catalog_table_name='wrt_tbl')",
        pg_conn,
    )
    pg_conn.commit()

    assert run_query(
        "SELECT a, b FROM read_only_defaults.read_tbl ORDER BY a", pg_conn
    ) == [[1, 5], [2, 6]]

    pg_conn.rollback()
    run_command("DROP SCHEMA read_only_defaults CASCADE", pg_conn)
    pg_conn.commit()
