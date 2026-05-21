"""Stage 16 (Iceberg v3 rollout, end of Iteration 2): UPDATE / DELETE /
MERGE on v3 tables must error cleanly.

The v3 spec replaces positional delete files (PDFs) with deletion vectors
(DVs). pg_lake's DV emission path is queued for Iteration 4 (Stages 20+);
until then we cannot safely mutate v3 data because mixing v2-style PDFs
with v3 row lineage is forbidden by the spec and would corrupt the table
from any v3-aware reader's perspective.

INSERT is allowed -- the row-lineage allocator from Stage 12 makes v3
appends spec-clean.

The gate fires from ``postgresPlanForeignModify`` so EXPLAIN reports the
same error and there is no partial planner state to roll back. This file
pins both halves of the contract:

  * v3: INSERT works; UPDATE / DELETE error with the documented
    feature-not-supported message.
  * v2: UPDATE / DELETE remain fully functional via the existing PDF
    path (Stage 16 must not regress v2).
"""

import psycopg2
import pytest

from utils_pytest import *

# ---------------------------------------------------------------------------
# v3 negative path: INSERT ok, UPDATE / DELETE refused.
# ---------------------------------------------------------------------------


def test_v3_insert_succeeds(s3, pg_conn, extension, app_user, with_default_location):
    """Stage 12 already pinned this contract; we re-pin it here so Stage
    16's negative tests aren't ambiguous about what's blocked vs. what's
    just broken."""
    table = "test_v3_dml_gate_insert"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 3);",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {table} SELECT i FROM generate_series(1, 5) i;", pg_conn)
    pg_conn.commit()

    rows = run_query(f"SELECT count(*) FROM {table};", pg_conn)
    assert rows[0][0] == 5

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


@pytest.mark.parametrize(
    "stmt_template,op_label",
    [
        ("DELETE FROM {table} WHERE a = 1;", "DELETE"),
        ("UPDATE {table} SET a = a + 1 WHERE a = 1;", "UPDATE"),
    ],
    ids=["delete", "update"],
)
def test_v3_dml_is_refused(
    s3, pg_conn, extension, app_user, with_default_location, stmt_template, op_label
):
    """Both UPDATE and DELETE must raise the same family of error so
    error-handling clients can branch on a single message.
    """
    table = "test_v3_dml_gate"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 3);",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {table} VALUES (1), (2), (3);", pg_conn)
    pg_conn.commit()

    with pytest.raises(
        psycopg2.Error,
        match=f"{op_label} on Iceberg format-version 3 tables is not yet supported",
    ):
        run_command(stmt_template.format(table=table), pg_conn)
    pg_conn.rollback()

    # Sanity: the failed DML must have left the rows untouched.
    rows = run_query(f"SELECT count(*) FROM {table};", pg_conn)
    assert rows[0][0] == 3

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


def test_v3_dml_error_also_fires_for_explain(
    s3, pg_conn, extension, app_user, with_default_location
):
    """The gate is in PlanForeignModify, so EXPLAIN must surface the
    same error -- otherwise users would see a clean plan and then a
    runtime crash on EXECUTE.
    """
    table = "test_v3_dml_gate_explain"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 3);",
        pg_conn,
    )
    pg_conn.commit()

    with pytest.raises(
        psycopg2.Error,
        match="DELETE on Iceberg format-version 3 tables is not yet supported",
    ):
        run_command(f"EXPLAIN DELETE FROM {table} WHERE a = 1;", pg_conn)
    pg_conn.rollback()

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# v2 positive path: UPDATE / DELETE must still work end-to-end.
# Stage 16 is a v3 gate -- v2 must be unchanged.
# ---------------------------------------------------------------------------


def test_v2_delete_still_works(s3, pg_conn, extension, app_user, with_default_location):
    table = "test_v3_dml_gate_v2_delete"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 2);",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {table} VALUES (1), (2), (3);", pg_conn)
    pg_conn.commit()

    run_command(f"DELETE FROM {table} WHERE a = 1;", pg_conn)
    pg_conn.commit()

    rows = run_query(f"SELECT count(*) FROM {table};", pg_conn)
    assert rows[0][0] == 2

    rows = run_query(f"SELECT a FROM {table} ORDER BY a;", pg_conn)
    assert [r[0] for r in rows] == [2, 3]

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()


def test_v2_update_still_works(s3, pg_conn, extension, app_user, with_default_location):
    table = "test_v3_dml_gate_v2_update"
    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    run_command(
        f"CREATE TABLE {table} (a int) USING pg_lake_iceberg "
        "WITH (format_version = 2);",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"INSERT INTO {table} VALUES (1), (2), (3);", pg_conn)
    pg_conn.commit()

    run_command(f"UPDATE {table} SET a = a + 10 WHERE a = 1;", pg_conn)
    pg_conn.commit()

    rows = run_query(f"SELECT a FROM {table} ORDER BY a;", pg_conn)
    assert [r[0] for r in rows] == [2, 3, 11]

    run_command(f"DROP TABLE IF EXISTS {table};", pg_conn)
    pg_conn.commit()
