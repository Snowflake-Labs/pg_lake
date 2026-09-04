"""Regression tests for PostgreSQL 19 commands/features interacting with pg_lake.

PostgreSQL 19 introduces new statements that target regular tables (e.g.
REPACK).  pg_lake managed iceberg tables (CREATE TABLE ... USING iceberg) and
pg_lake foreign tables are backed by the pg_lake FDW, so these heap-rewrite
commands must either be rejected with a meaningful error or be a harmless
no-op that does not corrupt the table.  These tests pin that behaviour so a
future PostgreSQL or pg_lake change cannot silently regress it.

Gated to PG19+ because the statements do not parse on older servers.
"""

import pytest
from utils_pytest import *


def _skip_if_not_pg19(conn):
    if get_pg_version_num(conn) < 190000:
        pytest.skip("PG19-only commands require PostgreSQL 19+")


def _make_parquet_foreign_table(conn, name, select_sql, columns_sql):
    """Materialise `select_sql` into a Parquet file and expose it as a pg_lake
    foreign table `name` with the given column definition. Returns the URL."""
    url = f"s3://{TEST_BUCKET}/test_pg19_features/{name}.parquet"
    run_command(f"COPY ({select_sql}) TO '{url}' WITH (format 'parquet')", conn)
    run_command(f"DROP FOREIGN TABLE IF EXISTS {name}", conn)
    run_command(
        f"""
        CREATE FOREIGN TABLE {name} ({columns_sql})
        SERVER pg_lake OPTIONS (format 'parquet', path '{url}')
        """,
        conn,
    )
    conn.commit()
    return url


def _snapshot_contents(conn, relation):
    """Return the full table contents as a deterministically-ordered list of
    row literals, so two snapshots can be compared for exact equality."""
    rows = run_query(f"SELECT t::text AS row FROM {relation} t ORDER BY 1", conn)
    conn.commit()
    return [r["row"] for r in rows]


def _assert_rejected_or_harmless(conn, relation, repack_sql, expected_count):
    """REPACK must either raise a meaningful error or leave the data intact.

    pg_lake tables are foreign tables under the hood, so PostgreSQL core is
    expected to reject REPACK on them.  We accept a clean error *or* a no-op,
    but in either case the full table contents (not just the row count) must be
    byte-for-byte unchanged afterwards; we reject silent data loss/corruption
    and crashes.
    """
    before = _snapshot_contents(conn, relation)
    assert len(before) == expected_count

    error = run_command(repack_sql, conn, raise_error=False)
    conn.rollback()

    if error:
        # Must be a clean, intelligible error -- not an internal crash.
        lowered = error.lower()
        assert any(
            kw in lowered
            for kw in (
                "foreign table",
                "not a table",
                "cannot",
                "not supported",
                "is not",
            )
        ), f"REPACK on {relation} raised an unclear error: {error!r}"

    # Whether REPACK errored or silently no-op'd, the server must still be
    # alive and the table contents identical to the pre-REPACK snapshot.
    after = _snapshot_contents(conn, relation)
    assert (
        after == before
    ), f"REPACK on {relation} changed the table contents unexpectedly"


def test_repack_managed_iceberg_table(pg_conn, s3, extension, with_default_location):
    """REPACK on a managed iceberg table must not corrupt it or crash PG."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_repack_iceberg", pg_conn)
    run_command(
        "CREATE TABLE pg19_repack_iceberg (id int) USING iceberg",
        pg_conn,
    )
    run_command(
        "INSERT INTO pg19_repack_iceberg SELECT generate_series(1, 100)",
        pg_conn,
    )
    pg_conn.commit()

    _assert_rejected_or_harmless(
        pg_conn, "pg19_repack_iceberg", "REPACK pg19_repack_iceberg", 100
    )

    run_command("DROP TABLE IF EXISTS pg19_repack_iceberg", pg_conn)
    pg_conn.commit()


def test_repack_foreign_table(pg_conn, s3, extension):
    """REPACK on a pg_lake foreign table must not corrupt it or crash PG."""
    _skip_if_not_pg19(pg_conn)

    url = f"s3://{TEST_BUCKET}/test_pg19_repack/data.parquet"
    run_command(
        f"COPY (SELECT s AS id FROM generate_series(1, 100) s) TO '{url}'",
        pg_conn,
    )
    run_command("DROP FOREIGN TABLE IF EXISTS pg19_repack_fdw", pg_conn)
    run_command(
        f"""
        CREATE FOREIGN TABLE pg19_repack_fdw (id int)
        SERVER pg_lake OPTIONS (format 'parquet', path '{url}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    _assert_rejected_or_harmless(
        pg_conn, "pg19_repack_fdw", "REPACK pg19_repack_fdw", 100
    )

    run_command("DROP FOREIGN TABLE IF EXISTS pg19_repack_fdw", pg_conn)
    pg_conn.commit()


# PG19 added IGNORE NULLS / RESPECT NULLS null-treatment to window functions
# (lead/lag/first_value/last_value/nth_value). pg_get_querydef (core PG19) emits
# the treatment *outside* the argument parens ("lag(v) IGNORE NULLS OVER ..."),
# so pushing such a query to DuckDB only yields correct results if DuckDB both
# parses that spelling and applies the same null skipping. These shapes pin
# correctness against a heap baseline; the data interleaves NULLs so that
# IGNORE vs RESPECT produce visibly different output.
# DuckDB's parser rejects the IGNORE NULLS spelling that core PG19 deparses, so
# pg_lake refuses to push these down (see the WindowFunc->ignore_nulls guard in
# query_pushdown.c) and runs them locally. Each must still match a heap baseline;
# the data interleaves NULLs so IGNORE vs RESPECT produce visibly different rows.
_IGNORE_NULLS_SHAPES = [
    "SELECT o, lag(v) IGNORE NULLS OVER w AS r FROM {t} WINDOW w AS (ORDER BY o)",
    "SELECT o, lead(v) IGNORE NULLS OVER w AS r FROM {t} WINDOW w AS (ORDER BY o)",
    "SELECT o, first_value(v) IGNORE NULLS OVER w AS r FROM {t} "
    "WINDOW w AS (ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
    "SELECT o, last_value(v) IGNORE NULLS OVER w AS r FROM {t} "
    "WINDOW w AS (ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
    "SELECT o, nth_value(v, 2) IGNORE NULLS OVER w AS r FROM {t} "
    "WINDOW w AS (ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
]

# RESPECT NULLS is the default; parse analysis collapses it to NO_NULLTREATMENT,
# so it carries no clause and is pushed down to DuckDB normally.
_RESPECT_NULLS_SHAPE = (
    "SELECT o, lag(v) RESPECT NULLS OVER w AS r FROM {t} WINDOW w AS (ORDER BY o)"
)


def test_window_ignore_nulls_correctness(pg_conn, s3, extension, with_default_location):
    """PG19 window IGNORE NULLS over a managed iceberg table must match an
    identical heap table. DuckDB cannot parse core PG19's deparsed IGNORE NULLS
    spelling, so pg_lake runs these locally; assert both correctness and that
    they are not pushed down (so the not-shippable guard keeps firing). The
    default RESPECT NULLS form still pushes down and must stay correct."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS wn_lake", pg_conn)
    run_command("DROP TABLE IF EXISTS wn_heap", pg_conn)
    run_command("CREATE TABLE wn_lake (o int, v int) USING iceberg", pg_conn)
    run_command("CREATE TABLE wn_heap (o int, v int)", pg_conn)
    data = """
        INSERT INTO {t} (o, v) VALUES
            (1, 10), (2, NULL), (3, 30), (4, NULL), (5, NULL),
            (6, 60), (7, NULL), (8, 80)
    """
    run_command(data.format(t="wn_lake"), pg_conn)
    run_command(data.format(t="wn_heap"), pg_conn)
    pg_conn.commit()

    for shape in _IGNORE_NULLS_SHAPES:
        query = shape.format(t="wn_lake")
        assert_query_results_on_tables(query, pg_conn, ["wn_lake"], ["wn_heap"])
        assert_query_not_pushdownable(
            query, pg_conn, f"IGNORE NULLS must not be pushed down: {query}"
        )

    respect_query = _RESPECT_NULLS_SHAPE.format(t="wn_lake")
    assert_query_results_on_tables(respect_query, pg_conn, ["wn_lake"], ["wn_heap"])

    run_command("DROP TABLE IF EXISTS wn_lake", pg_conn)
    run_command("DROP TABLE IF EXISTS wn_heap", pg_conn)
    pg_conn.commit()


def test_check_constraint_enforced_default_iceberg(
    pg_conn, s3, extension, with_default_location
):
    """A CHECK constraint on a managed iceberg table is ENFORCED by default
    (PG19 makes ENFORCED explicit): a row violating it must be rejected, a
    conforming row must be accepted."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_chk", pg_conn)
    run_command(
        "CREATE TABLE pg19_chk (a int, b int CHECK (b > 0)) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()

    err = run_command("INSERT INTO pg19_chk VALUES (1, -5)", pg_conn, raise_error=False)
    pg_conn.rollback()
    assert (
        err and "pg19_chk_b_check" in err
    ), f"ENFORCED CHECK must reject the violating row, got: {err!r}"

    run_command("INSERT INTO pg19_chk VALUES (1, 5)", pg_conn)
    pg_conn.commit()
    rows = run_query("SELECT count(*) AS n FROM pg19_chk", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 1

    run_command("DROP TABLE IF EXISTS pg19_chk", pg_conn)
    pg_conn.commit()


def test_check_constraint_not_enforced_iceberg(
    pg_conn, s3, extension, with_default_location
):
    """PG19 allows a CHECK constraint to be declared NOT ENFORCED at table
    creation. On a managed iceberg table such a constraint must NOT reject
    violating rows (core ExecRelCheck skips unenforced checks, and pg_lake
    delegates enforcement to it)."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_chk_ne", pg_conn)
    run_command(
        "CREATE TABLE pg19_chk_ne (a int, b int, "
        "CONSTRAINT b_pos CHECK (b > 0) NOT ENFORCED) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()

    # NOT ENFORCED: the violating row is accepted, unlike the enforced case.
    run_command("INSERT INTO pg19_chk_ne VALUES (1, -5)", pg_conn)
    pg_conn.commit()
    rows = run_query("SELECT count(*) AS n FROM pg19_chk_ne", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 1

    # Managed iceberg tables are foreign-table backed, so core PG rejects the
    # PG19 ALTER CONSTRAINT ... ENFORCED toggle outright. Pin that this is a
    # clean rejection (not a crash or silent no-op that would change semantics).
    enforce_err = run_command(
        "ALTER TABLE pg19_chk_ne ALTER CONSTRAINT b_pos ENFORCED",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert (
        enforce_err and "alter constraint" in enforce_err.lower()
    ), f"ALTER CONSTRAINT ENFORCED on iceberg should be cleanly rejected, got: {enforce_err!r}"

    run_command("DROP TABLE IF EXISTS pg19_chk_ne", pg_conn)
    pg_conn.commit()


def test_set_expression_rejected_iceberg(pg_conn, s3, extension, with_default_location):
    """ALTER COLUMN ... SET EXPRESSION (PG19 extends this to virtual generated
    columns) is not supported on managed iceberg tables and must be rejected
    cleanly rather than crash or silently no-op."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_setexpr", pg_conn)
    run_command(
        "CREATE TABLE pg19_setexpr (a int, g int GENERATED ALWAYS AS (a * 2) STORED) "
        "USING iceberg",
        pg_conn,
    )
    pg_conn.commit()

    err = run_command(
        "ALTER TABLE pg19_setexpr ALTER COLUMN g SET EXPRESSION AS (a * 3)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert err, "SET EXPRESSION on an iceberg column should be rejected"
    lowered = err.lower()
    assert (
        "set expression" in lowered or "not supported" in lowered or "cannot" in lowered
    ), f"SET EXPRESSION rejection should be intelligible, got: {err!r}"

    run_command("DROP TABLE IF EXISTS pg19_setexpr", pg_conn)
    pg_conn.commit()


def test_on_conflict_do_select_managed_iceberg(
    pg_conn, s3, extension, with_default_location
):
    """PG19 INSERT ... ON CONFLICT DO SELECT must error meaningfully (no unique
    index support on managed iceberg tables) rather than crash."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_on_conflict", pg_conn)
    run_command(
        "CREATE TABLE pg19_on_conflict (id int, v text) USING iceberg",
        pg_conn,
    )
    run_command("INSERT INTO pg19_on_conflict VALUES (1, 'a')", pg_conn)
    pg_conn.commit()

    # DO SELECT requires a RETURNING clause; include it so we actually reach
    # the conflict-arbiter resolution against the managed iceberg table.
    error = run_command(
        """
        INSERT INTO pg19_on_conflict VALUES (1, 'b')
        ON CONFLICT (id) DO SELECT RETURNING *
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    # Managed iceberg tables have no unique index / arbiter, so this must be
    # rejected with a clear error, not an internal failure.
    assert error, "Expected ON CONFLICT DO SELECT to be rejected"
    lowered = error.lower()
    assert any(
        kw in lowered
        for kw in (
            "no unique",
            "constraint",
            "on conflict",
            "not supported",
            "cannot",
            "unique or exclusion",
            "arbiter",
        )
    ), f"ON CONFLICT DO SELECT raised an unclear error: {error!r}"

    # Server still alive and table intact.
    rows = run_query("SELECT count(*) AS n FROM pg19_on_conflict", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 1

    run_command("DROP TABLE IF EXISTS pg19_on_conflict", pg_conn)
    pg_conn.commit()


def test_repack_concurrently_managed_iceberg(
    pg_conn, s3, extension, with_default_location
):
    """PG19 also adds REPACK CONCURRENTLY; it must be handled as cleanly as the
    plain form on a managed iceberg table (foreign table under the hood)."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_repack_conc", pg_conn)
    run_command("CREATE TABLE pg19_repack_conc (id int) USING iceberg", pg_conn)
    run_command("INSERT INTO pg19_repack_conc SELECT generate_series(1, 50)", pg_conn)
    pg_conn.commit()

    _assert_rejected_or_harmless(
        pg_conn,
        "pg19_repack_conc",
        "REPACK (CONCURRENTLY) pg19_repack_conc",
        50,
    )

    run_command("DROP TABLE IF EXISTS pg19_repack_conc", pg_conn)
    pg_conn.commit()


def test_on_conflict_do_select_foreign_table(pg_conn, s3, extension):
    """ON CONFLICT DO SELECT against a read-backed pg_lake foreign table must be
    rejected with a clear error (no arbiter index), not an internal failure."""
    _skip_if_not_pg19(pg_conn)

    _make_parquet_foreign_table(
        pg_conn,
        "pg19_oc_ft",
        "SELECT s AS id, 'v' || s AS v FROM generate_series(1, 5) s",
        "id int, v text",
    )

    error = run_command(
        """
        INSERT INTO pg19_oc_ft VALUES (1, 'dup')
        ON CONFLICT (id) DO SELECT RETURNING *
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error, "Expected ON CONFLICT DO SELECT on a foreign table to be rejected"
    lowered = error.lower()
    assert any(
        kw in lowered
        for kw in (
            "no unique",
            "constraint",
            "on conflict",
            "not supported",
            "cannot",
            "unique or exclusion",
            "arbiter",
            "foreign table",
        )
    ), f"ON CONFLICT DO SELECT on foreign table raised an unclear error: {error!r}"

    run_command("DROP FOREIGN TABLE IF EXISTS pg19_oc_ft", pg_conn)
    pg_conn.commit()


def test_copy_to_partitioned_parent_direct(pg_conn, s3, extension):
    """PG19 (commit 4bea91f21) lets COPY a partitioned table directly. pg_lake
    now supports this for lake-format COPY too: COPY <parent> TO descends into
    the partitions (parent holds no rows) and round-trips. The legacy
    COPY (SELECT ...) form must keep working as well.
    """
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_copy_part CASCADE", pg_conn)
    run_command("DROP TABLE IF EXISTS pg19_copy_part_rt", pg_conn)
    run_command(
        """
        CREATE TABLE pg19_copy_part (id int, v text) PARTITION BY RANGE (id);
        CREATE TABLE pg19_copy_part_1 PARTITION OF pg19_copy_part
            FOR VALUES FROM (0) TO (100);
        CREATE TABLE pg19_copy_part_2 PARTITION OF pg19_copy_part
            FOR VALUES FROM (100) TO (200);
        INSERT INTO pg19_copy_part SELECT g, 'v' || g FROM generate_series(0, 199) g;
        """,
        pg_conn,
    )
    pg_conn.commit()

    url = f"s3://{TEST_BUCKET}/test_pg19_features/copy_part.parquet"

    # Direct COPY <partitioned parent> TO now succeeds on PG19.
    run_command(
        f"COPY pg19_copy_part TO '{url}' WITH (format 'parquet')",
        pg_conn,
    )
    pg_conn.commit()

    run_command("CREATE TABLE pg19_copy_part_rt (id int, v text)", pg_conn)
    run_command(f"COPY pg19_copy_part_rt FROM '{url}' WITH (format 'parquet')", pg_conn)
    pg_conn.commit()

    rows = run_query("SELECT count(*) AS n FROM pg19_copy_part_rt", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 200

    # The legacy COPY (SELECT ...) TO form must keep working as well.
    run_command("TRUNCATE pg19_copy_part_rt", pg_conn)
    run_command(
        f"COPY (SELECT * FROM pg19_copy_part) TO '{url}' WITH (format 'parquet')",
        pg_conn,
    )
    run_command(f"COPY pg19_copy_part_rt FROM '{url}' WITH (format 'parquet')", pg_conn)
    pg_conn.commit()
    rows = run_query("SELECT count(*) AS n FROM pg19_copy_part_rt", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 200

    run_command("DROP TABLE IF EXISTS pg19_copy_part CASCADE", pg_conn)
    run_command("DROP TABLE IF EXISTS pg19_copy_part_rt", pg_conn)
    pg_conn.commit()


def test_merge_partitions_with_lake_partitions(pg_conn, s3, extension):
    """PG19 ALTER TABLE ... MERGE PARTITIONS cannot merge pg_lake foreign-table
    partitions: core rejects them because a foreign table "is not a table".
    Pin that exact rejection and that the data survives unchanged."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_merge_parent CASCADE", pg_conn)
    run_command(
        "CREATE TABLE pg19_merge_parent (id int) PARTITION BY RANGE (id)", pg_conn
    )
    pg_conn.commit()

    for part in range(2):
        lo, hi = part * 100, (part + 1) * 100
        url = f"s3://{TEST_BUCKET}/test_pg19_features/merge_part{part}.parquet"
        run_command(
            f"COPY (SELECT generate_series({lo}, {hi - 1}) AS id) "
            f"TO '{url}' WITH (format 'parquet')",
            pg_conn,
        )
        run_command(
            f"""
            CREATE FOREIGN TABLE pg19_merge_part{part}
                PARTITION OF pg19_merge_parent FOR VALUES FROM ({lo}) TO ({hi})
            SERVER pg_lake OPTIONS (format 'parquet', path '{url}')
            """,
            pg_conn,
        )
    pg_conn.commit()

    error = run_command(
        """
        ALTER TABLE pg19_merge_parent
        MERGE PARTITIONS (pg19_merge_part0, pg19_merge_part1) INTO pg19_merge_merged
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error and "is not a table" in error, (
        f"MERGE PARTITIONS over a foreign-table partition must be rejected with "
        f'"is not a table", got: {error!r}'
    )

    # The failed ALTER must leave the existing partitions and their data intact.
    rows = run_query("SELECT count(*) AS n FROM pg19_merge_parent", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 200

    run_command("DROP TABLE IF EXISTS pg19_merge_parent CASCADE", pg_conn)
    pg_conn.commit()


def test_split_partition_with_lake_partition(pg_conn, s3, extension):
    """PG19 ALTER TABLE ... SPLIT PARTITION cannot split a pg_lake foreign-table
    partition: core rejects it because a foreign table "is not a table". Pin
    that exact rejection and that the data survives unchanged."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_split_parent CASCADE", pg_conn)
    run_command(
        "CREATE TABLE pg19_split_parent (id int) PARTITION BY RANGE (id)", pg_conn
    )
    url = f"s3://{TEST_BUCKET}/test_pg19_features/split_part.parquet"
    run_command(
        f"COPY (SELECT generate_series(0, 199) AS id) "
        f"TO '{url}' WITH (format 'parquet')",
        pg_conn,
    )
    run_command(
        f"""
        CREATE FOREIGN TABLE pg19_split_part
            PARTITION OF pg19_split_parent FOR VALUES FROM (0) TO (200)
        SERVER pg_lake OPTIONS (format 'parquet', path '{url}')
        """,
        pg_conn,
    )
    pg_conn.commit()

    error = run_command(
        """
        ALTER TABLE pg19_split_parent SPLIT PARTITION pg19_split_part INTO (
            PARTITION pg19_split_a FOR VALUES FROM (0) TO (100),
            PARTITION pg19_split_b FOR VALUES FROM (100) TO (200)
        )
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error and "is not a table" in error, (
        f"SPLIT PARTITION over a foreign-table partition must be rejected with "
        f'"is not a table", got: {error!r}'
    )

    # The failed ALTER must leave the existing partition and its data intact.
    rows = run_query("SELECT count(*) AS n FROM pg19_split_parent", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 200

    run_command("DROP TABLE IF EXISTS pg19_split_parent CASCADE", pg_conn)
    pg_conn.commit()


def test_on_conflict_do_select_for_update_managed_iceberg(
    pg_conn, s3, extension, with_default_location
):
    """PG19 also allows the locking clause on INSERT ... ON CONFLICT DO SELECT
    (e.g. DO SELECT FOR UPDATE). Managed iceberg tables have no unique index to
    serve as a conflict arbiter, so this must be rejected with a clear error
    rather than crash, exactly like the non-locking DO SELECT form."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_ocfu", pg_conn)
    run_command("CREATE TABLE pg19_ocfu (id int, v text) USING iceberg", pg_conn)
    run_command("INSERT INTO pg19_ocfu VALUES (1, 'a')", pg_conn)
    pg_conn.commit()

    error = run_command(
        """
        INSERT INTO pg19_ocfu VALUES (1, 'b')
        ON CONFLICT (id) DO SELECT FOR UPDATE RETURNING *
        """,
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error, "Expected ON CONFLICT DO SELECT FOR UPDATE to be rejected"
    lowered = error.lower()
    assert any(
        kw in lowered
        for kw in ("no unique", "unique or exclusion", "arbiter", "constraint")
    ), f"ON CONFLICT DO SELECT FOR UPDATE raised an unclear error: {error!r}"

    rows = run_query("SELECT count(*) AS n FROM pg19_ocfu", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 1

    run_command("DROP TABLE IF EXISTS pg19_ocfu", pg_conn)
    pg_conn.commit()


def test_update_for_portion_of_rejected_iceberg(
    pg_conn, s3, extension, with_default_location
):
    """PG19 adds UPDATE/DELETE ... FOR PORTION OF for temporal range columns.
    pg_lake tables are foreign-table backed, and core PG explicitly rejects
    FOR PORTION OF on foreign tables. Pin that both UPDATE and DELETE forms are
    cleanly rejected and leave the data untouched."""
    _skip_if_not_pg19(pg_conn)

    run_command("DROP TABLE IF EXISTS pg19_portion", pg_conn)
    run_command(
        "CREATE TABLE pg19_portion (id int, valid int4range) USING iceberg", pg_conn
    )
    run_command("INSERT INTO pg19_portion VALUES (1, '[1,10)')", pg_conn)
    pg_conn.commit()

    update_err = run_command(
        "UPDATE pg19_portion FOR PORTION OF valid FROM 3 TO 5 SET id = 2",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert update_err and "for portion of" in update_err.lower(), (
        f"UPDATE FOR PORTION OF on iceberg must be cleanly rejected, "
        f"got: {update_err!r}"
    )

    delete_err = run_command(
        "DELETE FROM pg19_portion FOR PORTION OF valid FROM 3 TO 5",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert delete_err and "for portion of" in delete_err.lower(), (
        f"DELETE FOR PORTION OF on iceberg must be cleanly rejected, "
        f"got: {delete_err!r}"
    )

    rows = run_query("SELECT count(*) AS n FROM pg19_portion", pg_conn)
    pg_conn.commit()
    assert int(rows[0]["n"]) == 1

    run_command("DROP TABLE IF EXISTS pg19_portion", pg_conn)
    pg_conn.commit()


def test_property_graph_over_lake_table_rejected(pg_conn, s3, extension):
    """PG19 adds SQL/PGQ property graphs and GRAPH_TABLE. Defining a property
    graph requires every element table to have a key, but pg_lake foreign tables
    cannot declare a primary key, so CREATE PROPERTY GRAPH over a pg_lake table
    is cleanly rejected (no key). Pin that this is a clear error rather than a
    crash, documenting that pg_lake tables are not usable as graph elements."""
    _skip_if_not_pg19(pg_conn)

    url = _make_parquet_foreign_table(
        pg_conn,
        "pg19_graph_ft",
        "SELECT 1 AS id, 2 AS dst",
        "id int, dst int",
    )
    assert url

    error = run_command(
        "CREATE PROPERTY GRAPH pg19_graph VERTEX TABLES (pg19_graph_ft)",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()

    assert error, "Expected CREATE PROPERTY GRAPH over a pg_lake table to be rejected"
    lowered = error.lower()
    assert (
        "key" in lowered
    ), f"CREATE PROPERTY GRAPH over a pg_lake table raised an unclear error: {error!r}"

    run_command("DROP FOREIGN TABLE IF EXISTS pg19_graph_ft CASCADE", pg_conn)
    pg_conn.commit()
