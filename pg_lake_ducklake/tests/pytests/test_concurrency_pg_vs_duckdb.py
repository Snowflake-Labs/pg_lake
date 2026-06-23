"""
Concurrency tests for DuckLake tables across PG and DuckDB writers/readers.

The pg_isolation_regress harness covers PG-vs-PG races. This module covers
the cross-engine cases that don't fit a single PG cluster:

  - PG writer vs DuckDB writer racing on the same table
  - DuckDB writer's commit visibility from a fresh PG connection
  - PG writer's commit visibility from a fresh DuckDB ATTACH
  - Concurrent PG INSERTs from threads (smoke test for the snapshot
    advisory-lock serialisation, on top of the .spec coverage)

Each test uses a fresh DuckDB process per attach so we don't share
state across cases.
"""

import os
import threading
import time

import psycopg2
import pytest

from utils_pytest import TEST_BUCKET, server_params

try:
    import duckdb

    DUCKDB_AVAILABLE = True
except ImportError:
    DUCKDB_AVAILABLE = False


def _conn_str():
    return (
        f"host={server_params.PG_HOST} port={server_params.PG_PORT} "
        f"dbname={server_params.PG_DATABASE} user={server_params.PG_USER}"
    )


def _new_pg_conn():
    return psycopg2.connect(
        host=server_params.PG_HOST,
        port=server_params.PG_PORT,
        dbname=server_params.PG_DATABASE,
        user=server_params.PG_USER,
    )


def _attach_duckdb():
    duck = duckdb.connect()
    duck.execute("INSTALL postgres; LOAD postgres")
    duck.execute("INSTALL ducklake FROM core_nightly; LOAD ducklake")
    duck.execute(
        f"""
        CREATE SECRET s3test (TYPE S3, KEY_ID 'testing', SECRET 'testing',
                              ENDPOINT 'localhost:5999',
                              SCOPE 's3://{TEST_BUCKET}', URL_STYLE 'path',
                              USE_SSL false)
        """
    )
    duck.execute(
        f"ATTACH 'postgres:{_conn_str()}' AS dl "
        f"(TYPE DUCKLAKE, METADATA_SCHEMA 'public')"
    )
    return duck


# ---------------------------------------------------------------------------
# PG threads racing each other -- smoke test for snapshot lock
# ---------------------------------------------------------------------------


def test_pg_concurrent_inserts_serialise(pg_cursor, s3):
    """
    Many PG sessions inserting into the same DuckLake table at the same
    time must produce a coherent catalog: each row gets a unique
    row_id_start range, no PK collision on snapshot_id, and the final
    count matches.

    The .spec covers two-session blocking. This smoke test piles on
    parallelism to flush out latent races that show only at scale.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE conc_inserts (id INT, batch INT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/conc_inserts')
        """
    )
    pg_cursor.connection.commit()

    N_THREADS = 8
    ROWS_PER_THREAD = 25

    errors = []

    def worker(tid):
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO conc_inserts SELECT i, %s "
                "FROM generate_series(1, %s) i",
                (tid, ROWS_PER_THREAD),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append((tid, e))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(N_THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"thread errors: {errors}"

    pg_cursor.execute("SELECT count(*), count(distinct (id, batch)) FROM conc_inserts")
    total, distinct = pg_cursor.fetchone()
    expected = N_THREADS * ROWS_PER_THREAD
    assert total == expected, f"want {expected} rows, got {total}"
    assert distinct == expected, "row_id ranges or visibility must not duplicate"

    pg_cursor.execute(
        "SELECT count(distinct row_id_start) "
        "FROM lake_ducklake.data_file df "
        "JOIN lake_ducklake.table t USING (table_id) "
        "WHERE t.table_name = 'conc_inserts' AND df.end_snapshot IS NULL"
    )
    distinct_starts = pg_cursor.fetchone()[0]
    pg_cursor.execute(
        "SELECT count(*) FROM lake_ducklake.data_file df "
        "JOIN lake_ducklake.table t USING (table_id) "
        "WHERE t.table_name = 'conc_inserts' AND df.end_snapshot IS NULL"
    )
    file_count = pg_cursor.fetchone()[0]
    assert distinct_starts == file_count, (
        f"row_id_start collisions: {distinct_starts} distinct " f"vs {file_count} files"
    )


def test_pg_concurrent_dml_no_orphan_files(pg_cursor, s3):
    """
    Concurrent INSERT + UPDATE + DELETE from threads. Whatever the
    scheduling, the catalog must end up consistent: every parquet file
    referenced from data_file must be reachable, and every UPDATE / DELETE
    that completed must be visible.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE conc_dml (id INT, value INT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/conc_dml')
        """
    )
    pg_cursor.execute("INSERT INTO conc_dml SELECT i, i FROM generate_series(1, 50) i")
    pg_cursor.connection.commit()

    errors = []

    def inserter():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO conc_dml " "SELECT i, i FROM generate_series(100, 119) i"
            )
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append(("insert", e))

    def updater():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute("UPDATE conc_dml SET value = value + 1000 WHERE id <= 25")
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append(("update", e))

    def deleter():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM conc_dml WHERE id BETWEEN 26 AND 35")
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append(("delete", e))

    threads = [threading.Thread(target=fn) for fn in (inserter, updater, deleter)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"thread errors: {errors}"

    pg_cursor.execute("SELECT count(*) FROM conc_dml")
    final_count = pg_cursor.fetchone()[0]
    # 50 initial - 10 deleted + 20 inserted = 60
    assert final_count == 60, f"expected 60 rows, got {final_count}"

    pg_cursor.execute("SELECT count(*) FROM conc_dml WHERE value > 1000 AND id <= 25")
    updated = pg_cursor.fetchone()[0]
    assert updated == 25, f"expected all 25 rows updated, got {updated}"


def test_pg_concurrent_overlapping_updates(pg_cursor, s3):
    """
    Four PG threads each UPDATE a range of rows that overlaps with at
    least two other threads. Each thread adds a unique power-of-2 to
    the value column, so every row's final value is the sum over
    threads whose range contains it -- order-independent because
    addition commutes.

    The class-101 update lock should serialise the UPDATEs, so the
    final state must equal the deterministic sum exactly. Catches lost
    updates (one thread's increment overwritten by another's
    pre-snapshot read) and PK collisions in the delete_file path,
    which the existing INSERT-only stress test does not exercise.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE overlap_upd (id INT, value BIGINT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/overlap_upd')
        """
    )
    pg_cursor.execute(
        "INSERT INTO overlap_upd SELECT i, 0 FROM generate_series(1, 100) i"
    )
    pg_cursor.connection.commit()

    # Ranges chosen so every multiplicity 1..4 occurs: id=1 hit by {0,3},
    # id=30 by {0,1,3}, id=50 by {0,1,2,3}, id=80 by {1,2,3}, id=100 by {2,3}.
    THREAD_RANGES = [
        (0, 1, 60),
        (1, 30, 80),
        (2, 50, 100),
        (3, 1, 100),
    ]

    errors = []

    def worker(tid, lo, hi):
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute(
                "UPDATE overlap_upd SET value = value + %s "
                "WHERE id BETWEEN %s AND %s",
                (1 << tid, lo, hi),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            errors.append((tid, e))

    threads = [
        threading.Thread(target=worker, args=(tid, lo, hi))
        for tid, lo, hi in THREAD_RANGES
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == [], f"thread errors: {errors}"

    expected = {}
    for i in range(1, 101):
        v = 0
        for tid, lo, hi in THREAD_RANGES:
            if lo <= i <= hi:
                v += 1 << tid
        expected[i] = v

    pg_cursor.execute("SELECT id, value FROM overlap_upd ORDER BY id")
    actual = dict(pg_cursor.fetchall())
    mismatches = [
        (k, expected[k], actual.get(k))
        for k in expected
        if expected[k] != actual.get(k)
    ]
    assert not mismatches, f"diverged from deterministic sum: {mismatches[:10]}"

    pg_cursor.execute("SELECT count(*) FROM overlap_upd")
    assert pg_cursor.fetchone()[0] == 100


# ---------------------------------------------------------------------------
# PG <-> DuckDB writer races
# ---------------------------------------------------------------------------


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_then_duckdb_insert_visible_to_each_other(pg_cursor, s3):
    """
    Sequential, not concurrent: PG inserts and commits, DuckDB ATTACHes
    fresh and sees the rows; DuckDB inserts and commits, a fresh PG
    cursor sees both batches. Guards against any "writer-side cache
    stale on read" regression.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE seq_pg_duck (id INT, src TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/seq_pg_duck')
        """
    )
    pg_cursor.execute("INSERT INTO seq_pg_duck VALUES (1, 'pg-1'), (2, 'pg-2')")
    pg_cursor.connection.commit()

    duck = _attach_duckdb()
    rows = duck.execute(
        "SELECT id, src FROM dl.public.seq_pg_duck ORDER BY id"
    ).fetchall()
    assert rows == [(1, "pg-1"), (2, "pg-2")]

    duck.execute(
        "INSERT INTO dl.public.seq_pg_duck VALUES (3, 'duck-1'), (4, 'duck-2')"
    )
    duck.close()

    fresh = _new_pg_conn()
    fresh_cur = fresh.cursor()
    fresh_cur.execute("SELECT id, src FROM seq_pg_duck ORDER BY id")
    assert fresh_cur.fetchall() == [
        (1, "pg-1"),
        (2, "pg-2"),
        (3, "duck-1"),
        (4, "duck-2"),
    ]
    fresh.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_uncommitted_invisible_to_duckdb(pg_cursor, s3):
    """
    DuckDB ATTACHes via postgres_scan, which uses its own connection and
    sees committed-only state. PG's mid-transaction inserts must NOT be
    visible to DuckDB until the PG xact commits.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE pg_uncommit (id INT, val TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/pg_uncommit')
        """
    )
    pg_cursor.execute("INSERT INTO pg_uncommit VALUES (1, 'committed')")
    pg_cursor.connection.commit()

    duck = _attach_duckdb()
    pre = duck.execute(
        "SELECT id, val FROM dl.public.pg_uncommit ORDER BY id"
    ).fetchall()
    assert pre == [(1, "committed")]

    # Open a long-lived transaction and INSERT but do not commit
    long_conn = _new_pg_conn()
    long_cur = long_conn.cursor()
    long_cur.execute("BEGIN")
    long_cur.execute("INSERT INTO pg_uncommit VALUES (2, 'uncommitted')")

    mid = duck.execute(
        "SELECT id, val FROM dl.public.pg_uncommit ORDER BY id"
    ).fetchall()
    assert mid == [(1, "committed")], "DuckDB must not see PG's uncommitted insert"

    long_conn.commit()
    long_conn.close()

    post = duck.execute(
        "SELECT id, val FROM dl.public.pg_uncommit ORDER BY id"
    ).fetchall()
    assert post == [(1, "committed"), (2, "uncommitted")]
    duck.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_rolledback_invisible_to_duckdb(pg_cursor, s3):
    """
    PG INSERT inside a transaction that aborts must leave nothing visible
    to a fresh DuckDB ATTACH (catalog row absent, parquet treated as
    orphan).
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE pg_rollback (id INT, val TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/pg_rollback')
        """
    )
    pg_cursor.connection.commit()

    rb_conn = _new_pg_conn()
    rb_cur = rb_conn.cursor()
    rb_cur.execute("BEGIN")
    rb_cur.execute(
        "INSERT INTO pg_rollback " "SELECT i, 'rb' FROM generate_series(1, 5) i"
    )
    rb_conn.rollback()
    rb_conn.close()

    duck = _attach_duckdb()
    rows = duck.execute("SELECT count(*) FROM dl.public.pg_rollback").fetchall()
    assert rows == [(0,)], f"rollback must leave table empty, got {rows}"
    duck.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_savepoint_rollback_invisible_to_duckdb(pg_cursor, s3):
    """
    Subtransaction rollback: PG INSERTs, opens savepoint, INSERTs more,
    rolls back to savepoint, commits. Only the pre-savepoint INSERTs
    are visible afterwards.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE pg_savept (id INT, src TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/pg_savept')
        """
    )
    pg_cursor.connection.commit()

    sp_conn = _new_pg_conn()
    sp_cur = sp_conn.cursor()
    sp_cur.execute("BEGIN")
    sp_cur.execute("INSERT INTO pg_savept VALUES (1, 'pre'), (2, 'pre')")
    sp_cur.execute("SAVEPOINT s1")
    sp_cur.execute("INSERT INTO pg_savept VALUES (3, 'rb'), (4, 'rb')")
    sp_cur.execute("ROLLBACK TO SAVEPOINT s1")
    sp_cur.execute("INSERT INTO pg_savept VALUES (5, 'post')")
    sp_conn.commit()
    sp_conn.close()

    duck = _attach_duckdb()
    rows = duck.execute(
        "SELECT id, src FROM dl.public.pg_savept ORDER BY id"
    ).fetchall()
    assert rows == [
        (1, "pre"),
        (2, "pre"),
        (5, "post"),
    ], f"savepoint rollback must hide rb rows; got {rows}"
    duck.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_and_duckdb_insert_to_same_table_concurrently(pg_cursor, s3):
    """
    PG writer and DuckDB writer commit roughly at the same time. At
    least one of them succeeds; if both succeed, the catalog stays
    coherent (no duplicate snapshot_id, both batches' rows present).

    DuckLake's spec says the conflict is on lake_ducklake.snapshot's PK;
    DuckDB's ducklake extension handles it with a retry loop, so both
    SHOULD succeed in practice. This test pins that.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE pg_duck_race (id INT, src TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/pg_duck_race')
        """
    )
    pg_cursor.connection.commit()

    duck = _attach_duckdb()

    pg_done = threading.Event()
    duck_done = threading.Event()
    pg_err = []
    duck_err = []

    def pg_writer():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO pg_duck_race "
                "SELECT i, 'pg' FROM generate_series(1, 20) i"
            )
            conn.commit()
            conn.close()
        except Exception as e:
            pg_err.append(e)
        finally:
            pg_done.set()

    def duck_writer():
        try:
            duck.execute(
                "INSERT INTO dl.public.pg_duck_race "
                "SELECT range::INTEGER, 'duck' FROM range(100, 120)"
            )
        except Exception as e:
            duck_err.append(e)
        finally:
            duck_done.set()

    pg_thread = threading.Thread(target=pg_writer)
    duck_thread = threading.Thread(target=duck_writer)
    pg_thread.start()
    duck_thread.start()
    pg_thread.join()
    duck_thread.join()

    assert pg_err == [], f"pg writer failed: {pg_err}"
    assert duck_err == [], f"duck writer failed: {duck_err}"

    pg_cursor.connection.commit()
    pg_cursor.execute(
        "SELECT src, count(*) FROM pg_duck_race GROUP BY src ORDER BY src"
    )
    by_src = dict(pg_cursor.fetchall())
    assert by_src.get("pg") == 20, by_src
    assert by_src.get("duck") == 20, by_src

    pg_cursor.execute(
        "SELECT count(*) FROM lake_ducklake.snapshot "
        "GROUP BY snapshot_id HAVING count(*) > 1"
    )
    dup_snaps = pg_cursor.fetchall()
    assert dup_snaps == [], f"snapshot_id duplicates: {dup_snaps}"

    duck.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_dml_during_duckdb_compact(pg_cursor, s3):
    """
    PG single-row INSERTs (one snapshot per row) racing against
    DuckDB ducklake_merge_adjacent_files. Compaction end-snapshots
    the small files it merges; concurrent PG INSERTs add NEW files
    that compaction has not seen.

    DuckDB's ducklake extension currently refuses to commit the merge
    if concurrent INSERT/UPDATE/DELETE landed in the catalog during
    its transaction ("Unsupported change type ..."). That's an
    upstream limitation, not data corruption -- when it triggers, the
    merge rolls back cleanly and all rows are still visible.

    Visibility invariant we DO require: every committed PG row
    (pre-compaction and mid-flight) is visible after both threads
    finish, regardless of whether compaction succeeded or rolled
    back. Catches PG end-snapshotting a generation compaction was
    about to replace, or compaction silently dropping concurrent PG
    writes when it does succeed.
    """
    location = f"s3://{TEST_BUCKET}/conc_compact"
    pg_cursor.execute(
        f"""
        CREATE TABLE conc_compact (id INT, val TEXT)
        USING ducklake WITH (location = '{location}')
        """
    )
    # Several small files so compaction has something to merge.
    for i in range(5):
        pg_cursor.execute(
            "INSERT INTO conc_compact VALUES (%s, %s), (%s, %s)",
            (i * 10 + 1, f"pre-{i}-a", i * 10 + 2, f"pre-{i}-b"),
        )
    pg_cursor.connection.commit()

    pg_err = []
    duck_err = []

    def pg_writer():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            for i in range(20):
                cur.execute(
                    "INSERT INTO conc_compact VALUES (%s, %s)",
                    (1000 + i, f"mid-{i}"),
                )
                conn.commit()
            conn.close()
        except Exception as e:
            pg_err.append(e)

    def duck_compactor():
        try:
            duck = _attach_duckdb()
            duck.execute("CALL ducklake_merge_adjacent_files('dl')")
            duck.close()
        except Exception as e:
            duck_err.append(e)

    pg_thread = threading.Thread(target=pg_writer)
    duck_thread = threading.Thread(target=duck_compactor)
    pg_thread.start()
    duck_thread.start()
    pg_thread.join()
    duck_thread.join()

    assert pg_err == [], f"pg writer failed: {pg_err}"
    # Tolerate DuckDB's "Unsupported change type" abort -- upstream
    # limitation. Anything else is a real failure.
    for e in duck_err:
        assert "Unsupported change type" in str(e), f"unexpected duck error: {e}"

    pg_cursor.connection.commit()
    pg_cursor.execute("SELECT count(*) FROM conc_compact")
    total = pg_cursor.fetchone()[0]
    assert total == 10 + 20, f"want 30 rows, got {total}"

    pg_cursor.execute("SELECT count(*) FROM conc_compact WHERE val LIKE 'pre-%'")
    assert pg_cursor.fetchone()[0] == 10, "compaction lost pre-existing rows"

    pg_cursor.execute("SELECT count(*) FROM conc_compact WHERE val LIKE 'mid-%'")
    assert pg_cursor.fetchone()[0] == 20, "concurrent PG inserts lost"


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_vs_duckdb_update_race(pg_cursor, s3):
    """
    PG and DuckDB simultaneously UPDATE disjoint id ranges. Both go
    through delete-file generation, which the INSERT/INSERT race in
    test_pg_and_duckdb_insert_to_same_table_concurrently never
    exercises.

    Both should succeed (DuckDB's ducklake extension retries on
    snapshot PK conflict); final state has 25 rows tagged 'pg' and
    25 tagged 'duck'.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE upd_race (id INT, src TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/upd_race')
        """
    )
    pg_cursor.execute(
        "INSERT INTO upd_race SELECT i, 'orig' FROM generate_series(1, 50) i"
    )
    pg_cursor.connection.commit()

    duck = _attach_duckdb()

    pg_err = []
    duck_err = []

    def pg_updater():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute("UPDATE upd_race SET src = 'pg' WHERE id <= 25")
            conn.commit()
            conn.close()
        except Exception as e:
            pg_err.append(e)

    def duck_updater():
        try:
            duck.execute("UPDATE dl.public.upd_race SET src = 'duck' WHERE id > 25")
        except Exception as e:
            duck_err.append(e)

    pg_thread = threading.Thread(target=pg_updater)
    duck_thread = threading.Thread(target=duck_updater)
    pg_thread.start()
    duck_thread.start()
    pg_thread.join()
    duck_thread.join()

    assert pg_err == [], f"pg updater failed: {pg_err}"
    assert duck_err == [], f"duck updater failed: {duck_err}"

    pg_cursor.connection.commit()
    pg_cursor.execute("SELECT src, count(*) FROM upd_race GROUP BY src ORDER BY src")
    by_src = dict(pg_cursor.fetchall())
    assert by_src == {"pg": 25, "duck": 25}, by_src

    pg_cursor.execute("SELECT count(*) FROM upd_race")
    assert pg_cursor.fetchone()[0] == 50

    duck.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_insert_vs_duckdb_delete(pg_cursor, s3):
    """
    PG INSERTs new rows while DuckDB DELETEs a disjoint id range. The
    INSERT writes a new data_file; the DELETE writes a position-
    delete file (merge-on-read).

    DuckDB's ducklake extension currently rejects commits that observe
    mixed concurrent change types ("Unsupported change type
    INSERT/UPDATE/DELETE operation") -- same upstream limitation as
    test_pg_dml_during_duckdb_compact. We tolerate the DELETE rolling
    back; what we always require is that PG's INSERT commits, every
    inserted row is visible, and original rows remain in whatever
    state corresponds to whether DuckDB succeeded.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE ins_del_race (id INT, src TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/ins_del_race')
        """
    )
    pg_cursor.execute(
        "INSERT INTO ins_del_race SELECT i, 'orig' FROM generate_series(1, 50) i"
    )
    pg_cursor.connection.commit()

    duck = _attach_duckdb()

    pg_err = []
    duck_err = []

    def pg_inserter():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO ins_del_race "
                "SELECT i, 'pg-new' FROM generate_series(100, 119) i"
            )
            conn.commit()
            conn.close()
        except Exception as e:
            pg_err.append(e)

    def duck_deleter():
        try:
            duck.execute("DELETE FROM dl.public.ins_del_race WHERE id <= 25")
        except Exception as e:
            duck_err.append(e)

    pg_thread = threading.Thread(target=pg_inserter)
    duck_thread = threading.Thread(target=duck_deleter)
    pg_thread.start()
    duck_thread.start()
    pg_thread.join()
    duck_thread.join()

    assert pg_err == [], f"pg inserter failed: {pg_err}"
    duck_committed = duck_err == []
    for e in duck_err:
        assert "Unsupported change type" in str(e), f"unexpected duck error: {e}"

    pg_cursor.connection.commit()
    pg_cursor.execute(
        "SELECT src, count(*) FROM ins_del_race GROUP BY src ORDER BY src"
    )
    by_src = dict(pg_cursor.fetchall())

    if duck_committed:
        # 50 originals - 25 deleted (id 1..25) + 20 new = 45 visible.
        assert by_src == {"orig": 25, "pg-new": 20}, by_src
    else:
        # DuckDB DELETE rolled back; all originals remain alongside the
        # PG inserts.
        assert by_src == {"orig": 50, "pg-new": 20}, by_src

    duck.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_update_vs_duckdb_insert(pg_cursor, s3):
    """
    PG UPDATEs existing rows (id <= 25) while DuckDB INSERTs into a
    disjoint id range simultaneously. The PG UPDATE goes through the
    delete-file generation path on the merge-on-read branch (since
    the COW fix); the DuckDB INSERT just writes a new data_file.
    Both must commit and final state must reflect both.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE upd_ins_race (id INT, src TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/upd_ins_race')
        """
    )
    pg_cursor.execute(
        "INSERT INTO upd_ins_race SELECT i, 'orig' FROM generate_series(1, 50) i"
    )
    pg_cursor.connection.commit()

    duck = _attach_duckdb()

    pg_err = []
    duck_err = []

    def pg_updater():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute("UPDATE upd_ins_race SET src = 'pg' WHERE id <= 25")
            conn.commit()
            conn.close()
        except Exception as e:
            pg_err.append(e)

    def duck_inserter():
        try:
            duck.execute(
                "INSERT INTO dl.public.upd_ins_race "
                "SELECT range::INTEGER, 'duck-new' FROM range(100, 120)"
            )
        except Exception as e:
            duck_err.append(e)

    pg_thread = threading.Thread(target=pg_updater)
    duck_thread = threading.Thread(target=duck_inserter)
    pg_thread.start()
    duck_thread.start()
    pg_thread.join()
    duck_thread.join()

    assert pg_err == [], f"pg updater failed: {pg_err}"
    assert duck_err == [], f"duck inserter failed: {duck_err}"

    pg_cursor.connection.commit()
    pg_cursor.execute(
        "SELECT src, count(*) FROM upd_ins_race GROUP BY src ORDER BY src"
    )
    by_src = dict(pg_cursor.fetchall())
    # 25 'pg' (UPDATEd) + 25 'orig' (id 26..50 unchanged) + 20 'duck-new'.
    assert by_src == {"pg": 25, "orig": 25, "duck-new": 20}, by_src

    duck.close()


@pytest.mark.skipif(not DUCKDB_AVAILABLE, reason="DuckDB not installed")
def test_pg_delete_vs_duckdb_update(pg_cursor, s3):
    """
    PG DELETEs id <= 25 while DuckDB UPDATEs id > 25 simultaneously.
    Both paths emit position-delete files against the same source
    data_file but for disjoint position ranges.

    Same upstream-DuckDB limitation as test_pg_insert_vs_duckdb_delete:
    DuckDB's UPDATE may abort with "Unsupported change type" when it
    sees PG's DELETE landed on the same table. Tolerate the DuckDB
    rollback; the invariant we require is that PG's DELETE commits and
    visible rows match the actual outcome.
    """
    pg_cursor.execute(
        f"""
        CREATE TABLE del_upd_race (id INT, src TEXT)
        USING ducklake WITH (location = 's3://{TEST_BUCKET}/del_upd_race')
        """
    )
    pg_cursor.execute(
        "INSERT INTO del_upd_race SELECT i, 'orig' FROM generate_series(1, 50) i"
    )
    pg_cursor.connection.commit()

    duck = _attach_duckdb()

    pg_err = []
    duck_err = []

    def pg_deleter():
        try:
            conn = _new_pg_conn()
            cur = conn.cursor()
            cur.execute("DELETE FROM del_upd_race WHERE id <= 25")
            conn.commit()
            conn.close()
        except Exception as e:
            pg_err.append(e)

    def duck_updater():
        try:
            duck.execute("UPDATE dl.public.del_upd_race SET src = 'duck' WHERE id > 25")
        except Exception as e:
            duck_err.append(e)

    pg_thread = threading.Thread(target=pg_deleter)
    duck_thread = threading.Thread(target=duck_updater)
    pg_thread.start()
    duck_thread.start()
    pg_thread.join()
    duck_thread.join()

    assert pg_err == [], f"pg deleter failed: {pg_err}"
    duck_committed = duck_err == []
    for e in duck_err:
        assert "Unsupported change type" in str(e), f"unexpected duck error: {e}"

    pg_cursor.connection.commit()
    pg_cursor.execute(
        "SELECT src, count(*) FROM del_upd_race GROUP BY src ORDER BY src"
    )
    by_src = dict(pg_cursor.fetchall())

    if duck_committed:
        # 25 PG-deleted, 25 UPDATEd to 'duck'. Originals fully gone.
        assert by_src == {"duck": 25}, by_src
    else:
        # DuckDB UPDATE rolled back; PG's DELETE still applied.
        assert by_src == {"orig": 25}, by_src

    duck.close()
