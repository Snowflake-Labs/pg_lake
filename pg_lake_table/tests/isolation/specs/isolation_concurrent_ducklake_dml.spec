# Concurrent DML against DuckLake tables.
#
# DuckLake on the PG side serializes snapshot allocation with a transaction-
# scoped advisory lock (LOCKTAG_ADVISORY class 102, dbid scope) -- the lock
# guards lake_ducklake.snapshot.snapshot_id PK collisions between PG-side
# writers. UPDATE/DELETE additionally inherit pg_lake_table's per-relation
# advisory lock (class 101).
#
# Net behavior we want pinned here:
#   - Two INSERTs into the same table block on commit (snapshot allocation).
#   - Two INSERTs into DIFFERENT tables also block, because the snapshot
#     lock is database-scoped, not table-scoped.
#   - UPDATE/DELETE block each other on the same table (per-relation lock).
#   - SELECT never blocks DML.
#   - Empty INSERT-SELECT vs INSERT-SELECT does not block (no rows scanned).
#   - INSERT-SELECT with rows blocks against same-table INSERT-SELECT.

setup
{
    CREATE TABLE test_ducklake_dml (key int, value text) USING ducklake;

    CREATE TABLE test_ducklake_dml_2 (key int, value text) USING ducklake;
}

teardown
{
    DROP TABLE IF EXISTS test_ducklake_dml, test_ducklake_dml_2 CASCADE;
}

session "s1"


step "s1-begin"
{
    BEGIN;
}

step "s1-insert-select-1"
{
    INSERT INTO test_ducklake_dml SELECT * FROM test_ducklake_dml_2;
}

step "s1-insert-select-2"
{
    INSERT INTO test_ducklake_dml_2 SELECT * FROM test_ducklake_dml;
}

step "s1-insert"
{
    INSERT INTO test_ducklake_dml VALUES(1);
}

step "s1-insert-2"
{
    INSERT INTO test_ducklake_dml_2 VALUES(1);
}

step "s1-update-1"
{
    UPDATE test_ducklake_dml SET key = 15 WHERE key = 1;
}

step "s1-delete"
{
    DELETE FROM test_ducklake_dml WHERE key = 1;
}

step "s1-select-all"
{
   SELECT * FROM test_ducklake_dml ORDER BY 1,2;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-begin"
{
    BEGIN;
}

step "s2-insert-select-1"
{
    INSERT INTO test_ducklake_dml SELECT * FROM test_ducklake_dml_2;
}

step "s2-insert-select-2"
{
    INSERT INTO test_ducklake_dml_2 SELECT * FROM test_ducklake_dml;
}

step "s2-insert"
{
    INSERT INTO test_ducklake_dml VALUES(2);
}

step "s2-insert-other-table"
{
    INSERT INTO test_ducklake_dml_2 VALUES(2);
}

step "s2-update-1"
{
    UPDATE test_ducklake_dml SET key = 16 WHERE key = 1;
}

step "s2-update-15"
{
    UPDATE test_ducklake_dml SET key = 16 WHERE key = 15;
}

step "s2-delete"
{
    DELETE FROM test_ducklake_dml WHERE key = 1;
}

step "s2-select-all"
{
   SELECT * FROM test_ducklake_dml ORDER BY 1,2;
}

step "s2-commit"
{
    COMMIT;
}

# Two INSERTs into the same table block on snapshot allocation.
permutation "s1-begin" "s1-insert" "s2-insert" "s1-commit" "s1-select-all"

# Two INSERTs into DIFFERENT tables also block: the snapshot lock is
# database-scoped, not per-table, so this serialises across all DuckLake
# tables in the database.
permutation "s1-begin" "s1-insert" "s2-insert-other-table" "s1-commit" "s1-select-all"

# DELETE and UPDATE block each other on the same table (per-relation lock).
permutation "s1-begin" "s1-delete" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-update-1" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-update-1" "s2-update-1" "s1-commit" "s1-select-all"

# UPDATE/DELETE that touches no rows still races snapshot allocation with
# concurrent INSERT (since both want a fresh snapshot id).
permutation "s1-begin" "s1-insert" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-begin" "s1-insert" "s2-update-1" "s1-commit" "s1-select-all"

# When the table already has matching rows, UPDATE/DELETE blocks against
# concurrent INSERT for the same reason.
permutation "s1-insert" "s1-begin" "s1-insert" "s2-delete" "s1-commit" "s1-select-all"
permutation "s1-insert" "s1-begin" "s1-insert" "s2-update-1" "s1-commit" "s1-select-all"

# SELECT does not block.
permutation "s1-begin" "s1-select-all" "s2-select-all" "s1-commit"

# SELECT is not blocked by INSERT/UPDATE/DELETE.
permutation "s1-insert" "s1-begin" "s1-select-all" "s2-delete" "s1-commit"
permutation "s1-insert" "s1-begin" "s1-select-all" "s2-update-1" "s1-commit"
permutation "s1-insert" "s1-begin" "s1-select-all" "s2-insert" "s1-commit"

# INSERT-SELECT against empty source blocks the second INSERT-SELECT only
# at snapshot-allocation time (no row-level lock conflict).
permutation "s1-begin" "s1-insert-select-1" "s2-insert-select-1" "s1-commit"

# INSERT-SELECT into different tables: both still serialise on the
# database-scoped snapshot allocation lock.
permutation "s1-begin" "s1-insert-select-1" "s2-insert-select-2" "s1-commit"

# INSERT-SELECT against non-empty source blocks at commit time.
permutation "s1-insert-2" "s1-begin" "s1-insert-select-1" "s2-insert-select-1" "s1-commit"
