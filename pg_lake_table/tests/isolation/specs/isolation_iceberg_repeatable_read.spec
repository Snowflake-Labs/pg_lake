setup
{
    CREATE TABLE test_iceberg_rr (key int, value int) USING pg_lake_iceberg;
    ALTER FOREIGN TABLE test_iceberg_rr OPTIONS (ADD autovacuum_enabled 'false');

    INSERT INTO test_iceberg_rr VALUES (1, 10), (2, 20), (3, 30);
}

teardown
{
    DROP TABLE IF EXISTS test_iceberg_rr CASCADE;
}

session "s1"

setup { BEGIN ISOLATION LEVEL REPEATABLE READ; }

step "s1-insert"
{
    INSERT INTO test_iceberg_rr VALUES (4, 40);
}

step "s1-update"
{
    UPDATE test_iceberg_rr SET value = 99 WHERE key = 1;
}

step "s1-delete"
{
    DELETE FROM test_iceberg_rr WHERE key = 2;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

setup { BEGIN ISOLATION LEVEL REPEATABLE READ; }

step "s2-select-all"
{
    SELECT * FROM test_iceberg_rr ORDER BY key;
}

step "s2-insert"
{
    INSERT INTO test_iceberg_rr VALUES (5, 50);
}

step "s2-update"
{
    UPDATE test_iceberg_rr SET value = 88 WHERE key = 1;
}

step "s2-delete"
{
    DELETE FROM test_iceberg_rr WHERE key = 2;
}

step "s2-delete-key-1"
{
    DELETE FROM test_iceberg_rr WHERE key = 1;
}

step "s2-commit"
{
    COMMIT;
}

step "s2-rollback"
{
    ROLLBACK;
}

# REPEATABLE READ isolation tests for Iceberg tables.
# Snapshot stability: s2 does not see s1's committed changes.
permutation "s2-select-all" "s1-insert" "s1-commit" "s2-select-all" "s2-commit"
permutation "s2-select-all" "s1-update" "s1-commit" "s2-select-all" "s2-commit"
permutation "s2-select-all" "s1-delete" "s1-commit" "s2-select-all" "s2-commit"

# Concurrent update/delete on same row: one fails after other commits.
permutation "s1-update" "s2-update" "s1-commit" "s2-rollback"
permutation "s1-delete" "s2-delete" "s1-commit" "s2-rollback"

# Concurrent inserts: one fails after other commits.
permutation "s1-insert" "s2-insert" "s1-commit" "s2-rollback"

# Concurrent insert vs delete/update: one fails.
permutation "s1-insert" "s2-delete" "s1-commit" "s2-rollback"
permutation "s1-insert" "s2-update" "s1-commit" "s2-rollback"

# Concurrent update vs delete on same row: one fails.
permutation "s1-update" "s2-delete-key-1" "s1-commit" "s2-rollback"
