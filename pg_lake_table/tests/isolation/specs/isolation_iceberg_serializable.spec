setup
{
    CREATE TABLE test_iceberg_ser (key int, value int) USING pg_lake_iceberg;
    ALTER FOREIGN TABLE test_iceberg_ser OPTIONS (ADD autovacuum_enabled 'false');

    INSERT INTO test_iceberg_ser VALUES (1, 100), (2, 100);
}

teardown
{
    DROP TABLE IF EXISTS test_iceberg_ser CASCADE;
}

session "s1"

setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }

step "s1-select-all"
{
    SELECT * FROM test_iceberg_ser ORDER BY key;
}

step "s1-select-sum"
{
    SELECT SUM(value) FROM test_iceberg_ser;
}

step "s1-update-key-1"
{
    UPDATE test_iceberg_ser SET value = value - 50 WHERE key = 1;
}

step "s1-insert"
{
    INSERT INTO test_iceberg_ser VALUES (3, 30);
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

setup { BEGIN ISOLATION LEVEL SERIALIZABLE; }

step "s2-select-all"
{
    SELECT * FROM test_iceberg_ser ORDER BY key;
}

step "s2-select-sum"
{
    SELECT SUM(value) FROM test_iceberg_ser;
}

step "s2-update-key-2"
{
    UPDATE test_iceberg_ser SET value = value - 50 WHERE key = 2;
}

step "s2-update-key-1"
{
    UPDATE test_iceberg_ser SET value = value + 10 WHERE key = 1;
}

step "s2-insert"
{
    INSERT INTO test_iceberg_ser VALUES (4, 40);
}

step "s2-commit"
{
    COMMIT;
}

step "s2-rollback"
{
    ROLLBACK;
}

# SERIALIZABLE isolation tests for Iceberg tables.
#
# Write skew: one must fail to serialize.
permutation "s1-select-sum" "s2-select-sum" "s1-update-key-1" "s2-update-key-2" "s1-commit" "s2-rollback"

# Concurrent update on same row: one must fail.
permutation "s1-update-key-1" "s2-update-key-1" "s1-commit" "s2-rollback"

# Read then write with overlap; causes serialization failure.
permutation "s1-select-all" "s2-select-all" "s1-insert" "s2-insert" "s1-commit" "s2-rollback"
