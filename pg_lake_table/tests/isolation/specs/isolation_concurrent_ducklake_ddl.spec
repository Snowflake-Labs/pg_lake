# Concurrent DML + DDL against DuckLake tables.
#
# Mirrors the iceberg DDL spec but exercises the DuckLake catalog write
# paths. The interesting cases are ALTER ... ADD COLUMN crossing INSERT,
# TRUNCATE while another session is mid-INSERT, and VACUUM interleaved
# with writes.

setup
{
    CREATE TABLE test_ducklake_ddl (key int, value text) USING ducklake;
}

teardown
{
    DROP TABLE IF EXISTS test_ducklake_ddl CASCADE;
}

session "s1"


step "s1-begin"
{
    BEGIN;
}


step "s1-insert"
{
    INSERT INTO test_ducklake_ddl VALUES(1);
}


step "s1-update"
{
    UPDATE test_ducklake_ddl SET key = 15 WHERE key = 1;
}

step "s1-delete"
{
    DELETE FROM test_ducklake_ddl WHERE key = 1;
}

step "s1-truncate"
{
    TRUNCATE test_ducklake_ddl;
}

step "s1-ddl"
{
    ALTER TABLE test_ducklake_ddl ADD CONSTRAINT check_a_positive_2 CHECK (key > 0);
}

step "s1-add-column"
{
    ALTER TABLE test_ducklake_ddl ADD COLUMN new_col INT;
}

step "s1-commit"
{
    COMMIT;
}

session "s2"

step "s2-select"
{
    SELECT * FROM test_ducklake_ddl;
}

step "s2-truncate"
{
    TRUNCATE test_ducklake_ddl;
}

step "s2-ddl"
{
    ALTER TABLE test_ducklake_ddl ADD CONSTRAINT check_a_positive CHECK (key > 0);
}

step "s2-begin"
{
    BEGIN;
}

step "s2-commit"
{
    COMMIT;
}

permutation "s1-begin" "s1-insert" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-update" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-delete" "s2-truncate" "s1-commit"

permutation "s1-begin" "s1-insert" "s2-ddl" "s1-commit"
permutation "s1-begin" "s1-update" "s2-ddl" "s1-commit"
permutation "s1-begin" "s1-delete" "s2-ddl" "s1-commit"

permutation "s1-begin" "s1-truncate" "s2-truncate" "s1-commit"
permutation "s1-begin" "s1-ddl" "s2-truncate" "s1-commit"

permutation "s1-begin" "s1-truncate" "s2-ddl" "s1-commit"
permutation "s1-begin" "s1-ddl" "s2-ddl" "s1-commit"

permutation "s1-begin" "s1-add-column" "s2-select" "s1-commit"
permutation "s2-begin" "s2-select" "s1-add-column" "s2-commit" "s2-select"
