import pytest
import psycopg2
from utils_pytest import *

# Courtesy of ChatGPT

delete_statements = [
    ("Simple delete", "DELETE FROM test_table WHERE name = 'Alice' RETURNING age;"),
    ("Multiple rows", "DELETE FROM test_table WHERE age > 30 RETURNING id * 2;"),
    (
        "Modifying CTE",
        """
    WITH deletes AS (
        DELETE FROM test_table
        WHERE id > 1
        RETURNING age
    )
    SELECT sum(age) FROM deletes;
    """,
    ),
    (
        "Two modifying CTEs",
        """
    WITH d1 AS (
        DELETE FROM test_table
        WHERE id > 1
        RETURNING age
    ),
    d2 AS (
        DELETE FROM join_table
        WHERE user_id > 1
        RETURNING new_age
    )
    SELECT age FROM d1 UNION SELECT new_age FROM d2;
    """,
    ),
    (
        "with a CTE (Common Table Expression)",
        """
    WITH deleted_ages AS (
        SELECT id, age + 5 AS new_age
        FROM test_table
        WHERE age < 35
    )
    DELETE FROM test_table
    USING deleted_ages
    WHERE test_table.id = deleted_ages.id
    RETURNING deleted_ages.new_age + age;
    """,
    ),
    (
        "With a subquery",
        """
    DELETE FROM test_table
    USING (SELECT max(user_id) id FROM join_table) sub
    WHERE test_table.id = sub.id
    RETURNING sub.id, name, (SELECT max(age) FROM test_table)
    """,
    ),
    (
        "With a join",
        """
    DELETE FROM test_table
    USING join_table jt
    WHERE test_table.id = jt.user_id
    RETURNING jt.new_age, test_table.age
    """,
    ),
    (
        "WHERE EXISTS",
        """
    DELETE FROM test_table
    WHERE EXISTS (
        SELECT 1
        FROM join_table jt
        WHERE jt.user_id = test_table.id AND jt.new_age > 35
    )
    RETURNING NULL;
    """,
    ),
    (
        "Window function",
        """
    WITH ranked AS (
        SELECT id, age, RANK() OVER (ORDER BY age) as rank
        FROM test_table
    )
    DELETE FROM test_table
    USING ranked
    WHERE test_table.id = ranked.id AND ranked.rank = 1
    RETURNING (SELECT count(*) FROM ranked);
    """,
    ),
    (
        "Delete with FROM",
        """
    DELETE FROM test_table
    USING join_table jt
    WHERE test_table.id = jt.user_id AND jt.new_age < 40
    RETURNING jt.*, test_table.*;
    """,
    ),
    (
        "Delete with local USING",
        """
    DELETE FROM test_table
    USING even_ids e
    WHERE test_table.id = e.id
    RETURNING (test_table.*)::text;
    """,
    ),
    (
        "IN clause",
        """
    DELETE FROM test_table
    WHERE name IN ('Alice', 'Charlie')
    RETURNING 1;
    """,
    ),
    (
        "IS NULL condition",
        """
    DELETE FROM test_table
    WHERE age IS NULL
    RETURNING id, id, id;
    """,
    ),
    (
        "Delete with multiple conditions",
        """
    DELETE FROM test_table
    WHERE age > 20 AND age < 40
    RETURNING age - 2;
    """,
    ),
    (
        "Different table alias",
        """
    DELETE FROM test_table AS tt
    WHERE tt.name = 'David'
    RETURNING tt.*;
    """,
    ),
    (
        "Delete on a view",
        """
    DELETE FROM test_view
    RETURNING test_view.age;
    """,
    ),
    (
        "VALUES ",
        """
    DELETE FROM test_view
    USING (VALUES(5, 'old', 100), (4, 'young', 10)) AS v(i, j, k)
    WHERE id = v.i
    RETURNING v
    """,
    ),
    (
        "USING LATERAL",
        """
    DELETE FROM test_table t
    USING LATERAL (
         SELECT user_id as id, 'hey-' || new_age as hey
         FROM join_table
    ) sub
    WHERE t.id = sub.id
    RETURNING status;
    """,
    ),
]


@pytest.fixture(scope="module")
def create_writable_tables(pg_conn, s3, extension):
    url_test_table = f"s3://{TEST_BUCKET}/test_writable_delete/test_table/"
    url_join_table = f"s3://{TEST_BUCKET}/test_writable_delete/join_table/"

    iceberg_url_test_table = f"s3://{TEST_BUCKET}/iceberg_url_test_table/test_table/"
    iceberg_url_join_table = f"s3://{TEST_BUCKET}/iceberg_url_test_table/join_table/"

    run_command(
        f"""
        CREATE SCHEMA test_writable_table_delete;
        SET search_path TO test_writable_table_delete;
        CREATE FOREIGN TABLE test_table (
            id SERIAL,
            name VARCHAR(50),
            age INTEGER,
            status TEXT
        )
        SERVER pg_lake
        OPTIONS (writable 'true', location '{url_test_table}', format 'parquet');

        INSERT INTO test_table (name, age) VALUES
        ('Alice', 30),
        ('Bob', 25),
        ('Charlie', 35),
        ('David', 40),
        ('Egbert', 55),
        ('Fred', NULL);

        CREATE FOREIGN TABLE join_table (
            user_id INTEGER,
            new_age INTEGER
        )
        SERVER pg_lake
        OPTIONS (writable 'true', location '{url_join_table}', format 'parquet');

        INSERT INTO join_table (user_id, new_age) VALUES
        (1, 32),
        (3, 37);

        CREATE TABLE test_table_iceberg USING iceberg WITH (location = '{iceberg_url_test_table}')
        AS SELECT * FROM test_table WITH NO DATA;
        INSERT INTO test_table_iceberg SELECT * FROM test_table;

        CREATE TABLE join_table_iceberg USING iceberg WITH (location = '{iceberg_url_join_table}')
        AS SELECT * FROM join_table WITH NO DATA;
        INSERT INTO join_table_iceberg SELECT * FROM join_table;

        CREATE TABLE test_table_heap
        AS SELECT * FROM test_table;

        CREATE TABLE join_table_heap
        AS SELECT * FROM join_table;

        CREATE TABLE test_table_heap_for_iceberg
        AS SELECT * FROM test_table;

        CREATE TABLE join_table_heap_for_iceberg
        AS SELECT * FROM join_table;

        CREATE TABLE even_ids
        AS SELECT s * 1 AS id FROM generate_series(1,10) s;

        CREATE VIEW test_view AS SELECT * FROM test_table WHERE age >= 40;
        CREATE VIEW test_view_iceberg AS SELECT * FROM test_table_iceberg WHERE age >= 40;
        CREATE VIEW test_view_heap AS SELECT * FROM test_table_heap WHERE age >= 40;
        CREATE VIEW test_view_heap_for_iceberg AS SELECT * FROM test_table_heap_for_iceberg WHERE age >= 40;
    """,
        pg_conn,
    )

    pg_conn.commit()

    yield

    run_command(
        "DROP FOREIGN TABLE test_table, join_table, test_table_iceberg, join_table_iceberg CASCADE;",
        pg_conn,
    )
    run_command(
        "DROP TABLE test_table_heap, join_table_heap, test_table_heap_for_iceberg, join_table_heap_for_iceberg CASCADE;",
        pg_conn,
    )
    pg_conn.commit()


@pytest.mark.parametrize(
    "test_id, statement",
    delete_statements,
    ids=[ids_list[0] for ids_list in delete_statements],
)
def test_delete_statements(create_writable_tables, pg_conn, test_id, statement):

    assert_query_results_on_tables(
        statement,
        pg_conn,
        ["test_table", "join_table", "test_view"],
        ["test_table_heap", "join_table_heap", "test_view_heap"],
    )

    # now, run the same commands on iceberg tables
    statement = (
        statement.replace("test_table", "test_table_iceberg")
        .replace("join_table", "join_table_iceberg")
        .replace("test_view", "test_view_iceberg")
    )
    assert_query_results_on_tables(
        statement,
        pg_conn,
        ["test_table_iceberg", "join_table_iceberg", "test_view_iceberg"],
        [
            "test_table_heap_for_iceberg",
            "join_table_heap_for_iceberg",
            "test_view_heap_for_iceberg",
        ],
    )

    assert_query_results_on_tables(
        "SELECT count(*) FROM test_table", pg_conn, ["test_table"], ["test_table_heap"]
    )

    assert_query_results_on_tables(
        "SELECT count(*) FROM test_table_iceberg",
        pg_conn,
        ["test_table_iceberg"],
        ["test_table_heap_for_iceberg"],
    )

    pg_conn.rollback()


def test_two_modifying_ctes(create_writable_tables, pg_conn):
    error = run_command(
        """
        WITH d1 AS (
            DELETE FROM test_table WHERE name = 'Alice' RETURNING age
        ),
        d2 AS (
            DELETE FROM test_table WHERE name = 'Alice' RETURNING age
        )
        SELECT * FROM d1 UNION ALL SELECT * FROM d2;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake table test_table cannot be deleted by multiple operations" in error

    pg_conn.rollback()
    error = run_command(
        """
        WITH d1 AS (
            DELETE FROM test_table WHERE name = 'Alice' RETURNING age
        ),
        d2 AS (
            UPDATE test_table SET age = age + 1 WHERE name = 'Alice' RETURNING age
        )
        SELECT * FROM d1 UNION ALL SELECT * FROM d2;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake table test_table cannot be updated by multiple operations" in error

    pg_conn.rollback()

    # same with joins that repeat the target row, which use update tracking
    error = run_command(
        """
        WITH d1 AS (
            DELETE FROM test_table USING generate_series(1, 10) s
            WHERE name = 'Alice' RETURNING age
        ),
        d2 AS (
            UPDATE test_table SET age = age + 1 FROM generate_series(1, 10) s
            WHERE name = 'Alice' RETURNING age
        )
        SELECT * FROM d1 UNION ALL SELECT * FROM d2;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake table test_table cannot be updated by multiple operations" in error

    pg_conn.rollback()


def test_system_column(create_writable_tables, pg_conn):
    error = run_command(
        """
        DELETE FROM test_table WHERE name = 'Alice' RETURNING ctid
    """,
        pg_conn,
        raise_error=False,
    )
    assert "is not supported" in error

    pg_conn.rollback()

    error = run_command(
        """
        DELETE FROM test_table WHERE name = 'Alice' RETURNING xmin
    """,
        pg_conn,
        raise_error=False,
    )
    assert "is not supported" in error

    pg_conn.rollback()


def test_delete_join_repeated_rows(pg_conn, s3, extension, with_default_location):
    """
    A join that matches a target row many times must delete it only once.
    Counting the duplicates would make the deleted row count reach the live
    row count of the file, which removes the whole file including rows that
    the DELETE never matched.
    """
    location = f"s3://{TEST_BUCKET}/test_delete_join_repeated_rows/"

    run_command(
        f"""
        CREATE SCHEMA test_delete_repeated;
        CREATE FOREIGN TABLE test_delete_repeated.target (id int, val int)
        SERVER pg_lake OPTIONS (location '{location}', writable 'true', format 'parquet');
        INSERT INTO test_delete_repeated.target VALUES (1, 0), (2, 0), (3, 0);

        CREATE TABLE test_delete_repeated.target_iceberg USING iceberg
        AS SELECT * FROM test_delete_repeated.target;
    """,
        pg_conn,
    )
    pg_conn.commit()

    for table_name in ["target", "target_iceberg"]:
        # an earlier joined UPDATE in the same transaction must not affect
        # deduplication in the DELETE
        run_command(
            f"""
            UPDATE test_delete_repeated.{table_name} t SET val = t.val + 1
            FROM generate_series(1, 10) s
            WHERE t.id IN (1, 2)
        """,
            pg_conn,
        )

        result = run_query(
            f"""
            DELETE FROM test_delete_repeated.{table_name} t
            USING generate_series(1, 10) s
            WHERE t.id = 1
            RETURNING t.id
        """,
            pg_conn,
        )
        assert [tuple(row) for row in result] == [(1,)]
        pg_conn.commit()

        result = run_query(
            f"SELECT id, val FROM test_delete_repeated.{table_name} ORDER BY id",
            pg_conn,
        )
        assert [tuple(row) for row in result] == [(2, 1), (3, 0)]

    run_command("DROP SCHEMA test_delete_repeated CASCADE", pg_conn)
    pg_conn.commit()


def test_merge_on_read(pg_conn, extension):
    location = f"s3://{TEST_BUCKET}/test_merge_on_read/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_merge_on_read (
            x int,
            y int
        ) SERVER pg_lake OPTIONS (location '{location}', writable 'true', format 'parquet');

        INSERT INTO test_merge_on_read SELECT s, s FROM generate_series(1,100) s;

        -- automatic merge-on-read when deleting 19 rows
        DELETE FROM test_merge_on_read WHERE x < 20;
    """,
        pg_conn,
    )

    # original file should still be there
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 0
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["row_count"] == 100

    # deletion file subtracts 19 rows
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 1
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["row_count"] == 19

    check_table_size(pg_conn, "test_merge_on_read", 81)

    # automatic copy-on-write when 20% of file is deleted
    run_command(
        """
        DELETE FROM test_merge_on_read WHERE x <= 20;
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["row_count"] == 80

    check_table_size(pg_conn, "test_merge_on_read", 80)

    # force copy-on-write even for single row delete
    run_command(
        """
        SET pg_lake_table.copy_on_write_threshold TO 0;
        DELETE FROM test_merge_on_read WHERE x = 21;
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["row_count"] == 79

    check_table_size(pg_conn, "test_merge_on_read", 79)

    # force merge-on-read even for large delete
    run_command(
        """
        SET pg_lake_table.copy_on_write_threshold TO 100;
        DELETE FROM test_merge_on_read WHERE x > 30;
    """,
        pg_conn,
    )

    # original file should still be there
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 0
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["row_count"] == 79

    # deletion file subtracts many rows
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 1
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["row_count"] == 70

    check_table_size(pg_conn, "test_merge_on_read", 9)

    pg_conn.rollback()
