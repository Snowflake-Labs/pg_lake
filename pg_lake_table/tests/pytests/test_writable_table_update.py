import pytest
import psycopg2
from utils_pytest import *

# Courtesy of ChatGPT

update_statements = [
    (
        "Simple update",
        "UPDATE test_table SET age = 31 WHERE name = 'Alice' RETURNING age;",
    ),
    (
        "Multiple rows",
        "UPDATE test_table SET age = age + 1 WHERE age > 30 RETURNING id * 2;",
    ),
    (
        "Modifying CTE",
        """
    WITH updates AS (
        UPDATE test_table
        SET age = age + 1
        WHERE id > 1
        RETURNING age
    )
    SELECT sum(age) FROM updates;
    """,
    ),
    (
        "Two modifying CTEs",
        """
    WITH u1 AS (
        UPDATE test_table
        SET age = age + 1
        WHERE id > 1
        RETURNING age
    ),
    u2 AS (
        UPDATE join_table
        SET new_age = new_age + 1
        WHERE user_id > 1
        RETURNING new_age
    )
    SELECT age FROM u1 UNION SELECT new_age FROM u2;
    """,
    ),
    (
        "with a CTE (Common Table Expression)",
        """
    WITH updated_ages AS (
        SELECT id, age + 5 AS new_age
        FROM test_table
        WHERE age < 35
    )
    UPDATE test_table
    SET age = updated_ages.new_age
    FROM updated_ages
    WHERE test_table.id = updated_ages.id
    RETURNING updated_ages.new_age + age;
    """,
    ),
    (
        "With a subquery",
        """
    UPDATE test_table
    SET age = (SELECT AVG(age) FROM test_table)
    WHERE name = 'Charlie'
    RETURNING name, age, (SELECT max(age) FROM test_table)
    """,
    ),
    (
        "With a join",
        """
    UPDATE test_table
    SET age = jt.new_age
    FROM join_table jt
    WHERE test_table.id = jt.user_id
    RETURNING jt.new_age, test_table.age
    """,
    ),
    (
        "Conditional update with CASE",
        """
    UPDATE test_table
    SET age = CASE
        WHEN age < 30 THEN age + 10
        ELSE age - 5
    END
    RETURNING CASE WHEN age < 30 THEN 0 ELSE 1 END;
    """,
    ),
    (
        "WHERE EXISTS",
        """
    UPDATE test_table
    SET age = 29
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
    UPDATE test_table
    SET age = ranked.age + 10
    FROM ranked
    WHERE test_table.id = ranked.id AND ranked.rank = 1
    RETURNING (SELECT count(*) FROM ranked);
    """,
    ),
    (
        "Update with FROM",
        """
    UPDATE test_table
    SET age = 30
    FROM join_table jt
    WHERE test_table.id = jt.user_id AND jt.new_age < 40
    RETURNING jt.*, test_table.*;
    """,
    ),
    (
        "Update with local FROM",
        """
    UPDATE test_table
    SET age = 0
    FROM even_ids e
    WHERE test_table.id = e.id
    RETURNING (test_table.*)::text;
    """,
    ),
    (
        "IN clause",
        """
    UPDATE test_table
    SET age = 33
    WHERE name IN ('Alice', 'Charlie')
    RETURNING 1;
    """,
    ),
    (
        "IS NULL condition",
        """
    UPDATE test_table
    SET age = 40
    WHERE age IS NULL
    RETURNING id, id, id;
    """,
    ),
    (
        "COALESCE",
        """
    UPDATE test_table
    SET age = COALESCE(age, 25)
    RETURNING coalesce(id, 0);
    """,
    ),
    (
        "Complex subqueries",
        """
    UPDATE test_table
    SET age = (SELECT MAX(age) FROM test_table) - 5
    WHERE id IN (SELECT id FROM test_table WHERE age < 30)
    RETURNING 'subquery';
    """,
    ),
    (
        "Update with multiple conditions",
        """
    UPDATE test_table
    SET age = age + 2
    WHERE age > 20 AND age < 40
    RETURNING age - 2;
    """,
    ),
    (
        "Different table alias",
        """
    UPDATE test_table AS tt
    SET age = 35
    WHERE tt.name = 'David'
    RETURNING tt.*;
    """,
    ),
    (
        "Update on a view",
        """
    UPDATE test_view
    SET status = 'updated'
    RETURNING test_view.age;
    """,
    ),
    (
        "VALUES ",
        """
    UPDATE test_view
    SET (status,age) = ROW(v.j, v.k)
    FROM (VALUES(5, 'old', 100), (4, 'young', 10)) AS v(i, j, k)
    WHERE id = v.i
    RETURNING v
    """,
    ),
    (
        "Subquery in SET",
        """
    UPDATE test_view
    SET (status,age) = (SELECT status, age FROM test_table WHERE name = 'David')
    WHERE name = 'Egbert'
    RETURNING status
    """,
    ),
    (
        "FROM LATERAL",
        """
    UPDATE test_table t
    SET status = sub.hey
    FROM LATERAL (
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
    url_test_table = f"s3://{TEST_BUCKET}/test_writable_update/test_table/"
    url_join_table = f"s3://{TEST_BUCKET}/test_writable_update/join_table/"

    iceberg_url_test_table = (
        f"s3://{TEST_BUCKET}/test_writable_update/iceberg_url_test_table/"
    )
    iceberg_url_join_table = (
        f"s3://{TEST_BUCKET}/test_writable_update/iceberg_url_join_table/"
    )

    run_command(
        f"""
        CREATE SCHEMA test_writable_table_update;
        SET search_path TO test_writable_table_update;
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

        CREATE TABLE test_table_heap
        AS SELECT * FROM test_table;

        CREATE TABLE join_table_heap
        AS SELECT * FROM join_table;

        CREATE TABLE test_table_heap_for_iceberg
        AS SELECT * FROM test_table;

        CREATE TABLE join_table_heap_for_iceberg
        AS SELECT * FROM join_table;

        CREATE TABLE test_table_iceberg USING iceberg WITH (location = '{iceberg_url_test_table}')
        AS SELECT * FROM test_table WITH NO DATA;
        INSERT INTO test_table_iceberg SELECT * FROM test_table;

        CREATE TABLE join_table_iceberg USING iceberg WITH (location = '{iceberg_url_join_table}')
        AS SELECT * FROM join_table WITH NO DATA;
        INSERT INTO join_table_iceberg SELECT * FROM join_table;

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
    update_statements,
    ids=[ids_list[0] for ids_list in update_statements],
)
def test_update_statements(create_writable_tables, pg_conn, test_id, statement):

    assert_query_results_on_tables(
        statement,
        pg_conn,
        ["test_table", "join_table", "test_view"],
        ["test_table_heap", "join_table_heap", "test_view_heap"],
    )

    # now, the same test on iceberg table
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
        "SELECT * FROM test_table", pg_conn, ["test_table"], ["test_table_heap"]
    )
    assert_query_results_on_tables(
        "SELECT * FROM test_table", pg_conn, ["test_table"], ["test_table_iceberg"]
    )

    pg_conn.rollback()


def test_disallowed_update(create_writable_tables, pg_conn):
    error = run_command(
        """
        WITH u1 AS (
            UPDATE test_table SET age = 31 WHERE name = 'Alice' RETURNING age
        ),
        u2 AS (
            UPDATE test_table SET age = 32 WHERE name = 'Alice' RETURNING age
        )
        SELECT * FROM u1 UNION ALL SELECT * FROM u2;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake table test_table cannot be updated by multiple operations" in error

    pg_conn.rollback()


def test_disallowed_select_for_update(create_writable_tables, pg_conn):
    error = run_command(
        """
        SELECT * FROM test_table FOR UPDATE
    """,
        pg_conn,
        raise_error=False,
    )
    assert "SELECT .. FOR UPDATE is not yet supported" in error

    pg_conn.rollback()

    error = run_command(
        """
        SELECT count(*) FROM (SELECT id FROM test_table FOR UPDATE) a;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "SELECT .. FOR UPDATE is not yet supported" in error

    pg_conn.rollback()


def test_update_join_repeated_rows_serializable(
    pg_conn, s3, extension, with_default_location
):
    """
    A join that matches a target row twice makes update tracking find the row
    ID already present. At SERIALIZABLE, PostgreSQL then re-reads that row
    with the EState snapshot, which must be set.
    """
    location = f"s3://{TEST_BUCKET}/test_update_join_repeated_rows_serializable/"

    run_command(
        f"""
        CREATE SCHEMA test_update_serializable;
        CREATE FOREIGN TABLE test_update_serializable.target (id int, val int)
        SERVER pg_lake OPTIONS (location '{location}', writable 'true', format 'parquet');
        INSERT INTO test_update_serializable.target VALUES (1, 0), (2, 0), (3, 0);

        CREATE TABLE test_update_serializable.target_iceberg USING iceberg
        AS SELECT * FROM test_update_serializable.target;

        CREATE TABLE test_update_serializable.source (id int, val int);
        INSERT INTO test_update_serializable.source VALUES (1, 10), (1, 10), (2, 20), (2, 20);
    """,
        pg_conn,
    )
    pg_conn.commit()

    for table_name in ["target", "target_iceberg"]:
        run_command("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", pg_conn)

        result = run_query(
            f"""
            UPDATE test_update_serializable.{table_name} t
            SET val = s.val
            FROM test_update_serializable.source s
            WHERE t.id = s.id
            RETURNING t.id, t.val
        """,
            pg_conn,
        )
        assert sorted(tuple(row) for row in result) == [(1, 10), (2, 20)]

        result = run_query(
            f"SELECT id, val FROM test_update_serializable.{table_name} ORDER BY id",
            pg_conn,
        )
        assert [tuple(row) for row in result] == [(1, 10), (2, 20), (3, 0)]

        pg_conn.commit()

    run_command("DROP SCHEMA test_update_serializable CASCADE", pg_conn)
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

        -- merge-on-read when 5 rows deleted
        DELETE FROM test_merge_on_read WHERE x <= 5;

        -- merge-on-read when 10 rows (total) updated
        UPDATE test_merge_on_read SET y = y + 1 WHERE x <= 10;
    """,
        pg_conn,
    )

    # should have the original file (100 rows) and a new values file (5 rows)
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 0
        ORDER BY row_count DESC
    """,
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["row_count"] == 100
    assert result[1]["row_count"] == 5

    # 2 deletion files that both subtracted 5 rows
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 1
        ORDER BY row_count DESC
    """,
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["row_count"] == 5
    assert result[1]["row_count"] == 5

    check_table_size(pg_conn, "test_merge_on_read", 95)

    # automatic copy-on-write when 20% of file is updated
    run_command(
        """
        UPDATE test_merge_on_read SET y = y + 1 WHERE x <= 20
    """,
        pg_conn,
    )

    # Both the 5-row file and the 100-row file are rewritten
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 0
        ORDER BY row_count DESC
    """,
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["row_count"] == 80
    assert result[1]["row_count"] == 15

    # Deletion files are gone since all source files were rewritten
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 1
        ORDER BY row_count DESC
    """,
        pg_conn,
    )
    assert len(result) == 0

    # force copy-on-write even for single row update
    run_command(
        """
        SET pg_lake_table.copy_on_write_threshold TO 0;
        UPDATE test_merge_on_read SET y = y + 1 WHERE x = 21;
    """,
        pg_conn,
    )

    # 80-row file is rewritten, 1-row file is added
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass
        ORDER BY row_count DESC
    """,
        pg_conn,
    )
    assert len(result) == 3
    assert result[0]["row_count"] == 79
    assert result[1]["row_count"] == 15
    assert result[2]["row_count"] == 1

    check_table_size(pg_conn, "test_merge_on_read", 95)

    # force merge-on-read even for large update
    run_command(
        """
        SET pg_lake_table.copy_on_write_threshold TO 100;
        UPDATE test_merge_on_read SET y = y + 1 WHERE x > 30;
    """,
        pg_conn,
    )

    # original files + new values from update
    result = run_query(
        """
        SELECT row_count FROM lake_table.files
        WHERE table_name = 'test_merge_on_read'::regclass AND content = 0
        ORDER BY row_count DESC
    """,
        pg_conn,
    )
    assert len(result) == 4
    assert result[0]["row_count"] == 79
    assert result[1]["row_count"] == 70
    assert result[2]["row_count"] == 15
    assert result[3]["row_count"] == 1

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

    check_table_size(pg_conn, "test_merge_on_read", 95)

    pg_conn.rollback()
