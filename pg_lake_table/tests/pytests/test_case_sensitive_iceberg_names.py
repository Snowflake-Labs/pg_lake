import json

from utils_pytest import *


def test_case_sensitive_iceberg_column_names(
    pg_conn, s3, extension, with_default_location
):
    run_command(
        """
        CREATE SCHEMA test_case_sensitive_iceberg_names;
        CREATE TABLE test_case_sensitive_iceberg_names.empty (
            "a" int, "A" int, b int
        ) USING iceberg;
        CREATE TABLE test_case_sensitive_iceberg_names.cols (
            "a" int, "A" int, b int
        ) USING iceberg;
        INSERT INTO test_case_sensitive_iceberg_names.cols VALUES (1, 100, 7);
        INSERT INTO test_case_sensitive_iceberg_names.cols ("a", "A", b)
        SELECT "a", "A", b FROM test_case_sensitive_iceberg_names.cols;
        CREATE TABLE test_case_sensitive_iceberg_names.swapped (
            "A" int, "a" int, b int
        ) USING iceberg;
        INSERT INTO test_case_sensitive_iceberg_names.swapped ("a", "A", b)
        SELECT "a", "A", b FROM test_case_sensitive_iceberg_names.cols;
        CREATE TABLE test_case_sensitive_iceberg_names.plain (
            "a" int, z int
        ) USING iceberg;
        INSERT INTO test_case_sensitive_iceberg_names.plain VALUES (1, 5);
        CREATE TABLE test_case_sensitive_iceberg_names.lo ("a" int) USING iceberg;
        INSERT INTO test_case_sensitive_iceberg_names.lo VALUES (1);
        CREATE TABLE test_case_sensitive_iceberg_names.up ("A" int) USING iceberg;
        INSERT INTO test_case_sensitive_iceberg_names.up VALUES (2);
        """,
        pg_conn,
    )
    pg_conn.commit()

    assert (
        run_query(
            """
            SELECT "a", "A", b
            FROM test_case_sensitive_iceberg_names.empty
            """,
            pg_conn,
        )
        == []
    )

    for enabled in ("on", "off"):
        run_command(
            f"SET pg_lake_table.enable_full_query_pushdown = {enabled}",
            pg_conn,
        )
        result = run_query(
            """
            SELECT "a", "A", b
            FROM test_case_sensitive_iceberg_names.cols
            WHERE "A" = 100
            ORDER BY "a"
            """,
            pg_conn,
        )
        assert result == [[1, 100, 7], [1, 100, 7]]

        other = run_query(
            """
            SELECT "A" FROM test_case_sensitive_iceberg_names.cols
            WHERE "a" = 1
            """,
            pg_conn,
        )
        assert other == [[100], [100]]

        aliased = run_query(
            """
            SELECT foo, bar
            FROM test_case_sensitive_iceberg_names.cols AS x(foo, bar, baz)
            WHERE bar = 100
            ORDER BY foo
            """,
            pg_conn,
        )
        assert aliased == [[1, 100], [1, 100]]

        nested = run_query(
            """
            SELECT s."A"
            FROM (
                SELECT "a", "A", b
                FROM test_case_sensitive_iceberg_names.cols
            ) s
            WHERE s."a" = 1
            ORDER BY s."A"
            """,
            pg_conn,
        )
        assert nested == [[100], [100]]

        cte = run_query(
            """
            WITH s AS (
                SELECT "a", "A", b
                FROM test_case_sensitive_iceberg_names.cols
            )
            SELECT "A" FROM s WHERE "a" = 1 ORDER BY "A"
            """,
            pg_conn,
        )
        assert cte == [[100], [100]]

        swapped = run_query(
            """
            SELECT "a", "A", b
            FROM test_case_sensitive_iceberg_names.swapped
            ORDER BY b
            """,
            pg_conn,
        )
        assert swapped == [[1, 100, 7], [1, 100, 7]]

        partial = run_query(
            """
            SELECT foo, x."A", x.b
            FROM test_case_sensitive_iceberg_names.cols AS x(foo)
            WHERE x."A" = 100
            ORDER BY foo
            """,
            pg_conn,
        )
        assert partial == [[1, 100, 7], [1, 100, 7]]

        joined = run_query(
            """
            SELECT l."A", r."a"
            FROM test_case_sensitive_iceberg_names.cols l
            JOIN test_case_sensitive_iceberg_names.cols r USING ("a")
            WHERE l."A" = 100
            ORDER BY r.b
            """,
            pg_conn,
        )
        assert joined == [[100, 1], [100, 1], [100, 1], [100, 1]]

        two_tables = run_query(
            """
            SELECT s."a", s."A"
            FROM (
                SELECT l."a", r."A"
                FROM test_case_sensitive_iceberg_names.cols l
                CROSS JOIN test_case_sensitive_iceberg_names.swapped r
            ) s
            """,
            pg_conn,
        )
        assert two_tables == [[1, 100]] * 4

        cte_alias = run_query(
            """
            WITH s(x) AS (
                SELECT "a", "A", b FROM test_case_sensitive_iceberg_names.cols
            )
            SELECT x, "A" FROM s ORDER BY x
            """,
            pg_conn,
        )
        assert cte_alias == [[1, 100], [1, 100]]

        using_subquery = run_query(
            """
            SELECT l."A", r."A"
            FROM test_case_sensitive_iceberg_names.cols l
            JOIN (
                SELECT "a", "A", b FROM test_case_sensitive_iceberg_names.cols
            ) r USING ("a", "A")
            """,
            pg_conn,
        )
        assert using_subquery == [[100, 100]] * 4

        natural = run_query(
            """
            SELECT count(*)
            FROM test_case_sensitive_iceberg_names.cols
            NATURAL JOIN test_case_sensitive_iceberg_names.swapped
            """,
            pg_conn,
        )
        assert natural == [[4]]

        mixed = run_query(
            """
            SELECT l."A", p.z
            FROM test_case_sensitive_iceberg_names.cols l
            JOIN test_case_sensitive_iceberg_names.plain p USING ("a")
            """,
            pg_conn,
        )
        assert mixed == [[100, 5], [100, 5]]

        recursive = run_query(
            """
            WITH RECURSIVE r AS (
                SELECT DISTINCT "a", "A" FROM test_case_sensitive_iceberg_names.cols
                UNION ALL
                SELECT "a" + 1, "A" FROM r WHERE "a" < 3
            )
            SELECT "a", "A" FROM r ORDER BY "a"
            """,
            pg_conn,
        )
        assert recursive == [[1, 100], [2, 100], [3, 100]]

        aliased_join = run_query(
            """
            SELECT j."A"
            FROM (
                test_case_sensitive_iceberg_names.lo l
                CROSS JOIN test_case_sensitive_iceberg_names.up u
            ) AS j
            """,
            pg_conn,
        )
        assert aliased_join == [[2]]

    run_command(
        """
        UPDATE test_case_sensitive_iceberg_names.cols
        SET b = 9
        WHERE "a" = 1 AND "A" = 100
        """,
        pg_conn,
    )
    assert (
        run_query(
            """
            SELECT count(*)
            FROM test_case_sensitive_iceberg_names.cols
            WHERE "a" = 1 AND "A" = 100 AND b = 9
            """,
            pg_conn,
        )
        == [[2]]
    )
    pg_conn.commit()

    run_command("RESET pg_lake_table.enable_full_query_pushdown", pg_conn)

    metadata = run_query(
        """
        SELECT metadata_location FROM lake_iceberg.tables
        WHERE table_namespace = 'test_case_sensitive_iceberg_names'
          AND table_name = 'cols'
        """,
        pg_conn,
    )
    parsed = json.loads(read_s3_operations(s3, metadata[0][0]))
    schema = next(
        item
        for item in parsed["schemas"]
        if item["schema-id"] == parsed["current-schema-id"]
    )
    assert [(field["name"], field["id"]) for field in schema["fields"]] == [
        ("a", 1),
        ("A", 2),
        ("b", 3),
    ]

    run_command(
        f"""
        CREATE FOREIGN TABLE test_case_sensitive_iceberg_names.cols_ro (
            "a" int, "A" int, b int
        ) SERVER pg_lake OPTIONS (
            path '{metadata[0][0]}',
            format 'iceberg'
        )
        """,
        pg_conn,
    )
    assert (
        run_query(
            """
            SELECT "a", "A", b
            FROM test_case_sensitive_iceberg_names.cols_ro
            WHERE "a" = 1 AND "A" = 100
            ORDER BY b
            """,
            pg_conn,
        )
        == [[1, 100, 9], [1, 100, 9]]
    )

    run_command(
        "DROP SCHEMA test_case_sensitive_iceberg_names CASCADE",
        pg_conn,
    )
    pg_conn.commit()


def test_case_sensitive_iceberg_copy_from_json(
    pg_conn, superuser_conn, s3, extension, with_default_location
):
    key = "test_case_sensitive_iceberg_copy_from_json/data.json"
    s3.put_object(
        Bucket=TEST_BUCKET,
        Key=key,
        Body=(
            '{"a": 1, "A": "x", "tags": [1, 2], "m": {"k": 1}}\n'
            '{"a": 2, "A": "y", "tags": [], "m": {}}\n'
        ),
    )
    map_type = run_query(
        "SELECT map_type.create('text', 'int', 'case_json_text_int')::text",
        superuser_conn,
    )[0][0]
    superuser_conn.commit()
    run_command(
        f"""
        CREATE TABLE json_cols (
            "a" int, "A" text, tags int[], m {map_type}
        ) USING iceberg;
        COPY json_cols FROM 's3://{TEST_BUCKET}/{key}' WITH (format 'json');
        """,
        pg_conn,
    )

    assert run_query(
        f'SELECT "a", "A", tags, map_type.extract(m, \'k\') FROM json_cols ORDER BY "a"',
        pg_conn,
    ) == [
        [1, "x", [1, 2], 1],
        [2, "y", [], None],
    ]
    pg_conn.rollback()


def test_case_sensitive_copy_from_iceberg_without_source_clash(
    pg_conn, s3, extension, with_default_location
):
    run_command(
        """
        CREATE SCHEMA case_copy_from_iceberg;
        CREATE TABLE case_copy_from_iceberg.src (x int, y int, w int) USING iceberg;
        INSERT INTO case_copy_from_iceberg.src VALUES (1, 100, 7);
        CREATE TABLE case_copy_from_iceberg.cols ("a" int, "A" int, b int) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()
    metadata = run_query(
        """
        SELECT metadata_location FROM lake_iceberg.tables
        WHERE table_namespace = 'case_copy_from_iceberg'
          AND table_name = 'src'
        """,
        pg_conn,
    )
    run_command(
        f"COPY case_copy_from_iceberg.cols FROM '{metadata[0][0]}' WITH (format 'iceberg')",
        pg_conn,
    )
    assert run_query(
        'SELECT "a", "A", b FROM case_copy_from_iceberg.cols', pg_conn
    ) == [[1, 100, 7]]
    run_command("DROP SCHEMA case_copy_from_iceberg CASCADE", pg_conn)
    pg_conn.commit()


def test_case_sensitive_partition_and_add_column(
    pg_conn, s3, extension, with_default_location
):
    run_command(
        """
        CREATE TABLE parted ("a" int, "A" int, b int) USING iceberg
            WITH (partition_by = '"A"');
        INSERT INTO parted VALUES (1, 100, 7);
        CREATE TABLE grown ("a" int, b int) USING iceberg;
        INSERT INTO grown VALUES (1, 7);
        ALTER TABLE grown ADD COLUMN "A" int;
        """,
        pg_conn,
    )

    assert run_query('SELECT "a", "A", b FROM parted', pg_conn) == [[1, 100, 7]]
    assert run_query('SELECT "a", "A", b FROM grown ORDER BY "a"', pg_conn) == [
        [1, None, 7]
    ]

    run_command('INSERT INTO grown ("a", b, "A") VALUES (2, 8, 100)', pg_conn)
    assert run_query('SELECT "a", "A", b FROM grown ORDER BY "a"', pg_conn) == [
        [1, None, 7],
        [2, 100, 8],
    ]
    pg_conn.rollback()


def test_nested_case_collision_has_a_clear_error(
    pg_conn, s3, extension, with_default_location
):
    error = run_command(
        """
        CREATE TYPE case_nest AS ("a" int, "A" int);
        CREATE TABLE nested_case (n case_nest) USING iceberg;
        INSERT INTO nested_case VALUES (ROW(1, 2));
        """,
        pg_conn,
        raise_error=False,
    )
    assert "nested fields that differ only by letter case are not supported" in str(
        error
    )
    pg_conn.rollback()
