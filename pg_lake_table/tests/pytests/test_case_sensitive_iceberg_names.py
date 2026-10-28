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
        assert result == [(1, 100, 7), (1, 100, 7)]

        other = run_query(
            """
            SELECT "A" FROM test_case_sensitive_iceberg_names.cols
            WHERE "a" = 1
            """,
            pg_conn,
        )
        assert other == [(100,), (100,)]

        aliased = run_query(
            """
            SELECT foo, bar
            FROM test_case_sensitive_iceberg_names.cols AS x(foo, bar, baz)
            WHERE bar = 100
            ORDER BY foo
            """,
            pg_conn,
        )
        assert aliased == [(1, 100), (1, 100)]

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
        assert nested == [(100,), (100,)]

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
        assert cte == [(100,), (100,)]

        swapped = run_query(
            """
            SELECT "a", "A", b
            FROM test_case_sensitive_iceberg_names.swapped
            ORDER BY b
            """,
            pg_conn,
        )
        assert swapped == [(1, 100, 7), (1, 100, 7)]

        partial = run_query(
            """
            SELECT foo, x."A", x.b
            FROM test_case_sensitive_iceberg_names.cols AS x(foo)
            WHERE x."A" = 100
            ORDER BY foo
            """,
            pg_conn,
        )
        assert partial == [(1, 100, 7), (1, 100, 7)]

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
        assert joined == [(100, 1), (100, 1), (100, 1), (100, 1)]

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
        == [(2,)]
    )

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
        "DROP SCHEMA test_case_sensitive_iceberg_names CASCADE",
        pg_conn,
    )
    pg_conn.commit()
