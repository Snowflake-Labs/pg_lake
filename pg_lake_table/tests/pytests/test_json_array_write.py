import pytest
from utils_pytest import *


# json[] and jsonb[] values are staged in a CSV file and parsed back with
# read_csv on the way into a data file. They used to be parsed as JSON[],
# which reads the whole cell as one JSON document: each element came out as a
# JSON string holding the element's text, and a NULL element failed the write.
VALUES = """
    (1, ARRAY['{"a": 1}', '3', '"s"', 'null', NULL]::jsonb[],
        ARRAY['{"a" : 1}', '[1,2]', NULL]::json[],
        ARRAY['[1, 2]']::jsonb[], ARRAY['true', NULL]::d_jsonb[]),
    (2, ARRAY[NULL]::jsonb[], '{}', NULL, NULL),
    (3, '{}', ARRAY['"x"']::json[], ARRAY['{"b": [true, null]}']::jsonb[], '{}')
"""


def test_json_array_iceberg_roundtrip(pg_conn, extension, s3, with_default_location):
    run_command(
        """
        CREATE SCHEMA test_json_array_write;
        SET search_path TO test_json_array_write;
        CREATE DOMAIN d_jsonbs AS jsonb[];
        CREATE DOMAIN d_jsonb AS jsonb;
        CREATE TABLE heap_t (id int, jb jsonb[], js json[], dom d_jsonbs, doms d_jsonb[]);
        CREATE TABLE ice_t (id int, jb jsonb[], js json[], dom d_jsonbs, doms d_jsonb[])
            USING iceberg;
        """,
        pg_conn,
    )
    run_command(f"INSERT INTO heap_t VALUES {VALUES}", pg_conn)
    run_command(f"INSERT INTO ice_t VALUES {VALUES}", pg_conn)
    pg_conn.commit()

    # json has no equality operator, so compare the text forms
    query = "SELECT id, jb::text, js::text, dom::text, doms::text FROM {} ORDER BY id"
    expected = run_query(query.format("heap_t"), pg_conn)
    assert run_query(query.format("ice_t"), pg_conn) == expected

    # JSON null is a value, a NULL element is not
    result = run_query(
        "SELECT jb[4] IS NULL, jb[4]::text, jb[5] IS NULL FROM ice_t WHERE id = 1",
        pg_conn,
    )
    assert result == [[False, "null", True]]

    run_command("RESET search_path", pg_conn)
    run_command("DROP SCHEMA test_json_array_write CASCADE", pg_conn)
    pg_conn.commit()


def test_json_array_copy_to_parquet(pg_conn, extension, s3, with_default_location):
    url = f"s3://{TEST_BUCKET}/test_json_array_write/data.parquet"

    run_command(
        """
        CREATE SCHEMA test_json_array_copy;
        SET search_path TO test_json_array_copy;
        CREATE DOMAIN d_jsonbs AS jsonb[];
        CREATE DOMAIN d_jsonb AS jsonb;
        CREATE TABLE heap_t (id int, jb jsonb[], js json[], dom d_jsonbs, doms d_jsonb[]);
        """,
        pg_conn,
    )
    run_command(f"INSERT INTO heap_t VALUES {VALUES}", pg_conn)
    run_command(f"COPY heap_t TO '{url}'", pg_conn)
    run_command(
        "CREATE TABLE copied_t (id int, jb jsonb[], js json[], dom d_jsonbs, doms d_jsonb[])",
        pg_conn,
    )
    run_command(f"COPY copied_t FROM '{url}'", pg_conn)
    pg_conn.commit()

    query = "SELECT id, jb::text, js::text, dom::text, doms::text FROM {} ORDER BY id"
    assert run_query(query.format("copied_t"), pg_conn) == run_query(
        query.format("heap_t"), pg_conn
    )

    run_command("RESET search_path", pg_conn)
    run_command("DROP SCHEMA test_json_array_copy CASCADE", pg_conn)
    pg_conn.commit()
