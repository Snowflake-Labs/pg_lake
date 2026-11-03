import pytest
from utils_pytest import *

BLOOM_OPTION = '"write.parquet.bloom-filter-enabled.column.{}"'


def bloom_filter_counts(pg_conn, pgduck_conn, table_name):
    """Return {column: (column chunks with a bloom filter, all column chunks)}."""
    paths = run_query(
        f"SELECT path FROM lake_table.files WHERE table_name = '{table_name}'::regclass",
        pg_conn,
    )
    path_list = ", ".join(f"'{path}'" for (path,) in paths)
    rows = run_query(
        f"""
        SELECT path_in_schema, count(bloom_filter_offset), count(*)
        FROM parquet_metadata([{path_list}]) GROUP BY path_in_schema
        """,
        pgduck_conn,
    )
    return {row[0]: (row[1], row[2]) for row in rows}


def test_bloom_filter(s3, pg_conn, pgduck_conn, extension, with_default_location):
    pg_conn.autocommit = True

    # unique values are not dictionary-encoded, so DuckDB writes bloom
    # filters only for the enabled columns
    run_command(
        f"""
        CREATE TABLE test_bloom (something_id bigint, name text, other_id bigint)
            USING iceberg
            WITH ({BLOOM_OPTION.format("something_id")} = 'true',
                  {BLOOM_OPTION.format("name")} = 'true',
                  autovacuum_enabled = false);
        """,
        pg_conn,
    )
    run_command(
        "INSERT INTO test_bloom SELECT i, 'name-' || i, i FROM generate_series(1, 10000) i",
        pg_conn,
    )
    run_command("INSERT INTO test_bloom VALUES (-1, 'single', -1)", pg_conn)

    counts = bloom_filter_counts(pg_conn, pgduck_conn, "test_bloom")
    assert counts["something_id"][0] == counts["something_id"][1]
    assert counts["name"][0] == counts["name"][1]
    assert counts["other_id"][0] == 0

    result = run_query("SELECT name FROM test_bloom WHERE something_id = 4242", pg_conn)
    assert result == [["name-4242"]]

    run_command(
        f"ALTER FOREIGN TABLE test_bloom OPTIONS (ADD {BLOOM_OPTION.format('other_id')} 'true')",
        pg_conn,
    )
    run_command("INSERT INTO test_bloom VALUES (-2, 'other', -2)", pg_conn)

    counts = bloom_filter_counts(pg_conn, pgduck_conn, "test_bloom")
    assert counts["other_id"][0] == 1

    error = run_command(
        f"ALTER FOREIGN TABLE test_bloom OPTIONS (ADD {BLOOM_OPTION.format('missing')} 'true')",
        pg_conn,
        raise_error=False,
    )
    assert 'column "missing" in option' in error

    run_command("DROP TABLE test_bloom", pg_conn)
    pg_conn.autocommit = False


@pytest.mark.parametrize(
    "column, value, expected_error",
    [
        ("missing", "true", 'column "missing" in option'),
        ("id", "yes please", "requires a Boolean value"),
        ("flag", "true", 'column "flag" of type boolean does not support'),
        ("amount", "true", 'column "amount" of type numeric(30,2) does not support'),
        ("duration", "true", 'column "duration" of type interval does not support'),
    ],
)
def test_bloom_filter_invalid_options(
    s3, pg_conn, extension, with_default_location, column, value, expected_error
):
    error = run_command(
        f"""
        CREATE TABLE test_bloom_invalid (id bigint, flag bool, amount numeric(30,2), duration interval)
            USING iceberg WITH ({BLOOM_OPTION.format(column)} = '{value}')
        """,
        pg_conn,
        raise_error=False,
    )
    assert expected_error in error
    pg_conn.rollback()
