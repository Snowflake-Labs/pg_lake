import re

import pytest
from pyiceberg.table import StaticTable
from utils_pytest import *

SCHEMA = "special_partition_names"

# Partition field names are the field names of the partition record in each
# manifest's Avro schema, where a name may only contain letters, digits and
# underscores and cannot start with a digit. Each case partitions on a column
# whose name has to be escaped there, and lists the escaped partition field name.
cases = [
    ("space", "part col", "int", "{col}", [1, 2], "part_x20col"),
    ("hyphen", "a-b", "int", "truncate(10, {col})", [5, 25], "a_x2Db_trunc_10"),
    ("leading_digit", "1abc", "int", "bucket(4, {col})", [1, 3], "_1abc_bucket_4"),
    (
        "non_ascii",
        "café",
        "date",
        "day({col})",
        ["2024-01-01", "2024-01-02"],
        "caf_xC3_xA9_day",
    ),
    ("brace", "}", "text", "{col}", ["x", "y"], "_x7D"),
    ("double_quote", 'a"b', "int", "{col}", [1, 2], "a_x22b"),
    ("dot", "x.y", "int", "{col}", [1, 2], "x_x2Ey"),
    (
        "long_name",
        "-" * 63,
        "text",
        "truncate(1, {col})",
        ["a", "b"],
        "_x2D" * 63 + "_trunc_1",
    ),
]


@pytest.fixture(scope="module")
def special_partition_names_schema(pg_conn, s3, extension):
    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()


def quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


def sql_literal(value):
    return str(value) if isinstance(value, int) else f"'{value}'"


def metadata_location(table, pg_conn):
    return run_query(
        f"SELECT metadata_location FROM iceberg_tables "
        f"WHERE table_namespace = '{SCHEMA}' AND table_name = '{table}'",
        pg_conn,
    )[0][0]


def table_rows(table, col, pg_conn):
    return run_query(f"SELECT id, {col}::text FROM {table} ORDER BY id", pg_conn)


def create_external_table(table, pg_conn):
    """Create a read-only table over the current metadata, which reads the manifests."""
    external = f"{SCHEMA}.{table}_external"
    run_command(
        f"CREATE FOREIGN TABLE {external} () SERVER pg_lake "
        f"OPTIONS (path '{metadata_location(table, pg_conn)}')",
        pg_conn,
    )
    return external


@pytest.mark.parametrize(
    "test_id, name, column_type, partition_by, values, avro_name",
    cases,
    ids=[case[0] for case in cases],
)
def test_partition_by_special_column_name(
    special_partition_names_schema,
    create_reserialize_helper_functions,
    pg_conn,
    with_default_location,
    test_id,
    name,
    column_type,
    partition_by,
    values,
    avro_name,
):
    table = f"t_{test_id}"
    col = quote_ident(name)
    first, second = (sql_literal(v) for v in values)

    run_command(
        f"""
        CREATE TABLE {SCHEMA}.{table} (id int, {col} {column_type})
        USING iceberg WITH (partition_by = '{partition_by.format(col=col)}');
        INSERT INTO {SCHEMA}.{table} VALUES (1, {first}), (2, {second}), (3, {first});
        """,
        pg_conn,
    )
    pg_conn.commit()

    expected = [[1, str(values[0])], [2, str(values[1])], [3, str(values[0])]]
    assert table_rows(f"{SCHEMA}.{table}", col, pg_conn) == expected

    location = metadata_location(table, pg_conn)
    manifest_names = run_query(
        f"SELECT DISTINCT partition_field_name "
        f"FROM lake_iceberg.current_partition_fields('{location}')",
        pg_conn,
    )
    assert manifest_names == [[avro_name]]
    assert re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", avro_name)

    # the partition spec in the table metadata keeps the column's own name
    metadata_query = f"SELECT * FROM lake_iceberg.metadata('{location}')"
    metadata = run_query(metadata_query, pg_conn)[0][0]
    spec = metadata["partition-specs"][metadata["default-spec-id"]]
    assert spec["fields"][0]["name"].startswith(name)

    external = create_external_table(table, pg_conn)
    assert table_rows(external, col, pg_conn) == expected

    # pruning matches the partition values in the manifest to the spec by field id
    plan = run_query(
        f"EXPLAIN (analyze, verbose, format json) SELECT * FROM {external} "
        f"WHERE {col} = {second}",
        pg_conn,
    )
    assert int(fetch_data_files_used(plan)) == 1
    pg_conn.rollback()

    iceberg_table = StaticTable.from_metadata(
        location,
        properties={
            "s3.access-key-id": TEST_AWS_ACCESS_KEY_ID,
            "s3.secret-access-key": TEST_AWS_SECRET_ACCESS_KEY,
            "s3.endpoint": f"http://localhost:{MOTO_PORT}",
            "s3.region": TEST_AWS_REGION,
        },
    )
    pyiceberg_rows = sorted(
        [row["id"], str(row[name])]
        for row in iceberg_table.scan().to_arrow().to_pylist()
    )
    assert pyiceberg_rows == expected

    run_command(
        f"""
        UPDATE {SCHEMA}.{table} SET id = 4 WHERE id = 3;
        DELETE FROM {SCHEMA}.{table} WHERE id = 1;
        """,
        pg_conn,
    )
    pg_conn.commit()

    expected = [[2, str(values[1])], [4, str(values[0])]]
    assert table_rows(f"{SCHEMA}.{table}", col, pg_conn) == expected

    # VACUUM rewrites the data files and merges the manifests it reads back
    run_command_outside_tx(
        [
            "SET pg_lake_iceberg.manifest_min_count_to_merge = 2;",
            f"VACUUM {SCHEMA}.{table}",
        ]
    )

    assert table_rows(f"{SCHEMA}.{table}", col, pg_conn) == expected
    external = create_external_table(table, pg_conn)
    assert table_rows(external, col, pg_conn) == expected
    pg_conn.rollback()
