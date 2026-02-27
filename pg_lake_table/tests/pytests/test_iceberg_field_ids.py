import pytest
import json
import random
import string
import sys
from utils_pytest import *
from helpers.spark import *

TABLE_NAME = "test_pg_lake_get_leaf_fields"
TABLE_NAMESPACE = "public"


def test_pg_lake_iceberg_get_leaf_fields_for_spark_table(
    installcheck,
    spark_metadata_location,
    superuser_conn,
    spark_session,
    duckdb_conn,
    create_helper_functions,
):
    if installcheck:
        return

    pg_query = f"SELECT field_id FROM lake_iceberg.get_leaf_fields('{spark_metadata_location}') ORDER BY field_id;"

    # we make use of parquet extension to get the schema of the table, so lets get the path of the first data file
    first_data_file_path = (
        spark_session.sql(
            f"SELECT file_path FROM {TABLE_NAMESPACE}.{TABLE_NAME}.files;",
        )
        .collect()[0]
        .file_path
    )

    duckdb_query = f"SELECT field_id from parquet_schema('{first_data_file_path}') WHERE num_children is null ORDER BY field_id;"

    result = assert_query_result_on_duckdb_and_pg(
        duckdb_conn, superuser_conn, duckdb_query, pg_query
    )

    assert len(result) == 26

    pg_query = f"SELECT * FROM lake_iceberg.get_leaf_fields('{spark_metadata_location}') ORDER BY field_id;"

    result = run_query(pg_query, superuser_conn)

    assert result == [
        [3, "text", -1],
        [4, "real", -1],
        [5, "double precision", -1],
        [6, "integer", -1],
        [7, "bigint", -1],
        [8, "boolean", -1],
        [9, "date", -1],
        [10, "timestamp with time zone", -1],
        [11, "bytea", -1],
        [12, "numeric", 655366],
        [13, "integer", -1],
        [14, "timestamp without time zone", -1],
        [15, "text", -1],
        [16, "text", -1],
        [17, "integer", -1],
        [27, "integer", -1],
        [30, "integer", -1],
        [31, "text", -1],
        [32, "integer", -1],
        [35, "integer", -1],
        [36, "text", -1],
        [37, "integer", -1],
        [40, "integer", -1],
        [43, "integer", -1],
        [44, "text", -1],
        [45, "text", -1],
    ]

    superuser_conn.rollback()


def test_pg_lake_iceberg_get_leaf_fields_for_pg_lake_table(
    installcheck,
    pg_lake_metadata_location,
    superuser_conn,
    duckdb_conn,
    create_helper_functions,
):
    leaf_fields_catalog_query = f"""
    WITH RECURSIVE field_hierarchy AS (
                    SELECT table_name,
                            field_id,
                            field_pg_type,
                            field_pg_typemod,
                            parent_field_id,
                            pg_attnum AS top_level_pg_attnum 
                    FROM lake_table.field_id_mappings f JOIN pg_attribute attr
                        ON (attr.attrelid = f.table_name AND attr.attnum = f.pg_attnum)
                    WHERE parent_field_id IS NULL
                        AND NOT attr.attisdropped
                        AND table_name = '{TABLE_NAME}'::regclass
      
					UNION ALL

                    SELECT f.table_name,
                           f.field_id,
                           f.field_pg_type,
                           f.field_pg_typemod,
                           f.parent_field_id,
                           fh.top_level_pg_attnum
                    FROM lake_table.field_id_mappings f JOIN field_hierarchy fh
                      ON f.parent_field_id = fh.field_id AND f.table_name = fh.table_name
                   )

                   SELECT field_id, field_pg_type, field_pg_typemod 
                    FROM field_hierarchy fh 
                   WHERE NOT EXISTS (
                    SELECT 1
                     FROM lake_table.field_id_mappings f
                     WHERE f.parent_field_id = fh.field_id AND
                           f.table_name = fh.table_name
                   ) ORDER BY field_id;"""

    result = run_query(leaf_fields_catalog_query, superuser_conn)

    assert result == [
        [8, "text", -1],
        [9, "real", -1],
        [10, "double precision", -1],
        [11, "integer", -1],
        [12, "bigint", -1],
        [13, "boolean", -1],
        [14, "date", -1],
        [15, "timestamp with time zone", -1],
        [16, "bytea", -1],
        [17, "numeric", 655366],
        [18, "smallint", -1],
        [19, "timestamp without time zone", -1],
        [20, '"char"', -1],
        [21, "character varying", 14],
        [22, "bytea", -1],
        [25, "integer", -1],
        [28, "integer", -1],
        [29, "text", -1],
        [31, "integer", -1],
        [34, "integer", -1],
        [35, "text", -1],
        [37, "integer", -1],
        [40, "integer", -1],
        [43, "integer", -1],
        [44, "text", -1],
        [45, "text", -1],
    ]

    # we make use of parquet extension to get the schema of the table, so lets get the path of the first data file
    first_data_file_path = run_query(
        f"""SELECT path FROM lake_table.files
                                         WHERE table_name = '{TABLE_NAME}'::regclass""",
        superuser_conn,
    )[0][0]

    duckdb_query = f"SELECT field_id from parquet_schema('{first_data_file_path}') WHERE num_children is null ORDER BY field_id;"

    duck_result = perform_query_on_cursor(duckdb_query, duckdb_conn)

    # verify the field ids from the catalog and the field ids from the parquet schema are the same
    for pg_row, duck_row in zip(result, duck_result):
        pg_field_id = pg_row[0]
        duck_field_id = duck_row[0]
        assert pg_field_id == duck_field_id

    superuser_conn.rollback()


@pytest.fixture(scope="module")
def spark_metadata_location(installcheck, spark_session):
    if installcheck:
        yield
        return

    table_name = f"{TABLE_NAMESPACE}.{TABLE_NAME}"

    SPARK_TABLE_COLUMNS = [
        ["scalar_col_to_drop", "string"],
        [
            "struct_col_to_drop",
            "struct<id: int, dogs: array<struct<id: int, dog_name: string>>>",
        ],
        ["a", "string"],
        ["b", "float"],
        ["c", "double"],
        ["d", "integer"],
        ["e", "long"],
        ["f", "boolean"],
        ["g", "date"],
        ["h", "timestamp"],
        ["i", "binary"],
        ["j", "numeric(10,2)"],
        ["k", "short"],
        ["l", "timestamp_ntz"],
        ["m", "char(1)"],
        ["n", "varchar(10)"],
        ["o", "byte"],
        ["p", "array<struct<id: int, dogs: array<struct<id: int, dog_name: string>>>>"],
        ["r", "struct<id: int, dogs: array<struct<id: int, dog_name: string>>>"],
        [
            "s",
            "map<int,array<struct<id: int, dogs: array<struct<id: int, dog_name: string>>>>>",
        ],
    ]

    spark_session.sql(
        f"""CREATE TABLE {table_name}(
            {', '.join([f'{col[0]} {col[1]}' for col in SPARK_TABLE_COLUMNS])}
            ) USING iceberg"""
    )

    # drop columns, they wont show up in the field ids
    spark_session.sql(f"ALTER TABLE {table_name} DROP COLUMN scalar_col_to_drop")
    spark_session.sql(f"ALTER TABLE {table_name} DROP COLUMN struct_col_to_drop")

    # add a column, this will show up in the field ids
    spark_session.sql(f"ALTER TABLE {table_name} ADD COLUMN new_column string")

    # insert a data file since we make use of duckdb parquet extension to get the field ids from parquet file
    spark_session.sql(
        f"INSERT INTO {table_name} VALUES (null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null)"
    )

    metadata_location = (
        spark_session.sql(
            f"SELECT timestamp, file FROM {table_name}.metadata_log_entries ORDER BY timestamp DESC"
        )
        .collect()[0]
        .file
    )

    yield metadata_location

    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")


@pytest.fixture(scope="function")
def pg_lake_metadata_location(superuser_conn, extension, with_default_location):
    table_name = f"{TABLE_NAMESPACE}.{TABLE_NAME}"

    # create types
    run_command(
        """
                create type dog as (id int, dog_name text);
                create type dog_owner as (id int, dogs dog[]);
                """,
        superuser_conn,
    )

    superuser_conn.commit()

    map_type_name = create_map_type("int", "dog_owner[]")

    POSTGRES_TABLE_COLUMNS = [
        ["scalar_col_to_drop", "text"],
        ["struct_col_to_drop", "dog_owner"],
        ["a", "text"],
        ["b", "real"],
        ["c", "double precision"],
        ["d", "integer"],
        ["e", "bigint"],
        ["f", "boolean"],
        ["g", "date"],
        ["h", "timestamp with time zone"],
        ["i", "bytea"],
        ["j", "numeric(10,2)"],
        ["k", "smallint"],
        ["l", "timestamp without time zone"],
        ["m", '"char"'],
        ["n", "varchar(10)"],
        ["o", "bytea"],
        ["p", "dog_owner[]"],
        ["r", "dog_owner"],
        ["s", map_type_name],
    ]

    run_command(
        f"""CREATE TABLE {table_name}(
            {', '.join([f'{col[0]} {col[1]}' for col in POSTGRES_TABLE_COLUMNS])}
            ) USING iceberg;""",
        superuser_conn,
    )

    # drop columns, they wont show up in the field ids
    run_command(
        f"ALTER TABLE {table_name} DROP COLUMN scalar_col_to_drop;", superuser_conn
    )
    run_command(
        f"ALTER TABLE {table_name} DROP COLUMN struct_col_to_drop;", superuser_conn
    )

    # add a column, this will show up in the field ids
    run_command(f"ALTER TABLE {table_name} ADD COLUMN new_column text;", superuser_conn)

    # insert a data file since we make use of duckdb parquet extension to get the field ids from parquet file
    run_command(f"INSERT INTO {table_name} VALUES (default);", superuser_conn)

    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{TABLE_NAME}'",
        superuser_conn,
    )[0][0]

    superuser_conn.commit()

    yield metadata_location

    superuser_conn.rollback()

    run_command(f"DROP TABLE IF EXISTS {table_name}", superuser_conn)
    run_command(f"DROP TYPE IF EXISTS dog_owner CASCADE", superuser_conn)
    run_command(f"DROP TYPE IF EXISTS dog CASCADE", superuser_conn)

    superuser_conn.commit()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn, s3, extension):
    run_command(
        f"""
        CREATE OR REPLACE FUNCTION lake_iceberg.get_leaf_fields(metadataUri text)
        RETURNS TABLE(field_id int, pg_type regtype, pg_typmod int)
        LANGUAGE C STRICT
        AS 'pg_lake_iceberg', $$pg_lake_get_leaf_field_ids$$;
""",
        superuser_conn,
    )

    superuser_conn.commit()

    yield

    superuser_conn.rollback()

    # Teardown: Drop the function after the test(s) are done
    run_command(
        f"""
        DROP FUNCTION IF EXISTS lake_iceberg.get_leaf_fields;
""",
        superuser_conn,
    )

    superuser_conn.commit()
