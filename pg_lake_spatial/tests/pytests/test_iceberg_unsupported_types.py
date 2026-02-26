import pytest
import psycopg2
from utils_pytest import *


def test_unsupported_types(
    pg_conn,
    s3,
    spatial_analytics_extension,
    pg_lake_extension,
    extension,
    with_default_location,
):

    numeric_map_type_name = create_map_type("int", "numeric(40,40)")
    geo_map_type_name = create_map_type("int", "geometry")
    range_map_type_name = create_map_type("int", "int4multirange")
    table_map_type_name = create_map_type("int", "pg_class")

    # let's create the types that we'll use in the tests
    run_command(
        """
        CREATE SCHEMA test_unsupported_types;
        CREATE TABLE test_unsupported_types.table_type (a int);
        CREATE TYPE test_unsupported_types.nest_table_type as (a int, b test_unsupported_types.table_type);
        CREATE TYPE test_unsupported_types.geo_in_composite as (a int, b geometry);
        CREATE TYPE test_unsupported_types.numeric_in_composite as (a int, b numeric(40,40));
        CREATE TYPE test_unsupported_types.nest_level_1_geo as (a geometry);
        CREATE TYPE test_unsupported_types.nest_level_1_range as (a int8range);
        CREATE TYPE test_unsupported_types.nest_level_2_geo as (a int, b test_unsupported_types.nest_level_1_geo);
        CREATE TYPE test_unsupported_types.nest_level_2_range as (a int, b test_unsupported_types.nest_level_1_range);


    """,
        pg_conn,
    )
    pg_conn.commit()

    nested_map_type_name = create_map_type(
        "int", "test_unsupported_types.nest_level_2_geo"
    )

    # first level table
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.table_type) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # second level table
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.table_type[]) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # second level table in a composite type
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.nest_table_type) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # geometry array
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a geometry[]) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # geometry in composite key
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.nest_level_1_geo) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # geometry in second level composite key
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.nest_level_2_geo) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # range type array
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a int4range[]) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # multi-range type array
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a int4multirange[]) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # range type in second level composite key
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.nest_level_2_range) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported numeric
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a numeric(40,50)) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "with precision" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported numeric in array
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a numeric(40,50)[]) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "with precision" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported numeric in composite
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.numeric_in_composite) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "with precision" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported numeric in composite array
    error = run_command(
        "CREATE TABLE test_unsupported_types.t (a test_unsupported_types.numeric_in_composite[][]) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "with precision" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported geometry in map
    error = run_command(
        f"CREATE TABLE test_unsupported_types.t (a {geo_map_type_name}) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported (multi)range in map
    error = run_command(
        f"CREATE TABLE test_unsupported_types.t (a {range_map_type_name}) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported geometry in map
    error = run_command(
        f"CREATE TABLE test_unsupported_types.t (a {table_map_type_name}) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported geometry in map
    error = run_command(
        f"CREATE TABLE test_unsupported_types.t (a {nested_map_type_name}) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # create table as also triggers the same error
    # first create a local table then to ctas
    error = run_command(
        f"""CREATE TABLE test_unsupported_types.t_local (a {nested_map_type_name});
                            CREATE TABLE test_unsupported_types.t USING iceberg AS SELECT * FROM test_unsupported_types.t_local;
        """,
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # cannot add these types with ALTER TABLE .. ADD COLUMN either
    columns = [
        f"{nested_map_type_name}",
        "test_unsupported_types.nest_level_2_geo",
        "test_unsupported_types.geo_in_composite",
    ]
    for column in columns:
        error = run_command(
            f"""CREATE TABLE test_unsupported_types.t (a int) USING iceberg;
                                ALTER TABLE test_unsupported_types.t ADD COLUMN b {column};
            """,
            pg_conn,
            raise_error=False,
        )
        assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
        pg_conn.rollback()

    # cannot add via partition
    error = run_command(
        f"""CREATE TABLE orders (
                                order_id INT,
                                order_date DATE,
                                geo {nested_map_type_name}
                            ) PARTITION BY RANGE (order_date);

                            CREATE TABLE orders_2024
                            PARTITION OF orders
                            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')  USING iceberg;
        """,
        pg_conn,
        raise_error=False,
    )
    assert "pg_lake_iceberg:" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # unsupported type partition of with constraint
    error = run_command(
        f"""CREATE TABLE orders (
                                order_id INT,
                                order_date DATE,
                                amount numeric(40,40)
                            ) PARTITION BY RANGE (order_date);

                            CREATE TABLE orders_2024
                            PARTITION OF orders (amount default 0)
                            FOR VALUES FROM ('2024-01-01') TO ('2025-01-01')  USING iceberg;
        """,
        pg_conn,
        raise_error=False,
    )
    assert "with precision" in str(error) and "not supported" in str(error)
    pg_conn.rollback()

    # TODO: we cannot detect numeric types in map
    error = run_command(
        f"CREATE TABLE test_unsupported_types.t (a {numeric_map_type_name}) USING iceberg",
        pg_conn,
        raise_error=False,
    )
    assert error == None

    run_command("DROP SCHEMA test_unsupported_types CASCADE", pg_conn)

    pg_conn.commit()
