import pytest
from decimal import Decimal
from utils_pytest import *


def test_unbounded_numeric_converted_to_double(
    s3, pg_conn, extension, with_default_location
):
    """Unbounded numeric columns become float8 on Iceberg tables (GUC default on)."""
    run_command(
        """
        CREATE TABLE test_num_double (a numeric, b numeric(10,2)) USING iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_double' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["a", "double precision"], ["b", "numeric"]]

    pg_conn.rollback()


def test_precision_above_38_converted_to_double(
    s3, pg_conn, extension, with_default_location
):
    """Bounded numeric with precision > 38 becomes float8 on Iceberg tables."""
    run_command(
        """
        CREATE TABLE test_num_double (a numeric(50,10)) USING iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_double' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["a", "double precision"]]

    pg_conn.rollback()


def test_guc_off_unbounded_numeric_errors(
    s3, pg_conn, extension, with_default_location
):
    """With GUC disabled, unbounded numeric errors."""
    run_command("SET pg_lake_iceberg.unsupported_numeric_as_double TO off", pg_conn)

    error = run_command(
        """
        CREATE TABLE test_num_keep (a numeric, b numeric(10,2)) USING iceberg;
    """,
        pg_conn,
        raise_error=False,
    )

    assert "numeric type is not supported on Iceberg tables" in error

    pg_conn.rollback()


def test_guc_off_precision_above_38_errors(
    s3, pg_conn, extension, with_default_location
):
    """With GUC disabled, precision > 38 still errors (old behaviour)."""
    run_command("SET pg_lake_iceberg.unsupported_numeric_as_double TO off", pg_conn)

    error = run_command(
        """
        CREATE TABLE test_num_err (a numeric(50,10)) USING iceberg;
    """,
        pg_conn,
        raise_error=False,
    )

    assert "numeric type is not supported on Iceberg tables" in error

    pg_conn.rollback()


def test_insert_and_read_converted_unbounded_numeric(
    s3, pg_conn, extension, with_default_location
):
    """Insert values into a converted unbounded numeric column and read them back."""
    run_command(
        """
        CREATE TABLE test_num_insert (id int, val numeric) USING iceberg;
        INSERT INTO test_num_insert VALUES (1, 3.14), (2, -999.5), (3, 0);
    """,
        pg_conn,
    )

    result = run_query("SELECT id, val FROM test_num_insert ORDER BY id", pg_conn)
    assert result == [[1, 3.14], [2, -999.5], [3, 0.0]]

    pg_conn.rollback()


def test_insert_and_read_converted_large_precision_numeric(
    s3, pg_conn, extension, with_default_location
):
    """Insert values into a converted large-precision numeric column and read them back."""
    run_command(
        """
        CREATE TABLE test_num_large (id int, val numeric(50,10)) USING iceberg;
        INSERT INTO test_num_large VALUES (1, 12345.6789), (2, -0.001);
    """,
        pg_conn,
    )

    result = run_query("SELECT id, val FROM test_num_large ORDER BY id", pg_conn)
    assert result == [[1, 12345.6789], [2, -0.001]]

    pg_conn.rollback()


def test_iceberg_metadata_shows_double_for_converted_column(
    s3, pg_conn, extension, with_default_location
):
    """Iceberg metadata should show 'double' type for converted numeric columns."""
    location = f"s3://{TEST_BUCKET}/test_num_metadata"
    run_command(
        f"""
        CREATE FOREIGN TABLE test_num_meta (a numeric, b numeric(50,5))
            SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )
    pg_conn.commit()

    import json

    results = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 'test_num_meta'",
        pg_conn,
    )
    metadata_path = results[0][0]
    data = read_s3_operations(s3, metadata_path)
    parsed = json.loads(data)
    fields = parsed["schemas"][0]["fields"]

    assert fields[0]["name"] == "a"
    assert fields[0]["type"] == "double"
    assert fields[1]["name"] == "b"
    assert fields[1]["type"] == "double"

    run_command("DROP FOREIGN TABLE test_num_meta", pg_conn)
    pg_conn.commit()


def test_pushdown_works_for_converted_column(
    s3, pg_conn, extension, with_default_location
):
    """Filter pushdown should work for columns converted from numeric to float8."""
    run_command(
        """
        CREATE TABLE test_num_push (id int, val numeric) USING iceberg;
        INSERT INTO test_num_push VALUES (1, 10.0), (2, 20.0), (3, 30.0);
    """,
        pg_conn,
    )

    query = "SELECT id, val FROM test_num_push WHERE val > 15"
    result = run_query(query, pg_conn)
    assert len(result) == 2

    assert_remote_query_contains_expression(query, "val", pg_conn)

    pg_conn.rollback()


def test_alter_table_add_unsupported_numeric_column(
    s3, pg_conn, extension, with_default_location
):
    """ALTER TABLE ADD COLUMN with unsupported numeric should also convert."""
    run_command(
        """
        CREATE TABLE test_num_alter (id int) USING iceberg;
        ALTER TABLE test_num_alter ADD COLUMN val numeric;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_alter' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["id", "integer"], ["val", "double precision"]]

    pg_conn.rollback()


def test_bounded_numeric_within_limits_not_converted(
    s3, pg_conn, extension, with_default_location
):
    """Bounded numeric with precision <= 38 should remain numeric."""
    run_command(
        """
        CREATE TABLE test_num_ok (a numeric(38,10), b numeric(20,5)) USING iceberg;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_num_ok' ORDER BY column_name",
        pg_conn,
    )
    assert result == [["a", "numeric"], ["b", "numeric"]]

    pg_conn.rollback()


def test_array_unsupported_numeric_converted_to_double(
    s3, pg_conn, extension, with_default_location
):
    """numeric[] column is converted to float8[] at table creation time."""
    run_command(
        "CREATE TABLE test_arr_conv (id int, vals numeric[]) USING iceberg",
        pg_conn,
    )

    result = run_query(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'test_arr_conv' AND column_name = 'vals'",
        pg_conn,
    )
    assert result == [["vals", "ARRAY"]]

    elem_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_arr_conv'::regclass AND attname = 'vals'",
        pg_conn,
    )
    assert elem_type[0][0] == "double precision[]"

    pg_conn.rollback()


def test_composite_unsupported_numeric_keeps_declared_type(
    s3, pg_conn, extension, with_default_location
):
    """Composite type with unsupported numeric keeps the type the user declared."""
    run_command(
        """
        CREATE TYPE test_conv_comp AS (a int, b numeric);
        CREATE TABLE test_comp_conv (id int, val test_conv_comp) USING iceberg;
    """,
        pg_conn,
    )

    col_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_comp_conv'::regclass AND attname = 'val'",
        pg_conn,
    )
    assert col_type[0][0] == "test_conv_comp"

    attr_types = run_query(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_type t ON t.oid = (
            SELECT atttypid FROM pg_attribute
            WHERE attrelid = 'test_comp_conv'::regclass AND attname = 'val'
        )
        WHERE a.attrelid = t.typrelid AND a.attnum > 0
        ORDER BY a.attnum
    """,
        pg_conn,
    )
    assert attr_types == [["a", "integer"], ["b", "numeric"]]

    pg_conn.rollback()


def test_map_unsupported_numeric_keeps_declared_type(
    s3, pg_conn, extension, with_default_location
):
    """Map type with unsupported numeric value keeps the type the user declared."""
    map_type_name = create_map_type("int", "numeric")
    run_command(
        f"CREATE TABLE test_map_conv (id int, vals {map_type_name}) USING iceberg",
        pg_conn,
    )

    col_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_map_conv'::regclass AND attname = 'vals'",
        pg_conn,
    )
    assert col_type[0][0] == map_type_name

    pg_conn.rollback()


@pytest.mark.parametrize(
    "col_sql, setup_sql",
    [
        ("vals numeric[]", None),
        ("vals numeric(50,10)[]", None),
        (
            "val test_nested_num_comp",
            "CREATE TYPE test_nested_num_comp AS (a int, b numeric)",
        ),
        (
            "val test_nested_num_comp_lg",
            "CREATE TYPE test_nested_num_comp_lg AS (a int, b numeric(50,10))",
        ),
    ],
    ids=[
        "unbounded_array",
        "large_precision_array",
        "unbounded_composite",
        "large_precision_composite",
    ],
)
def test_nested_unsupported_numeric_creates_table(
    s3, pg_conn, extension, with_default_location, col_sql, setup_sql
):
    """Unsupported numeric inside nested types should not error when GUC is on."""
    if setup_sql:
        run_command(setup_sql, pg_conn)
    run_command(
        f"CREATE TABLE test_nested_num (id int, {col_sql}) USING iceberg",
        pg_conn,
    )
    pg_conn.rollback()


@pytest.mark.parametrize(
    "numeric_type, values, expected",
    [
        ("numeric", "ARRAY[3.14, -999.5, 0]::numeric[]", [3.14, -999.5, 0.0]),
        (
            "numeric(50,10)",
            "ARRAY[12345.6789, -0.001]::numeric(50,10)[]",
            [12345.6789, -0.001],
        ),
    ],
    ids=["unbounded", "large_precision"],
)
def test_insert_and_read_nested_numeric_array(
    s3,
    pg_conn,
    extension,
    with_default_location,
    numeric_type,
    values,
    expected,
):
    """Insert and read back unsupported numeric values in an array column."""
    run_command(
        f"""
        CREATE TABLE test_num_arr_io (id int, vals {numeric_type}[]) USING iceberg;
        INSERT INTO test_num_arr_io VALUES (1, {values});
    """,
        pg_conn,
    )

    result = run_query("SELECT id, vals FROM test_num_arr_io ORDER BY id", pg_conn)
    assert result[0][0] == 1
    assert [float(v) for v in result[0][1]] == expected

    pg_conn.rollback()


@pytest.mark.parametrize(
    "numeric_type, insert_val, expected_val",
    [
        ("numeric", 3.14, 3.14),
        ("numeric(50,10)", 12345.6789, 12345.6789),
    ],
    ids=["unbounded", "large_precision"],
)
def test_insert_and_read_nested_numeric_in_composite(
    s3,
    pg_conn,
    extension,
    with_default_location,
    numeric_type,
    insert_val,
    expected_val,
):
    """Insert and read back unsupported numeric inside a composite column."""
    run_command(
        f"""
        CREATE TYPE test_rw_comp AS (a int, b {numeric_type});
        CREATE TABLE test_rw_comp_tbl (id int, val test_rw_comp) USING iceberg;
        INSERT INTO test_rw_comp_tbl VALUES (1, ROW(42, {insert_val}));
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT id, (val).a, (val).b FROM test_rw_comp_tbl ORDER BY id", pg_conn
    )
    assert result[0][0] == 1
    assert result[0][1] == 42
    assert float(result[0][2]) == expected_val

    pg_conn.rollback()


def test_iceberg_metadata_for_nested_numeric(
    s3, pg_conn, extension, with_default_location
):
    """A numeric no Iceberg decimal can hold is stored as double at every level,
    in an array and in a composite field alike."""
    location = f"s3://{TEST_BUCKET}/test_nested_num_meta"
    run_command(
        f"""
        CREATE TYPE test_nested_meta_comp AS (a int, b numeric, c numeric(45,5));
        CREATE FOREIGN TABLE test_nested_num_meta (
            id int,
            arr numeric[],
            comp test_nested_meta_comp
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )
    pg_conn.commit()

    import json

    results = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 'test_nested_num_meta'",
        pg_conn,
    )
    metadata_path = results[0][0]
    data = read_s3_operations(s3, metadata_path)
    parsed = json.loads(data)
    fields = parsed["schemas"][0]["fields"]

    id_field = next(f for f in fields if f["name"] == "id")
    arr_field = next(f for f in fields if f["name"] == "arr")
    comp_field = next(f for f in fields if f["name"] == "comp")

    assert id_field["type"] == "int"
    assert arr_field["type"]["type"] == "list"
    assert arr_field["type"]["element"] == "double"
    assert comp_field["type"]["type"] == "struct"

    # the field keeps its declared PostgreSQL type, but neither an unbounded
    # numeric nor one with precision 45 fits an Iceberg decimal, so both are
    # stored as double
    comp_b = next(f for f in comp_field["type"]["fields"] if f["name"] == "b")
    assert comp_b["type"] == "double"

    comp_c = next(f for f in comp_field["type"]["fields"] if f["name"] == "c")
    assert comp_c["type"] == "double"

    run_command("DROP FOREIGN TABLE test_nested_num_meta", pg_conn)
    pg_conn.commit()


def test_nested_unsupported_numeric_map_creates_table(
    s3, pg_conn, extension, with_default_location
):
    """Unsupported numeric as map value should not error when GUC is on."""
    map_type_name = create_map_type("int", "numeric")
    run_command(
        f"CREATE TABLE test_nested_map_num (id int, vals {map_type_name}) USING iceberg",
        pg_conn,
    )
    pg_conn.rollback()


def test_insert_and_read_nested_numeric_map(
    s3, pg_conn, extension, with_default_location
):
    """Insert and read back unsupported numeric values in a map column."""
    map_type_name = create_map_type("int", "numeric")
    run_command(
        f"""
        CREATE TABLE test_num_map_io (id int, vals {map_type_name}) USING iceberg;
    """,
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_num_map_io VALUES
            (1, ARRAY[(1, 3.14), (2, -999.5)]::{map_type_name});
    """,
        pg_conn,
    )

    result = run_query("SELECT id, vals FROM test_num_map_io ORDER BY id", pg_conn)
    assert result[0][0] == 1
    assert result[0][1] is not None

    pg_conn.rollback()


def test_iceberg_metadata_shows_double_for_map_numeric(
    s3, pg_conn, extension, with_default_location
):
    """A numeric map value keeps its declared type and is stored as a double: a
    pg_map value carries no typmod, so no map value can be an exact decimal."""
    import json

    map_type_name = create_map_type("int", "numeric")
    location = f"s3://{TEST_BUCKET}/test_map_num_meta"
    run_command(
        f"""
        CREATE FOREIGN TABLE test_map_num_meta (
            id int,
            vals {map_type_name}
        ) SERVER pg_lake_iceberg OPTIONS (location '{location}');
    """,
        pg_conn,
    )
    pg_conn.commit()

    results = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 'test_map_num_meta'",
        pg_conn,
    )
    metadata_path = results[0][0]
    data = read_s3_operations(s3, metadata_path)
    parsed = json.loads(data)
    fields = parsed["schemas"][0]["fields"]

    vals_field = next(f for f in fields if f["name"] == "vals")
    assert vals_field["type"]["type"] == "map"
    assert vals_field["type"]["key"] == "int"
    assert vals_field["type"]["value"] == "double"

    run_command("DROP FOREIGN TABLE test_map_num_meta", pg_conn)
    pg_conn.commit()


def test_deeply_nested_unsupported_numeric_keeps_declared_type(
    s3, pg_conn, extension, with_default_location
):
    """Deep nesting: a composite two levels down keeps every declared type."""
    run_command(
        """
        CREATE TYPE deep_child AS (x int, y numeric);
        CREATE TYPE deep_parent AS (id int, child deep_child, nums numeric[]);
        CREATE TABLE test_deep (id int, val deep_parent) USING iceberg;
    """,
        pg_conn,
    )

    col_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_deep'::regclass AND attname = 'val'",
        pg_conn,
    )
    assert col_type[0][0] == "deep_parent"

    outer_attrs = run_query(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_type t ON t.typrelid = a.attrelid
        WHERE t.typname = 'deep_parent' AND a.attnum > 0
        ORDER BY a.attnum
    """,
        pg_conn,
    )
    assert outer_attrs == [
        ["id", "integer"],
        ["child", "deep_child"],
        ["nums", "numeric[]"],
    ]

    child_attrs = run_query(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_type t ON t.typrelid = a.attrelid
        WHERE t.typname = 'deep_child' AND a.attnum > 0
        ORDER BY a.attnum
    """,
        pg_conn,
    )
    assert child_attrs == [["x", "integer"], ["y", "numeric"]]

    pg_conn.rollback()


@pytest.mark.parametrize(
    "numeric_type",
    ["numeric", "numeric(50,10)"],
    ids=["unbounded", "large_precision"],
)
def test_composite_with_numeric_array_keeps_declared_type(
    s3, pg_conn, extension, with_default_location, numeric_type
):
    """A numeric[] attribute of a composite keeps its declared type."""
    run_command(
        f"""
        CREATE TYPE comp_numarr AS (a int, b {numeric_type}[]);
        CREATE TABLE test_comp_numarr (id int, val comp_numarr) USING iceberg;
    """,
        pg_conn,
    )

    col_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_comp_numarr'::regclass AND attname = 'val'",
        pg_conn,
    )
    assert col_type[0][0] == "comp_numarr"

    attrs = run_query(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_type t ON t.typrelid = a.attrelid
        WHERE t.typname = 'comp_numarr' AND a.attnum > 0
        ORDER BY a.attnum
    """,
        pg_conn,
    )
    assert attrs == [["a", "integer"], ["b", f"{numeric_type}[]"]]

    pg_conn.rollback()


@pytest.mark.parametrize(
    "numeric_type",
    ["numeric", "numeric(50,10)"],
    ids=["unbounded", "large_precision"],
)
def test_array_of_composite_with_numeric_keeps_declared_type(
    s3, pg_conn, extension, with_default_location, numeric_type
):
    """An array of composite keeps its declared type, the shape from issue #540."""
    run_command(
        f"""
        CREATE TYPE comp_in_arr AS (a int, b {numeric_type});
        CREATE TABLE test_arr_comp (id int, vals comp_in_arr[]) USING iceberg;
    """,
        pg_conn,
    )

    col_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_arr_comp'::regclass AND attname = 'vals'",
        pg_conn,
    )[0][0]

    assert col_type == "comp_in_arr[]"

    attrs = run_query(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_type t ON t.typrelid = a.attrelid
        WHERE t.typname = 'comp_in_arr' AND a.attnum > 0
        ORDER BY a.attnum
    """,
        pg_conn,
    )
    assert attrs == [["a", "integer"], ["b", numeric_type]]

    pg_conn.rollback()


@pytest.mark.parametrize(
    "numeric_type",
    ["numeric", "numeric(50,10)"],
    ids=["unbounded", "large_precision"],
)
def test_map_with_numeric_array_value_keeps_declared_type(
    s3, pg_conn, extension, with_default_location, numeric_type
):
    """A map whose value is numeric[] keeps the type the user declared."""
    map_type_name = create_map_type("int", f"{numeric_type}[]")
    run_command(
        f"CREATE TABLE test_map_numarr (id int, vals {map_type_name}) USING iceberg",
        pg_conn,
    )

    col_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_map_numarr'::regclass AND attname = 'vals'",
        pg_conn,
    )
    assert col_type[0][0] == map_type_name

    pg_conn.rollback()


@pytest.mark.parametrize(
    "key_type, val_type, expect_key, expect_val",
    [
        ("numeric", "int", "numeric", "integer"),
        ("numeric", "numeric", "numeric", "numeric"),
        # map_type.create takes regtype arguments, so the typmod is not kept
        ("numeric(50,10)", "text", "numeric", "text"),
    ],
    ids=["key_only", "key_and_value", "large_precision_key"],
)
def test_map_key_keeps_declared_type(
    s3,
    pg_conn,
    extension,
    with_default_location,
    key_type,
    val_type,
    expect_key,
    expect_val,
):
    """A numeric map key keeps the type the user declared, like a map value."""
    map_type_name = create_map_type(key_type, val_type)
    run_command(
        f"CREATE TABLE test_map_key (id int, vals {map_type_name}) USING iceberg",
        pg_conn,
    )

    attr_types = run_query(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        WHERE a.attrelid = (
            SELECT typrelid FROM pg_type WHERE oid = (
                SELECT typelem FROM pg_type WHERE oid = (
                    SELECT typbasetype FROM pg_type WHERE oid = (
                        SELECT atttypid FROM pg_attribute
                        WHERE attrelid = 'test_map_key'::regclass
                          AND attname = 'vals'
                    )
                )
            )
        ) AND a.attnum > 0
        ORDER BY a.attnum
    """,
        pg_conn,
    )

    assert attr_types[0][1] == expect_key
    assert attr_types[1][1] == expect_val

    pg_conn.rollback()


@pytest.mark.parametrize(
    "setup_sql, create_sql",
    [
        (None, "CREATE TABLE test_nested_err (id int, vals numeric[]) USING iceberg"),
        (
            "CREATE TYPE test_nested_err_comp AS (a int, b numeric(50,10))",
            "CREATE TABLE test_nested_err (id int, val test_nested_err_comp) USING iceberg",
        ),
    ],
    ids=["unbounded_array", "large_precision_composite"],
)
def test_guc_off_nested_unsupported_numeric_errors(
    s3, pg_conn, extension, with_default_location, setup_sql, create_sql
):
    """With GUC disabled, unsupported numeric in nested types should still error."""
    run_command("SET pg_lake_iceberg.unsupported_numeric_as_double TO off", pg_conn)

    if setup_sql:
        run_command(setup_sql, pg_conn)

    error = run_command(create_sql, pg_conn, raise_error=False)
    assert "not supported" in error

    pg_conn.rollback()


def test_ctas_array_of_composite_with_numeric(
    s3, pg_conn, extension, with_default_location
):
    """CREATE TABLE AS from a heap table with an array of composite, issue #540."""
    run_command(
        """
        CREATE TYPE accessorial AS (code text, charges numeric);
        CREATE TABLE heap_shipment (pro_number int, accessorials accessorial[]);
        INSERT INTO heap_shipment VALUES (1, array[row('LIFT', 25.00)]::accessorial[]);
        CREATE TABLE ice_shipment USING iceberg AS SELECT * FROM heap_shipment;
    """,
        pg_conn,
    )

    col_type = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'ice_shipment'::regclass AND attname = 'accessorials'",
        pg_conn,
    )
    assert col_type[0][0] == "accessorial[]"

    result = run_query(
        "SELECT pro_number, (accessorials[1]).code, (accessorials[1]).charges "
        "FROM ice_shipment",
        pg_conn,
    )
    assert result == [[1, "LIFT", Decimal("25.000000000")]]

    pg_conn.rollback()


def test_insert_select_array_of_composite_with_numeric(
    s3, pg_conn, extension, with_default_location
):
    """INSERT..SELECT from a heap table with an array of composite, issue #540."""
    run_command(
        """
        CREATE TYPE accessorial AS (code text, charges numeric);
        CREATE TABLE heap_shipment (pro_number int, accessorials accessorial[]);
        INSERT INTO heap_shipment VALUES (1, array[row('LIFT', 25.00)]::accessorial[]);
        CREATE TABLE ice_shipment (pro_number int, accessorials accessorial[])
            USING iceberg;
        INSERT INTO ice_shipment SELECT * FROM heap_shipment;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT pro_number, (accessorials[1]).charges FROM ice_shipment",
        pg_conn,
    )
    assert result == [[1, Decimal("25.000000000")]]

    pg_conn.rollback()


def test_insert_typed_row_array_of_composite_roundtrip(
    s3, pg_conn, extension, with_default_location
):
    """A typed ROW insert into a column of a user-declared composite type: the
    declared type is still assignable, and the nested numeric round-trips through
    the double it is stored as, so digits past double precision are lost."""
    run_command(
        """
        CREATE TYPE accessorial AS (code text, charges numeric);
        CREATE TABLE ice_shipment (pro_number int, accessorials accessorial[])
            USING iceberg;
        INSERT INTO ice_shipment VALUES
            (1, array[row('LIFT', 25.00)]::accessorial[]),
            (2, array[row('WAIT', 1234567890.123456789)]::accessorial[]);
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT pro_number, (accessorials[1]).charges FROM ice_shipment "
        "ORDER BY pro_number",
        pg_conn,
    )
    assert result == [
        [1, Decimal("25")],
        [2, Decimal("1234567890.1234567")],
    ]

    pg_conn.rollback()


def test_nested_numeric_accepts_values_no_decimal_can_hold(
    s3, pg_conn, extension, with_default_location
):
    """Storing the nested numeric as a double is what makes NaN, infinity and a
    magnitude past 38 digits writable and readable: an Iceberg decimal has room
    for none of them."""
    run_command(
        """
        CREATE TYPE wide_num AS (label text, amount numeric);
        CREATE TABLE ice_wide (id int, v wide_num) USING iceberg;
        INSERT INTO ice_wide VALUES
            (1, row('nan', 'NaN'::numeric)::wide_num),
            (2, row('inf', 'Infinity'::numeric)::wide_num),
            (3, row('-inf', '-Infinity'::numeric)::wide_num),
            (4, row('big', 1e30::numeric)::wide_num),
            (5, row('frac', 0.1234567890123::numeric)::wide_num);
    """,
        pg_conn,
    )

    result = run_query("SELECT id, (v).amount FROM ice_wide ORDER BY id", pg_conn)

    assert result[0][1].is_nan()
    assert result[1][1] == Decimal("Infinity")
    assert result[2][1] == Decimal("-Infinity")
    assert result[3][1] == Decimal("1E+30")
    # more than the 9 fractional digits an Iceberg decimal(38,9) would keep
    assert result[4][1] == Decimal("0.1234567890123")

    pg_conn.rollback()


def test_nested_unsupported_numeric_reports_notice(
    s3, pg_conn, extension, with_default_location
):
    """The declared type keeps saying numeric, so CREATE TABLE says what the
    storage type actually is."""
    run_command("CREATE TYPE notice_comp AS (a int, b numeric);", pg_conn)
    pg_conn.commit()

    cursor = pg_conn.cursor()
    del pg_conn.notices[:]
    cursor.execute("CREATE TABLE ice_notice (id int, c notice_comp) USING iceberg")

    notices = "\n".join(pg_conn.notices)
    assert "contains a nested numeric" in notices
    assert "storing it as double precision" in notices

    pg_conn.rollback()


@pytest.mark.parametrize(
    "domain_def, col_type",
    [
        ("CREATE DOMAIN dom_num AS numeric", "dom_num"),
        ("CREATE DOMAIN dom_num_lg AS numeric(50,10)", "dom_num_lg"),
    ],
    ids=["unbounded_domain", "large_precision_domain"],
)
def test_domain_over_numeric_converted_to_double(
    s3, pg_conn, extension, with_default_location, domain_def, col_type
):
    """Domain wrapping an unsupported numeric is unwrapped and converted to float8."""
    run_command(domain_def, pg_conn)
    run_command(
        f"CREATE TABLE test_dom_num (id int, val {col_type}) USING iceberg",
        pg_conn,
    )

    result = run_query(
        "SELECT format_type(atttypid, atttypmod) FROM pg_attribute "
        "WHERE attrelid = 'test_dom_num'::regclass AND attname = 'val'",
        pg_conn,
    )
    assert result[0][0] == "double precision"

    pg_conn.rollback()


@pytest.mark.parametrize(
    "setup_sql, col_type, query, expected",
    [
        (
            "CREATE DOMAIN dom_num AS numeric;"
            "CREATE TYPE comp_with_dom AS (a int, b dom_num)",
            "comp_with_dom",
            """
            SELECT a.attname, format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_type t ON t.typrelid = a.attrelid
            WHERE t.oid = (
                SELECT atttypid FROM pg_attribute
                WHERE attrelid = 'test_dom_nested'::regclass AND attname = 'val'
            ) AND a.attnum > 0
            ORDER BY a.attnum
            """,
            [["a", "integer"], ["b", "dom_num"]],
        ),
        (
            "CREATE DOMAIN dom_num AS numeric",
            "dom_num[]",
            """
            SELECT format_type(atttypid, atttypmod) FROM pg_attribute
            WHERE attrelid = 'test_dom_nested'::regclass AND attname = 'val'
            """,
            [["double precision[]"]],
        ),
    ],
    ids=["domain_in_composite", "domain_array"],
)
def test_domain_over_numeric_in_nested_type(
    s3, pg_conn, extension, with_default_location, setup_sql, col_type, query, expected
):
    """A domain over numeric follows its container: rewritten in an array, kept in a composite."""
    run_command(setup_sql, pg_conn)
    run_command(
        f"CREATE TABLE test_dom_nested (id int, val {col_type}) USING iceberg",
        pg_conn,
    )

    result = run_query(query, pg_conn)
    assert result == expected

    pg_conn.rollback()


@pytest.mark.parametrize(
    "domain_def, insert_val, expected",
    [
        ("CREATE DOMAIN dom_x AS numeric", "1e30", Decimal("1E+30")),
        ("CREATE DOMAIN dom_x AS numeric(50,10)", "1e30", Decimal("1E+30")),
        # 38 digits, more than a double keeps: only an exact decimal exchange
        # type returns this unchanged
        (
            "CREATE DOMAIN dom_x AS numeric(38,2)",
            "123456789012345678901234567890123456.78",
            Decimal("123456789012345678901234567890123456.78"),
        ),
    ],
    ids=["unbounded", "precision_above_38", "bounded"],
)
def test_domain_in_composite_roundtrips(
    s3, pg_conn, extension, with_default_location, domain_def, insert_val, expected
):
    """The exchange type is picked from the domain's base type and modifier, so a
    domain over a numeric no decimal can hold is exchanged as a double, while a
    bounded one still gets its exact decimal."""
    run_command(
        f"""
        {domain_def};
        CREATE TYPE dom_comp AS (label text, amount dom_x);
        CREATE TABLE ice_dom_comp (id int, v dom_comp) USING iceberg;
        INSERT INTO ice_dom_comp VALUES (1, row('a', {insert_val}::dom_x)::dom_comp);
    """,
        pg_conn,
    )

    result = run_query("SELECT (v).amount FROM ice_dom_comp", pg_conn)
    assert result[0][0] == expected

    pg_conn.rollback()


def test_array_of_composite_with_domain_roundtrips(
    s3, pg_conn, extension, with_default_location
):
    """An array element is visited by the same walk, so a domain nested two
    containers deep is stored and read as a double."""
    run_command(
        """
        CREATE DOMAIN dom_y AS numeric;
        CREATE TYPE dom_elem AS (label text, amount dom_y);
        CREATE TABLE ice_dom_arr (id int, v dom_elem[]) USING iceberg;
        INSERT INTO ice_dom_arr VALUES
            (1, ARRAY[row('a', 1e30::dom_y)::dom_elem,
                      row('b', 'NaN'::dom_y)::dom_elem]);
    """,
        pg_conn,
    )

    result = run_query("SELECT (v[1]).amount, (v[2]).amount FROM ice_dom_arr", pg_conn)
    assert result[0][0] == Decimal("1E+30")
    assert result[0][1].is_nan()

    pg_conn.rollback()
