import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def same_iceberg_representation_fn(superuser_conn, iceberg_extension):
    """Register a SQL wrapper over the exported SameIcebergStoredRepresentation
    C function so the tests can exercise it with plain type strings."""
    run_command(
        """
        CREATE OR REPLACE FUNCTION lake_iceberg.same_iceberg_representation(
            old_type text, new_type text,
            unsupported_numeric_as_double bool DEFAULT NULL,
            compatibility_mode text DEFAULT NULL)
        RETURNS bool
        LANGUAGE C
        AS 'pg_lake_iceberg', $$pg_lake_same_iceberg_representation$$;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        "DROP FUNCTION IF EXISTS lake_iceberg.same_iceberg_representation(text, text, bool, text);",
        superuser_conn,
    )
    superuser_conn.commit()


# Type pairs whose Iceberg representation is identical (ignoring anything
# Iceberg does not model: text length, which text-family member, etc.).
SAME_REPRESENTATION = [
    ("varchar(50)", "varchar(100)"),  # both Iceberg `string`
    ("varchar(100)", "varchar(20)"),  # narrowing is still `string`
    ("varchar(50)", "text"),
    ("char(10)", "varchar(30)"),  # bpchar and varchar are both `string`
    ("char(10)", "text"),
    ("smallint", "integer"),  # both `int`
    ("time", "timetz"),  # both `time`
    ("json", "text"),  # json falls back to `string`, same as text
    ("jsonb", "text"),
    ("numeric", "numeric(50,2)"),  # both unsupported -> normalized to `double`
]

# Type pairs whose Iceberg representation differs.
DIFFERENT_REPRESENTATION = [
    ("integer", "bigint"),  # `int` vs `long`
    ("integer", "text"),  # `int` vs `string`
    ("numeric(10,2)", "text"),  # `decimal(10,2)` vs `string`
    ("numeric(10,2)", "numeric(12,4)"),  # `decimal(10,2)` vs `decimal(12,4)`
    ("timestamp", "timestamptz"),  # `timestamp` vs `timestamptz`
    ("real", "double precision"),  # `float` vs `double`
]


@pytest.mark.parametrize("old_type,new_type", SAME_REPRESENTATION)
def test_same_iceberg_representation_true(
    old_type, new_type, superuser_conn, same_iceberg_representation_fn
):
    got = run_query(
        f"SELECT lake_iceberg.same_iceberg_representation('{old_type}', '{new_type}')",
        superuser_conn,
    )[0][0]
    superuser_conn.rollback()
    assert got is True, f"{old_type} -> {new_type}: expected same representation"


@pytest.mark.parametrize("old_type,new_type", DIFFERENT_REPRESENTATION)
def test_same_iceberg_representation_false(
    old_type, new_type, superuser_conn, same_iceberg_representation_fn
):
    got = run_query(
        f"SELECT lake_iceberg.same_iceberg_representation('{old_type}', '{new_type}')",
        superuser_conn,
    )[0][0]
    superuser_conn.rollback()
    assert got is False, f"{old_type} -> {new_type}: expected different representation"


def _same(
    conn,
    old_type,
    new_type,
    unsupported_numeric_as_double=None,
    compatibility_mode=None,
):
    """Call the wrapper, optionally pinning the create-path settings the
    comparison uses (unsupported_numeric_as_double and compatibility_mode)."""
    flag = (
        "NULL"
        if unsupported_numeric_as_double is None
        else ("true" if unsupported_numeric_as_double else "false")
    )
    mode = "NULL" if compatibility_mode is None else f"'{compatibility_mode}'"
    sql = (
        "SELECT lake_iceberg.same_iceberg_representation"
        f"('{old_type}', '{new_type}', {flag}, {mode})"
    )
    got = run_query(sql, conn)[0][0]
    conn.rollback()
    return got


# Nested type pairs, evaluated with the default GUC
# (unsupported_numeric_as_double on).  These exercise the recursive derivation:
# an unsupported numeric is stored as double at every level, so it must not be
# conflated with the `string` fallback that a genuine text type maps to.
NESTED_SAME = [
    ("numeric(50,2)[]", "numeric(60,3)[]"),  # both list<double>
    ("numeric[]", "numeric(50,2)[]"),  # both unsupported -> list<double>
    ("varchar(10)[]", "text[]"),  # both list<string>
]

NESTED_DIFFERENT = [
    # The false match this change fixes: a nested unsupported numeric is stored
    # as list<double>, not list<string>, so it differs from text[].
    ("numeric(50,2)[]", "text[]"),
    ("numeric(50,2)[]", "numeric(10,2)[]"),  # list<double> vs list<decimal(10,2)>
    ("integer[]", "bigint[]"),  # list<int> vs list<long>
]


@pytest.mark.parametrize("old_type,new_type", NESTED_SAME)
def test_nested_same_representation(
    old_type, new_type, superuser_conn, same_iceberg_representation_fn
):
    assert (
        _same(superuser_conn, old_type, new_type) is True
    ), f"{old_type} -> {new_type}: expected same representation"


@pytest.mark.parametrize("old_type,new_type", NESTED_DIFFERENT)
def test_nested_different_representation(
    old_type, new_type, superuser_conn, same_iceberg_representation_fn
):
    assert (
        _same(superuser_conn, old_type, new_type) is False
    ), f"{old_type} -> {new_type}: expected different representation"


def test_unsupported_numeric_double_when_enabled(
    superuser_conn, same_iceberg_representation_fn
):
    """With unsupported_numeric_as_double on, an unsupported numeric is stored
    as double at every nesting level, so it matches another unsupported numeric
    -- and an actual double, the type the create path converts it to -- but not
    text."""
    assert _same(superuser_conn, "numeric(50,2)[]", "text[]", True) is False
    assert _same(superuser_conn, "numeric(50,2)[]", "numeric(60,3)[]", True) is True
    assert _same(superuser_conn, "numeric(50,2)", "numeric(60,3)", True) is True
    # the converted leaf really is `double`, so it matches a genuine float8
    assert _same(superuser_conn, "numeric(50,2)[]", "double precision[]", True) is True
    assert _same(superuser_conn, "numeric[]", "double precision[]", True) is True


def test_unsupported_numeric_not_representable_when_disabled(
    superuser_conn, same_iceberg_representation_fn
):
    """With unsupported_numeric_as_double off, CREATE errors on an unsupported
    numeric at any level, so it has no stored representation: the comparison is
    conservatively false even for identical types.  Bounded numerics, which are
    genuine Iceberg decimals, are unaffected."""
    assert _same(superuser_conn, "numeric(50,2)", "numeric(50,2)", False) is False
    assert _same(superuser_conn, "numeric(50,2)[]", "numeric(50,2)[]", False) is False
    assert _same(superuser_conn, "numeric", "numeric", False) is False
    # bounded numerics remain comparable when the GUC is off
    assert _same(superuser_conn, "numeric(10,2)", "numeric(10,2)", False) is True
    assert _same(superuser_conn, "numeric(10,2)[]", "numeric(10,2)[]", False) is True


def test_compatibility_uuid_is_depth_dependent(
    superuser_conn, same_iceberg_representation_fn
):
    """Under the snowflake compatibility mode a uuid nested inside a container is
    stored as string, while a top-level uuid stays native.  So the same uuid vs
    text comparison flips with nesting -- the case that requires the comparison
    to be position-aware."""
    # nested: uuid[] and text[] are both stored as list<string> -> same
    assert (
        _same(superuser_conn, "uuid[]", "text[]", compatibility_mode="snowflake")
        is True
    )
    # top-level: uuid stays native `uuid`, text is `string` -> different
    assert (
        _same(superuser_conn, "uuid", "text", compatibility_mode="snowflake") is False
    )


def test_compatibility_auto_keeps_uuid_native(
    superuser_conn, same_iceberg_representation_fn
):
    """With no compatibility mode (auto), a nested uuid stays native `uuid`, so a
    nested uuid and a nested text differ."""
    assert _same(superuser_conn, "uuid[]", "text[]") is False
    assert _same(superuser_conn, "uuid[]", "uuid[]") is True


def test_composite_unsupported_numeric_matches_float8_composite(
    superuser_conn, same_iceberg_representation_fn
):
    """The recursion also descends into composites: a composite whose field is an
    unsupported numeric is stored as the same struct<double, ...> the create path
    produces for a float8 field, so it matches a float8 composite but not a text
    one."""
    run_command("DROP TYPE IF EXISTS rep_num, rep_dbl, rep_txt;", superuser_conn)
    run_command("CREATE TYPE rep_num AS (a numeric(50,2), b int);", superuser_conn)
    run_command("CREATE TYPE rep_dbl AS (a double precision, b int);", superuser_conn)
    run_command("CREATE TYPE rep_txt AS (a text, b int);", superuser_conn)
    superuser_conn.commit()
    try:
        # numeric field -> double, matches a genuine float8 composite
        assert _same(superuser_conn, "rep_num", "rep_dbl", True) is True
        # but not a composite whose field is text (`string`)
        assert _same(superuser_conn, "rep_num", "rep_txt", True) is False
    finally:
        run_command("DROP TYPE IF EXISTS rep_num, rep_dbl, rep_txt;", superuser_conn)
        superuser_conn.commit()
