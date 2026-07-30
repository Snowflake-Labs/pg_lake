"""
Tests for the Iceberg stored-representation primitives (iceberg_representation.h).

Two layers:

1. ``test_storage_type_*`` pin the *actual* Iceberg type the create path stores
   for a Postgres type, e.g. ``numeric(50,2)[]`` -> ``list<double>``.  Pairwise
   same/different assertions alone cannot catch a derivation that is wrong the
   same way on both sides -- the failure mode that made a nested unsupported
   numeric compare equal to ``text[]`` -- so the exact shape is asserted.

2. ``test_*_representation*`` pin type pairs, composing the primitives the way a
   caller with no persisted field to compare against would.  A caller that has
   the persisted field (from ``lake_table.field_id_mappings``) should derive only
   the new type and compare against that instead; see iceberg_representation.h.

The extraction that lets a comparison run the same code that produced the
persisted field is covered by the existing create-path suites
(``pg_lake_table/tests/pytests/test_compatibility_mode.py``,
``test_iceberg_uuid_compat.py``), which assert the resulting Iceberg schema
end-to-end.
"""

import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def iceberg_representation_fns(superuser_conn, iceberg_extension):
    """Register SQL wrappers over the exported representation primitives."""
    run_command(
        """
        CREATE OR REPLACE FUNCTION lake_iceberg.iceberg_storage_type(
            type text, compatibility_mode text DEFAULT NULL)
        RETURNS text
        LANGUAGE C
        AS 'pg_lake_iceberg', $$pg_lake_iceberg_storage_type$$;

        CREATE OR REPLACE FUNCTION lake_iceberg.same_iceberg_representation(
            old_type text, new_type text, compatibility_mode text DEFAULT NULL)
        RETURNS bool
        LANGUAGE C
        AS 'pg_lake_iceberg', $$pg_lake_same_iceberg_representation$$;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        "DROP FUNCTION IF EXISTS lake_iceberg.iceberg_storage_type(text, text);"
        "DROP FUNCTION IF EXISTS lake_iceberg.same_iceberg_representation(text, text, text);",
        superuser_conn,
    )
    superuser_conn.commit()


def _storage_type(
    conn, pg_type, unsupported_numeric_as_double=None, compatibility_mode=None
):
    """The Iceberg type the create path stores for pg_type.

    unsupported_numeric_as_double is pinned with SET rather than passed in: the
    primitives read the live GUC, exactly as the create path does.  The trailing
    rollback reverts the SET along with the query.
    """
    if unsupported_numeric_as_double is not None:
        run_command(
            "SET pg_lake_iceberg.unsupported_numeric_as_double TO "
            f"{str(unsupported_numeric_as_double).lower()}",
            conn,
        )

    mode = "NULL" if compatibility_mode is None else f"'{compatibility_mode}'"
    got = run_query(
        f"SELECT lake_iceberg.iceberg_storage_type('{pg_type}', {mode})", conn
    )[0][0]
    conn.rollback()
    return got


def _same(
    conn,
    old_type,
    new_type,
    unsupported_numeric_as_double=None,
    compatibility_mode=None,
):
    """Whether two Postgres types are stored with the same Iceberg representation."""
    if unsupported_numeric_as_double is not None:
        run_command(
            "SET pg_lake_iceberg.unsupported_numeric_as_double TO "
            f"{str(unsupported_numeric_as_double).lower()}",
            conn,
        )

    mode = "NULL" if compatibility_mode is None else f"'{compatibility_mode}'"
    got = run_query(
        "SELECT lake_iceberg.same_iceberg_representation"
        f"('{old_type}', '{new_type}', {mode})",
        conn,
    )[0][0]
    conn.rollback()
    return got


# ---------------------------------------------------------------------------
# Layer 1: the exact stored representation
# ---------------------------------------------------------------------------

# (postgres type, expected iceberg storage type) with the default GUC state
# (unsupported_numeric_as_double on) and no compatibility mode.
STORAGE_TYPES = [
    # text family: Iceberg models neither length nor which member
    ("varchar(50)", "string"),
    ("varchar(100)", "string"),
    ("char(10)", "string"),
    ("text", "string"),
    ("json", "string"),
    ("jsonb", "string"),
    # integers and floats
    ("smallint", "int"),
    ("integer", "int"),
    ("bigint", "long"),
    ("real", "float"),
    ("double precision", "double"),
    ("boolean", "boolean"),
    ("bytea", "binary"),
    ("uuid", "uuid"),
    # date/time: time and timetz collapse, timestamp and timestamptz do not
    ("date", "date"),
    ("time", "time"),
    ("timetz", "time"),
    ("timestamp", "timestamp"),
    ("timestamptz", "timestamptz"),
    # a numeric Iceberg can hold stays an exact decimal
    ("numeric(10,2)", "decimal(10,2)"),
    ("numeric(38,9)", "decimal(38,9)"),
    # one Iceberg cannot is stored as double (GUC on)
    ("numeric(50,2)", "double"),
    ("numeric(60,3)", "double"),
    ("numeric", "double"),
    # containers: the rules apply at every level
    ("text[]", "list<string>"),
    ("varchar(10)[]", "list<string>"),
    ("integer[]", "list<int>"),
    ("bigint[]", "list<long>"),
    ("uuid[]", "list<uuid>"),
    ("numeric(10,2)[]", "list<decimal(10,2)>"),
    # the case a top-level-only conversion got wrong: not list<string>
    ("numeric(50,2)[]", "list<double>"),
    ("numeric[]", "list<double>"),
    ("text[][]", "list<string>"),
]


@pytest.mark.parametrize("pg_type,expected", STORAGE_TYPES)
def test_storage_type(pg_type, expected, superuser_conn, iceberg_representation_fns):
    got = _storage_type(superuser_conn, pg_type)
    assert got == expected, f"{pg_type}: expected {expected}, got {got}"


def test_storage_type_unsupported_numeric_guc_off(
    superuser_conn, iceberg_representation_fns
):
    """With unsupported_numeric_as_double off there is no double rewrite, so an
    oversized numeric falls through to the `string` default -- a shape CREATE
    never actually stores, because it rejects such a column instead.  Pinned here
    because it is precisely why a caller must not read a `string` match as "same
    representation" (see test_unsupported_numeric_not_representable_when_disabled).
    """
    assert _storage_type(superuser_conn, "numeric(50,2)", False) == "string"
    assert _storage_type(superuser_conn, "numeric(50,2)[]", False) == "list<string>"
    # a bounded numeric is a genuine Iceberg decimal either way
    assert _storage_type(superuser_conn, "numeric(10,2)", False) == "decimal(10,2)"


def test_storage_type_compatibility_mode_is_depth_dependent(
    superuser_conn, iceberg_representation_fns
):
    """Under compatibility_mode='snowflake' a uuid nested in a container is stored
    as `string`, while a top-level uuid stays native.  Pinning both depths is what
    makes the uuid vs text comparisons below unambiguous.
    """
    assert (
        _storage_type(superuser_conn, "uuid", compatibility_mode="snowflake") == "uuid"
    )
    assert (
        _storage_type(superuser_conn, "uuid[]", compatibility_mode="snowflake")
        == "list<string>"
    )
    # auto applies no mapping at any depth
    assert (
        _storage_type(superuser_conn, "uuid[]", compatibility_mode="auto")
        == "list<uuid>"
    )


def test_storage_type_composite_and_interval(
    superuser_conn, iceberg_representation_fns
):
    """Composites derive to a struct and the rules apply to their fields; interval
    has no native Iceberg type and is modelled as a struct of longs.
    """
    run_command("DROP TYPE IF EXISTS rep_num, rep_dbl, rep_txt;", superuser_conn)
    run_command("CREATE TYPE rep_num AS (a numeric(50,2), b int);", superuser_conn)
    run_command("CREATE TYPE rep_dbl AS (a double precision, b int);", superuser_conn)
    run_command("CREATE TYPE rep_txt AS (a text, b int);", superuser_conn)
    superuser_conn.commit()

    try:
        assert _storage_type(superuser_conn, "rep_num") == "struct<a:double,b:int>"
        assert _storage_type(superuser_conn, "rep_dbl") == "struct<a:double,b:int>"
        assert _storage_type(superuser_conn, "rep_txt") == "struct<a:string,b:int>"
        assert (
            _storage_type(superuser_conn, "interval")
            == "struct<months:long,days:long,microseconds:long>"
        )
    finally:
        run_command("DROP TYPE IF EXISTS rep_num, rep_dbl, rep_txt;", superuser_conn)
        superuser_conn.commit()


# ---------------------------------------------------------------------------
# Layer 2: type-pair comparisons
# ---------------------------------------------------------------------------

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
    old_type, new_type, superuser_conn, iceberg_representation_fns
):
    assert (
        _same(superuser_conn, old_type, new_type) is True
    ), f"{old_type} -> {new_type}: expected same representation"


@pytest.mark.parametrize("old_type,new_type", DIFFERENT_REPRESENTATION)
def test_same_iceberg_representation_false(
    old_type, new_type, superuser_conn, iceberg_representation_fns
):
    assert (
        _same(superuser_conn, old_type, new_type) is False
    ), f"{old_type} -> {new_type}: expected different representation"


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
    # The false match a top-level-only conversion produces: a nested unsupported
    # numeric is stored as list<double>, not list<string>, so it differs from text[].
    ("numeric(50,2)[]", "text[]"),
    ("numeric(50,2)[]", "numeric(10,2)[]"),  # list<double> vs list<decimal(10,2)>
    ("integer[]", "bigint[]"),  # list<int> vs list<long>
]


@pytest.mark.parametrize("old_type,new_type", NESTED_SAME)
def test_nested_same_representation(
    old_type, new_type, superuser_conn, iceberg_representation_fns
):
    assert (
        _same(superuser_conn, old_type, new_type) is True
    ), f"{old_type} -> {new_type}: expected same representation"


@pytest.mark.parametrize("old_type,new_type", NESTED_DIFFERENT)
def test_nested_different_representation(
    old_type, new_type, superuser_conn, iceberg_representation_fns
):
    assert (
        _same(superuser_conn, old_type, new_type) is False
    ), f"{old_type} -> {new_type}: expected different representation"


def test_unsupported_numeric_double_when_enabled(
    superuser_conn, iceberg_representation_fns
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
    superuser_conn, iceberg_representation_fns
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
    superuser_conn, iceberg_representation_fns
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
    superuser_conn, iceberg_representation_fns
):
    """With no compatibility mode (auto), a nested uuid stays native `uuid`, so a
    nested uuid and a nested text differ."""
    assert _same(superuser_conn, "uuid[]", "text[]") is False
    assert _same(superuser_conn, "uuid[]", "uuid[]") is True


def test_composite_unsupported_numeric_matches_float8_composite(
    superuser_conn, iceberg_representation_fns
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
