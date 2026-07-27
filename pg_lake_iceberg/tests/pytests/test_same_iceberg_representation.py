import pytest
from utils_pytest import *


@pytest.fixture(scope="module")
def same_iceberg_representation_fn(superuser_conn, iceberg_extension):
    """Register a SQL wrapper over the exported SameIcebergRepresentation C
    function so the tests can exercise it with plain type strings."""
    run_command(
        """
        CREATE OR REPLACE FUNCTION lake_iceberg.same_iceberg_representation(
            old_type text, new_type text)
        RETURNS bool
        LANGUAGE C STRICT
        AS 'pg_lake_iceberg', $$pg_lake_same_iceberg_representation$$;
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        "DROP FUNCTION IF EXISTS lake_iceberg.same_iceberg_representation(text, text);",
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
