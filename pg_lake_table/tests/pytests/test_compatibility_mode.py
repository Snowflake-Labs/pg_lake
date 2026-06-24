"""
Tests for the ``compatibility_mode`` table option (option layer only).

This is the option/DDL layer: ``compatibility_mode`` is recognized and
validated ('auto' or 'snowflake'), it cannot be changed after creation, and a
type a restrictive mode cannot represent (a pg_map under 'snowflake') is
rejected up front at CREATE / ALTER ADD COLUMN.

On its own this layer performs NO storage shaping -- it is a pure storage
no-op: choosing 'snowflake' is accepted, but a nested uuid is still stored as
Iceberg ``uuid`` and the PostgreSQL column type is unchanged. The actual
surface->storage mapping (nested uuid stored as ``string``, persisted in
``lake_table.field_id_mappings``) is layered on top by the storage-mapping
change and is covered by its own test suite. The no-op tests below lock in that
this layer does not, by itself, change physical storage.

psycopg2 returns uuid values as plain strings here (no UUID adapter is
registered), so uuid values are normalized via ``_u`` before comparison.
"""

from utils_pytest import *
import json
import pytest

U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"


def _u(value):
    """Normalize a uuid value (psycopg may return str or uuid.UUID)."""
    return None if value is None else str(value).lower()


def _ulist(value):
    """Normalize a uuid array, preserving NULL for the whole array."""
    if value is None:
        return None
    if isinstance(value, str):
        inner = value.strip("{}")
        return [] if inner == "" else [_u(v) for v in inner.split(",")]
    return [_u(v) for v in value]


def _metadata_fields(s3, pg_conn, schema_name, table_name):
    """Return the top-level Iceberg schema fields list from metadata.json."""
    results = run_query(
        f"SELECT metadata_location FROM lake_iceberg.tables "
        f"WHERE table_name = '{table_name}' AND table_namespace = '{schema_name}'",
        pg_conn,
    )
    assert len(results) == 1, f"expected one iceberg table {schema_name}.{table_name}"
    data = read_s3_operations(s3, results[0][0])
    return json.loads(data)["schemas"][0]["fields"]


def _collect_leaf_types(field_type):
    """Recursively collect every scalar (leaf) Iceberg type reachable from a node."""
    if isinstance(field_type, str):
        return [field_type]

    kind = field_type.get("type")
    if kind == "struct":
        leaves = []
        for f in field_type["fields"]:
            leaves.extend(_collect_leaf_types(f["type"]))
        return leaves
    if kind == "list":
        return _collect_leaf_types(field_type["element"])
    if kind == "map":
        return _collect_leaf_types(field_type["key"]) + _collect_leaf_types(
            field_type["value"]
        )
    return []


def _field_by_name(fields, name):
    return next(f for f in fields if f["name"] == name)


def _persisted_compat_option(pg_conn, qualified_table):
    """Return the stored compatibility_mode foreign-table option, or None."""
    rows = run_query(
        f"""
        SELECT option_value
        FROM pg_options_to_table(
            (SELECT ftoptions FROM pg_foreign_table
             WHERE ftrelid = '{qualified_table}'::regclass)
        )
        WHERE option_name = 'compatibility_mode'
        """,
        pg_conn,
    )
    return rows[0][0] if rows else None


# ---------------------------------------------------------------------------
# Option validation
# ---------------------------------------------------------------------------


def test_compatibility_mode_option_validation(pg_conn, s3, with_default_location):
    """'auto' and 'snowflake' are accepted; anything else is rejected."""
    run_command("CREATE SCHEMA test_compat_opt;", pg_conn)
    pg_conn.commit()

    run_command(
        "CREATE TABLE test_compat_opt.a (id int) USING iceberg "
        "WITH (compatibility_mode = 'auto');",
        pg_conn,
    )
    run_command(
        "CREATE TABLE test_compat_opt.b (id int) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command(
            "CREATE TABLE test_compat_opt.bad (id int) USING iceberg "
            "WITH (compatibility_mode = 'redshift');",
            pg_conn,
        )
    pg_conn.rollback()
    assert "compatibility_mode" in str(exc.value).lower()

    run_command("DROP SCHEMA test_compat_opt CASCADE;", pg_conn)
    pg_conn.commit()


def test_compatibility_mode_is_immutable(pg_conn, s3, with_default_location):
    """compatibility_mode cannot be changed after the table is created."""
    run_command("CREATE SCHEMA test_compat_immut;", pg_conn)
    run_command(
        "CREATE TABLE test_compat_immut.t (id int) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command(
            "ALTER TABLE test_compat_immut.t "
            "OPTIONS (SET compatibility_mode 'auto');",
            pg_conn,
        )
    pg_conn.rollback()
    assert "compatibility_mode" in str(exc.value).lower()

    run_command(
        "CREATE TABLE test_compat_immut.t2 (id int) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()
    with pytest.raises(Exception):
        run_command(
            "ALTER TABLE test_compat_immut.t2 "
            "OPTIONS (ADD compatibility_mode 'snowflake');",
            pg_conn,
        )
    pg_conn.rollback()

    run_command("DROP SCHEMA test_compat_immut CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# default_compatibility_mode GUC
# ---------------------------------------------------------------------------


def test_default_compatibility_mode_guc_metadata(pg_conn, s3, with_default_location):
    """The GUC is a regular user-settable enum over exactly {auto, snowflake}."""
    rows = run_query(
        "SELECT context, vartype, enumvals, boot_val "
        "FROM pg_settings "
        "WHERE name = 'pg_lake_iceberg.default_compatibility_mode'",
        pg_conn,
    )
    assert len(rows) == 1, "GUC not registered"
    context, vartype, enumvals, boot_val = rows[0]
    assert context == "user", context
    assert vartype == "enum", vartype
    assert set(enumvals) == {"auto", "snowflake"}, enumvals
    assert boot_val == "auto", boot_val


def test_default_compatibility_mode_guc_value_validation(
    pg_conn, s3, with_default_location
):
    """Only auto/snowflake accepted; matching is case-insensitive; else rejected."""
    # Case-insensitive: Postgres normalizes enum GUC values to the canonical name.
    run_command(
        "SET pg_lake_iceberg.default_compatibility_mode = 'SNOWFLAKE';", pg_conn
    )
    assert (
        run_query("SHOW pg_lake_iceberg.default_compatibility_mode", pg_conn)[0][0]
        == "snowflake"
    )
    run_command("SET pg_lake_iceberg.default_compatibility_mode = 'Auto';", pg_conn)
    assert (
        run_query("SHOW pg_lake_iceberg.default_compatibility_mode", pg_conn)[0][0]
        == "auto"
    )

    with pytest.raises(Exception):
        run_command(
            "SET pg_lake_iceberg.default_compatibility_mode = 'redshift';", pg_conn
        )
    pg_conn.rollback()
    run_command("RESET pg_lake_iceberg.default_compatibility_mode;", pg_conn)
    pg_conn.commit()


def test_default_compatibility_mode_guc_seeds_new_tables(
    pg_conn, s3, with_default_location
):
    """
    With the GUC set to 'snowflake', a new table that does not specify
    compatibility_mode adopts (and persists) it; an explicit option still wins;
    and the GUC default 'auto' leaves the option absent.
    """
    map_typename = create_map_type("text", "int")
    run_command("CREATE SCHEMA test_compat_guc;", pg_conn)
    pg_conn.commit()

    # Default (auto): option not persisted, map column allowed.
    run_command("CREATE TABLE test_compat_guc.auto_t (id int) USING iceberg;", pg_conn)
    pg_conn.commit()
    assert _persisted_compat_option(pg_conn, "test_compat_guc.auto_t") is None

    # GUC = snowflake: new table persists compatibility_mode=snowflake...
    run_command(
        "SET pg_lake_iceberg.default_compatibility_mode = 'snowflake';", pg_conn
    )
    run_command("CREATE TABLE test_compat_guc.sf_t (id int) USING iceberg;", pg_conn)
    pg_conn.commit()
    assert (
        _persisted_compat_option(pg_conn, "test_compat_guc.sf_t") == "snowflake"
    ), "GUC default not seeded into new table"

    # ...so the snowflake DDL guard applies even without an explicit option.
    with pytest.raises(Exception) as exc:
        run_command(
            f"CREATE TABLE test_compat_guc.sf_map (id int, m {map_typename}) "
            f"USING iceberg;",
            pg_conn,
        )
    pg_conn.rollback()
    assert "snowflake" in str(exc.value).lower()

    # An explicit option overrides the GUC.
    run_command(
        "CREATE TABLE test_compat_guc.explicit_auto (id int) USING iceberg "
        "WITH (compatibility_mode = 'auto');",
        pg_conn,
    )
    pg_conn.commit()
    assert _persisted_compat_option(pg_conn, "test_compat_guc.explicit_auto") == "auto"

    run_command("RESET pg_lake_iceberg.default_compatibility_mode;", pg_conn)
    run_command("DROP SCHEMA test_compat_guc CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Map rejection under snowflake compat (DDL guard)
# ---------------------------------------------------------------------------


def test_map_rejected_under_snowflake_compat_on_create(
    pg_conn, s3, with_default_location
):
    """Map columns are rejected at CREATE under compatibility_mode='snowflake'."""
    map_typename = create_map_type("text", "int")
    run_command("CREATE SCHEMA test_compat_map;", pg_conn)
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command(
            f"CREATE TABLE test_compat_map.t (id int, m {map_typename}) USING iceberg "
            f"WITH (compatibility_mode = 'snowflake');",
            pg_conn,
        )
    pg_conn.rollback()
    assert "snowflake" in str(exc.value).lower()

    # The same map is fine without snowflake compat.
    run_command(
        f"CREATE TABLE test_compat_map.ok (id int, m {map_typename}) USING iceberg;",
        pg_conn,
    )
    pg_conn.commit()

    run_command("DROP SCHEMA test_compat_map CASCADE;", pg_conn)
    pg_conn.commit()


def test_map_rejected_under_snowflake_compat_on_add_column(
    pg_conn, s3, with_default_location
):
    """ALTER ... ADD COLUMN of a map is rejected under 'snowflake'."""
    map_typename = create_map_type("text", "int")
    run_command(
        "CREATE SCHEMA test_compat_map_add;"
        "CREATE TABLE test_compat_map_add.t (id int) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.commit()

    with pytest.raises(Exception) as exc:
        run_command(
            f"ALTER TABLE test_compat_map_add.t ADD COLUMN m {map_typename};",
            pg_conn,
        )
    pg_conn.rollback()
    assert "snowflake" in str(exc.value).lower()

    run_command("DROP SCHEMA test_compat_map_add CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Storage no-op: this layer alone does NOT shape storage
# ---------------------------------------------------------------------------


def test_top_level_uuid_stays_native_under_snowflake(
    pg_conn, s3, with_default_location
):
    """A top-level uuid column is stored as Iceberg uuid and round-trips."""
    run_command("CREATE SCHEMA test_compat_top;", pg_conn)
    run_command(
        "CREATE TABLE test_compat_top.t (id int, u uuid) USING iceberg "
        "WITH (compatibility_mode = 'snowflake');",
        pg_conn,
    )
    pg_conn.commit()

    fields = _metadata_fields(s3, pg_conn, "test_compat_top", "t")
    assert _field_by_name(fields, "u")["type"] == "uuid"

    run_command(
        f"INSERT INTO test_compat_top.t VALUES (1, '{U1}'), (2, NULL);", pg_conn
    )
    pg_conn.commit()
    result = run_query("SELECT id, u FROM test_compat_top.t ORDER BY id", pg_conn)
    assert [[r[0], _u(r[1])] for r in result] == [[1, U1], [2, None]]

    run_command("DROP SCHEMA test_compat_top CASCADE;", pg_conn)
    pg_conn.commit()


def test_nested_uuid_is_storage_noop_under_snowflake(
    pg_conn, s3, with_default_location
):
    """
    Option layer only: a nested uuid under 'snowflake' is STILL stored as
    Iceberg uuid (no surface->storage divergence yet) and the surface type is
    unchanged. The storage-mapping layer changes this to 'string'; this test
    pins down that the option alone is a storage no-op.
    """
    run_command(
        """
        CREATE SCHEMA test_compat_nested;
        SET search_path TO test_compat_nested;
        CREATE TYPE acct AS (name text, uid uuid);
        CREATE TABLE t (id int, a acct, us uuid[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    fields = _metadata_fields(s3, pg_conn, "test_compat_nested", "t")
    # The composite's uuid field (uid) stays Iceberg uuid; the text field
    # (name) is naturally Iceberg string, so we only assert the uuid leaf is
    # still uuid here (the storage-mapping layer would turn it into string).
    comp_leaves = _collect_leaf_types(_field_by_name(fields, "a")["type"])
    arr_leaves = _collect_leaf_types(_field_by_name(fields, "us")["type"])
    assert "uuid" in comp_leaves, comp_leaves
    # uuid[] is the clean discriminator: no-op keeps it list<uuid> (the
    # storage-mapping layer would make it list<string>).
    assert arr_leaves == ["uuid"], arr_leaves

    run_command(
        f"""
        INSERT INTO test_compat_nested.t VALUES
            (1, ROW('alice', '{U1}')::acct, ARRAY['{U1}','{U2}']::uuid[]),
            (2, ROW('bob', NULL)::acct, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()
    result = run_query(
        "SELECT id, (a).name, (a).uid, us FROM test_compat_nested.t ORDER BY id",
        pg_conn,
    )
    assert [[r[0], r[1], _u(r[2]), _ulist(r[3])] for r in result] == [
        [1, "alice", U1, [U1, U2]],
        [2, "bob", None, None],
    ]

    run_command("DROP SCHEMA test_compat_nested CASCADE;", pg_conn)
    pg_conn.commit()
