"""
Tests for compatibility_mode='snowflake' UUID surface/storage mapping.

Background
----------
Snowflake cannot store a uuid inside a structured/semi-structured type.
``compatibility_mode='snowflake'`` makes pg_lake keep the PostgreSQL column
type as ``uuid`` (the *surface* type) while physically storing any *nested*
uuid as an Iceberg ``string`` (the *storage* type).  A top-level uuid column
is unaffected (Iceberg/Snowflake both represent it natively).

The surface->storage divergence is persisted per leaf in
``lake_table.field_id_mappings`` (``field_storage_pg_type``), so:

  * the external Iceberg metadata.json shows ``string`` for nested uuids,
  * the *physical Parquet* leaf is written as ``VARCHAR`` (verified by reading
    the data file directly, because the Iceberg metadata claiming ``string`` is
    not proof of what was actually written to disk),
  * the PostgreSQL-visible column type stays ``uuid``,
  * the write path casts ``uuid -> VARCHAR`` before writing,
  * the read path casts the stored string back to ``uuid``.

psycopg2 returns uuid values as plain strings here (no UUID adapter is
registered), so uuid round-trips are compared via ``_u`` after normalizing
to a lowercase string.
"""

from utils_pytest import *
import json
import pytest


# A handful of stable uuid literals used across the tests.
U1 = "11111111-1111-1111-1111-111111111111"
U2 = "22222222-2222-2222-2222-222222222222"
U3 = "33333333-3333-3333-3333-333333333333"

# Distinct uuid literals for the deeply nested "pyramid" fixture below.  Named
# after their position so the round-trip assertions read unambiguously.
NODE1 = "a0000000-0000-0000-0000-00000000000a"
NODE2 = "b0000000-0000-0000-0000-00000000000b"
PRIM1 = "c1111111-1111-1111-1111-11111111111c"
SEC1 = "d2222222-2222-2222-2222-22222222222d"  # stored in a DOMAIN-over-uuid leaf
PRIM2 = "e3333333-3333-3333-3333-33333333333e"
DEV1 = "f4444444-4444-4444-4444-44444444444f"
BAK1 = "05555555-5555-5555-5555-555555555550"
DEV2 = "16666666-6666-6666-6666-666666666661"
TAG1 = "27777777-7777-7777-7777-777777777772"
TAG2 = "38888888-8888-8888-8888-888888888883"


def _u(value):
    """Normalize a uuid value (psycopg may return str or uuid.UUID)."""
    return None if value is None else str(value).lower()


def _ulist(value):
    """
    Normalize a uuid array, preserving NULL for the whole array.

    psycopg2 returns uuid[] as the raw PostgreSQL array text ``{u1,u2}`` here
    (uuid elements are unquoted and have no array-special characters), so parse
    that form as well as an already-decoded python list.
    """
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


def _catalog_storage_divergences(superuser_conn, qualified_table):
    """
    Return the per-leaf surface->storage overrides pg_lake recorded in
    ``lake_table.field_id_mappings`` for a table, as a sorted list of
    ``(field_pg_type, field_storage_pg_type)`` tuples for every leaf whose
    storage type diverges (``field_storage_pg_type IS NOT NULL``).

    The catalog is the source of truth that both the write and read paths
    consult to decide which leaves get cast.  The storage-mapping tests already
    assert the Iceberg ``metadata.json`` and the physical Parquet; asserting the
    catalog too proves all three agree on what diverges.
    """
    rows = run_query(
        "SELECT field_pg_type::text, field_storage_pg_type::text "
        "FROM lake_table.field_id_mappings "
        f"WHERE table_name = '{qualified_table}'::regclass "
        "  AND field_storage_pg_type IS NOT NULL "
        "ORDER BY 1, 2",
        superuser_conn,
    )
    return sorted((r[0], r[1]) for r in rows)


def _assert_uuid_to_text_divergences(superuser_conn, qualified_table, count):
    """
    Assert the table records exactly ``count`` storage overrides in the catalog
    and that every one is the ``uuid -> text`` mapping, the only divergence the
    'snowflake' compatibility mode produces today.  ``count=None`` only requires
    that at least one override exists and that all of them are ``uuid -> text``
    (handy for deep type pyramids where the exact leaf count is not the point).
    """
    divergences = _catalog_storage_divergences(superuser_conn, qualified_table)
    if count is None:
        assert (
            divergences
        ), f"expected at least one storage override for {qualified_table}"
        assert set(divergences) == {("uuid", "text")}, divergences
    else:
        assert divergences == [("uuid", "text")] * count, divergences


def _collect_leaf_types(field_type):
    """
    Recursively collect every scalar (leaf) Iceberg type string reachable
    from an Iceberg field ``type`` node (which may be a plain string for a
    scalar, or a dict for struct/list/map).
    """
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


def _parquet_column_types(superuser_conn, pgduck_conn, qualified_table):
    """
    Return ``{column_name: physical_parquet_type}`` for the table's actual data
    file(s), as resolved by DuckDB reading the bytes on disk.

    This deliberately inspects the Parquet files themselves rather than the
    Iceberg ``metadata.json``.  The metadata.json advertising ``string`` is not
    proof that the value was physically written as a Parquet string -- the
    write path could disagree with the declared schema.  Reading the file is
    the only check that catches that divergence, so every storage-mapping test
    asserts on this, not on the Iceberg type alone.

    DuckDB resolves a Parquet UUID logical leaf to ``UUID`` and a Parquet string
    leaf to ``VARCHAR``, including inside ``STRUCT(...)`` / ``...[]`` types, so a
    leaked native uuid shows up as the substring ``UUID`` and a uuid stored as
    string shows up as ``VARCHAR``.
    """
    files = run_query(
        f"SELECT path FROM lake_table.files "
        f"WHERE table_name = '{qualified_table}'::regclass",
        superuser_conn,
    )
    assert files, f"expected at least one data file for {qualified_table}"

    per_file = []
    for (path,) in files:
        described = run_query(
            f"DESCRIBE SELECT * FROM read_parquet('{path}')", pgduck_conn
        )
        per_file.append({row[0]: row[1] for row in described})

    # All data files for a table must share the same physical schema.
    for types in per_file[1:]:
        assert types == per_file[0], (per_file[0], types)
    return per_file[0]


def _parquet_leaf_fields(superuser_conn, pgduck_conn, qualified_table):
    """
    Return ``{field_id: (name, parquet_type, converted_type)}`` for every *leaf*
    column physically present in the table's data file(s), read straight from
    the Parquet footer.

    Keyed by the Parquet ``field_id`` that DuckDB stamped into the file.  This is
    the crux of the struct_pack correctness check: even though struct_pack
    rebuilds the struct into brand-new vectors, the write path reattaches Iceberg
    field ids to the new fields *by name*, so the physical leaf must still carry
    the exact Iceberg id pg_lake assigned -- otherwise readers map it wrongly.
    """
    files = run_query(
        f"SELECT path FROM lake_table.files "
        f"WHERE table_name = '{qualified_table}'::regclass",
        superuser_conn,
    )
    assert files, f"expected at least one data file for {qualified_table}"

    per_file = []
    for (path,) in files:
        rows = run_query(
            "SELECT field_id, name, type, converted_type "
            f"FROM parquet_schema('{path}') WHERE num_children IS NULL",
            pgduck_conn,
        )
        per_file.append(
            {int(r[0]): (r[1], r[2], r[3]) for r in rows if r[0] is not None}
        )

    for leaves in per_file[1:]:
        assert leaves == per_file[0], (per_file[0], leaves)
    return per_file[0]


def _metadata_leaf_ids(fields):
    """
    Walk the Iceberg metadata schema ``fields`` and return ``{name: (id, type)}``
    for every named scalar leaf (descending through struct/list/map), where
    ``type`` is the Iceberg type string the metadata advertises for that leaf.
    """
    out = {}

    def walk(field_type, name):
        if isinstance(field_type, str):
            if name is not None:
                out[name] = (None, field_type)
            return
        kind = field_type.get("type")
        if kind == "struct":
            for f in field_type["fields"]:
                t = f["type"]
                if isinstance(t, str):
                    out[f["name"]] = (f["id"], t)
                else:
                    walk(t, f["name"])
        elif kind == "list":
            walk(field_type["element"], name)
        elif kind == "map":
            walk(field_type["key"], name)
            walk(field_type["value"], name)

    for f in fields:
        t = f["type"]
        if isinstance(t, str):
            out[f["name"]] = (f["id"], t)
        else:
            walk(t, f["name"])
    return out


def _create_deep_uuid_types(conn, schema):
    """
    Build a deliberately deep, mixed type pyramid in ``schema`` and leave that
    schema on the session search_path::

        node[]                              -- array of structs
          node    (node_id uuid,
                   owners  owner_info,      -- sub-struct #1 (uuids + domain leaf)
                   devices device_info,     -- sub-struct #2 (uuids + non-uuid struct)
                   tags    uuid[])          -- uuid array nested in a struct
          owner_info  (primary_owner uuid,
                       secondary_owner account_id,   -- DOMAIN over uuid
                       label text)                   -- non-uuid leaf
          device_info (device_id uuid,
                       backup_id uuid,
                       location geo)
          geo (lat double precision, lon double precision)  -- non-uuid leaves

    Under ``compatibility_mode='snowflake'`` every uuid leaf at any depth -- the
    plain uuids, the uuid[] element, and the domain-over-uuid -- must store as
    string, while ``geo``'s doubles and the text label must pass through
    unchanged.  Tables are created per test (they differ in compat mode).
    """
    run_command(
        f"""
        CREATE SCHEMA {schema};
        SET search_path TO {schema};
        CREATE DOMAIN account_id AS uuid;
        CREATE TYPE geo AS (lat double precision, lon double precision);
        CREATE TYPE owner_info AS (
            primary_owner   uuid,
            secondary_owner account_id,
            label           text
        );
        CREATE TYPE device_info AS (
            device_id uuid,
            backup_id uuid,
            location  geo
        );
        CREATE TYPE node AS (
            node_id uuid,
            owners  owner_info,
            devices device_info,
            tags    uuid[]
        );
        """,
        conn,
    )


# One fully-populated node, one sparsely-populated node (NULL domain leaf, NULL
# nested backup uuid, NULL tags array), exercised by every deep test.  Assumes
# the deep types are on the search_path.
_DEEP_TWO_NODES = f"""
    ARRAY[
        ROW(
            '{NODE1}',
            ROW('{PRIM1}', '{SEC1}', 'main')::owner_info,
            ROW('{DEV1}', '{BAK1}', ROW(1.5, 2.5)::geo)::device_info,
            ARRAY['{TAG1}', '{TAG2}']::uuid[]
        )::node,
        ROW(
            '{NODE2}',
            ROW('{PRIM2}', NULL, 'sec')::owner_info,
            ROW('{DEV2}', NULL, NULL)::device_info,
            NULL
        )::node
    ]::node[]
"""


# ---------------------------------------------------------------------------
# Storage mapping: nested uuid becomes string (top-level stays native)
#
# Option validation, immutability, top-level-uuid-stays-native, and map
# rejection live in test_compatibility_mode.py, which owns the option/GUC/DDL
# layer. This file focuses on the nested surface->storage divergence.
# ---------------------------------------------------------------------------


def test_nested_uuid_stored_as_string_in_composite(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """uuid inside a composite is stored as Iceberg string, surface stays uuid."""
    run_command(
        """
        CREATE SCHEMA test_uuid_comp;
        SET search_path TO test_uuid_comp;
        CREATE TYPE acct AS (name text, uid uuid);
        CREATE TABLE t (id int, a acct) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # External Iceberg metadata: the nested uid leaf must be 'string'.
    fields = _metadata_fields(s3, pg_conn, "test_uuid_comp", "t")
    leaf_types = _collect_leaf_types(_field_by_name(fields, "a")["type"])
    assert "uuid" not in leaf_types, f"nested uuid leaked as uuid: {leaf_types}"
    assert "string" in leaf_types, f"nested uuid not stored as string: {leaf_types}"

    # Surface type in the catalog stays uuid, with storage override recorded.
    rows = run_query(
        """
        SELECT field_pg_type::text, field_storage_pg_type::text
        FROM lake_table.field_id_mappings
        WHERE table_name = 'test_uuid_comp.t'::regclass
          AND field_pg_type = 'uuid'::regtype
        """,
        superuser_conn,
    )
    assert rows == [["uuid", "text"]], rows

    # Round-trip: insert as uuid, read back as uuid.
    run_command(
        f"""
        INSERT INTO test_uuid_comp.t VALUES
            (1, ROW('alice', '{U1}')::acct),
            (2, ROW('bob', NULL)::acct),
            (3, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Physical Parquet: the nested uid leaf must be written as VARCHAR, never a
    # native UUID, regardless of what the Iceberg metadata claims.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_comp.t")
    assert "UUID" not in col_types["a"], col_types
    assert "VARCHAR" in col_types["a"], col_types

    result = run_query(
        "SELECT id, (a).name, (a).uid FROM test_uuid_comp.t ORDER BY id", pg_conn
    )
    assert [[r[0], r[1], _u(r[2])] for r in result] == [
        [1, "alice", U1],
        [2, "bob", None],
        [3, None, None],
    ]

    run_command("DROP SCHEMA test_uuid_comp CASCADE;", pg_conn)
    pg_conn.commit()


def test_nested_uuid_array_stored_as_string(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """uuid[] is stored as Iceberg list<string>, round-tripping as uuid[]."""
    run_command(
        """
        CREATE SCHEMA test_uuid_arr;
        CREATE TABLE test_uuid_arr.t (id int, us uuid[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    fields = _metadata_fields(s3, pg_conn, "test_uuid_arr", "t")
    us_type = _field_by_name(fields, "us")["type"]
    assert us_type["type"] == "list"
    assert _collect_leaf_types(us_type) == ["string"]

    run_command(
        f"""
        INSERT INTO test_uuid_arr.t VALUES
            (1, ARRAY['{U1}', '{U2}']::uuid[]),
            (2, ARRAY[]::uuid[]),
            (3, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Physical Parquet: the uuid array element must be written as VARCHAR.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_arr.t")
    assert "UUID" not in col_types["us"], col_types
    assert "VARCHAR" in col_types["us"], col_types

    result = run_query("SELECT id, us FROM test_uuid_arr.t ORDER BY id", pg_conn)
    assert [[r[0], _ulist(r[1])] for r in result] == [
        [1, [U1, U2]],
        [2, []],
        [3, None],
    ]

    run_command("DROP SCHEMA test_uuid_arr CASCADE;", pg_conn)
    pg_conn.commit()


def test_nested_uuid_default_auto_stays_native(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """Without snowflake compat, nested uuid stays native Iceberg uuid (control)."""
    run_command(
        """
        CREATE SCHEMA test_uuid_auto;
        SET search_path TO test_uuid_auto;
        CREATE TYPE acct2 AS (name text, uid uuid);
        CREATE TABLE t (id int, a acct2) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    fields = _metadata_fields(s3, pg_conn, "test_uuid_auto", "t")
    leaf_types = _collect_leaf_types(_field_by_name(fields, "a")["type"])
    assert (
        "uuid" in leaf_types
    ), f"expected native nested uuid in auto mode: {leaf_types}"

    # No storage override should be recorded.
    rows = run_query(
        """
        SELECT count(*) FROM lake_table.field_id_mappings
        WHERE table_name = 'test_uuid_auto.t'::regclass
          AND field_storage_pg_type IS NOT NULL
        """,
        superuser_conn,
    )
    assert rows == [[0]]

    # Physical Parquet: without snowflake compat the nested uuid is written as a
    # native UUID, confirming the divergence is opt-in and not always applied.
    run_command(
        f"INSERT INTO test_uuid_auto.t VALUES (1, ROW('alice', '{U1}')::acct2);",
        pg_conn,
    )
    pg_conn.commit()
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_auto.t")
    assert "UUID" in col_types["a"], col_types

    run_command("DROP SCHEMA test_uuid_auto CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Write-path codegen (INSERT..SELECT pushdown emits the uuid->varchar cast)
# ---------------------------------------------------------------------------


def test_nested_uuid_insert_select_pushdown_casts_to_varchar(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    INSERT..SELECT into a snowflake-compat table whose column has a nested
    uuid is pushed down and the Vectorized SQL casts the uuid leaf to VARCHAR
    via struct_pack/list_transform, then the data reads back as uuid.
    """
    run_command(
        """
        CREATE SCHEMA test_uuid_pd;
        SET search_path TO test_uuid_pd;
        CREATE TYPE acct3 AS (name text, uid uuid);
        CREATE TABLE src (id int, a acct3, us uuid[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        CREATE TABLE tgt (id int, a acct3, us uuid[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        f"""
        INSERT INTO test_uuid_pd.src VALUES
            (1, ROW('alice', '{U1}')::test_uuid_pd.acct3, ARRAY['{U2}','{U3}']::uuid[]),
            (2, ROW('bob', NULL)::test_uuid_pd.acct3, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    explain = "\n".join(
        line[0]
        for line in run_query(
            "EXPLAIN (VERBOSE) INSERT INTO test_uuid_pd.tgt "
            "SELECT * FROM test_uuid_pd.src",
            pg_conn,
        )
    )
    assert "Custom Scan (Query Pushdown)" in explain, explain
    assert "AS VARCHAR" in explain, (
        "Expected uuid->VARCHAR cast in Vectorized SQL:\n" + explain
    )
    assert "struct_pack" in explain, explain
    assert "list_transform" in explain, explain

    run_command("INSERT INTO test_uuid_pd.tgt SELECT * FROM test_uuid_pd.src", pg_conn)
    pg_conn.commit()

    # Physical Parquet on the pushed-down target: both the nested composite uuid
    # and the uuid array element land as VARCHAR on disk.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_pd.tgt")
    assert "UUID" not in col_types["a"], col_types
    assert "UUID" not in col_types["us"], col_types
    assert "VARCHAR" in col_types["a"] and "VARCHAR" in col_types["us"], col_types

    # Catalog agrees: the nested composite uuid and the uuid array element are
    # the two leaves recorded as uuid -> text overrides.
    _assert_uuid_to_text_divergences(superuser_conn, "test_uuid_pd.tgt", 2)

    result = run_query(
        "SELECT id, (a).name, (a).uid, us FROM test_uuid_pd.tgt ORDER BY id", pg_conn
    )
    assert [[r[0], r[1], _u(r[2]), _ulist(r[3])] for r in result] == [
        [1, "alice", U1, [U2, U3]],
        [2, "bob", None, None],
    ]

    run_command("DROP SCHEMA test_uuid_pd CASCADE;", pg_conn)
    pg_conn.commit()


def test_nested_uuid_field_id_fidelity_after_struct_pack(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    The crux of Marco's struct_pack concern: rebuilding a struct produces
    brand-new vectors, so the *only* thing keeping readers correct is that the
    rebuilt field still carries the right Iceberg field id.  Prove the id is
    identical across all three sources of truth, for a weird-quoted leaf name,
    with NULLs present:

        field_id_mappings (catalog)  ==  metadata.json (Iceberg)  ==  Parquet footer

    and that the physical leaf is a string (UTF8), not a native uuid, while the
    non-diverging sibling int leaf keeps its own id and physical type.
    """
    # The diverging leaf has a deliberately nasty quoted name.
    weird = 'uid;"weird'
    run_command(
        """
        CREATE SCHEMA test_uuid_fid;
        SET search_path TO test_uuid_fid;
        CREATE TYPE rec AS ("plain k" int, "uid;""weird" uuid);
        CREATE TABLE t (id int, a rec) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        f"""
        INSERT INTO test_uuid_fid.t VALUES
            (1, ROW(7, '{U1}')::rec),
            (2, ROW(8, NULL)::rec),
            (3, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # 1. Catalog: exactly one diverging leaf, uuid surface stored as text.
    catalog = run_query(
        """
        SELECT field_id, field_pg_type::text, field_storage_pg_type::text
        FROM lake_table.field_id_mappings
        WHERE table_name = 'test_uuid_fid.t'::regclass
          AND field_storage_pg_type IS NOT NULL
        """,
        superuser_conn,
    )
    assert len(catalog) == 1, catalog
    catalog_fid, surface_type, storage_type = catalog[0]
    assert (surface_type, storage_type) == ("uuid", "text"), catalog
    catalog_fid = int(catalog_fid)

    # 2. Iceberg metadata: the weird-quoted leaf is advertised as string and has
    #    the *same* field id the catalog recorded.
    meta_leaves = _metadata_leaf_ids(
        _metadata_fields(s3, pg_conn, "test_uuid_fid", "t")
    )
    assert weird in meta_leaves, meta_leaves
    meta_fid, meta_type = meta_leaves[weird]
    assert meta_type == "string", meta_leaves
    assert meta_fid == catalog_fid, (meta_fid, catalog_fid)

    # 3. Physical Parquet footer: the leaf with that exact field id is a string
    #    (UTF8), carries the raw (unquoted) field name, and is not a uuid.
    leaves = _parquet_leaf_fields(superuser_conn, pgduck_conn, "test_uuid_fid.t")
    assert catalog_fid in leaves, (catalog_fid, leaves)
    name, ptype, converted = leaves[catalog_fid]
    assert name == weird, leaves
    assert converted == "UTF8" and ptype == "BYTE_ARRAY", leaves

    # The non-diverging sibling int leaf keeps its own distinct id + physical type.
    int_leaves = [(fid, n, pt) for fid, (n, pt, _c) in leaves.items() if n == "plain k"]
    assert len(int_leaves) == 1, leaves
    int_fid, _, int_ptype = int_leaves[0]
    assert int_fid != catalog_fid, leaves
    assert int_ptype == "INT32", leaves

    # Round-trip survives the rebuild + NULLs.
    result = run_query(
        'SELECT id, (a)."plain k", (a)."uid;""weird" FROM test_uuid_fid.t ORDER BY id',
        pg_conn,
    )
    assert [[r[0], r[1], _u(r[2])] for r in result] == [
        [1, 7, U1],
        [2, 8, None],
        [3, None, None],
    ]

    run_command("DROP SCHEMA test_uuid_fid CASCADE;", pg_conn)
    pg_conn.commit()


def test_non_diverging_struct_is_not_repacked(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    Even under snowflake compat, a composite with *no* diverging leaf must be
    passed through verbatim -- never rebuilt with struct_pack.  This guards the
    gating in AppendStorageCastExpression and keeps writes cheap for the common
    case.  We assert the pushed-down SQL contains no struct_pack at all.
    """
    run_command(
        """
        CREATE SCHEMA test_uuid_nopack;
        SET search_path TO test_uuid_nopack;
        CREATE TYPE plainrec AS (x int, y text);
        CREATE TABLE src (id int, p plainrec) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        CREATE TABLE tgt (id int, p plainrec) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        """
        INSERT INTO test_uuid_nopack.src VALUES
            (1, ROW(7, 'a')::test_uuid_nopack.plainrec),
            (2, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    explain = "\n".join(
        line[0]
        for line in run_query(
            "EXPLAIN (VERBOSE) INSERT INTO test_uuid_nopack.tgt "
            "SELECT * FROM test_uuid_nopack.src",
            pg_conn,
        )
    )
    assert "Custom Scan (Query Pushdown)" in explain, explain
    assert "struct_pack" not in explain, (
        "non-diverging struct must not be rebuilt with struct_pack:\n" + explain
    )

    run_command(
        "INSERT INTO test_uuid_nopack.tgt SELECT * FROM test_uuid_nopack.src", pg_conn
    )
    pg_conn.commit()
    result = run_query(
        "SELECT id, (p).x, (p).y FROM test_uuid_nopack.tgt ORDER BY id", pg_conn
    )
    assert result == [[1, 7, "a"], [2, None, None]], result

    run_command("DROP SCHEMA test_uuid_nopack CASCADE;", pg_conn)
    pg_conn.commit()


def test_combined_native_encode_and_storage_cast_same_column(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    The nastiest interaction: one composite column whose leaves require *both*
    rewrite passes at once --

      * uid uuid    -> storage cast    (uuid -> iceberg string, struct_pack)
      * span interval -> native encode (-> struct(months,days,microseconds))
      * tz timetz     -> native encode (UTC-normalized TIME)
      * label text    -> passthrough

    Native encode and storage cast run in a single traversal
    (IcebergWrapQueryWithRewrites), so this column is rebuilt by ONE struct_pack
    that carries every transform at once, not by two stacked rebuilds.  We
    assert the pushed-down SQL carries every transform, that the composite is
    repacked exactly once, then that data (incl. all-NULL and NULL-row cases)
    round-trips.
    """
    run_command(
        """
        CREATE SCHEMA test_uuid_mix;
        SET search_path TO test_uuid_mix;
        CREATE TYPE mixrec AS (uid uuid, span interval, tz timetz, label text);
        CREATE TABLE src (id int, m mixrec) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        CREATE TABLE tgt (id int, m mixrec) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        f"""
        INSERT INTO test_uuid_mix.src VALUES
            (1, ROW('{U1}', interval '1 year 2 mons 3 days 04:05:06.789',
                    timetz '08:00:00+04', 'x')::test_uuid_mix.mixrec),
            (2, ROW(NULL, NULL, NULL, NULL)::test_uuid_mix.mixrec),
            (3, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    explain = "\n".join(
        line[0]
        for line in run_query(
            "EXPLAIN (VERBOSE) INSERT INTO test_uuid_mix.tgt "
            "SELECT * FROM test_uuid_mix.src",
            pg_conn,
        )
    )
    assert "Custom Scan (Query Pushdown)" in explain, explain
    # storage cast (uuid -> string) ...
    assert "AS VARCHAR" in explain, explain
    assert "struct_pack" in explain, explain
    # ... composed with both native encodes on the same column.
    assert "months :=" in explain, "interval encode missing:\n" + explain
    assert "AT TIME ZONE 'UTC'" in explain, "timetz encode missing:\n" + explain
    # Single-pass merge: exactly two struct_pack calls -- one rebuilding the
    # mixrec composite, one for the interval leaf.  The old two-pass code
    # rebuilt the composite twice (storage cast wrapping native encode), which
    # would show three.
    assert explain.count("struct_pack") == 2, (
        "expected one composite rebuild + one interval struct_pack:\n" + explain
    )

    run_command(
        "INSERT INTO test_uuid_mix.tgt SELECT * FROM test_uuid_mix.src", pg_conn
    )
    pg_conn.commit()

    # Physical Parquet: the nested uuid is a string; interval/timetz are encoded
    # away from any native type; no UUID leaks.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_mix.tgt")
    assert "UUID" not in col_types["m"], col_types
    assert "VARCHAR" in col_types["m"], col_types

    # Only the uuid leaf is a storage divergence; the interval/timetz leaves are
    # native encodes and carry no field_storage_pg_type override.
    _assert_uuid_to_text_divergences(superuser_conn, "test_uuid_mix.tgt", 1)

    result = run_query(
        "SELECT id, (m).uid, (m).span::text, (m).tz::text, (m).label "
        "FROM test_uuid_mix.tgt ORDER BY id",
        pg_conn,
    )
    assert [[r[0], _u(r[1]), r[2], r[3], r[4]] for r in result] == [
        [1, U1, "1 year 2 months 3 days 04:05:06.789", "04:00:00+00", "x"],
        [2, None, None, None, None],
        [3, None, None, None, None],
    ]

    run_command("DROP SCHEMA test_uuid_mix CASCADE;", pg_conn)
    pg_conn.commit()


def test_combined_native_storage_array_leaves_same_column(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    Same native+storage composition, but every leaf is an *array* so the
    list_transform reconstruction/cast paths compose too:

      * uids  uuid[]     -> storage cast on the element (uuid -> string)
      * spans interval[] -> native encode on the element (struct(...))
      * tzs   timetz[]   -> native encode on the element (UTC TIME)

    A uuid[] element living next to an interval[] element is exactly the read
    path that previously broke (interval reconstructed to INTERVAL but cast
    against the on-disk struct shape).  Covers empty arrays and NULLs.
    """
    run_command(
        """
        CREATE SCHEMA test_uuid_arrmix;
        SET search_path TO test_uuid_arrmix;
        CREATE TYPE arrrec AS (uids uuid[], spans interval[], tzs timetz[], label text);
        CREATE TABLE t (id int, m arrrec) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        f"""
        INSERT INTO test_uuid_arrmix.t VALUES
            (1, ROW(ARRAY['{U1}','{U2}']::uuid[],
                    ARRAY['1 day'::interval,'2 hours'::interval],
                    ARRAY['08:00:00+04'::timetz,'00:00:00+00'::timetz],
                    'x')::test_uuid_arrmix.arrrec),
            (2, ROW(ARRAY[]::uuid[], ARRAY[]::interval[], ARRAY[]::timetz[],
                    'empty')::test_uuid_arrmix.arrrec),
            (3, ROW(NULL, NULL, NULL, NULL)::test_uuid_arrmix.arrrec),
            (4, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Physical Parquet: uuid array element stored as string, no native uuid leak.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_arrmix.t")
    assert "UUID" not in col_types["m"], col_types
    assert "VARCHAR" in col_types["m"], col_types

    # Only the uuid[] element leaf diverges; the interval[]/timetz[] leaves are
    # native encodes with no storage override.
    _assert_uuid_to_text_divergences(superuser_conn, "test_uuid_arrmix.t", 1)

    result = run_query(
        "SELECT id, (m).uids, (m).spans[1]::text, (m).spans[2]::text, "
        "(m).tzs[1]::text, (m).tzs[2]::text, (m).label "
        "FROM test_uuid_arrmix.t ORDER BY id",
        pg_conn,
    )
    assert [[r[0], _ulist(r[1]), r[2], r[3], r[4], r[5], r[6]] for r in result] == [
        [1, [U1, U2], "1 day", "02:00:00", "04:00:00+00", "00:00:00+00", "x"],
        [2, [], None, None, None, None, "empty"],
        [3, None, None, None, None, None, None],
        [4, None, None, None, None, None, None],
    ]

    run_command("DROP SCHEMA test_uuid_arrmix CASCADE;", pg_conn)
    pg_conn.commit()


def test_nested_uuid_null_struct_vs_null_fields_distinct(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    NULL fidelity through the struct_pack rebuild (Marco's concern, at the SQL
    level): a non-null row whose fields are all NULL (ROW(NULL,NULL)) must stay
    distinct from a NULL row.  The "CASE WHEN m IS NOT NULL THEN struct_pack(...)
    ELSE NULL END" guard must preserve that, and identically to auto mode (which
    never repacks), so the storage rewrite introduces no NULL drift.
    """
    run_command(
        """
        CREATE SCHEMA test_uuid_null;
        SET search_path TO test_uuid_null;
        CREATE TYPE acctn AS (name text, uid uuid);
        CREATE TABLE sf (id int, a acctn) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        CREATE TABLE au (id int, a acctn) USING iceberg;
        """,
        pg_conn,
    )
    for tbl in ("sf", "au"):
        run_command(
            f"""
            INSERT INTO test_uuid_null.{tbl} VALUES
                (1, ROW('bob', '{U1}')::test_uuid_null.acctn),
                (2, ROW(NULL, NULL)::test_uuid_null.acctn),
                (3, NULL);
            """,
            pg_conn,
        )
    pg_conn.commit()

    expected = [
        [1, False, "bob", U1],
        [2, False, None, None],  # non-null row, null fields
        [3, True, None, None],  # null row
    ]
    for tbl in ("sf", "au"):
        result = run_query(
            f"SELECT id, a IS NULL, (a).name, (a).uid "
            f"FROM test_uuid_null.{tbl} ORDER BY id",
            pg_conn,
        )
        assert [[r[0], r[1], r[2], _u(r[3])] for r in result] == expected, tbl

    # The snowflake table physically stored the nested uuid as string.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_null.sf")
    assert "UUID" not in col_types["a"] and "VARCHAR" in col_types["a"], col_types

    run_command("DROP SCHEMA test_uuid_null CASCADE;", pg_conn)
    pg_conn.commit()


def test_drop_column_divergent_parity_and_readd(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    Dropping a column whose nested uuid is a *registered* storage-divergent leaf
    must behave exactly like dropping a plain column, and leave the remaining
    columns resolving correctly (matching is by name).  Uses adversarial quoted
    names throughout.

    Per the agreed parity_only behavior, the dropped column's field_id_mappings
    rows are retained (orphaned), not cleaned up -- and this is identical for a
    plain column, so a registered attribute is "not different".  Re-adding a
    divergent column re-registers a fresh mapping and stores as string again.
    """
    run_command(
        """
        CREATE SCHEMA test_uuid_drop;
        SET search_path TO test_uuid_drop;
        CREATE TYPE acct AS (name text, uid uuid);
        CREATE TABLE t (
            id int,
            "weird;""col" acct,
            keep int,
            a2 acct
        ) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        f"""
        INSERT INTO test_uuid_drop.t VALUES
            (1, ROW('a', '{U1}')::acct, 7, ROW('b', '{U2}')::acct),
            (2, NULL, NULL, ROW(NULL, NULL)::acct);
        """,
        pg_conn,
    )
    pg_conn.commit()

    def total_mappings():
        return run_query(
            "SELECT count(*) FROM lake_table.field_id_mappings "
            "WHERE table_name = 'test_uuid_drop.t'::regclass",
            superuser_conn,
        )[0][0]

    def divergent_mappings():
        return run_query(
            "SELECT count(*) FROM lake_table.field_id_mappings "
            "WHERE table_name = 'test_uuid_drop.t'::regclass "
            "AND field_storage_pg_type IS NOT NULL",
            superuser_conn,
        )[0][0]

    before_total = total_mappings()
    # two divergent leaves: the uid inside "weird;col" and inside a2.
    assert divergent_mappings() == 2, divergent_mappings()

    # Drop the *registered* (divergent) quoted column.
    run_command('ALTER TABLE test_uuid_drop.t DROP COLUMN "weird;""col";', pg_conn)
    pg_conn.commit()
    # Orphan rows are retained (parity_only): nothing is removed.
    assert total_mappings() == before_total, (before_total, total_mappings())
    assert divergent_mappings() == 2, divergent_mappings()

    # Dropping a plain column behaves identically (rows retained too).
    run_command("ALTER TABLE test_uuid_drop.t DROP COLUMN keep;", pg_conn)
    pg_conn.commit()
    assert total_mappings() == before_total, (before_total, total_mappings())

    # The surviving divergent column still resolves by name and stays string.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_drop.t")
    assert "UUID" not in col_types["a2"] and "VARCHAR" in col_types["a2"], col_types
    result = run_query(
        "SELECT id, (a2).name, (a2).uid FROM test_uuid_drop.t ORDER BY id", pg_conn
    )
    assert [[r[0], r[1], _u(r[2])] for r in result] == [
        [1, "b", U2],
        [2, None, None],
    ]

    # Re-add a divergent column with another nasty quoted name: registers fresh.
    run_command(
        'ALTER TABLE test_uuid_drop.t ADD COLUMN "re ""added"";" acct;', pg_conn
    )
    pg_conn.commit()
    assert divergent_mappings() == 3, divergent_mappings()
    run_command(
        f"""UPDATE test_uuid_drop.t SET "re ""added"";" = ROW('c', '{U3}')::acct WHERE id = 1;""",
        pg_conn,
    )
    pg_conn.commit()
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_drop.t")
    assert "UUID" not in col_types['re "added";'], col_types
    assert "VARCHAR" in col_types['re "added";'], col_types
    result = run_query(
        'SELECT id, ("re ""added"";").uid FROM test_uuid_drop.t ORDER BY id', pg_conn
    )
    assert [[r[0], _u(r[1])] for r in result] == [[1, U3], [2, None]]

    run_command("DROP SCHEMA test_uuid_drop CASCADE;", pg_conn)
    pg_conn.commit()


def test_storage_cast_copy_and_nonpushdown_paths(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    Exercise the write/read paths other than INSERT..SELECT pushdown (which is
    covered above), all on a nested-uuid composite under snowflake compat, all
    with NULLs:

      * INSERT..SELECT from a *heap* source  -> non-pushdown (row-by-row) write
      * COPY ... TO   (export / read path)
      * COPY ... FROM (import / write path)  -> records pushdown status
      * SELECT through a non-shippable barrier (no pushdown) read

    Each must still store the nested uuid as Parquet string and round-trip.
    """
    export_url = f"s3://{TEST_BUCKET}/test_uuid_paths/export.parquet"
    run_command(
        f"""
        CREATE SCHEMA test_uuid_paths;
        SET search_path TO test_uuid_paths;
        CREATE TYPE acct AS (name text, uid uuid);
        CREATE TABLE heap_src (id int, a acct) USING heap;
        CREATE TABLE ins_sel (id int, a acct) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        CREATE TABLE copy_tgt (id int, a acct) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        INSERT INTO heap_src VALUES
            (1, ROW('alice', '{U1}')::acct),
            (2, ROW('bob', NULL)::acct),
            (3, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # --- INSERT..SELECT from heap: non-pushdown write path ---
    explain = "\n".join(
        line[0]
        for line in run_query(
            "EXPLAIN (VERBOSE) INSERT INTO test_uuid_paths.ins_sel "
            "SELECT * FROM test_uuid_paths.heap_src",
            pg_conn,
        )
    )
    assert "Custom Scan (Query Pushdown)" not in explain, (
        "heap source must NOT push down:\n" + explain
    )
    run_command(
        "INSERT INTO test_uuid_paths.ins_sel SELECT * FROM test_uuid_paths.heap_src",
        pg_conn,
    )
    pg_conn.commit()
    col_types = _parquet_column_types(
        superuser_conn, pgduck_conn, "test_uuid_paths.ins_sel"
    )
    assert "UUID" not in col_types["a"] and "VARCHAR" in col_types["a"], col_types
    _assert_uuid_to_text_divergences(superuser_conn, "test_uuid_paths.ins_sel", 1)
    assert [
        [r[0], r[1], _u(r[2])]
        for r in run_query(
            "SELECT id, (a).name, (a).uid FROM test_uuid_paths.ins_sel ORDER BY id",
            pg_conn,
        )
    ] == [[1, "alice", U1], [2, "bob", None], [3, None, None]]

    # --- COPY TO (export / read path) then COPY FROM (import / write path) ---
    run_command(
        f"COPY (SELECT * FROM test_uuid_paths.ins_sel) TO '{export_url}'", pg_conn
    )
    pg_conn.commit()

    run_command(f"COPY test_uuid_paths.copy_tgt FROM '{export_url}'", pg_conn)
    pg_conn.commit()
    assert (
        run_query("SELECT count(*) FROM test_uuid_paths.copy_tgt", pg_conn)[0][0] == 3
    )
    col_types = _parquet_column_types(
        superuser_conn, pgduck_conn, "test_uuid_paths.copy_tgt"
    )
    assert "UUID" not in col_types["a"] and "VARCHAR" in col_types["a"], col_types
    _assert_uuid_to_text_divergences(superuser_conn, "test_uuid_paths.copy_tgt", 1)
    assert [
        [r[0], r[1], _u(r[2])]
        for r in run_query(
            "SELECT id, (a).name, (a).uid FROM test_uuid_paths.copy_tgt ORDER BY id",
            pg_conn,
        )
    ] == [[1, "alice", U1], [2, "bob", None], [3, None, None]]

    # --- SELECT through a non-shippable barrier (OFFSET in a CTE w/ volatile) ---
    # random() is volatile + the materialized CTE prevents full pushdown; the
    # nested uuid must still reconstruct from the stored string.
    rows = run_query(
        """
        WITH x AS MATERIALIZED (
            SELECT id, (a).uid AS uid, random() AS r
            FROM test_uuid_paths.copy_tgt
        )
        SELECT id, uid FROM x ORDER BY id
        """,
        pg_conn,
    )
    assert [[r[0], _u(r[1])] for r in rows] == [[1, U1], [2, None], [3, None]]

    run_command("DROP SCHEMA test_uuid_paths CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# ALTER TABLE ADD COLUMN goes through the same registration path
# ---------------------------------------------------------------------------


def test_alter_add_column_nested_uuid_stored_as_string(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """ALTER ... ADD COLUMN applies the same surface/storage mapping."""
    run_command(
        """
        CREATE SCHEMA test_uuid_alter;
        SET search_path TO test_uuid_alter;
        CREATE TYPE acct4 AS (name text, uid uuid);
        CREATE TABLE t (id int) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        ALTER TABLE t ADD COLUMN a acct4;
        """,
        pg_conn,
    )
    pg_conn.commit()

    fields = _metadata_fields(s3, pg_conn, "test_uuid_alter", "t")
    leaf_types = _collect_leaf_types(_field_by_name(fields, "a")["type"])
    assert "uuid" not in leaf_types and "string" in leaf_types, leaf_types

    run_command(
        f"INSERT INTO test_uuid_alter.t VALUES (1, ROW('alice', '{U1}')::test_uuid_alter.acct4);",
        pg_conn,
    )
    pg_conn.commit()

    # Physical Parquet: the column added via ALTER also writes the nested uuid
    # as VARCHAR, so the mapping is not specific to the CREATE TABLE path.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_alter.t")
    assert "UUID" not in col_types["a"], col_types
    assert "VARCHAR" in col_types["a"], col_types

    result = run_query("SELECT id, (a).uid FROM test_uuid_alter.t ORDER BY id", pg_conn)
    assert [[r[0], _u(r[1])] for r in result] == [[1, U1]]

    run_command("DROP SCHEMA test_uuid_alter CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# DEFAULT / GENERATED on uuid columns
# ---------------------------------------------------------------------------


def test_uuid_default_and_generated_columns(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    DEFAULT and GENERATED values must go through the storage rewrite when they
    are *composites with a nested uuid* -- a top-level uuid never enters the
    struct_pack path, so the interesting case is a DEFAULT/GENERATED value whose
    nested uuid has to be cast to string on write.

    Covers, under snowflake compat:
      * a top-level uuid DEFAULT/GENERATED (stays native UUID -- control),
      * a composite DEFAULT with a nested uuid (rewritten to string),
      * a STORED GENERATED composite derived from sibling columns, whose nested
        uuid is rewritten -- including a row where the source uuid is NULL,
      * the same generated table populated via INSERT..SELECT (which the STORED
        generated column forces onto the non-pushdown path).
    """
    run_command(
        f"""
        CREATE SCHEMA test_uuid_default;
        SET search_path TO test_uuid_default;
        CREATE TYPE acct AS (name text, uid uuid);
        CREATE TABLE t (
            id int,
            top_u uuid DEFAULT '{U1}'::uuid,
            gen_u uuid GENERATED ALWAYS AS ('{U2}'::uuid) STORED,
            -- composite DEFAULT: the nested uuid must be storage-cast to string
            def_acct acct DEFAULT ROW('def', '{U1}'::uuid)::acct,
            raw_name text,
            raw_uid uuid,
            -- STORED generated composite built from siblings; nested uuid -> string
            gen_acct acct GENERATED ALWAYS AS (ROW(raw_name, raw_uid)::acct) STORED
        ) USING iceberg WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO test_uuid_default.t (id, raw_name, raw_uid) VALUES
            (1, 'alice', '{U3}'),
            (2, NULL, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # Physical Parquet: top-level uuids stay native UUID; the nested uuids inside
    # the DEFAULT and GENERATED composites diverge to string.
    col_types = _parquet_column_types(
        superuser_conn, pgduck_conn, "test_uuid_default.t"
    )
    assert col_types["top_u"] == "UUID", col_types
    assert col_types["gen_u"] == "UUID", col_types
    assert "UUID" not in col_types["def_acct"], col_types
    assert "VARCHAR" in col_types["def_acct"], col_types
    assert "UUID" not in col_types["gen_acct"], col_types
    assert "VARCHAR" in col_types["gen_acct"], col_types

    result = run_query(
        "SELECT id, top_u, gen_u, (def_acct).name, (def_acct).uid, "
        "(gen_acct).name, (gen_acct).uid FROM test_uuid_default.t ORDER BY id",
        pg_conn,
    )
    assert [
        [r[0], _u(r[1]), _u(r[2]), r[3], _u(r[4]), r[5], _u(r[6])] for r in result
    ] == [
        [1, U1, U2, "def", U1, "alice", U3],
        [2, U1, U2, "def", U1, None, None],
    ]

    # INSERT..SELECT into the generated table: the STORED generated composite is
    # recomputed by Postgres (not pushed down), then its nested uuid is rewritten.
    explain = "\n".join(
        row[0]
        for row in run_query(
            "EXPLAIN (VERBOSE) INSERT INTO test_uuid_default.t (id, raw_name, raw_uid) "
            "SELECT id + 10, raw_name, raw_uid FROM test_uuid_default.t",
            pg_conn,
        )
    )
    assert "Custom Scan (Query Pushdown)" not in explain, (
        "STORED generated column should force the non-pushdown path:\n" + explain
    )
    run_command(
        "INSERT INTO test_uuid_default.t (id, raw_name, raw_uid) "
        "SELECT id + 10, raw_name, raw_uid FROM test_uuid_default.t WHERE id <= 2",
        pg_conn,
    )
    pg_conn.commit()
    gen_rows = run_query(
        "SELECT id, (gen_acct).name, (gen_acct).uid FROM test_uuid_default.t "
        "WHERE id > 10 ORDER BY id",
        pg_conn,
    )
    assert [[r[0], r[1], _u(r[2])] for r in gen_rows] == [
        [11, "alice", U3],
        [12, None, None],
    ]

    run_command("DROP SCHEMA test_uuid_default CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Deeply nested pyramid: array<struct> with two sub-structs, many uuids at
# several depths, a uuid[] inside a struct, a domain-over-uuid leaf, and a
# non-uuid (geo) sub-struct that must survive untouched.
# ---------------------------------------------------------------------------


def test_deeply_nested_uuid_pyramid_storage_and_roundtrip(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    Every uuid leaf at every depth diverges to string under snowflake compat,
    domains-over-uuid included, while geo doubles / text labels are untouched
    and the surface schema (including the domain) is preserved.  Verified end to
    end: Iceberg metadata, persisted per-leaf mappings, physical Parquet, an
    auto-mode control, and a deep round-trip with NULLs and an empty array.
    """
    _create_deep_uuid_types(pg_conn, "test_uuid_deep")
    run_command(
        """
        CREATE TABLE test_uuid_deep.t (id int, nodes node[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        -- control: identical types, default (auto) compat
        CREATE TABLE test_uuid_deep.t_auto (id int, nodes node[]) USING iceberg;
        """,
        pg_conn,
    )
    pg_conn.commit()

    # --- Iceberg metadata: count every leaf, not just "is there a string". ----
    nodes_type = _field_by_name(
        _metadata_fields(s3, pg_conn, "test_uuid_deep", "t"), "nodes"
    )["type"]
    leaves = _collect_leaf_types(nodes_type)
    # 6 uuid leaves -> string (node_id, primary, secondary-domain, device, backup,
    # tag element) + 1 text label -> string = 7 strings; geo lat/lon = 2 doubles.
    assert leaves.count("uuid") == 0, leaves
    assert leaves.count("string") == 7, leaves
    assert leaves.count("double") == 2, leaves

    # --- Persisted mapping: exactly the 6 uuid leaves get a storage override. -
    overrides = run_query(
        """
        SELECT field_storage_pg_type::text, count(*)
        FROM lake_table.field_id_mappings
        WHERE table_name = 'test_uuid_deep.t'::regclass
          AND field_storage_pg_type IS NOT NULL
        GROUP BY 1
        """,
        superuser_conn,
    )
    assert overrides == [["text", 6]], overrides
    # The domain-over-uuid leaf in particular must be one of them.
    domain_override = run_query(
        """
        SELECT count(*)
        FROM lake_table.field_id_mappings
        WHERE table_name = 'test_uuid_deep.t'::regclass
          AND field_pg_type = 'test_uuid_deep.account_id'::regtype
          AND field_storage_pg_type = 'text'::regtype
        """,
        superuser_conn,
    )
    assert domain_override == [[1]], domain_override

    # --- Surface schema unchanged: column type and the domain both preserved. -
    surface = run_query(
        """
        SELECT
            (SELECT atttypid::regtype::text FROM pg_attribute
              WHERE attrelid = 'test_uuid_deep.t'::regclass AND attname = 'nodes'),
            (SELECT atttypid::regtype::text FROM pg_attribute
              WHERE attrelid = 'test_uuid_deep.owner_info'::regclass
                AND attname = 'secondary_owner')
        """,
        superuser_conn,
    )
    assert surface == [["test_uuid_deep.node[]", "test_uuid_deep.account_id"]], surface

    run_command(
        f"""
        SET search_path TO test_uuid_deep;
        INSERT INTO test_uuid_deep.t VALUES
            (1, {_DEEP_TWO_NODES}),
            (2, NULL),
            (3, ARRAY[]::node[]);
        """,
        pg_conn,
    )
    pg_conn.commit()

    # --- Physical Parquet: no UUID anywhere, doubles survive, it's array<struct>.
    col_types = _parquet_column_types(superuser_conn, pgduck_conn, "test_uuid_deep.t")
    nodes_phys = col_types["nodes"]
    assert "UUID" not in nodes_phys, nodes_phys
    assert "DOUBLE" in nodes_phys, nodes_phys
    assert "STRUCT" in nodes_phys and nodes_phys.endswith("[]"), nodes_phys

    # --- Deep round-trip, including the sparse node and the NULL/empty rows. ---
    deep = run_query(
        """
        SELECT
            (nodes[1]).node_id,
            ((nodes[1]).owners).primary_owner,
            ((nodes[1]).owners).secondary_owner,
            ((nodes[1]).owners).label,
            ((nodes[1]).devices).device_id,
            ((nodes[1]).devices).backup_id,
            (((nodes[1]).devices).location).lat,
            (((nodes[1]).devices).location).lon,
            (nodes[1]).tags,
            (nodes[2]).node_id,
            ((nodes[2]).owners).secondary_owner,
            ((nodes[2]).devices).backup_id,
            (nodes[2]).tags
        FROM test_uuid_deep.t WHERE id = 1
        """,
        pg_conn,
    )[0]
    assert [
        _u(deep[0]),
        _u(deep[1]),
        _u(deep[2]),
        deep[3],
        _u(deep[4]),
        _u(deep[5]),
        deep[6],
        deep[7],
        _ulist(deep[8]),
        _u(deep[9]),
        deep[10],
        deep[11],
        _ulist(deep[12]),
    ] == [
        NODE1,
        PRIM1,
        SEC1,
        "main",
        DEV1,
        BAK1,
        1.5,
        2.5,
        [TAG1, TAG2],
        NODE2,
        None,
        None,
        None,
    ], deep

    shape = run_query(
        "SELECT id, nodes IS NULL, cardinality(nodes) "
        "FROM test_uuid_deep.t ORDER BY id",
        pg_conn,
    )
    assert shape == [[1, False, 2], [2, True, None], [3, False, 0]], shape

    # --- Auto-mode control: same types, uuids stay native, no overrides. ------
    run_command(
        f"""
        SET search_path TO test_uuid_deep;
        INSERT INTO test_uuid_deep.t_auto VALUES (1, {_DEEP_TWO_NODES});
        """,
        pg_conn,
    )
    pg_conn.commit()
    auto_phys = _parquet_column_types(
        superuser_conn, pgduck_conn, "test_uuid_deep.t_auto"
    )["nodes"]
    assert "UUID" in auto_phys, auto_phys
    assert "DOUBLE" in auto_phys, auto_phys
    auto_overrides = run_query(
        """
        SELECT count(*) FROM lake_table.field_id_mappings
        WHERE table_name = 'test_uuid_deep.t_auto'::regclass
          AND field_storage_pg_type IS NOT NULL
        """,
        superuser_conn,
    )
    assert auto_overrides == [[0]], auto_overrides

    run_command("DROP SCHEMA test_uuid_deep CASCADE;", pg_conn)
    pg_conn.commit()


def test_deeply_nested_uuid_pyramid_insert_select_writepath(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    Deep pyramid copied via INSERT..SELECT from an auto (native-uuid) source
    into a snowflake target.  The scan is offloaded to DuckDB while the write
    path must cast every nested uuid leaf -- at every depth, through both
    sub-structs, the uuid[], and the domain-over-uuid -- to its storage type,
    leaving geo doubles untouched.  Validated by the physical Parquet of the
    target plus a deep round-trip, rather than brittle generated-SQL matching
    (a top-level array<struct> isn't a single-node INSERT..SELECT pushdown, so
    the casts run in the foreign-modify path, not the Vectorized SQL).
    """
    _create_deep_uuid_types(pg_conn, "test_uuid_deep_pd")
    # src is auto (stores native uuid), tgt is snowflake.  This guarantees the
    # pushed-down statement must actually cast surface uuid -> storage string on
    # the way in -- a snowflake->snowflake copy would be a storage-identity move
    # and emit no casts at all.
    run_command(
        """
        CREATE TABLE test_uuid_deep_pd.src (id int, nodes node[]) USING iceberg;
        CREATE TABLE test_uuid_deep_pd.tgt (id int, nodes node[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        """,
        pg_conn,
    )
    run_command(
        f"""
        SET search_path TO test_uuid_deep_pd;
        INSERT INTO test_uuid_deep_pd.src VALUES (1, {_DEEP_TWO_NODES});
        """,
        pg_conn,
    )
    pg_conn.commit()

    explain = "\n".join(
        line[0]
        for line in run_query(
            "EXPLAIN (VERBOSE) INSERT INTO test_uuid_deep_pd.tgt "
            "SELECT * FROM test_uuid_deep_pd.src",
            pg_conn,
        )
    )
    # The source scan is offloaded to DuckDB (the native-uuid source is read via
    # the vectorized engine); the surface->storage casts then run on write.
    assert "Engine: DuckDB" in explain, explain

    run_command(
        "INSERT INTO test_uuid_deep_pd.tgt SELECT * FROM test_uuid_deep_pd.src",
        pg_conn,
    )
    pg_conn.commit()

    # The source physically stored native UUID; the target must end up VARCHAR
    # at every uuid leaf, proving the deep write-path cast actually fired.
    src_phys = _parquet_column_types(
        superuser_conn, pgduck_conn, "test_uuid_deep_pd.src"
    )["nodes"]
    assert "UUID" in src_phys, src_phys
    tgt_phys = _parquet_column_types(
        superuser_conn, pgduck_conn, "test_uuid_deep_pd.tgt"
    )["nodes"]
    assert "UUID" not in tgt_phys, tgt_phys
    assert "DOUBLE" in tgt_phys, tgt_phys

    roundtrip = run_query(
        """
        SELECT
            ((nodes[1]).owners).secondary_owner,
            (((nodes[1]).devices).location).lat,
            (nodes[1]).tags,
            ((nodes[2]).devices).backup_id
        FROM test_uuid_deep_pd.tgt WHERE id = 1
        """,
        pg_conn,
    )[0]
    assert [_u(roundtrip[0]), roundtrip[1], _ulist(roundtrip[2]), roundtrip[3]] == [
        SEC1,
        1.5,
        [TAG1, TAG2],
        None,
    ], roundtrip

    run_command("DROP SCHEMA test_uuid_deep_pd CASCADE;", pg_conn)
    pg_conn.commit()


# ---------------------------------------------------------------------------
# Cross-engine read-back (object_store catalog): an external Iceberg reader
# (DuckDB's iceberg_scan) must see the nested uuid as a *string*, which is the
# whole point of snowflake compat -- engines that cannot store a nested uuid
# still read a value.  pg_lake itself reads the surface uuid back.
# ---------------------------------------------------------------------------


def test_cross_engine_duckdb_iceberg_scan_sees_string(
    pg_conn, superuser_conn, pgduck_conn, s3, with_default_location
):
    """
    Write a snowflake-compat table via pg_lake (object_store catalog), then read
    it back through DuckDB's Iceberg reader straight from the catalog metadata.
    DuckDB has no pg_lake surface-type knowledge, so it must observe exactly the
    on-disk Iceberg schema: nested uuid -> string, uuid[] -> list<string>.  NULLs
    (null field, null struct, null array) must survive cross-engine.
    """
    run_command(
        f"""
        CREATE SCHEMA test_uuid_xeng;
        SET search_path TO test_uuid_xeng;
        CREATE TYPE acct AS (name text, uid uuid);
        CREATE TABLE t (id int, a acct, us uuid[]) USING iceberg
            WITH (compatibility_mode = 'snowflake');
        INSERT INTO t VALUES
            (1, ROW('alice', '{U1}')::acct, ARRAY['{U2}','{U3}']::uuid[]),
            (2, ROW('bob', NULL)::acct, NULL),
            (3, NULL, NULL);
        """,
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        "SELECT metadata_location FROM iceberg_tables "
        "WHERE table_name = 't' AND table_namespace = 'test_uuid_xeng'",
        pg_conn,
    )[0][0]

    # External engine's view of the schema: nested uuid is string, never uuid.
    described = {
        row[0]: row[1]
        for row in run_query(
            f"DESCRIBE SELECT * FROM iceberg_scan('{metadata_location}')", pgduck_conn
        )
    }
    assert "UUID" not in described["a"], described
    assert "VARCHAR" in described["a"], described
    assert described["us"] == "VARCHAR[]", described

    # External engine reads the uuid text out of the string leaves, NULLs intact.
    duck_rows = run_query(
        f"SELECT id, a.uid, us FROM iceberg_scan('{metadata_location}') ORDER BY id",
        pgduck_conn,
    )
    assert [[r[0], _u(r[1]), _ulist(r[2])] for r in duck_rows] == [
        [1, U1, [U2, U3]],
        [2, None, None],
        [3, None, None],
    ]

    # pg_lake itself still presents the surface uuid type.
    pg_rows = run_query(
        "SELECT id, (a).uid, us FROM test_uuid_xeng.t ORDER BY id", pg_conn
    )
    assert [[r[0], _u(r[1]), _ulist(r[2])] for r in pg_rows] == [
        [1, U1, [U2, U3]],
        [2, None, None],
        [3, None, None],
    ]

    run_command("DROP SCHEMA test_uuid_xeng CASCADE;", pg_conn)
    pg_conn.commit()
