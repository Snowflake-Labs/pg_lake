from utils_pytest import *


# show that re-setting the same partition spec
# doesn't add a new spec, instead re-uses
def test_re_set_partition_fields(
    extension, s3, with_default_location, pg_conn, superuser_conn
):

    run_command(f"""CREATE SCHEMA test_re_set_partition_fields;""", pg_conn)

    run_command(
        "CREATE TABLE test_re_set_partition_fields.tbl(key int, value text) USING iceberg",
        pg_conn,
    )
    pg_conn.commit()

    # when there are no partitions, the default spec-id is used, and there are no corresponding entries
    # in partition_specs
    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 0
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 0
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 1
    )

    # now, add a spec, and make sure it is pushed
    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (ADD partition_by 'key')",
        pg_conn,
    )
    pg_conn.commit()

    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 1
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 1
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 2
    )

    # now, drop the spec, and make sure it is pushed
    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (DROP partition_by)",
        pg_conn,
    )
    pg_conn.commit()

    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 0
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 0
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 2
    )

    # now, add another partition spec
    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (ADD partition_by 'bucket(10, key)')",
        pg_conn,
    )
    pg_conn.commit()

    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 2
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 2
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 3
    )

    # setting back to existing one should not push a new id
    # now, add a spec, and make sure it is pushed
    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (SET partition_by 'key')",
        pg_conn,
    )
    pg_conn.commit()

    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 1
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 1
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 3
    )

    # a spec with two fields swapped are considered separate specs
    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (SET partition_by 'truncate(100, key), bucket(50, value)')",
        pg_conn,
    )
    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (SET partition_by 'bucket(50, value), truncate(100, key)')",
        pg_conn,
    )
    pg_conn.commit()

    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 4
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 4
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 5
    )

    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (SET partition_by 'truncate(100, key), bucket(50, value)')",
        pg_conn,
    )
    pg_conn.commit()

    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 3
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 3
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 5
    )

    # finally, drop the spec and finish the test
    run_command(
        f"ALTER TABLE test_re_set_partition_fields.tbl OPTIONS (DROP partition_by)",
        pg_conn,
    )
    pg_conn.commit()

    default_spec_id = run_query(
        "SELECT default_spec_id FROM lake_iceberg.tables_internal WHERE table_name = 'test_re_set_partition_fields.tbl'::regclass",
        superuser_conn,
    )
    assert default_spec_id[0][0] == 0
    assert (
        default_table_partition_spec_id(pg_conn, "test_re_set_partition_fields", "tbl")
        == 0
    )
    assert (
        len(table_partition_specs(pg_conn, "test_re_set_partition_fields", "tbl")) == 5
    )

    run_command(f"""DROP SCHEMA test_re_set_partition_fields CASCADE;""", pg_conn)
    pg_conn.commit()


def test_ddl_with_identity_partitions(extension, s3, with_default_location, pg_conn):
    run_command(f"""CREATE SCHEMA test_ddl_with_partitions;""", pg_conn)

    run_command(
        "CREATE TABLE test_ddl_with_partitions.tbl USING iceberg WITH (partition_by=i) AS SELECT i FROM generate_series(0,1)i",
        pg_conn,
    )
    pg_conn.commit()

    # cannot drop current partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl DROP COLUMN i",
        pg_conn,
        raise_error=False,
    )
    assert "cannot drop column" in str(res)
    pg_conn.rollback()

    # cannot rename current partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl RENAME COLUMN i TO i_new",
        pg_conn,
        raise_error=False,
    )
    assert "cannot rename column" in str(res)
    pg_conn.rollback()

    # now, add column, change partition_by to only have this new column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl ADD COLUMN j INT;", pg_conn
    )
    run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl OPTIONS(SET partition_by 'j')",
        pg_conn,
    )
    pg_conn.commit()

    # cannot drop old partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl DROP COLUMN i",
        pg_conn,
        raise_error=False,
    )
    assert "cannot drop column" in str(res)
    pg_conn.rollback()

    # cannot drop current partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl DROP COLUMN j",
        pg_conn,
        raise_error=False,
    )
    assert "cannot drop column" in str(res)
    pg_conn.rollback()

    run_command(f"""DROP SCHEMA test_ddl_with_partitions CASCADE;""", pg_conn)
    pg_conn.commit()


# similar to test_ddl_with_identity_partitions but for truncate
# partitioning for completeness
def test_ddl_with_truncate_partitions(
    extension,
    s3,
    with_default_location,
    pg_conn,
):
    run_command(f"""CREATE SCHEMA test_ddl_with_partitions;""", pg_conn)

    run_command(
        "CREATE TABLE test_ddl_with_partitions.tbl USING iceberg WITH (partition_by='truncate(10,i)') AS SELECT i FROM generate_series(0,1)i",
        pg_conn,
    )
    pg_conn.commit()

    # cannot drop old partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl DROP COLUMN i",
        pg_conn,
        raise_error=False,
    )
    assert "cannot drop column" in str(res)
    pg_conn.rollback()

    # cannot rename old partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl RENAME COLUMN i TO i_new",
        pg_conn,
        raise_error=False,
    )
    assert "cannot rename column" in str(res)
    pg_conn.rollback()

    # now, add column, change partition_by to only have this new column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl ADD COLUMN j INT;", pg_conn
    )
    run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl OPTIONS(SET partition_by 'truncate(10,j)')",
        pg_conn,
    )
    pg_conn.commit()

    # cannot drop old partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl DROP COLUMN i",
        pg_conn,
        raise_error=False,
    )
    assert "cannot drop column" in str(res)
    pg_conn.rollback()

    # cannot drop current partition column
    res = run_command(
        "ALTER FOREIGN TABLE test_ddl_with_partitions.tbl DROP COLUMN j",
        pg_conn,
        raise_error=False,
    )
    assert "cannot drop column" in str(res)
    pg_conn.rollback()

    run_command(f"""DROP SCHEMA test_ddl_with_partitions CASCADE;""", pg_conn)
    pg_conn.commit()


# copy-on write on identity partition preserves the
# partition boundaries
def test_updates_keep_identity_partition_value(
    extension, s3, with_default_location, pg_conn, grant_access_to_data_file_partition
):
    run_command("CREATE SCHEMA test_update_identity;", pg_conn)

    # Two files: p = 0 (10 rows) and p = 1 (10 rows)
    run_command(
        """
        CREATE TABLE test_update_identity.tbl
        USING iceberg
        WITH (partition_by = 'p', autovacuum_enabled = false)
        AS
        SELECT *
        FROM (
            SELECT 0 AS p, gs AS id, 0 AS v FROM generate_series(1,10) gs  -- file #1
            UNION ALL
            SELECT 1 AS p, gs AS id, 0 AS v FROM generate_series(1,10) gs  -- file #2
        ) s;
        """,
        pg_conn,
    )
    vals = run_query(
        """
        SELECT count(*)
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_identity.tbl'::regclass
        """,
        pg_conn,
    )
    assert vals == [[2]]

    # ── update % of rows in the file where p = 0 ─────────────────────────
    run_command(
        """
        DELETE FROM test_update_identity.tbl
        WHERE  p = 0 AND id <= 5;   -- 5 of the 10 rows
        """,
        pg_conn,
    )

    vals = run_query(
        """
        SELECT DISTINCT value
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_identity.tbl'::regclass
        ORDER BY value;
        """,
        pg_conn,
    )
    assert vals == [["0"], ["1"]]

    vals = run_query(
        """
        SELECT count(*)
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_identity.tbl'::regclass
        """,
        pg_conn,
    )
    assert vals == [[2]]

    # ── update 50% of rows in the file where p = 1 ─────────────────────────
    run_command(
        """
        DELETE FROM test_update_identity.tbl
        WHERE  p = 1 AND id <= 5;
        """,
        pg_conn,
    )

    vals = run_query(
        """
        SELECT DISTINCT value
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_identity.tbl'::regclass
        ORDER BY value;
        """,
        pg_conn,
    )
    assert vals == [["0"], ["1"]]
    vals = run_query(
        """
        SELECT count(*)
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_identity.tbl'::regclass
        """,
        pg_conn,
    )
    assert vals == [[2]]
    run_command("DROP SCHEMA test_update_identity CASCADE;", pg_conn)


# copy-on write on truncate partition preserves the
# partition boundaries
def test_updates_keep_truncate_partition_value(
    extension, s3, with_default_location, pg_conn, grant_access_to_data_file_partition
):
    run_command("CREATE SCHEMA test_update_truncate;", pg_conn)

    # Two files: p 0‑9  → partition value 0,   p 10‑19 → partition value 10
    run_command(
        """
        CREATE TABLE test_update_truncate.tbl
        USING iceberg
        WITH (partition_by = 'truncate(10,p)', autovacuum_enabled = false)
        AS
        SELECT *
        FROM (
            SELECT i        AS p,
                   i + 1    AS id,   -- id = 1‑20 (unique)
                   0        AS v
            FROM generate_series(0,19) i
        ) s;
        """,
        pg_conn,
    )

    # ── update 50% of rows in partition value 0 (p 0‑9) ───────────────────
    run_command(
        """
        DELETE FROM test_update_truncate.tbl
        WHERE  p BETWEEN 0 AND 9
          AND  id <= 5;          -- 5 of the 10 rows
        """,
        pg_conn,
    )

    vals = run_query(
        """
        SELECT DISTINCT value
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_truncate.tbl'::regclass
        ORDER BY value;
        """,
        pg_conn,
    )
    assert vals == [["0"], ["10"]]

    vals = run_query(
        """
        SELECT count(*)
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_truncate.tbl'::regclass
        """,
        pg_conn,
    )
    assert vals == [[2]]

    # ── update % of rows in partition value 10 (p 10‑19) ────────────────
    run_command(
        """
        DELETE FROM test_update_truncate.tbl
        WHERE  p BETWEEN 10 AND 19
          AND  id <= 15;         -- ids 11‑15 → 5 rows
        """,
        pg_conn,
    )

    vals = run_query(
        """
        SELECT DISTINCT value
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_truncate.tbl'::regclass
        ORDER BY value;
        """,
        pg_conn,
    )
    assert vals == [["0"], ["10"]]

    vals = run_query(
        """
        SELECT count(*)
        FROM lake_table.data_file_partition_values
        WHERE table_name = 'test_update_truncate.tbl'::regclass
        """,
        pg_conn,
    )
    assert vals == [[2]]

    run_command("DROP SCHEMA test_update_truncate CASCADE;", pg_conn)


def test_basic_dml_with_mixed_partitions(
    extension,
    s3,
    with_default_location,
    pg_conn,
):
    run_command("CREATE SCHEMA test_mixed_parts;", pg_conn)

    # ── create & seed ───────────────────────────────────────────────────────
    run_command(
        """
        CREATE TABLE test_mixed_parts.tbl
        USING iceberg
        WITH (partition_by = 'id,truncate(10,val),year(ts)')
        AS
        SELECT
            gs                          AS id,       -- identity partition
            gs * 10                     AS val,      -- will be truncate(10,val)
            ('2025-01-01'::date
             + (gs - 1))::date   AS ts        -- will be year(ts)
        FROM generate_series(1,10) gs;
        """,
        pg_conn,
    )

    # helper for compact reads
    q = lambda: run_query(
        "SELECT id, val FROM test_mixed_parts.tbl ORDER BY id;", pg_conn
    )

    # check initial data
    assert q() == [[i, i * 10] for i in range(1, 11)]

    # ── delete 1 row ────────────────────────────────────────────────────────
    run_command("DELETE FROM test_mixed_parts.tbl WHERE id = 1;", pg_conn)
    assert q() == [[i, i * 10] for i in range(2, 11)]

    # ── update 1 row (id = 2) ──────────────────────────────────────────────
    run_command("UPDATE test_mixed_parts.tbl SET val = 999 WHERE id = 2;", pg_conn)
    assert q()[0] == [2, 999]  # first row is id = 2

    # ── delete half of the rows (ids ≥ 6) ──────────────────────────────────
    run_command("DELETE FROM test_mixed_parts.tbl WHERE id >= 6;", pg_conn)
    assert q() == [[2, 999], [3, 30], [4, 40], [5, 50]]

    # ── update half of what remains (even ids) ─────────────────────────────
    run_command(
        """
        UPDATE test_mixed_parts.tbl
        SET    val = val + 1000
        WHERE  id % 2 = 0;
        """,
        pg_conn,
    )
    assert q() == [[2, 1999], [3, 30], [4, 1040], [5, 50]]

    # ── truncate the table ─────────────────────────────────────────────────
    run_command("TRUNCATE TABLE test_mixed_parts.tbl;", pg_conn)
    assert q() == []

    run_command("DROP SCHEMA test_mixed_parts CASCADE;", pg_conn)


def test_drop_table_cleans_partition_values(
    extension, s3, with_default_location, pg_conn, grant_access_to_data_file_partition
):
    run_command("CREATE SCHEMA test_drop_parts;", pg_conn)

    # 1) create & seed a trivially partitioned table (one data file)
    run_command(
        """
        CREATE TABLE test_drop_parts.tbl
        USING iceberg
        WITH (partition_by = 'p')
        AS
        SELECT generate_series(1,5) AS p;
        """,
        pg_conn,
    )

    # capture the table's OID now—after DROP we can’t use ::regclass
    tbl_oid = run_query("SELECT 'test_drop_parts.tbl'::regclass::oid;", pg_conn)[0][0]

    # 2) metadata row(s) must exist
    rows_before = run_query(
        """
        SELECT count(*)
        FROM   lake_table.data_file_partition_values
        WHERE  table_name = 'test_drop_parts.tbl'::regclass;
        """,
        pg_conn,
    )[0][0]
    assert rows_before > 0

    # 3) drop the table
    run_command("DROP TABLE test_drop_parts.tbl;", pg_conn)

    # 4) rows for that OID should be gone
    rows_after = run_query(
        f"""
        SELECT count(*)
        FROM   lake_table.data_file_partition_values
        WHERE  table_name = {tbl_oid}::oid;
        """,
        pg_conn,
    )[0][0]
    assert rows_after == 0

    run_command("DROP SCHEMA test_drop_parts CASCADE;", pg_conn)


def test_partition_by_with_collation(extension, s3, with_default_location, pg_conn):
    run_command('CREATE COLLATION s_coll (LOCALE="C");', pg_conn)
    pg_conn.commit()

    run_command(f"""CREATE SCHEMA test_partition_by_with_collation;""", pg_conn)
    pg_conn.commit()

    res = run_command(
        "CREATE TABLE test_partition_by_with_collation.tbl (name text COLLATE s_coll) USING iceberg WITH (partition_by='truncate(10, name)');",
        pg_conn,
        raise_error=False,
    )
    pg_conn.rollback()
    assert "Columns with collation are not supported in partition_by" in str(res)

    pg_conn.rollback()

    run_command(
        "CREATE TABLE test_partition_by_with_collation.tbl (name text COLLATE s_coll, detail text) USING iceberg WITH (partition_by='truncate(10, detail)');",
        pg_conn,
    )

    res = run_command(
        "ALTER FOREIGN TABLE test_partition_by_with_collation.tbl OPTIONS (SET partition_by 'truncate(10, name)');",
        pg_conn,
        raise_error=False,
    )
    pg_conn.commit()
    assert "Columns with collation are not supported in partition_by" in str(res)

    pg_conn.rollback()

    run_command("DROP COLLATION s_coll;", pg_conn)
    pg_conn.commit()


def table_metadata(pg_conn, table_namespace, table_name):
    metadata_location = run_query(
        f"SELECT metadata_location FROM iceberg_tables WHERE table_name = '{table_name}' and table_namespace = '{table_namespace}'",
        pg_conn,
    )[0][0]

    pg_query = f"SELECT * FROM lake_iceberg.metadata('{metadata_location}')"

    metadata = run_query(pg_query, pg_conn)[0][0]

    return metadata


def table_partition_specs(pg_conn, table_namespace, table_name):
    metadata = table_metadata(pg_conn, table_namespace, table_name)
    return metadata["partition-specs"]


def default_table_partition_spec_id(pg_conn, table_namespace, table_name):
    metadata = table_metadata(pg_conn, table_namespace, table_name)
    return metadata["default-spec-id"]
