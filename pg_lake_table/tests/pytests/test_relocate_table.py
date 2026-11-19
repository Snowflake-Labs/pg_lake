import uuid

from utils_pytest import *


def test_relocate_table_rebuilds_visible_rows(
    s3, pg_conn, superuser_conn, extension, with_default_location
):
    table_name = f"test_relocate_table_{uuid.uuid4().hex}"
    qualified_table_name = f"public.{table_name}"
    location_prefix = run_query(
        "SHOW pg_lake_iceberg.default_location_prefix", pg_conn
    )[0][0].rstrip("/")
    new_location = f"{location_prefix}/relocated/{uuid.uuid4().hex}"

    try:
        run_command(
            f"CREATE TABLE {qualified_table_name} (id int) USING pg_lake_iceberg",
            pg_conn,
        )
        run_command(
            f"INSERT INTO {qualified_table_name} " "SELECT generate_series(1, 100)",
            pg_conn,
        )
        pg_conn.commit()

        run_command(f"DELETE FROM {qualified_table_name} WHERE id = 1", pg_conn)
        pg_conn.commit()

        source_metadata_location = run_query(
            "SELECT metadata_location FROM lake_iceberg.tables_internal "
            f"WHERE table_name = '{qualified_table_name}'::regclass",
            superuser_conn,
        )[0][0]
        source_position_delete_count = run_query(
            "SELECT count(*) "
            f"FROM lake_iceberg.files('{source_metadata_location}') "
            "WHERE content = 'POSITION_DELETES'",
            superuser_conn,
        )[0][0]
        assert source_position_delete_count > 0

        result = run_query(
            "SELECT lake_iceberg.relocate_table("
            f"'{qualified_table_name}'::regclass, '{new_location}')",
            pg_conn,
        )[0][0]
        assert result == new_location
        pg_conn.commit()

        rows = run_query(f"SELECT id FROM {qualified_table_name} ORDER BY id", pg_conn)
        assert [row[0] for row in rows] == list(range(2, 101))

        destination_metadata_location = run_query(
            "SELECT metadata_location FROM lake_iceberg.tables_internal "
            f"WHERE table_name = '{qualified_table_name}'::regclass",
            superuser_conn,
        )[0][0]
        destination_position_delete_count = run_query(
            "SELECT count(*) "
            f"FROM lake_iceberg.files('{destination_metadata_location}') "
            "WHERE content = 'POSITION_DELETES'",
            superuser_conn,
        )[0][0]
        assert destination_position_delete_count == 0

        destination_location = run_query(
            "SELECT option_value "
            "FROM pg_catalog.pg_options_to_table(("
            "SELECT ftoptions FROM pg_catalog.pg_foreign_table "
            f"WHERE ftrelid = '{qualified_table_name}'::regclass)) "
            "WHERE option_name = 'location'",
            pg_conn,
        )[0][0]
        assert destination_location == new_location
    finally:
        pg_conn.rollback()
        run_command(f"DROP TABLE IF EXISTS {qualified_table_name}", pg_conn)
        pg_conn.commit()


TABLE_PROPERTIES_QUERY = """
SELECT c.relacl::text, c.relowner::regrole::text,
       pg_catalog.obj_description(c.oid, 'pg_class'),
       (SELECT array_agg(ARRAY[a.attname::text, a.attnotnull::text,
                               a.attgenerated::text,
                               coalesce(pg_get_expr(d.adbin, d.adrelid), ''),
                               coalesce(a.attacl::text, ''),
                               coalesce(col_description(c.oid, a.attnum), '')]
                         ORDER BY a.attnum)
        FROM pg_attribute a
        LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
        WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped)::text,
       (SELECT array_agg(option_name || '=' || option_value ORDER BY option_name)
        FROM pg_options_to_table(f.ftoptions)
        WHERE option_name <> 'location')::text,
       (SELECT array_agg(s.objid::regclass::text)
        FROM pg_depend s
        WHERE s.refobjid = c.oid AND s.deptype = 'a'
          AND s.classid = 'pg_class'::regclass)::text,
       (SELECT array_agg(ARRAY[s.objsubid::text, s.refobjid::regrole::text,
                               s.deptype::text]
                         ORDER BY s.objsubid, s.refobjid::regrole::text)
        FROM pg_shdepend s
        WHERE s.objid = c.oid AND s.classid = 'pg_class'::regclass)::text
FROM pg_class c
JOIN pg_foreign_table f ON f.ftrelid = c.oid
WHERE c.oid = '{table_name}'::regclass
"""


def test_relocate_table_preserves_table_properties(
    s3, pg_conn, superuser_conn, extension, with_default_location
):
    table_name = f"public.test_relocate_props_{uuid.uuid4().hex}"
    role_name = f"test_relocate_reader_{uuid.uuid4().hex[:8]}"
    location_prefix = run_query(
        "SHOW pg_lake_iceberg.default_location_prefix", pg_conn
    )[0][0].rstrip("/")
    new_location = f"{location_prefix}/relocated/{uuid.uuid4().hex}"

    run_command(f"CREATE ROLE {role_name}", superuser_conn)
    superuser_conn.commit()

    try:
        run_command(
            f"""
            CREATE TABLE {table_name} (
                id bigserial,
                k int NOT NULL,
                v text DEFAULT 'x',
                g int GENERATED ALWAYS AS (k * 2) STORED
            ) USING pg_lake_iceberg WITH (partition_by = 'k');
            COMMENT ON FOREIGN TABLE {table_name} IS 'table comment';
            COMMENT ON COLUMN {table_name}.k IS 'column comment';
            GRANT SELECT ON {table_name} TO {role_name};
            GRANT UPDATE (v) ON {table_name} TO {role_name};
            INSERT INTO {table_name} (k) VALUES (1), (2);
            """,
            pg_conn,
        )
        pg_conn.commit()

        properties_query = TABLE_PROPERTIES_QUERY.format(table_name=table_name)
        properties_before = run_query(properties_query, superuser_conn)

        run_query(
            f"SELECT lake_iceberg.relocate_table('{table_name}', '{new_location}')",
            pg_conn,
        )
        pg_conn.commit()

        assert run_query(properties_query, superuser_conn) == properties_before

        # the serial sequence moved to the new table and keeps counting
        run_command(f"INSERT INTO {table_name} (k) VALUES (3)", pg_conn)
        pg_conn.commit()
        rows = run_query(f"SELECT id, k, v, g FROM {table_name} ORDER BY id", pg_conn)
        assert rows == [[1, 1, "x", 2], [2, 2, "x", 4], [3, 3, "x", 6]]
    finally:
        pg_conn.rollback()
        run_command(f"DROP TABLE IF EXISTS {table_name}", pg_conn)
        pg_conn.commit()
        run_command(f"DROP ROLE {role_name}", superuser_conn)
        superuser_conn.commit()


def test_relocate_table_rejects_triggers(s3, pg_conn, extension, with_default_location):
    table_name = f"public.test_relocate_trigger_{uuid.uuid4().hex}"
    function_name = f"public.test_relocate_trigger_fn_{uuid.uuid4().hex}"
    location_prefix = run_query(
        "SHOW pg_lake_iceberg.default_location_prefix", pg_conn
    )[0][0].rstrip("/")

    try:
        run_command(
            f"""
            CREATE TABLE {table_name} (id int) USING pg_lake_iceberg;
            CREATE FUNCTION {function_name}() RETURNS trigger
                LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
            CREATE TRIGGER relocate_trigger BEFORE INSERT ON {table_name}
                FOR EACH ROW EXECUTE FUNCTION {function_name}();
            """,
            pg_conn,
        )
        pg_conn.commit()

        error = run_query(
            f"SELECT lake_iceberg.relocate_table('{table_name}', "
            f"'{location_prefix}/relocated/{uuid.uuid4().hex}')",
            pg_conn,
            raise_error=False,
        )
        assert "relocate_table does not support tables with triggers" in str(error)
    finally:
        pg_conn.rollback()
        run_command(f"DROP TABLE IF EXISTS {table_name}", pg_conn)
        run_command(f"DROP FUNCTION IF EXISTS {function_name}()", pg_conn)
        pg_conn.commit()
