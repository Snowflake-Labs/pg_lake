import os
import pytest
import re
from collections import namedtuple
from utils_pytest import *


@pytest.fixture(scope="function")
def permissions_for_data_files(superuser_conn, app_user):
    run_command(
        f"""
        GRANT SELECT, INSERT, DELETE ON lake_table.files TO {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    yield

    run_command(
        f"""
        REVOKE SELECT, INSERT, DELETE ON lake_table.files FROM {app_user};
    """,
        superuser_conn,
    )
    superuser_conn.commit()


def test_foreign_table_types(pg_conn, s3, extension, app_user):
    url = f"s3://{TEST_BUCKET}/test_writable_table_type_verification/data.parquet"

    create_test_types(pg_conn, app_user)

    run_command(
        f"""
    COPY test_types TO '{url}';
    CREATE FOREIGN TABLE test_writable_table_type_verification ()
        SERVER pg_lake OPTIONS (path '{url}');
    """,
        pg_conn,
    )

    pg_conn.rollback()


def test_invalid_syntax(s3, pg_conn, extension):
    location = f"s3://{TEST_BUCKET}/test_writable_table/"

    # Writable without format
    error = run_command(
        f"""
        CREATE FOREIGN TABLE test_invalid_syntax (id int, value text) SERVER pg_lake OPTIONS (writable 'true', location '{location}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert '"format" option is required' in error

    pg_conn.rollback()

    # Empty table syntax is not supported
    error = run_command(
        f"""
        CREATE FOREIGN TABLE test_invalid_syntax () SERVER pg_lake OPTIONS (writable 'true', format 'parquet', location '{location}');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "column list cannot be empty" in error

    pg_conn.rollback()

    # Writable tables not supported with 'filename' option
    error = run_command(
        f"""
        CREATE FOREIGN TABLE test_invalid_syntax (id int, value text) SERVER pg_lake OPTIONS (writable 'true', format 'parquet', location '{location}', filename 'true');
    """,
        pg_conn,
        raise_error=False,
    )
    assert '"filename" option is not allowed for writable' in error

    pg_conn.rollback()


def test_add_remove_file(s3, pg_conn, extension, permissions_for_data_files):
    location = f"s3://{TEST_BUCKET}/test_add_file/"
    url1 = f"s3://{TEST_BUCKET}/test_add_file/data1.parquet"
    url2 = f"s3://{TEST_BUCKET}/test_add_file/data2.parquet"

    run_command(
        f"""
        COPY (SELECT generate_series, 'hello' FROM generate_series(1,10)) TO '{url1}';
        COPY (SELECT generate_series, 'world' FROM generate_series(1,10)) TO '{url2}';
        CREATE FOREIGN TABLE test_catalog (id int, value text) SERVER pg_lake OPTIONS (writable 'true', format 'parquet', location '{location}');
    """,
        pg_conn,
    )

    # Test empty table
    result = run_query("SELECT * FROM test_catalog", pg_conn)
    assert len(result) == 0

    # Test table with a single file
    run_command(
        f"""
        INSERT INTO lake_table.files (table_name, path, file_size, row_count, deleted_row_count)
        VALUES ('test_catalog', '{url1}', lake_file.size('{url1}'), 10, 0);
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_catalog", pg_conn)
    assert len(result) == 10

    # Add savepoint
    run_command(
        f"""
        SAVEPOINT single;
    """,
        pg_conn,
    )

    # Add another file
    run_command(
        f"""
        INSERT INTO lake_table.files (table_name, path, file_size, row_count, deleted_row_count)
        VALUES ('test_catalog', '{url2}', lake_file.size('{url2}'), 10, 0);
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_catalog", pg_conn)
    assert len(result) == 20

    # Roll back to savepoint
    run_command(
        f"""
        ROLLBACK TO SAVEPOINT single;
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_catalog", pg_conn)
    assert len(result) == 10

    # Remove file
    run_command(
        f"""
        DELETE FROM lake_table.files WHERE table_name = 'test_catalog'::regclass;
    """,
        pg_conn,
    )

    result = run_query("SELECT * FROM test_catalog", pg_conn)
    assert len(result) == 0

    pg_conn.rollback()


def get_table_options(table_type):
    if table_type == "pg_lake":
        return "writable 'true', format 'parquet',"
    elif table_type == "pg_lake_iceberg":
        return ""  # No options for pg_lake_iceberg


def test_insert(azure, pg_conn, duckdb_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_insert(azure, pg_conn, duckdb_conn, extension, table_type)


def internal_test_insert(azure, pg_conn, duckdb_conn, extension, table_type):
    location = f"az://{TEST_BUCKET}/internal_test_insert_{table_type}/"

    run_command(
        f"""
        CREATE TYPE lake_struct.point as (x int, y int);

        CREATE FOREIGN TABLE test_insert (
            id int not null,
            value text default 'D' collate "C",
            ser bigserial,
            gida bigint generated always as identity,
            gidd bigint generated by default as identity,
            id2 int generated always as (id * 2) stored,
            bytes bytea default '\\xdeadbeef',
            point lake_struct.point default (1,2)
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
        """,
        pg_conn,
    )

    run_command(
        """
    INSERT INTO test_insert VALUES (1, 'hello');
    INSERT INTO test_insert (gidd, id) VALUES (0, 2);
    INSERT INTO test_insert (value, id) VALUES ('world', 3), ('globe', 4);
    """,
        pg_conn,
    )

    # Register composite type with psycopg2
    point = namedtuple("point", ["x", "y"])
    psycopg2.extras.register_composite("lake_struct.point", pg_conn)

    # Check generated values
    result = run_query("SELECT * FROM test_insert WHERE id = 2", pg_conn)
    assert result[0]["value"] == "D"
    assert result[0]["ser"] == 2
    assert result[0]["gida"] == 2
    assert result[0]["gidd"] == 0
    assert result[0]["id2"] == 4
    assert bytes(result[0]["bytes"]) == b"\xde\xad\xbe\xef"
    assert result[0]["point"] == point(x=1, y=2)

    # Confirm multi-row insert went through
    result = run_query(
        "SELECT * FROM test_insert WHERE value IN ('world','globe') ORDER BY id",
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["gida"] == 3
    assert result[0]["gidd"] == 2

    # Check number of files
    result = run_query(
        "SELECT count(*), sum(row_count), min(file_size) file_size FROM lake_table.files WHERE table_name = 'test_insert'::regclass",
        pg_conn,
    )
    assert result[0]["count"] == 3
    assert result[0]["sum"] == 4
    assert result[0]["file_size"] > 0

    # Check that they're actually parquet
    if table_type == "pg_lake":
        duckdb_conn.execute(
            "SELECT count(*) AS count FROM read_parquet($1)", [str(location) + "**"]
        )
        duckdb_result = duckdb_conn.fetchall()
        assert duckdb_result[0][0] == 4
    elif table_type == "pg_lake_iceberg":
        duckdb_conn.execute(
            "SELECT count(*) AS count FROM read_parquet($1)",
            [str(location + "data/") + "**"],
        )
        duckdb_result = duckdb_conn.fetchall()
        assert duckdb_result[0][0] == 4

    # Do batch insert
    run_command(
        f"""
        INSERT INTO test_insert (value, id) SELECT 'hello-'||s, s FROM generate_series(1,96) s;
    """,
        pg_conn,
    )

    # Check row count
    result = run_query("SELECT count(*) FROM test_insert", pg_conn)
    assert result[0]["count"] == 100

    # Do copy from
    run_command(
        f"""
        COPY (SELECT 'hello-'||s, s FROM generate_series(1001,1005) s) TO '{location}input.parquet';
        COPY test_insert (value, id) FROM '{location}input.parquet';
    """,
        pg_conn,
    )

    # Check row count
    result = run_query("SELECT count(*) FROM test_insert", pg_conn)
    assert result[0]["count"] == 105

    # Check generated values
    result = run_query("SELECT * FROM test_insert WHERE id = 1001", pg_conn)
    assert result[0]["value"] == "hello-1001"
    assert result[0]["ser"] == 101
    assert result[0]["gida"] == 101
    assert result[0]["gidd"] == 100
    assert result[0]["id2"] == 2002

    # Check returning
    result = run_query(
        """
        INSERT INTO test_insert VALUES (2001, 'hello-2001'), (2002, 'bye')
        RETURNING id, gida
    """,
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["gida"] == 106
    assert result[1]["gida"] == 107

    # Check that metadata is cleaned up when dropping the table
    result = run_query(f"SELECT 'test_insert'::regclass::oid test_insert_oid", pg_conn)
    test_insert_oid = result[0]["test_insert_oid"]
    print(test_insert_oid)
    run_command("DROP FOREIGN TABLE test_insert", pg_conn)

    result = run_query(
        f"SELECT count(*) FROM lake_table.files WHERE table_name = {test_insert_oid}",
        pg_conn,
    )
    assert result[0]["count"] == 0

    pg_conn.rollback()


def test_insert_azure_long_prefix(azure, pg_conn, duckdb_conn, extension):
    """Test that azure:// prefix (long form) works the same as az:// for writable tables"""
    location = f"azure://{TEST_BUCKET}/test_insert_azure_long_prefix/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_insert_azure_long (
            id int not null,
            value text
        )
        SERVER pg_lake
        OPTIONS (format 'parquet', writable 'true', location '{location}');
        """,
        pg_conn,
    )

    run_command(
        """
        INSERT INTO test_insert_azure_long VALUES (1, 'hello'), (2, 'world');
    """,
        pg_conn,
    )

    # Check row count
    result = run_query("SELECT count(*) FROM test_insert_azure_long", pg_conn)
    assert result[0]["count"] == 2

    # Check data
    result = run_query("SELECT * FROM test_insert_azure_long ORDER BY id", pg_conn)
    assert len(result) == 2
    assert result[0]["value"] == "hello"
    assert result[1]["value"] == "world"

    # Verify files exist
    result = run_query(
        "SELECT count(*) FROM lake_table.files WHERE table_name = 'test_insert_azure_long'::regclass",
        pg_conn,
    )
    assert result[0]["count"] > 0

    pg_conn.rollback()


def test_update(s3, pg_conn, duckdb_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_update(s3, pg_conn, duckdb_conn, extension, table_type)


def internal_test_update(s3, pg_conn, duckdb_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/test_update/internal_test_update_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_update (
            id int not null,
            value text default 'D' collate "C",
            ser bigserial,
            gida bigint generated always as identity,
            gidd bigint generated by default as identity,
            id2 int generated always as (id * 2) stored
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
        """,
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_update VALUES (1, 'hello');
        INSERT INTO test_update (gidd, id) VALUES (0, 2);
        INSERT INTO test_update (value, id) VALUES ('world', 3), ('globe', 4);
        UPDATE test_update SET value = 'earth', id = 5 WHERE id = 2;
    """,
        pg_conn,
    )

    # Check generated values
    result = run_query("SELECT * FROM test_update WHERE id = 5", pg_conn)
    assert result[0]["id"] == 5
    assert result[0]["id2"] == 10
    assert result[0]["value"] == "earth"

    # Check number of files
    result = run_query(
        """
         SELECT count(*), sum(row_count), min(file_size) file_size
         FROM lake_table.files WHERE table_name = 'test_update'::regclass
    """,
        pg_conn,
    )
    assert result[0]["count"] == 3
    assert result[0]["sum"] == 4
    assert result[0]["file_size"] > 0

    # Check returning
    result = run_query(
        """
        UPDATE test_update
        SET value = 'updated'
        WHERE id = 4
        RETURNING id * 3 AS id3, value
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["id3"] == 12
    assert result[0]["value"] == "updated"

    pg_conn.rollback()


def test_delete(s3, pg_conn, duckdb_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_delete(s3, pg_conn, duckdb_conn, extension, table_type)


def internal_test_delete(s3, pg_conn, duckdb_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_delete_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_delete (
            id int not null,
            value text default 'D' collate "C",
            ser bigserial,
            gida bigint generated always as identity,
            gidd bigint generated by default as identity,
            id2 int generated always as (id * 2) stored
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
        """,
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_delete VALUES (1, 'hello');
        INSERT INTO test_delete (gidd, id) VALUES (0, 2);
        INSERT INTO test_delete (value, id) VALUES ('world', 3), ('globe', 4);
    """,
        pg_conn,
    )

    # Delete file partially
    result = run_query(
        "DELETE FROM test_delete WHERE id IN (3) RETURNING id, id2, value", pg_conn
    )
    assert result[0]["id"] == 3
    assert result[0]["id2"] == 6
    assert result[0]["value"] == "world"

    # Check number of files
    result = run_query(
        """
        SELECT count(*), sum(row_count), min(file_size) file_size
        FROM lake_table.files WHERE table_name = 'test_delete'::regclass
    """,
        pg_conn,
    )
    assert result[0]["count"] == 3
    assert result[0]["sum"] == 3
    assert result[0]["file_size"] > 0

    # Delete files fully
    run_command("DELETE FROM test_delete WHERE id IN (2,1)", pg_conn)

    # Check number of files
    result = run_query(
        "SELECT count(*), sum(row_count) FROM lake_table.files WHERE table_name = 'test_delete'::regclass",
        pg_conn,
    )
    assert result[0]["count"] == 1
    assert result[0]["sum"] == 1

    pg_conn.rollback()


def test_parquet_versions(superuser_conn, pg_conn, s3, duckdb_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_parquet_versions(pg_conn, s3, extension, table_type)


def internal_test_parquet_versions(pg_conn, s3, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_parquet_versions_{table_type}"

    run_command(
        f"""
        CREATE FOREIGN TABLE "test-parquet-versions" (
            a int, b float8, c text, d bytea
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
    """,
        pg_conn,
    )

    # insert with parquet v1 (default)
    run_command(
        'INSERT INTO "test-parquet-versions" SELECT i, i, md5(i::text), md5(i::text)::bytea FROM generate_series(1, 10000) i',
        pg_conn,
    )

    # now switch to parquet v2
    run_command("SET pg_lake_table.default_parquet_version = v2;", pg_conn)

    # insert with parquet v2
    run_command(
        'INSERT INTO "test-parquet-versions" SELECT i, i, md5(i::text), md5(i::text)::bytea FROM generate_series(1, 10000) i',
        pg_conn,
    )

    run_command("RESET pg_lake_table.default_parquet_version;", pg_conn)

    # sanity check
    result = run_query('SELECT count(*) FROM "test-parquet-versions"', pg_conn)
    assert result[0][0] == 20000

    pg_conn.rollback()


def test_parquet_v2(pg_conn, superuser_conn, pgduck_conn, tmp_path, extension):
    parquet_path = tmp_path / "test.parquet"

    # parquet v1 (default)
    run_command(
        f"""
        COPY (SELECT i::float8 FROM generate_series(1,10000) i) TO '{parquet_path}' WITH (format 'parquet');
        """,
        pg_conn,
    )
    encoding = run_query(
        f"select encodings FROM parquet_metadata('{parquet_path}')", pgduck_conn
    )[0][0]
    assert encoding == "PLAIN"

    # parquet v2
    run_command(
        f"""
        SET pg_lake_table.default_parquet_version = v2;
        COPY (SELECT i::float8 FROM generate_series(1,10000) i) TO '{parquet_path}' WITH (format 'parquet');
        """,
        pg_conn,
    )
    encoding = run_query(
        f"select encodings FROM parquet_metadata('{parquet_path}')", pgduck_conn
    )[0][0]
    assert encoding == "BYTE_STREAM_SPLIT"

    run_command(
        f"""
        CREATE TABLE test_parquet_v2 (a float8);
        COPY test_parquet_v2 FROM '{parquet_path}' WITH (format 'parquet');
        """,
        pg_conn,
    )

    result = run_query("SELECT COUNT(*) FROM test_parquet_v2", pg_conn)[0][0]
    assert result == 10000

    pg_conn.rollback()


# Use superuser to allow changing pg_lake_table.max_write_temp_file_size_mb
def test_limited_temp_file_size(s3, superuser_conn, duckdb_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_limited_temp_file_size(superuser_conn, s3, extension, table_type)


def internal_limited_temp_file_size(pg_conn, s3, extension, table_type):
    url = f"s3://{TEST_BUCKET}/test_temp_file_size/input.parquet"
    location = f"s3://{TEST_BUCKET}/internal_limited_temp_file_size_{table_type}"

    run_command(
        f"""
        CREATE FOREIGN TABLE temp_file_size (
            id int, value text
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
    """,
        pg_conn,
    )

    # Insert split across CSV files
    run_command(
        f"""
        SET pg_lake_table.max_write_temp_file_size_mb TO '3MB';

        INSERT INTO temp_file_size
        SELECT s, random() FROM generate_series(1, 200000) s RETURNING *;
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*)
        FROM lake_table.files
        WHERE table_name = 'temp_file_size'::regclass
    """,
        pg_conn,
    )
    assert result[0]["count"] == 2

    # Insert fits in 1 CSV file
    run_command(
        f"""
        SET pg_lake_table.max_write_temp_file_size_mb TO '5MB';

        COPY (SELECT random(), s FROM generate_series(1, 200000) s) TO '{url}';
        COPY temp_file_size (value, id) FROM '{url}';
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*)
        FROM lake_table.files
        WHERE table_name = 'temp_file_size'::regclass
    """,
        pg_conn,
    )
    assert result[0]["count"] == 3

    # New rows from update are also split across files
    run_command(
        f"""
        SET pg_lake_table.max_write_temp_file_size_mb TO '3MB';
        UPDATE temp_file_size SET value = repeat('#', 10);
    """,
        pg_conn,
    )

    result = run_query(
        """
        SELECT count(*)
        FROM lake_table.files
        WHERE table_name = 'temp_file_size'::regclass
    """,
        pg_conn,
    )
    assert result[0]["count"] == 3

    pg_conn.rollback()


def test_constraints(pg_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_constraints(pg_conn, extension, table_type)


def internal_test_constraints(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_constraints_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_constraints (
            value text default 'D' not null,
            id int check (id > 0)
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""
        INSERT INTO test_constraints (id) VALUES (1);
    """,
        pg_conn,
    )

    pg_conn.commit()

    # Violate not null constraint with insert
    error = run_command(
        """
        INSERT INTO test_constraints (id, value) VALUES (1, NULL);
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates not-null constraint" in error

    pg_conn.rollback()

    # Violate check constraint with insert
    error = run_command(
        """
        INSERT INTO test_constraints (id, value) VALUES (0, 'zero');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates check constraint" in error

    pg_conn.rollback()

    # Violate not null constraint with update
    error = run_command(
        """
        UPDATE test_constraints SET value = NULL;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates not-null constraint" in error

    pg_conn.rollback()

    # Violate check constraint with update
    error = run_command(
        """
        UPDATE test_constraints SET id = 0;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates check constraint" in error

    pg_conn.rollback()

    # Cleanup
    run_command("DROP FOREIGN TABLE test_constraints", pg_conn)
    pg_conn.commit()


def test_partitioning(pg_conn, duckdb_conn, extension, azure):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_partitioning(pg_conn, duckdb_conn, extension, table_type)


def internal_test_partitioning(pg_conn, duckdb_conn, extension, table_type):
    location = f"az://{TEST_BUCKET}/internal_test_partitioning_{table_type}/"

    # Create a partitioned table with 2 partitions (0-999, 1000-1999)
    run_command(
        f"""
        CREATE TABLE test_partitioned (
            id int not null check (id > 0),
            value text default 'D' not null,
            ser bigserial,
            gida bigint generated by default as identity,
            gidd bigint generated always as identity,
            id2 int generated always as (id * 2) stored
        )
        PARTITION BY RANGE (gida);

		CREATE FOREIGN TABLE test_partitioned_1
        PARTITION OF test_partitioned
        FOR VALUES FROM (0) TO (1001)
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}_1');

		CREATE FOREIGN TABLE test_partitioned_2
        PARTITION OF test_partitioned
        FOR VALUES FROM (1001) TO (3000)
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}_2');
        """,
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_partitioned (value, id)
        SELECT 'hello-'||s, s FROM generate_series(1,2000) s;
    """,
        pg_conn,
    )

    pg_conn.commit()

    # Check files (1 per partition)
    result = run_query(
        """
        SELECT path FROM lake_table.files WHERE table_name::text LIKE 'test_partitioned%' ORDER BY table_name
    """,
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["path"].startswith(location + "_1")
    assert result[1]["path"].startswith(location + "_2")

    # Check that they're actually Parquet
    if table_type == "pg_lake":
        duckdb_conn.execute(
            "SELECT count(*) AS count FROM read_parquet($1)", [str(location) + "_1/**"]
        )
    else:
        duckdb_conn.execute(
            "SELECT count(*) AS count FROM read_parquet($1)",
            [str(location + "_1/data/") + "**"],
        )

    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 1000

    if table_type == "pg_lake":
        duckdb_conn.execute(
            "SELECT count(*) AS count FROM read_parquet($1)", [str(location) + "_2/**"]
        )
    else:
        duckdb_conn.execute(
            "SELECT count(*) AS count FROM read_parquet($1)",
            [str(location + "_2/data/") + "**"],
        )

    duckdb_result = duckdb_conn.fetchall()
    assert duckdb_result[0][0] == 1000

    # Check row counts
    result = run_query("SELECT count(*) FROM test_partitioned", pg_conn)
    assert result[0]["count"] == 2000

    result = run_query("SELECT count(*) FROM test_partitioned_1", pg_conn)
    assert result[0]["count"] == 1000

    result = run_query("SELECT count(*) FROM test_partitioned_2", pg_conn)
    assert result[0]["count"] == 1000

    # Check generated values
    result = run_query("SELECT * FROM test_partitioned WHERE id = 1500", pg_conn)
    assert result[0]["value"] == "hello-1500"
    assert result[0]["ser"] == 1500
    assert result[0]["gida"] == 1500
    assert result[0]["gidd"] == 1500
    assert result[0]["id2"] == 3000

    # Check returning
    result = run_query(
        """
        INSERT INTO test_partitioned VALUES (2001, 'hello-2001'), (2002, 'bye')
        RETURNING id, gida
    """,
        pg_conn,
    )
    assert len(result) == 2
    assert result[0]["gida"] == 2001
    assert result[1]["gida"] == 2002

    # Perform updates on a partitioned table
    result = run_query(
        """
        UPDATE test_partitioned SET value = 'disabled', gida = DEFAULT WHERE id = 1500
        RETURNING gida, ser, value
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["gida"] == 2003
    assert result[0]["ser"] == 1500
    assert result[0]["value"] == "disabled"

    result = run_query(
        """
        UPDATE test_partitioned t
        SET value = 'max'
        FROM (SELECT max(gida) AS gida FROM test_partitioned) s
        WHERE t.gida = s.gida
        RETURNING t.gida, tableoid::regclass AS table_name
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["gida"] == 2003
    assert result[0]["table_name"] == "test_partitioned_2"

    result = run_query("SELECT value FROM test_partitioned WHERE gida = 2003", pg_conn)
    assert result[0]["value"] == "max"

    # Perform delete on a partitioned table
    result = run_query(
        """
        DELETE FROM test_partitioned WHERE id = 1500
        RETURNING gida, ser, value
    """,
        pg_conn,
    )
    assert len(result) == 1
    assert result[0]["gida"] == 2003
    assert result[0]["ser"] == 1500
    assert result[0]["value"] == "max"

    result = run_query(
        """
        DELETE FROM test_partitioned t
        USING (SELECT max(gida) AS gida FROM test_partitioned) s
        WHERE t.gida = s.gida
        RETURNING t.gida, tableoid::regclass AS table_name, t.*::text
    """,
        pg_conn,
    )
    assert len(result) == 1
    # Deleted the row with gida 2003, so new max gida is 2002
    assert result[0]["gida"] == 2002
    assert result[0]["table_name"] == "test_partitioned_2"

    result = run_query("SELECT value FROM test_partitioned WHERE gida = 2002", pg_conn)
    assert len(result) == 0

    # Test truncate
    run_command("SAVEPOINT beforetrunc", pg_conn)
    run_command("TRUNCATE test_partitioned RESTART IDENTITY", pg_conn)

    # Make sure table is empty
    result = run_query("SELECT count(*) FROM test_partitioned", pg_conn)
    assert result[0]["count"] == 0

    # Make sure sequences are restarted
    result = run_query(
        "INSERT INTO test_partitioned (value, id) VALUES ('restart', 1) RETURNING ser",
        pg_conn,
    )
    assert result[0]["ser"] == 1

    run_command("ROLLBACK TO SAVEPOINT beforetrunc", pg_conn)

    # Violate not null constraint
    error = run_command(
        """
        INSERT INTO test_partitioned (id, value) VALUES (1, NULL);
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates not-null constraint" in error

    pg_conn.rollback()

    error = run_command(
        """
        UPDATE test_partitioned SET value = NULL WHERE id = 1500
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates not-null constraint" in error

    pg_conn.rollback()

    # Violate check constraint
    error = run_command(
        """
        INSERT INTO test_partitioned (id, value) VALUES (0, 'zero');
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates check constraint" in error

    pg_conn.rollback()

    error = run_command(
        """
        UPDATE test_partitioned SET id = 0 WHERE id = 1500
    """,
        pg_conn,
        raise_error=False,
    )
    assert "violates check constraint" in error

    pg_conn.rollback()

    # Violate partition constraint
    error = run_command(
        """
        INSERT INTO test_partitioned (value, id)
        SELECT 'hello-'||s, s FROM generate_series(1,2000) s;
    """,
        pg_conn,
        raise_error=False,
    )
    assert "no partition of relation" in error

    pg_conn.rollback()

    error = run_command(
        """
        UPDATE test_partitioned SET gida = 5000 WHERE id = 1500
    """,
        pg_conn,
        raise_error=False,
    )
    assert "cross-partition updates are not supported" in error

    pg_conn.rollback()

    # Cleanup
    run_command("DROP TABLE test_partitioned", pg_conn)
    pg_conn.commit()


def test_transactions(pg_conn, azure, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_transactions(pg_conn, extension, table_type)


def internal_test_transactions(pg_conn, extension, table_type):
    location = f"az://{TEST_BUCKET}/internal_test_transactions_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_transactions (
            id int check (id > 0),
            value text default 'D' not null
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
    """,
        pg_conn,
    )

    pg_conn.commit()

    # Initial state
    result = run_query("SELECT count(*) FROM test_transactions", pg_conn)
    assert result[0]["count"] == 0

    # Insert rows with savepoint
    run_command(
        """
        SAVEPOINT initial;
        INSERT INTO test_transactions
        SELECT s, 'hello-'||s FROM generate_series(1,100) s
    """,
        pg_conn,
    )

    # Observe uncommitted rows
    result = run_query("SELECT count(*) FROM test_transactions", pg_conn)
    assert result[0]["count"] == 100

    # Update rows
    run_command(
        """
        SAVEPOINT afterinsert;
        UPDATE test_transactions SET id = 99;
    """,
        pg_conn,
    )

    # Observe uncommitted rows
    result = run_query("SELECT count(*) FROM test_transactions WHERE id = 99", pg_conn)
    assert result[0]["count"] == 100

    # Observe return to pre-update when rolling back to savepoint
    run_command(
        """
        ROLLBACK TO SAVEPOINT afterinsert;
    """,
        pg_conn,
    )

    result = run_query("SELECT count(*) FROM test_transactions WHERE id = 99", pg_conn)
    assert result[0]["count"] == 1

    # Test truncate
    run_command("TRUNCATE test_transactions", pg_conn)

    result = run_query("SELECT count(*) FROM test_transactions", pg_conn)
    assert result[0]["count"] == 0

    # Observe return to initial state when rolling back to savepoint
    run_command(
        """
        ROLLBACK TO SAVEPOINT initial;
    """,
        pg_conn,
    )

    result = run_query("SELECT count(*) FROM test_transactions", pg_conn)
    assert result[0]["count"] == 0

    # Insert rows
    run_command(
        """
        INSERT INTO test_transactions
        SELECT s, 'hello-'||s FROM generate_series(1,200) s
    """,
        pg_conn,
    )

    # Observe uncommitted rows
    result = run_query("SELECT count(*) FROM test_transactions", pg_conn)
    assert result[0]["count"] == 200

    pg_conn.rollback()

    # Observe return to initial state
    result = run_query("SELECT count(*) FROM test_transactions", pg_conn)
    assert result[0]["count"] == 0

    # Cleanup
    run_command("DROP FOREIGN TABLE test_transactions", pg_conn)
    pg_conn.commit()


def test_insert_subqueries(pg_conn, duckdb_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_insert_subqueries(pg_conn, extension, table_type)


def internal_test_insert_subqueries(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_insert_subqueries_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_insert_subqueries (
            id int,
            value text
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
    """,
        pg_conn,
    )

    # Insert with a recursive CTE
    run_command(
        f"""
        WITH RECURSIVE nums AS (
            SELECT 1 AS num
            UNION ALL
            SELECT num + 1 FROM nums WHERE num < 5
        )
        INSERT INTO test_insert_subqueries (id) SELECT num FROM nums;
    """,
        pg_conn,
    )

    result = run_query(f"SELECT count(*) FROM test_insert_subqueries", pg_conn)
    assert result[0]["count"] == 5

    # Insert with a self-referencing sublink
    result = run_query(
        f"""
        INSERT INTO test_insert_subqueries
        VALUES ((SELECT max(id)+1 FROM test_insert_subqueries)) RETURNING *;
    """,
        pg_conn,
    )
    assert result[0]["id"] == 6

    # 4 source rows and a duplicate
    result = run_query("SELECT count(*) FROM test_insert_subqueries", pg_conn)
    assert result[0]["count"] == 6

    result = run_query(
        f"""
        WITH cte_1 AS (INSERT INTO test_insert_subqueries VALUES (1), (2) RETURNING *)
        INSERT INTO test_insert_subqueries SELECT * FROM cte_1 RETURNING *;
    """,
        pg_conn,
    )
    assert result[0]["id"] == 1
    assert result[1]["id"] == 2

    # 4 source rows and a duplicate
    result = run_query("SELECT count(*) FROM test_insert_subqueries", pg_conn)
    assert result[0]["count"] == 10

    pg_conn.rollback()


def test_prepared(pg_conn, duckdb_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_prepared(pg_conn, extension, table_type)


def internal_test_prepared(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_prepared_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_prepared (
            id int check (id >= 0),
            value text generated always as ('hello-'||id) stored
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');

        PREPARE p1(int) as INSERT INTO test_prepared VALUES ($1) RETURNING value;
        PREPARE p2(int) as UPDATE test_prepared SET id = id * 2 WHERE id = $1 RETURNING value;
        PREPARE p3(int) as DELETE FROM test_prepared WHERE id = $1 RETURNING value;
    """,
        pg_conn,
    )

    for i in range(6):
        result = run_query(f"EXECUTE p1({i})", pg_conn)
        assert result[0]["value"] == f"hello-{i}"

    run_command("SAVEPOINT beforeupdate", pg_conn)

    for i in range(6):
        result = run_query(f"EXECUTE p2({i})", pg_conn)
        assert result[0]["value"] == f"hello-{i*2}"

    run_command("ROLLBACK TO SAVEPOINT beforeupdate", pg_conn)

    for i in range(6):
        result = run_query(f"EXECUTE p3({i})", pg_conn)
        assert result[0]["value"] == f"hello-{i}"

    run_command("DEALLOCATE ALL", pg_conn)

    pg_conn.rollback()


def test_insert_cursors(pg_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_insert_cursors(pg_conn, extension, table_type)


def internal_test_insert_cursors(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_insert_cursors_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_insert_cursors (
            id int check (id >= 0),
            value bigserial
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');

        DO $$
        DECLARE
            cursor_name SCROLL CURSOR FOR SELECT * FROM (VALUES (1), (2), (3), (4)) AS t(a);
            row_data integer;
        BEGIN
            -- Open the cursor
            OPEN cursor_name;

            -- Fetch first row
            FETCH NEXT FROM cursor_name INTO row_data;
            INSERT INTO test_insert_cursors VALUES (row_data);

            -- Fetch second row
            FETCH NEXT FROM cursor_name INTO row_data;
            INSERT INTO test_insert_cursors VALUES (row_data);

            -- Move backward one row
            MOVE BACKWARD 1 IN cursor_name;

            -- Re-fetch the second row
            FETCH NEXT FROM cursor_name INTO row_data;
            INSERT INTO test_insert_cursors VALUES (row_data);

            -- Fetch remaining rows
            LOOP
                FETCH NEXT FROM cursor_name INTO row_data;
                EXIT WHEN NOT FOUND;
                INSERT INTO test_insert_cursors VALUES (row_data);
            END LOOP;

            -- Close the cursor
            CLOSE cursor_name;
        END;
        $$;
    """,
        pg_conn,
    )

    # 4 source rows and a duplicate
    result = run_query("SELECT count(*) FROM test_insert_cursors", pg_conn)
    assert result[0]["count"] == 5

    # Second row was inserted twice
    result = run_query("SELECT count(*) FROM test_insert_cursors WHERE id = 2", pg_conn)
    assert result[0]["count"] == 2

    pg_conn.rollback()


def test_triggers(pg_conn, extension, azure):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_triggers(pg_conn, extension, table_type)


def internal_test_triggers(pg_conn, extension, table_type):
    location = f"az://{TEST_BUCKET}/internal_test_triggers_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_triggers (
            id int check (id >= 0),
            value text generated always as ('hello-'||id) stored
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');

        CREATE TABLE audit_log (
            log_id SERIAL PRIMARY KEY,
            operation_type TEXT,
            original_value TEXT,
            log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE OR REPLACE FUNCTION log_inserts()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Insert log entry into audit_log
            INSERT INTO audit_log (operation_type, original_value)
            VALUES ('INSERT', NEW.value);

            -- Return the new row to allow the insert to proceed
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_after_insert
        AFTER INSERT ON test_triggers
        FOR EACH ROW EXECUTE FUNCTION log_inserts();

        CREATE OR REPLACE FUNCTION log_updates()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Insert log entry into audit_log
            INSERT INTO audit_log (operation_type, original_value)
            VALUES ('UPDATE', NEW.value);

            -- Return the new row to allow the update to proceed
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_after_update
        AFTER UPDATE ON test_triggers
        FOR EACH ROW EXECUTE FUNCTION log_updates();

        CREATE OR REPLACE FUNCTION log_deletes()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Insert log entry into audit_log
            INSERT INTO audit_log (operation_type, original_value)
            VALUES ('DELETE', OLD.value);

            -- Return the old row to allow the delete to proceed
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER trigger_after_delete
        AFTER DELETE ON test_triggers
        FOR EACH ROW EXECUTE FUNCTION log_deletes();

        -- Perform an insert
        INSERT INTO test_triggers VALUES (10);
    """,
        pg_conn,
    )

    run_command(
        f"""
        -- Perform an update
        UPDATE test_triggers SET id = 1;

        -- Perform an delete
        DELETE FROM test_triggers;
    """,
        pg_conn,
    )

    result = run_query(
        "SELECT original_value FROM audit_log WHERE operation_type = 'INSERT'", pg_conn
    )
    assert result[0]["original_value"] == "hello-10"

    result = run_query(
        "SELECT original_value FROM audit_log WHERE operation_type = 'UPDATE'", pg_conn
    )
    assert result[0]["original_value"] == "hello-1"

    result = run_query(
        "SELECT original_value FROM audit_log WHERE operation_type = 'DELETE'", pg_conn
    )
    assert result[0]["original_value"] == "hello-1"

    pg_conn.rollback()


def test_array_column(pg_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_array_column(pg_conn, extension, table_type)


def internal_test_array_column(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_array_column_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_array_columns (
            int_col INT[],
            text_col text[]
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');
        """,
        pg_conn,
    )

    run_command(
        f"""
        /* Try updating an empty table */
        UPDATE test_array_columns SET int_col = ARRAY[1,2];
        """,
        pg_conn,
    )

    run_command(
        f"""
        INSERT INTO test_array_columns VALUES(ARRAY[1,2], '{{"hello","world"}}');
        """,
        pg_conn,
    )
    run_command(
        f"""UPDATE test_array_columns SET int_col = ARRAY[3,4] WHERE array['hello'] <@ text_col;""",
        pg_conn,
    )

    result = run_query("SELECT * FROM test_array_columns", pg_conn)
    assert result[0]["int_col"] == [3, 4]
    assert result[0]["text_col"] == ["hello", "world"]

    pg_conn.rollback()


def test_inherits(pg_conn, extension, azure):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_inherits(pg_conn, extension, table_type)


def internal_test_inherits(pg_conn, extension, table_type):
    location = f"az://{TEST_BUCKET}/internal_test_inherits_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_parent (
            a int
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}');

        CREATE TABLE test_child (
            a int
        )
        INHERITS (test_parent);

        INSERT INTO test_parent VALUES (1), (2);
        INSERT INTO test_child VALUES (3), (4);
    """,
        pg_conn,
    )

    result = run_query("select count(*) from test_parent", pg_conn)
    assert result[0]["count"] == 4

    pg_conn.rollback()


def test_truncate(pg_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_truncate(pg_conn, extension, table_type)


def internal_test_truncate(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_truncate_{table_type}/"

    run_command(
        f"""
        CREATE FOREIGN TABLE test_truncate_1 (
            a int
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}_1');

        CREATE FOREIGN TABLE test_truncate_2 (
            a int
        )
        SERVER {table_type}
        OPTIONS ({get_table_options(table_type)} location '{location}_2');

        CREATE TABLE test_truncate_3 (
            a int
        );

        INSERT INTO test_truncate_1 VALUES (1), (2);
        INSERT INTO test_truncate_2 VALUES (1), (2);
        INSERT INTO test_truncate_3 VALUES (1), (2);

        TRUNCATE test_truncate_1, test_truncate_3, test_truncate_2;
    """,
        pg_conn,
    )

    result = run_query("SELECT count(*) FROM test_truncate_1", pg_conn)
    assert result[0]["count"] == 0

    result = run_query("SELECT count(*) FROM test_truncate_2", pg_conn)
    assert result[0]["count"] == 0

    result = run_query("SELECT count(*) FROM test_truncate_3", pg_conn)
    assert result[0]["count"] == 0

    pg_conn.rollback()


def test_map_types_int_int(pg_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_map_types_int_int(pg_conn, extension, table_type)


def internal_test_map_types_int_int(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_map_types_int_int_{table_type}/"

    create_map_type("int", "int")

    run_command(
        f"""
    create foreign table map_writable_int_int (key text, value map_type.key_int_val_int)
    SERVER {table_type}
    OPTIONS ({get_table_options(table_type)} location '{location}');
    insert into map_writable_int_int values ('hello', '{{"(4,1)","(55,5)"}}'::map_type.key_int_val_int);
    """,
        pg_conn,
    )

    res = run_query(
        f"""
    select map_type.extract(value, 4) from map_writable_int_int;
    """,
        pg_conn,
    )

    assert res[0][0] == 1

    # add more rows so we can test multiple maps here
    run_command(
        f"""
    insert into map_writable_int_int values ('a', '{{"(1,5)","(2,4)","(3,3)","(4,2)","(5,1)"}}'::map_type.key_int_val_int);
    insert into map_writable_int_int values ('b', '{{"(1,21)","(2,-12)","(3,0)","(4,100)"}}'::map_type.key_int_val_int);
    insert into map_writable_int_int values ('c', '{{"(3,5)","(4,4)"}}'::map_type.key_int_val_int);
    insert into map_writable_int_int values ('d', '{{"(4,99999999)"}}'::map_type.key_int_val_int);
    insert into map_writable_int_int values ('e', '{{"(99999,)","(4,27)","(-1000,-1)","(0,0)"}}'::map_type.key_int_val_int);
    """,
        pg_conn,
    )

    res = run_query(
        f"""
    select map_type.extract(value, 4) from map_writable_int_int order by 1
    """,
        pg_conn,
    )

    assert res == [
        [1],
        [2],
        [4],
        [27],
        [100],
        [99999999],
    ]

    # some NULL some not
    res = run_query(
        f"""
    select map_type.extract(value, 3) from map_writable_int_int order by 1 nulls first
    """,
        pg_conn,
    )

    assert res == [[None], [None], [None], [0], [3], [5]]

    # should be NULL results since no key has this
    res = run_query(
        f"""
    select map_type.extract(value, 20) from map_writable_int_int order by 1 nulls first
    """,
        pg_conn,
    )

    assert res == [[None], [None], [None], [None], [None], [None]]

    pg_conn.rollback()


def test_map_type_int_text_array(pg_conn, extension):
    for table_type in ["pg_lake", "pg_lake_iceberg"]:
        internal_test_map_type_int_text_array(pg_conn, extension, table_type)


def internal_test_map_type_int_text_array(pg_conn, extension, table_type):
    location = f"s3://{TEST_BUCKET}/internal_test_map_type_int_text_array_{table_type}/"

    create_map_type("int", "text[]")

    run_command(
        f"""
    create foreign table map_writable_int_text_array (key text, value map_type.key_int_val_text_array)
    SERVER {table_type}
    OPTIONS ({get_table_options(table_type)} location '{location}');
    insert into map_writable_int_text_array values ('hello', '{{"(4,\\"{{a,b}}\\")","(55,\\"{{c,d,e,f}}\\")"}}'::map_type.key_int_val_text_array);
    """,
        pg_conn,
    )

    res = run_query(
        f"""
    select map_type.extract(value, 4) from map_writable_int_text_array;
    """,
        pg_conn,
    )

    assert res[0][0] == ["a", "b"]

    # add more rows so we can test multiple maps here
    maps = [
        {1: ["foo", "bar", "baz"], 4: ["bat"]},
        {
            5: [None],
            6: ["spidey", "sense"],
            8: ["999999"],
            4: ["Long word of some sort", "CAPITALS", "work 23 final cool"],
        },
        {5: ["a, 4=", "whoopsie"]},
    ]
    run_command(
        f"""
    insert into map_writable_int_text_array values ('a', '{stringify_map(maps[0])}'::map_type.key_int_val_text_array);
    insert into map_writable_int_text_array values ('b', '{stringify_map(maps[1])}'::map_type.key_int_val_text_array);
    insert into map_writable_int_text_array values ('c', '{stringify_map(maps[2])}'::map_type.key_int_val_text_array);
    """,
        pg_conn,
    )

    res = run_query(
        f"""
    select unnest(map_type.extract(value, 4)) from map_writable_int_text_array order by 1
    """,
        pg_conn,
    )

    assert sorted(res) == sorted(
        [
            ["a"],
            ["b"],
            ["bat"],
            ["CAPITALS"],
            ["Long word of some sort"],
            ["work 23 final cool"],
        ]
    )

    pg_conn.rollback()


def quote_elem(elem, type):
    output = ""
    if elem is None:
        # empty string for
        pass
    elif isinstance(elem, list):
        # no further quoting for multi-dimensional arrays at this level
        if type == "array":
            output += pg_quote_array(elem)
        else:
            output += pg_string_quote(pg_quote_array(elem))
    elif isinstance(elem, dict):
        # no further quoting for multi-dimensional arrays at this level
        output += pg_string_quote(pg_quote_record(elem))
    elif isinstance(elem, float):
        # no further quoting for multi-dimensional arrays at this level
        output += str(elem)
    elif isinstance(elem, int):
        # no further quoting for multi-dimensional arrays at this level
        output += str(elem)
    else:
        output += pg_string_quote(str(elem))
    return output


def pg_string_quote(string):
    # skip quotes if simple string
    if len(re.sub("[a-zA-Z0-9]", "", string)) == 0:
        return string
    # otherwise backslash-quote \ and ", return double-quoted
    replaced = string.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{replaced}"'


def pg_quote_record(myDict):
    "apply pg's record-style quoting to a dict; we only care about ordered values"
    output = "("
    for index, elem in enumerate(myDict.values()):
        if index:
            output += ","
        output += quote_elem(elem, "record")
    output += ")"
    return output


def pg_quote_array(myList):
    "apply pg's array-style quoting to a list"
    output = "{"

    for index, elem in enumerate(myList):
        if index:
            output += ","
        output += quote_elem(elem, "array")
    output += "}"
    return output


def stringify_map(myDict):
    "transforms a dict and outputs the quoted value"

    outlist = []

    for key, value in myDict.items():
        outlist.append({"key": key, "val": value})

    return pg_quote_array(outlist)


def test_stringify_map():
    result = stringify_map({"a": "b", "c": [1, 2, 3]})

    assert result == '{"(a,b)","(c,\\"{1,2,3}\\")"}'
