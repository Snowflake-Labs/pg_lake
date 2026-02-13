import pytest

from utils_pytest import *


def test_row_ids(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        create table test_row_ids (x int, y int) using iceberg with (row_ids = 'true');
        insert into test_row_ids select s, s from generate_series(1,10) s;
        insert into test_row_ids select s, s from generate_series(1,10) s;
    """,
        pg_conn,
    )

    validate_row_id_mappings("test_row_ids", pg_conn)

    pg_conn.rollback()


def test_row_id_sequence(s3, pg_conn, extension, with_default_location):
    "Verify the rowid sequence is created and dropped with the table"
    run_command(
        f"""
        create table test_row_ids (x int, y int) using iceberg with (row_ids = 'true');
    """,
        pg_conn,
    )

    seqname = run_query(
        "select format('rowid_%s_seq', oid::text) from pg_class where relname = 'test_row_ids'",
        pg_conn,
    )[0][0]

    assert [[1]] == run_query(
        f"select count(*) from pg_class where relnamespace = '__pg_lake_table_writes' :: regnamespace and relname = '{seqname}'",
        pg_conn,
    )

    # drop the table, ensure no sequence exists
    run_command("drop table test_row_ids", pg_conn)

    assert [[0]] == run_query(
        f"select count(*) from pg_class where relnamespace = '__pg_lake_table_writes' :: regnamespace and relname = '{seqname}'",
        pg_conn,
    )

    pg_conn.rollback()


def test_row_ids_ddl(s3, pg_conn, extension, with_default_location):
    run_command(
        f"""
        create table test_row_ids_ddl (x int, y int) using iceberg;
        insert into test_row_ids_ddl values (1,1);
        insert into test_row_ids_ddl values (2,2);
        """,
        pg_conn,
    )

    assert count_row_id_mappings("test_row_ids_ddl", pg_conn) == 0

    # Try adding row IDs
    res = run_command(
        "alter table test_row_ids_ddl options (add row_ids 'true')", pg_conn
    )

    assert count_row_id_mappings("test_row_ids_ddl", pg_conn) == 2

    # Make sure we set first_row_id as well
    res = run_query(
        """
        select min(first_row_id), max(first_row_id)
        from lake_table.files
        where table_name = 'test_row_ids_ddl'::regclass
        """,
        pg_conn,
    )
    assert res[0]["min"] == 1
    assert res[0]["max"] == 2

    # Try re-enabling (no change)
    res = run_command(
        "alter table test_row_ids_ddl options (set row_ids 'true')", pg_conn
    )

    assert count_row_id_mappings("test_row_ids_ddl", pg_conn) == 2

    validate_row_id_mappings("test_row_ids_ddl", pg_conn)

    pg_conn.commit()

    # Try disabling via DROP
    error = run_command(
        "alter table test_row_ids_ddl options (drop row_ids)",
        pg_conn,
        raise_error=False,
    )
    assert "currently not supported" in error

    pg_conn.rollback()

    # Try disabling via SET
    error = run_command(
        "alter table test_row_ids_ddl options (set row_ids 'false')",
        pg_conn,
        raise_error=False,
    )
    assert "currently not supported" in error

    pg_conn.rollback()

    # Cleanup
    table_oid = run_query("select 'test_row_ids_ddl'::regclass::oid as id", pg_conn)[0][
        "id"
    ]

    run_command("drop table test_row_ids_ddl", pg_conn)

    assert count_row_id_mappings(table_oid, pg_conn) == 0

    pg_conn.commit()


def test_row_ids_changes(s3, pg_conn, pgduck_conn, extension, with_default_location):
    run_command(
        f"""
        create table test_row_ids (x int, y int) using iceberg with (row_ids = 'true');
        insert into test_row_ids select s, s from generate_series(1,10) s;
    """,
        pg_conn,
    )

    validate_row_id_mappings("test_row_ids", pg_conn)

    # do a position delete update
    run_command(
        f"""
        update test_row_ids set y = 15 where x = 7;
        """,
        pg_conn,
    )

    validate_row_id_mappings("test_row_ids", pg_conn)

    pg_conn.commit()

    # compact down to 1 file
    run_command_outside_tx(["vacuum full test_row_ids"], pg_conn)

    validate_row_id_mappings("test_row_ids", pg_conn)
    validate_row_id_in_files("test_row_ids", pg_conn, pgduck_conn)

    # replace file
    run_command("update test_row_ids set y = 0", pg_conn)

    validate_row_id_mappings("test_row_ids", pg_conn)

    run_command("truncate test_row_ids", pg_conn)
    assert count_row_id_mappings("test_row_ids", pg_conn) == 0

    run_command("drop table test_row_ids", pg_conn)
    pg_conn.commit()


def test_row_ids_single_row_range(
    s3, pg_conn, pgduck_conn, extension, with_default_location
):
    # Create some single-row row ID ranges (e.g. x = 1, x = 3)
    run_command(
        f"""
        create table test_row_ids (x int, y int) using iceberg with (row_ids = 'true');
        insert into test_row_ids select s, s from generate_series(1,20) s;
        insert into test_row_ids select s, s from generate_series(1,20) s;
        delete from test_row_ids where x = 2 or x = 4;
    """,
        pg_conn,
    )

    validate_row_id_mappings("test_row_ids", pg_conn)
    assert count_row_id_mappings("test_row_ids", pg_conn) == 2

    pg_conn.commit()

    # compact down to 1 file
    run_command_outside_tx(["vacuum full test_row_ids"], pg_conn)

    validate_row_id_mappings("test_row_ids", pg_conn)
    validate_row_id_in_files("test_row_ids", pg_conn, pgduck_conn)

    # original insertions are both split into 3 ranges, but sometimes
    # row IDs 5-10 from the first insertion concatenates with row ID 11
    # in the second insertion
    assert 5 <= count_row_id_mappings("test_row_ids", pg_conn) <= 6

    run_command("drop table test_row_ids", pg_conn)
    pg_conn.commit()


# Insert a large number of rows and trigger a split during vacuum
# Use superuser to set low target_file_size_mb
def test_row_ids_split(
    s3, superuser_conn, pgduck_conn, extension, with_default_location
):
    run_command(
        f"""
        create table test_row_ids using iceberg with (row_ids = 'true') as
        select s x, md5(s::text) y from generate_series(1,200000) s;
    """,
        superuser_conn,
    )

    # We currently have a single file
    assert count_row_id_mappings("test_row_ids", superuser_conn) == 1
    validate_row_id_mappings("test_row_ids", superuser_conn)

    superuser_conn.commit()

    # Split across files
    run_command_outside_tx(
        [
            "set pg_lake_table.target_file_size_mb to '3MB'",
            "vacuum full test_row_ids",
        ],
        superuser_conn,
    )

    # Row IDs split across multiple files
    assert count_row_id_mappings("test_row_ids", superuser_conn) > 1
    validate_row_id_mappings("test_row_ids", superuser_conn)
    validate_row_id_in_files("test_row_ids", superuser_conn, pgduck_conn)

    run_command("drop table test_row_ids", superuser_conn)
    superuser_conn.commit()


def test_row_ids_large(s3, pg_conn, pgduck_conn, extension, with_default_location):
    run_command(
        f"""
        create table test_row_ids (x int, y int) using iceberg with (row_ids = 'true');
        insert into test_row_ids select s, s from generate_series(1,5000000) s;
        insert into test_row_ids select * from test_row_ids;
    """,
        pg_conn,
    )
    pg_conn.commit()

    validate_row_id_mappings("test_row_ids", pg_conn)

    run_command_outside_tx(["vacuum full test_row_ids"], pg_conn)

    validate_row_id_mappings("test_row_ids", pg_conn)
    validate_row_id_in_files("test_row_ids", pg_conn, pgduck_conn)

    run_command("drop table test_row_ids", pg_conn)
    pg_conn.commit()


def test_zero_size_rowids(s3, pg_conn, with_default_location, extension):
    # this generates a zero-length row mapping record
    error = run_command(
        """
        create table test_row_ids (x int, y int) using iceberg with (row_ids = 'true');
        insert into test_row_ids SELECT * FROM test_row_ids;
        """,
        pg_conn,
        raise_error=False,
    )

    # technically this caused an internal postgres assert, so the fact we're
    # here at all means it's fine.
    assert error is None

    pg_conn.rollback()


def count_row_id_mappings(table_name, pg_conn):
    res = run_query(
        f"""
        select count(*) from __pg_lake_table_writes.row_id_mappings
        where table_name = '{table_name}'::regclass
    """,
        pg_conn,
    )

    return res[0]["count"]


def count_row_id_mappings_rows(table_name, pg_conn):
    # Includes number of rows that were removed by position deletes
    res = run_query(
        f"""
        select sum(upper(row_id_range) - lower(row_id_range))
        from lake_table.row_id_mappings
        where table_name = '{table_name}'::regclass
    """,
        pg_conn,
    )

    return res[0]["sum"]


def count_data_files_rows(table_name, pg_conn):
    # Includes number of rows that were removed by position deletes
    res = run_query(
        f"""
        select sum(row_count)
        from lake_table.files
        where table_name = '{table_name}'::regclass
        and content = 0
    """,
        pg_conn,
    )

    return res[0]["sum"]


def count_table(table_name, pg_conn):
    res = run_query(
        f"""
        select count(*) from {table_name}
    """,
        pg_conn,
    )

    return res[0]["count"]


# This function counts the number of _row_id values in the Parquet
# files that correctly correspond to the row_id_mappings. Currently
# assumes that all files have a _row_id column.
def count_valid_row_ids_in_files(table_name, pgduck_conn, pg_conn):
    tmp_mappings_url = f"s3://{TEST_BUCKET}/validate_file_row_ids/mappings.parquet"

    # Dump the mappings to a temporary CSV
    run_command(
        f"""
        copy (
            select lower(row_id_range) row_id_start, upper(row_id_range) row_id_end, path, file_row_number as row_number_start
            from lake_table.row_id_mappings r
            join lake_table.files d
            on (r.table_name = d.table_name and r.file_id = d.id)
            where r.table_name = '{table_name}'::regclass
        ) to '{tmp_mappings_url}';
    """,
        pg_conn,
    )

    # Get all the data files
    result = run_query(
        f"""
        select array_agg(path) as paths
        from lake_table.files
        where table_name = '{table_name}'::regclass
        and content = 0
    """,
        pg_conn,
    )
    paths = result[0]["paths"]

    # Count the number of rows in which _row_id matches the mapping
    result = run_query(
        f"""
        select count(*) as count
        from read_parquet({paths}, filename=True, file_row_number=True) f
        join read_parquet('{tmp_mappings_url}') m
        on (f.filename = m.path and _row_id >= row_id_start and _row_id < row_id_end and f.file_row_number = row_number_start + _row_id - row_id_start)
    """,
        pgduck_conn,
    )

    return int(result[0]["count"])


def validate_row_id_mappings(table_name, pg_conn):
    assert count_row_id_mappings_rows(table_name, pg_conn) == count_data_files_rows(
        table_name, pg_conn
    )


def validate_row_id_in_files(table_name, pg_conn, pgduck_conn):
    assert count_valid_row_ids_in_files(
        table_name, pgduck_conn, pg_conn
    ) == count_table(table_name, pg_conn)
