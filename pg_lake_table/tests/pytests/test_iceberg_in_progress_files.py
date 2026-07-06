import pytest
from utils_pytest import *

TEST_TABLE_NAMESPACE = "test_iceberg_in_progress_files_nsp"

transaction_isolation = [("read committed"), ("repeatable read"), ("serializable")]


# insert + update + delete with rollback
@pytest.mark.parametrize("isolation_level", transaction_isolation)
def test_in_progress_files_1(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    isolation_level,
    create_injection_extension,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    table_name = create_iceberg_table

    run_command(f"SET transaction_isolation TO '{isolation_level}'", pg_conn)

    run_command(f"""SELECT public.injection_points_set_local();""", pg_conn)
    pg_conn.commit()

    run_command(
        "SELECT public.injection_points_attach('after-apply-iceberg-changes', 'error');",
        pg_conn,
    )

    # some inserts
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1),(2),(3),(4)",
        pg_conn,
    )
    run_command(f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (2)", pg_conn)
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} SELECT i FROM generate_series(0,100)i",
        pg_conn,
    )

    # merge-on-read / positional update + delete
    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 2", pg_conn
    )
    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 5", pg_conn
    )

    # copy-on-write update/delete
    run_command(
        f"UPDATE {TEST_TABLE_NAMESPACE}.{table_name} SET id = 0 WHERE id < 50", pg_conn
    )
    run_command(
        f"DELETE FROM {TEST_TABLE_NAMESPACE}.{table_name} WHERE id < 75", pg_conn
    )

    error = run_command("COMMIT;", pg_conn, raise_error=False)
    assert "after-apply-iceberg-changes" in error

    run_command(
        f"SELECT public.injection_points_detach('after-apply-iceberg-changes')", pg_conn
    )
    pg_conn.commit()

    # re-set isolation level
    run_command(f"SET transaction_isolation TO '{isolation_level}'", pg_conn)

    results = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn
    )
    assert results[0][0] == 0

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 18

    distinct_id_count = run_query(
        f"SELECT count(DISTINCT operation_id) FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert distinct_id_count[0][0] == 1

    run_command_outside_tx(
        [
            f"SET transaction_isolation TO '{isolation_level}'",
            f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name}",
        ],
        pg_conn,
    )

    pg_conn.commit()
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)


# insert + truncate with rollback
def test_in_progress_files_2(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    create_injection_extension,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    table_name = create_iceberg_table

    run_command(f"""SELECT public.injection_points_set_local();""", pg_conn)
    pg_conn.commit()

    run_command(
        "SELECT public.injection_points_attach('after-apply-iceberg-changes', 'error');",
        pg_conn,
    )

    # some inserts
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1),(2),(3),(4)",
        pg_conn,
    )

    run_command(f"TRUNCATE {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn)

    error = run_command("COMMIT;", pg_conn, raise_error=False)
    assert "after-apply-iceberg-changes" in error

    run_command(
        f"SELECT public.injection_points_detach('after-apply-iceberg-changes')", pg_conn
    )
    pg_conn.commit()

    results = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn
    )
    assert results[0][0] == 0

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 1

    run_command_outside_tx([f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name}"], pg_conn)
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)


# manifest compaction with rollback
def test_in_progress_files_3(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    create_injection_extension,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    run_command(f"""SELECT public.injection_points_set_local();""", pg_conn)
    pg_conn.commit()

    run_command(
        "SELECT public.injection_points_attach('after-apply-iceberg-changes', 'error');",
        pg_conn,
    )

    table_name = create_iceberg_table

    # trigger manifest compaction after 2nd insert
    run_command(f"SET pg_lake_iceberg.manifest_min_count_to_merge TO 2", pg_conn)
    run_command(f"SET pg_lake_iceberg.enable_manifest_merge_on_write TO on", pg_conn)

    # some inserts
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1),(2),(3),(4)",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1),(2),(3),(4)",
        pg_conn,
    )
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1),(2),(3),(4)",
        pg_conn,
    )

    error = run_command("COMMIT;", pg_conn, raise_error=False)
    assert "after-apply-iceberg-changes" in error

    run_command(
        f"SELECT public.injection_points_detach('after-apply-iceberg-changes')", pg_conn
    )
    pg_conn.commit()

    results = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn
    )
    assert results[0][0] == 0

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 6

    run_command_outside_tx(
        [f"VACUUM FULL {TEST_TABLE_NAMESPACE}.{table_name}"], pg_conn
    )
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)


# insert + savepoint + insert + rollback to savepoint; rollback
@pytest.mark.parametrize("isolation_level", transaction_isolation)
def test_in_progress_files_4(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    isolation_level,
    create_injection_extension,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    run_command(f"""SELECT public.injection_points_set_local();""", pg_conn)
    pg_conn.commit()

    table_name = create_iceberg_table

    run_command(f"SET transaction_isolation TO '{isolation_level}'", pg_conn)

    run_command(
        "SELECT public.injection_points_attach('after-apply-iceberg-changes', 'error');",
        pg_conn,
    )

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1),(2),(3),(4)",
        pg_conn,
    )
    run_command(f"SAVEPOINT s1;", pg_conn)
    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (10),(20),(30),(40)",
        pg_conn,
    )
    run_command(f"ROLLBACK TO SAVEPOINT s1;", pg_conn)
    run_command(f"SET transaction_isolation TO '{isolation_level}'", pg_conn)

    run_command(
        f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (100),(200),(300),(400)",
        pg_conn,
    )

    error = run_command("COMMIT;", pg_conn, raise_error=False)
    assert "after-apply-iceberg-changes" in error

    run_command(
        f"SELECT public.injection_points_detach('after-apply-iceberg-changes')", pg_conn
    )
    pg_conn.commit()

    results = run_query(
        f"SELECT count(*) FROM {TEST_TABLE_NAMESPACE}.{table_name}", pg_conn
    )
    assert results[0][0] == 0

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 6

    run_command_outside_tx(
        [
            f"SET transaction_isolation TO '{isolation_level}'",
            f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name}",
        ],
        pg_conn,
    )

    pg_conn.commit()
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)


# insert + savepoint + insert + rollback to savepoint; commit
@pytest.mark.parametrize("isolation_level", transaction_isolation)
def test_in_progress_files_5(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    isolation_level,
):

    table_name = create_iceberg_table

    run_command(f"SET transaction_isolation TO '{isolation_level}'", pg_conn)

    run_command(f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1)", pg_conn)
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )

    run_command(f"SAVEPOINT s1;", pg_conn)
    run_command(f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (2)", pg_conn)
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )

    run_command(f"ROLLBACK TO SAVEPOINT s1;", pg_conn)

    run_command(f"SET transaction_isolation TO '{isolation_level}'", pg_conn)

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )

    run_command(f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (3)", pg_conn)

    pg_conn.commit()

    results = run_query(
        f"SELECT * FROM {TEST_TABLE_NAMESPACE}.{table_name} ORDER BY 1", pg_conn
    )
    assert results == [[1], [3]]

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )

    assert len(in_progress_file_paths) == 1

    run_command_outside_tx(
        [
            f"SET transaction_isolation TO '{isolation_level}'",
            f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name}",
        ],
        pg_conn,
    )

    pg_conn.commit()
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)


# pass relevant GUCs
def test_in_progress_files_6(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    create_injection_extension,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    run_command(f"""SELECT public.injection_points_set_local();""", pg_conn)
    pg_conn.commit()

    table_name = create_iceberg_table

    run_command(
        "SELECT public.injection_points_attach('after-apply-iceberg-changes', 'error');",
        pg_conn,
    )

    for i in range(0, 70):
        run_command(
            f"INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES ({i})", pg_conn
        )

    error = run_command("COMMIT;", pg_conn, raise_error=False)
    assert "after-apply-iceberg-changes" in error

    run_command(
        f"SELECT public.injection_points_detach('after-apply-iceberg-changes')", pg_conn
    )
    pg_conn.commit()

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )

    assert len(in_progress_file_paths) == 73

    vacuum_commands = [
        "SET pg_lake_table.max_file_removals_per_vacuum TO 50",
        f"VACUUM {TEST_TABLE_NAMESPACE}.{table_name};",
    ]

    # first, remove max_file_removals_per_vacuum(50) entries
    run_command_outside_tx(vacuum_commands, pg_conn)
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 23

    # then, the set the max_file_removals_per_vacuum to 25
    run_command_outside_tx(vacuum_commands, pg_conn)
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    assert_iceberg_s3_file_consistency(pg_conn, s3, TEST_TABLE_NAMESPACE, table_name)


# non-iceberg tables
def test_in_progress_files_7(s3, pg_conn, extension, create_helper_functions):

    table_name = "test_in_progress_files_8"

    # Set up the foreign table
    location = f"s3://{TEST_BUCKET}/test_in_progress_files_8/folder"
    run_command(
        f"""
	    CREATE FOREIGN TABLE {table_name} (a int) SERVER pg_lake OPTIONS (location '{location}', writable 'true', format 'parquet');
	""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(f"""INSERT INTO {table_name} VALUES (1)""", pg_conn)
    pg_conn.rollback()

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '{location}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 1

    # now, flush and see all gone
    run_command(f"SELECT lake_engine.flush_in_progress_queue()", pg_conn)
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files WHERE path ilike '{location}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0


def test_in_progress_files_8(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    create_injection_extension,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    table_name = create_iceberg_table

    # todo: improve this by passing the WHERE path ILIKE location
    # show at first the queue is empty
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files  WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    # injection_points_set_local activates injection point only for this process (not for base worker tx)
    # this will rollback due to injection point at precommit hook
    run_command(f"""SELECT public.injection_points_set_local();""", pg_conn)
    pg_conn.commit()

    run_command(
        f"""
            SELECT public.injection_points_attach('after-apply-iceberg-changes', 'error');
			CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name}_second USING pg_lake_iceberg AS SELECT i FROM generate_series(0,1000)i;
			ALTER FOREIGN TABLE {TEST_TABLE_NAMESPACE}.{table_name}_second OPTIONS (ADD autovacuum_enabled 'false');
		""",
        pg_conn,
    )
    error = run_command("COMMIT;", pg_conn, raise_error=False)
    assert "after-apply-iceberg-changes" in error

    run_command(
        f"SELECT public.injection_points_detach('after-apply-iceberg-changes')", pg_conn
    )
    pg_conn.commit()

    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files ", pg_conn
    )
    assert len(in_progress_file_paths) > 1

    # we should be able to flush, and all should go
    run_command(f"SELECT lake_engine.flush_in_progress_queue()", pg_conn)
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files", pg_conn
    )
    assert len(in_progress_file_paths) == 0


# VACUUM (ICEBERG) gets rid of dropped tables' in_progress records
def test_in_progress_files_9(
    s3,
    pg_conn,
    extension,
    create_iceberg_table,
    create_helper_functions,
    create_injection_extension,
):

    # injection points only supported with 17+
    if get_pg_version_num(pg_conn) < 170000:
        return

    table_name = create_iceberg_table

    # nothing in the in_progress_file
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files  WHERE path ilike '%{table_name}%'",
        pg_conn,
    )
    assert len(in_progress_file_paths) == 0

    run_command(
        f"""
			CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name}_second USING pg_lake_iceberg AS SELECT i FROM generate_series(0,1000)i;
			ALTER FOREIGN TABLE {TEST_TABLE_NAMESPACE}.{table_name}_second OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )
    pg_conn.commit()

    # injection_points_set_local activates injection point only for this process (not for base worker tx)
    # this will rollback due to injection point at precommit hook
    run_command(f"""SELECT public.injection_points_set_local();""", pg_conn)
    pg_conn.commit()

    run_command(
        f"""SELECT public.injection_points_attach('after-apply-iceberg-changes', 'error');
                    INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name} VALUES (1);
                    INSERT INTO {TEST_TABLE_NAMESPACE}.{table_name}_second VALUES (1);
                """,
        pg_conn,
    )
    error = run_command("COMMIT;", pg_conn, raise_error=False)
    assert "after-apply-iceberg-changes" in error

    run_command(
        f"SELECT public.injection_points_detach('after-apply-iceberg-changes')", pg_conn
    )
    pg_conn.commit()

    # make sure both tables have entries in the in_progress_file
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files", pg_conn
    )
    paths = {entry["path"] for entry in in_progress_file_paths}
    expected_substrings = {f"{table_name}", f"{table_name}_second"}

    # Assert that each expected substring is found in at least one path
    assert len(paths) > 2 and all(
        any(expected in path for path in paths) for expected in expected_substrings
    ), f"Missing expected substrings in paths: {expected_substrings}"

    # now, drop the _second table
    run_command(f"DROP TABLE {TEST_TABLE_NAMESPACE}.{table_name}_second", pg_conn)
    pg_conn.commit()

    # we should be able to VACUUUM, and all should go
    # and VACUUM is safe to run inside a bg worker
    run_command_outside_tx(
        [f"SELECT extension_base.run_attached($$VACUUM (ICEBERG)$$)"], pg_conn
    )
    in_progress_file_paths = run_query(
        f"SELECT path FROM lake_engine.in_progress_files", pg_conn
    )
    assert len(in_progress_file_paths) == 0


# Sequence number to generate unique table names
table_counter = 0


# we need to generate unique table names
# otherwise we cannot call assert_iceberg_s3_file_consistency()
# as the s3 bucket would have artifacts from earlier tests
@pytest.fixture
def generate_table_name():
    global table_counter
    table_counter += 1

    TEST_TABLE_NAME = "test_iceberg_in_progress_files_" + str(table_counter)

    return f"{TEST_TABLE_NAME}_" + str(table_counter)


@pytest.fixture
def create_iceberg_table(pg_conn, s3, with_default_location, generate_table_name):
    table_name = generate_table_name  # Get the generated table name

    # Create schema and table
    run_command(f"CREATE SCHEMA {TEST_TABLE_NAMESPACE}", pg_conn)
    run_command(
        f"""
			CREATE TABLE {TEST_TABLE_NAMESPACE}.{table_name} (id int) USING pg_lake_iceberg;
			ALTER FOREIGN TABLE {TEST_TABLE_NAMESPACE}.{table_name} OPTIONS (ADD autovacuum_enabled 'false');
        """,
        pg_conn,
    )

    pg_conn.commit()

    yield table_name  # Yield the table name for further operations in the test

    # Rollback and clean up after test
    pg_conn.rollback()
    run_command(f"DROP SCHEMA {TEST_TABLE_NAMESPACE} CASCADE", pg_conn)
    pg_conn.commit()


@pytest.fixture(scope="module")
def create_helper_functions(superuser_conn):
    # These tests reach find_all_referenced_files through the shared
    # iceberg_get_referenced_files helper on a non-superuser connection.
    # find_all_referenced_files is owned by pg_lake_iceberg and REVOKEd from
    # public by the migration; just grant EXECUTE back for tests.
    run_command(
        "GRANT EXECUTE ON FUNCTION lake_iceberg.find_all_referenced_files(text) TO public;",
        superuser_conn,
    )
    superuser_conn.commit()
    yield
