from utils_pytest import *
import itertools


# don't accept 'catalog_name', 'catalog_namespace', 'catalog_table_name'
# for catalog='object_store' tables
def test_writable_object_store_catalog_options(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):

    run_command(f"""CREATE SCHEMA test_object_store_catalog_options""", pg_conn)
    pg_conn.commit()

    # cannot provide any of the options
    options = ["catalog_name", "catalog_namespace", "catalog_table_name"]

    for combo in itertools.product([None, "x"], repeat=len(options)):
        if all(v is None for v in combo):
            continue  # skip None,None,None

        # build WITH clause
        parts = [f"{k}='{k}_val'" for k, v in zip(options, combo) if v is not None]
        with_clause = ", ".join(["catalog='object_store'"] + parts)

        query = f"""
	        CREATE TABLE test_object_store_catalog_options.t1(a int)
	        USING iceberg
	        WITH ({with_clause})
	    """.strip()

        err = run_command(query, pg_conn, raise_error=False)
        assert (
            "writable object_store catalog iceberg tables do not allow explicit"
            in str(err)
        )
        pg_conn.rollback()

    run_command(f"""DROP SCHEMA test_object_store_catalog_options CASCADE""", pg_conn)
    pg_conn.commit()


# make sure nothing crashes
def test_writable_object_store_without_default_location_guc(pg_conn, s3, extension):

    run_command(f"""CREATE SCHEMA test_object_store_catalog_options""", pg_conn)
    pg_conn.commit()

    query = f"""
        CREATE TABLE test_object_store_catalog_options.t1(a int)
        USING iceberg
        WITH (catalog='object_store')
    """.strip()

    err = run_command(query, pg_conn, raise_error=False)
    assert (
        "object_store catalog iceberg tables require pg_lake_iceberg.object_store_catalog_location_prefix"
        in str(err)
    )
    pg_conn.rollback()

    run_command(f"""DROP SCHEMA test_object_store_catalog_options CASCADE""", pg_conn)
    pg_conn.commit()


# if there is no acceptable 'catalog_name', 'catalog_namespace', 'catalog_table_name'
def test_read_only_object_store_with_non_existing_options(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    run_command(
        f"""CREATE SCHEMA test_read_only_object_store_with_non_existing_options""",
        pg_conn,
    )

    # let's first create a writable table
    run_command(
        f"""CREATE TABLE test_read_only_object_store_with_non_existing_options.wrt_tbl(a INT) USING iceberg WITH (catalog='object_store')""",
        pg_conn,
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_read_only_object_store_with_non_existing_options", "wrt_tbl"
    )

    # now, given there is no 1-1 mapping,
    res = run_command(
        f"""CREATE TABLE test_read_only_object_store_with_non_existing_options.read_tbl(a INT) USING iceberg WITH (catalog='object_store', read_only=True)""",
        pg_conn,
        raise_error=False,
    )
    assert "no table found with catalog table namespace" in str(res)
    pg_conn.rollback()

    # now, give proper table name but different namespace name
    res = run_command(
        f"""CREATE TABLE test_read_only_object_store_with_non_existing_options.read_tbl(a INT) USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='wrt_tbl', catalog_namespace='no_nsp')""",
        pg_conn,
        raise_error=False,
    )
    assert "no table found with catalog table namespace" in str(res)
    pg_conn.rollback()

    # now, give proper table name but different catalog name
    res = run_command(
        f"""CREATE TABLE test_read_only_object_store_with_non_existing_options.read_tbl(a INT) USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='wrt_tbl', catalog_name='no_ctlg')""",
        pg_conn,
        raise_error=False,
    )
    assert "object_store catalog does not exist for" in str(res)
    pg_conn.rollback()

    # now, create the table in another schema
    run_command("CREATE SCHEMA tmp_read_only_schema", pg_conn)

    res = run_command(
        f"""CREATE TABLE tmp_read_only_schema.read_tbl(a INT) USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='wrt_tbl')""",
        pg_conn,
        raise_error=False,
    )
    assert "no table found with catalog table namespace" in str(res)
    pg_conn.rollback()

    run_command(
        f"""DROP SCHEMA test_read_only_object_store_with_non_existing_options CASCADE""",
        pg_conn,
    )
    pg_conn.commit()


# basic flow for object store catalog tables
# changes on the source is reflected on the target
def test_read_only_object_store_read_write(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    run_command("SET pg_lake_iceberg.default_catalog TO 'object_store'", pg_conn)
    run_command(
        f"""CREATE SCHEMA test_read_only_object_store_read_write""",
        pg_conn,
    )

    # let's first create a writable table
    run_command(
        f"""CREATE TABLE test_read_only_object_store_read_write.wrt_tbl(a INT) USING iceberg""",
        pg_conn,
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_read_only_object_store_read_write", "wrt_tbl"
    )

    # now, let's create the reader table
    run_command(
        f"""CREATE TABLE test_read_only_object_store_read_write.read_tbl(a INT) USING iceberg WITH (read_only=True, catalog_table_name = 'wrt_tbl')""",
        pg_conn,
    )
    pg_conn.commit()

    assert_tables_are_the_same(
        pg_conn,
        "test_read_only_object_store_read_write.read_tbl",
        "test_read_only_object_store_read_write.wrt_tbl",
    )

    # now, insert few rows to the wrt_table, and that's reflected in read_tbl
    run_command(
        "INSERT INTO test_read_only_object_store_read_write.wrt_tbl VALUES (1),(100),(1000)",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_read_only_object_store_read_write", "wrt_tbl"
    )

    assert_tables_are_the_same(
        pg_conn,
        "test_read_only_object_store_read_write.read_tbl",
        "test_read_only_object_store_read_write.wrt_tbl",
    )

    # now, some bacth insert and then a positional delete
    run_command(
        "INSERT INTO test_read_only_object_store_read_write.wrt_tbl SELECT i FROM generate_series(2000,2100) i",
        pg_conn,
    )
    run_command(
        "UPDATE test_read_only_object_store_read_write.wrt_tbl SET a = 10000 WHERE a IN (2005, 2006, 2007)",
        pg_conn,
    )
    run_command(
        "UPDATE test_read_only_object_store_read_write.wrt_tbl SET a = 10001 WHERE a = 10001",
        pg_conn,
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_read_only_object_store_read_write", "wrt_tbl"
    )

    assert_tables_are_the_same(
        pg_conn,
        "test_read_only_object_store_read_write.read_tbl",
        "test_read_only_object_store_read_write.wrt_tbl",
    )

    # more data files to compact
    for i in range(0, 5):
        run_command(
            "INSERT INTO test_read_only_object_store_read_write.wrt_tbl SELECT i FROM generate_series(2000,2010) i",
            pg_conn,
        )
    pg_conn.commit()

    pg_conn.autocommit = True
    run_command("VACUUM FULL test_read_only_object_store_read_write.wrt_tbl", pg_conn)
    pg_conn.autocommit = False

    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_read_only_object_store_read_write", "wrt_tbl"
    )

    assert_tables_are_the_same(
        pg_conn,
        "test_read_only_object_store_read_write.read_tbl",
        "test_read_only_object_store_read_write.wrt_tbl",
    )

    run_command(
        f"""DROP SCHEMA test_read_only_object_store_read_write CASCADE""",
        pg_conn,
    )
    pg_conn.commit()
    run_command("RESET pg_lake_iceberg.default_catalog", pg_conn)


# test all possible renames
def test_object_catalog_renames(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    run_command("SET pg_lake_iceberg.default_catalog TO 'OBJECT_STORE'", pg_conn)

    run_command(
        f"""
        CREATE SCHEMA object_store_sc1;
        CREATE TABLE object_store_sc1.tbl(a int) USING iceberg;
        INSERT INTO object_store_sc1.tbl SELECT i FROM generate_series(0,99)i;

        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl")

    run_command(
        f"""
		CREATE SCHEMA object_store_sc2;
		CREATE TABLE object_store_sc2.tbl(a int) USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1');
        
        	""",
        pg_conn,
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl")

    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl",
        "object_store_sc2.tbl",
    )

    # now, rename both tables
    run_command(
        """
    			ALTER TABLE object_store_sc1.tbl RENAME TO tbl_renamed;
    			ALTER FOREIGN TABLE object_store_sc2.tbl OPTIONS (set catalog_table_name  'tbl_renamed');		
    """,
        pg_conn,
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "object_store_sc1", "tbl_renamed"
    )

    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_renamed",
        "object_store_sc2.tbl",
    )

    # now, move object_store_sc1.tbl_renamed to object_store_sc2
    run_command(
        """
        	ALTER TABLE object_store_sc1.tbl_renamed SET SCHEMA object_store_sc2;
    		ALTER FOREIGN TABLE object_store_sc2.tbl OPTIONS (set catalog_namespace 'object_store_sc2');		

        """,
        pg_conn,
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(
        pg_conn, "object_store_sc2", "tbl_renamed"
    )

    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc2.tbl_renamed",
        "object_store_sc2.tbl",
    )
    # TODO:
    # rename schema is a bit tricky, for now we
    # do not automatically push the changes, but an insert can trigger
    # change. Or, an explicit regenerate_object_store_catalog() call.
    # In the future, we could look for all the tables in this schema,
    # and if there are any writable object catalog tables, we do trigger
    run_command("ALTER SCHEMA object_store_sc2 RENAME to sc3", pg_conn)
    run_command(
        "ALTER FOREIGN TABLE sc3.tbl OPTIONS (set catalog_namespace 'sc3');", pg_conn
    )
    run_command(
        "SELECT lake_iceberg.trigger_object_store_catalog_generation()", pg_conn
    )
    pg_conn.commit()

    wait_until_object_store_writable_table_pushed(pg_conn, "sc3", "tbl_renamed")

    assert_tables_are_the_same(
        pg_conn,
        "sc3.tbl_renamed",
        "sc3.tbl",
    )

    run_command(
        f"""DROP SCHEMA object_store_sc1, sc3 CASCADE""",
        pg_conn,
    )
    pg_conn.commit()
    run_command("RESET pg_lake_iceberg.default_catalog", pg_conn)


# two writers, and each writer has two readers
def test_multiple_readers_writers(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    run_command(
        f"""
        CREATE SCHEMA object_store_sc1;
        CREATE TABLE object_store_sc1.tbl_1(a int) USING iceberg WITH (catalog='objecT_store');
        INSERT INTO object_store_sc1.tbl_1 SELECT i FROM generate_series(0,99)i;

        CREATE TABLE object_store_sc1.tbl_2(a int) USING iceberg WITH (CATALOG='object_STORE');
        INSERT INTO object_store_sc1.tbl_2 SELECT i FROM generate_series(0,199)i;

        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_2")

    run_command(
        f"""
		CREATE SCHEMA object_store_sc2;
		CREATE TABLE object_store_sc2.tbl_1_1() USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1');
		CREATE TABLE object_store_sc2.tbl_1_2() USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1');

		CREATE TABLE object_store_sc2.tbl_2_1() USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_2');
		CREATE TABLE object_store_sc2.tbl_2_2() USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_2');

        
        	""",
        pg_conn,
    )
    pg_conn.commit()

    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_1",
        "object_store_sc2.tbl_1_1",
    )
    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_1",
        "object_store_sc2.tbl_1_2",
    )

    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_2",
        "object_store_sc2.tbl_2_1",
    )
    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_2",
        "object_store_sc2.tbl_2_2",
    )

    run_command(
        f"""
        INSERT INTO object_store_sc1.tbl_1 SELECT i FROM generate_series(0,299)i;
        INSERT INTO object_store_sc1.tbl_2 SELECT i FROM generate_series(0,399)i;

        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_2")

    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_1",
        "object_store_sc2.tbl_1_1",
    )
    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_1",
        "object_store_sc2.tbl_1_2",
    )

    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_2",
        "object_store_sc2.tbl_2_1",
    )
    assert_tables_are_the_same(
        pg_conn,
        "object_store_sc1.tbl_2",
        "object_store_sc2.tbl_2_2",
    )

    run_command(
        f"""DROP SCHEMA object_store_sc1, object_store_sc2 CASCADE""",
        pg_conn,
    )
    pg_conn.commit()


# we do not support diverged schemas between source and the target tables
def test_schema_mismatch(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    run_command(
        f"""
        CREATE SCHEMA object_store_sc1;
        CREATE TABLE object_store_sc1.tbl_1(a int) USING iceberg WITH (catalog='object_store');
        INSERT INTO object_store_sc1.tbl_1 SELECT i FROM generate_series(0,99)i;

        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_1")

    run_command(
        f"""
		CREATE SCHEMA object_store_sc2;
		CREATE TABLE object_store_sc2.tbl_1() USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1');

        	""",
        pg_conn,
    )
    pg_conn.commit()

    run_command("ALTER TABLE object_store_sc1.tbl_1 ADD COLUMN b INT", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_1")

    # now, cannot read object_store_sc2.tbl_1
    res = run_command(
        "SELECT * FROM object_store_sc2.tbl_1", pg_conn, raise_error=False
    )
    assert "Schema mismatch between Iceberg and Postgres for field ids 2 vs 2" in str(
        res
    )
    pg_conn.rollback()

    run_command(
        f"""DROP SCHEMA object_store_sc1, object_store_sc2 CASCADE""",
        pg_conn,
    )
    pg_conn.commit()


# show partition pruning is fine on read_only tables
def test_partitioned_read_only(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    explain_prefix = "EXPLAIN (analyze, verbose, format json) "

    run_command(
        f"""
        CREATE SCHEMA object_store_sc1;
        CREATE TABLE object_store_sc1.tbl_1(a int) USING iceberg WITH (catalog='object_store', partition_by='a');
        INSERT INTO object_store_sc1.tbl_1 SELECT i FROM generate_series(1,4)i;

        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_1")

    run_command(
        f"""
		CREATE SCHEMA object_store_sc2;
		CREATE TABLE object_store_sc2.tbl_1() USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1');

        	""",
        pg_conn,
    )
    pg_conn.commit()

    # first, verify 8 data files
    plan = run_query(f"{explain_prefix} SELECT * FROM object_store_sc1.tbl_1", pg_conn)
    assert fetch_data_files_used(plan) == str("4")
    plan = run_query(f"{explain_prefix} SELECT * FROM object_store_sc2.tbl_1", pg_conn)
    assert fetch_data_files_used(plan) == str("4")

    # first, verify pruning
    plan = run_query(
        f"{explain_prefix} SELECT * FROM object_store_sc1.tbl_1 WHERE a = 1", pg_conn
    )
    assert fetch_data_files_used(plan) == str("1")
    plan = run_query(
        f"{explain_prefix} SELECT * FROM object_store_sc2.tbl_1 WHERE a = 1", pg_conn
    )
    assert fetch_data_files_used(plan) == str("1")

    run_command(
        f"""DROP SCHEMA object_store_sc1, object_store_sc2 CASCADE""",
        pg_conn,
    )
    pg_conn.commit()


def test_unsupported_modifications_for_read_only(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):

    run_command(
        f"""
        CREATE SCHEMA object_store_sc1;
        CREATE TABLE object_store_sc1.tbl_1(a int) USING iceberg WITH (catalog='object_store');
        INSERT INTO object_store_sc1.tbl_1 SELECT i FROM generate_series(1,8)i;
        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_1")

    res = run_command(
        f"""
		CREATE SCHEMA object_store_sc2;
		CREATE TABLE object_store_sc2.tbl_1 USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1') AS SELECT * FROM object_store_sc1.tbl_1;

        	""",
        pg_conn,
        raise_error=False,
    )
    assert "does not allow inserts" in str(res)
    pg_conn.rollback()

    res = run_command(
        f"""
		CREATE SCHEMA object_store_sc2;
		CREATE TABLE object_store_sc2.tbl_1 () USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1');

        	""",
        pg_conn,
    )
    pg_conn.commit()

    # we cannot modify the table
    cmds = [
        ("INSERT INTO object_store_sc2.tbl_1 (a) VALUES (1)", "does not allow inserts"),
        ("DELETE FROM object_store_sc2.tbl_1", "does not allow deletes"),
        ("UPDATE object_store_sc2.tbl_1 SET a = 11111 ", "does not allow updates"),
        (
            "TRUNCATE object_store_sc2.tbl_1",
            "modifications on read-only iceberg tables are not supported",
        ),
        (
            "INSERT INTO object_store_sc2.tbl_1 SELECT * FROM object_store_sc2.tbl_1",
            "does not allow inserts",
        ),
        (
            "ALTER TABLE object_store_sc2.tbl_1 ADD COLUMN x INT",
            "modifications on read-only iceberg tables are not supported",
        ),
        (
            "ALTER TABLE object_store_sc2.tbl_1 SET (catalog_table_name='xx')",
            "modifications on read-only iceberg tables are not supported",
        ),
    ]
    for cmd, cmd_error in cmds:
        err = run_command(cmd, pg_conn, raise_error=False)
        assert cmd_error in str(err)
        pg_conn.rollback()

    res = run_command(
        f"""
		CREATE TABLE object_store_sc2.tbl_2 (a pg_class) USING iceberg WITH (catalog='object_store');

        	""",
        pg_conn,
        raise_error=False,
    )
    assert "table types are not supported as columns" in str(res)
    pg_conn.rollback()

    run_command(
        f"""DROP SCHEMA object_store_sc1, object_store_sc2 CASCADE""",
        pg_conn,
    )
    pg_conn.commit()


def test_if_not_exists_object_store(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):

    run_command(
        f"""
        CREATE SCHEMA object_store_sc1;
        CREATE TABLE IF NOT EXISTS object_store_sc1.tbl_1(a int) USING iceberg WITH (catalog='object_store');
          	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_1")

    res = run_command(
        f"""
		CREATE SCHEMA object_store_sc2;
		CREATE TABLE object_store_sc2.tbl_1 () USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1');
		CREATE TABLE IF NOT EXISTS object_store_sc2.tbl_1 () USING iceberg WITH (catalog='object_store', read_only=True, catalog_namespace='object_store_sc1', catalog_table_name='tbl_1');

        	""",
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"""DROP SCHEMA object_store_sc1, object_store_sc2 CASCADE""",
        pg_conn,
    )
    pg_conn.commit()


def test_iceberg_multiple_dbs(
    superuser_conn,
    s3,
    extension,
    installcheck,
    with_default_location,
    adjust_object_store_settings,
):
    if installcheck:
        return

    dbnames = [
        "Special-Table!_With.Multiple_Uses_Of@Chars#-Here~And*Here!name",
        "!~*();/?:@&=+$,#",
    ]

    superuser_conn.autocommit = True

    run_command(f'CREATE DATABASE "{dbnames[0]}";', superuser_conn)
    conn_to_db_1 = open_pg_conn_to_db(dbnames[0])
    run_command("CREATE EXTENSION pg_lake CASCADE", conn_to_db_1)
    run_command(
        f"SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}'",
        conn_to_db_1,
    )
    conn_to_db_1.commit()

    run_command("CREATE SCHEMA object_store_sc1;", conn_to_db_1)
    run_command(
        "CREATE TABLE object_store_sc1.tbl(a int) USING iceberg WITH (catalog='object_store');",
        conn_to_db_1,
    )
    run_command("INSERT INTO object_store_sc1.tbl VALUES (12345)", conn_to_db_1)
    conn_to_db_1.commit()
    wait_until_object_store_writable_table_pushed(
        conn_to_db_1, "object_store_sc1", "tbl"
    )

    run_command(f'CREATE DATABASE "{dbnames[1]}";', superuser_conn)

    conn_to_db_2 = open_pg_conn_to_db(dbnames[1])
    run_command("CREATE EXTENSION pg_lake CASCADE", conn_to_db_2)
    run_command(
        f"SET pg_lake_iceberg.default_location_prefix TO 's3://{TEST_BUCKET}'",
        conn_to_db_2,
    )
    conn_to_db_2.commit()

    run_command("CREATE SCHEMA object_store_sc1;", conn_to_db_2)
    run_command(
        f"CREATE TABLE object_store_sc1.tbl(a int) USING iceberg WITH (catalog='object_store', read_only=True, catalog_name='{dbnames[0]}');",
        conn_to_db_2,
    )

    res = run_query("SELECT count(*) FROM object_store_sc1.tbl", conn_to_db_2)
    assert len(res) == 1

    # now, create a writable table in this db
    run_command(
        f"CREATE TABLE object_store_sc1.tbl_2 USING iceberg WITH (catalog='object_store') AS SELECT i FROM generate_series(0,10)i;",
        conn_to_db_2,
    )

    conn_to_db_2.commit()
    wait_until_object_store_writable_table_pushed(
        conn_to_db_2, "object_store_sc1", "tbl_2"
    )

    run_command(
        f"CREATE TABLE object_store_sc1.tbl_2() USING iceberg WITH (catalog='object_store', read_only=True, catalog_name='{dbnames[1]}');",
        conn_to_db_1,
    )
    res = run_query("SELECT count(*) FROM object_store_sc1.tbl_2", conn_to_db_1)
    assert res == [[11]]

    conn_to_db_1.close()
    conn_to_db_2.close()

    for dbname in dbnames:
        superuser_conn.autocommit = True
        run_command(f'DROP DATABASE "{dbname}" WITH (FORCE);', superuser_conn)

    superuser_conn.autocommit = False


def test_create_table_with_default_location_object_store(
    pg_conn,
    superuser_conn,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
):
    dbname = run_query("SELECT current_database()", pg_conn)[0][0]
    run_command(
        "CREATE SCHEMA test_create_table_with_default_location_object_store", pg_conn
    )
    run_command(
        f"""CREATE TABLE test_create_table_with_default_location_object_store.tbl (a int, b int)
                    USING iceberg WITH (catalog='object_store')""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(
        pg_conn, "test_create_table_with_default_location_object_store", "tbl"
    )

    # assert metadata location
    result = run_query(
        """SELECT metadata_location FROM iceberg_tables
                           WHERE table_namespace = 'test_create_table_with_default_location_object_store' and table_name = 'tbl'
                       """,
        pg_conn,
    )
    first_table_metadata_location = result[0][0]

    table_oid = run_query(
        """SELECT oid FROM pg_class
                            WHERE oid = 'test_create_table_with_default_location_object_store.tbl'::regclass
                          """,
        pg_conn,
    )[0][0]

    prefix = run_query(
        "SHOW pg_lake_iceberg.internal_object_store_catalog_prefix", superuser_conn
    )[0][0]
    superuser_conn.commit()

    assert (
        f"s3://{TEST_BUCKET}/{prefix}/tables/{dbname}/test_create_table_with_default_location_object_store/tbl/{table_oid}"
        in first_table_metadata_location
    )

    # drop the table and create it again
    run_command(
        "DROP SCHEMA test_create_table_with_default_location_object_store CASCADE",
        pg_conn,
    )
    pg_conn.commit()


def test_complex_types_object_store(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):

    map_type_name = create_map_type("int", "text")

    run_command(
        f"""
        CREATE SCHEMA object_store_sc1;
    	
    	-- composite type
    	CREATE TYPE object_store_sc1.user_composite AS (a int, b float, map {map_type_name});


        CREATE TABLE IF NOT EXISTS object_store_sc1.tbl_1(a int, b object_store_sc1.user_composite, c int[]) USING iceberg WITH (catalog='object_store');

        INSERT INTO object_store_sc1.tbl_1 SELECT 1, ROW(1,2.5,'{{"(1,2)","(2,3)"}}'::{map_type_name})::object_store_sc1.user_composite, ARRAY[1,2,3];
        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "object_store_sc1", "tbl_1")

    # one table with column inferred, the other column explicit
    res = run_command(
        f"""
		CREATE TABLE object_store_sc1.tbl_2 () USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='tbl_1');
		CREATE TABLE object_store_sc1.tbl_3 (a int, b object_store_sc1.user_composite, c int[]) USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='tbl_1');

        	""",
        pg_conn,
    )
    pg_conn.commit()

    res_1 = run_query("SELECT a, b FROM object_store_sc1.tbl_1", pg_conn)
    res_2 = run_query("SELECT a, b FROM object_store_sc1.tbl_2", pg_conn)
    res_3 = run_query("SELECT a, b FROM object_store_sc1.tbl_3", pg_conn)

    assert res_1 == res_2
    assert res_1 == res_3

    run_command(
        f"""
        DROP SCHEMA object_store_sc1 CASCADE;
        	""",
        pg_conn,
    )
    pg_conn.commit()


def test_object_store_alter_column_type_int_to_bigint(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    """Test allowed Iceberg type promotion int -> bigint on object_store catalog table"""
    run_command("CREATE SCHEMA os_type_promo;", pg_conn)
    run_command(
        """
        CREATE TABLE os_type_promo.tbl (a int, b int)
        USING iceberg WITH (catalog='object_store');
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("INSERT INTO os_type_promo.tbl VALUES (1, 2);", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo", "tbl")

    # promote int -> bigint (allowed by Iceberg spec)
    run_command("ALTER TABLE os_type_promo.tbl ALTER COLUMN a TYPE bigint;", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo", "tbl")

    # verify column type changed in PG catalog
    result = run_query(
        "SELECT atttypid::regtype FROM pg_attribute "
        "WHERE attrelid = 'os_type_promo.tbl'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert result == [["bigint"]]

    # insert data that requires bigint range
    run_command(f"INSERT INTO os_type_promo.tbl VALUES ({2**40}, 3);", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo", "tbl")

    # verify all data is readable
    results = run_query("SELECT a, b FROM os_type_promo.tbl ORDER BY b ASC", pg_conn)
    assert results == [[1, 2], [2**40, 3]]

    # verify object store reader can see the promoted schema
    run_command(
        """
        CREATE TABLE os_type_promo.reader ()
        USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='tbl');
    """,
        pg_conn,
    )
    pg_conn.commit()

    reader_type = run_query(
        "SELECT atttypid::regtype FROM pg_attribute "
        "WHERE attrelid = 'os_type_promo.reader'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert reader_type == [["bigint"]]

    assert_tables_are_the_same(pg_conn, "os_type_promo.tbl", "os_type_promo.reader")

    run_command("DROP SCHEMA os_type_promo CASCADE;", pg_conn)
    pg_conn.commit()


def test_object_store_alter_column_type_float_to_double(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    """Test allowed Iceberg type promotion float -> double on object_store catalog table"""
    run_command("CREATE SCHEMA os_type_promo_f;", pg_conn)
    run_command(
        """
        CREATE TABLE os_type_promo_f.tbl (a real, b int)
        USING iceberg WITH (catalog='object_store');
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("INSERT INTO os_type_promo_f.tbl VALUES (1.5, 1);", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_f", "tbl")

    # promote float4 -> float8
    run_command(
        "ALTER TABLE os_type_promo_f.tbl ALTER COLUMN a TYPE double precision;",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_f", "tbl")

    # verify column type changed
    result = run_query(
        "SELECT atttypid::regtype FROM pg_attribute "
        "WHERE attrelid = 'os_type_promo_f.tbl'::regclass AND attname = 'a'",
        pg_conn,
    )
    assert result == [["double precision"]]

    # insert data with higher precision
    run_command(
        "INSERT INTO os_type_promo_f.tbl VALUES (1.23456789012345, 2);", pg_conn
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_f", "tbl")

    results = run_query("SELECT a, b FROM os_type_promo_f.tbl ORDER BY b ASC", pg_conn)
    assert results == [[1.5, 1], [1.23456789012345, 2]]

    run_command("DROP SCHEMA os_type_promo_f CASCADE;", pg_conn)
    pg_conn.commit()


def test_object_store_alter_column_type_decimal_widen_precision(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    """Test allowed Iceberg type promotion decimal(P,S) -> decimal(P',S) on object_store catalog table"""
    run_command("CREATE SCHEMA os_type_promo_d;", pg_conn)
    run_command(
        """
        CREATE TABLE os_type_promo_d.tbl (a numeric(10,2), b int)
        USING iceberg WITH (catalog='object_store');
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("INSERT INTO os_type_promo_d.tbl VALUES (12345.67, 1);", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_d", "tbl")

    # widen precision: numeric(10,2) -> numeric(20,2)
    run_command(
        "ALTER TABLE os_type_promo_d.tbl ALTER COLUMN a TYPE numeric(20,2);", pg_conn
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_d", "tbl")

    # insert data requiring wider precision
    run_command(
        "INSERT INTO os_type_promo_d.tbl VALUES (123456789012345.67, 2);", pg_conn
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_d", "tbl")

    results = run_query("SELECT a, b FROM os_type_promo_d.tbl ORDER BY b ASC", pg_conn)
    assert len(results) == 2
    assert str(results[0][0]) == "12345.67"
    assert str(results[1][0]) == "123456789012345.67"

    run_command("DROP SCHEMA os_type_promo_d CASCADE;", pg_conn)
    pg_conn.commit()


def test_object_store_alter_column_type_disallowed(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    """Test that disallowed type promotions are rejected for object_store catalog Iceberg tables"""
    run_command("CREATE SCHEMA os_type_promo_dis;", pg_conn)
    run_command(
        """
        CREATE TABLE os_type_promo_dis.tbl (a int, b bigint, c double precision, d numeric(10,2))
        USING iceberg WITH (catalog='object_store');
    """,
        pg_conn,
    )
    pg_conn.commit()

    disallowed_cases = [
        ("a", "varchar(255)", "int to varchar"),
        ("b", "int", "bigint to int"),
        ("c", "real", "double to float"),
        ("d", "numeric(10,3)", "decimal scale change"),
        ("d", "numeric(8,2)", "decimal precision narrowing"),
        ("a", "real", "int to real"),
    ]

    for col, new_type, description in disallowed_cases:
        error = run_command(
            f"ALTER TABLE os_type_promo_dis.tbl ALTER COLUMN {col} TYPE {new_type};",
            pg_conn,
            raise_error=False,
        )
        assert error is not None, f"Expected error for {description}"
        assert "not supported" in str(
            error
        ), f"Expected 'not supported' error for {description}, got: {error}"
        assert "Allowed type promotions for Iceberg tables are" in str(
            error
        ), f"Expected detail message for {description}, got: {error}"
        pg_conn.rollback()

    run_command("DROP SCHEMA os_type_promo_dis CASCADE;", pg_conn)
    pg_conn.commit()


def test_object_store_alter_column_type_with_reader(
    pg_conn, s3, extension, with_default_location, adjust_object_store_settings
):
    """Test type promotion followed by data insert, verifying object_store reader sees changes"""
    run_command("CREATE SCHEMA os_type_promo_rdr;", pg_conn)
    run_command(
        """
        CREATE TABLE os_type_promo_rdr.tbl (a int, b real, c int)
        USING iceberg WITH (catalog='object_store');
    """,
        pg_conn,
    )
    pg_conn.commit()

    run_command("INSERT INTO os_type_promo_rdr.tbl VALUES (1, 1.5, 10);", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_rdr", "tbl")

    # promote multiple columns
    run_command(
        "ALTER TABLE os_type_promo_rdr.tbl ALTER COLUMN a TYPE bigint;", pg_conn
    )
    run_command(
        "ALTER TABLE os_type_promo_rdr.tbl ALTER COLUMN b TYPE double precision;",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_rdr", "tbl")

    # insert data with promoted types
    run_command(
        f"INSERT INTO os_type_promo_rdr.tbl VALUES ({2**40}, 1.23456789012345, 20);",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, "os_type_promo_rdr", "tbl")

    results = run_query(
        "SELECT a, b, c FROM os_type_promo_rdr.tbl ORDER BY c ASC", pg_conn
    )
    assert results == [[1, 1.5, 10], [2**40, 1.23456789012345, 20]]

    # create read-only reader and verify schema + data match
    run_command(
        """
        CREATE TABLE os_type_promo_rdr.reader ()
        USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='tbl');
    """,
        pg_conn,
    )
    pg_conn.commit()

    reader_types = run_query(
        "SELECT attname, atttypid::regtype FROM pg_attribute "
        "WHERE attrelid = 'os_type_promo_rdr.reader'::regclass AND attnum > 0 "
        "ORDER BY attnum",
        pg_conn,
    )
    assert reader_types == [
        ["a", "bigint"],
        ["b", "double precision"],
        ["c", "integer"],
    ]

    assert_tables_are_the_same(
        pg_conn, "os_type_promo_rdr.tbl", "os_type_promo_rdr.reader"
    )

    run_command("DROP SCHEMA os_type_promo_rdr CASCADE;", pg_conn)
    pg_conn.commit()


namespaces = [
    "regular_nsp_name",
    "nonregular_nsp !~*() name:$Uses_Of@",
]


@pytest.mark.parametrize("namespace", namespaces)
def test_re_create_tables(
    pg_conn,
    namespace,
    s3,
    extension,
    with_default_location,
    adjust_object_store_settings,
):

    map_type_name = create_map_type("int", "text")

    run_command(
        f"""
        CREATE SCHEMA "{namespace}";
    	

        CREATE TABLE IF NOT EXISTS "{namespace}".tbl_1 (a int) USING iceberg WITH (catalog='object_store');

        INSERT INTO "{namespace}".tbl_1 SELECT i FROM generate_series(0,5)i;

        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, f"""{namespace}""", "tbl_1")

    res = run_command(
        f"""
		CREATE TABLE "{namespace}".tbl_2 () USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='tbl_1');
        	""",
        pg_conn,
    )
    pg_conn.commit()

    assert_tables_are_the_same(
        pg_conn,
        f'"{namespace}".tbl_1',
        f'"{namespace}".tbl_2',
    )

    # now, drop and recreate 2nd table
    res = run_command(
        f"""
		DROP TABLE "{namespace}".tbl_2;
		CREATE TABLE "{namespace}".tbl_2 () USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='tbl_1');
        	""",
        pg_conn,
    )
    pg_conn.commit()

    assert_tables_are_the_same(
        pg_conn,
        f'"{namespace}".tbl_1',
        f'"{namespace}".tbl_2',
    )

    # now, drop and recreate 2nd table, and INSERT in-between
    res = run_command(
        f"""
		DROP TABLE "{namespace}".tbl_2;
        INSERT INTO "{namespace}".tbl_1 SELECT i FROM generate_series(5,10)i;
		CREATE TABLE "{namespace}".tbl_2 () USING iceberg WITH (catalog='object_store', read_only=True, catalog_table_name='tbl_1');
        	""",
        pg_conn,
    )
    pg_conn.commit()
    wait_until_object_store_writable_table_pushed(pg_conn, f"{namespace}", "tbl_1")

    assert_tables_are_the_same(
        pg_conn,
        f'"{namespace}".tbl_1',
        f'"{namespace}".tbl_2',
    )

    # now, drop the writable one, the readable should fail
    run_command(f"""DROP TABLE "{namespace}".tbl_1""", pg_conn)
    pg_conn.commit()
    wait_until_object_store_writable_table_removed(pg_conn, f"{namespace}", "tbl_1")

    res = run_command(
        f"""SELECT * FROM "{namespace}".tbl_2""", pg_conn, raise_error=False
    )
    assert "no table found with catalog table namespace" in str(res)
    pg_conn.rollback()

    run_command(
        f"""
        DROP SCHEMA "{namespace}" CASCADE;
        	""",
        pg_conn,
    )
    pg_conn.commit()


def assert_tables_are_the_same(pg_conn, tbl_1, tbl_2):
    res = run_query(
        f"""
    SELECT
        (SELECT count(*) FROM (
	      SELECT * FROM {tbl_1}
	      EXCEPT
	      SELECT * FROM {tbl_2}
	  ) AS diff1) AS read_minus_wrt,
	  (SELECT count(*) FROM (
	      SELECT * FROM {tbl_2}
	      EXCEPT
	      SELECT * FROM {tbl_1}
	  ) AS diff2) AS wrt_minus_read;
	""",
        pg_conn,
    )

    assert res[0][0] == 0 and res[0][1] == 0


def set_catalog_prefixes(read_only_prefix="_catalog", read_write_prefix="_catalog"):
    run_command(
        f"""ALTER SYSTEM SET pg_lake_iceberg.object_store_catalog_read_only_prefix TO {read_only_prefix};""",
        superuser_conn,
    )
    run_command(
        f"""ALTER SYSTEM SET pg_lake_iceberg.object_store_catalog_read_write_prefix TO {read_write_prefix};""",
        superuser_conn,
    )

    run_command("SELECT pg_reload_conf()", superuser_conn)
