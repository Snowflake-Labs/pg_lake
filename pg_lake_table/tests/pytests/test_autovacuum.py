import pytest
from utils_pytest import *


# concurrently create 3 databases, and make sure
# autovacuum kicks in properly for all of them
# we also use different table counts in each test
# which is appended to the name of the table db_with_tbl_cnt_N
def test_iceberg_auto_vacuum_on_dbs(
    superuser_conn, s3, extension, installcheck, set_auto_vacuum_params
):
    if installcheck:
        return

    dbnames = ["db_with_tbl_cnt_0", "db_with_tbl_cnt_1", "db_with_tbl_cnt_5"]

    for dbname in dbnames:
        location = f"s3://{TEST_BUCKET}/test_iceberg_auto_vacuum_on_dbs/{dbname}"

        superuser_conn.autocommit = True
        run_command(f"CREATE DATABASE {dbname};", superuser_conn)

        conn_to_db = open_pg_conn_to_db(dbname)

        run_command("CREATE EXTENSION pg_lake CASCADE", conn_to_db)
        table_cnt = int(dbname.split("_")[-1])
        iceberg_autovacuum_on_db(conn_to_db, table_cnt, s3, extension, location)
        conn_to_db.close()

    for dbname in dbnames:
        superuser_conn.autocommit = True
        run_command(f"DROP DATABASE {dbname} WITH (FORCE);", superuser_conn)


def iceberg_autovacuum_on_db(conn_to_db, tbl_count, s3, extension, d_location):
    run_command(
        f"""
		CREATE SCHEMA test_iceberg_autovacuum;
	""",
        conn_to_db,
    )
    conn_to_db.commit()

    for i in range(0, tbl_count):
        run_command(
            f"""
			CREATE TABLE test_iceberg_autovacuum.test_iceberg_autovacuum_{i} USING iceberg WITH (location='{d_location}/{i}') AS
			SELECT s, md5(s::text) FROM generate_series(1,10) s;
   
            CREATE TABLE test_iceberg_autovacuum.test_iceberg_autovacuum_{i}_part USING iceberg WITH (location='{d_location}/{i}_part', partition_by = 'a') AS
			SELECT s % 2 as a, md5(s::text) FROM generate_series(1,10) s;
		""",
            conn_to_db,
        )
        conn_to_db.commit()
        print(f"created table {tbl_count}")
    # make sure that there are frequent enough autovacuum
    result = run_query(
        """
		SELECT current_setting('pg_lake_iceberg.autovacuum_naptime');
	""",
        conn_to_db,
    )
    assert result[0][0] == "1s"

    # now, check the number of data files
    for i in range(0, tbl_count):
        result = run_query(
            f"""
			SELECT count(*) FROM lake_iceberg.files((SELECT metadata_location FROM iceberg_tables WHERE table_name ='test_iceberg_autovacuum_{i}'))
		""",
            conn_to_db,
        )
        assert result[0][0] == 1

        result = run_query(
            f"""
			SELECT count(*) FROM lake_iceberg.files((SELECT metadata_location FROM iceberg_tables WHERE table_name ='test_iceberg_autovacuum_{i}_part'))
		""",
            conn_to_db,
        )
        assert result[0][0] == 2

    # now, as soon as we add a file, it should be merged via auto-vacuum
    for i in range(0, tbl_count):
        run_command(
            f"""
			INSERT INTO test_iceberg_autovacuum.test_iceberg_autovacuum_{i}
			SELECT s, md5(s::text) FROM generate_series(1,10) s;
   
            INSERT INTO test_iceberg_autovacuum.test_iceberg_autovacuum_{i}_part
            SELECT s % 2, md5(s::text) FROM generate_series(1,10) s;
		""",
            conn_to_db,
        )
        conn_to_db.commit()

    for i in range(0, tbl_count):
        cnt = 0
        # try at most 5 seconds, otherwise there is probably something wrong
        while True:
            result = run_query(
                f"""
				SELECT count(*) FROM lake_iceberg.files((SELECT metadata_location FROM iceberg_tables WHERE table_name ='test_iceberg_autovacuum_{i}'))
			""",
                conn_to_db,
            )[0][0]

            result_part = run_query(
                f"""
				SELECT count(*) FROM lake_iceberg.files((SELECT metadata_location FROM iceberg_tables WHERE table_name ='test_iceberg_autovacuum_{i}_part'))
			""",
                conn_to_db,
            )[0][0]

            if result != 1 or result_part != 2:
                time.sleep(0.1)
                cnt = cnt + 1
                if cnt == 50:
                    assert False, "autovacuum did not kick in"
            else:
                # happy case, as expected we compacted the files
                break


# run tests faster
@pytest.fixture(autouse=True, scope="function")
def set_auto_vacuum_params(superuser_conn):
    run_command_outside_tx(
        [
            "ALTER SYSTEM SET pg_lake_table.vacuum_compact_min_input_files = 1;",
            "ALTER SYSTEM SET pg_lake_iceberg.autovacuum_naptime TO '1s';",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )
    yield
    run_command_outside_tx(
        [
            "ALTER SYSTEM RESET pg_lake_table.vacuum_compact_min_input_files;",
            "ALTER SYSTEM RESET pg_lake_iceberg.autovacuum_naptime",
            "SELECT pg_reload_conf()",
        ],
        superuser_conn,
    )
