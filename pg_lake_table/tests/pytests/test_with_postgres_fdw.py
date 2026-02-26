import pytest
from utils_pytest import *


def test_with_postgres_fdw(s3, superuser_conn, extension, installcheck):

    url_1 = f"s3://{TEST_BUCKET}/test_s3_collation/data1.parquet"

    # Create a Parquet file in mock S3
    run_command(
        f"""
        COPY (SELECT s AS id, s::text FROM generate_series(1,100) s) TO '{url_1}';
    """,
        superuser_conn,
    )
    superuser_conn.commit()

    # Create a table with 2 columns on the fdw with varying collation
    run_command(
        """
                CREATE EXTENSION IF NOT EXISTS postgres_fdw CASCADE;
                CREATE SCHEMA test_fdw_postgres_fdw;
                CREATE FOREIGN TABLE test_fdw_postgres_fdw.pglake_table (
                    c0 int,
                    c1 text
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');

                CREATE FOREIGN TABLE test_fdw_postgres_fdw.pglake_table2 (
                    c0 int,
                    c1 text
                ) SERVER pg_lake OPTIONS (format 'parquet', path '%s');

            DO $d$
                BEGIN
                    EXECUTE $$CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw
                        OPTIONS (dbname '$$||current_database()||$$',
                                 port '$$||current_setting('port')||$$',
                                 host '%s'
                        )$$;
                END;
            $d$;

            CREATE TABLE test_fdw_postgres_fdw.local_table (
                c0 int,
                c1 text
            );

            INSERT INTO test_fdw_postgres_fdw.local_table SELECT i, i::text FROM generate_series(1,100)i;
            CREATE USER MAPPING FOR CURRENT_USER SERVER loopback OPTIONS (user '%s');

            CREATE FOREIGN TABLE test_fdw_postgres_fdw.postgres_fdw_table (
                c0 int,
                c1 text
            ) SERVER loopback OPTIONS  (schema_name 'test_fdw_postgres_fdw', table_name 'local_table');

            CREATE FOREIGN TABLE test_fdw_postgres_fdw.postgres_fdw_table2 (
                c0 int,
                c1 text
            ) SERVER loopback OPTIONS  (schema_name 'test_fdw_postgres_fdw', table_name 'local_table');

        """
        % (url_1, url_1, server_params.PG_HOST, server_params.PG_USER),
        superuser_conn,
    )
    superuser_conn.commit()

    # do a simple join between local table, postgres_fdw and pg_lake table
    result = perform_query_on_cursor(
        "SELECT count(*) FROM test_fdw_postgres_fdw.pglake_table2 JOIN test_fdw_postgres_fdw.local_table USING (c0) JOIN test_fdw_postgres_fdw.postgres_fdw_table USING(c0) JOIN test_fdw_postgres_fdw.pglake_table USING(c0) JOIN test_fdw_postgres_fdw.postgres_fdw_table2 USING(c0)",
        superuser_conn,
    )
    assert result == [(100,)], "Join result do not match"
    superuser_conn.commit()

    # Cleanup
    run_command("DROP SCHEMA test_fdw_postgres_fdw CASCADE;", superuser_conn)
    superuser_conn.commit()
