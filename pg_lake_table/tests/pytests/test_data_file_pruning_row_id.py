import pytest
from utils_pytest import *

SCHEMA = "test_data_file_pruning_row_id"

# Iceberg's reserved field id for the row lineage column _row_id.
ROW_ID_FIELD_ID = 2147483540

EXPLAIN = "EXPLAIN (analyze, verbose, format json) "


def test_row_id_ahead_of_columns(
    pg_conn, pgduck_conn, add_files_procedure, with_default_location
):
    """
    With _row_id ahead of a and b in the file, each column must still be pruned
    on its own bounds. Given the bounds of the column before it, a and b would
    look out of range for values the file holds, and the file would be skipped.
    """
    create_external_table(
        pg_conn,
        pgduck_conn,
        "row_id_first",
        "a bigint, b bigint",
        "SELECT 1000 + i AS _row_id, i AS a, 100 + i AS b FROM range(1, 11) t(i)",
        f"'_row_id': {ROW_ID_FIELD_ID}, 'a': 1, 'b': 2",
    )

    result = run_query(f"SELECT sum(a), sum(b) FROM {SCHEMA}.row_id_first", pg_conn)
    assert result == [[55, 1055]]

    result = run_query(
        f"SELECT count(*) FROM {SCHEMA}.row_id_first WHERE a = 5", pg_conn
    )
    assert result[0][0] == 1

    result = run_query(
        f"SELECT count(*) FROM {SCHEMA}.row_id_first WHERE b = 105", pg_conn
    )
    assert result[0][0] == 1

    results = run_query(
        f"{EXPLAIN} SELECT * FROM {SCHEMA}.row_id_first WHERE a = 50", pg_conn
    )
    assert fetch_data_files_used(results) == "0"


def test_row_id_between_columns(
    pg_conn, pgduck_conn, add_files_procedure, with_default_location
):
    """
    With _row_id between a and ts in the file, ts must keep its own bounds.
    Given _row_id's integers instead, casting the bounds to timestamptz fails.
    """
    create_external_table(
        pg_conn,
        pgduck_conn,
        "row_id_middle",
        "a bigint, ts timestamptz",
        "SELECT i AS a, 1000 + i AS _row_id, "
        "TIMESTAMPTZ '2026-01-01 00:00:00+00' + to_days(CAST(i AS INTEGER)) AS ts "
        "FROM range(1, 11) t(i)",
        f"'a': 1, '_row_id': {ROW_ID_FIELD_ID}, 'ts': 2",
    )

    result = run_query(
        f"SELECT count(*) FROM {SCHEMA}.row_id_middle "
        "WHERE ts >= '2026-01-06 00:00:00+00'",
        pg_conn,
    )
    assert result[0][0] == 6

    results = run_query(
        f"{EXPLAIN} SELECT * FROM {SCHEMA}.row_id_middle "
        "WHERE ts >= '2027-01-01 00:00:00+00'",
        pg_conn,
    )
    assert fetch_data_files_used(results) == "0"


def create_external_table(pg_conn, pgduck_conn, name, columns, file_query, field_ids):
    """
    Write file_query to a Parquet file with the given field ids, and read it
    through an external Iceberg table, whose pruning takes the bounds from the
    file's footer.
    """
    url = f"s3://{TEST_BUCKET}/{SCHEMA}/{name}.parquet"
    run_command(
        f"COPY ({file_query}) TO '{url}' (FORMAT parquet, FIELD_IDS {{{field_ids}}})",
        pgduck_conn,
    )

    run_command(
        f"""
        CREATE TABLE {SCHEMA}.{name}_internal ({columns})
        USING iceberg WITH (autovacuum_enabled = 'false')
        """,
        pg_conn,
    )
    pg_conn.commit()

    run_command(
        f"CALL lake_iceberg.add_files_to_table('{SCHEMA}.{name}_internal'::regclass, ARRAY['{url}'])",
        pg_conn,
    )
    pg_conn.commit()

    metadata_location = run_query(
        f"""
        SELECT metadata_location FROM iceberg_tables
        WHERE table_namespace = '{SCHEMA}' AND table_name = '{name}_internal'
        """,
        pg_conn,
    )[0][0]

    run_command(
        f"CREATE FOREIGN TABLE {SCHEMA}.{name} () SERVER pg_lake OPTIONS (path '{metadata_location}')",
        pg_conn,
    )
    pg_conn.commit()


@pytest.fixture(scope="module")
def add_files_procedure(superuser_conn, pg_conn, app_user, extension):
    run_command(
        f"""
        CREATE OR REPLACE PROCEDURE lake_iceberg.add_files_to_table(
                table_name REGCLASS,
                file_paths TEXT[])
        LANGUAGE C
        AS 'pg_lake_table', $function$add_files_to_table$function$;
        GRANT EXECUTE ON PROCEDURE lake_iceberg.add_files_to_table(regclass, text[]) TO {app_user};
        """,
        superuser_conn,
    )
    superuser_conn.commit()

    run_command(f"CREATE SCHEMA {SCHEMA}", pg_conn)
    pg_conn.commit()

    yield

    pg_conn.rollback()
    run_command(f"DROP SCHEMA {SCHEMA} CASCADE", pg_conn)
    pg_conn.commit()

    run_command("DROP PROCEDURE lake_iceberg.add_files_to_table", superuser_conn)
    superuser_conn.commit()
