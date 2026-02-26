"""DuckDB / EXPLAIN query inspection helpers."""

from .db import (
    perform_query_on_cursor,
    run_command,
    run_query,
)


def install_duckdb_extension(duckdb_conn, extension):
    run_command(f"INSTALL {extension};", duckdb_conn)
    run_command(f"LOAD {extension};", duckdb_conn)


# By ChatGPT
def find_key_in_json(data, target_key):
    # If the current data is a dictionary, check each key-value pair
    if isinstance(data, dict):
        for key, value in data.items():
            # If the key matches the target key, return the value
            if key == target_key:
                return value
            # Otherwise, recurse into the value
            found = find_key_in_json(value, target_key)
            if found is not None:
                return found

    # If the current data is a list, iterate over each element and recurse
    elif isinstance(data, list):
        for item in data:
            found = find_key_in_json(item, target_key)
            if found is not None:
                return found

    # Return None if the key is not found
    return None


def fetch_remote_sql(explain_result):
    return find_key_in_json(explain_result[0][0], "Vectorized SQL")


def fetch_data_files_used(explain_result):
    return find_key_in_json(explain_result[0][0], "Data Files Scanned")


def fetch_delete_files_used(explain_result):
    return find_key_in_json(explain_result[0][0], "Deletion Files Scanned")


def fetch_data_files_skipped(explain_result):
    return find_key_in_json(explain_result[0][0], "Data Files Skipped")


def data_file_count(pg_conn, table_name):
    result = run_query(
        f"select count(*) from lake_table.files where table_name = '{table_name}'::regclass",
        pg_conn,
    )
    return result[0]["count"]


def assert_remote_query_contains_expression(query, expression, pg_conn):
    explain = "EXPLAIN (ANALYZE, VERBOSE, format " "JSON" ") " + query
    explain_result = perform_query_on_cursor(explain, pg_conn)[0]
    remote_sql = fetch_remote_sql(explain_result)
    print(remote_sql)
    assert expression in remote_sql


def assert_remote_query_not_contains_expression(query, expression, pg_conn):
    explain = "EXPLAIN (ANALYZE, VERBOSE, format " "JSON" ") " + query
    explain_result = perform_query_on_cursor(explain, pg_conn)[0]
    remote_sql = fetch_remote_sql(explain_result)
    print(remote_sql)
    assert expression not in remote_sql
